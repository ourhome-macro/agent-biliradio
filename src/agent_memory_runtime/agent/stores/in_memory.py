from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import RLock
from uuid import uuid4

from agent_memory_runtime.agent.errors import (
    AgentApprovalError,
    AgentIdentityError,
    AgentRunConflictError,
    AgentRunNotFoundError,
)
from agent_memory_runtime.agent.models import (
    AgentCheckpoint,
    AgentRun,
    AgentTurn,
    ApprovalRecord,
    ApprovalStatus,
    RunStatus,
    ToolCallRecord,
    utc_now_iso,
)
from agent_memory_runtime.agent.stores.leases import assert_execution_lease


class InMemoryAgentStateStore:
    def __init__(self) -> None:
        self._runs: dict[str, AgentRun] = {}
        self._request_runs: dict[tuple[str, str], str] = {}
        self._checkpoints: dict[str, AgentCheckpoint] = {}
        self._turns: dict[str, AgentTurn] = {}
        self._tool_calls: dict[str, ToolCallRecord] = {}
        self._approvals: dict[str, ApprovalRecord] = {}
        self._approval_by_call: dict[str, str] = {}
        self._lock = RLock()

    @contextmanager
    def transaction(self):
        with self._lock:
            snapshot = {
                key: deepcopy(value) for key, value in vars(self).items() if isinstance(value, dict)
            }
            try:
                yield
            except BaseException:
                for key, value in snapshot.items():
                    setattr(self, key, value)
                raise

    def create_run(self, run: AgentRun) -> AgentRun:
        with self._lock:
            key = (run.tenant_id, run.request.request_id)
            existing_id = self._request_runs.get(key)
            if existing_id is not None:
                existing = self._runs[existing_id]
                if existing.request.to_dict() != run.request.to_dict():
                    raise AgentRunConflictError(
                        "request_id is already bound to a different agent request"
                    )
                return _clone_run(existing)
            if run.run_id in self._runs:
                raise AgentRunConflictError(f"run_id {run.run_id!r} already exists")
            stored = _clone_run(run)
            self._runs[stored.run_id] = stored
            self._request_runs[key] = stored.run_id
            return _clone_run(stored)

    def get_run(self, run_id: str) -> AgentRun | None:
        with self._lock:
            run = self._runs.get(run_id)
            return None if run is None else _clone_run(run)

    def get_run_by_request(self, tenant_id: str, request_id: str) -> AgentRun | None:
        with self._lock:
            run_id = self._request_runs.get((tenant_id, request_id))
            if run_id is None:
                return None
            return _clone_run(self._runs[run_id])

    def claim_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> AgentRun | None:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None or run.is_terminal:
                return None
            if run.status in {RunStatus.WAITING_APPROVAL, RunStatus.NEEDS_RECONCILIATION}:
                return None
            if run.status is RunStatus.RUNNING and not _lease_expired(run):
                return None
            now = datetime.now(UTC)
            claimed = replace(
                run,
                status=RunStatus.RUNNING,
                error_type=None,
                version=run.version + 1,
                updated_at=now.isoformat(),
                lease_owner=worker_id,
                lease_token=str(uuid4()),
                lease_expires_at=(now + timedelta(seconds=lease_seconds)).isoformat(),
            )
            self._runs[run_id] = claimed
            return _clone_run(claimed)

    def renew_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_token: str,
        lease_seconds: float,
    ) -> bool:
        with self._lock:
            run = self._runs.get(run_id)
            if (
                run is None
                or run.status is not RunStatus.RUNNING
                or run.lease_owner != worker_id
                or run.lease_token != lease_token
                or _lease_expired(run)
            ):
                return False
            now = datetime.now(UTC)
            self._runs[run_id] = replace(
                run,
                updated_at=now.isoformat(),
                lease_expires_at=(now + timedelta(seconds=lease_seconds)).isoformat(),
            )
            return True

    def update_run(
        self,
        run: AgentRun,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> AgentRun:
        with self._lock:
            current = self._runs.get(run.run_id)
            if current is None:
                raise AgentRunNotFoundError(f"run {run.run_id!r} was not found")
            if current.version != expected_version:
                raise AgentRunConflictError("agent run version changed concurrently")
            if current.request.to_dict() != run.request.to_dict():
                raise AgentRunConflictError("agent run request is immutable")
            if lease_token is not None:
                if current.lease_token != lease_token or _lease_expired(current):
                    raise AgentRunConflictError("agent run lease is no longer valid")
            stored = _next_run_version(run, current=current)
            self._runs[run.run_id] = stored
            return _clone_run(stored)

    def cancel_run(self, run_id: str, *, tenant_id: str) -> AgentRun:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise AgentRunNotFoundError(f"run {run_id!r} was not found")
            if run.tenant_id != tenant_id:
                raise AgentIdentityError("run does not belong to the requested tenant")
            if run.is_terminal:
                return _clone_run(run)
            cancelled = replace(
                run,
                status=RunStatus.CANCELLED,
                version=run.version + 1,
                error_type="AgentCancelledError",
                updated_at=utc_now_iso(),
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
            )
            self._runs[run_id] = cancelled
            return _clone_run(cancelled)

    def save_checkpoint(
        self,
        checkpoint: AgentCheckpoint,
        *,
        expected_version: int | None,
        lease_token: str | None = None,
    ) -> AgentCheckpoint:
        with self._lock:
            assert_execution_lease(self._runs.get(checkpoint.run_id), lease_token)
            if checkpoint.run_id not in self._runs:
                raise AgentRunNotFoundError(f"run {checkpoint.run_id!r} was not found")
            current = self._checkpoints.get(checkpoint.run_id)
            if current is None:
                if expected_version is not None:
                    raise AgentRunConflictError("agent checkpoint does not exist")
                version = 1
            else:
                if current.version != expected_version:
                    raise AgentRunConflictError("agent checkpoint version changed concurrently")
                version = current.version + 1
            stored = replace(checkpoint, version=version, updated_at=utc_now_iso())
            self._checkpoints[checkpoint.run_id] = _clone_checkpoint(stored)
            return _clone_checkpoint(stored)

    def get_checkpoint(self, run_id: str) -> AgentCheckpoint | None:
        with self._lock:
            checkpoint = self._checkpoints.get(run_id)
            return None if checkpoint is None else _clone_checkpoint(checkpoint)

    def save_turn(self, turn: AgentTurn, *, lease_token: str | None = None) -> AgentTurn:
        with self._lock:
            assert_execution_lease(self._runs.get(turn.run_id), lease_token)
            if turn.run_id not in self._runs:
                raise AgentRunNotFoundError(f"run {turn.run_id!r} was not found")
            for existing in self._turns.values():
                if existing.run_id == turn.run_id and existing.sequence == turn.sequence:
                    if existing.turn_id != turn.turn_id:
                        raise AgentRunConflictError("turn sequence is already in use")
            self._turns[turn.turn_id] = _clone_turn(turn)
            return _clone_turn(turn)

    def get_turn(self, turn_id: str) -> AgentTurn | None:
        with self._lock:
            turn = self._turns.get(turn_id)
            return None if turn is None else _clone_turn(turn)

    def list_turns(self, run_id: str) -> list[AgentTurn]:
        with self._lock:
            return [
                _clone_turn(turn)
                for turn in sorted(
                    (item for item in self._turns.values() if item.run_id == run_id),
                    key=lambda item: item.sequence,
                )
            ]

    def create_tool_call(
        self, record: ToolCallRecord, *, lease_token: str | None = None
    ) -> ToolCallRecord:
        with self._lock:
            assert_execution_lease(self._runs.get(record.run_id), lease_token)
            run = self._runs.get(record.run_id)
            if run is None:
                raise AgentRunNotFoundError(f"run {record.run_id!r} was not found")
            if run.tenant_id != record.tenant_id:
                raise AgentIdentityError("tool call tenant does not match its run")
            existing = self._tool_calls.get(record.call_id)
            if existing is not None:
                if _tool_identity(existing) != _tool_identity(record):
                    raise AgentRunConflictError("tool call_id is already bound to a different call")
                return _clone_tool_call(existing)
            self._tool_calls[record.call_id] = _clone_tool_call(record)
            return _clone_tool_call(record)

    def get_tool_call(self, call_id: str) -> ToolCallRecord | None:
        with self._lock:
            record = self._tool_calls.get(call_id)
            return None if record is None else _clone_tool_call(record)

    def update_tool_call(
        self,
        record: ToolCallRecord,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        with self._lock:
            assert_execution_lease(self._runs.get(record.run_id), lease_token)
            current = self._tool_calls.get(record.call_id)
            if current is None:
                raise AgentRunNotFoundError(f"tool call {record.call_id!r} was not found")
            if current.version != expected_version:
                raise AgentRunConflictError("tool call version changed concurrently")
            if _tool_identity(current) != _tool_identity(record):
                raise AgentRunConflictError("tool call identity is immutable")
            stored = replace(record, version=current.version + 1, updated_at=utc_now_iso())
            self._tool_calls[record.call_id] = _clone_tool_call(stored)
            return _clone_tool_call(stored)

    def list_tool_calls(self, run_id: str) -> list[ToolCallRecord]:
        with self._lock:
            return [
                _clone_tool_call(record)
                for record in sorted(
                    (item for item in self._tool_calls.values() if item.run_id == run_id),
                    key=lambda item: (item.created_at, item.call_id),
                )
            ]

    def create_approval(
        self, approval: ApprovalRecord, *, lease_token: str | None = None
    ) -> ApprovalRecord:
        with self._lock:
            assert_execution_lease(self._runs.get(approval.run_id), lease_token)
            call = self._tool_calls.get(approval.call_id)
            if call is None:
                raise AgentRunNotFoundError(f"tool call {approval.call_id!r} was not found")
            if call.run_id != approval.run_id or call.tenant_id != approval.tenant_id:
                raise AgentIdentityError("approval identity does not match its tool call")
            existing_id = self._approval_by_call.get(approval.call_id)
            if existing_id is not None:
                existing = self._approvals[existing_id]
                if existing.run_id != approval.run_id or existing.tenant_id != approval.tenant_id:
                    raise AgentRunConflictError("tool call approval identity changed")
                return _clone_approval(existing)
            self._approvals[approval.approval_id] = _clone_approval(approval)
            self._approval_by_call[approval.call_id] = approval.approval_id
            return _clone_approval(approval)

    def get_approval(self, approval_id: str) -> ApprovalRecord | None:
        with self._lock:
            approval = self._approvals.get(approval_id)
            return None if approval is None else _clone_approval(approval)

    def get_approval_for_call(self, call_id: str) -> ApprovalRecord | None:
        with self._lock:
            approval_id = self._approval_by_call.get(call_id)
            if approval_id is None:
                return None
            return _clone_approval(self._approvals[approval_id])

    def decide_approval(
        self,
        approval_id: str,
        *,
        tenant_id: str,
        decision: ApprovalStatus,
        reviewer_id: str,
        reason: str | None,
    ) -> ApprovalRecord:
        if decision not in {ApprovalStatus.APPROVED, ApprovalStatus.REJECTED}:
            raise AgentApprovalError("approval decision must be approved or rejected")
        with self._lock:
            approval = self._approvals.get(approval_id)
            if approval is None:
                raise AgentRunNotFoundError(f"approval {approval_id!r} was not found")
            if approval.tenant_id != tenant_id:
                raise AgentIdentityError("approval does not belong to the requested tenant")
            if approval.status is not ApprovalStatus.PENDING:
                if approval.status is decision and approval.reviewer_id == reviewer_id:
                    return _clone_approval(approval)
                raise AgentApprovalError("approval has already been decided")
            decided = replace(
                approval,
                status=decision,
                reviewer_id=reviewer_id,
                reason=reason,
                decided_at=utc_now_iso(),
            )
            self._approvals[approval_id] = decided
            return _clone_approval(decided)


def _next_run_version(run: AgentRun, *, current: AgentRun) -> AgentRun:
    active = run.status is RunStatus.RUNNING
    return replace(
        run,
        version=current.version + 1,
        updated_at=utc_now_iso(),
        lease_owner=current.lease_owner if active else None,
        lease_token=current.lease_token if active else None,
        lease_expires_at=current.lease_expires_at if active else None,
    )


def _lease_expired(run: AgentRun) -> bool:
    if not run.lease_expires_at:
        return True
    try:
        expires_at = datetime.fromisoformat(run.lease_expires_at)
    except ValueError:
        return True
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at <= datetime.now(UTC)


def _tool_identity(record: ToolCallRecord) -> tuple[object, ...]:
    return (
        record.call_id,
        record.run_id,
        record.tenant_id,
        record.tool_name,
        record.arguments,
        record.side_effects,
        record.idempotent,
    )


def _clone_run(value: AgentRun) -> AgentRun:
    return AgentRun.from_dict(value.to_dict())


def _clone_checkpoint(value: AgentCheckpoint) -> AgentCheckpoint:
    return AgentCheckpoint.from_dict(value.to_dict())


def _clone_turn(value: AgentTurn) -> AgentTurn:
    return AgentTurn.from_dict(value.to_dict())


def _clone_tool_call(value: ToolCallRecord) -> ToolCallRecord:
    return ToolCallRecord.from_dict(value.to_dict())


def _clone_approval(value: ApprovalRecord) -> ApprovalRecord:
    return ApprovalRecord.from_dict(value.to_dict())
