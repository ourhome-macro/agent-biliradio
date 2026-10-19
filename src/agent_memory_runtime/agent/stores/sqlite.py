from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
from agent_memory_runtime.agent.stores.codec import JsonStateCodec, StateCodec
from agent_memory_runtime.agent.stores.leases import assert_execution_lease
from agent_memory_runtime.memory.stores.sqlite_manager import SQLiteTransactionManager


class SQLiteAgentStateStore:
    """Durable run state with optimistic updates and fenced execution leases."""

    def __init__(
        self,
        path_or_manager: str | Path | SQLiteTransactionManager,
        *,
        codec: StateCodec | None = None,
    ) -> None:
        self._manager = (
            path_or_manager
            if isinstance(path_or_manager, SQLiteTransactionManager)
            else SQLiteTransactionManager(path_or_manager)
        )
        self.path = self._manager.path
        self._codec = codec or JsonStateCodec()

    def transaction(self):
        return self._manager.transaction()

    def _assert_lease(self, connection, run_id, lease_token):
        if lease_token is not None:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            run = None if row is None else AgentRun.from_dict(self._codec.decode(row[0]))
            assert_execution_lease(run, lease_token)

    def create_run(self, run: AgentRun) -> AgentRun:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE tenant_id = ? AND request_id = ?",
                (run.tenant_id, run.request.request_id),
            ).fetchone()
            if row is not None:
                existing = AgentRun.from_dict(self._codec.decode(row[0]))
                if existing.request.to_dict() != run.request.to_dict():
                    raise AgentRunConflictError(
                        "request_id is already bound to a different agent request"
                    )
                return existing
            if connection.execute(
                "SELECT 1 FROM agent_runs WHERE run_id = ?", (run.run_id,)
            ).fetchone():
                raise AgentRunConflictError(f"run_id {run.run_id!r} already exists")
            connection.execute(
                """
                INSERT INTO agent_runs(run_id, tenant_id, request_id, status, version, payload)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.tenant_id,
                    run.request.request_id,
                    run.status.value,
                    run.version,
                    self._codec.encode(run.to_dict()),
                ),
            )
        return run

    def get_run(self, run_id: str) -> AgentRun | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else AgentRun.from_dict(self._codec.decode(row[0]))

    def get_run_by_request(self, tenant_id: str, request_id: str) -> AgentRun | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE tenant_id = ? AND request_id = ?",
                (tenant_id, request_id),
            ).fetchone()
        return None if row is None else AgentRun.from_dict(self._codec.decode(row[0]))

    def claim_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> AgentRun | None:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = AgentRun.from_dict(self._codec.decode(row[0]))
            if run.is_terminal:
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
            self._update_run_row(connection, claimed)
        return claimed

    def renew_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_token: str,
        lease_seconds: float,
    ) -> bool:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return False
            run = AgentRun.from_dict(self._codec.decode(row[0]))
            if (
                run.status is not RunStatus.RUNNING
                or run.lease_owner != worker_id
                or run.lease_token != lease_token
                or _lease_expired(run)
            ):
                return False
            now = datetime.now(UTC)
            renewed = replace(
                run,
                updated_at=now.isoformat(),
                lease_expires_at=(now + timedelta(seconds=lease_seconds)).isoformat(),
            )
            self._update_run_row(connection, renewed)
        return True

    def update_run(
        self,
        run: AgentRun,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> AgentRun:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id = ?", (run.run_id,)
            ).fetchone()
            if row is None:
                raise AgentRunNotFoundError(f"run {run.run_id!r} was not found")
            current = AgentRun.from_dict(self._codec.decode(row[0]))
            if current.version != expected_version:
                raise AgentRunConflictError("agent run version changed concurrently")
            if current.request.to_dict() != run.request.to_dict():
                raise AgentRunConflictError("agent run request is immutable")
            if lease_token is not None:
                if current.lease_token != lease_token or _lease_expired(current):
                    raise AgentRunConflictError("agent run lease is no longer valid")
            stored = _next_run_version(run, current=current)
            self._update_run_row(connection, stored)
        return stored

    def cancel_run(self, run_id: str, *, tenant_id: str) -> AgentRun:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise AgentRunNotFoundError(f"run {run_id!r} was not found")
            run = AgentRun.from_dict(self._codec.decode(row[0]))
            if run.tenant_id != tenant_id:
                raise AgentIdentityError("run does not belong to the requested tenant")
            if run.is_terminal:
                return run
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
            self._update_run_row(connection, cancelled)
        return cancelled

    def save_checkpoint(
        self,
        checkpoint: AgentCheckpoint,
        *,
        expected_version: int | None,
        lease_token: str | None = None,
    ) -> AgentCheckpoint:
        with self._manager.connection() as connection:
            self._assert_lease(connection, checkpoint.run_id, lease_token)
            if not connection.execute(
                "SELECT 1 FROM agent_runs WHERE run_id = ?", (checkpoint.run_id,)
            ).fetchone():
                raise AgentRunNotFoundError(f"run {checkpoint.run_id!r} was not found")
            row = connection.execute(
                "SELECT version FROM agent_checkpoints WHERE run_id = ?",
                (checkpoint.run_id,),
            ).fetchone()
            if row is None:
                if expected_version is not None:
                    raise AgentRunConflictError("agent checkpoint does not exist")
                version = 1
                stored = replace(checkpoint, version=version, updated_at=utc_now_iso())
                connection.execute(
                    "INSERT INTO agent_checkpoints(run_id, version, payload) VALUES (?, ?, ?)",
                    (
                        stored.run_id,
                        stored.version,
                        self._codec.encode(stored.to_dict()),
                    ),
                )
                return stored
            current_version = int(row[0])
            if current_version != expected_version:
                raise AgentRunConflictError("agent checkpoint version changed concurrently")
            stored = replace(
                checkpoint,
                version=current_version + 1,
                updated_at=utc_now_iso(),
            )
            connection.execute(
                "UPDATE agent_checkpoints SET version = ?, payload = ? WHERE run_id = ?",
                (
                    stored.version,
                    self._codec.encode(stored.to_dict()),
                    stored.run_id,
                ),
            )
        return stored

    def get_checkpoint(self, run_id: str) -> AgentCheckpoint | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_checkpoints WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else AgentCheckpoint.from_dict(self._codec.decode(row[0]))

    def save_turn(self, turn: AgentTurn, *, lease_token: str | None = None) -> AgentTurn:
        with self._manager.connection() as connection:
            self._assert_lease(connection, turn.run_id, lease_token)
            row = connection.execute(
                "SELECT turn_id FROM agent_turns WHERE run_id = ? AND sequence = ?",
                (turn.run_id, turn.sequence),
            ).fetchone()
            if row is not None and str(row[0]) != turn.turn_id:
                raise AgentRunConflictError("turn sequence is already in use")
            connection.execute(
                """
                INSERT INTO agent_turns(turn_id, run_id, sequence, status, payload)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(turn_id) DO UPDATE SET
                    status = excluded.status,
                    payload = excluded.payload
                """,
                (
                    turn.turn_id,
                    turn.run_id,
                    turn.sequence,
                    turn.status.value,
                    self._codec.encode(turn.to_dict()),
                ),
            )
        return turn

    def get_turn(self, turn_id: str) -> AgentTurn | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_turns WHERE turn_id = ?", (turn_id,)
            ).fetchone()
        return None if row is None else AgentTurn.from_dict(self._codec.decode(row[0]))

    def list_turns(self, run_id: str) -> list[AgentTurn]:
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM agent_turns WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
        return [AgentTurn.from_dict(self._codec.decode(row[0])) for row in rows]

    def create_tool_call(
        self, record: ToolCallRecord, *, lease_token: str | None = None
    ) -> ToolCallRecord:
        with self._manager.connection() as connection:
            self._assert_lease(connection, record.run_id, lease_token)
            run_row = connection.execute(
                "SELECT tenant_id FROM agent_runs WHERE run_id = ?", (record.run_id,)
            ).fetchone()
            if run_row is None:
                raise AgentRunNotFoundError(f"run {record.run_id!r} was not found")
            if str(run_row[0]) != record.tenant_id:
                raise AgentIdentityError("tool call tenant does not match its run")
            row = connection.execute(
                "SELECT payload FROM agent_tool_calls WHERE call_id = ?", (record.call_id,)
            ).fetchone()
            if row is not None:
                existing = ToolCallRecord.from_dict(self._codec.decode(row[0]))
                if _tool_identity(existing) != _tool_identity(record):
                    raise AgentRunConflictError("tool call_id is already bound to a different call")
                return existing
            connection.execute(
                """
                INSERT INTO agent_tool_calls(
                    call_id, run_id, tenant_id, status, version, payload
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.call_id,
                    record.run_id,
                    record.tenant_id,
                    record.status.value,
                    record.version,
                    self._codec.encode(record.to_dict()),
                ),
            )
        return record

    def get_tool_call(self, call_id: str) -> ToolCallRecord | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_tool_calls WHERE call_id = ?", (call_id,)
            ).fetchone()
        return None if row is None else ToolCallRecord.from_dict(self._codec.decode(row[0]))

    def update_tool_call(
        self,
        record: ToolCallRecord,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        with self._manager.connection() as connection:
            self._assert_lease(connection, record.run_id, lease_token)
            row = connection.execute(
                "SELECT payload FROM agent_tool_calls WHERE call_id = ?", (record.call_id,)
            ).fetchone()
            if row is None:
                raise AgentRunNotFoundError(f"tool call {record.call_id!r} was not found")
            current = ToolCallRecord.from_dict(self._codec.decode(row[0]))
            if current.version != expected_version:
                raise AgentRunConflictError("tool call version changed concurrently")
            if _tool_identity(current) != _tool_identity(record):
                raise AgentRunConflictError("tool call identity is immutable")
            stored = replace(record, version=current.version + 1, updated_at=utc_now_iso())
            connection.execute(
                """
                UPDATE agent_tool_calls
                SET status = ?, version = ?, payload = ?
                WHERE call_id = ? AND version = ?
                """,
                (
                    stored.status.value,
                    stored.version,
                    self._codec.encode(stored.to_dict()),
                    stored.call_id,
                    expected_version,
                ),
            )
        return stored

    def list_tool_calls(self, run_id: str) -> list[ToolCallRecord]:
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM agent_tool_calls WHERE run_id = ? ORDER BY rowid",
                (run_id,),
            ).fetchall()
        return [ToolCallRecord.from_dict(self._codec.decode(row[0])) for row in rows]

    def create_approval(
        self, approval: ApprovalRecord, *, lease_token: str | None = None
    ) -> ApprovalRecord:
        with self._manager.connection() as connection:
            self._assert_lease(connection, approval.run_id, lease_token)
            call_row = connection.execute(
                "SELECT run_id, tenant_id FROM agent_tool_calls WHERE call_id = ?",
                (approval.call_id,),
            ).fetchone()
            if call_row is None:
                raise AgentRunNotFoundError(f"tool call {approval.call_id!r} was not found")
            if str(call_row[0]) != approval.run_id or str(call_row[1]) != approval.tenant_id:
                raise AgentIdentityError("approval identity does not match its tool call")
            row = connection.execute(
                "SELECT payload FROM agent_approvals WHERE call_id = ?", (approval.call_id,)
            ).fetchone()
            if row is not None:
                existing = ApprovalRecord.from_dict(self._codec.decode(row[0]))
                if existing.run_id != approval.run_id or existing.tenant_id != approval.tenant_id:
                    raise AgentRunConflictError("tool call approval identity changed")
                return existing
            connection.execute(
                """
                INSERT INTO agent_approvals(
                    approval_id, run_id, call_id, tenant_id, status, payload
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    approval.approval_id,
                    approval.run_id,
                    approval.call_id,
                    approval.tenant_id,
                    approval.status.value,
                    self._codec.encode(approval.to_dict()),
                ),
            )
        return approval

    def get_approval(self, approval_id: str) -> ApprovalRecord | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
        return None if row is None else ApprovalRecord.from_dict(self._codec.decode(row[0]))

    def get_approval_for_call(self, call_id: str) -> ApprovalRecord | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_approvals WHERE call_id = ?", (call_id,)
            ).fetchone()
        return None if row is None else ApprovalRecord.from_dict(self._codec.decode(row[0]))

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
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM agent_approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
            if row is None:
                raise AgentRunNotFoundError(f"approval {approval_id!r} was not found")
            approval = ApprovalRecord.from_dict(self._codec.decode(row[0]))
            if approval.tenant_id != tenant_id:
                raise AgentIdentityError("approval does not belong to the requested tenant")
            if approval.status is not ApprovalStatus.PENDING:
                if approval.status is decision and approval.reviewer_id == reviewer_id:
                    return approval
                raise AgentApprovalError("approval has already been decided")
            decided = replace(
                approval,
                status=decision,
                reviewer_id=reviewer_id,
                reason=reason,
                decided_at=utc_now_iso(),
            )
            connection.execute(
                "UPDATE agent_approvals SET status = ?, payload = ? WHERE approval_id = ?",
                (
                    decided.status.value,
                    self._codec.encode(decided.to_dict()),
                    approval_id,
                ),
            )
        return decided

    def _update_run_row(self, connection: object, run: AgentRun) -> None:
        connection.execute(
            """
            UPDATE agent_runs
            SET status = ?, version = ?, payload = ?
            WHERE run_id = ?
            """,
            (
                run.status.value,
                run.version,
                self._codec.encode(run.to_dict()),
                run.run_id,
            ),
        )


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
