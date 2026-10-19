"""Audited, idempotent outcome reconciliation in one state-store transaction."""

from dataclasses import replace
from datetime import UTC, datetime

from agent_memory_runtime.agent.errors import (
    AgentIdentityError,
    AgentRunConflictError,
    AgentRunNotFoundError,
)
from agent_memory_runtime.agent.models import RunStatus, ToolCallStatus, utc_now_iso
from agent_memory_runtime.audit.hashing import secure_hash


def reconcile_tool_record(
    store,
    call_id,
    *,
    tenant_id,
    reviewer_id,
    succeeded,
    output=None,
    reason="Outcome verified",
    resolution_id=None,
    fail_run_on_failure=False,
):
    if not reviewer_id.strip() or not reason.strip():
        raise ValueError("reconciliation requires an operator and reason")
    values = dict(output or {}) if succeeded else {}
    evidence = {
        "reviewer_id": reviewer_id,
        "reason": reason,
        "succeeded": succeeded,
        "output_hash": secure_hash(values),
        "fail_run_on_failure": fail_run_on_failure,
    }
    resolution_id = resolution_id or secure_hash({"call_id": call_id, **evidence})
    with store.transaction():
        record = store.get_tool_call(call_id)
        if record is None:
            raise AgentRunNotFoundError(f"tool call {call_id!r} was not found")
        if record.tenant_id != tenant_id:
            raise AgentIdentityError("tool call does not belong to the requested tenant")
        if record.reconciliation:
            saved = record.reconciliation
            if saved.get("resolution_id") == resolution_id and all(
                saved.get(k) == v for k, v in evidence.items()
            ):
                return record
            raise AgentRunConflictError("tool outcome is already bound to another resolution")
        run = store.get_run(record.run_id)
        if run.status is RunStatus.RUNNING:
            expires = (
                datetime.fromisoformat(run.lease_expires_at)
                if run.lease_expires_at
                else datetime.min.replace(tzinfo=UTC)
            )
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires > datetime.now(UTC):
                raise AgentRunConflictError("cannot reconcile an actively owned execution")
        elif run.status not in {
            RunStatus.NEEDS_RECONCILIATION,
            RunStatus.CANCELLED,
            RunStatus.FAILED,
        }:
            raise AgentRunConflictError("run is not awaiting outcome reconciliation")
        if record.status not in {ToolCallStatus.RECONCILIATION_REQUIRED, ToolCallStatus.EXECUTING}:
            raise AgentRunConflictError("tool call has no unknown execution outcome")
        updated = store.update_tool_call(
            replace(
                record,
                status=ToolCallStatus.SUCCEEDED if succeeded else ToolCallStatus.FAILED,
                output=values,
                error_type=None if succeeded else "ReconciledFailure",
                error_hash=None if succeeded else secure_hash(evidence),
                reconciliation={
                    "resolution_id": resolution_id,
                    "resolved_at": utc_now_iso(),
                    **evidence,
                },
            ),
            expected_version=record.version,
        )
        unresolved = any(
            call.status is ToolCallStatus.RECONCILIATION_REQUIRED
            or (
                call.status is ToolCallStatus.EXECUTING
                and call.side_effects
                and not call.idempotent
            )
            for call in store.list_tool_calls(run.run_id)
        )
        status = (
            run.status
            if run.is_terminal
            else RunStatus.FAILED
            if fail_run_on_failure and not succeeded
            else RunStatus.NEEDS_RECONCILIATION
            if unresolved
            else RunStatus.PENDING
        )
        store.update_run(
            replace(
                run,
                status=status,
                error_type=(
                    run.error_type
                    if run.is_terminal
                    else "ReconciledFailure"
                    if status is RunStatus.FAILED
                    else None
                ),
            ),
            expected_version=run.version,
        )
        return updated
