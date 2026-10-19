"""Resumable reconciliation across the SQL job and Agent state databases."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from database import DATA_DIR, begin_write, get_connection, mysql_target

from agent_memory_runtime.agent.models import RunStatus
from agent_memory_runtime.agent.recovery import reconcile_tool_record
from agent_memory_runtime.agent.stores.sqlite import SQLiteAgentStateStore


def agent_state_path(db_path):
    return (DATA_DIR if mysql_target(db_path) else Path(db_path).parent) / "agent-runs.sqlite3"


def resume_job(db_path, job_id, *, resolution_id, operator, reason):
    """Resume known Agent state after a worker died before the job commit."""
    from agent_memory_runtime.agent.models import ToolCallStatus

    if (
        not resolution_id.strip()
        or len(resolution_id) > 180
        or not operator.strip()
        or not reason.strip()
    ):
        raise ValueError("resolution id, operator and reason are required")
    path = agent_state_path(db_path)
    if not path.is_file():
        raise ValueError("Agent journal is unavailable")
    store = SQLiteAgentStateStore(path)
    evidence = json.dumps(
        dict(resolutionId=resolution_id, operator=operator, reason=reason), sort_keys=True
    )
    with get_connection(db_path) as conn:
        begin_write(conn, namespace="job", key=job_id)
        job = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not job or not job["agent_run_id"]:
            raise ValueError("job has no linked Agent run")
        previous = conn.execute(
            "SELECT reason FROM durable_job_resolutions WHERE job_id=? AND decision='agent_resume'",
            (job_id,),
        ).fetchall()
        for row in previous:
            if json.loads(row[0])["resolutionId"] == resolution_id:
                if row[0] != evidence:
                    raise ValueError("resolution ID is bound to different evidence")
                return {"jobId": job_id, "status": job["status"]}
        if job["status"] != "needs_reconciliation":
            raise ValueError("job is not awaiting reconciliation")
        run = store.get_run(job["agent_run_id"])
        if not run or run.request.user_id != job["user_id"]:
            raise ValueError("job and Agent identity do not match")
        calls = store.list_tool_calls(run.run_id)
        if any(
            call.status is ToolCallStatus.RECONCILIATION_REQUIRED
            or call.status is ToolCallStatus.EXECUTING
            and call.side_effects
            and not call.idempotent
            for call in calls
        ):
            raise ValueError("unknown side effects must be resolved with reconcile-tool first")
        if run.status is RunStatus.RUNNING:
            expires = (
                datetime.fromisoformat(run.lease_expires_at)
                if run.lease_expires_at
                else datetime.min.replace(tzinfo=UTC)
            )
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires > datetime.now(UTC):
                raise ValueError("Agent execution is still actively owned")
        elif run.status not in {RunStatus.PENDING, RunStatus.COMPLETED}:
            raise ValueError("Agent run cannot be resumed")
        if job["retry_safe"] and job["attempts"] >= 8:
            raise ValueError("job retry budget is exhausted")
        conn.execute(
            "INSERT INTO durable_job_resolutions(job_id,decision,reason,operator,created_at) "
            "VALUES (?,?,?,?,?)",
            (job_id, "agent_resume", evidence, operator, time.time()),
        )
        conn.execute(
            """UPDATE durable_jobs SET status='queued',lease_token=NULL,lease_until=0,
                     publish_token=NULL,publish_lease_until=0,next_publish_at=0,available_at=0,
                     error=NULL,updated_at=? WHERE job_id=?""",
            (time.time(), job_id),
        )
    return {"jobId": job_id, "status": "queued"}


def reconcile_job_tool(
    db_path,
    job_id,
    *,
    call_id,
    resolution_id,
    succeeded,
    output,
    operator,
    reason,
    state_store=None,
):
    if (
        not resolution_id.strip()
        or len(resolution_id) > 180
        or not operator.strip()
        or not reason.strip()
    ):
        raise ValueError("resolution id, operator and reconciliation reason are required")
    if not isinstance(output, dict):
        raise ValueError("verified output must be a JSON object")
    intent = dict(
        resolutionId=resolution_id,
        callId=call_id,
        succeeded=bool(succeeded),
        output=output,
        operator=operator,
        reason=reason,
    )
    raw = json.dumps(intent, sort_keys=True, ensure_ascii=False, allow_nan=False)
    if len(raw.encode()) > 1024 * 1024:
        raise ValueError("reconciliation evidence exceeds one MiB")
    if state_store is None:
        path = agent_state_path(db_path)
        if not path.is_file():
            raise ValueError("Agent journal is unavailable; cannot resolve only the job")
        state_store = SQLiteAgentStateStore(path)
    # Persist intent before touching the second database. Repeating this command
    # with the same resolution ID safely completes any interrupted boundary.
    with get_connection(db_path) as conn:
        begin_write(conn, namespace="job", key=job_id)
        job = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not job or not job["agent_run_id"]:
            raise ValueError("job has no linked Agent run")
        records = conn.execute(
            "SELECT * FROM durable_job_resolutions WHERE job_id=? AND decision IN (?,?)",
            (job_id, "agent_tool_pending", "agent_tool_applied"),
        ).fetchall()
        prior = next(
            (row for row in records if json.loads(row["reason"])["resolutionId"] == resolution_id),
            None,
        )
        if prior and prior["reason"] != raw:
            raise ValueError("resolution ID is bound to different evidence")
        if prior and prior["decision"] == "agent_tool_applied":
            return {"jobId": job_id, "status": job["status"], "resolutionId": resolution_id}
        if job["status"] not in {"needs_reconciliation", "failed"}:
            raise ValueError("job is not awaiting reconciliation")
        run = state_store.get_run(job["agent_run_id"])
        call = state_store.get_tool_call(call_id)
        if (
            not run
            or not call
            or call.run_id != run.run_id
            or run.request.user_id != job["user_id"]
            or call.tenant_id != run.tenant_id
        ):
            raise ValueError("tool, job and Agent identity do not match")
        if succeeded and not run.is_terminal and job["retry_safe"] and job["attempts"] >= 8:
            raise ValueError("job retry budget is exhausted")
        if prior is None:
            conn.execute(
                "INSERT INTO durable_job_resolutions(job_id,decision,reason,operator,created_at) "
                "VALUES (?,?,?,?,?)",
                (job_id, "agent_tool_pending", raw, operator, time.time()),
            )
    reconcile_tool_record(
        state_store,
        call_id,
        tenant_id=run.tenant_id,
        reviewer_id=operator,
        succeeded=succeeded,
        output=output,
        reason=reason,
        resolution_id=resolution_id,
        fail_run_on_failure=True,
    )
    with get_connection(db_path) as conn:
        begin_write(conn, namespace="job", key=job_id)
        current = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
        if (
            current["status"] not in {"needs_reconciliation", "failed"}
            or current["agent_run_id"] != run.run_id
        ):
            # Another identical invocation may have completed this same intent.
            done = conn.execute(
                "SELECT 1 FROM durable_job_resolutions WHERE job_id=? AND decision=? AND reason=?",
                (job_id, "agent_tool_applied", raw),
            ).fetchone()
            if not done:
                raise ValueError("job changed while reconciling; repeat the same resolution")
        else:
            resolved_run = state_store.get_run(run.run_id)
            statuses = {
                RunStatus.PENDING: "queued",
                RunStatus.NEEDS_RECONCILIATION: "needs_reconciliation",
                RunStatus.FAILED: "failed",
                RunStatus.CANCELLED: "failed",
            }
            if resolved_run.status not in statuses:
                raise ValueError("Agent changed while reconciling; job remains blocked")
            status = statuses[resolved_run.status]
            conn.execute(
                """UPDATE durable_jobs SET status=?,lease_token=NULL,lease_until=0,
                publish_token=NULL,publish_lease_until=0,next_publish_at=0,available_at=0,
                error=?,updated_at=? WHERE job_id=?""",
                (status, "ReconciledFailure" if status == "failed" else None, time.time(), job_id),
            )
            conn.execute(
                "UPDATE durable_job_resolutions SET decision=? "
                "WHERE job_id=? AND decision=? AND reason=?",
                ("agent_tool_applied", job_id, "agent_tool_pending", raw),
            )
    return {
        "jobId": job_id,
        "status": status
        if current["status"] in {"needs_reconciliation", "failed"}
        else current["status"],
        "resolutionId": resolution_id,
    }
