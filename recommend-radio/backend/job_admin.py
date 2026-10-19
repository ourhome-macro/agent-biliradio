"""Local operational inspection and audited resolution of interrupted jobs."""

import getpass
import json
import time

import click
from database import DEFAULT_DB_PATH, begin_write, get_connection, init_db


@click.group()
def cli():
    init_db()


@cli.command()
def status():
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT kind,status,COUNT(*) AS count FROM durable_jobs GROUP BY kind,status"
        ).fetchall()
        blocked = conn.execute(
            "SELECT job_id,kind,user_id,error FROM durable_jobs "
            "WHERE status IN ('failed','needs_reconciliation') "
            "ORDER BY updated_at DESC LIMIT 100"
        ).fetchall()
    click.echo(
        json.dumps(
            {"counts": [dict(row) for row in rows], "attention": [dict(row) for row in blocked]},
            ensure_ascii=False,
        )
    )


@cli.command("inspect-job")
@click.argument("job_id")
def inspect_job(job_id):
    """Inspect unknown calls without printing private tool arguments or outputs."""
    from agent_job_recovery import agent_state_path

    from agent_memory_runtime.agent.stores.sqlite import SQLiteAgentStateStore

    with get_connection() as conn:
        row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        raise click.ClickException("Job not found")
    result = {"jobId": job_id, "status": row["status"], "agentRunId": row["agent_run_id"]}
    if row["agent_run_id"]:
        path = agent_state_path(DEFAULT_DB_PATH)
        if not path.is_file():
            raise click.ClickException("Linked Agent journal is unavailable")
        store = SQLiteAgentStateStore(path)
        run = store.get_run(row["agent_run_id"])
        result["runStatus"] = run.status.value if run else "missing"
        result["calls"] = [
            {
                "callId": call.call_id,
                "tool": call.tool_name,
                "status": call.status.value,
                "attempts": call.attempts,
                "resolutionId": call.reconciliation.get("resolution_id"),
            }
            for call in store.list_tool_calls(row["agent_run_id"])
        ]
    click.echo(json.dumps(result, ensure_ascii=False))


@cli.command()
@click.argument("job_id")
@click.option("--decision", type=click.Choice(["completed", "failed"]), required=True)
@click.option("--reason", required=True)
def resolve(job_id, decision, reason):
    """Resolve an inspected unknown outcome; never blindly replay side effects."""
    if not reason.strip():
        raise click.ClickException("A reconciliation reason is required")
    with get_connection() as conn:
        begin_write(conn, namespace="job", key=job_id)
        row = conn.execute(
            "SELECT agent_run_id FROM durable_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row and row["agent_run_id"]:
            raise click.ClickException(
                "Linked Agent jobs require reconcile-tool or resume-job; "
                "resolving only the job would orphan tool state"
            )
        changed = conn.execute(
            "UPDATE durable_jobs SET status=?,updated_at=? "
            "WHERE job_id=? AND status='needs_reconciliation'",
            (decision, time.time(), job_id),
        )
        if not changed.rowcount:
            raise click.ClickException("Job is not awaiting reconciliation")
        conn.execute(
            "INSERT INTO durable_job_resolutions "
            "(job_id,decision,reason,operator,created_at) VALUES (?,?,?,?,?)",
            (job_id, decision, reason.strip(), getpass.getuser(), time.time()),
        )
    click.echo(f"{job_id}: {decision}")


@cli.command("reconcile-tool")
@click.argument("job_id")
@click.option("--call-id", required=True)
@click.option(
    "--resolution-id", required=True, help="Stable command ID; reuse it after interruption"
)
@click.option("--outcome", type=click.Choice(["succeeded", "failed"]), required=True)
@click.option("--output-file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--reason", required=True)
def reconcile_tool(job_id, call_id, resolution_id, outcome, output_file, reason):
    """Record verified tool output, then resume the job or fail both layers."""
    from pathlib import Path

    from agent_job_recovery import reconcile_job_tool

    try:
        output = json.loads(Path(output_file).read_text(encoding="utf-8-sig"))
        result = reconcile_job_tool(
            DEFAULT_DB_PATH,
            job_id,
            call_id=call_id,
            resolution_id=resolution_id,
            succeeded=outcome == "succeeded",
            output=output,
            operator=getpass.getuser(),
            reason=reason,
        )
    except Exception as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(result, ensure_ascii=False))


@cli.command("resume-job")
@click.argument("job_id")
@click.option("--resolution-id", required=True)
@click.option("--reason", required=True)
def resume_job(job_id, resolution_id, reason):
    """Resume a known result/checkpoint; refuses unknown non-idempotent effects."""
    from agent_job_recovery import resume_job as resume

    try:
        result = resume(
            DEFAULT_DB_PATH,
            job_id,
            resolution_id=resolution_id,
            operator=getpass.getuser(),
            reason=reason,
        )
    except Exception as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    cli()
