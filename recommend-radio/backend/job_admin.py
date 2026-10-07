"""Local operational inspection and audited resolution of interrupted jobs."""

import getpass
import json
import time

import click
from database import begin_write, get_connection, init_db


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


if __name__ == "__main__":
    cli()
