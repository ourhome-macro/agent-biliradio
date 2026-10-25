"""Transactional job outbox. Application state stays in SQLite, transport in Celery."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from database import begin_write


def ensure_schema(conn) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS durable_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL, user_id TEXT NOT NULL,
            lane TEXT NOT NULL DEFAULT '', payload_json TEXT NOT NULL,
            input_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
            retry_safe INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
            next_publish_at REAL NOT NULL DEFAULT 0, lease_until REAL NOT NULL DEFAULT 0,
            available_at REAL NOT NULL DEFAULT 0,
            lease_token TEXT, result_json TEXT, error TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_durable_jobs_pending
            ON durable_jobs(status, next_publish_at);
        CREATE INDEX IF NOT EXISTS idx_durable_jobs_lane ON durable_jobs(lane, status);
        CREATE INDEX IF NOT EXISTS idx_durable_jobs_user ON durable_jobs(user_id, status);
        CREATE TABLE IF NOT EXISTS durable_job_resolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
            decision TEXT NOT NULL, reason TEXT NOT NULL, operator TEXT NOT NULL,
            created_at REAL NOT NULL
        );
    """)


def enqueue(
    conn,
    *,
    kind: str,
    user_id: str,
    payload: dict,
    job_id: str | None = None,
    lane: str = "",
    retry_safe: bool = False,
    limit: int = 1000,
) -> str:
    # Acquire the writer before duplicate/capacity checks, including when the
    # caller has not written any business data yet.
    begin_write(conn, namespace="job-capacity", key=user_id)
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    projection_key = ''
    if kind=='feed_projection':
        projection_key = payload.get('content_id')
        if not isinstance(projection_key,str) or not projection_key or len(projection_key)>180:
            raise ValueError('Projection content identity is required')
    digest = hashlib.sha256(raw.encode()).hexdigest()
    job_id = job_id or f"{kind}:{uuid4().hex}"
    begin_write(conn, namespace="job", key=job_id)
    old = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
    if old:
        if old["kind"] != kind or old["user_id"] != user_id or old["input_hash"] != digest:
            raise ValueError("idempotency key is already bound to a different input")
        return job_id
    count = conn.execute(
        "SELECT COUNT(*) FROM durable_jobs WHERE user_id=? AND status IN ('queued','running')",
        (user_id,),
    ).fetchone()[0]
    if count >= limit:
        raise ValueError("background task capacity exceeded; retry later")
    now = time.time()
    from agent_memory_runtime.telemetry import carrier

    conn.execute(
        """INSERT INTO durable_jobs
        (job_id,kind,user_id,lane,payload_json,input_hash,retry_safe,created_at,updated_at,trace_context,projection_key)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            job_id,
            kind,
            user_id,
            lane,
            raw,
            digest,
            int(retry_safe),
            now,
            now,
            json.dumps(carrier()),
            projection_key,
        ),
    )
    return job_id


def enqueue_behavior(
    conn,
    *,
    user_id: str,
    event: str,
    scene: str,
    track: Any = None,
    payload: dict | None = None,
    event_id: str | None = None,
) -> str | None:
    from job_transport import async_jobs_enabled

    if not async_jobs_enabled():
        return None
    values = dict(payload or {})
    values.update(userId=user_id, event=event, scene=scene)
    if track is not None:
        values["track"] = track.to_dict() if hasattr(track, "to_dict") else dict(track)
    supplied_id = event_id or values.get("event_id") or values.get("eventId")
    identity = json.dumps([user_id, event, scene, str(supplied_id)], ensure_ascii=False)
    stable_id = (
        "behavior:" + hashlib.sha256(identity.encode()).hexdigest()
        if supplied_id
        else "behavior:" + uuid4().hex
    )
    old = conn.execute(
        "SELECT payload_json FROM durable_jobs WHERE job_id=?", (stable_id,)
    ).fetchone()
    previous = json.loads(old["payload_json"]) if old else {}
    values.update(
        event_id=stable_id,
        eventId=stable_id,
        occurred_at=previous.get("occurred_at") or datetime.now(UTC).isoformat(),
    )
    return enqueue(
        conn, kind="behavior", user_id=user_id, payload=values, job_id=stable_id, retry_safe=True
    )


def claim(conn, job_id: str, *, now: float | None = None, lease_seconds: int = 330):
    now = time.time() if now is None else now
    begin_write(conn, namespace="job", key=job_id)
    row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
    if row and row['kind']=='feed_projection' and getattr(conn,'dialect',None)=='mysql':
        # Batch consumers also lease projection rows. Re-read under that same
        # row lock before deciding to claim; a plain SELECT can precede their commit.
        row = conn.lock_row('durable_jobs',{'job_id':job_id})
    if row is None or row["status"] in ("completed", "failed", "needs_reconciliation"):
        return None
    if row["status"] == "queued" and row["available_at"] > now:
        return None
    if row["retry_safe"] and row["attempts"] >= 8:
        conn.execute(
            "UPDATE durable_jobs SET status='failed',error='AttemptsExhausted' WHERE job_id=?",
            (job_id,),
        )
        return None
    if row["status"] == "running":
        if row["lease_until"] > now:
            return None
        if not row["retry_safe"]:
            conn.execute(
                """UPDATE durable_jobs SET status='needs_reconciliation',
                error='WorkerLost: partial side effects require reconciliation',updated_at=?
                WHERE job_id=?""",
                (now, job_id),
            )
            return None
    if row["lane"]:
        begin_write(conn, namespace="job-lane", key=row["lane"])
    if (
        row["lane"]
        and conn.execute(
            """SELECT 1 FROM durable_jobs
        WHERE lane=? AND job_id<>? AND (status IN ('running','needs_reconciliation')
        OR (status='queued' AND id < ?))
        LIMIT 1""",
            (row["lane"], job_id, row["id"]),
        ).fetchone()
    ):
        return None
    token = uuid4().hex
    conn.execute(
        """UPDATE durable_jobs SET status='running',lease_token=?,lease_until=?,
        attempts=attempts+1,updated_at=? WHERE job_id=?""",
        (token, now + lease_seconds, now, job_id),
    )
    return dict(conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone())


def finish(
    conn, job: dict, *, result: Any = None, error: Exception | None = None, now: float | None = None
) -> None:
    now = time.time() if now is None else now
    status = "completed"
    if error:
        status = (
            "queued"
            if job["retry_safe"] and job["attempts"] < 8
            else "failed"
            if job["retry_safe"]
            else "needs_reconciliation"
        )
        from job_errors import JobPermanentFailure, JobReconciliationRequired

        if isinstance(error, JobReconciliationRequired):
            status = "needs_reconciliation"
        elif isinstance(error, JobPermanentFailure):
            status = "failed"
    conn.execute(
        """UPDATE durable_jobs SET status=?,result_json=?,error=?,lease_token=NULL,
        lease_until=0,next_publish_at=?,available_at=?,updated_at=?
        WHERE job_id=? AND lease_token=? AND status='running' AND lease_until>?""",
        (
            status,
            json.dumps(result, ensure_ascii=False),
            type(error).__name__ if error else None,
            now + min(2 ** min(job["attempts"], 8), 120),
            now + min(2 ** min(job["attempts"], 8), 120),
            now,
            job["job_id"],
            job["lease_token"],
            now,
        ),
    )


def recover_expired(conn, *, now: float | None = None) -> None:
    now = time.time() if now is None else now
    conn.execute(
        """UPDATE durable_jobs SET
        status=CASE WHEN retry_safe=1 AND attempts<8 THEN 'queued'
                    WHEN retry_safe=1 THEN 'failed' ELSE 'needs_reconciliation' END,
        error='WorkerLost',lease_token=NULL,lease_until=0,updated_at=?
        WHERE status='running' AND lease_until < ?""",
        (now, now),
    )


def claim_publish_batch(db_path, *, now: float | None = None, limit=32, lease_seconds=30):
    from database import get_connection

    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        recover_expired(conn, now=now)
        if getattr(conn, "dialect", None) == "mysql":
            import sqlalchemy as sa

            table = conn.tables["durable_jobs"]
            predecessor = table.alias("predecessor")
            blocked = sa.exists(
                sa.select(1).where(
                    predecessor.c.lane == table.c.lane,
                    predecessor.c.job_id != table.c.job_id,
                    sa.or_(
                        predecessor.c.status.in_(("running", "needs_reconciliation")),
                        sa.and_(predecessor.c.status == "queued", predecessor.c.id < table.c.id),
                    ),
                )
            )
            statement = (
                sa.select(table)
                .where(
                    table.c.status == "queued",
                    table.c.next_publish_at <= now,
                    table.c.publish_lease_until <= now,
                    sa.or_(table.c.lane == "", ~blocked),
                )
                .order_by(table.c.next_publish_at, table.c.id)
                .with_hint(table, "FORCE INDEX (idx_durable_jobs_pending)", dialect_name="mysql")
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = [dict(row) for row in conn.connection.execute(statement).mappings()]
        else:
            if not conn.in_transaction:
                conn.execute("BEGIN IMMEDIATE")
            rows = [
                dict(row)
                for row in conn.execute(
                    """SELECT j.* FROM durable_jobs AS j
            WHERE j.status='queued' AND j.next_publish_at<=? AND j.publish_lease_until<=?
              AND (j.lane='' OR NOT EXISTS (
                SELECT 1 FROM durable_jobs AS predecessor
                WHERE predecessor.lane=j.lane AND predecessor.job_id<>j.job_id
                  AND (predecessor.status IN ('running','needs_reconciliation')
                       OR (predecessor.status='queued' AND predecessor.id<j.id))
              ))
            ORDER BY j.created_at,j.id LIMIT ?""",
                    (now, now, limit),
                )
            ]
        # This publication lease is independent of the task execution lease.
        for row in rows:
            token = uuid4().hex
            conn.execute(
                """UPDATE durable_jobs SET next_publish_at=?,publish_token=?,publish_lease_until=?,
                publish_attempts=publish_attempts+1 WHERE job_id=?""",
                (now + lease_seconds, token, now + lease_seconds, row["job_id"]),
            )
            row.update(publish_token=token, publish_lease_until=now + lease_seconds)
    return rows


def complete_publication(db_path, row, *, now=None, error=None):
    from database import get_connection

    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        if error:
            # A callback failure can mean the broker accepted the record. Keep
            # the finite lease so recovery is delayed but never lost.
            return conn.execute(
                """UPDATE durable_jobs SET last_publish_error=?
                WHERE job_id=? AND publish_token=? AND publish_lease_until>?""",
                (type(error).__name__, row["job_id"], row["publish_token"], now),
            ).rowcount
        return conn.execute(
            """UPDATE durable_jobs SET published_at=?,publish_token=NULL,
            publish_lease_until=0,last_publish_error=NULL
            WHERE job_id=? AND publish_token=? AND publish_lease_until>?""",
            (now, row["job_id"], row["publish_token"], now),
        ).rowcount


def renew_publication(db_path, row, *, now=None, lease_seconds=30):
    from database import get_connection

    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        return bool(
            conn.execute(
                """UPDATE durable_jobs SET publish_lease_until=?
            WHERE job_id=? AND publish_token=? AND publish_lease_until>?""",
                (now + lease_seconds, row["job_id"], row["publish_token"], now),
            ).rowcount
        )


def publish_pending(db_path, publisher, *, now: float | None = None) -> int:
    rows = claim_publish_batch(db_path, now=now)
    failure = None
    sent = 0
    for row in rows:
        if not renew_publication(db_path, row, now=now):
            continue
        try:
            publisher(row["job_id"], row["kind"])
            complete_publication(db_path, row, now=now)
            sent += 1
        except Exception as error:
            complete_publication(db_path, row, now=now, error=error)
            failure = failure or error
    if failure:
        raise failure
    # Until a worker claims the row, periodic redelivery also recovers broker loss.
    return sent
