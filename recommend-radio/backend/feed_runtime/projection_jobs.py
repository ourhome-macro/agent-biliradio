"""Bounded SQL leasing for projection peers; never claims ordered or unrelated jobs."""

from __future__ import annotations

import hashlib
import json
import time
from uuid import uuid4

from database import get_connection
from job_errors import JobPermanentFailure

from .bitmap import user_bitmap_id
from .projection_sql import MAX_BATCH, validate


class ProjectionLeaseLost(RuntimeError):
    pass


def user_bitmap_ids(db_path, users):
    users = sorted(users)
    if not users or len(users) > MAX_BATCH:
        raise ValueError("Bounded projection users required")
    with get_connection(db_path) as conn:
        found = {
            r["user_id"]: int(r["bitmap_id"])
            for r in conn.execute(
                "SELECT user_id,bitmap_id FROM reaction_bitmap_users WHERE user_id IN ("
                + ",".join("?" for _ in users)
                + ")",
                tuple(users),
            )
        }
    for user in users:
        if user not in found:
            found[user] = user_bitmap_id(db_path, user, create=True)
    return found


def payload(job):
    value = json.loads(job["payload_json"])
    validate(value)
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    if (
        job["kind"] != "feed_projection"
        or job["user_id"] != value["user_id"]
        or hashlib.sha256(raw.encode()).hexdigest() != job["input_hash"]
        or job["projection_key"] not in {"", value["content_id"]}
    ):
        raise JobPermanentFailure("ProjectionJobAuthorityMismatch")
    return value


def fence(conn, jobs, cancelled=None):
    if cancelled is not None and cancelled.is_set():
        raise ProjectionLeaseLost("Projection assignment lost")
    ids = sorted(job["job_id"] for job in jobs)
    if getattr(conn, "dialect", None) == "mysql":
        import sqlalchemy as sa

        table = conn.tables["durable_jobs"]
        rows = [
            dict(row)
            for row in conn.connection.execute(
                sa.select(table)
                .where(table.c.job_id.in_(ids))
                .order_by(table.c.job_id)
                .with_for_update()
            ).mappings()
        ]
    else:
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM durable_jobs WHERE job_id IN (" + ",".join("?" for _ in ids) + ")",
                tuple(ids),
            )
        ]
    actual = {row["job_id"]: row for row in rows}
    now = time.time()
    for job in jobs:
        row = actual.get(job["job_id"])
        if (
            not row
            or row["status"] != "running"
            or row["lease_token"] != job["lease_token"]
            or row["lease_until"] <= now
            or row["input_hash"] != job["input_hash"]
        ):
            raise ProjectionLeaseLost("Projection SQL lease lost")


def renew_many(db_path, jobs, cancelled=None):
    # Acquire exactly the same ordered row set as projection/completion before
    # updating either token group. Never invert anchor/peer lock order.
    with get_connection(db_path) as conn:
        if getattr(conn, "dialect", None) != "mysql":
            conn.execute("BEGIN IMMEDIATE")
        ids = sorted(j["job_id"] for j in jobs)
        if getattr(conn, "dialect", None) == "mysql":
            import sqlalchemy as sa

            table = conn.tables["durable_jobs"]
            rows = [
                dict(r)
                for r in conn.connection.execute(
                    sa.select(table)
                    .where(table.c.job_id.in_(ids))
                    .order_by(table.c.job_id)
                    .with_for_update()
                ).mappings()
            ]
        else:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM durable_jobs WHERE job_id IN ("
                    + ",".join("?" for _ in ids)
                    + ")",
                    tuple(ids),
                )
            ]
        actual = {r["job_id"]: r for r in rows}
        live = []
        now = time.time()
        for job in jobs:
            row = actual.get(job["job_id"])
            if row and row["status"] in {"completed", "failed"}:
                continue  # A poison event may already have been parked by this batch.
            if (
                not row
                or row["status"] != "running"
                or row["lease_token"] != job["lease_token"]
                or row["lease_until"] <= now
            ):
                raise ProjectionLeaseLost("Projection renewal lost")
            live.append(job["job_id"])
        if cancelled is not None and cancelled.is_set():
            raise ProjectionLeaseLost("Projection assignment lost")
        if live:
            conn.execute(
                "UPDATE durable_jobs SET lease_until=?,updated_at=? WHERE job_id IN ("
                + ",".join("?" for _ in live)
                + ")",
                (now + 120, now, *live),
            )


def claim_peers(db_path, anchor, *, limit=MAX_BATCH, lease_seconds=120):
    if not 1 <= limit <= MAX_BATCH:
        raise ValueError("Invalid projection batch limit")
    identity = payload(anchor)["content_id"]
    now, token = time.time(), uuid4().hex
    if limit == 1 or anchor.get("lane"):
        return []
    with get_connection(db_path) as conn:
        if getattr(conn, "dialect", None) == "mysql":
            import sqlalchemy as sa

            table = conn.tables["durable_jobs"]
            rows = [
                dict(row)
                for row in conn.connection.execute(
                    sa.select(table)
                    .where(
                        table.c.kind == "feed_projection",
                        table.c.projection_key == identity,
                        table.c.status == "queued",
                        table.c.lane == "",
                        table.c.available_at <= now,
                        table.c.attempts < 8,
                    )
                    # Match the ready-queue index instead of sorting and locking
                    # an entire hot content's backlog before applying LIMIT.
                    .order_by(table.c.available_at, table.c.id)
                    .limit(limit - 1)
                    .with_for_update(skip_locked=True)
                ).mappings()
            ]
        else:
            conn.execute("BEGIN IMMEDIATE")
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM durable_jobs WHERE kind='feed_projection' "
                    "AND projection_key=? AND status='queued' AND lane='' AND available_at<=? "
                    "AND attempts<8 ORDER BY available_at,id LIMIT ?",
                    (identity, now, limit - 1),
                )
            ]
        if rows:
            ids = [r["job_id"] for r in rows]
            conn.execute(
                "UPDATE durable_jobs SET status='running',lease_token=?,lease_until=?,"
                "attempts=attempts+1,updated_at=? WHERE job_id IN ("
                + ",".join("?" for _ in ids)
                + ")",
                (token, now + lease_seconds, now, *ids),
            )
            for row in rows:
                row.update(
                    status="running",
                    lease_token=token,
                    lease_until=now + lease_seconds,
                    attempts=row["attempts"] + 1,
                )
    return rows


def finish_many(db_path, jobs, *, result=None, error=None, cancelled=None):
    if not jobs:
        return
    now = time.time()
    with get_connection(db_path) as conn:
        if getattr(conn, "dialect", None) != "mysql":
            conn.execute("BEGIN IMMEDIATE")
        fence(conn, jobs, cancelled)
        # Peer jobs share a batch token; the anchor retains its original token.
        for token in sorted({job["lease_token"] for job in jobs}):
            group = [job for job in jobs if job["lease_token"] == token]
            # Retry policy remains per job even after a batch failure.
            for status in ("completed", "queued", "failed"):
                selected = [
                    job
                    for job in group
                    if (
                        "completed"
                        if error is None
                        else "failed"
                        if isinstance(error, JobPermanentFailure) or job["attempts"] >= 8
                        else "queued"
                    )
                    == status
                ]
                if not selected:
                    continue
                ids = [job["job_id"] for job in selected]
                delay = min(2 ** min(max(j["attempts"] for j in selected), 8), 120) if error else 0
                conn.execute(
                    "UPDATE durable_jobs SET status=?,result_json=?,error=?,lease_token=NULL,"
                    "lease_until=0,next_publish_at=?,available_at=?,updated_at=? "
                    "WHERE lease_token=? "
                    "AND status='running' AND job_id IN (" + ",".join("?" for _ in ids) + ")",
                    (
                        status,
                        json.dumps(result, ensure_ascii=False),
                        type(error).__name__ if error else None,
                        now + delay,
                        now + delay,
                        now,
                        token,
                        *ids,
                    ),
                )
        if cancelled is not None and cancelled.is_set():
            raise ProjectionLeaseLost("Projection assignment lost before completion")


def release_peers(db_path, jobs, anchor_id):
    peers = [job for job in jobs if job["job_id"] != anchor_id]
    with get_connection(db_path) as conn:
        for token in {j["lease_token"] for j in peers}:
            ids = [j["job_id"] for j in peers if j["lease_token"] == token]
            conn.execute(
                "UPDATE durable_jobs SET status='queued',lease_token=NULL,lease_until=0,"
                "available_at=0,next_publish_at=0 WHERE lease_token=? AND status='running' "
                "AND job_id IN (" + ",".join("?" for _ in ids) + ")",
                (token, *ids),
            )
