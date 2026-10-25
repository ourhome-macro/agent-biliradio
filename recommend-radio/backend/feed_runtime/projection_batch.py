"""Production projection batching used by every task transport."""

from __future__ import annotations

import threading
import time

from job_errors import JobPermanentFailure

from .metrics import count
from .projection_jobs import (
    ProjectionLeaseLost,
    claim_peers,
    fence,
    finish_many,
    payload,
    release_peers,
    renew_many,
)
from .projection_sql import MAX_BATCH, ProjectionConflict


def run_job(service, anchor, *, limit=MAX_BATCH, cancelled=None):
    payload(anchor)
    # A bounded micro-batch window collects concurrent writes without delaying
    # HTTP acknowledgements or changing the source relationship transaction.
    if cancelled is not None:
        if cancelled.wait(0.1):
            raise ProjectionLeaseLost("Projection assignment lost")
    else:
        time.sleep(0.1)
    jobs = [anchor, *claim_peers(service.repo.db_path, anchor, limit=limit)]
    stop, lost = threading.Event(), threading.Event()
    leased_jobs = tuple(jobs)

    def guard(conn):
        if lost.is_set():
            raise ProjectionLeaseLost("Projection renewal lost")
        fence(conn, jobs, cancelled)

    def renew():
        while not stop.wait(5):
            try:
                if cancelled is not None and cancelled.is_set():
                    lost.set()
                    return
                renew_many(service.repo.db_path, leased_jobs, cancelled)
            except Exception:
                lost.set()

    pulse = threading.Thread(target=renew, daemon=True, name="projection-batch-lease")
    pulse.start()
    try:
        # Corrupt peers are parked individually; they cannot poison unrelated users.
        values, valid_jobs = [], []
        invalid_jobs = []
        for job in jobs:
            try:
                values.append(payload(job))
                valid_jobs.append(job)
            except (ValueError, KeyError, TypeError, JobPermanentFailure):
                invalid_jobs.append(job)
        if invalid_jobs:
            finish_many(
                service.repo.db_path,
                invalid_jobs,
                error=JobPermanentFailure("InvalidProjection"),
                cancelled=cancelled,
            )
        jobs[:] = valid_jobs
        while jobs:
            try:
                result = service.project_many(values, guard=guard)
                break
            except ProjectionConflict as conflict:
                bad = [
                    job
                    for job, value in zip(jobs, values, strict=True)
                    if value["event_id"] in conflict.event_ids
                ]
                if not bad:
                    raise
                finish_many(
                    service.repo.db_path,
                    bad,
                    error=JobPermanentFailure("ProjectionConflict"),
                    cancelled=cancelled,
                )
                remaining = [
                    (job, value) for job, value in zip(jobs, values, strict=True) if job not in bad
                ]
                jobs[:] = [j for j, _v in remaining]
                values = [v for _j, v in remaining]
        else:
            return {"events": 0, "parked": True}
        stop.set()
        pulse.join(timeout=2)
        finish_many(
            service.repo.db_path,
            jobs,
            result={
                key: result[key] for key in ("events", "appliedPairs", "coalesced", "newReceipts")
            },
            cancelled=cancelled,
        )
        count("projection_batch", "jobs", len(jobs))
        count("projection_batch", "batches")
        return {key: result[key] for key in ("events", "appliedPairs", "coalesced", "newReceipts")}
    except Exception as error:
        # Never mark a new owner's job complete/failed, or finish an old assignment.
        if not isinstance(error, ProjectionLeaseLost) and not (cancelled and cancelled.is_set()):
            try:
                finish_many(service.repo.db_path, jobs, error=error, cancelled=cancelled)
            except ProjectionLeaseLost:
                release_peers(service.repo.db_path, jobs, anchor["job_id"])
        else:
            release_peers(service.repo.db_path, jobs, anchor["job_id"])
        raise
    finally:
        stop.set()
        pulse.join(timeout=2)


def execute(db_path, job, *, cancelled=None, limit=MAX_BATCH):
    from pathlib import Path

    from database import DATA_DIR, mysql_target

    from .config import FeedConfig
    from .services import FeedServices

    config = FeedConfig.from_env(
        data_dir=DATA_DIR if mysql_target(db_path) else Path(db_path).parent
    )
    runtime = FeedServices(user_id=job["user_id"], db_path=db_path, config=config)
    return run_job(runtime.reactions, job, cancelled=cancelled, limit=limit)
