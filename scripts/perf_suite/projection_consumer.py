"""Isolated SQL consumer process; production handler without Kafka transport cost."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from database import get_connection
from durable_jobs import claim, finish
from feed_runtime.cache import FeedCache
from feed_runtime.projection_batch import run_job
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository


def main():
    config = json.loads(os.environ["PERF_CONFIG"])
    db = config["db"]
    repo = FeedRepository(db)
    cache = FeedCache(config["redis"], prefix=config["prefix"])
    service = ReactionService(repo, cache)
    with get_connection(db) as conn:
        conn.execute("SELECT 1")
    Path(config["workerReady"]).write_text("ready", encoding="ascii")
    while True:
        time.sleep(0.01)
        with get_connection(db) as conn:
            row = conn.execute(
                "SELECT job_id FROM durable_jobs WHERE kind='feed_projection' "
                "AND status='queued' AND available_at<=? ORDER BY id LIMIT 1",
                (time.time(),),
            ).fetchone()
            job = claim(conn, row[0]) if row else None
        if job:
            try:
                if config.get("scalarProjection"):
                    result = service.project(json.loads(job["payload_json"]))
                    with get_connection(db) as conn:
                        finish(conn, job, result=result)
                else:
                    run_job(service, job)
            except Exception as error:
                print("Projection consumer retry:", type(error).__name__, flush=True)
                with get_connection(db) as conn:
                    finish(conn, job, error=error)


if __name__ == "__main__":
    main()
