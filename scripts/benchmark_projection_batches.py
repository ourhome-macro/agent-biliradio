"""Fixed submitted-event comparison: scalar vs production batch/coalescing consumer."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def experiment(db, redis_url, seed, batched):
    from database import get_connection
    from durable_jobs import claim, finish
    from feed_runtime.cache import FeedCache
    from feed_runtime.projection_batch import run_job
    from feed_runtime.reactions import ReactionService
    from feed_runtime.repository import FeedRepository
    from mysql_storage.connection import engine_for
    from sqlalchemy import event
    from ugc.service import UGCService

    key = "fixed-" + uuid4().hex
    repo = FeedRepository(db)
    posts = UGCService(db)
    post = posts.command(
        "legacy-owner",
        "create",
        {"kind": "text", "title": "Fixed volume", "visibility": "public"},
        key,
    )
    post = posts.command(
        "legacy-owner",
        "publish",
        {"expectedVersion": post["version"]},
        key + "-pub",
        post["contentId"],
    )
    cache = FeedCache(redis_url, prefix="projection-bench:" + key)
    reactions = ReactionService(repo, cache)
    # Both arms use identical per-user histories, generated before consumer timing.
    users, history = (
        64,
        ("like", "neutral", "dislike", "neutral", "like", "dislike", "neutral", "like"),
    )
    for user in range(users):
        for version, state in enumerate(history, 1):
            reactions.set(f"{key}-{user}", post["contentId"], state, f"{key}:{user}:{version}")
    sql_count, redis_count = [0], [0]

    def count_sql(*_args):
        sql_count[0] += 1

    def count_redis(*args, **kwargs):
        redis_count[0] += 1
        return original(*args, **kwargs)

    # The fixture's redacted str subclass must use the same cache key as
    # database.mysql_target(), which normalizes the URL to a plain str.
    engine = engine_for(str(db))
    event.listen(engine, "before_cursor_execute", count_sql)
    original = cache.redis.execute_command
    cache.redis.execute_command = count_redis
    began = time.perf_counter()
    batches, coalesced = 0, 0
    try:
        while True:
            with get_connection(db) as conn:
                row = conn.execute(
                    "SELECT job_id FROM durable_jobs WHERE projection_key=? "
                    "AND status='queued' ORDER BY id LIMIT 1",
                    (post["contentId"],),
                ).fetchone()
                job = claim(conn, row[0]) if row else None
            if job is None:
                break
            if batched:
                result = run_job(reactions, job)
                coalesced += result["coalesced"]
            else:
                result = reactions.project(json.loads(job["payload_json"]))
                with get_connection(db) as conn:
                    finish(conn, job, result=result)
            batches += 1
        elapsed = time.perf_counter() - began
    finally:
        event.remove(engine, "before_cursor_execute", count_sql)
        cache.redis.execute_command = original
        cached = json.loads(cache.redis.get(f"{cache.prefix}:counter:{post['contentId']}"))
        cache.close()
    with get_connection(db) as conn:
        counter = dict(
            conn.execute(
                "SELECT * FROM content_counters WHERE content_id=?", (post["contentId"],)
            ).fetchone()
        )
        statuses = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT status,COUNT(*) FROM durable_jobs WHERE projection_key=? GROUP BY status",
                (post["contentId"],),
            )
        }
        events = [
            json.loads(r[0])["event_id"]
            for r in conn.execute(
                "SELECT payload_json FROM durable_jobs WHERE projection_key=?", (post["contentId"],)
            )
        ]
        receipts = conn.execute(
            "SELECT COUNT(*) FROM feed_inbox WHERE consumer='reaction' AND event_id IN ("
            + ",".join("?" for _ in events)
            + ")",
            tuple(events),
        ).fetchone()[0]
    assert counter["likes"] == users and counter["dislikes"] == 0
    assert sql_count[0] > 0, "SQL listener did not observe the business connection"
    assert cached["likes"] == users and cached["dislikes"] == 0
    assert statuses == {"completed": users * len(history)} and receipts == users * len(history)
    return {
        "seed": seed,
        "arm": "batch" if batched else "scalar",
        "submittedEvents": len(events),
        "elapsedSeconds": elapsed,
        "eventsPerSecond": len(events) / elapsed,
        "sqlStatements": sql_count[0],
        "scalarRedisCalls": redis_count[0],
        "redisPipelineBatches": batches if batched else 0,
        "consumerBatches": batches,
        "coalescedEvents": coalesced,
        "inboxReceipts": receipts,
        "finalLikes": counter["likes"],
        "finalDislikes": counter["dislikes"],
        "redisLikes": cached["likes"],
        "redisDislikes": cached["dislikes"],
        "completedJobs": statuses["completed"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    output = ROOT / "artifacts/perf/projection-batches"
    output.mkdir(parents=True, exist_ok=True)
    if args.child:
        sys.path[:0] = [
            str(ROOT / "recommend-radio/backend"),
            str(ROOT / "recommend-radio/scripts"),
        ]
        from mysql_fixture import NativeMySQL
        from mysql_storage.connection import engine_for
        from redis_fixture import NativeRedis

        os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
        mysql = NativeMySQL(args.child / "mysql")
        redis = None
        try:
            redis = NativeRedis(args.child / "redis")
            rows = []
            for seed in range(1 if args.quick else 3):
                for arm in (False, True) if seed % 2 == 0 else (True, False):
                    row = experiment(mysql.url, redis.url, seed, arm)
                    rows.append(row)
                    print(json.dumps(row), flush=True)
            (output / "fixed-volume.json").write_text(
                json.dumps(
                    {
                        "scope": (
                            "512 submitted events, 64 users x 8 transitions; "
                            "real MySQL/Redis, sequential consumer, not HTTP capacity"
                        ),
                        "samples": rows,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        finally:
            if redis:
                redis.close()
            engine_for(str(mysql.url)).dispose()
            engine_for(mysql.url).dispose()
            mysql.close()
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(prefix="projection-fixed-", dir=output.parent) as directory:
        env = isolated_environment(directory)
        env.update(FEED_ENABLED="true", JOB_TRANSPORT="disabled")
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--child",
                directory,
                *(["--quick"] if args.quick else []),
            ],
            check=True,
            cwd=ROOT,
            env=env,
        )


if __name__ == "__main__":
    main()
