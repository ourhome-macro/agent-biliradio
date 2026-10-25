"""Actual Feed blueprint and cache code over HTTP, with explicit fixture identities."""

from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter

import uvicorn
from a2wsgi import WSGIMiddleware
from database import get_connection
from durable_jobs import claim, finish
from error_code import APIError
from feed_runtime.api import register_feed
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.hotkeys import HotKeyConfig
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from flask import Flask, g, request
from mysql_storage.connection import engine_for
from prometheus_client import REGISTRY
from result import Result
from sqlalchemy import event

from .common import resources


class NoFlights(dict):
    def get(self, *args):
        return None


class UnmergedCache(FeedCache):
    """Experimental control: retain cache/limits, disable both loader coalescing layers."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._flights = NoFlights()

    def _redis_call(self, command, *args, **kwargs):
        if command == "set" and args[0].endswith(":refresh"):
            return True
        return super()._redis_call(command, *args, **kwargs)


def main():
    from types import SimpleNamespace

    config = json.loads(os.environ["PERF_CONFIG"])
    db, prefix = config["db"], config["prefix"]
    repo = FeedRepository(db)
    # Resolve the SQL dialect/catalog once before concurrent arrivals. Report
    # cold process startup separately from steady-state request capacity.
    with get_connection(db) as warm_connection:
        warm_connection.execute("SELECT 1")
    lock = threading.Lock()
    counters, sql_buckets, origin_buckets = Counter(), Counter(), Counter()
    sql_profile = {}
    started = time.monotonic()

    def sql_count(_conn, _cursor, statement, _params, _context, _many):
        _context._perf_started = time.perf_counter()
        text = statement.lower()
        _context._perf_category = next(
            (
                name
                for name, token in (
                    ("locks", "business_locks"),
                    ("bitmap_mapping", "reaction_bitmap_users"),
                    ("joins", " join "),
                    ("reaction_state", "content_reactions"),
                    ("counters", "content_counters"),
                    ("jobs", "durable_jobs"),
                )
                if token in text
            ),
            "other",
        )
        with lock:
            counters["sql"] += 1
            sql_buckets[int(time.monotonic() - started)] += 1
            if statement.lstrip().upper().startswith("SELECT"):
                counters["select"] += 1

    def sql_elapsed(_conn, _cursor, _statement, _params, context, _many):
        elapsed = (time.perf_counter() - context._perf_started) * 1000
        with lock:
            bucket = sql_profile.setdefault(context._perf_category, {"calls": 0, "wallMs": 0})
            bucket["calls"] += 1
            bucket["wallMs"] += elapsed

    event.listen(engine_for(db), "before_cursor_execute", sql_count)
    event.listen(engine_for(db), "after_cursor_execute", sql_elapsed)

    def load(identity):
        with lock:
            counters["originLoads"] += 1
            origin_buckets[int(time.monotonic() - started)] += 1
        with get_connection(db) as connection:
            row = connection.execute(
                "SELECT metadata_json FROM feed_content WHERE content_id=?",
                ("bench-" + str(int(identity)),),
            ).fetchone()
        return json.loads(row[0])

    hot = HotKeyConfig(
        enabled=config.get("hot", False),
        window=2,
        promote=30,
        demote=5,
        refresh=0.45,
        soft=0.7,
        hard=1.2,
        tick=0.05,
        lease=1,
        timeout=0.3,
    )
    cache_type = FeedCache if config.get("merge", True) else UnmergedCache
    cache = cache_type(
        config["redis"],
        prefix=prefix,
        hotkey_config=hot,
        metadata_loader=lambda identity: (
            f"{prefix}:content:{identity}:acl:1:public:v1",
            load(identity),
            1,
        ),
    )
    redis_execute = cache.redis.execute_command

    def observed_redis(*args, **kwargs):
        with lock:
            counters["redisCalls"] += 1
        return redis_execute(*args, **kwargs)

    cache.redis.execute_command = observed_redis
    if config.get("compressed"):

        def remember(key, value):
            with cache._lock:
                cache._local[key] = (time.time() + 0.04, dict(value))
                cache._local.move_to_end(key)
                while len(cache._local) > 2048:
                    cache._local.popitem(last=False)

        cache._remember = remember
    feed_config = FeedConfig()
    feed = FeedService(repo, feed_config, cache)
    original_get = cache.get

    def observed_get(*args, **kwargs):
        with lock:
            counters["cacheCalls"] += 1
        return original_get(*args, **kwargs)

    cache.get = observed_get
    reactions = ReactionService(repo, cache)
    stop = threading.Event()

    def projection():
        while not stop.wait(0.01):
            try:
                with get_connection(db) as conn:
                    row = conn.execute(
                        "SELECT job_id FROM durable_jobs WHERE kind='feed_projection' "
                        "AND status='queued' AND available_at<=? ORDER BY id LIMIT 1",
                        (time.time(),),
                    ).fetchone()
                    job = claim(conn, row[0]) if row else None
                if job:
                    try:
                        from feed_runtime.projection_batch import run_job

                        value = run_job(reactions, job)
                        with get_connection(db) as conn:
                            finish(conn, job, result=value)
                    except Exception as error:
                        with get_connection(db) as conn:
                            finish(conn, job, error=error)
            except Exception:
                with lock:
                    counters["projectionErrors"] += 1
                stop.wait(0.1)

    if config.get("projector"):
        for _ in range(2):
            threading.Thread(target=projection, daemon=True).start()
    app = Flask("isolated-perf")

    @app.before_request
    def identity():
        with lock:
            counters["flaskActive"] += 1
        g._bench_counted = True
        user = request.headers.get("X-Bench-User", "legacy-owner")
        g.current_user = {"id": user}
        g._feed_services = SimpleNamespace(
            repo=repo,
            config=feed_config,
            cache=cache,
            feed=feed,
            reactions=reactions,
            user_id=user,
        )

    @app.teardown_request
    def request_done(_error):
        if g.pop("_bench_counted", False):
            with lock:
                counters["flaskActive"] -= 1

    @app.errorhandler(APIError)
    def error(value):
        return Result.fail(value.message, value.code.name).json_with_status(value.status_code)

    register_feed(app, music_services_factory=lambda: None)

    @app.get("/bench/ready")
    def ready():
        return {"ready": True}

    @app.get("/bench/metadata/<int:identifier>")
    def metadata(identifier):
        if not cache.request_slots.acquire(timeout=0.25):
            return {"error": "admission"}, 503
        try:
            data = cache.get(
                f"content:{identifier}:acl:1:public",
                lambda: load(identifier),
                shared=config.get("local", True),
                content_id=str(identifier),
                soft=0.7 if config.get("compressed") else 60,
                hard=1.2 if config.get("compressed") else 300,
            )
            return data
        finally:
            cache.request_slots.release()

    @app.post("/bench/clear/<int:identifier>")
    def clear(identifier):
        cache.invalidate_content(str(identifier))
        return {"cleared": True}

    @app.get("/bench/stats")
    def stats():
        metrics = {}
        for family in REGISTRY.collect():
            for sample in family.samples:
                if sample.name == "radio_feed_operations_total":
                    metrics[sample.labels["operation"] + ":" + sample.labels["outcome"]] = (
                        sample.value
                    )
        with lock:
            result = {
                **counters,
                "sqlBuckets": dict(sql_buckets),
                "originBuckets": dict(origin_buckets),
                "sqlProfile": {k: dict(v) for k, v in sql_profile.items()},
            }
        return {**result, "metrics": metrics, "resources": resources()}

    @app.get("/bench/reconcile")
    def reconcile():
        with get_connection(db) as conn:
            row = conn.execute(
                "SELECT likes,dislikes FROM content_counters WHERE content_id=?",
                (config.get("content"),),
            ).fetchone()
            expected = conn.execute(
                "SELECT SUM(CASE WHEN state='like' THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN state='dislike' THEN 1 ELSE 0 END) "
                "FROM content_reactions WHERE content_id=?",
                (config.get("content"),),
            ).fetchone()
            pending = conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE kind='feed_projection' "
                "AND status IN ('queued','running')"
            ).fetchone()[0]
        with lock:
            active = {"httpActive": counters["httpActive"], "flaskActive": counters["flaskActive"]}
        return {
            "likes": row[0],
            "dislikes": row[1],
            "expectedLikes": expected[0] or 0,
            "expectedDislikes": expected[1] or 0,
            "pending": pending,
            **active,
        }

    try:
        wsgi = WSGIMiddleware(app, workers=16)

        async def measured(scope, receive, send):
            if scope["type"] == "http":
                with lock:
                    counters["httpActive"] += 1
            try:
                await wsgi(scope, receive, send)
            finally:
                if scope["type"] == "http":
                    with lock:
                        counters["httpActive"] -= 1

        uvicorn.run(
            measured,
            host="127.0.0.1",
            port=config["port"],
            log_level="error",
            access_log=False,
        )
    finally:
        stop.set()
        cache.close()


if __name__ == "__main__":
    main()
