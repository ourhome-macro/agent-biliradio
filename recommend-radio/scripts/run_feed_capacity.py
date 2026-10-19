"""Local bounded HTTP/Redis acceptance, never a production throughput claim."""

from __future__ import annotations

import json
import math
import os
import platform
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
from redis_fixture import NativeRedis

ROOT = Path(__file__).resolve().parents[2]


def percentile(values, fraction):
    values = sorted(values)
    return round(values[max(0, math.ceil(len(values) * fraction) - 1)], 3) if values else None


def batch(name, call, concurrency, *, total=100):
    barrier = threading.Barrier(concurrency)

    def one(index):
        if index < concurrency:
            barrier.wait(timeout=10)
        started = time.perf_counter()
        try:
            status, correct = call(index)
            failure = None
        except Exception as exc:
            status, correct, failure = "exception", False, type(exc).__name__
        return (time.perf_counter() - started) * 1000, status, correct, failure

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(one, range(total)))
    durations = [row[0] for row in results]
    good = [row[0] for row in results if row[1] == 200]
    result = {
        "scenario": name,
        "concurrency": concurrency,
        "requests": total,
        "wallSeconds": round(time.perf_counter() - started, 3),
        "latencyMs": {
            "p50": percentile(durations, 0.5),
            "p95": percentile(durations, 0.95),
            "max": round(max(durations), 3),
        },
        "successLatencyMs": {"p50": percentile(good, 0.5), "p95": percentile(good, 0.95)},
        "statusCounts": dict(Counter(str(row[1]) for row in results)),
        "incorrectResponses": sum(not row[2] for row in results),
        "exceptions": dict(Counter(row[3] for row in results if row[3])),
    }
    print(json.dumps(result), flush=True)
    return result


def main():
    import sys

    sys.path.insert(0, str(ROOT / "recommend-radio/backend"))
    with tempfile.TemporaryDirectory(prefix="radio-feed-capacity-") as directory:
        temp = Path(directory)
        redis = NativeRedis(temp / "redis")
        cache, server = None, None
        report = {
            "date": "2026-10-07",
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "cpuLogical": os.cpu_count(),
                "redisVersion": redis.client.info()["redis_version"],
                "database": "temporary SQLite",
                "redis": "isolated native process/random loopback port",
            },
            "method": {
                "httpServer": "threaded Werkzeug, actual registered Feed Flask API blueprint",
                "authorization": "fixed test user injected into Flask g; no OIDC/CSRF benchmark",
                "page": "fixed10-item page, actual caches + SQL,100HTTP requests per scenario",
                "projection": "direct ReactionService.project duplicate consumer invocations; no broker throughput claim",
                "concurrency": [8, 16],
                "feedAdmissionSlots": 8,
                "httpTimeoutSeconds": 20,
                "admissionWaitSeconds": 0.25,
                "redisSdkRetries": 0,
                "redisCircuitBackoffSeconds": 2,
            },
            "caveats": [
                "Local bounded acceptance only, not production capacity or SLA.",
                "Excludes production ASGI process topology, reverse proxy, OIDC/CSRF, WAN, MinIO and video throughput.",
                "503 is intentional admission rejection; success-only latency is reported separately.",
                "Projection runs real SQL/Inbox/Redis code directly, not through Redis Stream workers.",
            ],
            "results": [],
        }
        try:
            os.environ.update(
                {
                    "APP_DATA_DIR": str(temp / "app"),
                    "FEED_ENABLED": "true",
                    "RABBITMQ_ENABLED": "false",
                    "JOB_TRANSPORT": "redis_stream",
                    "JOB_REDIS_URL": redis.url,
                    "RECOMMEND_LLM_ENABLED": "false",
                    "AMEM_ENABLED": "false",
                }
            )
            from database import get_connection
            from error_code import APIError
            from feed_runtime.api import register_feed
            from feed_runtime.cache import FeedCache
            from feed_runtime.config import FeedConfig
            from feed_runtime.feed import FeedService
            from feed_runtime.reactions import ReactionService
            from feed_runtime.repository import FeedRepository
            from flask import Flask, g
            from models import Track
            from result import Result
            from werkzeug.serving import WSGIRequestHandler, make_server

            repo = FeedRepository(temp / "capacity.sqlite3")
            config = replace(FeedConfig(), redis_url=redis.url, workdir=temp / "work")
            cache = FeedCache(redis.url, prefix="radio:capacity")
            feed, reactions = FeedService(repo, config, cache), ReactionService(repo, cache)
            contents = [
                repo.admit(
                    Track(bvid=f"BV{i:010d}", cid=i + 1, title=f"Music live {i}", duration=60),
                    user_id="legacy-owner",
                )
                for i in range(25)
            ]
            runtime = SimpleNamespace(
                repo=repo,
                config=config,
                cache=cache,
                feed=feed,
                reactions=reactions,
                user_id="legacy-owner",
            )
            app = Flask("feed-capacity-fixture")

            @app.before_request
            def test_identity():
                g.current_user = {"id": "legacy-owner"}
                g._feed_services = runtime

            @app.errorhandler(APIError)
            def api_error(error):
                return Result.fail(error.message, error.code.name).json_with_status(
                    error.status_code
                )

            register_feed(app, music_services_factory=lambda: SimpleNamespace(recommendations=None))

            class QuietHandler(WSGIRequestHandler):
                def log(self, *_args, **_kwargs):
                    pass

            server = make_server("127.0.0.1", 0, app, threaded=True, request_handler=QuietHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with httpx.Client(
                base_url=f"http://127.0.0.1:{server.server_port}",
                timeout=20,
                limits=httpx.Limits(max_connections=32, max_keepalive_connections=32),
            ) as client:
                first = client.post(
                    "/api/feed/sessions",
                    json={"mode": "new"},
                    headers={"Idempotency-Key": "capacity-start"},
                )
                assert first.status_code == 201, first.text
                page = first.json()["data"]
                expected = [item["itemId"] for item in page["items"]]
                target = f"/api/feed/sessions/{page['sessionId']}/items"

                def fetch_page(_index):
                    response = client.get(target)
                    payload = response.json()
                    correct = (
                        response.status_code == 200
                        and [item["itemId"] for item in payload["data"]["items"]] == expected
                    ) or (response.status_code == 503 and payload["success"] is False)
                    return response.status_code, correct

                def react(index):
                    content = contents[index % 10]
                    response = client.put(
                        f"/api/content/{content}/reaction",
                        json={"state": "like"},
                        headers={"Idempotency-Key": "capacity-like-" + content},
                    )
                    payload = response.json()
                    return response.status_code, (
                        (
                            response.status_code == 200
                            and payload["data"]["state"] == "like"
                            and payload["data"]["relationVersion"] == 1
                        )
                        or (response.status_code == 503 and payload["success"] is False)
                    )

                for concurrency in (8, 16):
                    report["results"].append(
                        batch("fixed_page_redis_online_http", fetch_page, concurrency)
                    )
                    report["results"].append(
                        batch("idempotent_reaction_command_http", react, concurrency)
                    )
                # Ensure every desired relationship exists even if an HTTP burst was shed.
                for index in range(10):
                    assert react(index)[0] == 200

                def project(index):
                    content = contents[index % 10]
                    value = reactions.project(
                        {
                            "event_id": f"reaction:legacy-owner:{content}:1",
                            "user_id": "legacy-owner",
                            "content_id": content,
                            "state": "like",
                            "version": 1,
                        }
                    )
                    return 200, value["likes"] == 1 and value["dislikes"] == 0

                for concurrency in (8, 16):
                    report["results"].append(
                        batch("duplicate_projection_direct_service", project, concurrency)
                    )
                with get_connection(repo.db_path) as conn:
                    counter = conn.execute(
                        "SELECT SUM(likes),SUM(dislikes) FROM content_counters"
                    ).fetchone()
                    report["counterReconciliation"] = {
                        "expectedLikes": 10,
                        "actualLikes": counter[0],
                        "actualDislikes": counter[1],
                    }
                    assert tuple(counter) == (10, 0)
                for _ in range(8):
                    assert cache.request_slots.acquire(blocking=False)
                try:
                    saturated = batch("all_request_slots_held_http", fetch_page, 16)
                    report["results"].append(saturated)
                    assert saturated["statusCounts"] == {"503": 100}
                finally:
                    for _ in range(8):
                        cache.request_slots.release()

                redis.close()
                report["redisOutage"] = "only the owned test Redis process was terminated"
                for concurrency in (8, 16):
                    with cache._lock:
                        cache._local.clear()
                    report["results"].append(
                        batch("fixed_page_redis_unavailable_http", fetch_page, concurrency)
                    )
                # Reuse the owned directory/port only; no application Redis service is touched.
                redis = NativeRedis(temp / "redis", port=redis.port)
                assert cache.health(), "health probe must clear the recovered connection circuit"
                report["results"].append(batch("fixed_page_redis_recovered_http", fetch_page, 16))
                report["passed"] = all(
                    result["incorrectResponses"] == 0 and not result["exceptions"]
                    for result in report["results"]
                )
                assert report["passed"], "capacity acceptance contains incorrect/timeout responses"
        except Exception as exc:
            report["passed"] = False
            report["failure"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            if server:
                server.shutdown()
                server.server_close()
            if cache:
                cache.close()
            redis.close()
            destination = ROOT / "doc/feed-capacity-2026-10-07.json"
            destination.write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print(str(destination), flush=True)


if __name__ == "__main__":
    main()
