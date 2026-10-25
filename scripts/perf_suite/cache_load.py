from __future__ import annotations

import asyncio
import json
import os
import random
import subprocess
import sys
import time
from uuid import uuid4

import httpx
import requests
from database import get_connection

from .common import offered_load, save, summary
from .infra import port


class Server:
    def __init__(self, directory, db, redis, **options):
        self.port = port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.log = (directory / ("http-" + uuid4().hex[:8] + ".log")).open("wb")
        config = {
            "db": str(db),
            "redis": redis,
            "port": self.port,
            "prefix": "perf:" + uuid4().hex,
            **options,
        }
        self.process = subprocess.Popen(
            [sys.executable, "-m", "perf_suite.http_server"],
            env={
                **os.environ,
                "PERF_CONFIG": json.dumps(config),
                "FEED_ENABLED": "true",
                "JOB_TRANSPORT": "disabled",
                "AMEM_ENABLED": "false",
            },
            stdout=self.log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Benchmark HTTP server exited; inspect its local log")
            try:
                if requests.get(self.base + "/bench/ready", timeout=0.3).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        self.close()
        raise RuntimeError("Benchmark HTTP server readiness timeout")

    def stats(self):
        return requests.get(self.base + "/bench/stats", timeout=5).json()

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.log.close()


def seed(db):
    import sqlalchemy as sa
    from feed_runtime.repository import FeedRepository
    from models import Track
    from ugc.service import UGCService

    repo = FeedRepository(db)
    with get_connection(db) as conn:
        tracks, contents = [], []
        for i in range(4096):
            data = Track(
                track_id=f"bench-track-{i}",
                bvid=f"BV{i:010d}",
                cid=i + 1,
                title=f"Music live concert {i}",
                duration=60,
            ).to_dict()
            data.update(id=i, description="x" * 1024)
            raw = json.dumps(data)
            tracks.append(
                {
                    "track_id": data["trackId"],
                    "bvid": data["bvid"],
                    "cid": i + 1,
                    "title": data["title"],
                    "duration": 60,
                    "raw_json": raw,
                    "updated_at": "fixture",
                }
            )
            contents.append(
                {
                    "content_id": f"bench-{i}",
                    "track_id": data["trackId"],
                    "scope": "public",
                    "metadata_json": raw,
                    "admitted_at": time.time() - 1000 + i * 0.001,
                }
            )
        conn.connection.execute(sa.insert(conn.tables["tracks"]), tracks)
        conn.connection.execute(sa.insert(conn.tables["feed_content"]), contents)
        conn.executemany(
            "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
            [(f"load-{i}", "fixture", "fixture") for i in range(512)],
        )
    post = UGCService(db).command(
        "legacy-owner", "create", {"kind": "audio", "title": "Popular fixture audio"}, "perf-audio"
    )
    # Fixture only: byte processing is tested separately; this test exercises reaction authority.
    with get_connection(db) as conn:
        conn.execute(
            "UPDATE ugc_posts SET status='published',visibility='public' WHERE content_id=?",
            (post["contentId"],),
        )
        conn.execute(
            "UPDATE feed_content SET status='admitted',scope='public' WHERE content_id=?",
            (post["contentId"],),
        )
    for i in range(24):
        repo.admit(
            Track(bvid=f"BV{i:010d}", cid=i + 1, title=f"Music live concert {i}", duration=60),
            user_id="legacy-owner",
        )
    return post["contentId"]


def attach_stats(result, before, after):
    result["sqlProfile"] = {
        key: {
            "calls": value["calls"] - before.get("sqlProfile", {}).get(key, {}).get("calls", 0),
            "wallMs": value["wallMs"] - before.get("sqlProfile", {}).get(key, {}).get("wallMs", 0),
        }
        for key, value in after.get("sqlProfile", {}).items()
    }
    result["sqlStatements"] = after.get("sql", 0) - before.get("sql", 0)
    result["selectStatements"] = after.get("select", 0) - before.get("select", 0)
    result["originLoads"] = after.get("originLoads", 0) - before.get("originLoads", 0)
    result["peakSQLPerSecond"] = max(
        (
            v - before.get("sqlBuckets", {}).get(k, 0)
            for k, v in after.get("sqlBuckets", {}).items()
        ),
        default=0,
    )
    result["peakOriginLoadsPerSecond"] = max(
        (
            v - before.get("originBuckets", {}).get(k, 0)
            for k, v in after.get("originBuckets", {}).items()
        ),
        default=0,
    )
    delta = {
        key: value - before.get("metrics", {}).get(key, 0)
        for key, value in after["metrics"].items()
    }
    result["cacheEvents"] = delta
    hits = delta.get("cache:local_hit", 0) + delta.get("cache:redis_hit", 0)
    misses = delta.get("cache:miss", 0)
    calls = after.get("cacheCalls", 0) - before.get("cacheCalls", 0)
    result["cacheHitRate"] = hits / calls if calls else None
    result["loaderMisses"] = misses
    result["coalesced"] = delta.get("cache:coalesced", 0)
    result["coalescingRate"] = result["coalesced"] / calls if calls else None
    result["redisClientCalls"] = after.get("redisCalls", 0) - before.get("redisCalls", 0)
    result["redisClientQps"] = result["redisClientCalls"] / result["elapsedSeconds"]
    result["processResources"] = after["resources"]


def run_cache(directory, db, redis, *, quick=False, feed_only=False):
    results = []
    experiments = [
        ("tiers", [("redis", False), ("local_redis", True)]),
        ("singleflight", [("off", False), ("on", True)]),
        ("hotkey", [("soft_expiry", False), ("proactive", True)]),
    ]
    for experiment, arms in [] if feed_only else experiments:
        for seed_value in range(1 if quick else 3):
            ordered = arms if seed_value % 2 == 0 else list(reversed(arms))
            for arm, enabled in ordered:
                server = Server(
                    directory,
                    db,
                    redis,
                    local=enabled if experiment == "tiers" else True,
                    merge=enabled if experiment == "singleflight" else True,
                    hot=enabled if experiment == "hotkey" else False,
                    compressed=experiment == "hotkey",
                )
                try:
                    for pattern in (
                        ["stampede"] if experiment == "singleflight" else ["hot", "uniform"]
                    ):
                        rng = random.Random(seed_value)
                        rate, seconds = (60, 8) if experiment == "hotkey" else (60, 4)
                        if quick:
                            seconds = 3
                        total = round(rate * seconds)
                        ids = [
                            0
                            if pattern != "uniform" and rng.random() < 0.9
                            else rng.randrange(4096)
                            for _ in range(total)
                        ]
                        if pattern == "stampede":
                            ids = [0] * total
                        before = server.stats()

                        async def request(client, i, trace=ids):
                            return await client.get("/bench/metadata/" + str(trace[i]))

                        if pattern == "stampede":

                            async def bursts(base=server.base):
                                values = []
                                burst_started = time.perf_counter()
                                async with httpx.AsyncClient(
                                    base_url=base, timeout=5, trust_env=False
                                ) as client:
                                    await client.get("/bench/metadata/0")
                                    for _wave in range(4 if quick else 10):
                                        await client.post("/bench/clear/0")
                                        began = time.perf_counter()

                                        async def one(index, epoch=began):
                                            try:
                                                response = await client.get("/bench/metadata/0")
                                                status = response.status_code
                                            except httpx.HTTPError:
                                                status = 0
                                            values.append(
                                                {
                                                    "status": status,
                                                    "latencyMs": (time.perf_counter() - epoch)
                                                    * 1000,
                                                    "correct": True,
                                                    "index": index,
                                                }
                                            )

                                        await asyncio.gather(*(one(i) for i in range(32)))
                                value = summary(values)
                                value.update(
                                    offeredRequests=len(values),
                                    clientDropped=0,
                                    offeredRps=None,
                                    coldBursts=4 if quick else 10,
                                    concurrency=32,
                                    elapsedSeconds=time.perf_counter() - burst_started,
                                )
                                return value, values

                            result, rows = asyncio.run(bursts())
                        else:
                            result, rows = asyncio.run(
                                offered_load(server.base, rate, seconds, request)
                            )
                        after = server.stats()
                        attach_stats(result, before, after)
                        result.update(
                            experiment=experiment,
                            arm=arm,
                            seed=seed_value,
                            pattern=pattern,
                            scope=(
                                "production FeedCache over HTTP; actual MySQL loader; "
                                "no authorization query"
                            ),
                        )
                        save(
                            directory / f"raw-cache-{experiment}-{arm}-{pattern}-{seed_value}.json",
                            rows,
                        )
                        results.append(result)
                        save(directory / "cache-results.partial.json", results)
                        print(
                            json.dumps(
                                {
                                    k: result[k]
                                    for k in (
                                        "experiment",
                                        "arm",
                                        "pattern",
                                        "p99Ms",
                                        "originLoads",
                                        "statusCounts",
                                    )
                                }
                            ),
                            flush=True,
                        )
                finally:
                    server.close()
    server = Server(directory, db, redis)
    try:
        response = requests.post(
            server.base + "/api/feed/sessions",
            json={"mode": "new"},
            headers={"Idempotency-Key": "feed-read"},
            timeout=20,
        )
        response.raise_for_status()
        page = response.json()["data"]
        for rate in (200, 500, 1000):
            before = server.stats()

            async def request(client, _i):
                return await client.get(f"/api/feed/sessions/{page['sessionId']}/items")

            result, rows = asyncio.run(offered_load(server.base, rate, 3, request))
            attach_stats(result, before, server.stats())
            result.update(
                experiment="feed_http",
                arm="current",
                scope="actual 10-item Feed blueprint, fixture identity, ASGI WSGI workers=16",
            )
            results.append(result)
            save(directory / f"raw-feed-{rate}.json", rows)
    finally:
        server.close()
    return results


def run_reactions(directory, db, redis, content, *, quick=False):
    server = Server(directory, db, redis, projector=True, content=content)
    result_rows = []
    try:
        for phase, rate in enumerate((200, 500, 1000, 2000)):
            before = server.stats()

            async def request(client, i, stage=phase):
                user = f"load-{i % 512}"
                state = ("like", "dislike", "neutral")[(i // 512 + stage) % 3]
                return await client.put(
                    f"/api/content/{content}/reaction",
                    json={"state": state},
                    headers={"X-Bench-User": user, "Idempotency-Key": f"phase-{stage}-{i}"},
                )

            result, rows = asyncio.run(offered_load(server.base, rate, 2 if quick else 4, request))
            attach_stats(result, before, server.stats())
            result.update(
                experiment="reactions_http",
                scope=(
                    "real ReactionService SQL, SQL projection workers=2, Redis Bitmap; "
                    "excludes Kafka transport"
                ),
            )
            save(directory / f"raw-reaction-{rate}.json", rows)
            result_rows.append(result)
            print(
                json.dumps(
                    {
                        "reactionOfferedRps": rate,
                        "p99Ms": result["p99Ms"],
                        "statuses": result["statusCounts"],
                    }
                ),
                flush=True,
            )
        started = time.monotonic()
        quiet = 0
        while time.monotonic() - started < 45:
            value = requests.get(server.base + "/bench/reconcile", timeout=5).json()
            if (
                value["pending"] == 0
                and value.get("httpActive", 0) <= 1
                and value.get("flaskActive", 0) <= 1
            ):
                quiet += 1
            else:
                quiet = 0
            if quiet >= 3:
                break
            time.sleep(0.1)
        valid = (
            value["likes"] == value["expectedLikes"]
            and value["dislikes"] == value["expectedDislikes"]
        )
        return {
            "stages": result_rows,
            "convergenceSecondsAfterProducerStop": time.monotonic() - started,
            "stateCorrect": valid,
            "serverQuiescent": quiet >= 3,
            "finalState": value,
        }
    finally:
        server.close()
