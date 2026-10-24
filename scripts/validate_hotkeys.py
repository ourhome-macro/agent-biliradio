"""Run isolated hot-key regression, native Redis fault tests and paired cache A/B load."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, int(len(values) * fraction))]


def benchmark(url, *, enabled, seed, duration=8, rate=240):
    from feed_runtime.cache import FeedCache
    from feed_runtime.hotkeys import HotKeyConfig

    prefix = f"ab:{rate}:{seed}:{'B' if enabled else 'A'}"
    lock = threading.Lock()
    foreground, background, errors = 0, 0, 0
    sql_ms = 8
    # Both arms share exactly the same cache deadlines and request schedule.
    # Short deadlines compress lifecycle time; these are NOT production defaults.
    config = HotKeyConfig(
        enabled=enabled,
        window=2,
        promote=30,
        demote=5,
        cold_windows=3,
        refresh=0.45,
        soft=0.7,
        hard=1.2,
        tick=0.05,
        lease=1,
        timeout=0.3,
        capacity=8,
        tracked=32,
        batch=32,
    )

    def loader(identity):
        nonlocal background
        with lock:
            background += 1
        time.sleep(sql_ms / 1000)
        return f"{prefix}:content:{identity}:acl:1:public:v1", {"title": identity}, 1

    caches = [
        FeedCache(url, prefix=prefix, hotkey_config=config, metadata_loader=loader)
        for _ in range(2)
    ]
    # Content L1 normally lives for 2 seconds; scale it alongside compressed TTLs.
    for cache in caches:

        def remember(key, result, target=cache):
            with target._lock:
                target._local[key] = (time.time() + 0.04, dict(result))

        cache._remember = remember

    def request(task):
        nonlocal foreground, errors
        index, identity, due = task
        remaining = start + due - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        began = time.perf_counter()

        def load():
            nonlocal foreground
            with lock:
                foreground += 1
            time.sleep(sql_ms / 1000)
            return {"title": identity}

        try:
            value = caches[index % 2].get(
                f"content:{identity}:acl:1:public",
                load,
                soft=config.soft,
                hard=config.hard,
                shared=True,
                content_id=identity,
            )
            if value != {"title": identity}:
                raise AssertionError("cache identity contamination")
        except Exception:
            with lock:
                errors += 1
        ended = time.perf_counter()
        return (ended - began) * 1000, max(0, ended - start - due) * 1000

    rng = random.Random(seed)
    tasks = [
        (i, "hot" if rng.random() < 0.9 else f"cold{rng.randrange(12)}", i / rate)
        for i in range(int(duration * rate))
    ]
    start = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=24) as pool:
            timings = list(pool.map(request, tasks))
        latencies = [item[0] for item in timings]
        scheduled = [item[1] for item in timings]
        elapsed = time.perf_counter() - start
        result = {
            "arm": "B_hotkeys" if enabled else "A_request_refresh",
            "seed": seed,
            "offeredRequestsPerSecond": rate,
            "requests": len(tasks),
            "errors": errors,
            "elapsedSeconds": round(elapsed, 3),
            "throughput": round(len(tasks) / elapsed, 2),
            "p50Ms": round(statistics.median(latencies), 3),
            "p95Ms": round(percentile(latencies, 0.95), 3),
            "p99Ms": round(percentile(latencies, 0.99), 3),
            "scheduledResponseP95Ms": round(percentile(scheduled, 0.95), 3),
            "foregroundPathLoads": foreground,
            "proactiveLoads": background,
            "totalLoads": foreground + background,
            "hotMembers": caches[0].redis.zrange(caches[0].hotkeys.due, 0, -1) if enabled else [],
        }
        return result
    finally:
        for cache in caches:
            cache.close()


def abandoned_worker(url, prefix, pipe):
    from feed_runtime.cache import FeedCache
    from feed_runtime.hotkeys import HotKeyConfig

    cache = FeedCache(
        url,
        prefix=prefix,
        hotkey_config=HotKeyConfig(enabled=True, lease=0.3, timeout=0.1),
        metadata_loader=lambda _: None,
        hotkey_background=False,
    )
    pipe.send(cache.hotkeys.claim("song"))
    pipe.close()
    time.sleep(60)  # Parent deliberately kills this process after the claim.


def native_faults(server):
    from feed_runtime.cache import FeedCache
    from feed_runtime.hotkeys import HotKeyConfig

    cfg = HotKeyConfig(enabled=True, promote=4, demote=1)
    c = FeedCache(
        server.url,
        prefix="native:faults",
        hotkey_config=cfg,
        metadata_loader=lambda identity: (
            f"native:faults:content:{identity}:acl:1:public:v1",
            {"ok": True},
            1,
        ),
        hotkey_background=False,
    )
    h = c.hotkeys
    try:
        c.redis.config_set("appendfsync", "always")
        for _ in range(4):
            h.observe("song")
        h.flush()
        c.redis.zadd(h.due, {"song": 0})
        context = multiprocessing.get_context("spawn")
        reader, writer = context.Pipe(duplex=False)
        worker = context.Process(target=abandoned_worker, args=(server.url, c.prefix, writer))
        worker.start()
        writer.close()
        try:
            assert reader.poll(10), "child failed before claiming work"
            token = reader.recv()
            assert token
        finally:
            worker.terminate()
            worker.join(timeout=5)
            reader.close()
        time.sleep(0.4)
        replacement = h.claim("song")
        assert replacement and replacement != token
        h.refresh_one("song", token)
        key = "native:faults:content:song:acl:1:public:v1"
        assert c.redis.get(key) is None
        h.refresh_one("song", replacement)
        assert json.loads(c.redis.get(key))["data"] == {"ok": True}
        # Actual server death; no existing business service is involved.
        server.process.terminate()
        server.process.wait(timeout=5)
        for _ in range(4):
            h.observe("song")
        try:
            h.flush()
        except Exception:
            pass
        else:
            raise AssertionError("server outage was not observed")
        assert not h.pending
        result = c.get(
            "content:outage:acl:1:public", lambda: {"ok": True}, shared=True, content_id="outage"
        )
        assert result == {"ok": True}
        server.close()
        from redis_fixture import NativeRedis

        recovered = NativeRedis(server.directory, port=server.port)
        try:
            assert c.health()
            assert c.redis.zscore(h.due, "song") is not None
            c.redis.zadd(h.due, {"song": 0})
            current = h.claim("song")
            assert current
            h.refresh_one("song", current)
            assert json.loads(c.redis.get(key))["data"] == {"ok": True}
        finally:
            recovered.close()
        return {
            "nativeLeaseExpiryAndFencing": True,
            "nativeRedisTermination": True,
            "boundedOutageFallback": True,
            "workerProcessKilled": True,
            "redisRestartRecoveredSchedule": True,
        }
    finally:
        c.close()


def child(directory, output, *, quick=False):
    sys.path.insert(0, str(ROOT / "recommend-radio/backend"))
    sys.path.insert(0, str(ROOT / "recommend-radio/scripts"))
    from redis_fixture import NativeRedis

    files = [
        "test_feed_hotkeys.py",
        "test_feed_cache_outage.py",
        "test_feed_runtime.py",
        "test_feed_recommendation_policy.py",
        "test_ugc_lifecycle.py",
    ]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--tb=short",
            *[str(ROOT / "recommend-radio/backend/tests" / name) for name in files],
            f"--basetemp={directory / 'pytest'}",
            f"--junitxml={output.parent / 'hotkey-regression.xml'}",
        ],
        check=True,
        cwd=ROOT,
    )
    server = NativeRedis(directory / "redis")
    try:
        samples = []
        for seed in range(1 if quick else 3):
            # Alternate order to reduce order/thermal bias.
            for enabled in (False, True) if seed % 2 == 0 else (True, False):
                result = benchmark(
                    server.url, enabled=enabled, seed=seed, duration=3 if quick else 8
                )
                samples.append(result)
                print(json.dumps(result), flush=True)
        if not quick:
            for enabled in (True, False):
                result = benchmark(server.url, enabled=enabled, seed=3, duration=6, rate=1200)
                samples.append(result)
                print(json.dumps(result), flush=True)
        faults = native_faults(server)
        report = {
            "scope": "isolated native Redis, two cache instances, synthetic 8ms origin; "
            "not full Feed HTTP throughput or recommendation quality",
            "samples": samples,
            "faults": faults,
        }
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        if any(s["errors"] for s in samples):
            raise RuntimeError("A/B cache requests failed; inspect report")
        if any("hot" not in s["hotMembers"] for s in samples if s["arm"] == "B_hotkeys"):
            raise RuntimeError("B arm did not detect the intended hot content")
    finally:
        server.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/quality/hotkey-ab.json")
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.child:
        child(args.child, output, quick=args.quick)
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(prefix="hotkey-", dir=output.parent) as folder:
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            folder,
            "--output",
            str(output),
        ]
        if args.quick:
            command.append("--quick")
        subprocess.run(command, check=True, cwd=ROOT, env=isolated_environment(folder))


if __name__ == "__main__":
    main()
