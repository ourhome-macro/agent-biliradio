from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time

import httpx
import requests

from .common import percentile, resources, save


def serve():
    from pathlib import Path

    from feed_runtime.bili_proxy import _SLOTS
    from flask import request
    from validate_media_paths import proxy_playback

    config = json.loads(os.environ["PERF_STREAM"])

    def observer(context):
        from error_code import APIError

        app, service = context["app"], context["service"]

        @app.errorhandler(APIError)
        def api_error(error):
            return {"error": error.message}, error.status_code

        @app.errorhandler(Exception)
        def unexpected(error):
            with context["lock"]:
                errors = context["stats"].setdefault("errors", {})
                errors[type(error).__name__] = errors.get(type(error).__name__, 0) + 1
            return {"errorType": type(error).__name__}, 500

        if not config["cache"]:

            def uncached(row, user_id, **kwargs):
                manifest = json.loads(row["manifest_json"])
                source = service.source_factory(
                    user_id=user_id, scope=manifest["scope"], db_path=service.repo.db_path
                )
                try:
                    from models import Track

                    return source.resolve(Track.from_dict({**manifest, "source": "bili"}))
                finally:
                    source.close()

            service._bundle = uncached

        @app.post("/bench/play")
        def play():
            return service.create("legacy-owner", context["content"], str(request.json["key"]))

        @app.delete("/bench/play/<identifier>")
        def release(identifier):
            return service.release(identifier, "legacy-owner")

        @app.post("/bench/beat/<identifier>")
        def beat(identifier):
            return service.heartbeat(identifier, "legacy-owner")

        @app.get("/bench/stats")
        def stats():
            with context["lock"]:
                result = dict(context["stats"])
            return {**result, **resources(), "proxySlotsAvailable": _SLOTS._value}

        save(
            config["ready"],
            {
                "base": context["base"],
                "playback": context["playback"],
                "bytes": len(context["files"]["video"]),
            },
        )
        while not os.path.exists(config["stop"]):
            time.sleep(0.1)
        return {"stopped": True}

    proxy_playback(
        Path(config["directory"]),
        observer=observer,
        db_path=config["db"],
        resolver_delay=0.02,
        upstream_delay=0.002,
        fixture_seconds=20,
    )


async def wave(base, concurrency, *, cycles=3, total_bytes=65536):
    rows = []
    async with httpx.AsyncClient(
        base_url=base,
        timeout=10,
        trust_env=False,
        limits=httpx.Limits(
            max_connections=concurrency * 2, max_keepalive_connections=concurrency * 2
        ),
    ) as client:

        async def viewer(index):
            for cycle in range(cycles):
                began = time.perf_counter()
                playback = None
                try:
                    started = await client.post(
                        "/bench/play", json={"key": f"{time.time_ns()}:{index}:{cycle}"}
                    )
                    if started.status_code != 200:
                        rows.append(
                            {
                                "status": started.status_code,
                                "ttfbMs": (time.perf_counter() - began) * 1000,
                                "bytes": 0,
                            }
                        )
                        continue
                    playback = started.json()
                    root = playback["manifestUrl"].replace("index.m3u8", "video.mp4")
                    # Initial buffer, seek range, then abort a later request and switch playback.
                    for offset in (0, 4096, 8192):
                        if time.perf_counter() - began > 4:
                            await client.post("/bench/beat/" + playback["playbackId"])
                        request_started = time.perf_counter()
                        size, first = 0, None
                        end = total_bytes - 1 if offset == 8192 else offset + 8191
                        async with client.stream(
                            "GET", root, headers={"Range": f"bytes={offset}-{end}"}
                        ) as response:
                            async for chunk in response.aiter_bytes():
                                if first is None:
                                    first = time.perf_counter()
                                size += len(chunk)
                                if offset == 8192:
                                    break
                        rows.append(
                            {
                                "status": response.status_code,
                                "ttfbMs": (
                                    (first or time.perf_counter())
                                    - (began if offset == 0 else request_started)
                                )
                                * 1000,
                                "bytes": size,
                                "mode": "abort_switch"
                                if offset == 8192
                                else "seek"
                                if offset
                                else "initial",
                            }
                        )
                except httpx.HTTPError:
                    rows.append(
                        {"status": 0, "ttfbMs": (time.perf_counter() - began) * 1000, "bytes": 0}
                    )
                finally:
                    if playback:
                        try:
                            await client.delete("/bench/play/" + playback["playbackId"])
                        except httpx.HTTPError:
                            pass

        began = time.perf_counter()
        await asyncio.gather(*(viewer(i) for i in range(concurrency)))
        elapsed = time.perf_counter() - began
    from collections import Counter

    return {
        "concurrentViewers": concurrency,
        "requests": len(rows),
        "elapsedSeconds": elapsed,
        "statusCounts": dict(Counter(str(r["status"]) for r in rows)),
        "ttfbP95Ms": percentile([r["ttfbMs"] for r in rows], 0.95),
        "ttfbP99Ms": percentile([r["ttfbMs"] for r in rows], 0.99),
        "successTtfbP99Ms": percentile([r["ttfbMs"] for r in rows if r["status"] == 206], 0.99),
        "egressMiBPerSecond": sum(r["bytes"] for r in rows) / 1024 / 1024 / elapsed,
    }, rows


def run(directory, output, db, *, quick=False):
    all_results = []
    for seed in range(1 if quick else 3):
        for cached in (False, True) if seed % 2 == 0 else (True, False):
            fixture = directory / f"stream-{seed}-{cached}"
            fixture.mkdir()
            ready, stop = fixture / "ready.json", fixture / "stop"
            config = {
                "cache": cached,
                "directory": str(fixture),
                "db": str(db),
                "ready": str(ready),
                "stop": str(stop),
            }
            log = (output / f"stream-{seed}-{cached}.log").open("wb")
            process = subprocess.Popen(
                [sys.executable, "-m", "perf_suite.stream_load"],
                env={**os.environ, "PERF_STREAM": json.dumps(config)},
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            try:
                deadline = time.monotonic() + 30
                while not ready.exists() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("Stream fixture exited; inspect its log")
                    time.sleep(0.1)
                value = json.loads(ready.read_text())
                stages = (16, 32, 64, 128, 256) if cached and seed == 0 else (32,)
                for concurrency in stages:
                    before = requests.get(value["base"] + "/bench/stats", timeout=5).json()
                    samples = []
                    sampling_stop = threading.Event()

                    def sample(base=value["base"], values=samples, stopped=sampling_stop):
                        while not stopped.is_set():
                            try:
                                values.append(requests.get(base + "/bench/stats", timeout=2).json())
                            except requests.RequestException:
                                pass
                            stopped.wait(0.2)

                    sampling = threading.Thread(target=sample, daemon=True)
                    sampling.start()
                    result, rows = asyncio.run(
                        wave(
                            value["base"],
                            concurrency,
                            cycles=1 if quick else 2,
                            total_bytes=value["bytes"],
                        )
                    )
                    sampling_stop.set()
                    sampling.join(timeout=3)
                    time.sleep(0.5)
                    after = requests.get(value["base"] + "/bench/stats", timeout=5).json()
                    result.update(
                        experiment="playurl" if len(stages) == 1 else "proxy_capacity",
                        arm="cached_coalesced" if cached else "uncached",
                        seed=seed,
                        resolutions=after["resolutions"] - before["resolutions"],
                        resourcesBefore=before,
                        resourcesAfter=after,
                        peakRssBytes=max(
                            (s["rssBytes"] for s in samples), default=after["rssBytes"]
                        ),
                        peakActiveUpstream=max((s["activeUpstream"] for s in samples), default=0),
                        connectionSlotsRecovered=after["proxySlotsAvailable"] == 32
                        and after["activeUpstream"] == 0,
                        scope=(
                            "production proxy over HTTP, real MySQL auth; "
                            "controlled 20ms resolver, 2ms/4KiB upstream"
                        ),
                    )
                    all_results.append(result)
                    save(output / "stream-results.partial.json", all_results)
                    save(output / f"raw-stream-{cached}-{seed}-{concurrency}.json", rows)
                    print(
                        json.dumps(
                            {
                                k: result[k]
                                for k in (
                                    "arm",
                                    "concurrentViewers",
                                    "ttfbP99Ms",
                                    "resolutions",
                                    "statusCounts",
                                )
                            }
                        ),
                        flush=True,
                    )
            finally:
                stop.touch()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=5)
                log.close()
    return all_results


if __name__ == "__main__":
    serve()
