"""Public Bilibili -> real media worker -> isolated native MinIO smoke test.

Never uses account cookies or existing MinIO data. Reports upstream failures without
recording signed URLs, cookies, credentials or exception messages.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from uuid import uuid4

from run_feed_e2e import ROOT, port, wait_until


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bvid", required=True)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--transport", choices=("redis_stream", "direct"), default="redis_stream")
    parser.add_argument("--output", default=str(ROOT / "doc" / "bili-media-smoke-2026-10-07.json"))
    args = parser.parse_args()
    folder = Path(tempfile.mkdtemp(prefix="radio-bili-smoke-"))
    api_port, console_port = port(), port()
    access, secret = "feedtest" + uuid4().hex, uuid4().hex + uuid4().hex
    os.environ.update(
        APP_DATA_DIR=str(folder / "db"),
        AMEM_ENABLED="false",
        RABBITMQ_ENABLED="false",
        JOB_TRANSPORT="redis_stream" if args.transport == "redis_stream" else "disabled",
        FEED_ENABLED="true",
        MEDIA_WORKDIR=str(folder / "work"),
        MINIO_ENDPOINT=f"http://127.0.0.1:{api_port}",
        MINIO_PUBLIC_ENDPOINT=f"http://127.0.0.1:{api_port}",
        MINIO_ACCESS_KEY=access,
        MINIO_SECRET_KEY=secret,
        MEDIA_BUCKET="radio-media-smoke",
        REQUIRE_DB_MIGRATION="0",
    )
    report = {
        "schemaVersion": "radio.media.smoke.v1",
        "bvid": args.bvid,
        "scope": "public",
        "fixtureDirectory": str(folder),
        "productionDataTouched": False,
        "passed": False,
    }
    process = None
    redis_server = None
    started = time.monotonic()
    stop = threading.Event()
    worker_thread = None
    source = None
    runtime = None
    playback = None
    log = (folder / "minio.log").open("wb")
    try:
        from feed_runtime.source import BiliMediaSource

        source = BiliMediaSource(
            user_id="legacy-owner", scope="public", db_path=folder / "db" / "radio.sqlite3"
        )
        report["stage"] = "metadata"
        info = source.client.get_video_info(args.bvid)
        from models import Track

        track = Track(
            bvid=args.bvid, cid=info.cid, title=info.title, duration=info.duration, type_name="音乐"
        )
        source.close()
        source = None
        process = subprocess.Popen(
            [
                str(ROOT / "minio.exe"),
                "server",
                str(folder / "minio-data"),
                "--address",
                f"127.0.0.1:{api_port}",
                "--console-address",
                f"127.0.0.1:{console_port}",
            ],
            env=dict(os.environ, MINIO_ROOT_USER=access, MINIO_ROOT_PASSWORD=secret),
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        import requests

        wait_until(
            lambda: (
                requests.get(
                    f"http://127.0.0.1:{api_port}/minio/health/live", timeout=1
                ).status_code
                == 200
            ),
            30,
        )
        from database import get_connection
        from feed_runtime.services import FeedServices
        from feed_runtime.worker import MediaWorker

        runtime = FeedServices(user_id="legacy-owner")
        runtime.storage.ensure_bucket()
        content = runtime.repo.admit(track, user_id="legacy-owner")
        if args.transport == "redis_stream":
            from redis_fixture import NativeRedis

            redis_server = NativeRedis(folder / "redis-data")
            os.environ["JOB_REDIS_URL"] = redis_server.url
        playback = runtime.media.create("legacy-owner", content, "smoke")
        with get_connection(runtime.repo.db_path) as conn:
            import_id = conn.execute("SELECT import_id FROM media_playbacks").fetchone()[0]
        report["stage"] = "acquisition"
        outcome = {}

        def run_worker():
            try:
                if args.transport == "redis_stream":
                    from durable_jobs import publish_pending
                    from redis_stream_jobs import RedisJobBus, RedisJobWorker

                    bus = RedisJobBus(runtime.repo.db_path, redis_server.url)
                    bus.ensure_groups(("media",))
                    publish_pending(runtime.repo.db_path, bus.publish)
                    delivery = bus.read(("media",), "smoke")[0]
                    outcome["result"] = RedisJobWorker(bus, lanes=("media",)).process(
                        *delivery, "smoke"
                    )
                else:
                    outcome["result"] = MediaWorker(
                        runtime.repo, runtime.config, runtime.storage
                    ).run(import_id)
            except Exception as error:
                outcome["errorType"] = type(error).__name__

        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()
        deadline = time.monotonic() + args.timeout
        first = False
        while worker_thread.is_alive() and time.monotonic() < deadline:
            descriptor = runtime.media.heartbeat(playback["playbackId"], "legacy-owner")
            if descriptor["status"] in {"streaming", "ready"} and not first:
                first = True
                report["firstFragmentSeconds"] = round(time.monotonic() - started, 3)
            stop.wait(2)
        if worker_thread.is_alive():
            runtime.media.release(playback["playbackId"], "legacy-owner")
            worker_thread.join(timeout=30)
            raise TimeoutError()
        if outcome.get("errorType"):
            report["failureType"] = outcome["errorType"]
        else:
            descriptor = runtime.media.descriptor(playback["playbackId"], "legacy-owner")
            playlist = runtime.media.stream_file(
                playback["playbackId"], "legacy-owner", "index.m3u8"
            )["text"]
            again = runtime.media.create("legacy-owner", content, "replay")
            report.update(
                stage="stored_replay",
                passed=descriptor["status"] == "ready"
                and again["mode"] == "stored"
                and "#EXT-X-ENDLIST" in playlist,
                firstFragmentObserved=first,
            )
            report["mediaBytes"] = runtime.repo.import_job(import_id)["bytes"]
            report["transport"] = args.transport
            report["heavyTaskProcessIsolation"] = args.transport == "redis_stream"
    except Exception as error:
        report["failureType"] = type(error).__name__
        status = getattr(getattr(error, "response", None), "status_code", None)
        if status is not None:
            report["httpStatus"] = status
    finally:
        stop.set()
        if source:
            source.close()
        if runtime and playback:
            runtime.media.release(playback["playbackId"], "legacy-owner")
        if worker_thread:
            worker_thread.join(timeout=30)
        if redis_server:
            redis_server.close()
        if process:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log.close()
        report["durationSeconds"] = round(time.monotonic() - started, 3)
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
