"""Isolated browser + native MinIO + native Redis Streams + HTTP-source acceptance.

No production credentials/database are used. Redis delivery uses real consumer
 groups, pending recovery and SQLite commits before acknowledgment.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "recommend-radio" / "backend"
sys.path[:0] = [str(BACKEND), str(ROOT / "src")]


def port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def wait_until(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception:
            pass
        time.sleep(0.25)
    raise AssertionError("Timed out waiting for isolated fixture")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ROOT / "doc" / "feed-e2e-2026-10-07.json"))
    parser.add_argument("--screenshot", default=str(ROOT / "doc" / "feed-e2e-2026-10-07.png"))
    parser.add_argument("--signed-seconds", type=int, default=7200)
    parser.add_argument("--refresh-watch-seconds", type=int, default=0)
    args = parser.parse_args()
    folder = Path(tempfile.mkdtemp(prefix="radio-feed-e2e-"))
    api_port, minio_port, console_port, source_port, redis_port = [port() for _ in range(5)]
    access, secret = "feedtest" + uuid4().hex, uuid4().hex + uuid4().hex
    os.environ.update(
        APP_DATA_DIR=str(folder / "db"),
        DATABASE_URL="",
        BUSINESS_DATABASE_URL="",
        AUTH_MODE="disabled",
        AMEM_ENABLED="false",
        RABBITMQ_ENABLED="false",
        JOB_TRANSPORT="redis_stream",
        JOB_REDIS_URL=f"redis://127.0.0.1:{redis_port}/1",
        AMEM_TRANSPORT="embedded",
        RECOMMEND_LLM_ENABLED="false",
        RECOMMEND_AUTO_DISCOVERY_ENABLED="false",
        AUTO_DREAM_ENABLED="false",
        AMEM_EMBEDDING_MODEL="",
        RECOMMEND_TEXT_EMBEDDING_BASE_URL="",
        RECOMMEND_AUDIO_EMBEDDING_BASE_URL="",
        FEED_ENABLED="true",
        MINIO_ENDPOINT=f"http://127.0.0.1:{minio_port}",
        MINIO_PUBLIC_ENDPOINT=f"http://127.0.0.1:{minio_port}",
        MINIO_ACCESS_KEY=access,
        MINIO_SECRET_KEY=secret,
        MEDIA_BUCKET="radio-media-test",
        MEDIA_WORKDIR=str(folder / "work"),
        FEED_REDIS_URL=f"redis://127.0.0.1:{redis_port}/0",
        REQUIRE_DB_MIGRATION="0",
        MEDIA_SIGNED_SECONDS=str(args.signed_seconds),
    )
    fixtures = folder / "source"
    fixtures.mkdir()
    for filename, source, options in (
        (
            "video.mp4",
            "testsrc=size=320x180:rate=30",
            ["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "120", "-an"],
        ),
        ("audio.m4a", "sine=frequency=1000:sample_rate=48000", ["-c:a", "aac", "-vn"]),
    ):
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                source,
                "-t",
                "60",
                *options,
                "-movflags",
                "+faststart",
                str(fixtures / filename),
            ],
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    source_requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            name = self.path.strip("/")
            if name not in {"video.mp4", "audio.m4a"}:
                self.send_error(404)
                return
            source_requests.append(name)
            data = (fixtures / name).read_bytes()
            start = 0
            end = len(data) - 1
            range_header = self.headers.get("Range", "")
            if range_header.startswith("bytes="):
                left, right = range_header[6:].split("-", 1)
                start = int(left or 0)
                end = min(end, int(right) if right else end)
            self.send_response(206 if range_header else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "video/mp4" if name == "video.mp4" else "audio/mp4")
            if range_header:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            try:
                for offset in range(start, end + 1, 8192):
                    self.wfile.write(data[offset : min(offset + 8192, end + 1)])
                    self.wfile.flush()
                    time.sleep(0.12)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

    upstream = ThreadingHTTPServer(("127.0.0.1", source_port), Handler)
    upstream.daemon_threads = True
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    from redis_fixture import NativeRedis

    redis_server = NativeRedis(folder / "redis-data", port=redis_port)
    minio_data = folder / "minio-data"
    minio_data.mkdir()
    minio_env = dict(os.environ, MINIO_ROOT_USER=access, MINIO_ROOT_PASSWORD=secret)
    minio_log = (folder / "minio.log").open("wb")
    minio = subprocess.Popen(
        [
            str(ROOT / "minio.exe"),
            "server",
            str(minio_data),
            "--address",
            f"127.0.0.1:{minio_port}",
            "--console-address",
            f"127.0.0.1:{console_port}",
        ],
        env=minio_env,
        stdout=minio_log,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    stop = threading.Event()
    pump_threads = []
    server = None
    report = {
        "fixtureDirectory": str(folder),
        "redis": "native Redis 8.2.2; real Streams consumers",
        "minio": "native isolated server",
        "checks": {},
        "productionDataTouched": False,
    }
    try:
        import requests

        wait_until(
            lambda: (
                requests.get(
                    f"http://127.0.0.1:{minio_port}/minio/health/live", timeout=1
                ).status_code
                == 200
            ),
            30,
        )
        from database import DEFAULT_DB_PATH, get_connection, init_db

        from database import mysql_target
        assert mysql_target() is None and DEFAULT_DB_PATH.resolve().is_relative_to(folder.resolve())
        init_db()
        with get_connection() as conn:
            for name in ("alice", "bob"):
                conn.execute(
                    "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                    (name, "2026-10-07", "2026-10-07"),
                )
        from candidate_pool import CandidatePool
        from models import Track
        from request_spec import RequestSpec

        for user in ("legacy-owner", "alice", "bob"):
            CandidatePool(str(DEFAULT_DB_PATH), user_id=user).admit(
                [
                    Track(
                        bvid=f"BV{index:010d}",
                        cid=index + 1,
                        title=f"音乐现场 Rock Concert {index}",
                        duration=60,
                        type_name="音乐",
                        tags=("音乐", "现场"),
                    )
                    for index in range(25)
                ],
                source="discovery_search",
                request_spec=RequestSpec(),
                query="音乐现场",
            )
        from feed_runtime.source import BiliMediaSource, MediaSource

        resolve_calls = []

        def resolve(self, track):
            resolve_calls.append(track.bvid)
            return MediaSource(
                f"http://127.0.0.1:{source_port}/video.mp4",
                f"http://127.0.0.1:{source_port}/audio.m4a",
                60,
                {},
            )

        BiliMediaSource.resolve = resolve
        from bili_client import BiliClient
        from models import AudioStreamInfo, VideoDetail, VideoInfo

        # Creator attribution is controlled too; the browser must not contact a
        # real account or provider while exercising the new follow footer.
        def controlled_detail(self, bvid):
            info = VideoInfo(bvid=bvid, cid=int(bvid[2:]) + 1, title="音乐现场",
                             duration=60, owner="Fixture UP", owner_mid=1001, cover="")
            return VideoDetail(info=info, pages=[info.to_track()])
        BiliClient.get_video_detail = controlled_detail

        def controlled_audio(self, bvid, cid, quality="auto"):
            return AudioStreamInfo(
                url=f"http://127.0.0.1:{source_port}/audio.m4a",
                backup_urls=[],
                duration=60,
                bitrate=128000,
                sample_rate=48000,
                channels=1,
            )

        BiliClient.get_audio_stream = controlled_audio
        from feed_runtime.services import FeedServices

        runtime = FeedServices(user_id="alice")
        runtime.storage.ensure_bucket()
        from app import oidc_auth

        original = oidc_auth.current_user
        oidc_auth.current_user = lambda raw_token, touch=True: (
            oidc_auth.identity_service.get_user(raw_token)
            if raw_token in {"alice", "bob"}
            else original(raw_token, touch=touch)
        )
        from durable_jobs import publish_pending
        from redis_stream_jobs import RedisJobBus, RedisJobWorker
        from task_app import dispatch

        bus = RedisJobBus(DEFAULT_DB_PATH, os.environ["JOB_REDIS_URL"])
        workers = [
            RedisJobWorker(bus, lanes=lanes, dispatcher=dispatch)
            for lanes in (("jobs", "events"), ("media",), ("media",))
        ]

        def publish():
            while not stop.is_set():
                publish_pending(DEFAULT_DB_PATH, bus.publish)
                stop.wait(0.1)

        for target in [publish, *(worker.run for worker in workers)]:
            thread = threading.Thread(target=target, daemon=True)
            pump_threads.append(thread)
            thread.start()
        import uvicorn
        from web_asgi import app as asgi

        server = uvicorn.Server(
            uvicorn.Config(
                asgi, host="127.0.0.1", port=api_port, log_level="warning", access_log=False
            )
        )
        threading.Thread(target=server.run, daemon=True).start()
        base = f"http://127.0.0.1:{api_port}"
        wait_until(lambda: requests.get(base + "/health/live", timeout=1).status_code == 200, 15)
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1280, "height": 900})
            context.add_cookies([{"name": oidc_auth.cookie_name, "value": "alice", "url": base}])
            page = context.new_page()
            console_errors = []
            stored_responses = []
            stored_urls = []
            page.on("pageerror", lambda error: console_errors.append(str(error)))
            page.on(
                "response",
                lambda response: (
                    stored_responses.append(response.status)
                    if response.url.startswith(f"http://127.0.0.1:{minio_port}/radio-media-test/")
                    else None
                ),
            )
            page.on('response', lambda response: stored_urls.append(response.url)
                    if response.url.startswith(f'http://127.0.0.1:{minio_port}/radio-media-test/') else None)
            page.goto(base + "/#/feed")
            page.wait_for_selector("video", timeout=20000)
            page.wait_for_function(
                "() => { const v=document.querySelector('video'); "
                "return v && !v.paused && v.currentTime > .2 }",
                timeout=45000,
            )
            with get_connection() as conn:
                cold = conn.execute(
                    "SELECT status,complete_source FROM media_import_jobs "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            report["checks"]["cold_play_before_full_acquisition"] = bool(
                cold and cold["status"] == "running" and not cold["complete_source"]
            )
            session = requests.Session()
            session.cookies.set(oidc_auth.cookie_name, "alice")
            bob = requests.Session()
            bob.cookies.set(oidc_auth.cookie_name, "bob")
            with get_connection() as conn:
                first_playback = dict(conn.execute(
                    "SELECT * FROM media_playbacks WHERE user_id='alice' ORDER BY created_at LIMIT 1"
                ).fetchone())
            shared = bob.post(base + "/api/media/playbacks", json={"contentId": first_playback["content_id"]},
                              headers={"Idempotency-Key": uuid4().hex}).json()["data"]
            assert shared["assetId"] == first_playback["asset_id"]
            with get_connection() as conn:
                assert conn.execute("SELECT COUNT(*) FROM media_import_jobs WHERE asset_id=? AND status IN ('queued','running')", (first_playback["asset_id"],)).fetchone()[0] == 1
            report["checks"]["concurrent_viewers_share_one_import"] = True
            screenshot = Path(args.screenshot).resolve()
            screenshot.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(screenshot))
            page.get_by_role("button", name="喜欢", exact=False).click()

            def scalar(sql):
                with get_connection() as conn:
                    return conn.execute(sql).fetchone()[0]

            wait_until(lambda: scalar("SELECT COALESCE(SUM(likes),0) FROM content_counters") > 0, 5)
            report["checks"]["reaction_outbox_projection"] = True
            page.get_by_role("button", name="喜欢 1", exact=True).wait_for(timeout=10000)
            report["checks"]["reaction_count_visible_without_reload"] = True
            page.get_by_role("button", name="下一个", exact=False).click()
            bob.post(base + f"/api/media/playbacks/{shared['playbackId']}/heartbeat", json={}).raise_for_status()
            page.wait_for_timeout(4000)
            with get_connection() as conn:
                still_shared = conn.execute("SELECT status FROM media_import_jobs WHERE import_id=?", (first_playback["import_id"],)).fetchone()[0]
            assert still_shared == "running", still_shared
            report["checks"]["one_viewer_leaves_keeps_shared_import"] = True
            bob.delete(base + f"/api/media/playbacks/{shared['playbackId']}").raise_for_status()
            wait_until(
                lambda: (
                    scalar("SELECT COUNT(*) FROM media_import_jobs WHERE status='cancelled'") > 0
                ),
                15,
            )
            report["checks"]["swipe_cancels_unfinished_import"] = True
            def first_progress():
                with get_connection() as conn:
                    row = conn.execute("SELECT watch_ms FROM feed_playback_progress WHERE playback_id=?", (first_playback["playback_id"],)).fetchone()
                    return row and row[0] > 0
            wait_until(first_progress, 10)
            with get_connection() as conn:
                progress = conn.execute("SELECT watch_ms FROM feed_playback_progress WHERE playback_id=?", (first_playback["playback_id"],)).fetchone()[0]
                item_id = conn.execute("SELECT item_id FROM feed_events WHERE event_type='play_started' AND json_extract(payload_json,'$.playbackId')=?", (first_playback["playback_id"],)).fetchone()[0]
            late = {"eventId": uuid4().hex, "itemId": item_id, "type": "watch_progress", "playbackId": first_playback["playback_id"], "watchMs": progress}
            for _ in range(2):
                session.post(base + "/api/feed/events", json={"events": [late]}).raise_for_status()
            with get_connection() as conn:
                assert conn.execute("SELECT watch_ms FROM feed_playback_progress WHERE playback_id=?", (first_playback["playback_id"],)).fetchone()[0] == progress
                assert conn.execute("SELECT COUNT(*) FROM feed_events WHERE event_id=?", (late["eventId"],)).fetchone()[0] == 1
                assert conn.execute("SELECT last_exposed_at FROM feed_user_content WHERE user_id='alice' AND content_id=?", (first_playback["content_id"],)).fetchone()[0] > 0
            report["checks"]["final_watch_after_release_idempotent_projection"] = True
            report["checks"]["real_exposure_projects_seen_state"] = True
            wait_until(
                lambda: scalar("SELECT COUNT(*) FROM media_assets WHERE status='ready'") > 0, 60
            )
            report["checks"]["minio_complete_asset"] = True
            response = session.post(
                base + "/api/feed/sessions",
                json={"mode": "new"},
                headers={"Idempotency-Key": uuid4().hex},
            ).json()["data"]
            repeat = session.get(base + f"/api/feed/sessions/{response['sessionId']}/items").json()[
                "data"
            ]
            assert [i["itemId"] for i in response["items"]] == [
                i["itemId"] for i in repeat["items"]
            ]
            report["checks"]["fixed_page_replay"] = True
            with get_connection() as conn:
                ready = conn.execute(
                    "SELECT content_id FROM media_assets WHERE status='ready' LIMIT 1"
                ).fetchone()[0]
            before = len(resolve_calls)
            descriptor = session.post(
                base + "/api/media/playbacks",
                json={"contentId": ready},
                headers={"Idempotency-Key": uuid4().hex},
            ).json()["data"]
            playlist = session.get(base + descriptor["manifestUrl"]).text
            saved_signed_segment = next(line for line in playlist.splitlines() if line.startswith('http') and '.m4s?' in line)
            assert (
                descriptor["mode"] == "stored"
                and "/radio-media-test/" in playlist
                and len(resolve_calls) == before
            )
            report["checks"]["stored_replay_zero_upstream_calls"] = True
            assert (
                bob.get(base + f"/api/media/playbacks/{descriptor['playbackId']}").status_code
                == 404
            )
            report["checks"]["cross_user_playback_denied"] = True
            page.get_by_role("button", name="新发现", exact=True).click()
            page.wait_for_selector("video", timeout=10000)
            page.wait_for_function(
                "() => { const v=document.querySelector('video'); "
                "return v && !v.paused && v.currentTime > .2 }",
                timeout=20000,
            )
            report["checks"]["feed_mode_switch"] = True
            # Seen-content suppression intentionally removes this asset from new pages;
            # history/share replay opens an authorized singleton page instead.
            page.goto(base + "/#/feed?content=" + ready)
            page.wait_for_function("document.querySelector('video')?.currentTime > .2", timeout=20000)
            report["checks"]["history_share_reopens_seen_content"] = True
            if args.refresh_watch_seconds:
                with get_connection() as conn:
                    saved_playback = conn.execute("SELECT playback_id FROM media_playbacks WHERE content_id=? AND user_id='alice' ORDER BY created_at DESC LIMIT 1", (ready,)).fetchone()[0]
                    playback_count = conn.execute("SELECT COUNT(*) FROM media_playbacks WHERE content_id=? AND user_id='alice'", (ready,)).fetchone()[0]
                positions, reload_samples = [], 0
                for _ in range(args.refresh_watch_seconds):
                    page.wait_for_timeout(1000)
                    sample = page.locator('video').evaluate('v => ({position:v.currentTime,ready:v.readyState,paused:v.paused,seeking:v.seeking})')
                    if sample['ready'] >= 2 and not sample['paused'] and not sample['seeking']:
                        positions.append(sample['position'])
                    else:
                        reload_samples += 1
                expired_status = requests.get(saved_signed_segment, timeout=10).status_code
                assert expired_status == 403, expired_status
                assert len(positions) >= args.refresh_watch_seconds * .8, (positions, reload_samples)
                assert positions[-1] > 24, positions
                assert all(later >= earlier - .15 for earlier, later in zip(positions, positions[1:])), positions
                with get_connection() as conn:
                    assert conn.execute("SELECT COUNT(*) FROM media_playbacks WHERE content_id=? AND user_id='alice'", (ready,)).fetchone()[0] == playback_count
                    assert conn.execute("SELECT active FROM media_playbacks WHERE playback_id=?", (saved_playback,)).fetchone()[0] == 1
                assert len(set(stored_urls)) > 12
                report['checks']['real_minio_signature_expired_403'] = True
                report['checks']['signed_refresh_beyond_buffer_same_playback'] = True
                report['signedRefresh'] = {'ttlSeconds': args.signed_seconds, 'watchSeconds': args.refresh_watch_seconds, 'positionAtEnd': positions[-1], 'expiredUrlStatus': expired_status, 'uniqueObjectUrls': len(set(stored_urls)), 'playingSamples': len(positions), 'reloadSamples': reload_samples}
            report["checks"]["browser_plays_presigned_minio_segments"] = bool(
                stored_responses
            ) and all(code in (200, 206) for code in stored_responses)
            with get_connection() as conn:
                current = json.loads(conn.execute("SELECT metadata_json FROM feed_content WHERE content_id=?", (ready,)).fetchone()[0])
            page.evaluate(
                "track => document.querySelector('#app').__vue_app__.config.globalProperties"
                ".$pinia._s.get('player').playTrack(track)",
                current,
            )
            page.wait_for_function(
                "() => document.querySelector('#app').__vue_app__.config.globalProperties"
                ".$pinia._s.get('player').isPlaying && document.querySelector('video').paused",
                timeout=15000,
            )
            page.get_by_role("button", name="播放", exact=True).click()
            page.wait_for_function(
                "() => { const player=document.querySelector('#app').__vue_app__"
                ".config.globalProperties"
                ".$pinia._s.get('player'); const v=document.querySelector('video'); "
                "return !player.isPlaying && v && !v.paused && v.currentTime>.2 }",
                timeout=15000,
            )
            report["checks"]["audio_video_mutual_exclusion"] = True
            report["minioHttpStatuses"] = stored_responses
            report["browserErrors"] = console_errors
            browser.close()
        with get_connection() as conn:
            report["events"] = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT event_type,COUNT(*) FROM feed_events GROUP BY event_type"
                )
            }
            report["assets"] = [
                dict(row)
                for row in conn.execute(
                    "SELECT status,COUNT(*) AS count FROM media_assets GROUP BY status"
                )
            ]
        report["resolveCalls"] = len(resolve_calls)
        report["passed"] = all(report["checks"].values()) and not report["browserErrors"]
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(
            json.dumps(
                {"passed": report["passed"], "checks": report["checks"], "output": str(output)},
                ensure_ascii=False,
            )
        )
        if not report["passed"]:
            raise SystemExit(1)
    except BaseException as exc:
        report["passed"] = False
        report["failureType"] = type(exc).__name__
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise
    finally:
        stop.set()
        for worker in locals().get("workers", []):
            worker.stop.set()
        if "runtime" in locals():
            with get_connection() as conn:
                conn.execute("UPDATE media_playbacks SET active=0")
            for thread in pump_threads:
                thread.join(timeout=20)
        if server:
            server.should_exit = True
        upstream.shutdown()
        redis_server.close()
        minio.terminate()
        try:
            minio.wait(timeout=10)
        except subprocess.TimeoutExpired:
            minio.kill()
            minio.wait(timeout=5)
        minio_log.close()


if __name__ == "__main__":
    main()
