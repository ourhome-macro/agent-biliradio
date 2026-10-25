"""Isolated direct-upload / audio processing and Bilibili no-storage regression."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def upload_browser(directory, origin, user, secret, wav):
    import requests
    import uvicorn
    from database import DEFAULT_DB_PATH, get_connection
    from durable_jobs import claim, finish
    from feed_runtime.config import FeedConfig
    from feed_runtime.storage import MinIOStorage
    from playwright.sync_api import sync_playwright
    from redis_fixture import NativeRedis
    from ugc.audio_worker import AudioWorker

    redis = NativeRedis(directory / "browser-redis")
    os.environ.update(
        MINIO_ENDPOINT=origin,
        MINIO_PUBLIC_ENDPOINT=origin,
        MINIO_ACCESS_KEY=user,
        MINIO_SECRET_KEY=secret,
        MEDIA_BUCKET="test-audio",
        FEED_REDIS_URL=redis.url,
        FEED_ENABLED="true",
        UGC_ENABLED="true",
        AMEM_ENABLED="false",
        AUTO_DREAM_ENABLED="false",
        RADIO_OUTBOX_EMBEDDED="false",
        RECOMMEND_LLM_ENABLED="false",
        CONTENT_SEARCH_ENABLED="false",
        JOB_TRANSPORT="disabled",
    )
    from app import oidc_auth
    from web_asgi import app

    assert Path(DEFAULT_DB_PATH).resolve().is_relative_to(directory.resolve())
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO app_users(id,display_name,created_at,updated_at) VALUES "
            "('media-browser-owner','Fixture','now','now')"
        )
    current_user = oidc_auth.current_user
    oidc_auth.current_user = lambda raw, touch=True: (
        oidc_auth.identity_service.get_user(raw)
        if raw == "media-browser-owner"
        else current_user(raw, touch=touch)
    )
    config = FeedConfig.from_env()
    storage = MinIOStorage(config)
    stop = threading.Event()

    def jobs():
        while not stop.wait(0.1):
            with get_connection() as conn:
                row = conn.execute(
                    "SELECT job_id FROM durable_jobs WHERE kind='ugc_audio' "
                    "AND status='queued' ORDER BY id LIMIT 1"
                ).fetchone()
                job = claim(conn, row[0]) if row else None
            if job:
                try:
                    result = AudioWorker(DEFAULT_DB_PATH, config, storage).run(
                        json.loads(job["payload_json"])["upload_id"], job
                    )
                    with get_connection() as conn:
                        finish(conn, job, result=result)
                except Exception as error:
                    with get_connection() as conn:
                        finish(conn, job, error=error)

    worker = threading.Thread(target=jobs, daemon=True)
    worker.start()
    address = port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=address, log_level="warning", access_log=False)
    )
    serving = threading.Thread(target=server.run, daemon=True)
    serving.start()
    base = f"http://127.0.0.1:{address}"
    try:
        for _ in range(100):
            try:
                if requests.get(base + "/health/live", timeout=0.2).status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(0.1)
        with sync_playwright() as runner:
            browser = runner.chromium.launch(headless=True)
            try:
                context = browser.new_context()
                context.add_cookies(
                    [{"name": oidc_auth.cookie_name, "value": "media-browser-owner", "url": base}]
                )
                tab = context.new_page()
                errors = []
                tab.on("pageerror", lambda error: errors.append(str(error)))
                tab.goto(base + "/#/studio")
                tab.get_by_label("内容形式", exact=True).select_option("audio")
                tab.get_by_label("标题", exact=True).fill("浏览器直传音频")
                tab.get_by_label("可见范围", exact=True).select_option("public")
                tab.locator("input[type=file]").set_input_files(wav)
                tab.get_by_text("音频已就绪，可以发布", exact=True).wait_for(timeout=30000)
                tab.get_by_role("button", name="发布", exact=True).click()
                tab.get_by_text("已发布", exact=True).wait_for(timeout=10000)
                tab.get_by_role("link", name="查看详情", exact=True).click()
                tab.locator("video").wait_for(timeout=10000)
                tab.wait_for_function(
                    'document.querySelector("video").readyState>=2', timeout=15000
                )
                tab.evaluate(
                    'document.querySelector("video").muted=true;document.querySelector("video").play()'
                )
                tab.wait_for_function(
                    'document.querySelector("video").currentTime>0.25', timeout=15000
                )
                assert not errors, errors
                return {
                    "realFrontendAudioDirectUpload": True,
                    "realFrontendAsyncPublish": True,
                    "realFrontendAudioPlayback": True,
                    "frontendJavascriptErrors": 0,
                }
            finally:
                browser.close()
    finally:
        server.should_exit = True
        serving.join(timeout=5)
        stop.set()
        worker.join(timeout=5)
        oidc_auth.current_user = current_user
        redis.close()


def mysql_upgrade(directory):
    from types import SimpleNamespace

    import sqlalchemy as sa
    from database import get_connection
    from feed_runtime.config import FeedConfig
    from feed_runtime.repository import FeedRepository
    from mysql_fixture import NativeMySQL
    from mysql_storage.connection import engine_for
    from mysql_storage.schema import canonical_schema, metadata_from_catalog
    from mysql_storage.upgrade import upgrade_mysql
    from ugc.service import UGCService
    from ugc.uploads import AudioUploads

    # Always own a fresh server, even when a separate integration endpoint is configured.
    os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
    server = NativeMySQL(directory / "mysql")
    engine = engine_for(server.url)
    try:
        catalog = canonical_schema(revision="radio_009")
        metadata = metadata_from_catalog(catalog)
        metadata.create_all(engine)
        with engine.begin() as conn:
            for table in metadata.sorted_tables:
                if table.name in catalog and catalog[table.name]["rows"]:
                    conn.execute(sa.insert(table), catalog[table.name]["rows"])
            conn.exec_driver_sql(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES "
                "('migration-probe','2026-10-25','2026-10-25')"
            )
        assert upgrade_mysql(server.url)["changed"]
        assert not upgrade_mysql(server.url)["changed"]
        repo = FeedRepository(server.url)
        service = UGCService(repo.db_path)
        storage = SimpleNamespace(
            config=FeedConfig(),
            ensure_bucket=lambda: None,
            upload_policy=lambda *a: {"url": "https://fixture.invalid", "fields": {}},
        )
        post = service.command(
            "legacy-owner", "create", {"kind": "audio", "title": "MySQL audio"}, "draft"
        )
        uploads = AudioUploads(repo.db_path, storage)
        session = uploads.initiate(
            "legacy-owner",
            post["contentId"],
            {"bytes": 1, "sha256": "a" * 64, "contentType": "audio/wav"},
            "upload",
        )
        service.command(
            "legacy-owner",
            "edit",
            {"audioUploadId": session["uploadId"], "expectedVersion": post["version"]},
            "attach",
            post["contentId"],
        )
        assert uploads.complete("legacy-owner", session["uploadId"])["status"] == "queued"
        with get_connection(repo.db_path) as conn:
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM app_users WHERE id='migration-probe'"
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute("SELECT COUNT(*) FROM durable_jobs WHERE kind='ugc_audio'").fetchone()[
                    0
                ]
                == 1
            )
        return {
            "nativeMySQL009To010PreservedData": True,
            "nativeMySQLUpgradeIdempotent": True,
            "nativeMySQLAudioUploadQueue": True,
        }
    finally:
        engine.dispose()
        server.close()


def proxy_playback(directory):
    from feed_runtime import bili_proxy
    from feed_runtime.bili_proxy import ProxyMediaService
    from feed_runtime.config import FeedConfig
    from feed_runtime.repository import FeedRepository
    from feed_runtime.source import MediaSource, validate_media_url
    from flask import Flask, Response, request, send_file
    from models import Track
    from playwright.sync_api import sync_playwright
    from werkzeug.serving import make_server

    ranges, files, indexes = [], {}, {}
    for kind, source in [
        ("video", "testsrc2=size=160x90:rate=24"),
        ("audio", "sine=frequency=440"),
    ]:
        path = directory / (kind + ".mp4")
        codec = (
            ["-c:v", "libx264", "-g", "24", "-an"] if kind == "video" else ["-c:a", "aac", "-vn"]
        )
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                source,
                "-t",
                "4",
                *codec,
                "-movflags",
                "+dash+global_sidx",
                str(path),
            ],
            check=True,
            timeout=20,
            capture_output=True,
        )
        data = path.read_bytes()
        cursor = 0
        while cursor + 8 <= len(data):
            size = int.from_bytes(data[cursor : cursor + 4], "big")
            if size < 8:
                raise ValueError("Invalid fixture MP4 box")
            if data[cursor + 4 : cursor + 8] == b"sidx":
                indexes[kind] = (f"0-{cursor - 1}", f"{cursor}-{cursor + size - 1}")
                break
            cursor += size
        assert kind in indexes
        files[kind] = data

    class Upstream(BaseHTTPRequestHandler):
        def do_GET(self):
            kind = self.path.split("/")[-1].split(".")[0]
            body = files[kind]
            header = self.headers.get("Range")
            assert header, "proxy unexpectedly downloaded a full upstream file"
            a, b = header.removeprefix("bytes=").split("-")
            start, end = int(a), int(b) if b else len(body) - 1
            ranges.append((kind, start, end))
            self.send_response(206)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            self.send_header("Content-Type", "video/mp4" if kind == "video" else "audio/mp4")
            self.end_headers()
            self.wfile.write(body[start : end + 1])

        def log_message(self, *args):
            pass

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{upstream.server_port}"

    class Source:
        def __init__(self, **kwargs):
            pass

        def resolve(self, track):
            return MediaSource(
                origin + "/video.mp4",
                origin + "/audio.mp4",
                4,
                {},
                video_codec="avc1.64001e",
                audio_codec="mp4a.40.2",
                video_init=indexes["video"][0],
                video_index=indexes["video"][1],
                audio_init=indexes["audio"][0],
                audio_index=indexes["audio"][1],
            )

        def close(self):
            pass

    original = bili_proxy.validate_media_url
    bili_proxy.validate_media_url = lambda value: validate_media_url(value, allow_local=True)
    repo = FeedRepository(directory / "proxy.sqlite3")
    content = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live concert", duration=12),
        user_id="legacy-owner",
    )
    service = ProxyMediaService(repo, FeedConfig(), None, source_factory=Source)
    playback = service.create("legacy-owner", content, "browser")
    app = Flask("proxy-fixture")

    @app.get("/hls.js")
    def hls():
        return send_file(ROOT / "recommend-radio/frontend/node_modules/hls.js/dist/hls.min.js")

    @app.get("/")
    def page():
        return (
            '<video id="video" muted controls></video><script src="/hls.js"></script><script>'
            'window.errors=[]; const v=document.getElementById("video");window.h=new Hls();'
            "h.on(Hls.Events.ERROR,(_,d)=>{if(d.fatal)errors.push(d.details)});"
            f"h.loadSource({json.dumps(playback['manifestUrl'])});h.attachMedia(v);"
            "h.on(Hls.Events.MANIFEST_PARSED,()=>v.play());</script>"
        )

    @app.route("/api/media/streams/<playback_id>/<filename>", methods=["GET", "HEAD"])
    def media(playback_id, filename):
        result = service.stream_file(
            playback_id,
            "legacy-owner",
            filename,
            expected_generation=request.args.get("v"),
            range_header=request.headers.get("Range"),
            method=request.method,
        )
        if "text" in result:
            return Response(result["text"], content_type=result["mime"])
        response = result["upstream"]

        def generate():
            try:
                yield from response.iter_content(65536)
            finally:
                result["close"]()

        return Response(
            generate(),
            status=response.status_code,
            content_type=result["mime"],
            headers={
                k: v
                for k, v in response.headers.items()
                if k.lower() in {"content-length", "content-range", "accept-ranges"}
            },
        )

    http = make_server("127.0.0.1", 0, app, threaded=True)
    serving = threading.Thread(target=http.serve_forever, daemon=True)
    serving.start()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                tab = browser.new_page()
                tab.goto(f"http://127.0.0.1:{http.server_port}")
                tab.wait_for_function("video.currentTime > 1 || errors.length", timeout=25000)
                assert tab.evaluate("errors") == [], tab.evaluate("errors")
                tab.evaluate("video.currentTime=2.5")
                tab.wait_for_function(
                    "video.currentTime >= 2.5 && video.readyState >= 2", timeout=10000
                )
            finally:
                browser.close()
        assert len(ranges) >= 6
        return {
            "realHlsByteRangePlayback": True,
            "realHlsSeek": True,
            "upstreamRangeRequests": len(ranges),
            "biliMinioWrites": 0,
        }
    finally:
        http.shutdown()
        http.server_close()
        upstream.shutdown()
        upstream.server_close()
        bili_proxy.validate_media_url = original


def port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def native(directory):
    import requests
    from database import get_connection
    from durable_jobs import claim
    from feed_runtime.config import FeedConfig
    from feed_runtime.repository import FeedRepository
    from feed_runtime.storage import MinIOStorage
    from ugc.audio_worker import AudioWorker
    from ugc.service import UGCService
    from ugc.uploads import AudioUploads

    binary = shutil.which("minio") or str(ROOT / "minio.exe")
    address, console = port(), port()
    user, secret = "test-" + uuid4().hex[:12], uuid4().hex + uuid4().hex
    data = directory / "minio-data"
    data.mkdir()
    log = (directory / "minio.log").open("wb")
    process = subprocess.Popen(
        [
            binary,
            "server",
            str(data),
            "--address",
            f"127.0.0.1:{address}",
            "--console-address",
            f"127.0.0.1:{console}",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, "MINIO_ROOT_USER": user, "MINIO_ROOT_PASSWORD": secret},
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    try:
        deadline = time.monotonic() + 20
        origin = f"http://127.0.0.1:{address}"
        while True:
            if process.poll() is not None:
                raise RuntimeError("Isolated MinIO failed to start")
            try:
                if requests.get(origin + "/minio/health/live", timeout=0.3).status_code == 200:
                    break
            except requests.RequestException:
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("Isolated MinIO did not become ready")
            time.sleep(0.1)
        config = replace(
            FeedConfig(),
            minio_endpoint=f"127.0.0.1:{address}",
            minio_public_endpoint=f"127.0.0.1:{address}",
            minio_access_key=user,
            minio_secret_key=secret,
            bucket="test-audio",
            workdir=directory / "work",
        )
        storage = MinIOStorage(config)
        repo = FeedRepository(directory / "business.sqlite3")
        service, uploads = UGCService(repo.db_path), AudioUploads(repo.db_path, storage)
        post = service.command(
            "legacy-owner",
            "create",
            {"kind": "audio", "title": "Fixture audio", "visibility": "public"},
            "draft",
        )
        wav = directory / "sample.wav"
        subprocess.run(
            [
                config.ffmpeg,
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440",
                "-t",
                "2",
                str(wav),
            ],
            check=True,
            timeout=10,
            capture_output=True,
        )
        body = wav.read_bytes()
        session = uploads.initiate(
            "legacy-owner",
            post["contentId"],
            {
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "contentType": "audio/wav",
            },
            "native-upload",
        )
        fields = session["upload"]["fields"]
        response = requests.post(
            session["upload"]["url"],
            data=fields,
            files={"file": ("test.wav", body + b"x", "audio/wav")},
            timeout=10,
        )
        assert response.status_code >= 400, "POST policy accepted the wrong byte count"
        response = requests.post(
            session["upload"]["url"],
            data=fields,
            files={"file": ("test.wav", body, "audio/wav")},
            timeout=10,
        )
        assert response.status_code in {200, 201, 204}, response.text[:200]
        post = service.command(
            "legacy-owner",
            "edit",
            {"audioUploadId": session["uploadId"], "expectedVersion": post["version"]},
            "attach",
            post["contentId"],
        )
        uploads.complete("legacy-owner", session["uploadId"])
        with get_connection(repo.db_path) as conn:
            job = claim(conn, "ugc-audio:" + session["uploadId"])
        worker = AudioWorker(repo.db_path, config, storage)
        worker.run(session["uploadId"], job)
        assert uploads.get("legacy-owner", session["uploadId"])["status"] == "ready"
        # A still-live source signature must not mutate the already-verified asset.
        response = requests.post(
            session["upload"]["url"],
            data=fields,
            files={"file": ("bad.wav", b"x" * len(body), "audio/wav")},
            timeout=10,
        )
        assert response.status_code in {200, 201, 204}
        fresh = service.detail("legacy-owner", post["contentId"])
        published = service.command(
            "legacy-owner",
            "publish",
            {"expectedVersion": fresh["version"]},
            "publish",
            post["contentId"],
        )
        assert published["status"] == "published"
        with get_connection(repo.db_path) as conn:
            asset = conn.execute(
                "SELECT manifest_json FROM media_assets WHERE asset_id=?", (session["assetId"],)
            ).fetchone()
        manifest = json.loads(asset[0])
        entry = manifest["files"]["audio.m4a"]
        response = requests.get(storage.presign(entry["key"]), timeout=10)
        assert (
            response.status_code == 200
            and hashlib.sha256(response.content).hexdigest() == entry["sha256"]
        )
        return {
            "nativeMinioExactSizePolicy": True,
            "nativePresignedDirectUpload": True,
            "nativeFFmpegAudioValidation": True,
            "sourceOverwriteCannotMutatePublishedAsset": True,
            "publicationAndOutbox": True,
            **upload_browser(directory, origin, user, secret, wav),
        }
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--mysql-only", action="store_true")
    args = parser.parse_args()
    output = ROOT / "artifacts/quality"
    output.mkdir(parents=True, exist_ok=True)
    if args.child:
        sys.path.insert(0, str(ROOT / "recommend-radio/backend"))
        sys.path.insert(0, str(ROOT / "recommend-radio/scripts"))
        if args.mysql_only:
            report = mysql_upgrade(args.child)
            (output / "media-mysql-upgrade.json").write_text(
                json.dumps(report, indent=2), encoding="utf-8"
            )
            print(json.dumps(report))
            return
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "--tb=short",
                "recommend-radio/backend/tests/test_audio_uploads_and_proxy.py",
                f"--basetemp={args.child / 'pytest'}",
                f"--junitxml={output / 'media-paths.xml'}",
            ],
            check=True,
            cwd=ROOT,
        )
        report = {**native(args.child), **proxy_playback(args.child), **mysql_upgrade(args.child)}
        (output / "media-paths.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report))
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(prefix="media-paths-", dir=output) as directory:
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--child",
                directory,
                *(["--mysql-only"] if args.mysql_only else []),
            ],
            check=True,
            cwd=ROOT,
            env=isolated_environment(directory),
        )


if __name__ == "__main__":
    main()
