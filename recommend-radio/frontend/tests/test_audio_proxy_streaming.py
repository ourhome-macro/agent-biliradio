from __future__ import annotations

import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import docker_static_proxy  # noqa: E402


def test_audio_proxy_forwards_first_chunk_before_upstream_finishes(monkeypatch):
    release = threading.Event()

    class SlowAudio(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(206)
            self.send_header("Content-Type", "audio/mp4")
            self.send_header("Content-Length", "16384")
            self.send_header("Content-Range", "bytes 0-16383/16384")
            self.end_headers()
            self.wfile.write(b"a" * 8192)
            self.wfile.flush()
            release.wait(2)
            self.wfile.write(b"b" * 8192)
            self.wfile.flush()

        def log_message(self, *_args):
            pass

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), SlowAudio)
    proxy = ThreadingHTTPServer(("127.0.0.1", 0), docker_static_proxy.StaticProxyHandler)
    monkeypatch.setattr(
        docker_static_proxy, "BACKEND_URL", f"http://127.0.0.1:{upstream.server_port}"
    )
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    upstream_thread.start()
    proxy_thread.start()
    try:
        started = time.perf_counter()
        with requests.get(
            f"http://127.0.0.1:{proxy.server_port}/api/tracks/BV1234567890/1/stream",
            headers={"Range": "bytes=0-16383"},
            stream=True,
            timeout=5,
        ) as response:
            first = next(response.iter_content(chunk_size=8192))
            first_chunk_seconds = time.perf_counter() - started
            assert response.status_code == 206
            assert response.headers["Content-Range"] == "bytes 0-16383/16384"
            assert first == b"a" * 8192
            assert first_chunk_seconds < 1
            release.set()
            assert b"".join(response.iter_content(chunk_size=8192)) == b"b" * 8192
    finally:
        release.set()
        proxy.shutdown()
        upstream.shutdown()
        proxy.server_close()
        upstream.server_close()
