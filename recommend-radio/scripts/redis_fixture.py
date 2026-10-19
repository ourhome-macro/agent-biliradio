"""Own a native Redis test process without touching supplied dumps or services."""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

from redis import Redis

ROOT = Path(__file__).resolve().parents[2]


def redis_binary():
    configured = os.getenv("REDIS_SERVER_PATH")
    if configured:
        return Path(configured).resolve()
    candidates = list(ROOT.glob("Redis-*/redis-server.exe"))
    if not candidates:
        raise FileNotFoundError("Set REDIS_SERVER_PATH to the supplied native server")
    return candidates[0].resolve()


class NativeRedis:
    def __init__(self, directory, *, port=None):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        if port is None:
            with socket.socket() as stream:
                stream.bind(("127.0.0.1", 0))
                port = stream.getsockname()[1]
        self.port = port
        self.url = f"redis://127.0.0.1:{port}/0"
        self.log = (self.directory / "redis.log").open("wb")
        self.process = subprocess.Popen(
            [
                str(redis_binary()),
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--dir",
                str(self.directory),
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--maxmemory",
                "512mb",
                "--maxmemory-policy",
                "noeviction",
                "--protected-mode",
                "yes",
            ],
            cwd=redis_binary().parent,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.client = Redis.from_url(
            self.url, decode_responses=True, socket_timeout=1, socket_connect_timeout=1
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.close()
                raise RuntimeError("Native Redis stopped before becoming ready")
            try:
                if self.client.ping():
                    return
            except Exception:
                time.sleep(0.1)
        self.close()
        raise TimeoutError("Native Redis did not become ready")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.client.close()
        self.log.close()
