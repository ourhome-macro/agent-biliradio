from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class FeedConfig:
    enabled: bool = False
    redis_url: str = "redis://127.0.0.1:6379/0"
    minio_endpoint: str = "127.0.0.1:9000"
    minio_public_endpoint: str = "127.0.0.1:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_secure: bool = False
    minio_public_secure: bool = False
    bucket: str = "radio-media"
    workdir: Path = Path("data/media-work")
    ffmpeg: str = "ffmpeg"
    ffprobe: str = "ffprobe"
    session_seconds: int = 1800
    playback_seconds: int = 15
    cancel_grace_seconds: float = 3.0
    lease_seconds: float = 120.0
    heartbeat_seconds: float = 10.0
    max_attempts: int = 3
    max_bytes: int = 2 * 1024**3
    max_duration: int = 3600
    scratch_bytes: int = 10 * 1024**3
    storage_bytes: int = 100 * 1024**3
    signed_seconds: int = 7200
    import_budget_seconds: float = 3600.0

    @classmethod
    def from_env(cls, *, data_dir: Path | None = None) -> FeedConfig:
        root = data_dir or Path(os.getenv("APP_DATA_DIR", "data"))
        enabled = os.getenv("FEED_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
        endpoint = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
        public = os.getenv("MINIO_PUBLIC_ENDPOINT", endpoint)

        def parse(value: str) -> tuple[str, bool]:
            parsed = urlsplit(value if "://" in value else "http://" + value)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.path not in {"", "/"}
                or parsed.username
            ):
                raise ValueError(
                    "MinIO endpoint must be an HTTP(S) origin without credentials/path"
                )
            return parsed.netloc, parsed.scheme == "https"

        host, secure = parse(endpoint)
        public_host, public_secure = parse(public)
        return cls(
            enabled=enabled,
            redis_url=os.getenv("FEED_REDIS_URL", cls.redis_url),
            minio_endpoint=host,
            minio_public_endpoint=public_host,
            minio_secure=secure,
            minio_public_secure=public_secure,
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", ""),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", ""),
            bucket=os.getenv("MEDIA_BUCKET", "radio-media"),
            workdir=Path(
                os.getenv("MEDIA_WORKDIR", "").strip() or str(root / "media-work")
            ).resolve(),
            ffmpeg=os.getenv("FFMPEG_PATH", "ffmpeg"),
            ffprobe=os.getenv("FFPROBE_PATH", "ffprobe"),
            session_seconds=int(os.getenv("FEED_SESSION_SECONDS", "1800")),
            playback_seconds=int(os.getenv("MEDIA_PLAYBACK_LEASE_SECONDS", "15")),
            cancel_grace_seconds=float(os.getenv("MEDIA_CANCEL_GRACE_SECONDS", "3")),
            heartbeat_seconds=float(os.getenv("MEDIA_IMPORT_HEARTBEAT_SECONDS", "10")),
            lease_seconds=float(os.getenv("MEDIA_IMPORT_LEASE_SECONDS", "120")),
            max_attempts=int(os.getenv("MEDIA_FETCH_MAX_ATTEMPTS", "3")),
            signed_seconds=int(os.getenv("MEDIA_SIGNED_SECONDS", "7200")),
            import_budget_seconds=float(os.getenv("MEDIA_IMPORT_BUDGET_SECONDS", "3600")),
            max_bytes=int(os.getenv("MEDIA_MAX_BYTES", str(2 * 1024**3))),
            max_duration=int(os.getenv("MEDIA_MAX_DURATION_SECONDS", "3600")),
            scratch_bytes=int(os.getenv("MEDIA_SCRATCH_BYTES", str(10 * 1024**3))),
            storage_bytes=int(os.getenv("MEDIA_STORAGE_BYTES", str(100 * 1024**3))),
        )

    def __post_init__(self):
        if self.playback_seconds < 10:
            raise ValueError("Playback lease must allow the 5-second client heartbeat")
        if (self.workdir / "pyproject.toml").exists() or (self.workdir / ".minio.sys").exists():
            raise ValueError("MEDIA_WORKDIR must be a dedicated scratch directory")
        if self.enabled and (not self.minio_access_key or not self.minio_secret_key):
            raise ValueError("Feed requires configured MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
        if (
            min(
                self.max_bytes,
                self.max_duration,
                self.scratch_bytes,
                self.storage_bytes,
                self.session_seconds,
                self.playback_seconds,
                self.max_attempts,
            )
            <= 0
        ):
            raise ValueError("Feed limits must be positive")
        timers = (
            self.heartbeat_seconds,
            self.lease_seconds,
            self.cancel_grace_seconds,
            self.import_budget_seconds,
        )
        if (
            not all(math.isfinite(value) for value in timers)
            or self.heartbeat_seconds <= 0
            or self.heartbeat_seconds >= self.lease_seconds
            or self.cancel_grace_seconds < 0
            or not 1 <= self.signed_seconds <= 604800
            or not 0 < self.import_budget_seconds <= 3600
        ):
            raise ValueError("Invalid media lease policy")
