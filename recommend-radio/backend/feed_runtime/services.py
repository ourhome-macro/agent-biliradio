from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from database import DATA_DIR, DEFAULT_DB_PATH, database_identity, mysql_target

from .cache import FeedCache
from .config import FeedConfig
from .feed import FeedService
from .media import MediaService
from .reactions import ReactionService
from .repository import FeedRepository
from .storage import MinIOStorage


def resources(config: FeedConfig, db_path: str):
    return _resources(config, database_identity(db_path)[:12])


@lru_cache(maxsize=8)
def _resources(config: FeedConfig, namespace: str):
    return FeedCache(config.redis_url, prefix=f"radio:feed:{namespace}"), MinIOStorage(config)


class FeedServices:
    def __init__(
        self, *, user_id, db_path=None, config=None, recommendations=None, cache=None, storage=None
    ):
        self.user_id = user_id
        self.repo = FeedRepository(db_path or DEFAULT_DB_PATH)
        data_dir = DATA_DIR if mysql_target(self.repo.db_path) else Path(self.repo.db_path).parent
        self.config = config or FeedConfig.from_env(data_dir=data_dir)
        if cache is None or storage is None:
            shared_cache, shared_storage = resources(self.config, self.repo.db_path)
            cache = cache or shared_cache
            storage = storage or shared_storage
        self.cache, self.storage = cache, storage
        self.media = MediaService(self.repo, self.config, storage)
        self.feed = FeedService(self.repo, self.config, cache, recommendations=recommendations)
        self.reactions = ReactionService(self.repo, cache)

    def health(self):
        import shutil

        status = {
            "enabled": self.config.enabled,
            "redis": self.cache.health(),
            "minio": self.storage.health(),
            "ffmpeg": bool(shutil.which(self.config.ffmpeg)),
            "ffprobe": bool(shutil.which(self.config.ffprobe)),
        }
        from job_transport import job_transport, redis_bus

        if job_transport() == "redis_stream":
            import json
            import os

            try:
                bus = redis_bus(
                    self.repo.db_path, os.getenv("JOB_REDIS_URL", "redis://127.0.0.1:6379/1")
                )
                status["queue"] = bool(bus.redis.ping())
                workers = []
                for key in bus.redis.scan_iter(match=f"{bus.prefix}:worker:*", count=100):
                    raw = bus.redis.get(key)
                    if raw:
                        workers.append(json.loads(raw))
                status["mediaWorker"] = any("media" in row.get("lanes", ()) for row in workers)
            except Exception:
                status["queue"], status["mediaWorker"] = False, False
        return status
