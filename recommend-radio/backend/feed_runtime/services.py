from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from database import DATA_DIR, DEFAULT_DB_PATH, database_identity, get_connection, mysql_target

from .cache import FeedCache
from .config import FeedConfig
from .feed import FeedService
from .media import MediaService
from .reactions import ReactionService
from .repository import FeedRepository
from .storage import MinIOStorage


def resources(config: FeedConfig, db_path: str):
    return _resources(config, database_identity(db_path)[:12], str(db_path))


@lru_cache(maxsize=8)
def _resources(config: FeedConfig, namespace: str, db_path: str):
    prefix = f"radio:feed:{namespace}"
    return FeedCache(
        config.redis_url,
        prefix=prefix,
        hotkey_config=config.hotkeys,
        metadata_loader=public_metadata_loader(db_path, prefix),
    ), MinIOStorage(config)


def public_metadata_loader(db_path, prefix):
    """Capture only database identity, never a user request or an old SQL row."""

    def load(content_id):
        from resource_policy import ResourcePolicy

        with get_connection(db_path) as conn:
            row = (
                ResourcePolicy(db_path)
                .resolve_many(
                    "__feed_hotkey_public__",
                    [content_id],
                    action="feed",
                    include_private=False,
                    conn=conn,
                )
                .get(content_id)
            )
        if row is None:
            return None
        key = (
            f"{prefix}:content:{content_id}:acl:{row['permissionVersion']}:public:v{row['version']}"
        )
        return key, row["track"], row["version"]

    return load


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
            "hotkeys": self.cache.hotkey_status(),
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
