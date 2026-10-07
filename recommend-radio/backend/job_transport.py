"""Transport selection; SQLite remains the authority for every durable job."""

from __future__ import annotations

import os
from functools import lru_cache


def job_transport():
    value = os.getenv("JOB_TRANSPORT", "").strip().lower()
    if value:
        if value not in {"redis_stream", "rabbitmq", "disabled"}:
            raise ValueError("JOB_TRANSPORT must be redis_stream, rabbitmq or disabled")
        return value
    return (
        "rabbitmq"
        if os.getenv("RABBITMQ_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
        else "disabled"
    )


def async_jobs_enabled():
    return job_transport() != "disabled"


def redis_bus(db_path, url):
    from database import database_identity
    return _redis_bus(str(db_path), url, database_identity(db_path))


@lru_cache(maxsize=4)
def _redis_bus(db_path, url, identity):
    from redis_stream_jobs import RedisJobBus

    return RedisJobBus(db_path, url)


def publish_job(job_id, kind):
    if job_transport() == "redis_stream":
        from database import DEFAULT_DB_PATH

        url = os.getenv("JOB_REDIS_URL", os.getenv("FEED_REDIS_URL", "redis://127.0.0.1:6379/1"))
        return redis_bus(str(DEFAULT_DB_PATH), url).publish(job_id, kind)
    if job_transport() == "rabbitmq":
        from task_app import publish_job as publish_celery

        return publish_celery(job_id, kind)
    raise RuntimeError("Async job transport is disabled")
