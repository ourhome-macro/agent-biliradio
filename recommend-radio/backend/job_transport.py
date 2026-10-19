"""Transport selection; SQLite remains the authority for every durable job."""

from __future__ import annotations

import os
from functools import lru_cache


def job_transport():
    value = os.getenv("JOB_TRANSPORT", "").strip().lower()
    if value:
        if value not in {"kafka", "redis_stream", "rabbitmq", "disabled"}:
            raise ValueError("JOB_TRANSPORT must be kafka, redis_stream, rabbitmq or disabled")
        return value
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        return "kafka"
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
    if job_transport() == "kafka":
        from database import DEFAULT_DB_PATH

        return kafka_bus(str(DEFAULT_DB_PATH)).publish(job_id, kind)
    if job_transport() == "redis_stream":
        from database import DEFAULT_DB_PATH

        url = os.getenv("JOB_REDIS_URL", os.getenv("FEED_REDIS_URL", "redis://127.0.0.1:6379/1"))
        return redis_bus(str(DEFAULT_DB_PATH), url).publish(job_id, kind)
    if job_transport() == "rabbitmq":
        from task_app import publish_job as publish_celery

        return publish_celery(job_id, kind)
    raise RuntimeError("Async job transport is disabled")


def kafka_bus(db_path):
    from database import database_identity
    from kafka_transport import KafkaSettings

    return _kafka_bus(str(db_path), KafkaSettings.from_env(), database_identity(db_path))


@lru_cache(maxsize=4)
def _kafka_bus(db_path, settings, identity):
    from kafka_transport import KafkaJobBus

    return KafkaJobBus(db_path, settings)
