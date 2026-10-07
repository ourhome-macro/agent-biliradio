from __future__ import annotations

import logging
import os
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import pika
except ImportError:  # pragma: no cover - only possible in incomplete local environments
    pika = None  # type: ignore[assignment]


LOGGER = logging.getLogger("recommend-radio.rabbitmq")


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_", populate_by_name=True, frozen=True, extra="ignore"
    )

    host: str = Field(default="127.0.0.1", min_length=1, pattern=r"^[^\s/]+$")
    port: int = Field(default=5672, ge=1, le=65535)
    user: str = Field(default="radio", min_length=1)
    password: str = Field(default="radio_dev_password", min_length=1, repr=False)
    virtual_host: str = Field(default="/", validation_alias="RABBITMQ_VHOST")
    exchange: str = Field(default="recommend-radio.events.v1", min_length=1)
    routing_key: str = Field(
        default="behavior.music", min_length=1, validation_alias="RABBITMQ_BEHAVIOR_ROUTING_KEY"
    )
    queue: str = Field(
        default="recommend-radio.amem.behavior.v1",
        min_length=1,
        validation_alias="RABBITMQ_BEHAVIOR_QUEUE",
    )
    dead_letter_exchange: str = Field(default="recommend-radio.dlx.v1", min_length=1)
    dead_letter_queue: str = Field(
        default="recommend-radio.amem.behavior.dlq.v1",
        min_length=1,
        validation_alias="RABBITMQ_BEHAVIOR_DLQ",
    )
    delivery_limit: int = Field(default=8, ge=1, le=100)
    prefetch_count: int = Field(default=16, ge=1, le=1000)

    @classmethod
    def from_env(cls) -> RabbitMQSettings:
        return cls()

    def validate(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ValueError("RabbitMQ port must be between 1 and 65535")
        if self.delivery_limit < 1 or self.prefetch_count < 1:
            raise ValueError("RabbitMQ delivery and prefetch limits must be positive")
        required = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "exchange": self.exchange,
            "routing_key": self.routing_key,
            "queue": self.queue,
            "dead_letter_exchange": self.dead_letter_exchange,
            "dead_letter_queue": self.dead_letter_queue,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"RabbitMQ settings are missing: {', '.join(missing)}")

    def connection_parameters(self) -> Any:
        if pika is None:
            raise RuntimeError("pika is required when RabbitMQ is enabled")
        self.validate()
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=pika.PlainCredentials(
                self.user,
                self.password,
                erase_on_connect=False,
            ),
            heartbeat=30,
            blocked_connection_timeout=30,
            connection_attempts=3,
            retry_delay=2,
            socket_timeout=5,
            stack_timeout=15,
            client_properties={"connection_name": "recommend-radio"},
        )


def declare_topology(channel: Any, settings: RabbitMQSettings) -> None:
    """Declare the durable behavior topology identically on producers and consumers."""
    settings.validate()
    channel.exchange_declare(
        exchange=settings.exchange,
        exchange_type="topic",
        durable=True,
    )
    channel.exchange_declare(
        exchange=settings.dead_letter_exchange,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=settings.dead_letter_queue,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    channel.queue_bind(
        exchange=settings.dead_letter_exchange,
        queue=settings.dead_letter_queue,
        routing_key=settings.dead_letter_queue,
    )
    channel.queue_declare(
        queue=settings.queue,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": settings.delivery_limit,
            "x-dead-letter-exchange": settings.dead_letter_exchange,
            "x-dead-letter-routing-key": settings.dead_letter_queue,
        },
    )
    channel.queue_bind(
        exchange=settings.exchange,
        queue=settings.queue,
        routing_key=settings.routing_key,
    )


class RabbitMQBehaviorBridge:
    """Publish behavior writes asynchronously while delegating all AMEM reads."""

    enabled = True

    def __init__(self, delegate: Any, publisher: Any | None = None) -> None:
        self._delegate = delegate
        self._publisher = publisher or OutboxBehaviorPublisher()

    def record_behavior(self, payload: dict[str, Any]) -> dict[str, Any]:
        from job_transport import job_transport
        event_id = self._publisher.publish(dict(payload or {}))
        return {
            "enabled": True,
            "queued": True,
            "eventId": event_id,
            "memoryIds": [],
            "source": "redis_stream" if job_transport()=="redis_stream" else "rabbitmq",
        }

    def behavior_bus_health(self) -> bool:
        return bool(self._publisher.health())

    def close(self) -> None:
        self._publisher.close()
        close = getattr(self._delegate, "close", None)
        if callable(close):
            close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def rabbitmq_enabled() -> bool:
    return os.getenv("RABBITMQ_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def wrap_behavior_bridge(bridge: Any) -> Any:
    from job_transport import async_jobs_enabled
    if not async_jobs_enabled() or not getattr(bridge, "enabled", False):
        return bridge
    return RabbitMQBehaviorBridge(bridge)


class OutboxBehaviorPublisher:
    def publish(self, payload: dict[str, Any]) -> str:
        from database import get_connection
        from durable_jobs import enqueue_behavior

        with get_connection() as conn:
            event_id = enqueue_behavior(
                conn,
                user_id=payload["userId"],
                event=payload["event"],
                scene=payload["scene"],
                payload=payload,
            )
        if event_id is None:
            raise RuntimeError("RabbitMQ behavior outbox is not enabled")
        return event_id

    def health(self) -> bool:
        from database import get_connection

        with get_connection() as conn:
            conn.execute("SELECT job_id FROM durable_jobs LIMIT 1").fetchone()
        return True

    def close(self) -> None:
        pass
