"""Drain the v1 raw AMQP queue into the durable v2 Celery outbox."""

from __future__ import annotations

import hashlib
import json
import logging
import signal
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from database import begin_write, get_connection, init_db
from durable_jobs import enqueue
from rabbitmq_bus import RabbitMQSettings, declare_topology, pika

LOGGER = logging.getLogger("recommend-radio.legacy-behavior")


def ingest_legacy_message(conn, body: bytes) -> str:
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict) or not payload.get("event"):
        raise ValueError("behavior message must contain an event")
    event_id = str(payload.get("event_id") or payload.get("eventId") or "").strip()
    user_id = str(payload.get("userId") or payload.get("user_id") or "").strip()
    if not event_id or not user_id:
        raise ValueError("behavior event_id and userId are required")
    job_id = "legacy:" + hashlib.sha256(json.dumps([user_id, event_id]).encode()).hexdigest()
    begin_write(conn, namespace="job-capacity", key=user_id)
    begin_write(conn, namespace="job", key=job_id)
    old = conn.execute("SELECT payload_json FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
    previous = json.loads(old["payload_json"]) if old else {}
    payload["occurred_at"] = (
        payload.get("occurred_at")
        or previous.get("occurred_at")
        or datetime.now(UTC).isoformat()
    )
    return enqueue(
        conn, kind="behavior", user_id=user_id, job_id=job_id, payload=payload, retry_safe=True
    )


class BehaviorWorker:
    def __init__(self) -> None:
        if pika is None:
            raise RuntimeError("pika is required to drain the legacy queue")
        self.settings = RabbitMQSettings.from_env()
        self.stopped = threading.Event()

    def run(self) -> None:
        init_db()
        while not self.stopped.is_set():
            connection = None
            try:
                connection = pika.BlockingConnection(self.settings.connection_parameters())
                channel = connection.channel()
                declare_topology(channel, self.settings)
                channel.basic_qos(prefetch_count=1)
                for method, _properties, body in channel.consume(
                    self.settings.queue, inactivity_timeout=1
                ):
                    if self.stopped.is_set():
                        break
                    Path("/tmp/radio-legacy-heartbeat").write_text(
                        str(time.time()), encoding="ascii"
                    )
                    if method is None:
                        continue
                    try:
                        with get_connection() as conn:
                            ingest_legacy_message(conn, body)
                    except (ValueError, UnicodeDecodeError):
                        LOGGER.exception("Invalid legacy message; routing to DLQ")
                        channel.basic_reject(method.delivery_tag, requeue=False)
                    else:
                        channel.basic_ack(method.delivery_tag)
                channel.cancel()
            except Exception:
                LOGGER.exception("Legacy drain interrupted")
                self.stopped.wait(2)
            finally:
                if connection and connection.is_open:
                    connection.close()

    def stop(self, *_args) -> None:
        self.stopped.set()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = BehaviorWorker()
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    worker.run()
