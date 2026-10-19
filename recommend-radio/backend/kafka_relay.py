"""Independently scalable SQL Outbox relays and bounded discovery scheduling."""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import threading
import time
from uuid import uuid4

from database import DEFAULT_DB_PATH, get_connection, init_db
from domain_outbox import DomainOutboxRelay
from durable_jobs import publish_pending
from kafka_transport import KafkaJobBus


def main():
    os.environ["JOB_TRANSPORT"] = "kafka"
    init_db()
    from telemetry_setup import setup

    setup("radio-kafka-relay")
    bus = KafkaJobBus(DEFAULT_DB_PATH)
    relay = DomainOutboxRelay(DEFAULT_DB_PATH, bus)
    stop = threading.Event()
    worker_id = f"relay:{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("radio.kafka-relay")
    pulse_at = 0
    try:
        while not stop.is_set():
            try:
                now = time.time()
                if now - pulse_at > 5:
                    with get_connection() as conn:
                        conn.execute(
                            """INSERT INTO job_worker_leases
                            (worker_id,transport,lanes_json,lease_until,updated_at)
                            VALUES (?,'kafka',?,?,?) ON CONFLICT(worker_id) DO UPDATE SET
                            lease_until=excluded.lease_until,updated_at=excluded.updated_at""",
                            (worker_id, json.dumps(["relay"]), now + 15, now),
                        )
                    pulse_at = now
                from following.scheduler import periodic_tick

                periodic_tick(DEFAULT_DB_PATH)
                publish_pending(DEFAULT_DB_PATH, bus.publish)
                relay.run_once()
            except Exception as error:
                logger.warning("Kafka Outbox recovery deferred: %s", type(error).__name__)
            stop.wait(1)
    finally:
        bus.producer.flush(5)
        try:
            with get_connection() as conn:
                conn.execute("DELETE FROM job_worker_leases WHERE worker_id=?", (worker_id,))
        except Exception:
            logger.warning("Relay heartbeat will expire after SQL outage")


if __name__ == "__main__":
    main()
