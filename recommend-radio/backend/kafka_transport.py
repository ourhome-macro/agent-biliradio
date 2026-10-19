"""Kafka clients with explicit delivery acknowledgements and SQL-backed identity."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from database import get_connection


def lane_for(kind):
    return (
        "media"
        if kind == "media_import"
        else "events"
        if kind in {"behavior", "sse", "feed_projection", "feed_asset_ready"}
        else "jobs"
    )


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_prefix: str = "radio"
    partitions: int = 6
    replication_factor: int = 1

    @classmethod
    def from_env(cls):
        return cls(
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            os.getenv("KAFKA_TOPIC_PREFIX", "radio"),
            int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6")),
            int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")),
        )

    def topic(self, lane):
        if lane not in {"jobs", "events", "media", "domain"}:
            raise ValueError("Unknown Kafka topic lane")
        if (
            not self.topic_prefix
            or len(self.topic_prefix) > 100
            or any(
                character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
                for character in self.topic_prefix
            )
        ):
            raise ValueError("Invalid Kafka topic prefix")
        return f"{self.topic_prefix}.{lane}.v1"

    def client(self):
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        }
        for environment, field in (
            ("KAFKA_SASL_MECHANISM", "sasl.mechanism"),
            ("KAFKA_SASL_USERNAME", "sasl.username"),
            ("KAFKA_SASL_PASSWORD", "sasl.password"),
            ("KAFKA_SSL_CA_LOCATION", "ssl.ca.location"),
        ):
            if os.getenv(environment):
                config[field] = os.environ[environment]
        return config


class KafkaJobBus:
    def __init__(self, db_path, settings=None):
        self.db_path, self.settings = str(db_path), settings or KafkaSettings.from_env()
        self.producer = Producer(
            {
                **self.settings.client(),
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 15000,
                "request.timeout.ms": 5000,
                "max.in.flight.requests.per.connection": 5,
                "linger.ms": 5,
            }
        )

    def provision(self):
        admin = AdminClient(self.settings.client())
        futures = admin.create_topics(
            [
                NewTopic(
                    self.settings.topic(lane),
                    num_partitions=self.settings.partitions,
                    replication_factor=self.settings.replication_factor,
                )
                for lane in ("jobs", "events", "media", "domain")
            ],
            request_timeout=15,
        )
        for future in futures.values():
            try:
                future.result(timeout=20)
            except KafkaException as error:
                from confluent_kafka import KafkaError

                if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise

    def send(self, lane, key, envelope):
        raw = json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        if len(raw.encode()) > 1024 * 1024:
            raise ValueError("Kafka delivery exceeds one MiB")
        done, delivered = threading.Event(), {}

        def confirm(error, message):
            delivered.update(
                error=error,
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )
            done.set()

        self.producer.produce(
            self.settings.topic(lane), key=key.encode(), value=raw.encode(), on_delivery=confirm
        )
        deadline = time.monotonic() + 17
        while not done.is_set() and time.monotonic() < deadline:
            self.producer.poll(0.1)
        if not done.is_set():
            raise TimeoutError("Kafka delivery acknowledgement deadline exceeded")
        if delivered.pop("error") is not None:
            raise RuntimeError("Kafka broker rejected delivery")
        return delivered

    def publish(self, job_id, kind):
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT kind,input_hash,lane FROM durable_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        if not row or row["kind"] != kind:
            raise ValueError("Broker metadata does not match the durable job")
        ordering = row["lane"] or job_id
        key = hashlib.sha256(str(ordering).encode()).hexdigest()
        return self.send(
            lane_for(kind),
            key,
            {
                "schemaVersion": "radio.job.v1",
                "jobId": job_id,
                "kind": kind,
                "inputHash": row["input_hash"],
            },
        )

    def publish_domain(self, envelope):
        return self.send("domain", envelope["aggregateKey"], envelope)

    def consumer(self, group, *, max_poll_ms=300000):
        return Consumer(
            {
                **self.settings.client(),
                "group.id": group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "max.poll.interval.ms": max_poll_ms,
                "session.timeout.ms": 10000,
                "heartbeat.interval.ms": 3000,
                "isolation.level": "read_committed",
            }
        )

    def health(self):
        try:
            metadata = AdminClient(self.settings.client()).list_topics(timeout=3)
            return bool(metadata.brokers) and all(
                self.settings.topic(lane) in metadata.topics
                and metadata.topics[self.settings.topic(lane)].error is None
                for lane in ("jobs", "events", "media", "domain")
            )
        except Exception:
            return False
