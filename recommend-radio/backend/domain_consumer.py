"""Consumer Inbox transaction; external handlers still require idempotent effects."""

from __future__ import annotations

import hashlib
import json

from database import begin_write, get_connection
from domain_outbox import encode
from kafka_worker import KafkaConsumerLoop, decode_message, reject_delivery


class DomainEventConsumer:
    def __init__(self, bus, *, consumer_name, handler=None, prepare=None):
        if not consumer_name or len(consumer_name) > 48:
            raise ValueError("Consumer name must be between1 and48 characters")
        self.bus, self.consumer_name, self.handler, self.prepare = (
            bus,
            consumer_name,
            handler,
            prepare,
        )

    def process(self, message, handler=None, cancelled=None, prepare=None):
        handler = handler or self.handler
        if handler is None:
            raise ValueError("A domain event handler is required")
        if cancelled is not None and cancelled.is_set():
            return False
        try:
            envelope = decode_message(message)
            event_id = envelope["eventId"]
            if (
                envelope.get("schemaVersion") != "radio.domain.v1"
                or not isinstance(event_id, str)
                or len(event_id) > 180
            ):
                raise ValueError("Invalid domain event id")
            digest = hashlib.sha256(encode(envelope).encode()).hexdigest()
        except (ValueError, TypeError, KeyError, UnicodeError):
            return reject_delivery(
                self.bus.db_path, self.consumer_name, message, "InvalidDomainEnvelope"
            )
        with get_connection(self.bus.db_path) as conn:
            authority = conn.execute(
                "SELECT * FROM domain_event_outbox WHERE event_id=?", (event_id,)
            ).fetchone()
            existing = conn.execute(
                """SELECT event_hash FROM domain_event_inbox
                WHERE consumer_name=? AND event_id=?""",
                (self.consumer_name, event_id),
            ).fetchone()
            head = conn.execute(
                """SELECT last_sequence FROM domain_consumer_heads
                WHERE consumer_name=? AND aggregate_key=?""",
                (self.consumer_name, authority["aggregate_key"] if authority else ""),
            ).fetchone()
        if (
            not authority
            or authority["event_hash"] != digest
            or message.topic() != self.bus.settings.topic("domain")
        ):
            return reject_delivery(
                self.bus.db_path, self.consumer_name, message, "DomainAuthorityMismatch"
            )
        # Re-read canonical SQL data; broker payload is neither authority nor permission.
        envelope = json.loads(authority["event_json"])
        if existing:
            if existing[0] != digest:
                raise ValueError("Inbox event identity changed")
            return True
        # Network I/O does not hold the SQL transaction or aggregate lock. Its
        # effects may repeat and must use domain/external version fencing.
        preparation = prepare or self.prepare
        if preparation is not None and (head is None or head[0] < envelope["sequence"]):
            preparation(envelope)
        if cancelled is not None and cancelled.is_set():
            return False
        with get_connection(self.bus.db_path) as conn:
            begin_write(
                conn,
                namespace="domain-inbox",
                key=encode([self.consumer_name, envelope["aggregateKey"]]),
            )
            old = conn.execute(
                """SELECT event_hash FROM domain_event_inbox
                WHERE consumer_name=? AND event_id=?""",
                (self.consumer_name, event_id),
            ).fetchone()
            if old:
                if old[0] != digest:
                    raise ValueError("Inbox event identity changed")
                return True
            head = conn.execute(
                """SELECT last_sequence FROM domain_consumer_heads
                WHERE consumer_name=? AND aggregate_key=?""",
                (self.consumer_name, envelope["aggregateKey"]),
            ).fetchone()
            if head is None or head[0] < envelope["sequence"]:
                handler(conn, envelope)
                if cancelled is not None and cancelled.is_set():
                    raise RuntimeError("Kafka assignment lost before Inbox commit")
                conn.execute(
                    """INSERT INTO domain_consumer_heads
                    (consumer_name,aggregate_key,last_sequence) VALUES (?,?,?)
                    ON CONFLICT(consumer_name,aggregate_key)
                    DO UPDATE SET last_sequence=excluded.last_sequence""",
                    (self.consumer_name, envelope["aggregateKey"], envelope["sequence"]),
                )
            import time

            conn.execute(
                """INSERT INTO domain_event_inbox
                (consumer_name,event_id,event_hash,aggregate_key,sequence,processed_at)
                VALUES (?,?,?,?,?,?)""",
                (
                    self.consumer_name,
                    event_id,
                    digest,
                    envelope["aggregateKey"],
                    envelope["sequence"],
                    time.time(),
                ),
            )
        return True

    def __call__(self, message, cancelled):
        return self.process(message, cancelled=cancelled)

    def worker(self, *, concurrency=2):
        return KafkaConsumerLoop(
            self.bus,
            group=f"{self.bus.settings.topic_prefix}.{self.consumer_name}",
            lanes=("domain",),
            processor=self,
            concurrency=concurrency,
        )
