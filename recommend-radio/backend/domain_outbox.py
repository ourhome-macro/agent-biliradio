"""Domain facts and recoverable broker delivery, in separate SQL transactions."""

from __future__ import annotations

import hashlib
import json
import time
from uuid import uuid4

from database import begin_write, get_connection


def encode(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def enqueue_event(
    conn,
    *,
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    owner_id="",
    visibility_revision=0,
    event_id=None,
):
    event_id = event_id or uuid4().hex
    if not all(
        isinstance(value, str) and value and len(value) <= 180
        for value in (aggregate_type, aggregate_id, event_type, event_id)
    ):
        raise ValueError("Bounded event and aggregate identifiers are required")
    if (
        not isinstance(payload, dict)
        or type(visibility_revision) is not int
        or visibility_revision < 0
    ):
        raise ValueError("Invalid event payload or visibility revision")
    key = hashlib.sha256(encode([aggregate_type, aggregate_id]).encode()).hexdigest()
    begin_write(conn, namespace="domain-event", key=key)
    previous = conn.execute(
        "SELECT event_json FROM domain_event_outbox WHERE event_id=?", (event_id,)
    ).fetchone()
    if previous:
        old = json.loads(previous[0])
        expected = {
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id,
            "eventType": event_type,
            "payload": payload,
            "ownerId": owner_id,
            "visibilityRevision": visibility_revision,
        }
        if any(old[field] != value for field, value in expected.items()):
            raise ValueError("Event id is already bound to another domain fact")
        return event_id
    old = conn.execute(
        "SELECT sequence FROM domain_event_sequences WHERE aggregate_key=?", (key,)
    ).fetchone()
    sequence = (old[0] if old else 0) + 1
    conn.execute(
        """INSERT INTO domain_event_sequences(aggregate_key,sequence) VALUES (?,?)
        ON CONFLICT(aggregate_key) DO UPDATE SET sequence=excluded.sequence""",
        (key, sequence),
    )
    now = time.time()
    envelope = {
        "schemaVersion": "radio.domain.v1",
        "eventId": event_id,
        "eventType": event_type,
        "aggregateType": aggregate_type,
        "aggregateId": aggregate_id,
        "aggregateKey": key,
        "sequence": sequence,
        "ownerId": owner_id,
        "visibilityRevision": visibility_revision,
        "occurredAt": now,
        "payload": payload,
    }
    raw = encode(envelope)
    if len(raw.encode()) > 1024 * 1024:
        raise ValueError("Domain event exceeds the one MiB delivery contract")
    conn.execute(
        """INSERT INTO domain_event_outbox
        (event_id,aggregate_key,sequence,event_json,event_hash,created_at) VALUES (?,?,?,?,?,?)""",
        (event_id, key, sequence, raw, hashlib.sha256(raw.encode()).hexdigest(), now),
    )
    return event_id


def claim_events(db_path, *, limit=32, now=None, lease_seconds=30):
    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        if getattr(conn, "dialect", None) == "mysql":
            import sqlalchemy as sa

            table = conn.tables["domain_event_outbox"]
            earlier = table.alias("earlier")
            statement = (
                sa.select(table)
                .where(
                    table.c.status != "published",
                    table.c.available_at <= now,
                    table.c.publish_lease_until <= now,
                    ~sa.exists(
                        sa.select(1).where(
                            earlier.c.aggregate_key == table.c.aggregate_key,
                            earlier.c.sequence < table.c.sequence,
                            earlier.c.status != "published",
                        )
                    ),
                )
                .order_by(
                    table.c.status,
                    table.c.available_at,
                    table.c.publish_lease_until,
                    table.c.event_id,
                )
                .with_hint(table, "FORCE INDEX (idx_domain_outbox_pending)", dialect_name="mysql")
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = [dict(row) for row in conn.connection.execute(statement).mappings()]
        else:
            conn.execute("BEGIN IMMEDIATE")
            rows = [
                dict(row)
                for row in conn.execute(
                    """SELECT e.* FROM domain_event_outbox e
                WHERE status<>'published' AND available_at<=? AND publish_lease_until<=?
                AND NOT EXISTS (SELECT 1 FROM domain_event_outbox p
                    WHERE p.aggregate_key=e.aggregate_key
                    AND p.sequence<e.sequence AND p.status<>'published')
                ORDER BY created_at,event_id LIMIT ?""",
                    (now, now, limit),
                )
            ]
        for row in rows:
            token = uuid4().hex
            conn.execute(
                """UPDATE domain_event_outbox SET status='publishing',publish_token=?,
                publish_lease_until=?,publish_attempts=publish_attempts+1 WHERE event_id=?""",
                (token, now + lease_seconds, row["event_id"]),
            )
            row.update(
                publish_token=token,
                publish_lease_until=now + lease_seconds,
                publish_attempts=row["publish_attempts"] + 1,
            )
    return rows


def complete_publish(db_path, event, delivery, *, now=None):
    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        return bool(
            conn.execute(
                """UPDATE domain_event_outbox SET status='published',published_at=?,
            kafka_topic=?,kafka_partition=?,kafka_offset=?,publish_token=NULL,publish_lease_until=0,
            last_error=NULL WHERE event_id=? AND publish_token=? AND publish_lease_until>?""",
                (
                    now,
                    delivery["topic"],
                    delivery["partition"],
                    delivery["offset"],
                    event["event_id"],
                    event["publish_token"],
                    now,
                ),
            ).rowcount
        )


def retry_publish(db_path, event, error, *, now=None):
    now = time.time() if now is None else now
    with get_connection(db_path) as conn:
        conn.execute(
            """UPDATE domain_event_outbox SET status='pending',publish_token=NULL,
            publish_lease_until=0,available_at=?,last_error=?
            WHERE event_id=? AND publish_token=? AND publish_lease_until>?""",
            (
                now + min(2 ** min(event["publish_attempts"], 8), 120),
                type(error).__name__,
                event["event_id"],
                event["publish_token"],
                now,
            ),
        )


class DomainOutboxRelay:
    def __init__(self, db_path, producer):
        self.db_path, self.producer = db_path, producer

    def run_once(self, *, limit=32):
        completed = 0
        for event in claim_events(self.db_path, limit=limit):
            now = time.time()
            with get_connection(self.db_path) as conn:
                owned = conn.execute(
                    """UPDATE domain_event_outbox SET publish_lease_until=?
                    WHERE event_id=? AND publish_token=? AND publish_lease_until>?""",
                    (now + 30, event["event_id"], event["publish_token"], now),
                ).rowcount
            if not owned:
                continue
            try:
                delivery = self.producer.publish_domain(json.loads(event["event_json"]))
                completed += complete_publish(self.db_path, event, delivery)
            except Exception as error:
                retry_publish(self.db_path, event, error)
        return completed
