"""Version-coalesced projection SQL. Every event retains an Inbox receipt."""

from __future__ import annotations

import hashlib
import time
from collections import defaultdict

from .bitmap import validate_reaction
from .repository import encode

MAX_BATCH = 64


class ProjectionConflict(ValueError):
    def __init__(self, event_ids):
        self.event_ids = set(event_ids)
        super().__init__("Projection event identity/version conflict")


def validate(payload):
    if not isinstance(payload, dict):
        raise ValueError("Invalid projection payload")
    for field in ("event_id", "user_id", "content_id"):
        if (
            not isinstance(payload.get(field), str)
            or not payload[field]
            or len(payload[field]) > 180
        ):
            raise ValueError("Invalid projection identity")
    validate_reaction(payload.get("state"), payload.get("version"))


def insert_rows(conn, table, rows, *, upsert=False):
    if not rows:
        return
    if getattr(conn, "dialect", None) == "mysql":
        import sqlalchemy as sa
        from sqlalchemy.dialects.mysql import insert

        target = conn.tables[table]
        if upsert:
            statement = insert(target).values(rows)
            statement = statement.on_duplicate_key_update(
                state=statement.inserted.state, version=statement.inserted.version
            )
            conn.connection.execute(statement)
        else:
            conn.connection.execute(sa.insert(target), rows)
        return
    fields = list(rows[0])
    sql = (
        "INSERT INTO "
        + table
        + " ("
        + ",".join(fields)
        + ") VALUES ("
        + ",".join("?" for _ in fields)
        + ")"
    )
    if upsert:
        sql += (
            " ON CONFLICT(user_id,content_id) DO UPDATE SET state=excluded.state,"
            "version=excluded.version"
        )
    conn.executemany(sql, [tuple(row[f] for f in fields) for row in rows])


def apply_many(conn, payloads, lock):
    if not payloads or len(payloads) > MAX_BATCH:
        raise ValueError("A bounded nonempty projection batch is required")
    events, hashes = {}, {}
    for payload in payloads:
        validate(payload)
        digest = hashlib.sha256(encode(payload).encode()).hexdigest()
        identity = payload["event_id"]
        if identity in hashes and hashes[identity] != digest:
            raise ProjectionConflict([identity])
        events[identity], hashes[identity] = payload, digest
    ids = sorted({p["content_id"] for p in events.values()})
    rows = conn.execute(
        "SELECT content_id,track_id FROM feed_content WHERE content_id IN ("
        + ",".join("?" for _ in ids)
        + ")",
        tuple(ids),
    ).fetchall()
    tracks = {r["content_id"]: r["track_id"] for r in rows}
    if set(tracks) != set(ids):
        raise ValueError("Projection content no longer exists")
    for key in sorted({tracks[cid] or "content:" + cid for cid in ids}):
        lock(conn, namespace="reaction-projection", key=key)
    event_ids = sorted(events)
    existing = {
        r["event_id"]: r["payload_hash"]
        for r in conn.execute(
            "SELECT event_id,payload_hash FROM feed_inbox "
            "WHERE consumer='reaction' AND event_id IN (" + ",".join("?" for _ in event_ids) + ")",
            tuple(event_ids),
        )
    }
    conflicting = [identity for identity, digest in existing.items() if hashes[identity] != digest]
    if conflicting:
        raise ProjectionConflict(conflicting)
    users = sorted({p["user_id"] for p in events.values()})
    current = {
        (r["user_id"], r["content_id"]): dict(r)
        for r in conn.execute(
            "SELECT * FROM reaction_projection WHERE content_id IN ("
            + ",".join("?" for _ in ids)
            + ") AND user_id IN ("
            + ",".join("?" for _ in users)
            + ")",
            tuple(ids) + tuple(users),
        )
    }
    latest, pairs, versions = {}, set(), {}
    for identity, payload in events.items():
        key = (payload["user_id"], payload["content_id"])
        pairs.add(key)
        if identity in existing:
            continue
        version_key = (*key, payload["version"])
        prior = versions.get(version_key)
        if prior and prior["state"] != payload["state"]:
            raise ProjectionConflict([prior["event_id"], identity])
        versions[version_key] = payload
        old = latest.get(key)
        if old and old["version"] == payload["version"] and old["state"] != payload["state"]:
            raise ProjectionConflict([old["event_id"], identity])
        if old is None or payload["version"] > old["version"]:
            latest[key] = payload
    updates, deltas = [], defaultdict(lambda: [0, 0])
    for key, payload in latest.items():
        old = current.get(key, {"state": "neutral", "version": 0})
        if payload["version"] == old["version"] and payload["state"] != old["state"]:
            raise ProjectionConflict([payload["event_id"]])
        if payload["version"] <= old["version"]:
            continue
        cid = payload["content_id"]
        deltas[cid][0] += int(payload["state"] == "like") - int(old["state"] == "like")
        deltas[cid][1] += int(payload["state"] == "dislike") - int(old["state"] == "dislike")
        value = {
            "user_id": key[0],
            "content_id": key[1],
            "state": payload["state"],
            "version": payload["version"],
        }
        updates.append(value)
        current[key] = value
    for cid, (likes, dislikes) in deltas.items():
        result = conn.execute(
            "UPDATE content_counters SET likes=likes+?,dislikes=dislikes+?, "
            "version=version+1,updated_at=? WHERE content_id=?",
            (likes, dislikes, time.time(), cid),
        )
        if not result.rowcount:
            raise ValueError("Missing authoritative content counter; reconciliation required")
    insert_rows(conn, "reaction_projection", updates, upsert=True)
    insert_rows(
        conn,
        "feed_inbox",
        [
            {
                "consumer": "reaction",
                "event_id": identity,
                "payload_hash": hashes[identity],
                "processed_at": time.time(),
            }
            for identity in events
            if identity not in existing
        ],
    )
    counters = {
        r["content_id"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM content_counters WHERE content_id IN ("
            + ",".join("?" for _ in ids)
            + ")",
            tuple(ids),
        )
    }
    if set(counters) != set(ids):
        raise ValueError("Missing authoritative content counter; reconciliation required")
    relations = [
        current.get(
            key, {"user_id": key[0], "content_id": key[1], "state": "neutral", "version": 0}
        )
        for key in sorted(pairs)
    ]
    return {
        "counters": counters,
        "relations": relations,
        "events": len(events),
        "appliedPairs": len(updates),
        "coalesced": len(events) - len(existing) - len(latest),
        "newReceipts": len(events) - len(existing),
    }
