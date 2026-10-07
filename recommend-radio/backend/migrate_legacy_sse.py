"""One-time, repeatable import of the Go gateway's replay history."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from database import get_connection


def import_legacy_sse_events(
    target_db: str | Path,
    source_db: str | Path,
) -> int:
    source = Path(source_db).resolve()
    if not source.is_file():
        return 0
    inserted = 0
    with sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True) as old:
        exists = old.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dialogue_events'"
        ).fetchone()
        if exists is None:
            return 0
        cursor = old.execute(
            """SELECT event_id, task_id, session_id, user_id, event_type,
                      status, payload_json, created_at
               FROM dialogue_events ORDER BY event_id"""
        )
        while rows := cursor.fetchmany(500):
            with get_connection(target_db) as current:
                for row in rows:
                    legacy_id = int(row[0])
                    key = f"legacy:{legacy_id}"
                    collision = current.execute(
                        "SELECT event_key FROM dialogue_events WHERE event_id = ?", (legacy_id,)
                    ).fetchone()
                    if collision is not None:
                        if collision[0] != key:
                            raise RuntimeError("legacy SSE ID collides with a new event")
                        continue
                    payload = row[6]
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8")
                    current.execute(
                        """INSERT INTO dialogue_events
                        (event_id, event_key, task_id, session_id, user_id,
                         event_type, status, payload_json, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (legacy_id, key, row[1], row[2], row[3], row[4], row[5], payload, row[7]),
                    )
                    inserted += 1
    return inserted
