"""Replayable, idempotent dialogue events in the business SQLite database."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from database import get_connection

EVENT_TYPES = {"task", "progress", "session", "discovery", "done", "error"}
PAGE_SIZE = 500


def append_event(
    connection: Any,
    *,
    task_id: str,
    session_id: str,
    user_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    status: str = "running",
) -> int:
    task_id, session_id, user_id = (str(value).strip() for value in (task_id, session_id, user_id))
    event_type = str(event_type).strip().lower()
    if not all((task_id, session_id, user_id)) or event_type not in EVENT_TYPES:
        raise ValueError("invalid dialogue event identity or type")
    body = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    identity = json.dumps(
        [task_id, session_id, user_id, event_type, status, body],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    event_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    connection.execute(
        """INSERT OR IGNORE INTO dialogue_events
        (event_key, task_id, session_id, user_id, event_type, status, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            event_key, task_id, session_id, user_id, event_type, str(status), body,
            datetime.now(UTC).isoformat(),
        ),
    )
    row = connection.execute(
        "SELECT event_id FROM dialogue_events WHERE event_key = ?", (event_key,)
    ).fetchone()
    return int(row[0])


def list_events_after(
    db_path: str | None,
    *,
    user_id: str,
    session_id: str,
    after_id: int,
    limit: int = PAGE_SIZE,
) -> list[dict[str, Any]]:
    with get_connection(db_path) as connection:
        rows = connection.execute(
            """SELECT event_id, task_id, session_id, user_id, event_type,
                      status, payload_json, created_at
               FROM dialogue_events
               WHERE user_id = ? AND session_id = ? AND event_id > ?
               ORDER BY event_id LIMIT ?""",
            (user_id, session_id, max(0, after_id), min(max(1, limit), PAGE_SIZE)),
        ).fetchall()
    return [
        {
            "eventId": int(row["event_id"]),
            "taskId": row["task_id"],
            "sessionId": row["session_id"],
            "userId": row["user_id"],
            "type": row["event_type"],
            "status": row["status"],
            "payload": json.loads(row["payload_json"]),
            "createdAt": row["created_at"],
        }
        for row in rows
    ]


def latest_event_id(db_path: str | None, *, user_id: str, session_id: str) -> int:
    with get_connection(db_path) as connection:
        row = connection.execute(
            "SELECT COALESCE(MAX(event_id), 0) FROM dialogue_events "
            "WHERE user_id = ? AND session_id = ?",
            (user_id, session_id),
        ).fetchone()
    return int(row[0])


def prune_events(db_path: str | None, *, retention_days: int = 7) -> int:
    cutoff = (datetime.now(UTC) - timedelta(days=max(retention_days, 1))).isoformat()
    with get_connection(db_path) as connection:
        cursor = connection.execute(
            "DELETE FROM dialogue_events WHERE created_at < ?", (cutoff,)
        )
        return int(cursor.rowcount)
