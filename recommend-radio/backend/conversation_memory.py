from __future__ import annotations

import json
import re
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from database import begin_write, get_connection

HOT_TURN_LIMIT = 8
WARM_TOPIC_TTL_HOURS = 24
_HOT: dict[tuple[str, str], deque[dict[str, str]]] = {}


class ConversationMemoryService:
    """Hot ring buffer plus SQLite-backed warm topic summaries for one local user."""

    def __init__(self, db_path: str, *, user_id: str) -> None:
        self.db_path = db_path
        self.user_id = user_id

    def append(self, *, session_id: str, role: str, content: str) -> None:
        key = (self.user_id, session_id)
        buffer = _HOT.setdefault(key, deque(maxlen=HOT_TURN_LIMIT))
        buffer.append({"role": role, "content": content[:500]})

    def hot(self, *, session_id: str) -> list[dict[str, str]]:
        key = (self.user_id, session_id)
        buffer = _HOT.get(key)
        if buffer is None:
            buffer = deque(maxlen=HOT_TURN_LIMIT)
            with get_connection(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT role, content FROM agent_dialogue_turns WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                    (session_id, HOT_TURN_LIMIT),
                ).fetchall()
            for row in reversed(rows):
                buffer.append({"role": str(row["role"]), "content": str(row["content"])[:500]})
            _HOT[key] = buffer
        return list(buffer)

    def refresh_warm(
        self,
        *,
        session_id: str,
        topic: str,
        memory_type: str = "topic_summary",
        scope_type: str = "session",
        scope_key: str | None = None,
    ) -> None:
        normalized_topic = _topic_key(topic)
        if not normalized_topic:
            return
        normalized_memory_type = _bounded_key(memory_type, default="topic_summary")
        normalized_scope_type = _bounded_key(scope_type, default="session")
        normalized_scope_key = _bounded_key(scope_key or session_id, default=session_id)
        hot = self.hot(session_id=session_id)
        user_lines = [item["content"] for item in hot if item["role"] == "user"][-4:]
        if not user_lines:
            return
        summary = "；".join(user_lines)[-900:]
        keywords = _keywords(" ".join(user_lines))
        now = _utc_now()
        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=WARM_TOPIC_TTL_HOURS)
        ).isoformat()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="conversation-warm", key=json.dumps([
                self.user_id, normalized_scope_type, normalized_scope_key, normalized_memory_type
            ]))
            conn.execute(
                """
                UPDATE conversation_warm_memories SET status='inactive'
                WHERE user_id=? AND scope_type=? AND scope_key=? AND memory_type=?
                  AND memory_key<>? AND status='active'
                """,
                (
                    self.user_id,
                    normalized_scope_type,
                    normalized_scope_key,
                    normalized_memory_type,
                    normalized_topic,
                ),
            )
            conn.execute(
                """
                INSERT INTO conversation_warm_memories (
                    memory_id, user_id, scope_type, scope_key, memory_type,
                    memory_key, source_session_id, summary, keywords_json,
                    status, updated_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(user_id, scope_type, scope_key, memory_type, memory_key)
                DO UPDATE SET
                    source_session_id=excluded.source_session_id,
                    summary = excluded.summary, keywords_json = excluded.keywords_json,
                    status = 'active', updated_at = excluded.updated_at, expires_at = excluded.expires_at
                """,
                (
                    f"warm:{uuid4().hex}",
                    self.user_id,
                    normalized_scope_type,
                    normalized_scope_key,
                    normalized_memory_type,
                    normalized_topic,
                    session_id,
                    summary,
                    _json(keywords),
                    now,
                    expires_at,
                ),
            )

    def warm(
        self,
        *,
        session_id: str,
        scope_type: str = "session",
        scope_key: str | None = None,
        memory_types: tuple[str, ...] = (),
    ) -> dict[str, Any] | None:
        now = _utc_now()
        normalized_scope_type = _bounded_key(scope_type, default="session")
        normalized_scope_key = _bounded_key(scope_key or session_id, default=session_id)
        type_sql = ""
        parameters: list[Any] = [
            self.user_id,
            normalized_scope_type,
            normalized_scope_key,
            now,
        ]
        if memory_types:
            placeholders = ",".join("?" for _ in memory_types)
            type_sql = f" AND memory_type IN ({placeholders})"
            parameters.extend(memory_types)
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE conversation_warm_memories SET status='expired'
                WHERE user_id=? AND scope_type=? AND scope_key=? AND expires_at<=?
                """,
                (self.user_id, normalized_scope_type, normalized_scope_key, now),
            )
            row = conn.execute(
                f"""
                SELECT memory_type, memory_key, summary, keywords_json,
                       source_session_id, updated_at, expires_at
                FROM conversation_warm_memories
                WHERE user_id=? AND scope_type=? AND scope_key=?
                  AND expires_at>? AND status='active'{type_sql}
                ORDER BY updated_at DESC LIMIT 1
                """,
                tuple(parameters),
            ).fetchone()
        if row is None:
            return None
        return {
            "memoryType": row["memory_type"],
            "memoryKey": row["memory_key"],
            "topic": row["memory_key"],
            "summary": row["summary"],
            "keywords": _json_load(row["keywords_json"]),
            "sourceSessionId": row["source_session_id"],
            "updatedAt": row["updated_at"],
            "expiresAt": row["expires_at"],
        }


def _topic_key(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().casefold())
    return text[:80]


def _bounded_key(value: str, *, default: str) -> str:
    normalized = re.sub(r"\s+", "_", str(value or "").strip().casefold())
    return normalized[:80] or default


def _keywords(value: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]{2,4}", value.casefold())
    return list(dict.fromkeys(token for token in tokens if len(token) >= 2))[:12]


def _json(value: list[str]) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)


def _json_load(value: str) -> list[str]:
    import json

    try:
        data = json.loads(value)
    except (TypeError, ValueError):
        return []
    return [str(item) for item in data] if isinstance(data, list) else []


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
