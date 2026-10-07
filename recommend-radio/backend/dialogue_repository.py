from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from database import begin_write, get_connection
from dialogue_rules import (
    CHAT_LLM_HISTORY_LIMIT,
    VISIBLE_CARD_LIMIT,
    _json_loads,
    _serialize_card,
    _snapshot_list,
    _utc_now,
)


class DialogueRepository:
    """User-scoped persistence. Callers own transaction boundaries."""

    def __init__(self, db_path, user_id: str):
        self.db_path = db_path
        self.user_id = user_id

    def _save_checkpoint(self, conn: Any, session_id: str, *, reason: str) -> None:
        self._lock_session(conn, session_id)
        session = self._load_session(conn, session_id)
        if session is None:
            return
        now = _utc_now()
        snapshot = {
            "session": dict(session),
            "turns": self._snapshot_rows(conn, "agent_dialogue_turns", session_id),
            "cards": self._snapshot_rows(conn, "agent_dialogue_cards", session_id),
            "signals": self._snapshot_rows(conn, "agent_dialogue_signals", session_id),
        }
        conn.execute(
            """
            INSERT INTO agent_dialogue_checkpoints (
                checkpoint_id, session_id, user_id, reason, snapshot_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                f"agent-dialogue-checkpoint:{uuid4().hex}",
                session_id,
                self.user_id,
                reason,
                json.dumps(snapshot, ensure_ascii=False),
                now,
            ),
        )

    def _latest_checkpoint(self, conn: Any, session_id: str) -> Any:
        return conn.execute(
            """
            SELECT *
            FROM agent_dialogue_checkpoints
            WHERE session_id = ? AND user_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (session_id, self.user_id),
        ).fetchone()

    def _restore_checkpoint(self, conn: Any, checkpoint: Any) -> None:
        self._lock_session(conn, checkpoint["session_id"])
        snapshot = _json_loads(checkpoint["snapshot_json"])
        session = snapshot.get("session") if isinstance(snapshot.get("session"), dict) else {}
        if session.get("session_id") != checkpoint["session_id"]:
            raise ValueError("checkpoint session mismatch")

        conn.execute(
            """
            UPDATE agent_dialogue_sessions
            SET state = ?, focus = ?, created_at = ?, updated_at = ?, pending_context_json = ?
            WHERE session_id = ? AND user_id = ?
            """,
            (
                session.get("state") or "chatting",
                session.get("focus") or "新的聊天",
                session.get("created_at") or checkpoint["created_at"],
                session.get("updated_at") or checkpoint["created_at"],
                session.get("pending_context_json") or "{}",
                checkpoint["session_id"],
                self.user_id,
            ),
        )

        for table in (
            "agent_dialogue_turns",
            "agent_dialogue_cards",
            "agent_dialogue_signals",
        ):
            conn.execute(f"DELETE FROM {table} WHERE session_id = ?", (checkpoint["session_id"],))

        for row in _snapshot_list(snapshot, "turns"):
            conn.execute(
                """
                INSERT INTO agent_dialogue_turns (
                    id, session_id, role, content, card_id, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("id"),
                    row.get("session_id"),
                    row.get("role"),
                    row.get("content"),
                    row.get("card_id"),
                    row.get("payload_json") or "{}",
                    row.get("created_at"),
                ),
            )
        for row in _snapshot_list(snapshot, "cards"):
            conn.execute(
                """
                INSERT INTO agent_dialogue_cards (
                    card_id, session_id, kind, status, title, prompt, statement,
                    topic, polarity, source_text, payload_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("card_id"),
                    row.get("session_id"),
                    row.get("kind"),
                    row.get("status"),
                    row.get("title"),
                    row.get("prompt"),
                    row.get("statement"),
                    row.get("topic"),
                    row.get("polarity"),
                    row.get("source_text"),
                    row.get("payload_json") or "{}",
                    row.get("created_at"),
                    row.get("updated_at"),
                ),
            )
        for row in _snapshot_list(snapshot, "signals"):
            conn.execute(
                """
                INSERT INTO agent_dialogue_signals (
                    id, session_id, user_id, kind, topic, statement,
                    confidence, status, source_text, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("id"),
                    row.get("session_id"),
                    row.get("user_id"),
                    row.get("kind"),
                    row.get("topic"),
                    row.get("statement"),
                    row.get("confidence"),
                    row.get("status"),
                    row.get("source_text"),
                    row.get("created_at"),
                ),
            )

    def _snapshot_rows(self, conn: Any, table: str, session_id: str) -> list[dict[str, Any]]:
        order_column = "id" if table != "agent_dialogue_cards" else "created_at"
        rows = conn.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE session_id = ?
            ORDER BY {order_column} ASC
            """,
            (session_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _get_or_create_session(self, conn: Any, *, session_id: str | None) -> Any:
        if session_id:
            self._lock_session(conn, session_id)
        else:
            begin_write(conn, namespace="dialogue-latest", key=self.user_id)
        session = self._load_session(conn, session_id) if session_id else self._latest_session(conn)
        if session is not None:
            self._lock_session(conn, session["session_id"])
            fresh = self._load_session(conn, session["session_id"])
            if fresh is not None:
                return fresh
        return self._create_session(conn)

    def _lock_session(self, conn: Any, session_id: str) -> None:
        begin_write(conn, namespace="dialogue-session", key=json.dumps([self.user_id, session_id]))

    def _latest_session(self, conn: Any) -> Any:
        return conn.execute(
            """
            SELECT *
            FROM agent_dialogue_sessions
            WHERE user_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (self.user_id,),
        ).fetchone()

    def _load_session(self, conn: Any, session_id: str | None) -> Any:
        if not session_id:
            return None
        return conn.execute(
            """
            SELECT *
            FROM agent_dialogue_sessions
            WHERE session_id = ? AND user_id = ?
            """,
            (session_id, self.user_id),
        ).fetchone()

    def _create_session(self, conn: Any) -> Any:
        begin_write(conn, namespace="dialogue-latest", key=self.user_id)
        now = _utc_now()
        session_id = f"agent-dialogue:{uuid4().hex}"
        conn.execute(
            """
            INSERT INTO agent_dialogue_sessions (
                session_id, user_id, state, focus, created_at, updated_at, pending_context_json
            )
            VALUES (?, ?, 'probing', 'onboarding', ?, ?, '{}')
            """,
            (session_id, self.user_id, now, now),
        )
        self._insert_turn(
            conn,
            session_id,
            "assistant",
            "你好，我在。",
        )
        self._touch_session(
            conn,
            session_id,
            state="chatting",
            focus="新的聊天",
            pending_context={},
        )
        return self._load_session(conn, session_id)

    def _recent_user_context(self, session_id: str, limit: int = 4) -> list[str]:
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT content
                FROM agent_dialogue_turns
                WHERE session_id = ? AND role = 'user'
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [str(row["content"]) for row in reversed(rows)]

    def _recent_turn_context(
        self, session_id: str, limit: int = CHAT_LLM_HISTORY_LIMIT
    ) -> list[dict[str, str]]:
        bounded_limit = min(max(int(limit or CHAT_LLM_HISTORY_LIMIT), 2), 20)
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT role, content
                FROM agent_dialogue_turns
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, bounded_limit),
            ).fetchall()
        return [
            {"role": str(row["role"] or ""), "content": str(row["content"] or "")}
            for row in reversed(rows)
            if str(row["content"] or "").strip()
        ]

    def _insert_card(
        self,
        conn: Any,
        session_id: str,
        *,
        kind: str,
        title: str,
        prompt: str,
        statement: str,
        topic: str,
        polarity: str,
        source_text: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._lock_session(conn, session_id)
        now = _utc_now()
        card_id = f"agent-card:{uuid4().hex}"
        conn.execute(
            """
            INSERT INTO agent_dialogue_cards (
                card_id, session_id, kind, status, title, prompt, statement,
                topic, polarity, source_text, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_id,
                session_id,
                kind,
                title[:80],
                prompt[:500],
                statement[:1000],
                topic[:80],
                polarity,
                source_text[:1000],
                json.dumps(payload or {}, ensure_ascii=False),
                now,
                now,
            ),
        )
        return {
            "card_id": card_id,
            "session_id": session_id,
            "kind": kind,
            "status": "pending",
            "title": title,
            "prompt": prompt,
            "statement": statement,
            "topic": topic,
            "polarity": polarity,
            "source_text": source_text,
            "payload_json": json.dumps(payload or {}, ensure_ascii=False),
            "created_at": now,
            "updated_at": now,
        }

    def _load_card(self, conn: Any, card_id: str | None, *, for_update: bool = False) -> Any:
        if not card_id:
            return None
        row = conn.execute(
            """
            SELECT c.*
            FROM agent_dialogue_cards c
            JOIN agent_dialogue_sessions s ON s.session_id = c.session_id
            WHERE c.card_id = ? AND s.user_id = ?
            """,
            (card_id, self.user_id),
        ).fetchone()
        if row is not None and for_update:
            self._lock_session(conn, row["session_id"])
            return self._load_card(conn, card_id)
        return row

    def _update_card(
        self,
        conn: Any,
        card_id: str,
        *,
        status: str,
        payload: dict[str, Any],
    ) -> None:
        if self._load_card(conn, card_id, for_update=True) is None:
            raise KeyError(card_id)
        conn.execute(
            """
            UPDATE agent_dialogue_cards
            SET status = ?, payload_json = ?, updated_at = ?
            WHERE card_id = ?
            """,
            (status, json.dumps(payload, ensure_ascii=False), _utc_now(), card_id),
        )

    def _insert_turn(
        self,
        conn: Any,
        session_id: str,
        role: str,
        content: str,
        *,
        card_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._lock_session(conn, session_id)
        conn.execute(
            """
            INSERT INTO agent_dialogue_turns (
                session_id, role, content, card_id, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                role,
                content[:2000],
                card_id,
                json.dumps(payload or {}, ensure_ascii=False),
                _utc_now(),
            ),
        )

    def _touch_session(
        self,
        conn: Any,
        session_id: str,
        *,
        state: str,
        focus: str,
        pending_context: dict[str, Any],
    ) -> None:
        self._lock_session(conn, session_id)
        conn.execute(
            """
            UPDATE agent_dialogue_sessions
            SET state = ?, focus = ?, pending_context_json = ?, updated_at = ?
            WHERE session_id = ?
            """,
            (
                state,
                (focus or "general")[:80],
                json.dumps(pending_context, ensure_ascii=False),
                _utc_now(),
                session_id,
            ),
        )

    def _pending_cards(self, conn: Any, session_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT *
            FROM agent_dialogue_cards
            WHERE session_id = ?
            ORDER BY
                CASE status
                    WHEN 'pending' THEN 0
                    WHEN 'discussing' THEN 1
                    WHEN 'failed' THEN 2
                    WHEN 'deferred' THEN 3
                    ELSE 4
                END,
                updated_at DESC
            LIMIT ?
            """,
            (session_id, VISIBLE_CARD_LIMIT),
        ).fetchall()
        return [_serialize_card(row) for row in rows]
