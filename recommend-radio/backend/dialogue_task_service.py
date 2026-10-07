from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any
from uuid import uuid4

from database import begin_write, get_connection
from durable_jobs import enqueue
from error_code import APIError
from sse_event_client import SSEEventPublisher

LOGGER = logging.getLogger("recommend-radio.dialogue-tasks")
DISCOVERY_POLL_SECONDS = 0.75
DISCOVERY_MAX_POLLS = 40


class DialogueTaskService:
    """Run dialogue work off-request and publish durable progress events."""

    def __init__(self, publisher: SSEEventPublisher) -> None:
        self.publisher = publisher

    def submit(
        self,
        *,
        service: Any,
        user_id: str,
        message: str,
        session_id: str | None,
        context_card_id: str | None,
        context_track_id: str | None,
        idempotency_key: str | None = None,
    ) -> dict[str, str]:
        normalized = str(message or "").strip()
        if not normalized:
            raise ValueError("message is required")
        key = hashlib.sha256(f"{user_id}:{idempotency_key}".encode()).hexdigest()
        task_id = f"dialogue:{key if idempotency_key else uuid4().hex}"
        if idempotency_key:
            with get_connection(service.db_path) as conn:
                old = conn.execute(
                    "SELECT * FROM durable_jobs WHERE job_id=? AND user_id=?", (task_id, user_id)
                ).fetchone()
            if old:
                previous = json.loads(old["payload_json"])
                if (
                    previous["message"] != normalized
                    or previous["context_card_id"] != context_card_id
                    or previous["context_track_id"] != context_track_id
                    or (session_id and previous["session_id"] != session_id)
                ):
                    raise ValueError("idempotency key is already bound to another request")
                return {
                    "taskId": task_id,
                    "sessionId": previous["session_id"],
                    "status": old["status"],
                }
        resolved_session_id = service.resolve_session_id(session_id=session_id)
        with get_connection(service.db_path) as conn:
            begin_write(conn, namespace="dialogue-submit", key=json.dumps([user_id, task_id]))
            # The preflight read can race another identical submit. Serialize the
            # command and reuse its committed session instead of binding it twice.
            old = conn.execute(
                "SELECT * FROM durable_jobs WHERE job_id=? AND user_id=?", (task_id, user_id)
            ).fetchone()
            if old:
                previous = json.loads(old["payload_json"])
                if (previous["message"] != normalized
                    or previous["context_card_id"] != context_card_id
                    or previous["context_track_id"] != context_track_id
                    or (session_id and previous["session_id"] != session_id)):
                    raise ValueError("idempotency key is already bound to another request")
                return {"taskId": task_id, "sessionId": previous["session_id"], "status": old["status"]}
            begin_write(conn, namespace="dialogue-session", key=json.dumps([user_id, resolved_session_id]))
            if conn.execute("SELECT 1 FROM agent_dialogue_sessions WHERE session_id=? AND user_id=?",
                            (resolved_session_id, user_id)).fetchone() is None:
                raise APIError.not_found("dialogue session not found")
            enqueue(
                conn,
                kind="dialogue",
                user_id=user_id,
                job_id=task_id,
                lane=f"dialogue:{user_id}:{resolved_session_id}",
                payload={
                    "session_id": resolved_session_id,
                    "message": normalized,
                    "context_card_id": context_card_id,
                    "context_track_id": context_track_id,
                },
            )
            self.publisher.publish(
                connection=conn,
                task_id=task_id,
                session_id=resolved_session_id,
                user_id=user_id,
                event_type="task",
                status="queued",
                payload={"status": "queued"},
            )

        return {"taskId": task_id, "sessionId": resolved_session_id, "status": "queued"}

    def _run(
        self,
        *,
        service: Any,
        user_id: str,
        task_id: str,
        session_id: str,
        message: str,
        context_card_id: str | None,
        context_track_id: str | None,
    ) -> None:
        def publish(event_type: str, payload: dict[str, Any], status: str = "running") -> None:
            self.publisher.publish(
                task_id=task_id,
                session_id=session_id,
                user_id=user_id,
                event_type=event_type,
                status=status,
                payload=payload,
            )

        try:
            from settings_service import SettingsService

            if not SettingsService(
                db_path=service.db_path, user_id=user_id
            ).has_deepseek_api_key():
                raise RuntimeError("Personal DeepSeek API Key is required")
            publish("progress", {"stage": "routing", "label": "正在理解你的需求"})
            result = service.send_message(
                message,
                session_id=session_id,
                context_card_id=context_card_id,
                context_track_id=context_track_id,
                progress=lambda stage, payload: publish(
                    "progress",
                    {"stage": stage, **dict(payload or {})},
                ),
            )
            publish("session", {"session": result})
            pending_cards = _pending_discovery_cards(result)
            if pending_cards:
                publish(
                    "discovery",
                    {"status": "running", "cardIds": [item["cardId"] for item in pending_cards]},
                )
                with get_connection(service.db_path) as conn:
                    enqueue(
                        conn,
                        kind="discovery_watch",
                        user_id=user_id,
                        job_id=f"watch:{task_id}",
                        retry_safe=True,
                        payload={"task_id": task_id, "session_id": session_id, "initial": result},
                    )
            publish(
                "done",
                {"session": result, "discoveryPending": bool(pending_cards)},
                status="streaming" if pending_cards else "completed",
            )
            return result
        except Exception as exc:
            LOGGER.exception("dialogue task failed", extra={"task_id": task_id})
            publish(
                "error",
                {"message": type(exc).__name__},
                status="failed",
            )
            raise

    def _watch_discovery(
        self,
        service: Any,
        user_id: str,
        task_id: str,
        session_id: str,
        initial: dict[str, Any],
    ) -> None:
        current = initial
        fingerprints: dict[str, tuple[str, int]] = {}
        for _attempt in range(DISCOVERY_MAX_POLLS):
            pending = _pending_discovery_cards(current)
            if not pending:
                self.publisher.publish(
                    task_id=task_id,
                    session_id=session_id,
                    user_id=user_id,
                    event_type="discovery",
                    status="completed",
                    payload={"status": "completed", "session": current},
                )
                return
            time.sleep(DISCOVERY_POLL_SECONDS)
            for card in pending:
                card_id = str(card["cardId"])
                try:
                    refreshed = service.refresh_recommendation_card(card_id)
                except Exception as exc:
                    LOGGER.warning("discovery card refresh failed for %s: %s", card_id, exc)
                    continue
                refreshed_card = _card_by_id(refreshed, card_id)
                fingerprint = (
                    str((refreshed_card or {}).get("discoveryStatus") or ""),
                    len((refreshed_card or {}).get("recommendations") or []),
                )
                if fingerprints.get(card_id) != fingerprint:
                    fingerprints[card_id] = fingerprint
                    self.publisher.publish(
                        task_id=task_id,
                        session_id=session_id,
                        user_id=user_id,
                        event_type="session",
                        status="running",
                        payload={"session": refreshed},
                    )
                current = refreshed
        self.publisher.publish(
            task_id=task_id,
            session_id=session_id,
            user_id=user_id,
            event_type="discovery",
            status="timeout",
            payload={"status": "timeout", "session": current},
        )

    def close(self) -> None:
        self.publisher.close()


def _pending_discovery_cards(session: dict[str, Any]) -> list[dict[str, Any]]:
    cards = session.get("cards") if isinstance(session, dict) else []
    if not isinstance(cards, list):
        return []
    return [
        card
        for card in cards
        if isinstance(card, dict)
        and card.get("kind") == "recommendation_carousel"
        and card.get("discoveryJobId")
        and card.get("discoveryStatus") not in {"completed", "failed", "needs_reconciliation"}
        and len(card.get("recommendations") or []) < 8
    ]


def _card_by_id(session: dict[str, Any], card_id: str) -> dict[str, Any] | None:
    cards = session.get("cards") if isinstance(session, dict) else []
    for card in cards if isinstance(cards, list) else []:
        if isinstance(card, dict) and card.get("cardId") == card_id:
            return card
    return None
