"""Compatibility publisher backed by the business database."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from database import DEFAULT_DB_PATH, get_connection
from sse_event_store import append_event


class SSEEventPublisher:
    def __init__(
        self,
        db_path: str | Path | None = None,
        url: str | None = None,
        token: str | None = None,
    ) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        configured_url = os.getenv("SSE_GATEWAY_INTERNAL_URL") or "" if url is None else url
        self.url = str(configured_url).rstrip("/")
        self.token = str(token or os.getenv("SSE_INTERNAL_TOKEN") or "local-sse-token")
        self.session = requests.Session() if self.url else None

    @property
    def enabled(self) -> bool:
        return True

    def publish(self, connection: Any = None, **event: Any) -> str:
        if self.url:
            from durable_jobs import enqueue

            if connection is not None:
                return enqueue(
                    connection, kind="sse", user_id=event["user_id"],
                    lane=f"sse:{event['user_id']}:{event['task_id']}",
                    payload=event, retry_safe=True,
                )
            with get_connection(self.db_path) as owned:
                return enqueue(
                    owned, kind="sse", user_id=event["user_id"],
                    lane=f"sse:{event['user_id']}:{event['task_id']}",
                    payload=event, retry_safe=True,
                )
        if connection is not None:
            return str(append_event(connection, **event))
        with get_connection(self.db_path) as owned:
            return str(append_event(owned, **event))

    def publish_http(self, **event: Any) -> int:
        if self.session is None:
            raise RuntimeError("legacy SSE gateway is not configured")
        response = self.session.post(
            self.url,
            json={
                "taskId": event["task_id"],
                "sessionId": event["session_id"],
                "userId": event["user_id"],
                "type": event["event_type"],
                "status": event.get("status", "running"),
                "payload": event.get("payload") or {},
            },
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=(2, 5),
        )
        response.raise_for_status()
        return int(response.json()["eventId"])

    def close(self) -> None:
        if self.session is not None:
            self.session.close()
