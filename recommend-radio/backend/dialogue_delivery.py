"""Publish usable recommendation cards atomically with their business records."""

from __future__ import annotations


class DialogueProgress:
    def __init__(self, publisher, *, task_id, session_id, user_id):
        self.publisher = publisher
        self.identity = dict(task_id=task_id, session_id=session_id, user_id=user_id)

    def __call__(self, stage, payload):
        self.publisher.publish(
            **self.identity,
            event_type="progress",
            status="running",
            payload={"stage": stage, **dict(payload or {})},
        )

    def recommendation_ready(self, connection, session):
        # Uses the same connection: rollback removes both card and event/outbox.
        # This is a session update, never a premature task completion.
        last = (session.get("messages") or [{}])[-1]
        has_results = bool((last.get("card") or {}).get("recommendations"))
        self.publisher.publish(
            connection=connection,
            **self.identity,
            event_type="session",
            status="running",
            payload={"session": session, "firstRecommendation": has_results},
        )
