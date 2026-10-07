"""Durable per-user Auto Dream trigger state."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from agent_memory_runtime.domain.event import Event
from agent_memory_runtime.memory.intake.worker import SQLiteDreamStore


class AutoDreamTriggers:
    def __init__(self, store: SQLiteDreamStore, *, timezone: str = "Asia/Shanghai") -> None:
        self.store = store
        self.timezone = ZoneInfo(timezone)
        with self.store._manager.connection() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS dream_trigger_state (
                    scope_key TEXT PRIMARY KEY, payload TEXT NOT NULL
                )"""
            )

    def record_event(self, event: Event) -> tuple[str, ...]:
        if event.user_id is None:
            return ()
        now = datetime.now(UTC)
        scope = self._scope(event.tenant_id, event.user_id, event.agent_id)
        with self.store._manager.connection() as connection:
            state = self._get(connection, scope)
            if event.sequence <= state["last_sequence"]:
                return ()
            kind = str(event.payload.get("event") or "")
            meaningful = kind in {
                "skipped",
                "dismissed",
                "dislike",
                "liked",
                "collection_added",
                "completed",
                "track_reviewed",
                "search",
                "profile_statement",
            }
            state["last_sequence"] = event.sequence
            if kind != "shown":
                state["last_activity_at"] = now.isoformat()
            if meaningful:
                state["unprocessed_count"] += 1
                if state["last_batch_at"] is None:
                    state["last_batch_at"] = now.isoformat()
            listen_ms = event.payload.get("listenMs")
            if listen_ms is None and event.payload.get("playedSeconds") is not None:
                try:
                    listen_ms = int(event.payload["playedSeconds"]) * 1000
                except (TypeError, ValueError):
                    listen_ms = None
            try:
                quick_skip = kind == "skipped" and listen_ms is not None and int(listen_ms) < 15_000
            except (TypeError, ValueError):
                quick_skip = False
            if quick_skip:
                previous = _parse(state.get("last_skip_at"))
                track = event.payload.get("track")
                track_id = str(
                    event.payload.get("trackId")
                    or (track.get("trackId") if isinstance(track, dict) else "")
                    or event.event_id
                )
                if track_id != state.get("last_skip_track"):
                    state["skip_streak"] = (
                        state["skip_streak"] + 1
                        if previous and now - previous <= timedelta(minutes=2)
                        else 1
                    )
                state["last_skip_track"] = track_id
                state["last_skip_at"] = now.isoformat()
            elif meaningful:
                state["skip_streak"] = 0

            modes: list[str] = []
            high_signal = state["skip_streak"] >= 3 or kind in {
                "dislike",
                "dismissed",
                "liked",
                "collection_added",
                "search",
            }
            last_micro = _parse(state.get("last_micro_at"))
            if high_signal:
                if (last_micro is None or now - last_micro >= timedelta(minutes=3)) and state[
                    "micro_count"
                ] < 3:
                    modes.append("micro")
                    state["last_micro_at"] = now.isoformat()
                    state["micro_count"] += 1
                    state["skip_streak"] = 0
                else:
                    state["pending_flush"] = True
            last_batch = _parse(state.get("last_batch_at"))
            batch_due = state["unprocessed_count"] >= 10 or (
                state["unprocessed_count"] > 0
                and last_batch is not None
                and now - last_batch >= timedelta(minutes=15)
            )
            if state["pending_flush"] and (
                last_micro is None or now - last_micro >= timedelta(minutes=3)
            ):
                batch_due = True
            if batch_due:
                modes.append("batch")
                state["unprocessed_count"] = 0
                state["last_batch_at"] = now.isoformat()
                state["pending_flush"] = False
            self._put(connection, scope, state)
            for mode in modes:
                self.store.schedule(
                    tenant_id=event.tenant_id,
                    user_id=event.user_id,
                    agent_id=event.agent_id,
                    mode=mode,
                    reason=f"trigger:{mode}",
                )
        return tuple(modes)

    def schedule_due(self) -> int:
        now = datetime.now(UTC)
        local = now.astimezone(self.timezone)
        count = 0
        with self.store._manager.connection() as connection:
            rows = connection.execute(
                "SELECT scope_key, payload FROM dream_trigger_state"
            ).fetchall()
            for scope, raw in rows:
                state = json.loads(raw)
                tenant_id, user_id, agent_id = json.loads(scope)
                modes: list[str] = []
                last_batch = _parse(state.get("last_batch_at"))
                last_micro = _parse(state.get("last_micro_at"))
                activity = _parse(state.get("last_activity_at"))
                if state["unprocessed_count"] > 0 and (
                    (last_batch is not None and now - last_batch >= timedelta(minutes=15))
                    or (
                        state["pending_flush"]
                        and last_micro is not None
                        and now - last_micro >= timedelta(minutes=3)
                    )
                ):
                    modes.append("batch")
                    state["unprocessed_count"] = 0
                    state["pending_flush"] = False
                    state["last_batch_at"] = now.isoformat()
                idle = activity is not None and now - activity >= timedelta(minutes=5)
                if idle and state.get("last_session_reset_at") != state["last_activity_at"]:
                    state["micro_count"] = 0
                    state["last_session_reset_at"] = state["last_activity_at"]
                last_deep = _parse(state.get("last_deep_at"))
                if (
                    idle
                    and state.get("last_deep_activity_at") != state["last_activity_at"]
                    and (last_deep is None or now - last_deep >= timedelta(hours=1))
                ):
                    modes.append("deep")
                    state["last_deep_activity_at"] = state["last_activity_at"]
                    state["last_deep_at"] = now.isoformat()
                if local.hour >= 3 and state.get("last_nightly_date") != local.date().isoformat():
                    if activity is not None and now - activity <= timedelta(days=30):
                        modes.append("deep")
                        state["last_deep_at"] = now.isoformat()
                        state["last_deep_activity_at"] = state["last_activity_at"]
                    state["last_nightly_date"] = local.date().isoformat()
                self._put(connection, scope, state)
                for mode in dict.fromkeys(modes):
                    self.store.schedule(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        agent_id=agent_id,
                        mode=mode,
                        reason=f"trigger:{mode}",
                    )
                    count += 1
        return count

    @staticmethod
    def _scope(tenant_id: str, user_id: str, agent_id: str | None) -> str:
        return json.dumps([tenant_id, user_id, agent_id], separators=(",", ":"))

    @staticmethod
    def _get(connection: object, scope: str) -> dict:
        row = connection.execute(
            "SELECT payload FROM dream_trigger_state WHERE scope_key = ?", (scope,)
        ).fetchone()
        if row is not None:
            return json.loads(row[0])
        return {
            "last_sequence": 0,
            "last_activity_at": None,
            "last_batch_at": None,
            "last_micro_at": None,
            "last_skip_at": None,
            "skip_streak": 0,
            "last_skip_track": None,
            "micro_count": 0,
            "unprocessed_count": 0,
            "pending_flush": False,
            "last_deep_activity_at": None,
            "last_deep_at": None,
            "last_session_reset_at": None,
            "last_nightly_date": None,
        }

    @staticmethod
    def _put(connection: object, scope: str, state: dict) -> None:
        connection.execute(
            "INSERT INTO dream_trigger_state(scope_key, payload) VALUES (?, ?) "
            "ON CONFLICT(scope_key) DO UPDATE SET payload = excluded.payload",
            (scope, json.dumps(state, sort_keys=True)),
        )


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
