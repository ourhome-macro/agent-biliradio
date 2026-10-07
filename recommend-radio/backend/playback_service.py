from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from database import DEFAULT_DB_PATH, LEGACY_OWNER_USER_ID, get_connection, init_db
from error_code import APIError
from library_service import LibraryService, utc_now


RECENT_RECORD_RATIO = 0.1
QUICK_SKIP_MS = 15_000
COMPLETE_REMAINING_MS = 30_000


class PlaybackService:
    def __init__(
        self,
        db_path: Optional[Path | str] = None,
        user_id: str = LEGACY_OWNER_USER_ID,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.user_id = user_id
        init_db(self.db_path)
        self.library = LibraryService(self.db_path, user_id=self.user_id)

    def record_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        session_id = str(payload.get("sessionId") or payload.get("session_id") or "").strip()
        track_id = str(payload.get("trackId") or payload.get("track_id") or "").strip()
        event = str(payload.get("event") or "heartbeat").strip().lower()
        if not session_id:
            raise APIError.validation_error("sessionId is required")
        if not track_id:
            raise APIError.validation_error("trackId is required")

        track = self.library.get_track(track_id)
        if not track:
            raise APIError.not_found(f"Track not found: {track_id}")

        position_ms = max(int(payload.get("positionMs") or payload.get("position_ms") or 0), 0)
        listen_ms = max(int(payload.get("listenMs") or payload.get("listen_ms") or 0), 0)
        completed = bool(payload.get("completed")) or self._is_completed(
            position_ms=position_ms,
            duration_seconds=track.duration,
        )
        recent_threshold_ms = self._recent_threshold_ms(track.duration)
        qualifies_for_recent = listen_ms >= recent_threshold_ms or completed
        skipped = event in {"skip", "next", "change", "stop"} and listen_ms < QUICK_SKIP_MS and not completed
        now = utc_now()
        ended_at = now if event in {"pause", "end", "ended", "skip", "next", "stop"} else None

        with get_connection(self.db_path) as conn:
            from durable_jobs import enqueue_behavior
            import hashlib
            import json
            event_key = str(payload.get('eventId') or payload.get('event_id') or
                            hashlib.sha256(json.dumps([session_id,track_id,event,position_ms,
                                                       listen_ms,completed,skipped]).encode()).hexdigest())
            enqueue_behavior(conn, user_id=self.user_id,
                             event='completed' if completed else 'skipped' if skipped else 'played',
                             scene='playback', track=track, event_id=event_key,
                             payload={'sessionId': session_id, 'positionMs': position_ms,
                                      'listenMs': listen_ms, 'completed': completed, 'skipped': skipped})
            conn.execute(
                """
                INSERT INTO playback_sessions (
                    user_id, session_id, track_id, started_at, ended_at, last_position_ms,
                    listen_ms, completed, skipped, last_event
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, session_id) DO UPDATE SET
                    ended_at = COALESCE(excluded.ended_at, playback_sessions.ended_at),
                    last_position_ms = excluded.last_position_ms,
                    listen_ms = MAX(playback_sessions.listen_ms, excluded.listen_ms),
                    completed = excluded.completed,
                    skipped = excluded.skipped,
                    last_event = excluded.last_event
                """,
                (
                    self.user_id,
                    session_id,
                    track_id,
                    now,
                    ended_at,
                    position_ms,
                    listen_ms,
                    int(completed),
                    int(skipped),
                    event,
                ),
            )

            if event != "heartbeat":
                conn.execute(
                    """
                    INSERT INTO playback_events (
                        user_id, session_id, track_id, event,
                        position_ms, listen_ms, completed, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.user_id,
                        session_id,
                        track_id,
                        event,
                        position_ms,
                        listen_ms,
                        int(completed),
                        now,
                    ),
                )

            if qualifies_for_recent:
                conn.execute(
                    """
                    INSERT INTO playback_recent (
                        user_id, track_id, last_played_at,
                        position_ms, listen_ms, completed, skipped
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id, track_id) DO UPDATE SET
                        last_played_at = excluded.last_played_at,
                        position_ms = excluded.position_ms,
                        listen_ms = MAX(playback_recent.listen_ms, excluded.listen_ms),
                        completed = excluded.completed,
                        skipped = excluded.skipped
                    """,
                    (
                        self.user_id,
                        track_id,
                        now,
                        position_ms,
                        listen_ms,
                        int(completed),
                        int(skipped),
                    ),
                )

        if qualifies_for_recent:
            self.library.add_recent(track, position_ms, listen_ms, completed)

        return {
            "eventId": event_key,
            "sessionId": session_id,
            "trackId": track_id,
            "positionMs": position_ms,
            "listenMs": listen_ms,
            "completed": completed,
            "skipped": skipped,
            "event": event,
        }

    def list_recent(self, limit: int = 100) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT t.*, pr.last_played_at, pr.position_ms, pr.listen_ms,
                       pr.completed, pr.skipped
                FROM playback_recent pr
                JOIN tracks t ON t.track_id = pr.track_id
                WHERE pr.user_id = ? AND pr.skipped = 0
                ORDER BY pr.last_played_at DESC
                LIMIT ?
                """,
                (self.user_id, limit),
            ).fetchall()
        result = []
        for row in rows:
            track = self.library._track_from_row(row).to_dict()
            track.update(
                {
                    "lastPlayedAt": row["last_played_at"],
                    "positionMs": row["position_ms"],
                    "listenMs": row["listen_ms"],
                    "completed": bool(row["completed"]),
                    "skipped": bool(row["skipped"]),
                }
            )
            result.append(track)
        return result

    def get_resume(self, track_id: str) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT track_id, position_ms, listen_ms, completed, last_played_at
                FROM playback_recent
                WHERE user_id = ? AND track_id = ?
                """,
                (self.user_id, track_id),
            ).fetchone()
        if not row:
            return {
                "trackId": track_id,
                "positionMs": 0,
                "listenMs": 0,
                "completed": False,
                "lastPlayedAt": None,
            }
        return {
            "trackId": row["track_id"],
            "positionMs": row["position_ms"],
            "listenMs": row["listen_ms"],
            "completed": bool(row["completed"]),
            "lastPlayedAt": row["last_played_at"],
        }

    @staticmethod
    def _is_completed(position_ms: int, duration_seconds: int) -> bool:
        if duration_seconds <= 0:
            return False
        duration_ms = duration_seconds * 1000
        return position_ms >= duration_ms * 0.9 or (
            duration_ms > COMPLETE_REMAINING_MS and duration_ms - position_ms <= COMPLETE_REMAINING_MS
        )

    @staticmethod
    def _recent_threshold_ms(duration_seconds: int) -> int:
        if duration_seconds <= 0:
            return QUICK_SKIP_MS
        return max(1_000, int(duration_seconds * 1000 * RECENT_RECORD_RATIO))
