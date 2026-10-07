"""Monotonic read models derived only from accepted, user-scoped Feed events."""

from __future__ import annotations


def project_event(conn, *, user_id, item, payload, now):
    kind = payload["type"]
    content_id = item["content_id"]
    if kind not in {"impression", "play_started", "watch_progress", "complete"}:
        return
    conn.execute(
        """INSERT OR IGNORE INTO feed_user_content(user_id,content_id)
        VALUES (?,?)""",
        (user_id, content_id),
    )
    if kind == "impression":
        conn.execute(
            """UPDATE feed_user_content SET last_exposed_at=MAX(last_exposed_at,?),
            last_event_at=MAX(last_event_at,?) WHERE user_id=? AND content_id=?""",
            (now, now, user_id, content_id),
        )
        return
    playback_id = payload["playbackId"]
    old = conn.execute(
        "SELECT * FROM feed_playback_progress WHERE playback_id=?", (playback_id,)
    ).fetchone()
    previous = float(old["watch_ms"]) if old else 0
    watched = max(previous, float(payload.get("watchMs", 0)))
    buffering = max(float(old["buffering_ms"]) if old else 0, float(payload.get("bufferingMs", 0)))
    startup = old["startup_ms"] if old else None
    if startup is None and payload.get("startupMs") is not None:
        startup = float(payload["startupMs"])
    completed = max(int(old["completed"]) if old else 0, int(kind == "complete"))
    conn.execute(
        """INSERT INTO feed_playback_progress
        (playback_id,user_id,content_id,watch_ms,buffering_ms,startup_ms,completed,last_event_at)
        VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(playback_id) DO UPDATE SET
        watch_ms=excluded.watch_ms,buffering_ms=excluded.buffering_ms,
        startup_ms=excluded.startup_ms,completed=excluded.completed,last_event_at=excluded.last_event_at""",
        (playback_id, user_id, content_id, watched, buffering, startup, completed, now),
    )
    conn.execute(
        """UPDATE feed_user_content SET watch_ms=watch_ms+?,
        last_watched_at=CASE WHEN ?>0 THEN MAX(last_watched_at,?) ELSE last_watched_at END,
        completed=MAX(completed,?),last_event_at=MAX(last_event_at,?)
        WHERE user_id=? AND content_id=?""",
        (watched - previous, watched, now, completed, now, user_id, content_id),
    )
