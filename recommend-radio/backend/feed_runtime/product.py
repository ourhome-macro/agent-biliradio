from __future__ import annotations

import base64
import json
import math
import time
from uuid import uuid4

from database import get_connection
from error_code import APIError

from .repository import encode


class FeedProductService:
    def __init__(self, repo, config):
        self.repo, self.config = repo, config

    def detail(self, user_id, content_id):
        with get_connection(self.repo.db_path) as conn:
            row = conn.execute(
                """SELECT c.*,a.status AS asset_status,a.manifest_json,
                r.state,r.version AS relation_version,p.version AS projected_version,
                k.likes,k.dislikes,k.version AS counter_version FROM feed_content c
                LEFT JOIN content_media_links l ON l.content_id=c.content_id
                LEFT JOIN media_assets a ON a.asset_id=l.asset_id
                LEFT JOIN content_reactions r ON r.content_id=c.content_id AND r.user_id=?
                LEFT JOIN reaction_projection p ON p.content_id=c.content_id AND p.user_id=?
                LEFT JOIN content_counters k ON k.content_id=c.content_id
                WHERE c.content_id=?""",
                (user_id, user_id, content_id),
            ).fetchone()
            self.repo.authorize(row, user_id)
        manifest = json.loads(row["manifest_json"] or "{}")
        return {
            "contentId": content_id,
            "track": json.loads(row["metadata_json"]),
            "assetStatus": row["asset_status"] or "empty",
            "reaction": row["state"] or "neutral",
            "relationVersion": row["relation_version"] or 0,
            "counts": {
                "likes": row["likes"] or 0,
                "dislikes": row["dislikes"] or 0,
                "version": row["counter_version"] or 0,
            },
            "projectionPending": (row["projected_version"] or 0) < (row["relation_version"] or 0),
            "sharePath": f"/#/feed?content={content_id}" if row["scope"] == "public" else None,
            "coverUrl": f"/api/content/{content_id}/cover"
            if manifest.get("files", {}).get("cover.jpg")
            else None,
        }

    def open(self, user_id, content_id):
        detail = self.detail(user_id, content_id)
        if detail["assetStatus"] in {"unsupported", "retired"}:
            raise APIError.not_found("Content is unavailable")
        session, page, item, cursor, next_cursor = [str(uuid4()) for _ in range(5)]
        now, revision = time.time(), str(uuid4())
        expires = now + self.config.session_seconds
        with get_connection(self.repo.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.repo.authorize(
                conn.execute(
                    "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
                ).fetchone(),
                user_id,
            )
            conn.execute(
                """INSERT INTO feed_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    session,
                    user_id,
                    f"open:{session}",
                    "new",
                    "{}",
                    "{}",
                    "explicit",
                    "explicit-replay-v1",
                    revision,
                    expires,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO feed_pages VALUES (?,?,?,?,?,?,?)",
                (page, session, 0, cursor, next_cursor, "exhausted", now),
            )
            conn.execute(
                "INSERT INTO feed_items VALUES (?,?,?,?,?,?,?)",
                (
                    item,
                    session,
                    page,
                    content_id,
                    0,
                    encode({"decisionTraceId": f"explicit:{session}"}),
                    now,
                ),
            )
        return {
            "sessionId": session,
            "revision": revision,
            "mode": "new",
            "items": [{**detail, "itemId": item, "rank": 0, "suppressed": False}],
            "nextCursor": next_cursor,
            "hasMore": False,
            "supplyState": "exhausted",
            "expiresAt": expires,
        }

    def history(self, user_id, *, cursor=None, limit=20):
        if type(limit) is not int or not 1 <= limit <= 50:
            raise APIError.validation_error("limit must be between 1 and 50")
        before, last_id = float("inf"), ""
        if cursor:
            try:
                if not isinstance(cursor, str) or len(cursor) > 2048:
                    raise ValueError
                before, last_id = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if (
                    type(before) not in {int, float}
                    or not math.isfinite(before)
                    or not isinstance(last_id, str)
                ):
                    raise ValueError
            except (ValueError, TypeError, UnicodeError):
                raise APIError.validation_error("Invalid history cursor") from None
        with get_connection(self.repo.db_path) as conn:
            rows = conn.execute(
                """WITH visits AS (
                    SELECT i.content_id,json_extract(e.payload_json,'$.playbackId') AS playback_id,
                    MAX(CAST(json_extract(e.payload_json,'$.watchMs') AS INTEGER)) AS watch_ms,
                    MAX(e.recorded_at) AS watched_at,MAX(e.event_type='complete') AS completed
                    FROM feed_events e JOIN feed_items i ON i.item_id=e.item_id
                    WHERE e.user_id=? AND e.event_type IN ('watch_progress','complete')
                    GROUP BY i.content_id,playback_id
                ), history AS (
                    SELECT content_id,SUM(watch_ms) AS watch_ms,MAX(watched_at) AS watched_at,
                    MAX(completed) AS completed FROM visits WHERE watch_ms>0 GROUP BY content_id
                ) SELECT h.*,c.metadata_json FROM history h JOIN feed_content c
                ON c.content_id=h.content_id WHERE c.status='admitted'
                AND (c.scope='public' OR c.owner_id=?)
                AND (h.watched_at<? OR (h.watched_at=? AND h.content_id>?))
                ORDER BY h.watched_at DESC,h.content_id ASC LIMIT ?""",
                (user_id, user_id, before, before, last_id, limit + 1),
            ).fetchall()
        entries = [
            {
                "contentId": row["content_id"],
                "track": json.loads(row["metadata_json"]),
                "watchMs": row["watch_ms"],
                "watchedAt": row["watched_at"],
                "completed": bool(row["completed"]),
            }
            for row in rows[:limit]
        ]
        next_cursor = None
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = base64.urlsafe_b64encode(
                encode([tail["watched_at"], tail["content_id"]]).encode()
            ).decode()
        return {"items": entries, "nextCursor": next_cursor}
