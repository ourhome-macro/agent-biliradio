from __future__ import annotations

import hashlib
import json
import math
import time
from uuid import uuid4

from database import begin_write, get_connection
from error_code import APIError, ErrorCode
from models import Track
from music_profile import MusicProfile
from request_spec import RequestInterpreter, RequestSpec

from agent_memory_runtime.telemetry import span

from .metrics import count
from .repository import encode


class FeedService:
    def __init__(self, repo, config, cache, *, recommendations=None):
        self.repo, self.config, self.cache, self.recommendations = (
            repo,
            config,
            cache,
            recommendations,
        )

    def _sync(self, user_id, spec):
        if self.recommendations:
            for candidate in self.recommendations.candidate_pool.list_ready(spec):
                # Search supply is public; imported personal libraries remain scoped.
                scope = (
                    "public"
                    if candidate.source in {"discovery_search", "related", "search"}
                    else f"user:{user_id}"
                )
                self.repo.admit(
                    candidate.track,
                    user_id=user_id,
                    scope=scope,
                    facets=candidate.facets,
                    evidence=candidate.evidence,
                )

    def create(self, user_id, *, mode="personal", request_text="", request_key):
        if (
            not isinstance(mode, str)
            or mode not in {"personal", "new", "following"}
            or not isinstance(request_text, str)
            or len(request_text) > 2000
            or not request_key
            or len(request_key) > 180
        ):
            raise APIError.validation_error("Invalid feed request or Idempotency-Key")
        spec = RequestInterpreter().interpret(request_text)
        legacy_snapshot = (
            self.recommendations.feed_legacy_snapshot() if self.recommendations else {}
        )
        profile = (
            self.recommendations.feed_snapshot(legacy_snapshot=legacy_snapshot)
            if self.recommendations
            else MusicProfile.empty()
        )
        now = time.time()
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="feed-command", key=encode([user_id, request_key]))
            old = conn.execute(
                "SELECT * FROM feed_sessions WHERE user_id=? AND request_key=?",
                (user_id, request_key),
            ).fetchone()
            if old:
                if old["mode"] != mode or json.loads(old["spec_json"]) != spec.to_dict():
                    raise APIError.conflict("Idempotency key is bound to another feed request")
                session_id = old["session_id"]
            else:
                session_id, revision = uuid4().hex, uuid4().hex
                snapshot = profile.to_dict()
                snapshot["legacy_snapshot"] = legacy_snapshot
                if mode == "following":
                    from .following_feed import following_snapshot

                    snapshot["following"] = following_snapshot(self.repo.db_path, user_id)
                version = hashlib.sha256(encode(snapshot).encode()).hexdigest()
                conn.execute(
                    "INSERT INTO feed_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        session_id,
                        user_id,
                        request_key,
                        mode,
                        encode(spec.to_dict()),
                        encode(snapshot),
                        version,
                        "music-feed-v1",
                        revision,
                        now + self.config.session_seconds,
                        now,
                    ),
                )
        return self.page(user_id, session_id, cursor=None)

    def page(self, user_id, session_id, *, cursor=None):
        with get_connection(self.repo.db_path) as conn:
            session = conn.execute(
                "SELECT * FROM feed_sessions WHERE session_id=? AND user_id=?",
                (session_id, user_id),
            ).fetchone()
            if not session:
                raise APIError.not_found("Feed session not found")
            session = dict(session)
            if session["expires_at"] <= time.time():
                raise APIError(ErrorCode.CONFLICT, "Feed session expired; refresh the feed", 410)
            if cursor:
                page = conn.execute(
                    "SELECT * FROM feed_pages WHERE session_id=? AND cursor=?", (session_id, cursor)
                ).fetchone()
                if page:
                    return self._render(user_id, session, dict(page))
                previous = conn.execute(
                    "SELECT * FROM feed_pages WHERE session_id=? AND next_cursor=?",
                    (session_id, cursor),
                ).fetchone()
                if not previous:
                    raise APIError.validation_error("Invalid feed cursor")
                number = previous["page_number"] + 1
            else:
                number = 0
                page = conn.execute(
                    "SELECT * FROM feed_pages WHERE session_id=? AND page_number=0", (session_id,)
                ).fetchone()
                if page:
                    return self._render(user_id, session, dict(page))
            seen = {
                row[0]
                for row in conn.execute(
                    "SELECT c.track_id FROM feed_items i JOIN feed_content c "
                    "ON c.content_id=i.content_id WHERE i.session_id=?",
                    (session_id,),
                )
            }
        spec = RequestSpec.from_dict(json.loads(session["spec_json"]))
        if session["mode"] == "following":
            from .following_feed import following_page

            return following_page(self, user_id, session, number, cursor, spec)
        self._sync(user_id, spec)
        from .recommendation import catalog_exclusions, deduplicate_catalog, distinct_catalog_count

        rows = []
        seen_since = time.time() - self.config.seen_cooldown_seconds
        identity_exclusions = catalog_exclusions(
            user_id=user_id,
            session_id=session_id,
            db_path=self.repo.db_path,
            seen_since=seen_since,
        )
        profile_snapshot = json.loads(session["profile_json"])
        frozen_profile = MusicProfile.from_dict(profile_snapshot, source="feed_snapshot")
        # Exclude issued/seen records in SQL before applying a candidate window.
        # Scan bounded windows for strict conditions, rather than starving behind 500 old rows.
        for offset in range(0, 5000, 500):
            batch = self.repo.list_content(
                user_id, session_id=session_id, seen_since=seen_since, offset=offset
            )
            matching = [
                row
                for row in batch
                if spec.matches_candidate(
                    json.loads(row["metadata_json"]), json.loads(row["facets_json"])
                )
            ]
            if session["mode"] == "personal" and self.recommendations:
                allowed = self.recommendations.eligible_feed_catalog(
                    profile=frozen_profile,
                    request_spec=spec,
                    legacy_snapshot=profile_snapshot.get("legacy_snapshot", {}),
                    catalog=[
                        (
                            Track.from_dict(json.loads(row["metadata_json"])),
                            json.loads(row["facets_json"]),
                        )
                        for row in matching
                    ],
                )
                matching = [
                    row for row, accepted in zip(matching, allowed, strict=True) if accepted
                ]
            rows.extend(matching)
            if (
                distinct_catalog_count(rows, excluded=identity_exclusions) >= 100
                or len(batch) < 500
            ):
                break
        rows = deduplicate_catalog(
            sorted(rows, key=lambda row: row["asset_status"] == "ready", reverse=True),
            excluded=identity_exclusions,
        )
        decisions = {}
        if session["mode"] == "personal" and self.recommendations:
            candidates, _diagnostics = self.recommendations.decide_feed(
                profile=frozen_profile,
                request_spec=spec,
                limit=min(200, max(40, len(rows))),
                excluded=seen,
                trace_id=f"feed:{session_id}:{number}",
                legacy_snapshot=profile_snapshot.get("legacy_snapshot"),
                catalog=[
                    (
                        Track.from_dict(json.loads(row["metadata_json"])),
                        json.loads(row["facets_json"]),
                    )
                    for row in rows
                ],
            )
            decisions = {c.track["trackId"]: c.to_dict() for c in candidates}
            ranks = {c.track["trackId"]: i for i, c in enumerate(candidates)}
            rows = [row for row in rows if row["track_id"] in decisions]
            rows.sort(
                key=lambda r: (
                    r["asset_status"] == "ready",
                    -ranks[r["track_id"]],
                    r["admitted_at"],
                    r["content_id"],
                ),
                reverse=True,
            )
        else:
            rows.sort(
                key=lambda r: (r["asset_status"] == "ready", r["admitted_at"], r["content_id"]),
                reverse=True,
            )
        from .recommendation import rank_catalog

        rows = rank_catalog(
            rows,
            user_id=user_id,
            session_id=session_id,
            db_path=self.repo.db_path,
            mode=session["mode"],
            page_number=number,
            exploration_ratio=self.config.exploration_ratio,
            seen_since=seen_since,
        )
        selected = rows[:10]
        supply = "available" if len(rows) > 10 else "exhausted"
        if len(rows) <= 10 and self.recommendations and self.recommendations.auto_discovery:
            self.recommendations.enqueue_discovery(scene="feed", limit=10, request_spec=spec)
            supply = "replenishing"
        now, page_id = time.time(), uuid4().hex
        with span("feed.page", attributes={"feed.mode": session["mode"]}):
            with get_connection(self.repo.db_path) as conn:
                begin_write(conn, namespace="feed-page", key=session_id)
                existing = conn.execute(
                    "SELECT * FROM feed_pages WHERE session_id=? AND page_number=?",
                    (session_id, number),
                ).fetchone()
                if existing:
                    page = dict(existing)
                else:
                    # Recheck expiry and issued content under the writer.
                    current_session = conn.execute(
                        "SELECT expires_at FROM feed_sessions WHERE session_id=?", (session_id,)
                    ).fetchone()
                    if current_session[0] <= now:
                        raise APIError(ErrorCode.CONFLICT, "Feed session expired", 410)
                    page_cursor = cursor or uuid4().hex
                    next_cursor = uuid4().hex
                    conn.execute(
                        "INSERT INTO feed_pages VALUES (?,?,?,?,?,?,?)",
                        (page_id, session_id, number, page_cursor, next_cursor, supply, now),
                    )
                    for rank, row in enumerate(selected, 1):
                        current = conn.execute(
                            "SELECT status FROM feed_content WHERE content_id=?",
                            (row["content_id"],),
                        ).fetchone()
                        if current[0] != "admitted":
                            continue
                        snapshot = {
                            "decisionTraceId": f"feed:{session_id}:{number}",
                            "profileVersion": session["profile_version"],
                            "policyVersion": session["policy_version"],
                            "decision": decisions.get(row["track_id"], {}),
                            "source": "bili",
                            "feedPolicy": row.get("_feed_policy", {}),
                        }
                        conn.execute(
                            "INSERT OR IGNORE INTO feed_items VALUES (?,?,?,?,?,?,?)",
                            (
                                uuid4().hex,
                                session_id,
                                page_id,
                                row["content_id"],
                                rank,
                                encode(snapshot),
                                now,
                            ),
                        )
                    page = dict(
                        conn.execute(
                            "SELECT * FROM feed_pages WHERE page_id=?", (page_id,)
                        ).fetchone()
                    )
        count("page")
        return self._render(user_id, session, page)

    def _render(self, user_id, session, page):
        remaining = max(0.1, session["expires_at"] - time.time())
        key = f"page:{user_id}:{session['session_id']}:{page['page_number']}"

        def load():
            with get_connection(self.repo.db_path) as conn:
                return {
                    "items": [
                        dict(r)
                        for r in conn.execute(
                            "SELECT * FROM feed_items WHERE page_id=? ORDER BY rank",
                            (page["page_id"],),
                        )
                    ]
                }

        issued = self.cache.get(key, load, hard=remaining, soft=remaining)["items"]
        items = []
        following = None
        if session["mode"] == "following":
            from .following_feed import render_state

            following = render_state(self.repo.db_path, user_id, session)
        with get_connection(self.repo.db_path) as conn:
            bitmap_user = conn.execute(
                "SELECT bitmap_id FROM reaction_bitmap_users WHERE user_id=?",
                (user_id,),
            ).fetchone()
            bitmap_id = int(bitmap_user[0]) if bitmap_user else None
            for item in issued:
                # Cache never authorizes visibility or reproduces a user's current dislike.
                row = conn.execute(
                    """SELECT c.*,a.status AS asset_status,a.manifest_json,
                    r.state AS reaction,r.version AS reaction_version,
                    k.likes,k.dislikes,k.version AS counter_version
                    FROM feed_content c LEFT JOIN content_media_links l ON l.content_id=c.content_id
                    LEFT JOIN media_assets a ON a.asset_id=l.asset_id
                    LEFT JOIN content_reactions r ON r.content_id=c.content_id AND r.user_id=?
                    LEFT JOIN content_counters k ON k.content_id=c.content_id
                    WHERE c.content_id=?""",
                    (user_id, item["content_id"]),
                ).fetchone()
                suppressed = (
                    not row
                    or row["status"] != "admitted"
                    or (row["scope"] != "public" and row["owner_id"] != user_id)
                    or row["reaction"] == "dislike"
                    or row["asset_status"] in {"unsupported", "retired"}
                )
                if following is not None and row:
                    creator = conn.execute(
                        "SELECT creator_id FROM creator_content WHERE content_id=?",
                        (item["content_id"],),
                    ).fetchone()
                    suppressed = suppressed or not creator or creator[0] not in following["allowed"]
                output = {
                    "itemId": item["item_id"],
                    "contentId": item["content_id"],
                    "rank": item["rank"],
                    "suppressed": suppressed,
                }
                if not suppressed:
                    metadata = self.cache.get(
                        f"content:{row['content_id']}",
                        lambda r=row: json.loads(r["metadata_json"]),
                        version=row["version"],
                        shared=row["scope"] == "public",
                    )
                    manifest = json.loads(row["manifest_json"] or "{}")
                    relation = self.cache.reaction(
                        user_id,
                        row["content_id"],
                        lambda r=row: {
                            "state": r["reaction"] or "neutral",
                            "version": r["reaction_version"] or 0,
                        },
                        bitmap_id=bitmap_id,
                        expected_version=row["reaction_version"] or 0,
                    )
                    output.update(
                        track=metadata,
                        assetStatus=row["asset_status"] or "empty",
                        reaction=relation["state"],
                        relationVersion=relation["version"],
                        counts={
                            "likes": row["likes"] or 0,
                            "dislikes": row["dislikes"] or 0,
                            "version": row["counter_version"] or 0,
                        },
                        decisionTraceId=json.loads(item["snapshot_json"])["decisionTraceId"],
                        coverUrl=f"/api/content/{row['content_id']}/cover"
                        if manifest.get("files", {}).get("cover.jpg")
                        else None,
                    )
                    counters = self.cache.counter(
                        row["content_id"],
                        lambda r=row: {
                            "likes": r["likes"] or 0,
                            "dislikes": r["dislikes"] or 0,
                            "version": r["counter_version"] or 0,
                        },
                        min_version=row["counter_version"] or 0,
                    )
                    output["counts"] = {
                        key: counters.get(key, 0) for key in ("likes", "dislikes", "version")
                    }
                items.append(output)
        result = {
            "sessionId": session["session_id"],
            "revision": session["revision"],
            "mode": session["mode"],
            "items": items,
            "nextCursor": page["next_cursor"],
            "hasMore": page["supply_state"] != "exhausted",
            "supplyState": page["supply_state"],
            "expiresAt": session["expires_at"],
        }
        if following is not None:
            result["followingState"] = following["state"]
        return result

    def events(self, user_id, events):
        if not isinstance(events, list) or len(events) > 50:
            raise APIError.validation_error("Expected up to 50 feed events")
        accepted = []
        allowed = {
            "impression",
            "play_started",
            "watch_progress",
            "complete",
            "swipe_next",
            "playback_error",
            "interrupt",
            "autoplay_blocked",
        }
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="feed-events", key=user_id)
            for payload in events:
                if (
                    not isinstance(payload, dict)
                    or not isinstance(payload.get("type"), str)
                    or payload.get("type") not in allowed
                ):
                    raise APIError.validation_error("Invalid feed event")
                event_id = payload.get("eventId")
                if not isinstance(event_id, str) or not event_id or len(event_id) > 180:
                    raise APIError.validation_error("eventId is required")
                item = conn.execute(
                    """SELECT i.*,s.user_id,c.track_id,c.metadata_json FROM feed_items i
                    JOIN feed_sessions s ON s.session_id=i.session_id
                    JOIN feed_content c ON c.content_id=i.content_id
                    WHERE i.item_id=? AND s.user_id=?""",
                    (payload.get("itemId"), user_id),
                ).fetchone()
                if not item:
                    raise APIError.not_found("Feed item not found")
                now = time.time()
                raw = encode(payload)
                if len(raw) > 8192:
                    raise APIError.validation_error("Feed event is too large")
                digest = hashlib.sha256(raw.encode()).hexdigest()
                old = conn.execute(
                    "SELECT * FROM feed_events WHERE event_id=?", (event_id,)
                ).fetchone()
                if old:
                    if old["user_id"] != user_id or old["payload_hash"] != digest:
                        raise APIError.conflict("Event identity changed")
                    accepted.append(event_id)
                    continue
                event_type = payload["type"]
                if event_type == "impression":
                    ratio = event_number(payload, "visibleRatio")
                    visible = event_number(payload, "visibleMs")
                    if (
                        not math.isfinite(ratio)
                        or not 0.5 <= ratio <= 1
                        or not math.isfinite(visible)
                        or not 1000 <= visible <= 86400000
                    ):
                        raise APIError.validation_error("Exposure threshold not reached")
                    if conn.execute(
                        "SELECT 1 FROM feed_events WHERE item_id=? AND event_type='impression'",
                        (item["item_id"],),
                    ).fetchone():
                        accepted.append(event_id)
                        continue
                if event_type in {"play_started", "watch_progress", "complete"}:
                    playback = conn.execute(
                        """SELECT p.*,a.manifest_json,a.status,j.complete_source
                        FROM media_playbacks p
                        JOIN content_media_links l ON l.asset_id=p.asset_id
                        JOIN media_assets a ON a.asset_id=p.asset_id
                        LEFT JOIN media_import_jobs j ON j.import_id=p.import_id
                        WHERE p.playback_id=? AND p.user_id=? AND l.content_id=?
                        AND COALESCE(p.content_id,a.content_id)=?""",
                        (
                            payload.get("playbackId"),
                            user_id,
                            item["content_id"],
                            item["content_id"],
                        ),
                    ).fetchone()
                    if not playback:
                        raise APIError.validation_error("Event does not belong to this playback")
                    if event_type == "play_started":
                        if playback["playable_at"] is None:
                            raise APIError.validation_error("Playback is not ready")
                    watched = event_number(payload, "watchMs")
                    for field in ("bufferingMs", "startupMs", "positionMs"):
                        if field in payload and not 0 <= event_number(payload, field) <= 86400000:
                            raise APIError.validation_error(f"Invalid {field}")
                    if (
                        not math.isfinite(watched)
                        or watched < 0
                        or watched > max(0, now - playback["created_at"]) * 2000 + 2000
                    ):
                        raise APIError.validation_error("Invalid watched duration")
                    if event_type == "complete":
                        duration = (
                            float(
                                json.loads(playback["manifest_json"]).get("duration")
                                or json.loads(item["metadata_json"]).get("duration")
                                or 0
                            )
                            * 1000
                        )
                        if (
                            not (playback["status"] == "ready" or playback["complete_source"])
                            or duration <= 0
                            or watched < duration * 0.9
                        ):
                            raise APIError.validation_error(
                                "Incomplete source or insufficient watching"
                            )
                    if (
                        event_type in {"play_started", "complete"}
                        and conn.execute(
                            """SELECT 1 FROM feed_events WHERE item_id=? AND event_type=?
                        AND json_extract(payload_json,'$.playbackId')=?""",
                            (item["item_id"], event_type, payload.get("playbackId")),
                        ).fetchone()
                    ):
                        accepted.append(event_id)
                        continue
                conn.execute(
                    "INSERT INTO feed_events VALUES (?,?,?,?,?,?,?,?)",
                    (event_id, user_id, item["item_id"], event_type, digest, raw, now, now),
                )
                accepted.append(event_id)
                from .engagement import project_event

                project_event(conn, user_id=user_id, item=dict(item), payload=payload, now=now)
                # Only grounded positive evidence enters the existing memory outbox.
                if event_type in {"play_started", "complete"}:
                    from durable_jobs import enqueue_behavior

                    enqueue_behavior(
                        conn,
                        user_id=user_id,
                        event="completed" if event_type == "complete" else "played",
                        scene="feed",
                        track=Track.from_dict(json.loads(item["metadata_json"])),
                        event_id=event_id,
                        payload={
                            "contentId": item["content_id"],
                            "feedItemId": item["item_id"],
                            "playedSeconds": float(payload.get("watchMs", 0)) / 1000,
                            "completed": event_type == "complete",
                        },
                    )
                count("event", event_type)
        return {"acceptedEventIds": accepted}


def event_number(payload, field):
    value = payload.get(field, 0)
    if type(value) not in (int, float):
        raise APIError.validation_error(f"{field} must be a finite number")
    try:
        number = float(value)
    except OverflowError:
        raise APIError.validation_error(f"{field} must be a finite number") from None
    if not math.isfinite(number):
        raise APIError.validation_error(f"{field} must be a finite number")
    return number
