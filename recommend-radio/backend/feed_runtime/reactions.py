from __future__ import annotations

import hashlib
import os
import time

from database import begin_write, get_connection
from error_code import APIError
from models import Track

from .bitmap import user_bitmap_id
from .metrics import count
from .repository import encode


def feed_enabled():
    return os.getenv("FEED_ENABLED", "false").lower() in {"1", "true", "yes", "on"}


class ReactionService:
    def __init__(self, repo, cache=None):
        self.repo, self.cache = repo, cache

    def set(
        self,
        user_id,
        content_id,
        state,
        command_id,
        *,
        expected_version=None,
        require_admitted=True,
    ):
        if (
            not isinstance(state, str)
            or state not in {"like", "dislike", "neutral"}
            or not isinstance(command_id, str)
            or not command_id
            or len(command_id) > 180
        ):
            raise APIError.validation_error("Invalid reaction or Idempotency-Key")
        user_bitmap_id(self.repo.db_path, user_id, create=True)
        now = time.time()
        with get_connection(self.repo.db_path) as conn:
            # Serialize aliases of the same user's track and idempotency keys.
            begin_write(conn, namespace="reaction-user", key=user_id)
            content = conn.execute(
                "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
            ).fetchone()
            if not content or (content["scope"] != "public" and content["owner_id"] != user_id):
                raise APIError.not_found("Content not found")
            begin_write(conn, namespace="reaction-track", key=content["track_id"])
            if require_admitted and content["status"] != "admitted" and state != "neutral":
                raise APIError.not_found("Content is unavailable")
            previous = conn.execute(
                "SELECT * FROM reaction_commands WHERE user_id=? AND command_id=?",
                (user_id, command_id),
            ).fetchone()
            if previous:
                if previous["content_id"] != content_id or previous["desired_state"] != state:
                    raise APIError.conflict("Idempotency key is bound to another command")
                import json

                return json.loads(previous["result_json"])
            old = conn.execute(
                "SELECT * FROM content_reactions WHERE user_id=? AND content_id=?",
                (user_id, content_id),
            ).fetchone()
            old_state, version = (old["state"], old["version"]) if old else ("neutral", 0)
            if expected_version is not None and (
                type(expected_version) is not int or expected_version != version
            ):
                raise APIError.conflict("Reaction version changed")
            changed = old_state != state
            if changed:
                version += 1
                conn.execute(
                    """INSERT INTO content_reactions VALUES (?,?,?,?,?)
                    ON CONFLICT(user_id,content_id) DO UPDATE SET state=excluded.state,
                    version=excluded.version,updated_at=excluded.updated_at""",
                    (user_id, content_id, state, version, now),
                )
                if state == "like":
                    conn.execute(
                        "INSERT OR IGNORE INTO likes(user_id,track_id,created_at) VALUES (?,?,?)",
                        (
                            user_id,
                            content["track_id"],
                            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                        ),
                    )
                else:
                    conn.execute(
                        "DELETE FROM likes WHERE user_id=? AND track_id=?",
                        (user_id, content["track_id"]),
                    )
                event_id = f"reaction:{user_id}:{content_id}:{version}"
                payload = {
                    "event_id": event_id,
                    "user_id": user_id,
                    "content_id": content_id,
                    "state": state,
                    "version": version,
                }
                if feed_enabled():
                    from durable_jobs import enqueue

                    enqueue(
                        conn,
                        kind="feed_projection",
                        user_id=user_id,
                        job_id=event_id,
                        retry_safe=True,
                        payload=payload,
                    )
                else:
                    self._apply_projection(conn, payload)
                import json

                from durable_jobs import enqueue_behavior

                behavior = (
                    "liked" if state == "like" else "dislike" if state == "dislike" else "unliked"
                )
                enqueue_behavior(
                    conn,
                    user_id=user_id,
                    event=behavior,
                    scene="library",
                    track=Track.from_dict(json.loads(content["metadata_json"])),
                    event_id=event_id,
                    payload={
                        "contentId": content_id,
                        "relationVersion": version,
                        "desiredState": state,
                    },
                )
            counters = conn.execute(
                "SELECT likes,dislikes,version FROM content_counters WHERE content_id=?",
                (content_id,),
            ).fetchone()
            aliases = conn.execute(
                """SELECT c.content_id,r.state,r.version FROM feed_content c
                LEFT JOIN content_reactions r ON r.content_id=c.content_id AND r.user_id=?
                WHERE c.track_id=? AND c.content_id<>? AND (c.scope='public' OR c.owner_id=?)""",
                (user_id, content["track_id"], content_id, user_id),
            ).fetchall()
            for alias in aliases:
                if (alias["state"] or "neutral") == state:
                    continue
                alias_version = (alias["version"] or 0) + 1
                conn.execute(
                    """INSERT INTO content_reactions VALUES (?,?,?,?,?)
                    ON CONFLICT(user_id,content_id) DO UPDATE SET state=excluded.state,
                    version=excluded.version,updated_at=excluded.updated_at""",
                    (user_id, alias["content_id"], state, alias_version, now),
                )
                event_id = f"reaction:{user_id}:{alias['content_id']}:{alias_version}"
                payload = {
                    "event_id": event_id,
                    "user_id": user_id,
                    "content_id": alias["content_id"],
                    "state": state,
                    "version": alias_version,
                }
                if feed_enabled():
                    from durable_jobs import enqueue

                    enqueue(
                        conn,
                        kind="feed_projection",
                        user_id=user_id,
                        job_id=event_id,
                        retry_safe=True,
                        payload=payload,
                    )
                else:
                    self._apply_projection(conn, payload)
                # Alias projections share the command; they never create extra memory evidence.
            result = {
                "contentId": content_id,
                "state": state,
                "relationVersion": version,
                "changed": changed,
                "counts": dict(counters) if counters else {"likes": 0, "dislikes": 0, "version": 0},
            }
            conn.execute(
                "INSERT INTO reaction_commands VALUES (?,?,?,?,?)",
                (user_id, command_id, content_id, state, encode(result)),
            )
        count("reaction", "changed" if changed else "unchanged")
        return result

    def for_track(self, user_id, track, state, command_id):
        content_id = self.repo.admit(track, user_id=user_id, eligible=False)
        return self.set(user_id, content_id, state, command_id, require_admitted=False)

    def _apply_projection(self, conn, payload):
        event_id = payload["event_id"]
        track = conn.execute(
            "SELECT track_id FROM feed_content WHERE content_id=?", (payload["content_id"],)
        ).fetchone()
        if not track:
            raise ValueError("Projection content no longer exists")
        # A track's public/private aliases share the same lock, in either command order.
        begin_write(conn, namespace="reaction-projection", key=track[0])
        digest = hashlib.sha256(encode(payload).encode()).hexdigest()
        inbox = conn.execute(
            "SELECT payload_hash FROM feed_inbox WHERE consumer='reaction' AND event_id=?",
            (event_id,),
        ).fetchone()
        if inbox:
            if inbox[0] != digest:
                raise ValueError("Projection event identity changed")
            return
        content_id, user_id = payload["content_id"], payload["user_id"]
        current = conn.execute(
            "SELECT * FROM reaction_projection WHERE user_id=? AND content_id=?",
            (user_id, content_id),
        ).fetchone()
        old_state, old_version = (
            (current["state"], current["version"]) if current else ("neutral", 0)
        )
        if payload["version"] > old_version:
            delta_like = int(payload["state"] == "like") - int(old_state == "like")
            delta_dislike = int(payload["state"] == "dislike") - int(old_state == "dislike")
            conn.execute(
                "INSERT OR IGNORE INTO content_counters(content_id,updated_at) VALUES (?,?)",
                (content_id, time.time()),
            )
            conn.execute(
                """UPDATE content_counters SET likes=likes+?,dislikes=dislikes+?,
                version=version+1,updated_at=? WHERE content_id=?""",
                (delta_like, delta_dislike, time.time(), content_id),
            )
            conn.execute(
                """INSERT INTO reaction_projection VALUES (?,?,?,?) ON CONFLICT(user_id,content_id)
                DO UPDATE SET state=excluded.state,version=excluded.version""",
                (user_id, content_id, payload["state"], payload["version"]),
            )
        conn.execute(
            "INSERT INTO feed_inbox VALUES ('reaction',?,?,?)", (event_id, digest, time.time())
        )

    def project(self, payload):
        bitmap_id = user_bitmap_id(self.repo.db_path, payload["user_id"], create=True)
        with get_connection(self.repo.db_path) as conn:
            self._apply_projection(conn, payload)
            counter = dict(
                conn.execute(
                    "SELECT * FROM content_counters WHERE content_id=?", (payload["content_id"],)
                ).fetchone()
            )
            relation = dict(
                conn.execute(
                    "SELECT * FROM reaction_projection WHERE user_id=? AND content_id=?",
                    (payload["user_id"], payload["content_id"]),
                ).fetchone()
            )
        # SQL + Inbox commit precedes Redis; a failed write replays absolute projections.
        if self.cache:
            self.cache.publish_counter(payload["content_id"], counter)
            self.cache.publish_reaction(
                payload["user_id"],
                payload["content_id"],
                relation["state"],
                relation["version"],
                bitmap_id=bitmap_id,
            )
        count("projection")
        return counter

    def rebuild(self):
        with get_connection(self.repo.db_path) as conn:
            ids = [
                (r[0], r[1])
                for r in conn.execute(
                    "SELECT content_id,track_id FROM feed_content ORDER BY content_id"
                )
            ]
        total_relations = 0
        for content_id, track_id in ids:
            with get_connection(self.repo.db_path) as conn:
                begin_write(conn, namespace="reaction-projection", key=track_id)
                # One snapshot supplies both contribution rows and the counter.
                # New commands enqueue newer versions, applied after this lock releases.
                relations = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM content_reactions WHERE content_id=?",
                        (content_id,),
                    )
                ]
                conn.execute(
                    "DELETE FROM reaction_projection WHERE content_id=?",
                    (content_id,),
                )
                for relation in relations:
                    conn.execute(
                        "INSERT INTO reaction_projection VALUES (?,?,?,?)",
                        (relation["user_id"], content_id, relation["state"], relation["version"]),
                    )
                conn.execute(
                    "INSERT INTO content_counters(content_id,likes,dislikes,version,updated_at) "
                    "VALUES (?,?,?,1,?) ON CONFLICT(content_id) DO UPDATE SET "
                    "likes=excluded.likes,dislikes=excluded.dislikes,"
                    "version=content_counters.version+1,updated_at=excluded.updated_at",
                    (
                        content_id,
                        sum(r["state"] == "like" for r in relations),
                        sum(r["state"] == "dislike" for r in relations),
                        time.time(),
                    ),
                )
                counter = dict(
                    conn.execute(
                        "SELECT * FROM content_counters WHERE content_id=?", (content_id,)
                    ).fetchone()
                )
            if self.cache:
                self.cache.publish_counter(content_id, counter)
                for relation in relations:
                    self.cache.publish_reaction(
                        relation["user_id"],
                        content_id,
                        relation["state"],
                        relation["version"],
                        bitmap_id=user_bitmap_id(
                            self.repo.db_path, relation["user_id"], create=True
                        ),
                    )
            total_relations += len(relations)
        return {"contents": len(ids), "relations": total_relations}

    def rebuild_bitmaps(self, *, batch_size=500):
        """Bounded online backfill without rewriting authoritative counts or Outbox."""
        if not self.cache or self.cache.redis is None:
            raise RuntimeError("Redis is required for a Bitmap projection rebuild")
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("Bitmap rebuild batch size must be between 1 and 1000")
        cursor, processed, published = None, 0, 0
        while True:
            parameters = []
            after = ""
            if cursor:
                after = " WHERE r.content_id>? OR (r.content_id=? AND r.user_id>?)"
                parameters.extend((cursor[0], cursor[0], cursor[1]))
            parameters.append(batch_size)
            with get_connection(self.repo.db_path) as conn:
                rows = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT r.*,b.bitmap_id FROM content_reactions r "
                        "LEFT JOIN reaction_bitmap_users b ON b.user_id=r.user_id"
                        + after
                        + " ORDER BY r.content_id,r.user_id LIMIT ?",
                        tuple(parameters),
                    )
                ]
            if not rows:
                break
            for relation in rows:
                bitmap_id = relation["bitmap_id"] or user_bitmap_id(
                    self.repo.db_path,
                    relation["user_id"],
                    create=True,
                )
                published += int(
                    self.cache.publish_reaction(
                        relation["user_id"],
                        relation["content_id"],
                        relation["state"],
                        relation["version"],
                        bitmap_id=int(bitmap_id),
                    )
                )
            processed += len(rows)
            cursor = (rows[-1]["content_id"], rows[-1]["user_id"])
        return {
            "relations": processed,
            "published": published,
            "skippedNewerVersions": processed - published,
        }
