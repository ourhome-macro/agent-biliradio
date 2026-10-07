from __future__ import annotations

import json
import time
from uuid import NAMESPACE_URL, uuid4, uuid5

from database import get_connection, init_db
from error_code import APIError
from library_service import LibraryService


def encode(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def identity(kind: str, *parts) -> str:
    return str(uuid5(NAMESPACE_URL, encode(["radio-feed", kind, *parts])))


class FeedRepository:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        init_db(db_path)

    def admit(
        self,
        track,
        *,
        user_id: str,
        scope: str = "public",
        facets=None,
        evidence=None,
        eligible=True,
    ) -> str:
        if scope not in {"public", f"user:{user_id}"}:
            raise APIError.forbidden("Invalid content scope")
        if eligible:
            from candidate_pool import is_admissible
            from music_keyword_pool import has_music_relevance_signal

            text = " ".join(
                (
                    track.title,
                    track.page_title or "",
                    track.type_name,
                    " ".join(track.tags),
                    track.description,
                )
            )
            eligible = is_admissible(track, facets or {}) and (
                "音乐" in track.type_name
                or "music" in track.type_name.casefold()
                or has_music_relevance_signal(text)
            )
        LibraryService(self.db_path, user_id=user_id).upsert_track(track)
        content_id = identity("content", track.track_id, scope)
        metadata, facets_value = encode(track.to_dict()), encode(facets or {})
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            old = conn.execute(
                "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
            ).fetchone()
            if old is None:
                conn.execute(
                    """INSERT INTO feed_content
                    (content_id,track_id,scope,owner_id,metadata_json,facets_json,evidence_json,admitted_at,status)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        content_id,
                        track.track_id,
                        scope,
                        None if scope == "public" else user_id,
                        metadata,
                        facets_value,
                        encode(evidence or []),
                        now,
                        "admitted" if eligible else "catalogued",
                    ),
                )
                conn.execute(
                    """INSERT INTO content_reactions(user_id,content_id,state,version,updated_at)
                    SELECT user_id,?,'like',1,? FROM likes
                    WHERE track_id=? AND (?='public' OR user_id=?)""",
                    (content_id, now, track.track_id, scope, user_id),
                )
                conn.execute(
                    """INSERT INTO reaction_projection SELECT user_id,content_id,state,version
                    FROM content_reactions WHERE content_id=?""",
                    (content_id,),
                )
                baseline = conn.execute(
                    "SELECT COUNT(*) FROM content_reactions WHERE content_id=? AND state='like'",
                    (content_id,),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO content_counters(content_id,likes,version,updated_at) "
                    "VALUES (?,?,1,?)",
                    (content_id, baseline, now),
                )
            elif old["metadata_json"] != metadata or old["facets_json"] != facets_value:
                conn.execute(
                    """UPDATE feed_content SET metadata_json=?,facets_json=?,evidence_json=?,
                    version=version+1 WHERE content_id=?""",
                    (metadata, facets_value, encode(evidence or []), content_id),
                )
            if eligible:
                conn.execute(
                    "UPDATE feed_content SET status='admitted',version=version+1 "
                    "WHERE content_id=? AND status='catalogued'",
                    (content_id,),
                )
        return content_id

    @staticmethod
    def authorize(row, user_id: str):
        if row is None:
            raise APIError.not_found("Content not found")
        if row["scope"] != "public" and row["owner_id"] != user_id:
            raise APIError.not_found("Content not found")
        if row["status"] != "admitted":
            raise APIError.not_found("Content is unavailable")
        return dict(row)

    def content(self, content_id: str, user_id: str):
        with get_connection(self.db_path) as conn:
            return self.authorize(
                conn.execute(
                    "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
                ).fetchone(),
                user_id,
            )

    def list_content(self, user_id: str):
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """SELECT c.*,a.status AS asset_status,a.asset_id
                FROM feed_content c LEFT JOIN content_media_links l ON l.content_id=c.content_id
                LEFT JOIN media_assets a ON a.asset_id=l.asset_id
                LEFT JOIN content_reactions r ON r.content_id=c.content_id AND r.user_id=?
                WHERE c.status='admitted' AND (c.scope='public' OR c.owner_id=?)
                  AND COALESCE(a.status,'empty') NOT IN ('unsupported','retired')
                  AND COALESCE(r.state,'neutral')<>'dislike'
                ORDER BY c.admitted_at DESC,c.content_id DESC LIMIT 500""",
                (user_id, user_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def import_job(self, import_id):
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                """SELECT j.*,a.content_id,c.scope,c.owner_id,c.metadata_json,
                c.status AS content_status FROM media_import_jobs j
                JOIN media_assets a ON a.asset_id=j.asset_id
                JOIN feed_content c ON c.content_id=a.content_id
                WHERE j.import_id=?""",
                (import_id,),
            ).fetchone()
        return dict(row) if row else None

    def claim_import(self, import_id, config):
        now, token = time.time(), uuid4().hex
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                """UPDATE media_import_jobs SET status='running',lease_token=?,
                lease_until=?,updated_at=? WHERE import_id=? AND
                (status='queued' OR (status='running' AND lease_until<?))""",
                (token, now + config.lease_seconds, now, import_id, now),
            )
            if not updated.rowcount:
                return None
        return self.import_job(import_id)

    def owned(self, conn, job):
        row = conn.execute(
            "SELECT * FROM media_import_jobs WHERE import_id=?", (job["import_id"],)
        ).fetchone()
        if (
            not row
            or row["status"] != "running"
            or row["lease_token"] != job["lease_token"]
            or row["lease_until"] < time.time()
        ):
            raise RuntimeError("MediaLeaseLost")
        return dict(row)

    def progress(self, job, config, *, stage=None, source_complete=None, byte_count=None):
        if job.get("deadline") is not None and time.monotonic() >= job["deadline"]:
            raise TimeoutError("MediaExecutionBudgetExceeded")
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self.owned(conn, job)
            conn.execute(
                """UPDATE media_import_jobs SET stage=?,complete_source=?,bytes=?,
                lease_until=?,updated_at=? WHERE import_id=? AND lease_token=?""",
                (
                    stage or current["stage"],
                    int(source_complete)
                    if source_complete is not None
                    else current["complete_source"],
                    byte_count if byte_count is not None else current["bytes"],
                    now + config.lease_seconds,
                    now,
                    job["import_id"],
                    job["lease_token"],
                ),
            )
            # Durable transport lease and media lease advance together.
            if job.get("transport_token"):
                renewed = conn.execute(
                    """UPDATE durable_jobs SET lease_until=?,updated_at=?
                    WHERE job_id=? AND status='running' AND lease_token=?""",
                    (now + config.lease_seconds, now, job["job_id"], job["transport_token"]),
                )
                if not renewed.rowcount:
                    raise RuntimeError("TransportLeaseLost")

    def demand(self, job, config) -> bool:
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self.owned(conn, job)
            if row["complete_source"]:
                return True
            viewers = conn.execute(
                """SELECT COUNT(*) FROM media_playbacks
                WHERE import_id=? AND active=1 AND expires_at>?""",
                (job["import_id"], now),
            ).fetchone()[0]
            if viewers:
                conn.execute(
                    "UPDATE media_import_jobs SET idle_since=NULL WHERE import_id=?",
                    (job["import_id"],),
                )
                return True
            idle_since = row["idle_since"]
            if idle_since is None:
                conn.execute(
                    "UPDATE media_import_jobs SET idle_since=? WHERE import_id=?",
                    (now, job["import_id"]),
                )
                return True
            return now - idle_since < config.cancel_grace_seconds

    def cancel_if_idle(self, job, config) -> bool:
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self.owned(conn, job)
            viewers = conn.execute(
                """SELECT COUNT(*) FROM media_playbacks
                WHERE import_id=? AND active=1 AND expires_at>?""",
                (job["import_id"], now),
            ).fetchone()[0]
            if (
                row["complete_source"]
                or viewers
                or row["idle_since"] is None
                or now - row["idle_since"] < config.cancel_grace_seconds
            ):
                return False
            conn.execute(
                """UPDATE media_import_jobs SET status='cancelled',stage='cancelled',
                lease_token=NULL,lease_until=0,updated_at=? WHERE import_id=?""",
                (now, job["import_id"]),
            )
            return True

    def stop_import(self, job, *, status, error_type=None):
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.owned(conn, job)
            conn.execute(
                """UPDATE media_import_jobs SET status=?,error_type=?,lease_until=0,
                lease_token=NULL,updated_at=? WHERE import_id=?""",
                (status, error_type, time.time(), job["import_id"]),
            )
            conn.execute(
                "DELETE FROM media_storage_reservations WHERE import_id=?", (job["import_id"],)
            )
            conn.execute(
                "UPDATE media_assets SET error_type=?,updated_at=? "
                "WHERE asset_id=? AND status<>'ready'",
                (error_type, time.time(), job["asset_id"]),
            )

    def reserve_storage(self, job, size, config):
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.owned(conn, job)
            used = conn.execute(
                "SELECT COALESCE(SUM(bytes),0) FROM media_assets WHERE bytes>0"
            ).fetchone()[0]
            reserved = conn.execute(
                "SELECT COALESCE(SUM(bytes),0) FROM media_storage_reservations WHERE import_id<>?",
                (job["import_id"],),
            ).fetchone()[0]
            if size > config.max_bytes or used + reserved + size > config.storage_bytes:
                raise RuntimeError("MediaStorageQuotaExceeded")
            conn.execute(
                "INSERT OR REPLACE INTO media_storage_reservations VALUES (?,?,?)",
                (job["import_id"], size, time.time()),
            )

    def complete_import(self, job, manifest):
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self.owned(conn, job)
            if not row["complete_source"]:
                raise RuntimeError("IncompleteMediaCannotBePublished")
            content = conn.execute(
                "SELECT status FROM feed_content WHERE content_id=?", (job["content_id"],)
            ).fetchone()
            if content["status"] != "admitted":
                raise RuntimeError("ContentRetiredDuringImport")
            conn.execute(
                """UPDATE media_assets SET status='ready',version=?,manifest_json=?,bytes=?,
                error_type=NULL,updated_at=? WHERE asset_id=?""",
                (job["attempt"], encode(manifest), manifest["bytes"], now, job["asset_id"]),
            )
            conn.execute(
                """UPDATE media_import_jobs SET status='completed',stage='ready',
                lease_until=0,lease_token=NULL,updated_at=? WHERE import_id=?""",
                (now, job["import_id"]),
            )
            conn.execute(
                "DELETE FROM media_storage_reservations WHERE import_id=?", (job["import_id"],)
            )
            from durable_jobs import enqueue

            enqueue(
                conn,
                kind="feed_asset_ready",
                user_id=job["owner_id"] or "legacy-owner",
                job_id=f"asset-ready:{job['import_id']}",
                retry_safe=True,
                payload={
                    "asset_id": job["asset_id"],
                    "content_id": job["content_id"],
                    "version": job["attempt"],
                },
            )

    def retire(self, content_id, user_id):
        self.content(content_id, user_id)
        with get_connection(self.db_path) as conn:
            conn.execute(
                "UPDATE feed_content SET status='retired',version=version+1 WHERE content_id=?",
                (content_id,),
            )
            conn.execute(
                "UPDATE media_assets SET status='retired' WHERE content_id=?", (content_id,)
            )
