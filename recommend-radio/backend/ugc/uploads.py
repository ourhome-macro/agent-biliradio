"""Private bounded direct uploads; completion queues validation, never trusts client success."""

from __future__ import annotations

import hashlib
import re
import time
from uuid import uuid4

from database import begin_write, get_connection
from durable_jobs import enqueue
from error_code import APIError, ErrorCode
from feed_runtime.config import FeedConfig
from feed_runtime.repository import encode

MAX_AUDIO_BYTES = 64 * 1024 * 1024
MIME_TYPES = {
    "audio/mpeg",
    "audio/mp4",
    "audio/wav",
    "audio/x-wav",
    "audio/flac",
    "audio/ogg",
    "audio/aac",
    "audio/webm",
}


class AudioUploads:
    def __init__(self, db_path, storage):
        self.db_path, self.storage = db_path, storage

    def initiate(self, user_id, content_id, payload, request_key):
        if not isinstance(request_key, str) or not request_key or len(request_key) > 180:
            raise APIError.validation_error("Idempotency-Key is required")
        if not isinstance(payload, dict) or set(payload) != {"bytes", "sha256", "contentType"}:
            raise APIError.validation_error("bytes, sha256 and contentType are required")
        size, digest, mime = payload["bytes"], payload["sha256"], payload["contentType"]
        if (
            type(size) is not int
            or not 0 < size <= MAX_AUDIO_BYTES
            or not isinstance(digest, str)
            or not re.fullmatch("[0-9a-f]{64}", digest)
            or not isinstance(mime, str)
            or mime not in MIME_TYPES
        ):
            raise APIError.validation_error("Invalid audio upload size, checksum or media type")
        signature = hashlib.sha256(encode([content_id, payload]).encode()).hexdigest()
        now = time.time()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="audio-upload-owner", key=user_id)
            post = conn.execute(
                "SELECT * FROM ugc_posts WHERE content_id=?", (content_id,)
            ).fetchone()
            if not post or post["author_id"] != user_id or post["kind"] != "audio":
                raise APIError.not_found("Audio draft not found")
            if post["status"] != "draft":
                raise APIError.conflict("Upload audio to a draft before publication")
            old = conn.execute(
                "SELECT * FROM ugc_audio_uploads WHERE user_id=? AND request_key=?",
                (user_id, request_key),
            ).fetchone()
            if old:
                if old["input_hash"] != signature:
                    raise APIError.conflict("Upload key is bound to different input")
                row = dict(old)
            else:
                begin_write(conn, namespace="media-storage", key="audio-capacity")
                used = conn.execute("SELECT COALESCE(SUM(bytes),0) FROM media_assets").fetchone()[0]
                reserved = conn.execute(
                    "SELECT COUNT(*) FROM ugc_audio_uploads "
                    "WHERE (staging_deleted=0 AND expires_at>?) "
                    "OR status IN ('queued','processing')",
                    (now,),
                ).fetchone()[0]
                limit = getattr(self.storage, "config", FeedConfig()).storage_bytes
                if used + (reserved + 1) * 160 * 1024 * 1024 > limit:
                    raise APIError(ErrorCode.CONFLICT, "Audio storage capacity exhausted", 507)
                count = conn.execute(
                    "SELECT COUNT(*) FROM ugc_audio_uploads WHERE user_id=? "
                    "AND status IN ('awaiting','queued','processing') AND expires_at>?",
                    (user_id, now),
                ).fetchone()[0]
                if count >= 4:
                    raise APIError.conflict("Audio upload capacity exceeded")
                upload_id = uuid4().hex
                key = f"ugc-staging/{upload_id}/source"
                asset_id, job_id = "audio-" + upload_id, "ugc-audio:" + upload_id
                conn.execute(
                    "INSERT INTO media_assets(asset_id,content_id,policy,status,updated_at) "
                    "VALUES (?,?,?,'empty',?)",
                    (asset_id, content_id, "ugc-audio:" + upload_id, now),
                )
                conn.execute(
                    "INSERT INTO ugc_audio_uploads "
                    "(upload_id,content_id,user_id,request_key,input_hash,object_key,"
                    "expected_bytes,expected_sha256,content_type,asset_id,job_id,expires_at,"
                    "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        upload_id,
                        content_id,
                        user_id,
                        request_key,
                        signature,
                        key,
                        size,
                        digest,
                        mime,
                        asset_id,
                        job_id,
                        now + 900,
                        now,
                        now,
                    ),
                )
                row = dict(
                    conn.execute(
                        "SELECT * FROM ugc_audio_uploads WHERE upload_id=?", (upload_id,)
                    ).fetchone()
                )
        # Signing / object-store I/O is outside the command transaction.
        if row["expires_at"] <= now or row["status"] != "awaiting":
            raise APIError.conflict("Upload session is not writable")
        self.storage.ensure_bucket()
        return {
            **self.dto(row),
            "upload": self.storage.upload_policy(
                row["object_key"], row["expected_bytes"], row["content_type"], row["expires_at"]
            ),
        }

    @staticmethod
    def dto(row):
        return {
            key: row[field]
            for key, field in (
                ("uploadId", "upload_id"),
                ("contentId", "content_id"),
                ("status", "status"),
                ("assetId", "asset_id"),
                ("expiresAt", "expires_at"),
                ("errorType", "error_type"),
            )
        }

    def get(self, user_id, upload_id):
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM ugc_audio_uploads WHERE upload_id=? AND user_id=?",
                (upload_id, user_id),
            ).fetchone()
        if row is None:
            raise APIError.not_found("Upload not found")
        if row["status"] in {"queued", "processing"}:
            with get_connection(self.db_path) as conn:
                job = conn.execute(
                    "SELECT status,error FROM durable_jobs WHERE job_id=?", (row["job_id"],)
                ).fetchone()
                if job and job["status"] == "failed":
                    conn.execute(
                        "UPDATE ugc_audio_uploads SET status='failed',error_type=?,updated_at=? "
                        "WHERE upload_id=? AND status IN ('queued','processing')",
                        ("AudioProcessingFailed", time.time(), upload_id),
                    )
                    row = {**dict(row), "status": "failed", "error_type": "AudioProcessingFailed"}
        return self.dto(row)

    def complete(self, user_id, upload_id):
        now = time.time()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="audio-upload", key=upload_id)
            row = conn.execute(
                "SELECT * FROM ugc_audio_uploads WHERE upload_id=? AND user_id=?",
                (upload_id, user_id),
            ).fetchone()
            if row is None:
                raise APIError.not_found("Upload not found")
            if row["status"] == "awaiting":
                if row["expires_at"] <= now:
                    raise APIError.conflict("Upload expired; start a new upload")
                enqueue(
                    conn,
                    kind="ugc_audio",
                    lane="ugc-audio:" + row["content_id"],
                    user_id=user_id,
                    job_id=row["job_id"],
                    payload={"upload_id": upload_id},
                    retry_safe=True,
                )
                conn.execute(
                    "UPDATE ugc_audio_uploads SET status='queued',updated_at=? WHERE upload_id=?",
                    (now, upload_id),
                )
        return self.get(user_id, upload_id)

    def expire(self, limit=100):
        """Run from maintenance: expire abandoned uploads, then remove staging bytes.

        Waiting until the signed POST expires is essential: deleting it earlier
        would allow a still-valid upload policy to recreate an orphan afterwards.
        """
        now = time.time()
        with get_connection(self.db_path) as conn:
            conn.execute(
                "UPDATE ugc_audio_uploads SET status='failed',error_type='AudioProcessingFailed' "
                "WHERE status IN ('queued','processing') AND job_id IN "
                "(SELECT job_id FROM durable_jobs WHERE status='failed')"
            )
            rows = conn.execute(
                "SELECT * FROM ugc_audio_uploads WHERE expires_at<? AND staging_deleted=0 "
                "AND status IN ('awaiting','ready','failed','expired') "
                "ORDER BY expires_at LIMIT ?",
                (now, min(limit, 100)),
            ).fetchall()
        removed = 0
        for row in rows:
            self.storage.remove(row["object_key"])
            with get_connection(self.db_path) as conn:
                conn.execute(
                    "UPDATE ugc_audio_uploads SET status=CASE WHEN status='awaiting' "
                    "THEN 'expired' ELSE status END,staging_deleted=1,updated_at=? "
                    "WHERE upload_id=?",
                    (now, row["upload_id"]),
                )
            removed += 1
        return {"removedStagingObjects": removed}
