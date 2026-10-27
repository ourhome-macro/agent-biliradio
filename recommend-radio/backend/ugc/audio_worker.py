"""Validate untrusted direct-upload bytes and publish an immutable AAC asset."""

from __future__ import annotations

import json
import math
import subprocess
import tempfile
import time
from pathlib import Path

from database import begin_write, get_connection
from domain_outbox import enqueue_event
from feed_runtime.repository import encode
from feed_runtime.storage import file_hash
from job_errors import JobPermanentFailure


class AudioWorker:
    def __init__(self, db_path, config, storage):
        self.db_path, self.config, self.storage = db_path, config, storage

    @staticmethod
    def fence(conn, job):
        begin_write(conn, namespace="job", key=job["job_id"])
        row = conn.execute(
            "SELECT status,lease_token,lease_until FROM durable_jobs WHERE job_id=?",
            (job["job_id"],),
        ).fetchone()
        if (
            not row
            or row["status"] != "running"
            or row["lease_token"] != job["lease_token"]
            or row["lease_until"] <= time.time()
        ):
            raise RuntimeError("AudioWorkerLeaseLost")

    def run(self, upload_id, job):
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="audio-upload", key=upload_id)
            self.fence(conn, job)
            row = conn.execute(
                "SELECT * FROM ugc_audio_uploads WHERE upload_id=?", (upload_id,)
            ).fetchone()
            if row is None or row["job_id"] != job["job_id"] or row["user_id"] != job["user_id"]:
                raise JobPermanentFailure("AudioUploadIdentityMismatch")
            if row["status"] == "ready":
                return {"status": "ready", "assetId": row["asset_id"]}
            if row["status"] not in {"queued", "processing"}:
                raise JobPermanentFailure("AudioUploadNotQueued")
            row = dict(row)
            conn.execute(
                "UPDATE ugc_audio_uploads SET status='processing',updated_at=? WHERE upload_id=?",
                (time.time(), upload_id),
            )
        self.config.workdir.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(prefix="audio-", dir=self.config.workdir) as directory:
                source, target = Path(directory) / "source", Path(directory) / "audio.m4a"
                self.storage.download_verified(
                    row["object_key"], source, row["expected_bytes"], row["expected_sha256"]
                )
                probe = subprocess.run(
                    [
                        self.config.ffprobe,
                        "-v",
                        "error",
                        "-protocol_whitelist",
                        "file,pipe",
                        "-format_whitelist",
                        "wav,mp3,flac,ogg,mov,aac,matroska,webm",
                        "-show_streams",
                        "-show_format",
                        "-of",
                        "json",
                        str(source),
                    ],
                    check=True,
                    capture_output=True,
                    timeout=30,
                )
                info = json.loads(probe.stdout)
                streams = info.get("streams") or []
                duration = float(info.get("format", {}).get("duration") or 0)
                if (
                    not math.isfinite(duration)
                    or not 0 < duration <= self.config.max_duration
                    or not any(s.get("codec_type") == "audio" for s in streams)
                    or any(
                        s.get("codec_type") == "video"
                        and not s.get("disposition", {}).get("attached_pic")
                        for s in streams
                    )
                ):
                    raise ValueError("InvalidAudioMedia")
                subprocess.run(
                    [
                        self.config.ffmpeg,
                        "-nostdin",
                        "-v",
                        "error",
                        "-protocol_whitelist",
                        "file,pipe",
                        "-format_whitelist",
                        "wav,mp3,flac,ogg,mov,aac,matroska,webm",
                        "-i",
                        str(source),
                        "-map",
                        "0:a:0",
                        "-vn",
                        "-map_metadata",
                        "-1",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "160k",
                        "-movflags",
                        "+faststart",
                        str(target),
                    ],
                    check=True,
                    capture_output=True,
                    timeout=180,
                )
                if target.stat().st_size > 96 * 1024 * 1024:
                    raise ValueError("ProcessedAudioTooLarge")
                key = f"assets/{row['asset_id']}/{file_hash(target)}/audio.m4a"
                stored = self.storage.put_file(key, target, content_type="audio/mp4")
            manifest = {
                "format": "ugc-audio-v1",
                "duration": duration,
                "files": {"audio.m4a": stored},
                "sourceGeneration": upload_id,
            }
            with get_connection(self.db_path) as conn:
                begin_write(conn, namespace="audio-upload", key=upload_id)
                begin_write(conn, namespace="content", key=row["content_id"])
                self.fence(conn, job)
                conn.execute(
                    "UPDATE media_assets SET status='ready',version=version+1,manifest_json=?,"
                    "bytes=?,updated_at=? WHERE asset_id=?",
                    (encode(manifest), stored["bytes"], time.time(), row["asset_id"]),
                )
                conn.execute(
                    "UPDATE ugc_audio_uploads SET status='ready',error_type=NULL,updated_at=? "
                    "WHERE upload_id=?",
                    (time.time(), upload_id),
                )
                post = conn.execute(
                    "SELECT * FROM ugc_posts WHERE content_id=?", (row["content_id"],)
                ).fetchone()
                if post and post["status"] == "draft" and post["audio_upload_id"] == upload_id:
                    metadata = json.loads(
                        conn.execute(
                            "SELECT metadata_json FROM feed_content WHERE content_id=?",
                            (row["content_id"],),
                        ).fetchone()[0]
                    )
                    metadata["duration"] = duration
                    conn.execute(
                        "UPDATE feed_content SET metadata_json=?,version=version+1 "
                        "WHERE content_id=?",
                        (encode(metadata), row["content_id"]),
                    )
                    conn.execute(
                        "UPDATE ugc_posts SET version=version+1,updated_at=? WHERE content_id=?",
                        (time.time(), row["content_id"]),
                    )
                    enqueue_event(
                        conn,
                        aggregate_type="content",
                        aggregate_id=row["content_id"],
                        event_type="content.updated",
                        owner_id=row["user_id"],
                        visibility_revision=post["permission_version"],
                        payload={"contentId": row["content_id"], "version": post["version"] + 1},
                    )
            return {"status": "ready", "assetId": row["asset_id"]}
        except (ValueError, subprocess.CalledProcessError) as error:
            with get_connection(self.db_path) as conn:
                begin_write(conn, namespace="audio-upload", key=upload_id)
                self.fence(conn, job)
                conn.execute(
                    "UPDATE ugc_audio_uploads SET status='failed',error_type=?,updated_at=? "
                    "WHERE upload_id=?",
                    (type(error).__name__, time.time(), upload_id),
                )
            raise JobPermanentFailure("AudioValidationFailed") from error
