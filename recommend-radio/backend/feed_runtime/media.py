from __future__ import annotations

import json
import re
import time
from pathlib import Path
from uuid import uuid4

from database import begin_write, get_connection
from error_code import APIError, ErrorCode
from models import Track

from .config import FeedConfig
from .repository import FeedRepository, encode, identity
from .source import BiliMediaSource

POLICY = "avc-aac-720-hls-v1"
FILE_NAME = re.compile(
    r"(?:init\.mp4|index\.m3u8|seg_[0-9]{6}\.m4s|cover\.(?:jpg|png)|audio\.m4a)\Z"
)


class MediaService:
    def __init__(
        self, repo: FeedRepository, config: FeedConfig, storage, *, source_factory=BiliMediaSource
    ):
        self.repo, self.config, self.storage = repo, config, storage
        self.source_factory = source_factory

    def create(self, user_id: str, content_id: str, request_key: str):
        if not request_key or len(request_key) > 180:
            raise APIError.validation_error("A bounded Idempotency-Key is required")
        content = self.repo.content(content_id, user_id)
        with get_connection(self.repo.db_path) as conn:
            previous = conn.execute(
                """SELECT p.*,a.content_id FROM media_playbacks p
                JOIN media_assets a ON a.asset_id=p.asset_id
                WHERE p.user_id=? AND p.request_key=?""",
                (user_id, request_key),
            ).fetchone()
            linked = conn.execute(
                "SELECT asset_id FROM content_media_links WHERE content_id=?", (content_id,)
            ).fetchone()
        if previous:
            # Alias content can share an asset; verify against the link, not its first owner.
            if (
                not linked
                or previous["asset_id"] != linked["asset_id"]
                or previous["content_id"] != content_id
            ):
                raise APIError.conflict("Idempotency key is bound to another content")
            return self.descriptor(previous["playback_id"], user_id)
        metadata = json.loads(content["metadata_json"])
        if linked:
            asset_id = linked["asset_id"]
        else:
            track = Track.from_dict(metadata)
            if not track.cid:
                source = self.source_factory(
                    user_id=user_id, scope=content["scope"], db_path=self.repo.db_path
                )
                try:
                    cid = source.resolve_cid(track)
                finally:
                    source.close()
                metadata["cid"] = cid
            cid = int(metadata["cid"])
            if cid <= 0:
                raise APIError.validation_error("Invalid media cid")
            asset_id = identity("asset", metadata["bvid"], cid, content["scope"], POLICY)
        now, playback_id = time.time(), uuid4().hex
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="playback-command", key=encode([user_id, request_key]))
            begin_write(conn, namespace="media-asset", key=asset_id)
            begin_write(conn, namespace="content", key=content_id)
            # Recheck visibility under the writer lock before issuing a playback.
            current_content = self.repo.authorize(
                conn.execute(
                    "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
                ).fetchone(),
                user_id,
            )
            current_metadata = json.loads(current_content["metadata_json"])
            if (current_metadata.get("source"), current_metadata.get("bvid")) != (
                metadata.get("source"),
                metadata.get("bvid"),
            ) or (
                current_metadata.get("cid") and current_metadata.get("cid") != metadata.get("cid")
            ):
                raise APIError.conflict(
                    "Media identity changed while resolving source; retry playback"
                )
            current_metadata["cid"] = metadata.get("cid")
            metadata = current_metadata
            previous = conn.execute(
                "SELECT * FROM media_playbacks WHERE user_id=? AND request_key=?",
                (user_id, request_key),
            ).fetchone()
            if previous:
                if previous["asset_id"] != asset_id or previous["content_id"] != content_id:
                    raise APIError.conflict("Idempotency key is bound to another content")
                playback_id = previous["playback_id"]
            else:
                conn.execute(
                    "UPDATE feed_content SET metadata_json=?,version=version+1 "
                    "WHERE content_id=? AND metadata_json<>?",
                    (encode(metadata), content_id, encode(metadata)),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO media_assets(asset_id,content_id,policy,updated_at)
                    VALUES (?,?,?,?)""",
                    (asset_id, content_id, POLICY, now),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO content_media_links VALUES (?,?)", (content_id, asset_id)
                )
                asset = conn.execute(
                    "SELECT * FROM media_assets WHERE asset_id=?", (asset_id,)
                ).fetchone()
                if asset["status"] in {"retired", "unsupported"}:
                    raise APIError.not_found("Media is unavailable")
                import_id = None
                if asset["status"] != "ready":
                    active = conn.execute(
                        """SELECT * FROM media_import_jobs WHERE asset_id=?
                        AND status IN ('queued','running')""",
                        (asset_id,),
                    ).fetchone()
                    if active:
                        import_id = active["import_id"]
                    else:
                        used = conn.execute(
                            "SELECT COALESCE(SUM(bytes),0) FROM media_assets WHERE bytes>0"
                        ).fetchone()[0]
                        reserved = conn.execute(
                            "SELECT COALESCE(SUM(bytes),0) FROM media_storage_reservations"
                        ).fetchone()[0]
                        if used + reserved >= self.config.storage_bytes:
                            raise APIError(
                                ErrorCode.CONFLICT, "Media storage capacity exhausted", 507
                            )
                        last = conn.execute(
                            "SELECT * FROM media_import_jobs WHERE asset_id=? "
                            "ORDER BY attempt DESC LIMIT 1",
                            (asset_id,),
                        ).fetchone()
                        attempt = (last["attempt"] if last else 0) + 1
                        import_id, job_id = uuid4().hex, f"media:{uuid4().hex}"
                        work_key = f"{asset_id}/{import_id}"
                        # Completed acquisition can be reused after an upload-only failure.
                        complete_source = bool(
                            last and last["complete_source"] and last["status"] == "failed"
                        )
                        if complete_source:
                            prior_directory = self.workdir(last)
                            complete_source = (prior_directory / "source.json").is_file() and (
                                prior_directory / "index.m3u8"
                            ).is_file()
                        if complete_source:
                            work_key = last["work_key"]
                        conn.execute(
                            """INSERT INTO media_import_jobs
                            (import_id,asset_id,job_id,attempt,stage,work_key,complete_source,created_at,updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?)""",
                            (
                                import_id,
                                asset_id,
                                job_id,
                                attempt,
                                "uploading" if complete_source else "queued",
                                work_key,
                                int(complete_source),
                                now,
                                now,
                            ),
                        )
                        from durable_jobs import enqueue

                        enqueue(
                            conn,
                            kind="media_import",
                            user_id=user_id,
                            job_id=job_id,
                            retry_safe=True,
                            payload={"import_id": import_id},
                        )
                conn.execute(
                    """INSERT INTO media_playbacks
                    (playback_id,user_id,asset_id,import_id,request_key,expires_at,created_at,content_id)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        playback_id,
                        user_id,
                        asset_id,
                        import_id,
                        request_key,
                        now + self.config.playback_seconds,
                        now,
                        content_id,
                    ),
                )
        return self.descriptor(playback_id, user_id)

    def _playback(self, playback_id, user_id, *, require_active=True):
        with get_connection(self.repo.db_path) as conn:
            row = conn.execute(
                """SELECT p.*,a.status AS asset_status,a.version AS asset_version,
                a.manifest_json,a.content_id,c.scope,c.owner_id,c.status AS content_status
                FROM media_playbacks p JOIN media_assets a ON a.asset_id=p.asset_id
                JOIN feed_content c ON c.content_id=COALESCE(p.content_id,a.content_id)
                WHERE p.playback_id=? AND p.user_id=?""",
                (playback_id, user_id),
            ).fetchone()
        if not row:
            raise APIError.not_found("Playback not found")
        if row["scope"] != "public" and row["owner_id"] != user_id:
            raise APIError.not_found("Playback not found")
        if row["content_status"] != "admitted" or row["asset_status"] == "retired":
            raise APIError.not_found("Content is unavailable")
        if require_active and (not row["active"] or row["expires_at"] <= time.time()):
            raise APIError(ErrorCode.CONFLICT, "Playback has expired", 410)
        return dict(row)

    def descriptor(self, playback_id, user_id):
        row = self._playback(playback_id, user_id)
        mode, status, complete = "stored", "ready", True
        if row["asset_status"] != "ready":
            mode, status, complete = "cold", "preparing", False
            job = self.repo.import_job(row["import_id"])
            if job["status"] in {"failed", "cancelled"}:
                status = job["status"]
            else:
                playlist = self.workdir(job) / "index.m3u8"
                if playlist.is_file() and (playlist.parent / "init.mp4").is_file():
                    text = playlist.read_text(encoding="utf-8")
                    segments = [s for s in text.splitlines() if s.startswith("seg_")]
                    if segments and all(
                        FILE_NAME.fullmatch(s) and (playlist.parent / s).is_file() for s in segments
                    ):
                        status = "streaming"
                complete = bool(job["complete_source"])
        now = time.time()
        if status in {"streaming", "ready"} and row["playable_at"] is None:
            with get_connection(self.repo.db_path) as conn:
                conn.execute(
                    "UPDATE media_playbacks SET playable_at=? "
                    "WHERE playback_id=? AND playable_at IS NULL",
                    (now, playback_id),
                )
        generation_key = (
            f"stored:{row['asset_version']}"
            if mode == "stored"
            else f"{job['import_id']}:{job['fetch_attempts']}"
        )
        with get_connection(self.repo.db_path) as conn:
            metadata = conn.execute(
                "SELECT metadata_json FROM feed_content WHERE content_id=?", (row["content_id"],)
            ).fetchone()
        duration = float(
            json.loads(row["manifest_json"]).get("duration")
            or json.loads(metadata[0]).get("duration")
            or 0
        )
        return {
            "playbackId": playback_id,
            "assetId": row["asset_id"],
            "mode": mode,
            "status": status,
            "manifestUrl": f"/api/media/streams/{playback_id}/index.m3u8?v={generation_key}"
            if status in {"streaming", "ready"}
            else None,
            "expiresAt": row["expires_at"],
            "leaseExpiresAt": row["expires_at"],
            "manifestExpiresAt": now + self.config.signed_seconds if mode == "stored" else None,
            "refreshAfterSeconds": max(
                1, self.config.signed_seconds - min(60, self.config.signed_seconds / 3)
            ),
            "generationKey": generation_key,
            "durationSeconds": duration,
            "seekable": mode == "stored" or complete,
            "sourceComplete": complete,
            "generation": self.repo.import_job(row["import_id"])["fetch_attempts"]
            if mode == "cold"
            else row["asset_version"],
            "errorType": self.repo.import_job(row["import_id"])["error_type"]
            if status == "failed"
            else None,
        }

    def heartbeat(self, playback_id, user_id):
        playback = self._playback(playback_id, user_id, require_active=False)
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="media-asset", key=playback["asset_id"])
            state = conn.execute(
                "SELECT a.status AS asset_status,j.status AS import_status FROM media_playbacks p "
                "JOIN media_assets a ON a.asset_id=p.asset_id "
                "LEFT JOIN media_import_jobs j ON j.import_id=p.import_id "
                "WHERE p.playback_id=?",
                (playback_id,),
            ).fetchone()
            if (
                not state
                or state["asset_status"] == "retired"
                or (
                    state["asset_status"] != "ready"
                    and state["import_status"] in {"cancelled", "failed"}
                )
            ):
                raise APIError(ErrorCode.CONFLICT, "Playback import is no longer active", 410)
            updated = conn.execute(
                """UPDATE media_playbacks SET expires_at=?
                WHERE playback_id=? AND user_id=? AND active=1""",
                (time.time() + self.config.playback_seconds, playback_id, user_id),
            )
            if not updated.rowcount:
                raise APIError.conflict("Playback is released")
        return self.descriptor(playback_id, user_id)

    def release(self, playback_id, user_id):
        playback = self._playback(playback_id, user_id, require_active=False)
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="media-asset", key=playback["asset_id"])
            conn.execute(
                "UPDATE media_playbacks SET active=0,expires_at=? "
                "WHERE playback_id=? AND user_id=?",
                (time.time(), playback_id, user_id),
            )
        return {"released": True}

    def workdir(self, job) -> Path:
        root = self.config.workdir.resolve()
        path = (root / job["work_key"]).resolve()
        if not path.is_relative_to(root) or path == root:
            raise RuntimeError("InvalidMediaWorkPath")
        return path

    def stream_file(self, playback_id, user_id, filename):
        if not FILE_NAME.fullmatch(filename):
            raise APIError.not_found("Media file not found")
        row = self._playback(playback_id, user_id)
        if row["asset_status"] == "ready":
            manifest = json.loads(row["manifest_json"])
            if filename == "index.m3u8":
                text = manifest["playlist"]
                for name, entry in manifest["files"].items():
                    if name == "index.m3u8":
                        continue
                    signed_url = self.storage.presign(
                        entry["key"], version_id=entry.get("versionId")
                    )
                    text = text.replace(
                        f'URI="{name}"',
                        f'URI="{signed_url}"',
                    )
                    text = re.sub(
                        r"(?m)^" + re.escape(name) + r"$",
                        lambda _, url=signed_url: url,
                        text,
                    )
                return {"text": text, "mime": "application/vnd.apple.mpegurl"}
            entry = manifest["files"].get(filename)
            if not entry:
                raise APIError.not_found("Media file not found")
            return {
                "redirect": self.storage.presign(entry["key"], version_id=entry.get("versionId"))
            }
        job = self.repo.import_job(row["import_id"])
        if job["status"] not in {"queued", "running"}:
            raise APIError(ErrorCode.CONFLICT, "Cold media is interrupted", 409)
        directory = self.workdir(job)
        if filename == "index.m3u8":
            path = directory / filename
            if not path.is_file():
                raise APIError(ErrorCode.CONFLICT, "Media is preparing", 425)
            text = path.read_text(encoding="utf-8")
            for line in text.splitlines():
                if (
                    line
                    and not line.startswith("#")
                    and (not FILE_NAME.fullmatch(line) or not (directory / line).is_file())
                ):
                    raise APIError(ErrorCode.CONFLICT, "Media is preparing", 425)
            return {"text": text, "mime": "application/vnd.apple.mpegurl"}
        playlist = (
            (directory / "index.m3u8").read_text(encoding="utf-8")
            if (directory / "index.m3u8").exists()
            else ""
        )
        if filename not in playlist or not (directory / filename).is_file():
            raise APIError.not_found("Media segment not ready")
        return {"path": directory / filename, "mime": "video/mp4"}
