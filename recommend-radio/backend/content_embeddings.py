from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse
from uuid import uuid4

import requests
from database import get_connection
from models import Track


class ContentEmbeddingService:
    """Persist candidate vectors outside the online ranking request."""

    def __init__(
        self,
        db_path: str,
        *,
        user_id: str | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.db_path = db_path
        self.user_id = user_id
        self.text_base_url = os.getenv("AMEM_EMBEDDING_BASE_URL", "").rstrip("/")
        self.text_model = os.getenv("AMEM_EMBEDDING_MODEL", "bge-m3")
        self.text_model_version = os.getenv("AMEM_EMBEDDING_MODEL_VERSION", "default")
        self.audio_base_url = os.getenv("RECOMMEND_AUDIO_EMBEDDING_BASE_URL", "").rstrip("/")
        self.audio_model = os.getenv("RECOMMEND_AUDIO_EMBEDDING_MODEL", "laion-clap")
        self.audio_model_version = os.getenv(
            "RECOMMEND_AUDIO_EMBEDDING_MODEL_VERSION",
            "default",
        )
        self.timeout_seconds = max(float(timeout_seconds), 0.1)

    def ensure_text_embeddings(self, tracks: Iterable[Track]) -> dict[str, Any]:
        by_recording: dict[str, Track] = {}
        for track in tracks:
            if not track.recording_id:
                continue
            current = by_recording.get(track.recording_id)
            if current is None or len(text_for_track(track)) > len(text_for_track(current)):
                by_recording[track.recording_id] = track
        values = list(by_recording.values())
        if not values:
            return {"enabled": bool(self.text_base_url), "embedded": 0, "queued": 0}
        pending: list[tuple[Track, str, str]] = []
        with get_connection(self.db_path) as conn:
            for track in values:
                content = text_for_track(track)
                content_hash = _hash(content)
                current = conn.execute(
                    """
                    SELECT content_hash FROM content_embeddings
                    WHERE recording_id=? AND modality='text'
                      AND model_name=? AND model_version=? AND status='ready'
                    """,
                    (track.recording_id, self.text_model, self.text_model_version),
                ).fetchone()
                if current is not None and current["content_hash"] == content_hash:
                    continue
                if current is not None:
                    conn.execute(
                        """
                        UPDATE content_embeddings SET status='stale', updated_at=?
                        WHERE recording_id=? AND modality='text'
                          AND model_name=? AND model_version=?
                        """,
                        (
                            _utc_now(),
                            track.recording_id,
                            self.text_model,
                            self.text_model_version,
                        ),
                    )
                self._upsert_job(
                    conn,
                    track=track,
                    modality="text",
                    model_name=self.text_model,
                    model_version=self.text_model_version,
                    content_hash=content_hash,
                )
                pending.append((track, content, content_hash))
        if not pending or not self.text_base_url:
            return {
                "enabled": bool(self.text_base_url),
                "embedded": 0,
                "queued": len(pending),
            }

        try:
            vectors = self._embed_texts([content for _track, content, _hash_value in pending])
            if len(vectors) != len(pending):
                raise ValueError("embedding count mismatch")
            dimensions = {len(vector) for vector in vectors if vector}
            if len(dimensions) != 1 or any(not vector for vector in vectors):
                raise ValueError("embedding vectors are empty or dimensionally inconsistent")
        except Exception as exc:
            self._fail_jobs(
                [track.recording_id or "" for track, _content, _hash_value in pending],
                modality="text",
                error=exc,
            )
            return {
                "enabled": True,
                "embedded": 0,
                "queued": len(pending),
                "error": type(exc).__name__,
            }

        now = _utc_now()
        saved, superseded = 0, 0
        with get_connection(self.db_path) as conn:
            if getattr(conn, "dialect", None) != "mysql":
                conn.execute("BEGIN IMMEDIATE")
            from library_service import LibraryService

            # Lock in a stable order, then recheck metadata after the model call.
            # A slow old embedding must not replace a newer metadata projection.
            current = {}
            for track, _content, _hash_value in sorted(pending, key=lambda p: p[0].track_id):
                row = (
                    conn.lock_row("tracks", {"track_id": track.track_id})
                    if getattr(conn, "dialect", None) == "mysql"
                    else conn.execute("SELECT * FROM tracks WHERE track_id=?", (track.track_id,)).fetchone()
                )
                current[track.track_id] = row
            for (track, _content, content_hash), vector in zip(
                pending,
                vectors,
                strict=True,
            ):
                row = current[track.track_id]
                if row is None or _hash(
                    text_for_track(LibraryService._track_from_row(row))
                ) != content_hash:
                    superseded += 1
                    continue
                self._save_vector(
                    conn,
                    recording_id=track.recording_id or "",
                    modality="text",
                    model_name=self.text_model,
                    model_version=self.text_model_version,
                    content_hash=content_hash,
                    vector=vector,
                    now=now,
                )
                saved += 1
        return {"enabled": True, "embedded": saved, "queued": superseded, "superseded": superseded}

    def text_vectors(self, track_ids: Iterable[str]) -> dict[str, list[float]]:
        return self._vectors(
            track_ids,
            modality="text",
            model_name=self.text_model,
            model_version=self.text_model_version,
        )

    def audio_vectors(self, track_ids: Iterable[str]) -> dict[str, list[float]]:
        return self._vectors(
            track_ids,
            modality="audio",
            model_name=self.audio_model,
            model_version=self.audio_model_version,
        )

    def _vectors(
        self,
        track_ids: Iterable[str],
        *,
        modality: str,
        model_name: str,
        model_version: str,
    ) -> dict[str, list[float]]:
        values = tuple(dict.fromkeys(str(value) for value in track_ids if value))
        if not values:
            return {}
        placeholders = ",".join("?" for _ in values)
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT t.track_id, e.vector_json
                FROM tracks t
                JOIN content_embeddings e ON e.recording_id=t.recording_id
                WHERE t.track_id IN ({placeholders})
                  AND e.modality=? AND e.model_name=? AND e.model_version=?
                  AND e.status='ready'
                """,
                (*values, modality, model_name, model_version),
            ).fetchall()
        return {
            str(row["track_id"]): _vector(row["vector_json"])
            for row in rows
            if _vector(row["vector_json"])
        }

    def embed_query(self, text: str) -> list[float] | None:
        if not self.text_base_url or not text.strip():
            return None
        try:
            # Online query encoding has a separate budget from offline batches.
            # A copy avoids racing worker/query settings on a shared service.
            query = copy.copy(self)
            query.timeout_seconds = min(self.timeout_seconds, 0.25)
            vectors = query._embed_texts([text])
        except Exception:
            return None
        return vectors[0] if vectors else None

    def enqueue_audio_embeddings(self, tracks: Iterable[Track], *, force: bool = False) -> int:
        if not self.audio_base_url:
            return 0
        queued = 0
        with get_connection(self.db_path) as conn:
            for track in tracks:
                if not track.recording_id or not track.cid:
                    continue
                if not force and self.user_id and not self._audio_eligible(track.track_id or ""):
                    continue
                content_hash = _hash(
                    f"{track.bvid}:{track.cid}:{track.duration}:{self.audio_model_version}"
                )
                current = conn.execute(
                    """
                    SELECT content_hash FROM content_embeddings
                    WHERE recording_id=? AND modality='audio'
                      AND model_name=? AND model_version=? AND status='ready'
                    """,
                    (track.recording_id, self.audio_model, self.audio_model_version),
                ).fetchone()
                if current is not None and current["content_hash"] == content_hash:
                    continue
                self._upsert_job(
                    conn,
                    track=track,
                    modality="audio",
                    model_name=self.audio_model,
                    model_version=self.audio_model_version,
                    content_hash=content_hash,
                )
                queued += 1
        return queued

    def _audio_eligible(self, track_id: str) -> bool:
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT 1
                WHERE EXISTS (
                    SELECT 1 FROM likes WHERE user_id=? AND track_id=?
                ) OR EXISTS (
                    SELECT 1 FROM playback_recent
                    WHERE user_id=? AND track_id=? AND completed=1
                ) OR EXISTS (
                    SELECT 1
                    FROM discovery_item_sources s
                    JOIN discovery_keywords k ON k.keyword_id=s.keyword_id
                    WHERE s.user_id=? AND s.track_id=? AND k.yield_score>=0.45
                )
                """,
                (
                    self.user_id,
                    track_id,
                    self.user_id,
                    track_id,
                    self.user_id,
                    track_id,
                ),
            ).fetchone()
        return row is not None

    def process_audio_jobs(self, bili_client: Any, *, limit: int = 8) -> dict[str, Any]:
        if not self.audio_base_url:
            return {"enabled": False, "processed": 0, "failed": 0}
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT j.*, t.bvid, t.cid, t.duration
                FROM content_embedding_jobs j
                JOIN tracks t ON t.track_id=j.track_id
                WHERE j.status IN ('pending', 'failed') AND j.modality='audio'
                  AND j.attempts < 3
                ORDER BY j.updated_at
                LIMIT ?
                """,
                (max(1, min(int(limit), 32)),),
            ).fetchall()
        processed = 0
        failed = 0
        for row in rows:
            now = _utc_now()
            try:
                stream = bili_client.get_audio_stream(str(row["bvid"]), int(row["cid"]))
                if not _allowed_audio_url(stream.url):
                    raise ValueError("audio stream host is not trusted")
                response = requests.post(
                    f"{self.audio_base_url}/audio-embeddings",
                    json={
                        "model": self.audio_model,
                        "input": [
                            {
                                "id": row["recording_id"],
                                "url": stream.url,
                                "headers": {
                                    "Referer": f"https://www.bilibili.com/video/{row['bvid']}"
                                },
                                "startSeconds": min(30, max(int(row["duration"] or 0) // 4, 0)),
                                "durationSeconds": 30,
                            }
                        ],
                    },
                    timeout=max(self.timeout_seconds, 30.0),
                )
                response.raise_for_status()
                data = response.json().get("data") or []
                vector = _vector(data[0].get("embedding") if data else None)
                if not vector:
                    raise ValueError("audio embedding response did not include a vector")
                with get_connection(self.db_path) as conn:
                    self._save_vector(
                        conn,
                        recording_id=str(row["recording_id"]),
                        modality="audio",
                        model_name=self.audio_model,
                        model_version=self.audio_model_version,
                        content_hash=str(row["content_hash"]),
                        vector=vector,
                        now=now,
                    )
                processed += 1
            except Exception as exc:
                self._fail_jobs(
                    [str(row["recording_id"])],
                    modality="audio",
                    error=exc,
                )
                failed += 1
        return {"enabled": True, "processed": processed, "failed": failed}

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        batch_size = int(os.getenv("RECOMMEND_EMBEDDING_BATCH_SIZE", "8"))
        if not 1 <= batch_size <= 64:
            raise ValueError("RECOMMEND_EMBEDDING_BATCH_SIZE must be between 1 and 64")
        vectors: list[list[float]] = []
        for offset in range(0, len(texts), batch_size):
            batch = texts[offset:offset + batch_size]
            response = requests.post(
                f"{self.text_base_url}/embeddings",
                json={"model": self.text_model, "input": batch},
                headers={"Authorization": f"Bearer {os.getenv('BGE_M3_API_KEY', 'local-embedding')}"},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = sorted(response.json().get("data") or [], key=lambda item: int(item["index"]))
            if len(data) != len(batch) or [int(item["index"]) for item in data] != list(range(len(batch))):
                raise ValueError("embedding response does not match the input batch")
            vectors.extend(_vector(item.get("embedding")) for item in data)
        return vectors

    @staticmethod
    def _save_vector(
        conn: Any,
        *,
        recording_id: str,
        modality: str,
        model_name: str,
        model_version: str,
        content_hash: str,
        vector: list[float],
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO content_embeddings (
                recording_id, modality, model_name, model_version, content_hash,
                vector_json, dimension, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?)
            ON CONFLICT(recording_id, modality, model_name, model_version) DO UPDATE SET
                content_hash=excluded.content_hash,
                vector_json=excluded.vector_json,
                dimension=excluded.dimension,
                status='ready',
                updated_at=excluded.updated_at
            """,
            (
                recording_id,
                modality,
                model_name,
                model_version,
                content_hash,
                json.dumps(vector, separators=(",", ":")),
                len(vector),
                now,
                now,
            ),
        )
        conn.execute(
            """
            UPDATE content_embedding_jobs SET status='completed', error='', updated_at=?
            WHERE recording_id=? AND modality=? AND model_name=? AND model_version=?
            """,
            (now, recording_id, modality, model_name, model_version),
        )

    @staticmethod
    def _upsert_job(
        conn: Any,
        *,
        track: Track,
        modality: str,
        model_name: str,
        model_version: str,
        content_hash: str,
    ) -> None:
        now = _utc_now()
        conn.execute(
            """
            INSERT INTO content_embedding_jobs (
                job_id, recording_id, track_id, modality, model_name,
                model_version, content_hash, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(recording_id, modality, model_name, model_version, content_hash)
            DO UPDATE SET track_id=excluded.track_id,
                          status=CASE
                              WHEN content_embedding_jobs.status='completed' THEN 'completed'
                              ELSE 'pending'
                          END,
                          updated_at=excluded.updated_at
            """,
            (
                f"embedding:{uuid4().hex}",
                track.recording_id,
                track.track_id,
                modality,
                model_name,
                model_version,
                content_hash,
                now,
                now,
            ),
        )

    def _fail_jobs(self, recording_ids: list[str], *, modality: str, error: Exception) -> None:
        values = [value for value in recording_ids if value]
        if not values:
            return
        placeholders = ",".join("?" for _ in values)
        with get_connection(self.db_path) as conn:
            conn.execute(
                f"""
                UPDATE content_embedding_jobs
                SET status='failed', attempts=attempts+1, error=?, updated_at=?
                WHERE recording_id IN ({placeholders}) AND modality=?
                  AND status!='completed'
                """,
                (str(error)[:300], _utc_now(), *values, modality),
            )


def text_for_track(track: Track) -> str:
    values = [
        track.canonical_title,
        track.canonical_artist,
        track.version_type,
        track.title,
        track.page_title or "",
        track.owner,
        track.type_name,
        " ".join(track.tags),
        track.description,
    ]
    return " ".join(" ".join(str(value).split()) for value in values if value)[:4000]


def _vector(value: Any) -> list[float]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, list):
        return []
    try:
        return [float(item) for item in value]
    except (TypeError, ValueError):
        return []


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _allowed_audio_url(value: str) -> bool:
    parsed = urlparse(str(value or ""))
    host = (parsed.hostname or "").casefold()
    return parsed.scheme == "https" and (
        host.endswith(".bilivideo.com") or host.endswith(".hdslb.com")
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
