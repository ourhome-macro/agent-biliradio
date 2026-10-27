"""Durable, bounded candidate text embedding work outside first-result delivery."""

from __future__ import annotations

import hashlib
import json

from content_embeddings import ContentEmbeddingService, text_for_track
from database import get_connection
from durable_jobs import enqueue
from job_errors import JobPermanentFailure
from library_service import LibraryService


def enqueue_text(service, tracks):
    values = {t.track_id: t for t in tracks if t.track_id and t.recording_id}
    if len(values) > 64:
        raise ValueError("Embedding batch exceeds 64 tracks")
    if not values or not service.text_base_url:
        return {"enabled": bool(service.text_base_url), "embedded": 0, "queued": 0}
    payload = {
        "trackIds": sorted(values),
        "hashes": [
            hashlib.sha256(text_for_track(values[k]).encode()).hexdigest() for k in sorted(values)
        ],
        "model": service.text_model,
        "modelVersion": service.text_model_version,
    }
    user = service.user_id or "legacy-owner"
    digest = hashlib.sha256(json.dumps([user, payload], sort_keys=True).encode()).hexdigest()
    with get_connection(service.db_path) as conn:
        job_id = enqueue(
            conn,
            kind="content_embedding",
            user_id=user,
            payload=payload,
            job_id="content-embedding:" + digest,
            retry_safe=True,
        )
    return {"enabled": True, "embedded": 0, "queued": len(values), "jobId": job_id}


def execute(db_path, job):
    payload = json.loads(job["payload_json"])
    ids = payload.get("trackIds")
    if (
        not isinstance(ids, list)
        or not 0 < len(ids) <= 64
        or any(not isinstance(k, str) or not k for k in ids)
        or not isinstance(payload.get("model"), str)
        or not isinstance(payload.get("modelVersion"), str)
    ):
        raise JobPermanentFailure("InvalidContentEmbeddingBatch")
    service = ContentEmbeddingService(db_path, user_id=job["user_id"])
    service.text_model = payload["model"]
    service.text_model_version = payload["modelVersion"]
    if not service.text_base_url:
        raise RuntimeError("Candidate embedding endpoint is unavailable")
    library = LibraryService(db_path, user_id=job["user_id"])
    tracks = [t for k in ids if (t := library.get_track(k)) is not None]
    report = service.ensure_text_embeddings(tracks)
    if report.get("error") or report.get("superseded"):
        raise RuntimeError("Candidate embedding batch must be retried")
    return report
