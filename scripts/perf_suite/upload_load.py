from __future__ import annotations

import hashlib
import io
import time
import wave
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import requests
from database import get_connection
from durable_jobs import claim, finish
from feed_runtime.config import FeedConfig
from feed_runtime.storage import MinIOStorage
from ugc.audio_worker import AudioWorker
from ugc.service import UGCService
from ugc.uploads import AudioUploads

from .common import percentile, save
from .infra import Minio


def wav_bytes(size):
    data = io.BytesIO()
    with wave.open(data, "wb") as stream:
        stream.setparams((1, 2, 44100, 0, "NONE", "not compressed"))
        stream.writeframes(b"\0\0" * ((size - 44) // 2))
    return data.getvalue()


def run(directory, output, db, *, quick=False):
    server = Minio(directory / "minio")
    try:
        config = replace(
            FeedConfig(),
            minio_endpoint=f"127.0.0.1:{server.port}",
            minio_public_endpoint=f"127.0.0.1:{server.port}",
            minio_access_key=server.user,
            minio_secret_key=server.secret,
            bucket="perf-audio",
            workdir=directory / "audio-work",
        )
        storage = MinIOStorage(config)
        uploads, posts = AudioUploads(db, storage), UGCService(db)
        cases = []
        bodies = {
            size: wav_bytes(size)
            for size in (64 * 1024, 1024 * 1024, 8 * 1024 * 1024, 32 * 1024 * 1024)
        }

        def one(index):
            size = list(bodies)[index % len(bodies)]
            body = bodies[size]
            user = "upload-" + uuid4().hex
            post = posts.command(
                user, "create", {"kind": "audio", "title": "Load fixture"}, "draft"
            )
            session = uploads.initiate(
                user,
                post["contentId"],
                {
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "contentType": "audio/wav",
                },
                "upload",
            )
            # Short real signatures for this fixture only; product default stays 15 minutes.
            expiry = time.time() + 15
            key = session["upload"]["fields"]["key"]
            policy = storage.upload_policy(key, len(body), "audio/wav", expiry)
            with get_connection(db) as conn:
                conn.execute(
                    "UPDATE ugc_audio_uploads SET expires_at=? WHERE upload_id=?",
                    (expiry, session["uploadId"]),
                )
            post = posts.command(
                user,
                "edit",
                {"audioUploadId": session["uploadId"], "expectedVersion": post["version"]},
                "attach",
                post["contentId"],
            )
            began = time.monotonic()
            response = requests.post(
                policy["url"],
                data=policy["fields"],
                files={"file": ("fixture.wav", body, "audio/wav")},
                timeout=30,
            )
            uploaded = time.monotonic()
            if response.status_code not in {200, 201, 204}:
                return {
                    "bytes": size,
                    "status": response.status_code,
                    "uploadMs": (uploaded - began) * 1000,
                }
            # One abandonment per wave tests orphan staging cleanup without inventing corruption.
            abandoned = index == 0
            confirm_ms, ready_ms = None, None
            if not abandoned:
                confirm_start = time.monotonic()
                uploads.complete(user, session["uploadId"])
                confirm_ms = (time.monotonic() - confirm_start) * 1000
                with get_connection(db) as conn:
                    job = claim(conn, "ugc-audio:" + session["uploadId"])
                try:
                    value = AudioWorker(db, config, storage).run(session["uploadId"], job)
                    with get_connection(db) as conn:
                        finish(conn, job, result=value)
                    ready_ms = (time.monotonic() - uploaded) * 1000
                except Exception as error:
                    with get_connection(db) as conn:
                        finish(conn, job, error=error)
                    return {
                        "bytes": size,
                        "status": 0,
                        "errorType": type(error).__name__,
                        "uploadMs": (uploaded - began) * 1000,
                    }
            return {
                "bytes": size,
                "status": response.status_code,
                "uploadMs": (uploaded - began) * 1000,
                "confirmMs": confirm_ms,
                "readyMs": ready_ms,
                "expiry": expiry,
                "abandoned": abandoned,
            }

        for concurrency in (1, 4, 8):
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                rows = list(pool.map(one, range(4 if quick else max(4, concurrency * 2))))
            result = {
                "concurrency": concurrency,
                "requests": len(rows),
                "uploadSuccessRate": sum(r["status"] in {200, 201, 204} for r in rows) / len(rows),
                "uploadP95Ms": percentile([r["uploadMs"] for r in rows], 0.95),
                "confirmationP95Ms": percentile(
                    [r["confirmMs"] for r in rows if r.get("confirmMs") is not None], 0.95
                ),
                "readyP95Ms": percentile(
                    [r["readyMs"] for r in rows if r.get("readyMs") is not None], 0.95
                ),
            }
            cases.append(result)
            save(output / f"raw-uploads-{concurrency}.json", rows)
        latest = max((row.get("expiry", 0) for row in rows), default=0)
        if latest > time.time():
            time.sleep(latest - time.time() + 0.2)
        for _ in range(3):
            uploads.expire()
        residual = len(
            list(storage.client.list_objects(config.bucket, prefix="ugc-staging/", recursive=True))
        )
        return {
            "stages": cases,
            "stagingResidualObjects": residual,
            "scope": (
                "real MinIO POST and AudioWorker; valid silence WAVs; 15-second fixture signatures"
            ),
            "sizesBytes": list(bodies),
        }
    finally:
        server.close()
