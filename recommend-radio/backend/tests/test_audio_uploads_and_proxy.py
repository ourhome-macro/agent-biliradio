from __future__ import annotations

import hashlib
import io
import struct
import subprocess
import time
from dataclasses import replace

import pytest
from database import get_connection
from durable_jobs import claim
from error_code import APIError
from feed_runtime import bili_proxy
from feed_runtime.bili_proxy import ProxyMediaService, sidx_segments
from feed_runtime.config import FeedConfig
from feed_runtime.repository import FeedRepository
from feed_runtime.source import MediaSource
from job_errors import JobPermanentFailure
from models import Track
from ugc.audio_worker import AudioWorker
from ugc.service import UGCService
from ugc.uploads import AudioUploads


class Storage:
    def __init__(self):
        self.objects = {}

    def ensure_bucket(self):
        pass

    def upload_policy(self, key, size, mime, expires):
        return {
            "url": "https://storage.invalid/media",
            "fields": {"key": key, "Content-Type": mime},
            "method": "POST",
        }

    def download_verified(self, key, path, size, digest):
        data = self.objects[key]
        if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("checksum mismatch")
        path.write_bytes(data)

    def put_file(self, key, path, *, content_type):
        data = path.read_bytes()
        self.objects[key] = data
        return {"key": key, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    def remove(self, key):
        self.objects.pop(key, None)

    def presign(self, key, **kwargs):
        return "https://storage.invalid/" + key


@pytest.fixture
def studio(tmp_path):
    repo = FeedRepository(tmp_path / "audio.sqlite3")
    service, storage = UGCService(repo.db_path), Storage()
    post = service.command(
        "legacy-owner",
        "create",
        {"kind": "audio", "title": "Original music", "visibility": "public"},
        "draft",
    )
    config = replace(FeedConfig(), workdir=tmp_path / "work")
    return repo, service, storage, post, config


def upload(studio, data=b"not yet validated"):
    repo, service, storage, post, config = studio
    uploads = AudioUploads(repo.db_path, storage)
    payload = {
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "contentType": "audio/wav",
    }
    session = uploads.initiate("legacy-owner", post["contentId"], payload, "upload")
    post = service.command(
        "legacy-owner",
        "edit",
        {"audioUploadId": session["uploadId"], "expectedVersion": post["version"]},
        "attach",
        post["contentId"],
    )
    storage.objects[session["upload"]["fields"]["key"]] = data
    return uploads, session, post


def test_audio_publication_waits_for_verified_asset_and_completion_is_idempotent(studio):
    repo, service, storage, _, _ = studio
    uploads, session, post = upload(studio)
    with pytest.raises(APIError):
        service.command(
            "legacy-owner",
            "publish",
            {"expectedVersion": post["version"]},
            "publish",
            post["contentId"],
        )
    assert uploads.complete("legacy-owner", session["uploadId"])["status"] == "queued"
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_jobs WHERE kind='ugc_audio'").fetchone()[0]
            == 1
        )
        assert conn.execute("SELECT status FROM ugc_posts").fetchone()[0] == "draft"


def test_upload_identity_size_and_ownership_cannot_be_substituted(studio):
    repo, service, storage, post, _ = studio
    uploads, session, _ = upload(studio)
    with pytest.raises(APIError):
        uploads.complete("other-user", session["uploadId"])
    with pytest.raises(APIError):
        uploads.initiate(
            "legacy-owner",
            post["contentId"],
            {"bytes": 1, "sha256": "a" * 64, "contentType": "audio/wav"},
            "upload",
        )
    with pytest.raises(APIError):
        uploads.initiate(
            "legacy-owner",
            post["contentId"],
            {"bytes": 65 * 1024 * 1024, "sha256": "a" * 64, "contentType": "audio/wav"},
            "large",
        )


def test_checksum_failure_cannot_create_a_ready_asset(studio):
    repo, _, storage, _, config = studio
    uploads, session, _ = upload(studio)
    storage.objects[session["upload"]["fields"]["key"]] = b"overwritten"
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        job = claim(conn, "ugc-audio:" + session["uploadId"])
    with pytest.raises(JobPermanentFailure):
        AudioWorker(repo.db_path, config, storage).run(session["uploadId"], job)
    assert uploads.get("legacy-owner", session["uploadId"])["status"] == "failed"
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM media_assets WHERE status='ready'").fetchone()[0]
            == 0
        )


def test_real_audio_processing_outbox_and_native_audio_descriptor(studio, tmp_path):
    repo, service, storage, _, config = studio
    wav = tmp_path / "tone.wav"
    subprocess.run(
        [
            config.ffmpeg,
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440",
            "-t",
            "1",
            str(wav),
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    uploads, session, post = upload(studio, wav.read_bytes())
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        job = claim(conn, "ugc-audio:" + session["uploadId"])
    worker = AudioWorker(repo.db_path, config, storage)
    assert worker.run(session["uploadId"], job)["status"] == "ready"
    assert worker.run(session["uploadId"], job)["status"] == "ready"
    fresh = service.detail("legacy-owner", post["contentId"])
    published = service.command(
        "legacy-owner",
        "publish",
        {"expectedVersion": fresh["version"]},
        "publish",
        post["contentId"],
    )
    media = ProxyMediaService(repo, config, storage)
    playback = media.create("legacy-owner", post["contentId"], "play")
    assert playback["manifestUrl"].endswith("/audio.m4a") and playback["sourceComplete"]
    assert media.stream_file(playback["playbackId"], "legacy-owner", "audio.m4a")["redirect"]
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM domain_event_outbox "
                "WHERE json_extract(event_json,'$.aggregateId')=?",
                (post["contentId"],),
            ).fetchone()[0]
            >= 4
        )
    assert published["status"] == "published"


def test_expiry_cleans_staging_only_after_signature_deadline(studio):
    repo, _, storage, _, _ = studio
    uploads, session, _ = upload(studio)
    assert uploads.expire()["removedStagingObjects"] == 0
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE ugc_audio_uploads SET expires_at=?", (time.time() - 1,))
    assert uploads.expire()["removedStagingObjects"] == 1
    assert uploads.expire()["removedStagingObjects"] == 0
    assert not storage.objects


def test_stale_worker_cannot_publish_or_fail_a_new_owners_upload(studio):
    repo, _, storage, _, config = studio
    uploads, session, _ = upload(studio)
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        job = claim(conn, "ugc-audio:" + session["uploadId"])
        conn.execute(
            "UPDATE durable_jobs SET lease_token='new-owner' WHERE job_id=?", (job["job_id"],)
        )
    with pytest.raises(RuntimeError, match="LeaseLost"):
        AudioWorker(repo.db_path, config, storage).run(session["uploadId"], job)
    assert uploads.get("legacy-owner", session["uploadId"])["status"] == "queued"


def test_untrusted_playlist_cannot_make_ffmpeg_read_external_files(studio):
    repo, _, storage, _, config = studio
    uploads, session, _ = upload(studio, b"#EXTM3U\n#EXTINF:2,\nfile:///etc/passwd\n")
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        job = claim(conn, "ugc-audio:" + session["uploadId"])
    with pytest.raises(JobPermanentFailure):
        AudioWorker(repo.db_path, config, storage).run(session["uploadId"], job)
    assert uploads.get("legacy-owner", session["uploadId"])["status"] == "failed"


def test_exhausted_transport_job_is_not_stuck_in_processing(studio):
    repo, _, _, _, _ = studio
    uploads, session, _ = upload(studio)
    uploads.complete("legacy-owner", session["uploadId"])
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE durable_jobs SET status='failed',error='AttemptsExhausted'")
    assert uploads.get("legacy-owner", session["uploadId"])["status"] == "failed"


def make_sidx():
    content = b"\0\0\0\0" + struct.pack(">IIIIHH", 1, 1000, 0, 0, 0, 2)
    content += struct.pack(">III", 12, 1000, 0x90000000) * 2
    return struct.pack(">I4s", len(content) + 8, b"sidx") + content


@pytest.fixture
def proxy(tmp_path):
    repo = FeedRepository(tmp_path / "proxy.sqlite3")
    content = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live concert", duration=12),
        user_id="legacy-owner",
    )

    class Source:
        def __init__(self, **kwargs):
            pass

        def resolve(self, track):
            return MediaSource(
                "https://cdn.bilivideo.com/v",
                "https://cdn.bilivideo.com/a",
                2,
                {},
                video_codec="avc1.64001e",
                audio_codec="mp4a.40.2",
                video_init="0-99",
                video_index="100-155",
                audio_init="0-99",
                audio_index="100-155",
            )

        def close(self):
            pass

    # Any object-store call in the proxy path is a regression.
    class ForbiddenStorage:
        def __getattr__(self, name):
            raise AssertionError("Bilibili proxy must not access MinIO: " + name)

    media = ProxyMediaService(repo, FeedConfig(enabled=True), ForbiddenStorage(), source_factory=Source)
    playback = media.create("legacy-owner", content, "start")
    return repo, content, media, playback


def test_bili_proxy_never_enqueues_imports_or_stores_signed_upstream_urls(proxy):
    repo, content, media, playback = proxy
    assert playback["mode"] == "proxy" and playback["persistenceStatus"] == "not_required"
    assert media.create("legacy-owner", content, "start")["playbackId"] == playback["playbackId"]
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM media_import_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()[0] == 0
        assert "http" not in conn.execute("SELECT manifest_json FROM media_assets").fetchone()[0]


def test_sidx_playlist_ranges_preserve_original_offsets(proxy, monkeypatch):
    _, _, media, playback = proxy
    sidx = make_sidx()
    assert len(sidx) == 56
    assert sidx_segments(sidx, 100) == [(156, 12, 1.0), (168, 12, 1.0)]

    class Upstream:
        status_code = 206
        raw = io.BytesIO(sidx)

        def close(self):
            pass

    class Session:
        def close(self):
            pass

    monkeypatch.setattr(bili_proxy, "open_upstream", lambda *a, **kw: (Upstream(), Session()))
    text = media.stream_file(playback["playbackId"], "legacy-owner", "video.m3u8")["text"]
    assert "#EXT-X-BYTERANGE:12@156" in text and "#EXT-X-BYTERANGE:12@168" in text
    assert 'BYTERANGE="100@0"' in text


def test_proxy_rechecks_visibility_and_playback_lease(proxy):
    repo, content, media, playback = proxy
    media.release(playback["playbackId"], "legacy-owner")
    with pytest.raises(APIError):
        media.stream_file(playback["playbackId"], "legacy-owner", "index.m3u8")
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET scope='user:other',owner_id='other' WHERE content_id=?",
            (content,),
        )
    with pytest.raises(APIError):
        media.create("legacy-owner", content, "new-key")


def test_expired_upstream_signature_is_resolved_once_for_index_requests(proxy, monkeypatch):
    _, _, media, playback = proxy
    calls = []

    class Response:
        def __init__(self, status):
            self.status_code, self.raw = status, io.BytesIO(make_sidx())

        def close(self):
            pass

    class Session:
        def close(self):
            pass

    def request(*args, **kwargs):
        calls.append(1)
        return Response(403 if len(calls) == 1 else 206), Session()

    monkeypatch.setattr(bili_proxy, "open_upstream", request)
    assert media.stream_file(playback["playbackId"], "legacy-owner", "video.m3u8")["text"]
    assert len(calls) == 2


def test_proxy_capacity_is_returned_when_clients_disconnect(proxy, monkeypatch):
    _, _, media, playback = proxy

    class Response:
        status_code = 206

        def close(self):
            pass

    class Session:
        def close(self):
            pass

    monkeypatch.setattr(bili_proxy, "open_upstream", lambda *a, **kw: (Response(), Session()))
    open_requests = []
    try:
        for _ in range(32):
            open_requests.append(
                media.stream_file(
                    playback["playbackId"], "legacy-owner", "video.mp4", range_header="bytes=0-10"
                )
            )
        with pytest.raises(APIError) as error:
            media.stream_file(playback["playbackId"], "legacy-owner", "video.mp4")
        assert error.value.status_code == 503
    finally:
        for response in open_requests:
            response["close"]()
            response["close"]()
    response = media.stream_file(playback["playbackId"], "legacy-owner", "video.mp4")
    response["close"]()


def test_untrusted_redirect_cannot_turn_proxy_into_local_network_access(monkeypatch):
    class Response:
        status_code = 302
        headers = {"Location": "http://127.0.0.1:1234/internal"}

        def close(self):
            pass

    class Session:
        calls = 0

        def request(self, *args, **kwargs):
            self.calls += 1
            return Response()

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(bili_proxy.requests, "Session", lambda: session)
    with pytest.raises(ValueError):
        bili_proxy.open_upstream("https://cdn.bilivideo.com/video", {})
    assert session.calls == 1


@pytest.mark.parametrize("data", [b"", b"bad", struct.pack(">I4s", 8000, b"sidx")])
def test_invalid_sidx_cannot_generate_a_playlist(data):
    with pytest.raises((ValueError, struct.error)):
        sidx_segments(data, 0)
