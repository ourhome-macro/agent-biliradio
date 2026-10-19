from __future__ import annotations

import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from database import get_connection
from feed_runtime.config import FeedConfig
from feed_runtime.media import MediaService
from feed_runtime.repository import FeedRepository
from feed_runtime.source import MediaSource
from feed_runtime.storage import file_hash
from feed_runtime.worker import MediaWorker
from job_errors import JobPermanentFailure
from models import Track


class ObjectStorage:
    def __init__(self):
        self.objects = {}
        self.fail = False

    def ensure_bucket(self):
        pass

    def put_file(self, key, path, *, content_type):
        if self.fail and key.endswith("index.m3u8"):
            raise ConnectionError("controlled object failure")
        data = path.read_bytes()
        self.objects[key] = data
        return {"key": key, "bytes": len(data), "sha256": file_hash(path), "versionId": None}

    def presign(self, key, *, version_id=None):
        assert key in self.objects
        return "https://objects.invalid/" + key


@pytest.fixture(scope="module")
def video_files(tmp_path_factory):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("FFmpeg and ffprobe required for media pipeline")
    folder = tmp_path_factory.mktemp("media-fixture")
    for filename, source, options in (
        (
            "video.mp4",
            "testsrc=size=320x180:rate=30",
            ["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "120", "-an"],
        ),
        ("audio.m4a", "sine=frequency=1000:sample_rate=48000", ["-c:a", "aac", "-vn"]),
    ):
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                source,
                "-t",
                "12",
                *options,
                "-movflags",
                "+faststart",
                str(folder / filename),
            ],
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW if __import__("os").name == "nt" else 0,
        )
    return folder


@pytest.fixture
def setup(tmp_path, monkeypatch, video_files):
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(tmp_path / "media.sqlite3")
    config = replace(FeedConfig(), workdir=tmp_path / "work", cancel_grace_seconds=0)
    storage = ObjectStorage()
    content = repo.admit(
        Track(bvid="BV1234567890", cid=1, title="Music Live Concert", duration=12),
        user_id="legacy-owner",
    )

    class Source:
        calls = 0

        def __init__(self, **kwargs):
            pass

        def resolve_cid(self, track):
            return track.cid or 1

        def resolve(self, track):
            Source.calls += 1
            return MediaSource(
                str(video_files / "video.mp4"), str(video_files / "audio.m4a"), 12, {}
            )

        def close(self):
            pass

    media = MediaService(repo, config, storage, source_factory=Source)
    return repo, config, storage, content, Source, media


def import_id(repo):
    with get_connection(repo.db_path) as conn:
        return conn.execute(
            "SELECT import_id FROM media_import_jobs ORDER BY attempt DESC LIMIT 1"
        ).fetchone()[0]


def test_complete_materialization_and_replay_never_fetch_upstream(setup):
    repo, config, storage, content, Source, media = setup
    first = media.create("legacy-owner", content, "first")
    assert first["status"] == "preparing"
    result = MediaWorker(repo, config, storage, source_factory=Source).run(import_id(repo))
    assert result["status"] == "ready" and Source.calls == 1
    descriptor = media.descriptor(first["playbackId"], "legacy-owner")
    assert descriptor["mode"] == "stored" and descriptor["sourceComplete"]
    text = media.stream_file(first["playbackId"], "legacy-owner", "index.m3u8")["text"]
    assert "https://objects.invalid/" in text and "#EXT-X-ENDLIST" in text
    assert any(key.endswith("audio.m4a") for key in storage.objects)

    def forbidden(_):
        raise AssertionError("ready playback must not fetch upstream")

    Source.resolve = forbidden
    again = media.create("legacy-owner", content, "again")
    assert again["mode"] == "stored"
    assert media.stream_file(again["playbackId"], "legacy-owner", "seg_000000.m4s")["redirect"]
    assert Source.calls == 1


def test_cold_to_stored_keeps_media_generation_and_fresh_fragment_signatures(setup):
    repo, config, storage, content, Source, media = setup
    playback = media.create("legacy-owner", content, "continuity")
    cold_generations = []
    upload = storage.put_file

    def observe(key, path, **kwargs):
        cold = media.descriptor(playback["playbackId"], "legacy-owner")
        assert cold["status"] == "streaming"
        cold_generations.append(cold["generationKey"])
        return upload(key, path, **kwargs)

    storage.put_file = observe
    MediaWorker(repo, config, storage, source_factory=Source).run(import_id(repo))
    stored = media.descriptor(playback["playbackId"], "legacy-owner")
    assert stored["mode"] == "stored"
    assert cold_generations and set(cold_generations) == {stored["generationKey"]}
    signatures = iter(
        ("https://objects.invalid/new-token-1", "https://objects.invalid/new-token-2")
    )
    storage.presign = lambda *_args, **_kwargs: next(signatures)
    assert media.stream_file(playback["playbackId"], "legacy-owner", "seg_000000.m4s")[
        "redirect"
    ].endswith("new-token-1")
    assert media.stream_file(playback["playbackId"], "legacy-owner", "seg_000000.m4s")[
        "redirect"
    ].endswith("new-token-2")


def test_failed_object_upload_reuses_complete_source(setup):
    repo, config, storage, content, Source, media = setup
    playback = media.create("legacy-owner", content, "one")
    storage.fail = True
    with pytest.raises(JobPermanentFailure):
        MediaWorker(repo, config, storage, source_factory=Source).run(import_id(repo))
    assert Source.calls == 1
    interrupted = media.descriptor(playback["playbackId"], "legacy-owner")
    assert interrupted["status"] == "streaming" and interrupted["persistenceStatus"] == "failed"
    assert media.heartbeat(playback["playbackId"], "legacy-owner")["sourceComplete"]
    assert media.stream_file(playback["playbackId"], "legacy-owner", "seg_000000.m4s")[
        "path"
    ].is_file()
    from feed_runtime.maintenance import cleanup

    cleanup(repo, config, None, apply=True, minimum_age=0)
    assert media.stream_file(playback["playbackId"], "legacy-owner", "seg_000000.m4s")[
        "path"
    ].is_file()
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT status FROM media_assets").fetchone()[0] != "ready"
        assert conn.execute("SELECT complete_source FROM media_import_jobs").fetchone()[0] == 1
    storage.fail = False
    second = media.create("legacy-owner", content, "two")
    MediaWorker(repo, config, storage, source_factory=Source).run(import_id(repo))
    assert (
        media.descriptor(playback["playbackId"], "legacy-owner")["generationKey"]
        == interrupted["generationKey"]
    )
    assert (
        Source.calls == 1
        and media.descriptor(second["playbackId"], "legacy-owner")["status"] == "ready"
    )


def test_last_viewer_detach_cancels_without_publishing_partial(setup):
    repo, config, storage, content, Source, media = setup
    playback = media.create("legacy-owner", content, "one")
    media.release(playback["playbackId"], "legacy-owner")
    # Zero-grace cancellation is observed on the second demand check.
    job = repo.claim_import(import_id(repo), config)
    assert repo.demand(job, config)
    assert not repo.demand(job, config)
    repo.stop_import(job, status="queued")
    result = MediaWorker(repo, config, storage, source_factory=Source).run(import_id(repo))
    assert result["status"] == "cancelled"
    assert not storage.objects and Source.calls == 0
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT status FROM media_assets").fetchone()[0] != "ready"
    new = media.create("legacy-owner", content, "new")
    assert new["status"] == "preparing"
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM media_import_jobs").fetchone()[0] == 2


def test_replacement_lease_cannot_share_or_delete_the_old_fetch_directory(setup):
    repo, config, storage, content, Source, media = setup
    media.create("legacy-owner", content, "one")
    key = import_id(repo)
    started = threading.Event()

    class StalledProcess:
        returncode = None

        def __init__(self, command, **kwargs):
            (Path(kwargs["cwd"]) / "old-worker.marker").write_text("old")
            started.set()

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = -15

        kill = terminate

        def wait(self, timeout=None):
            return self.returncode

    with ThreadPoolExecutor(max_workers=1) as executor:
        old = executor.submit(
            MediaWorker(
                repo, config, storage, source_factory=Source, process_factory=StalledProcess
            ).run,
            key,
        )
        assert started.wait(5)
        with get_connection(repo.db_path) as conn:
            old_directory = conn.execute("SELECT work_key FROM media_import_jobs").fetchone()[0]
            conn.execute("UPDATE media_import_jobs SET lease_until=?", (time.time() - 1,))
        replacement = MediaWorker(repo, config, storage, source_factory=Source).run(key)
        assert replacement["status"] == "ready"
        with pytest.raises(JobPermanentFailure):
            old.result(timeout=5)
    with get_connection(repo.db_path) as conn:
        new_directory = conn.execute("SELECT work_key FROM media_import_jobs").fetchone()[0]
        assert conn.execute("SELECT status FROM media_assets").fetchone()[0] == "ready"
    assert old_directory != new_directory
    assert (config.workdir / new_directory / "index.m3u8").exists()


def test_stream_file_is_fenced_to_complete_segments_and_scope(setup):
    repo, config, storage, content, Source, media = setup
    playback = media.create("legacy-owner", content, "one")
    job = repo.claim_import(import_id(repo), config)
    directory = media.workdir(job)
    directory.mkdir(parents=True)
    (directory / "index.m3u8").write_text('#EXTM3U\n#EXT-X-MAP:URI="init.mp4"\nseg_000000.m4s\n')
    (directory / "init.mp4").write_bytes(b"init")
    (directory / "seg_000000.m4s.tmp").write_bytes(b"partial")
    from error_code import APIError

    with pytest.raises(APIError):
        media.stream_file(playback["playbackId"], "legacy-owner", "../metadata.json")
    with pytest.raises(APIError):
        media.stream_file(playback["playbackId"], "legacy-owner", "index.m3u8")
    (directory / "seg_000000.m4s").write_bytes(b"complete")
    assert media.descriptor(playback["playbackId"], "legacy-owner")["status"] == "streaming"
    assert (
        media.stream_file(playback["playbackId"], "legacy-owner", "seg_000000.m4s")[
            "path"
        ].read_bytes()
        == b"complete"
    )
    assert (
        "ENDLIST"
        not in media.stream_file(playback["playbackId"], "legacy-owner", "index.m3u8")["text"]
    )
    generation = media.descriptor(playback["playbackId"], "legacy-owner")["generationKey"]
    manifest = media.stream_file(
        playback["playbackId"], "legacy-owner", "index.m3u8", expected_generation=generation
    )["text"]
    assert "init.mp4?v=" in manifest and "seg_000000.m4s?v=" in manifest
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE media_import_jobs SET fetch_attempts=fetch_attempts+1 WHERE import_id=?",
            (job["import_id"],),
        )
    with pytest.raises(APIError) as mismatch:
        media.stream_file(
            playback["playbackId"], "legacy-owner", "seg_000000.m4s", expected_generation=generation
        )
    assert mismatch.value.status_code == 409


def test_disabled_feed_cancels_queued_domain_job_and_can_restart(setup, monkeypatch):
    repo, config, storage, content, Source, media = setup
    import task_app

    playback = media.create("legacy-owner", content, "one")
    key = import_id(repo)
    monkeypatch.setenv("FEED_ENABLED", "false")
    monkeypatch.setattr(task_app, "get_connection", lambda *_args: get_connection(repo.db_path))
    with pytest.raises(JobPermanentFailure):
        task_app.dispatch(
            {
                "kind": "media_import",
                "payload_json": __import__("json").dumps({"import_id": key}),
                "user_id": "legacy-owner",
            }
        )
    assert media.descriptor(playback["playbackId"], "legacy-owner")["status"] == "cancelled"
    fresh = media.create("legacy-owner", content, "two")
    assert fresh["status"] == "preparing" and import_id(repo) != key


def test_media_execution_budget_interrupts_a_stalled_fetch_without_retry(setup):
    repo, config, storage, content, Source, media = setup
    config = replace(config, import_budget_seconds=0.05, heartbeat_seconds=0.01)
    media.create("legacy-owner", content, "one")

    class StalledProcess:
        returncode = None

        def __init__(self, *_, **__):
            pass

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = -15

        kill = terminate

        def wait(self, timeout=None):
            return self.returncode

    with pytest.raises(JobPermanentFailure):
        MediaWorker(
            repo, config, storage, source_factory=Source, process_factory=StalledProcess
        ).run(import_id(repo))
    assert Source.calls == 1 and not storage.objects
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT status FROM media_assets").fetchone()[0] != "ready"
