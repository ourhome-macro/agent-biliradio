from __future__ import annotations

import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from database import get_connection
from error_code import APIError
from feed_runtime.config import FeedConfig
from feed_runtime.media import MediaService
from feed_runtime.operations import FeedOperations, distribution
from feed_runtime.operations_api import register_feed_operations
from feed_runtime.product import FeedProductService
from feed_runtime.repository import FeedRepository
from flask import Flask, g, request
from models import Track


@pytest.fixture
def operations(tmp_path, monkeypatch):
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(tmp_path / "operations.sqlite3")
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "admin"):
            conn.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                (user, "2026-10-07", "2026-10-07"),
            )
    config = replace(
        FeedConfig(),
        enabled=True,
        workdir=tmp_path / "media",
        minio_access_key="fixture-key",
        minio_secret_key="fixture-secret",
    )
    storage = SimpleNamespace(presign=lambda key, **_: f"https://objects.invalid/{key}")
    runtime = SimpleNamespace(repo=repo, config=config, storage=storage)
    runtime.media = MediaService(repo, config, storage)
    return FeedOperations(runtime)


def create(ops, number=1, eligible=True):
    content = ops.repo.admit(
        Track(bvid=f"BV{number:010d}", cid=number, title="Music Live", duration=60),
        user_id="alice",
        eligible=eligible,
    )
    playback = ops.runtime.media.create("alice", content, f"play-{number}") if eligible else None
    with get_connection(ops.repo.db_path) as conn:
        job = (
            dict(
                conn.execute(
                    "SELECT * FROM media_import_jobs ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            )
            if eligible
            else None
        )
    return content, playback, job


def test_operations_api_rejects_non_admin_before_services(operations, monkeypatch):
    from feed_runtime import operations_api

    monkeypatch.setattr(operations_api, "FeedServices", lambda **_: operations.runtime)
    app = Flask(__name__)

    @app.before_request
    def identity():
        g.current_user = {"id": "admin", "role": request.headers.get("X-Fixture-Role", "user")}

    @app.errorhandler(APIError)
    def error(value):
        return {"error": value.message}, value.status_code

    register_feed_operations(app)
    client = app.test_client()
    assert client.get("/api/admin/feed/operations").status_code == 403
    assert (
        client.put(
            "/api/admin/feed/content/x/status", json={"status": "retired", "reason": "test"}
        ).status_code
        == 403
    )
    response = client.get("/api/admin/feed/operations", headers={"X-Fixture-Role": "admin"})
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"


def test_retirement_fences_worker_revokes_viewer_and_blocks_explicit_open(operations):
    content, playback, queued = create(operations)
    owned = operations.repo.claim_import(queued["import_id"], operations.config)
    operations.repo.progress(owned, operations.config, source_complete=True)
    operations.set_status(content, "retired", actor_id="admin", reason="operator withdrawal")
    with pytest.raises(RuntimeError, match="MediaLeaseLost"):
        operations.repo.complete_import(owned, {"files": {}, "bytes": 1})
    with pytest.raises(APIError):
        operations.runtime.media.heartbeat(playback["playbackId"], "alice")
    with pytest.raises(APIError):
        FeedProductService(operations.repo, operations.config).open("alice", content)
    with get_connection(operations.repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT status FROM durable_jobs WHERE job_id=?", (queued["job_id"],)
            ).fetchone()[0]
            == "failed"
        )
        assert (
            conn.execute(
                "SELECT status FROM media_import_jobs WHERE import_id=?", (queued["import_id"],)
            ).fetchone()[0]
            == "cancelled"
        )
        assert conn.execute("SELECT COUNT(*) FROM feed_ops_audit").fetchone()[0] == 1


def test_restore_retains_ready_asset_and_repeated_admit_preserves_state(operations):
    content, playback, job = create(operations)
    manifest = {
        "files": {"index.m3u8": {"key": "assets/ready/index.m3u8"}},
        "playlist": "#EXTM3U\n#EXT-X-ENDLIST",
        "bytes": 123,
        "duration": 60,
    }
    with get_connection(operations.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_assets SET status='ready',manifest_json=?,bytes=123 WHERE asset_id=?",
            (json.dumps(manifest), playback["assetId"]),
        )
        conn.execute(
            "UPDATE media_import_jobs SET status='completed' WHERE import_id=?", (job["import_id"],)
        )
    operations.set_status(content, "retired", actor_id="admin", reason="test withdrawal")
    operations.set_status(content, "admitted", actor_id="admin", reason="test restore")
    repeated = operations.runtime.media.create("alice", content, "stored-replay")
    assert repeated["mode"] == "stored"
    with get_connection(operations.repo.db_path) as conn:
        row = conn.execute(
            "SELECT * FROM media_assets WHERE asset_id=?", (playback["assetId"],)
        ).fetchone()
        assert row["status"] == "ready" and row["bytes"] == 123
        assert json.loads(row["manifest_json"]) == manifest
        conn.execute(
            "UPDATE media_assets SET status='unsupported' WHERE asset_id=?", (playback["assetId"],)
        )
    operations.set_status(content, "admitted", actor_id="admin", reason="repeat status command")
    with get_connection(operations.repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT status FROM media_assets WHERE asset_id=?", (playback["assetId"],)
            ).fetchone()[0]
            == "unsupported"
        )


def test_cannot_bypass_music_admission_and_invalid_status_is_api_error(operations):
    content, _, _ = create(operations, eligible=False)
    with pytest.raises(APIError):
        operations.set_status(content, "admitted", actor_id="admin", reason="not music admitted")
    with pytest.raises(APIError):
        operations.set_status(content, [], actor_id="admin", reason="bad shape")


def test_retry_requires_failed_latest_admitted_import(operations):
    content, playback, job = create(operations)
    with pytest.raises(APIError):
        operations.retry(job["import_id"], actor_id="admin", reason="still active")
    with get_connection(operations.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_import_jobs SET status='failed' WHERE import_id=?", (job["import_id"],)
        )
        conn.execute("UPDATE durable_jobs SET status='failed' WHERE job_id=?", (job["job_id"],))
    result = operations.retry(
        job["import_id"], actor_id="admin", reason="upload failure investigated"
    )
    assert result["status"] == "queued"
    with get_connection(operations.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_import_jobs SET status='failed' WHERE import_id=?", (job["import_id"],)
        )
        conn.execute(
            "UPDATE media_assets SET status='ready' WHERE asset_id=?", (playback["assetId"],)
        )
    with pytest.raises(APIError):
        operations.retry(job["import_id"], actor_id="admin", reason="already stored asset")
    operations.set_status(content, "retired", actor_id="admin", reason="content removed")
    with pytest.raises(APIError):
        operations.retry(job["import_id"], actor_id="admin", reason="retired retry")


def test_summary_aggregates_persisted_playback_facts(operations):
    content, playback, _ = create(operations)
    with get_connection(operations.repo.db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(feed_playback_progress)")]
        values = {
            "playback_id": playback["playbackId"],
            "user_id": "alice",
            "content_id": content,
            "watch_ms": 9000,
            "buffering_ms": 1000,
            "startup_ms": 2500,
            "last_event_at": time.time(),
        }
        selected = [key for key in values if key in columns]
        conn.execute(
            f"INSERT INTO feed_playback_progress ({','.join(selected)}) "
            f"VALUES ({','.join('?' for _ in selected)})",
            tuple(values[key] for key in selected),
        )
    summary = operations.summary()
    assert summary["metrics"]["bufferingRatio"] == pytest.approx(0.1)
    assert summary["metrics"]["startupMs"]["cold"]["p95"] == 2500
    assert summary["metrics"]["startupMs"]["stored"]["count"] == 0
    assert distribution([1000, 2000])["p95"] >= 1900


def test_old_failure_cannot_supersede_newer_attempt(operations):
    content, _, old = create(operations)
    with get_connection(operations.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_import_jobs SET status='failed' WHERE import_id=?", (old["import_id"],)
        )
    operations.runtime.media.create("alice", content, "newer-request")
    with get_connection(operations.repo.db_path) as conn:
        conn.execute("UPDATE media_import_jobs SET status='failed'")
    with pytest.raises(APIError):
        operations.retry(old["import_id"], actor_id="admin", reason="historical failure")


def test_retry_after_scratch_cleanup_does_not_reuse_missing_complete_files(operations):
    _, _, job = create(operations)
    with get_connection(operations.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_import_jobs SET status='failed',complete_source=1 WHERE import_id=?",
            (job["import_id"],),
        )
    try:
        operations.retry(job["import_id"], actor_id="admin", reason="scratch expired before retry")
    except APIError:
        return
    with get_connection(operations.repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT complete_source FROM media_import_jobs WHERE import_id=?",
                (job["import_id"],),
            ).fetchone()[0]
            == 0
        )


def test_repeated_error_events_do_not_inflate_failure_rate(operations):
    content, playback, _ = create(operations)
    item = FeedProductService(operations.repo, operations.config).open("alice", content)["items"][0]
    with get_connection(operations.repo.db_path) as conn:
        for index, (kind, playback_id, age) in enumerate(
            [
                ("play_started", playback["playbackId"], 0),
                ("playback_error", playback["playbackId"], 0),
                ("playback_error", playback["playbackId"], 0),
                ("playback_error", "failed-before-start", 0),
                ("swipe_next", playback["playbackId"], 0),
                ("swipe_next", playback["playbackId"], 90000),
            ]
        ):
            at = time.time() - age
            conn.execute(
                "INSERT INTO feed_events VALUES (?,?,?,?,?,?,?,?)",
                (
                    f"event-{index}",
                    "alice",
                    item["itemId"],
                    kind,
                    f"hash-{index}",
                    json.dumps({"playbackId": playback_id}),
                    at,
                    at,
                ),
            )
    metrics = operations.summary()["metrics"]
    assert metrics["playbackErrorRate"] == 1
    assert metrics["startedCount"] == 1
    assert metrics["failedBeforeStartCount"] == 1
    assert metrics["swipeCount"] == 1
