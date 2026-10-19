"""Authority and execution must use the same explicitly supplied business database."""

import json
import threading
from types import SimpleNamespace

import task_app
from database import get_connection
from durable_jobs import enqueue
from feed_runtime.repository import FeedRepository
from kafka_worker import JobProcessor


def test_kafka_follow_job_executes_against_its_authority_database(tmp_path, monkeypatch):
    from following import service as service_module
    from following import worker as worker_module

    repo = FeedRepository(tmp_path / "authority.sqlite3")
    wrong = tmp_path / "different-default.sqlite3"
    captured = []
    monkeypatch.setattr(task_app, "DEFAULT_DB_PATH", wrong)

    def factory(path):
        captured.append(str(path))
        return SimpleNamespace(db_path=str(path))

    monkeypatch.setattr(service_module, "FollowingService", factory)
    monkeypatch.setattr(worker_module, "execute_follow", lambda *_a, **_kw: {"status": "fixture"})
    with get_connection(repo.db_path) as conn:
        job_id = enqueue(
            conn, kind="creator_follow", user_id="legacy-owner", payload={}, retry_safe=True
        )
        job = dict(conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone())
    envelope = {
        "schemaVersion": "radio.job.v1",
        "jobId": job_id,
        "kind": job["kind"],
        "inputHash": job["input_hash"],
    }
    message = SimpleNamespace(
        value=lambda: json.dumps(envelope).encode(),
        topic=lambda: "fixture.jobs.v1",
        partition=lambda: 0,
        offset=lambda: 0,
    )
    bus = SimpleNamespace(
        db_path=repo.db_path, settings=SimpleNamespace(topic=lambda lane: f"fixture.{lane}.v1")
    )
    assert JobProcessor(bus)(message, threading.Event())
    assert captured == [repo.db_path]
    assert not wrong.exists()
