import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from database import get_connection, init_db
from durable_jobs import enqueue
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from kafka_transport import KafkaSettings
from kafka_worker import JobProcessor
from models import Track
from redis_stream_jobs import RedisJobWorker


@pytest.fixture
def databases(tmp_path, monkeypatch):
    import task_app

    first, other = tmp_path / "first.sqlite3", tmp_path / "default.sqlite3"
    init_db(first)
    init_db(other)
    monkeypatch.setattr(task_app, "DEFAULT_DB_PATH", other)
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    monkeypatch.setenv("AMEM_ENABLED", "false")
    monkeypatch.setenv("FEED_REDIS_URL", "")
    monkeypatch.setenv("MEDIA_WORKDIR", str(tmp_path / "media"))
    monkeypatch.setenv("SSE_GATEWAY_INTERNAL_URL", "")
    with get_connection(other) as connection:
        connection.execute(
            "INSERT INTO settings VALUES ('legacy-owner','default-marker','untouched','now')"
        )
    return first, other, task_app


def test_projection_executes_in_validated_database_not_default(databases, monkeypatch):
    first, other, task_app = databases
    monkeypatch.setenv("FEED_ENABLED", "true")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "fixture")
    monkeypatch.setenv("MINIO_SECRET_KEY", "fixture")
    track = Track(bvid="BVroute000001", cid=1, title="Music live routing", duration=30)
    repo = FeedRepository(first)
    content = repo.admit(track, user_id="legacy-owner")
    assert FeedRepository(other).admit(track, user_id="legacy-owner") == content
    ReactionService(repo).set("legacy-owner", content, "like", "route-command")
    with get_connection(first) as connection:
        job = dict(
            connection.execute("SELECT * FROM durable_jobs WHERE kind='feed_projection'").fetchone()
        )
    task_app.dispatch(job, db_path=first)
    with get_connection(first) as connection:
        assert (
            connection.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (content,)
            ).fetchone()[0]
            == 1
        )
    with get_connection(other) as connection:
        assert (
            connection.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (content,)
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute("SELECT value FROM settings WHERE key='default-marker'").fetchone()[
                0
            ]
            == "untouched"
        )


def make_sse(db, name):
    with get_connection(db) as connection:
        enqueue(
            connection,
            kind="sse",
            user_id="legacy-owner",
            job_id=name,
            retry_safe=True,
            payload={
                "task_id": name,
                "session_id": "same-session",
                "user_id": "legacy-owner",
                "event_type": "progress",
                "payload": {"origin": name},
            },
        )
        return dict(
            connection.execute("SELECT * FROM durable_jobs WHERE job_id=?", (name,)).fetchone()
        )


@pytest.mark.parametrize("transport", ["kafka", "redis"])
def test_default_worker_dispatch_preserves_database_and_parallel_routes(databases, transport):
    first, other, _task_app = databases
    jobs = [make_sse(first, "from-first"), make_sse(other, "from-other")]

    def execute(pair):
        db, job = pair
        if transport == "kafka":
            settings = KafkaSettings("unused.invalid:1", topic_prefix="route")
            bus = SimpleNamespace(db_path=db, settings=settings)
            message = SimpleNamespace(
                value=lambda: json.dumps(
                    {
                        "schemaVersion": "radio.job.v1",
                        "jobId": job["job_id"],
                        "kind": "sse",
                        "inputHash": job["input_hash"],
                    }
                ).encode(),
                topic=lambda: settings.topic("events"),
                partition=lambda: 0,
                offset=lambda: 0,
            )
            assert JobProcessor(bus)(message, threading.Event())
        else:
            bus = SimpleNamespace(db_path=db, acknowledge=lambda *_: True)
            result = RedisJobWorker(bus).process(
                "events",
                "delivery",
                {"job_id": job["job_id"], "input_hash": job["input_hash"]},
                "worker",
            )
            assert result == "completed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(execute, [(first, jobs[0]), (other, jobs[1])]))
    for path, expected in ((first, "from-first"), (other, "from-other")):
        with get_connection(path) as connection:
            rows = connection.execute("SELECT task_id,payload_json FROM dialogue_events").fetchall()
            assert len(rows) == 1 and rows[0]["task_id"] == expected
            assert json.loads(rows[0]["payload_json"])["origin"] == expected


def test_explicit_other_database_does_not_use_global_legacy_sse_gateway(databases, monkeypatch):
    first, _other, task_app = databases
    job = make_sse(first, "local-authority")
    monkeypatch.setenv("SSE_GATEWAY_INTERNAL_URL", "https://unrelated.invalid/publish")
    task_app.dispatch(job, db_path=first)
    with get_connection(first) as connection:
        assert (
            connection.execute("SELECT task_id FROM dialogue_events").fetchone()[0]
            == "local-authority"
        )
