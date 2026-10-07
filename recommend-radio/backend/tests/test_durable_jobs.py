from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from database import get_connection, init_db
from durable_jobs import claim, enqueue, finish, publish_pending, recover_expired
from library_service import LibraryService
from models import Track


@pytest.fixture
def path(tmp_path):
    value = tmp_path / "jobs.sqlite3"
    init_db(value)
    return value


def test_business_and_job_rollback_together(path):
    with pytest.raises(RuntimeError), get_connection(path) as conn:
        conn.execute(
            "INSERT INTO settings(user_id,key,value,updated_at) "
            "VALUES ('legacy-owner','x','1','now')"
        )
        enqueue(conn, kind="behavior", user_id="u", payload={}, job_id="job")
        raise RuntimeError("crash before commit")
    with get_connection(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM settings WHERE key='x'").fetchone()[0] == 0


def test_publish_crash_leaves_retryable_outbox_and_duplicate_delivery_is_safe(path):
    with get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={}, job_id="job", retry_safe=True)
    calls = []

    def publisher(*args):
        calls.append(args)
        if len(calls) == 1:
            raise RuntimeError("confirmed at broker, dispatcher crashed before returning")

    with pytest.raises(RuntimeError):
        publish_pending(path, publisher, now=100)
    assert publish_pending(path, publisher, now=129) == 0
    assert publish_pending(path, publisher, now=131) == 1
    with get_connection(path) as conn:
        job = claim(conn, "job", now=132)
    with get_connection(path) as conn:
        finish(conn, job, result={"accepted": True})
    with get_connection(path) as conn:
        assert claim(conn, "job", now=133) is None
    assert len(calls) == 2


def test_two_workers_claim_once_and_expired_writes_require_reconciliation(path):
    with get_connection(path) as conn:
        enqueue(conn, kind="dialogue", user_id="u", payload={}, job_id="job")

    def acquire():
        with get_connection(path) as conn:
            return claim(conn, "job", now=100, lease_seconds=10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claimed = list(pool.map(lambda _: acquire(), range(2)))
    assert sum(value is not None for value in claimed) == 1
    with get_connection(path) as conn:
        recover_expired(conn, now=111)
    with get_connection(path) as conn:
        assert (
            conn.execute("SELECT status FROM durable_jobs").fetchone()[0] == "needs_reconciliation"
        )


def test_idempotency_conflict_and_capacity(path):
    with get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={"v": 1}, job_id="job")
    with pytest.raises(ValueError), get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={"v": 2}, job_id="job")
    with pytest.raises(ValueError), get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={}, job_id="other", limit=1)


def test_likes_generate_one_atomic_event_per_state_transition(path, monkeypatch):
    monkeypatch.setenv("RABBITMQ_ENABLED", "true")
    library = LibraryService(path)
    track = Track(bvid="BV1234567890", title="song")
    library.add_like(track)
    library.add_like(track)
    library.remove_like(track.bvid)
    with get_connection(path) as conn:
        messages = conn.execute(
            "SELECT payload_json FROM durable_jobs ORDER BY created_at"
        ).fetchall()
    assert [json.loads(row[0])["event"] for row in messages] == ["liked", "unliked"]


def test_same_lane_is_ordered_other_users_are_independent(path):
    with get_connection(path) as conn:
        for job_id, lane in [("one", "u:session"), ("two", "u:session"), ("three", "v:session")]:
            enqueue(conn, kind="dialogue", user_id=lane[0], lane=lane, payload={}, job_id=job_id)
    with get_connection(path) as conn:
        assert claim(conn, "two") is None
    with get_connection(path) as conn:
        assert claim(conn, "one") is not None
    with get_connection(path) as conn:
        assert claim(conn, "three") is not None


def test_outbox_publishes_only_lane_head_until_it_completes(path):
    with get_connection(path) as conn:
        enqueue(conn, kind="sse", user_id="u", lane="sse:u:task", payload={}, job_id="first")
        enqueue(conn, kind="sse", user_id="u", lane="sse:u:task", payload={}, job_id="second")
        enqueue(conn, kind="sse", user_id="v", lane="sse:v:task", payload={}, job_id="other")

    published = []
    assert publish_pending(path, lambda job_id, kind: published.append(job_id), now=100) == 2
    assert published == ["first", "other"]
    assert publish_pending(path, lambda job_id, kind: published.append(job_id), now=101) == 0

    with get_connection(path) as conn:
        first = claim(conn, "first", now=101)
    assert first is not None
    with get_connection(path) as conn:
        finish(conn, first, result={"delivered": True})

    assert publish_pending(path, lambda job_id, kind: published.append(job_id), now=102) == 1
    assert published[-1] == "second"


def test_expired_owner_cannot_overwrite_reclaimed_job(path):
    with get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={}, job_id="j", retry_safe=True)
    with get_connection(path) as conn:
        old = claim(conn, "j", now=10, lease_seconds=1)
    with get_connection(path) as conn:
        recover_expired(conn, now=12)
    with get_connection(path) as conn:
        new = claim(conn, "j", now=13)
    with get_connection(path) as conn:
        finish(conn, old, result="late old result")
    with get_connection(path) as conn:
        row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", ("j",)).fetchone()
        assert row["lease_token"] == new["lease_token"]
        assert row["status"] == "running"


def test_retry_delay_is_not_bypassed_by_broker_duplicates(path, monkeypatch):
    import durable_jobs

    monkeypatch.setattr(durable_jobs.time, "time", lambda: 100.0)
    with get_connection(path) as conn:
        enqueue(conn, kind="behavior", user_id="u", payload={}, job_id="j", retry_safe=True)
    with get_connection(path) as conn:
        job = claim(conn, "j")
    with get_connection(path) as conn:
        finish(conn, job, error=RuntimeError("transient"))
    with get_connection(path) as conn:
        assert claim(conn, "j", now=101) is None
    with get_connection(path) as conn:
        assert claim(conn, "j", now=103) is not None
