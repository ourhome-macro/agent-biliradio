from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import fakeredis
import pytest
from database import get_connection
from durable_jobs import claim, enqueue
from feed_runtime.cache import FeedCache
from feed_runtime.projection_batch import run_job
from feed_runtime.projection_jobs import ProjectionLeaseLost, claim_peers
from feed_runtime.projection_sql import ProjectionConflict
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from ugc.service import UGCService


@pytest.fixture
def case(tmp_path, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED", "true")
    repo = FeedRepository(
        os.getenv("PROJECTION_TEST_DATABASE_URL") or tmp_path / "projection.sqlite3"
    )
    key = uuid4().hex
    posts = UGCService(repo.db_path)
    post = posts.command(
        "legacy-owner", "create", {"kind": "text", "title": "Batch", "visibility": "public"}, key
    )
    post = posts.command(
        "legacy-owner",
        "publish",
        {"expectedVersion": post["version"]},
        key + "-publish",
        post["contentId"],
    )
    cache = FeedCache(
        None, prefix="batch:" + key, client=fakeredis.FakeRedis(decode_responses=True)
    )
    service = ReactionService(repo, cache)
    for user in range(4):
        for version, state in enumerate(("like", "dislike", "neutral", "like"), 1):
            service.set(key + str(user), post["contentId"], state, f"{key}:{user}:{version}")
    with get_connection(repo.db_path) as conn:
        jobs = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM durable_jobs WHERE kind='feed_projection' "
                "AND projection_key=? ORDER BY id",
                (post["contentId"],),
            )
        ]
    yield repo, service, post["contentId"], jobs
    cache.close()


def values(jobs):
    return [json.loads(j["payload_json"]) for j in jobs]


def test_transition_coalescing_preserves_all_receipts_and_latest_states(case):
    repo, service, cid, jobs = case
    result = service.project_many(values(jobs))
    assert result["events"] == 16 and result["appliedPairs"] == 4 and result["coalesced"] == 12
    assert result["counters"][cid]["likes"] == 4 and result["counters"][cid]["dislikes"] == 0
    assert {r["version"] for r in result["relations"]} == {4}
    replay = service.project_many(list(reversed(values(jobs))))
    assert replay["appliedPairs"] == 0 and replay["newReceipts"] == 0
    assert replay["counters"][cid]["version"] == result["counters"][cid]["version"]
    with get_connection(repo.db_path) as conn:
        receipts = conn.execute(
            "SELECT COUNT(*) FROM feed_inbox WHERE consumer='reaction' "
            "AND event_id IN (" + ",".join("?" for _ in jobs) + ")",
            tuple(v["event_id"] for v in values(jobs)),
        ).fetchone()[0]
        assert receipts == 16


def test_batch_worker_completes_peer_jobs_and_leaves_raw_events_unchanged(case):
    repo, service, cid, jobs = case
    with get_connection(repo.db_path) as conn:
        unrelated = enqueue(
            conn, kind="behavior", user_id="legacy-owner", payload={"event": "fixture"}
        )
        anchor = claim(conn, jobs[0]["job_id"])
    result = run_job(service, anchor)
    assert result["coalesced"] == 12
    with get_connection(repo.db_path) as conn:
        actual = [
            dict(r)
            for r in conn.execute("SELECT * FROM durable_jobs WHERE projection_key=?", (cid,))
        ]
        assert {r["status"] for r in actual} == {"completed"}
        assert {r["job_id"]: r["payload_json"] for r in actual} == {
            r["job_id"]: r["payload_json"] for r in jobs
        }
        assert (
            conn.execute("SELECT status FROM durable_jobs WHERE job_id=?", (unrelated,)).fetchone()[
                0
            ]
            == "queued"
        )


def test_redis_failure_replays_absolute_state_without_recounting(case, monkeypatch):
    repo, service, cid, jobs = case
    original = service.cache.publish_projection_batch

    def partial_failure(counters, relations):
        original(counters, relations[:1])
        raise ConnectionError("injected after partial Redis publication")

    monkeypatch.setattr(
        service.cache,
        "publish_projection_batch",
        partial_failure,
    )
    with get_connection(repo.db_path) as conn:
        anchor = claim(conn, jobs[0]["job_id"])
    with pytest.raises(ConnectionError):
        run_job(service, anchor)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 4
        )
        conn.execute("UPDATE durable_jobs SET available_at=0 WHERE projection_key=?", (cid,))
        anchor = claim(conn, jobs[0]["job_id"])
    monkeypatch.setattr(service.cache, "publish_projection_batch", original)
    result = run_job(service, anchor)
    assert result["newReceipts"] == 0
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 4
        )


def test_inbox_hash_conflict_rolls_back_every_change(case):
    repo, service, cid, jobs = case
    service.project_many(values(jobs))
    changed = values(jobs)
    changed[0]["state"] = "dislike"
    with pytest.raises(ProjectionConflict):
        service.project_many(changed)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 4
        )


def test_lost_anchor_lease_cannot_apply_or_complete_peers(case, monkeypatch):
    from feed_runtime import projection_batch as module

    repo, service, cid, jobs = case
    with get_connection(repo.db_path) as conn:
        anchor = claim(conn, jobs[0]["job_id"])
    original = module.claim_peers

    def steal(*args, **kwargs):
        peers = original(*args, **kwargs)
        with get_connection(repo.db_path) as conn:
            conn.execute(
                "UPDATE durable_jobs SET lease_token='new-owner' WHERE job_id=?",
                (anchor["job_id"],),
            )
        return peers

    monkeypatch.setattr(module, "claim_peers", steal)
    with pytest.raises(ProjectionLeaseLost):
        run_job(service, anchor)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT lease_token FROM durable_jobs WHERE job_id=?", (anchor["job_id"],)
            ).fetchone()[0]
            == "new-owner"
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE projection_key=? AND status='queued'",
                (cid,),
            ).fetchone()[0]
            == 15
        )


def test_projection_claim_cannot_steal_a_live_batch_peer(case):
    repo, service, cid, jobs = case
    with get_connection(repo.db_path) as conn:
        anchor = claim(conn, jobs[0]["job_id"])
    peers = claim_peers(repo.db_path, anchor)
    with get_connection(repo.db_path) as conn:
        assert claim(conn, peers[0]["job_id"]) is None


def test_end_guard_failure_rolls_back_projection_and_inbox(case):
    repo, service, cid, jobs = case
    attempts = []

    def guard(_conn):
        attempts.append(1)
        if len(attempts) == 2:
            raise ProjectionLeaseLost("injected lease deadline")

    with pytest.raises(ProjectionLeaseLost):
        service.project_many(values(jobs), guard=guard)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 0
        )


def test_duplicate_batch_consumers_do_not_double_count(case):
    repo, service, cid, jobs = case
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(service.project_many, [values(jobs), list(reversed(values(jobs)))]))
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (cid,)
            ).fetchone()[0]
            == 4
        )


def test_assignment_loss_after_sql_commit_leaves_replayable_jobs(case, monkeypatch):
    repo, service, cid, jobs = case
    cancelled = threading.Event()
    original = service.cache.publish_projection_batch

    def revoke(*args):
        original(*args)
        cancelled.set()

    monkeypatch.setattr(service.cache, "publish_projection_batch", revoke)
    with get_connection(repo.db_path) as conn:
        anchor = claim(conn, jobs[0]["job_id"])
    with pytest.raises(ProjectionLeaseLost):
        run_job(service, anchor, cancelled=cancelled)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE projection_key=? AND status='completed'",
                (cid,),
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?",
                (cid,),
            ).fetchone()[0]
            == 4
        )
    replay = service.project_many(values(jobs))
    assert replay["newReceipts"] == 0 and replay["appliedPairs"] == 0


def test_poison_peer_is_parked_without_losing_valid_transitions(case):
    repo, service, cid, jobs = case
    # Corrupt an old transition; the highest version remains a valid like.
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE durable_jobs SET input_hash='corrupt' WHERE job_id=?", (jobs[1]["job_id"],)
        )
        anchor = claim(conn, jobs[0]["job_id"])
    result = run_job(service, anchor)
    assert result["events"] == 15
    with get_connection(repo.db_path) as conn:
        statuses = dict(
            conn.execute(
                "SELECT status,COUNT(*) FROM durable_jobs WHERE projection_key=? GROUP BY status",
                (cid,),
            ).fetchall()
        )
        assert statuses == {"completed": 15, "failed": 1}
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?",
                (cid,),
            ).fetchone()[0]
            == 4
        )


def test_ordered_lane_anchor_does_not_pull_unordered_peers(case):
    repo, service, cid, jobs = case
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE durable_jobs SET lane='ordered' WHERE job_id=?", (jobs[-1]["job_id"],))
        anchor = claim(conn, jobs[-1]["job_id"])
    assert run_job(service, anchor)["events"] == 1
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE projection_key=? AND status='queued'",
                (cid,),
            ).fetchone()[0]
            == 15
        )
        anchor = claim(conn, jobs[0]["job_id"])
    assert run_job(service, anchor)["events"] == 15
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes,dislikes FROM content_counters WHERE content_id=?",
                (cid,),
            ).fetchone()[0]
            == 4
        )
