from __future__ import annotations

import asyncio
import json
import os
import random
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from database import get_connection
from error_code import APIError
from feed_runtime import admission
from feed_runtime.bili_proxy import ProxyMediaService
from feed_runtime.config import FeedConfig
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from kafka_worker import KafkaConsumerLoop, _Ticket
from ugc.service import UGCService


def test_media_rejects_before_touching_the_database():
    service = ProxyMediaService(SimpleNamespace(), FeedConfig(), None)
    for _ in range(admission.AUTHORIZATION_LIMIT):
        assert admission._SLOTS.acquire(blocking=False)
    try:
        with pytest.raises(APIError) as error:
            service.create("user", "unknown", "key")
        assert error.value.status_code == 503
    finally:
        for _ in range(admission.AUTHORIZATION_LIMIT):
            admission._SLOTS.release()


def test_media_budget_is_reentrant_across_asgi_worker_boundary_and_recovers():
    async def run():
        with admission.media_admission():

            def nested():
                with admission.media_admission():
                    return admission._SLOTS._value

            return await asyncio.to_thread(nested)

    assert asyncio.run(run()) == admission.AUTHORIZATION_LIMIT - 1
    assert admission._SLOTS._value == admission.AUTHORIZATION_LIMIT
    with pytest.raises(RuntimeError), admission.media_admission():
        raise RuntimeError("injected")
    assert admission._SLOTS._value == admission.AUTHORIZATION_LIMIT


def test_concurrent_ugc_reactions_reconcile_after_duplicate_and_out_of_order_delivery(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FEED_ENABLED", "true")
    repo = FeedRepository(os.getenv("CAPACITY_TEST_DATABASE_URL") or tmp_path / "counter.sqlite3")
    posts = UGCService(repo.db_path)
    post = posts.command(
        "legacy-owner",
        "create",
        {"kind": "text", "title": "Public fixture", "visibility": "public"},
        "fixture",
    )
    post = posts.command(
        "legacy-owner",
        "publish",
        {"expectedVersion": post["version"]},
        "publish",
        post["contentId"],
    )
    reactions = ReactionService(repo)

    def actor(i):
        for index, state in enumerate(("like", "dislike", "neutral", "like")):
            reactions.set(f"actor-{i}", post["contentId"], state, f"{i}:{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(actor, range(16)))
    with get_connection(repo.db_path) as conn:
        payloads = [
            json.loads(r[0])
            for r in conn.execute(
                "SELECT payload_json FROM durable_jobs WHERE kind='feed_projection'"
            )
        ]
    replay = payloads * 2
    random.Random(19).shuffle(replay)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(reactions.project, replay))
    with get_connection(repo.db_path) as conn:
        expected = conn.execute(
            "SELECT COUNT(*) FROM content_reactions WHERE content_id=? AND state='like'",
            (post["contentId"],),
        ).fetchone()[0]
        actual = conn.execute(
            "SELECT likes,dislikes FROM content_counters WHERE content_id=?", (post["contentId"],)
        ).fetchone()
        assert actual[0] == expected == 16 and actual[1] == 0
        assert conn.execute("SELECT COUNT(*) FROM feed_inbox WHERE consumer='reaction'").fetchone()[
            0
        ] == len(payloads)


class Consumer:
    def __init__(self):
        self.commits, self.resumes = [], []
        self.fail = False
        self.on_commit = None

    def subscribe(self, *args, **kwargs):
        pass

    def commit(self, *, offsets, asynchronous):
        self.commits.append([(p.partition, p.offset) for p in offsets])
        assert asynchronous is False
        if self.fail:
            raise RuntimeError("commit failed")
        if self.on_commit:
            self.on_commit()

    def resume(self, partitions):
        self.resumes.extend(p.partition for p in partitions)

    def poll(self, timeout):
        return None

    def close(self):
        pass


@pytest.fixture
def worker(tmp_path):
    repo = FeedRepository(tmp_path / "kafka.sqlite3")
    consumer = Consumer()
    bus = SimpleNamespace(db_path=repo.db_path, settings=SimpleNamespace(topic=lambda lane: lane))
    loop = KafkaConsumerLoop(
        bus, group="fixture", lanes=("jobs",), processor=lambda *a: True, consumer=consumer
    )
    loop._heartbeat = lambda: None
    for partition in (0, 1):
        future = Future()
        future.set_result(True)
        loop.epochs[("jobs", partition)] = 1
        loop.inflight[("jobs", partition)] = _Ticket(
            SimpleNamespace(offset=lambda: 7), 1, threading.Event(), future
        )
    yield loop, consumer
    loop.close()


def test_kafka_batches_only_durable_offsets(worker):
    loop, consumer = worker
    loop.inflight[("jobs", 1)].future = Future()
    loop.poll_once()
    assert consumer.commits == [[(0, 8)]]
    assert ("jobs", 1) in loop.inflight and consumer.resumes == [0]


def test_kafka_commit_retry_does_not_rerun_successful_handlers(worker):
    loop, consumer = worker
    consumer.fail = True
    loop.poll_once()
    assert len(loop.inflight) == 2 and not consumer.resumes
    consumer.fail = False
    for ticket in loop.inflight.values():
        ticket.retry_at = 0
    loop.poll_once()
    assert consumer.commits == [[(0, 8), (1, 8)], [(0, 8), (1, 8)]]
    assert not loop.inflight and sorted(consumer.resumes) == [0, 1]


def test_kafka_revocation_during_commit_cannot_resume_old_assignment(worker):
    loop, consumer = worker
    consumer.on_commit = lambda: loop._revoke(
        consumer, [SimpleNamespace(topic="jobs", partition=0)]
    )
    loop.poll_once()
    assert consumer.resumes == [1]


def test_kafka_partial_commit_error_keeps_only_failed_partition_pending(worker):
    loop, consumer = worker
    consumer.commit = lambda **kwargs: [
        SimpleNamespace(topic="jobs", partition=1, error=RuntimeError())
    ]
    loop.poll_once()
    assert consumer.resumes == [0]
    assert set(loop.inflight) == {("jobs", 1)}


@pytest.mark.parametrize("batched", [False, True])
def test_rebuild_excludes_incremental_projection_for_same_ugc_content(
    tmp_path, monkeypatch, batched
):
    import feed_runtime.reactions as module

    monkeypatch.setenv("FEED_ENABLED", "true")
    repo = FeedRepository(os.getenv("CAPACITY_TEST_DATABASE_URL") or tmp_path / "rebuild.sqlite3")
    posts = UGCService(repo.db_path)
    post = posts.command(
        "legacy-owner",
        "create",
        {"kind": "text", "title": "Rebuild guard", "visibility": "public"},
        "rebuild-draft",
    )
    post = posts.command(
        "legacy-owner",
        "publish",
        {"expectedVersion": post["version"]},
        "rebuild-publish",
        post["contentId"],
    )
    service = ReactionService(repo)
    service.set("rebuild-actor", post["contentId"], "like", "rebuild-like")
    held, release, attempted = threading.Event(), threading.Event(), threading.Event()
    original = module.begin_write

    def guarded(conn, *, namespace, key):
        target = namespace == "reaction-projection" and key == "content:" + post["contentId"]
        if target and threading.current_thread().name.startswith("guard-project"):
            attempted.set()
        original(conn, namespace=namespace, key=key)
        if target and threading.current_thread().name.startswith("guard-rebuild"):
            held.set()
            assert release.wait(5)

    monkeypatch.setattr(module, "begin_write", guarded)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="guard-rebuild") as rebuilding:
        rebuild = rebuilding.submit(service.rebuild)
        assert held.wait(5)
        try:
            with ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="guard-project"
            ) as projecting:
                payload = {
                    "event_id": "rebuild-projection",
                    "user_id": "rebuild-actor",
                    "content_id": post["contentId"],
                    "state": "like",
                    "version": 1,
                }
                projection = (
                    projecting.submit(service.project_many, [payload])
                    if batched
                    else projecting.submit(service.project, payload)
                )
                # SQLite blocks at BEGIN IMMEDIATE; MySQL reaches the named lock.
                if os.getenv("CAPACITY_TEST_DATABASE_URL"):
                    assert attempted.wait(5)
                assert not projection.done()
                release.set()
                projection.result(timeout=5)
        finally:
            release.set()
        rebuild.result(timeout=5)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT likes FROM content_counters WHERE content_id=?", (post["contentId"],)
            ).fetchone()[0]
            == 1
        )
