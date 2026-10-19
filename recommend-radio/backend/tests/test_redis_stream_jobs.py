from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pytest
from database import get_connection
from durable_jobs import enqueue, publish_pending
from feed_runtime.repository import FeedRepository
from isolated_job import ProcessTree
from redis_fixture import NativeRedis
from redis_stream_jobs import RedisJobBus, RedisJobWorker


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("JOB_TRANSPORT", "redis_stream")
    server = NativeRedis(tmp_path / "redis")
    repo = FeedRepository(tmp_path / "jobs.sqlite3")
    bus = RedisJobBus(repo.db_path, server.url)
    bus.ensure_groups(("jobs", "events", "media"))
    yield repo, bus, server
    server.close()


def add(repo, kind="feed_projection"):
    with get_connection(repo.db_path) as conn:
        return enqueue(
            conn, kind=kind, user_id="legacy-owner", payload={"value": 1}, retry_safe=True
        )


def read(bus, consumer="one", lane="events"):
    return bus.read((lane,), consumer, block_ms=10)[0]


def test_atomic_publish_duplicate_execution_and_ack(setup):
    repo, bus, _ = setup
    key = add(repo)
    assert bus.publish(key, "feed_projection") == bus.publish(key, "feed_projection")
    delivery = read(bus)
    called = []
    worker = RedisJobWorker(bus, dispatcher=lambda job: called.append(job["job_id"]))
    assert worker.process(*delivery, "one") == "completed"
    assert called == [key] and bus.redis.xlen(bus.key("events")) == 0
    bus.publish(key, "feed_projection")
    assert worker.process(*read(bus), "one") == "deferred_or_terminal" and called == [key]


def test_pending_recovery_after_sql_commit_does_not_execute_again(setup):
    repo, bus, _ = setup
    key = add(repo)
    bus.publish(key, "feed_projection")
    delivery = read(bus, "dead")
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE durable_jobs SET status='completed' WHERE job_id=?", (key,))
    recovered = bus.recover("events", "new", idle_ms=0)[1][0]
    worker = RedisJobWorker(bus, dispatcher=lambda _: pytest.fail("committed job must not rerun"))
    worker.process("events", *recovered, "new")
    assert bus.redis.xpending(bus.key("events"), bus.group)["pending"] == 0
    assert not bus.acknowledge("events", delivery[1], "dead", key)


def test_expired_worker_recovery_retries_with_fenced_sql_identity(setup):
    repo, bus, _ = setup
    key = add(repo)
    bus.publish(key, "feed_projection")
    read(bus, "dead")
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE durable_jobs SET status='running',lease_until=0,lease_token='dead',attempts=1 "
            "WHERE job_id=?",
            (key,),
        )
    _, items, *_ = bus.recover("events", "new", idle_ms=0)
    worker = RedisJobWorker(bus, dispatcher=lambda job: {"token": job["lease_token"]})
    assert worker.process("events", *items[0], "new") == "completed"
    with get_connection(repo.db_path) as conn:
        row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (key,)).fetchone()
        assert row["attempts"] == 2 and json.loads(row["result_json"])["token"] != "dead"


def test_failed_delivery_is_committed_before_ack_and_republished(setup):
    repo, bus, _ = setup
    key = add(repo)
    bus.publish(key, "feed_projection")
    worker = RedisJobWorker(bus, dispatcher=lambda _: (_ for _ in ()).throw(ConnectionError()))
    assert worker.process(*read(bus), "one") == "queued"
    assert bus.redis.xpending(bus.key("events"), bus.group)["pending"] == 0
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE durable_jobs SET available_at=0,next_publish_at=0")
    assert publish_pending(repo.db_path, bus.publish) == 1
    assert RedisJobWorker(bus, dispatcher=lambda _: "ok").process(*read(bus), "one") == "completed"


def test_active_sql_lease_is_not_acknowledged_by_pending_reclaimer(setup):
    repo, bus, _ = setup
    key = add(repo)
    bus.publish(key, "feed_projection")
    first = read(bus, "active")
    from durable_jobs import claim

    with get_connection(repo.db_path) as conn:
        claim(conn, key)
    recovered = bus.recover("events", "other", idle_ms=0)[1][0]
    RedisJobWorker(bus, dispatcher=lambda _: pytest.fail("active job must not rerun")).process(
        "events", *recovered, "other"
    )
    assert bus.redis.xpending(bus.key("events"), bus.group)["pending"] == 1
    assert not bus.acknowledge("events", first[1], "active", key)


def test_process_tree_containment_terminates_descendants(tmp_path):
    marker = tmp_path / "descendant.txt"
    script = (
        "import subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "open(sys.argv[1],'w').write(str(p.pid)); time.sleep(60)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(marker)],
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
    tree = ProcessTree(process)
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert marker.exists()
        pid = int(marker.read_text())
        tree.terminate()
        process.wait(timeout=5)
        deadline = time.monotonic() + 5

        def alive():
            if os.name == "nt":
                import ctypes
                from ctypes import wintypes

                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
                kernel.OpenProcess.restype = wintypes.HANDLE
                kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
                kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                handle = kernel.OpenProcess(0x100000, False, pid)
                if not handle:
                    return False
                try:
                    return kernel.WaitForSingleObject(handle, 0) != 0
                finally:
                    kernel.CloseHandle(handle)
            from pathlib import Path

            status = Path(f"/proc/{pid}/stat")
            return status.exists() and status.read_text().split(") ")[1][0] != "Z"

        while alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not alive()
    finally:
        tree.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
