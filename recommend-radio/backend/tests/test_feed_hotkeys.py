from __future__ import annotations

import gc
import json
import threading
import time
import weakref
from dataclasses import replace

import fakeredis
import pytest
from feed_runtime.cache import FeedCache
from feed_runtime.hotkeys import HotKeyConfig


class Clock:
    now = 6000.0

    def __call__(self):
        return self.now


@pytest.fixture
def make_cache():
    caches = []

    def make(*, client=None, loader=None, config=None, prefix="test:hot"):
        cache = FeedCache(
            None,
            client=client or fakeredis.FakeRedis(decode_responses=True),
            prefix=prefix,
            hotkey_config=config or HotKeyConfig(enabled=True, promote=10, demote=3),
            metadata_loader=loader
            or (
                lambda identity: (
                    f"{prefix}:content:{identity}:acl:1:public:v1",
                    {"title": "fresh"},
                    1,
                )
            ),
            hotkey_background=False,
        )
        cache.hotkeys.clock = Clock()
        caches.append(cache)
        return cache

    yield make
    for cache in caches:
        cache.close()


def promote(cache, identity="song", n=10):
    for _ in range(n):
        cache.hotkeys.observe(identity)
    cache.hotkeys.flush()


def test_local_hits_count_but_private_pages_and_refresh_do_not(make_cache):
    cache = make_cache()
    key = "content:song:acl:1:public"
    for _ in range(10):
        assert cache.get(key, lambda: {"title": "fresh"}, shared=True, content_id="song")
    for _ in range(20):
        cache.get("page:alice:session:0", lambda: {}, content_id="page")
        cache.get("content:private:acl:1:alice", lambda: {}, content_id="private")
    cache.hotkeys.flush()
    assert cache.redis.zrange(cache.hotkeys.due, 0, -1) == ["song"]
    cache.hotkeys.clock.now += 46
    token = cache.hotkeys.claim("song")
    assert token
    before = cache.redis.hgetall(cache.hotkeys._data("song"))
    cache.hotkeys.refresh_one("song", token)
    after = cache.redis.hgetall(cache.hotkeys._data("song"))
    assert {k: v for k, v in before.items() if k.startswith("b")} == {
        k: v for k, v in after.items() if k.startswith("b")
    }


def test_multiple_api_instances_aggregate_and_only_one_claims(make_cache):
    server = fakeredis.FakeServer()
    a = make_cache(client=fakeredis.FakeRedis(server=server, decode_responses=True))
    b = make_cache(client=fakeredis.FakeRedis(server=server, decode_responses=True))
    promote(a, n=6)
    assert a.redis.zcard(a.hotkeys.due) == 0
    promote(b, n=4)
    assert a.redis.zcard(a.hotkeys.due) == 1
    a.hotkeys.clock.now += 46
    b.hotkeys.clock.now += 46
    assert a.hotkeys.claim("song")
    assert b.hotkeys.claim("song") is None


def test_hysteresis_and_three_separate_cold_windows(make_cache):
    cache = make_cache()
    promote(cache)
    cache.hotkeys.clock.now += 61
    for index in range(3):
        token = cache.hotkeys.claim("song")
        if index < 2:
            assert token
            cache.hotkeys.refresh_one("song", token)
            # Re-evaluating the same window cannot increment the cold count twice.
            cache.redis.zadd(cache.hotkeys.due, {"song": 0})
            second = cache.hotkeys.claim("song")
            assert second
            cache.hotkeys.refresh_one("song", second)
        else:
            assert token is None
        cache.hotkeys.clock.now += 60
    assert cache.redis.zcard(cache.hotkeys.due) == 0


def test_registry_and_local_buffers_have_hard_capacity(make_cache):
    cfg = HotKeyConfig(enabled=True, promote=2, demote=1, capacity=2, tracked=4, batch=2)
    cache = make_cache(config=cfg)
    for index in range(20):
        for _ in range(3):
            cache.hotkeys.observe(f"song{index}")
    assert len(cache.hotkeys.pending) == 4
    cache.hotkeys.flush()
    assert cache.redis.zcard(cache.hotkeys.tracked) == 4
    assert cache.redis.zcard(cache.hotkeys.due) == 2
    status = cache.hotkey_status()
    assert status["enabled"] and status["available"]
    assert status["hotKeys"] == 2 and status["trackedKeys"] == 4


def test_old_owner_cannot_publish_after_lease_is_replaced(make_cache):
    cache = make_cache()
    promote(cache)
    cache.hotkeys.clock.now += 46
    old = cache.hotkeys.claim("song")
    cache.redis.set(cache.hotkeys._lease("song"), "new-owner", ex=10)
    cache.hotkeys.refresh_one("song", old)
    assert cache.redis.get("test:hot:content:song:acl:1:public:v1") is None
    assert cache.redis.get(cache.hotkeys._lease("song")) == "new-owner"


def test_failure_never_extends_old_cache_lifetime(make_cache):
    def broken(_identity):
        raise RuntimeError("injected database failure")

    cache = make_cache(loader=broken)
    key = "test:hot:content:song:acl:1:public:v1"
    cache.redis.set(key, json.dumps({"hard_at": 123, "data": {"title": "old"}}), px=50000)
    prior = cache.redis.pttl(key)
    promote(cache)
    cache.hotkeys.clock.now += 46
    cache.hotkeys.refresh_one("song", cache.hotkeys.claim("song"))
    assert cache.redis.pttl(key) <= prior
    assert json.loads(cache.redis.get(key))["hard_at"] == 123
    assert cache.redis.zscore(cache.hotkeys.due, "song") > cache.hotkeys.clock.now


def test_expired_claim_is_recoverable_after_worker_crash(make_cache):
    cache = make_cache()
    promote(cache)
    cache.hotkeys.clock.now += 46
    old = cache.hotkeys.claim("song")
    cache.redis.delete(cache.hotkeys._lease("song"))  # Simulate Redis lease expiration.
    cache.hotkeys.clock.now += 11
    assert cache.hotkeys.claim("song") not in {None, old}


def test_global_refresh_budget_applies_across_api_processes(make_cache):
    server = fakeredis.FakeServer()
    config = replace(HotKeyConfig(enabled=True, promote=10, demote=3), workers=1)
    a = make_cache(client=fakeredis.FakeRedis(server=server, decode_responses=True), config=config)
    b = make_cache(client=fakeredis.FakeRedis(server=server, decode_responses=True), config=config)
    promote(a, "one")
    promote(a, "two")
    a.hotkeys.clock.now += 46
    b.hotkeys.clock.now += 46
    token = a.hotkeys.claim("one")
    assert token and b.hotkeys.claim("two") is None
    a.hotkeys.refresh_one("one", token)
    assert b.hotkeys.claim("two")


def test_invalidation_fences_inflight_refresh(make_cache):
    cache = make_cache()
    promote(cache)
    cache.hotkeys.clock.now += 46
    old = cache.hotkeys.claim("song")
    cache.invalidate_content("song")
    cache.hotkeys.refresh_one("song", old)
    assert cache.redis.zcard(cache.hotkeys.due) == 0
    assert cache.redis.get("test:hot:content:song:acl:1:public:v1") is None


def test_authoritative_loader_reads_new_versions_and_retires_private_content(make_cache, tmp_path):
    from database import get_connection
    from feed_runtime.repository import FeedRepository
    from feed_runtime.services import public_metadata_loader
    from models import Track

    repo = FeedRepository(tmp_path / "hot.sqlite3")
    identity = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live"), user_id="legacy-owner"
    )
    cache = make_cache(loader=public_metadata_loader(repo.db_path, "test:hot"))
    promote(cache, identity)
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET metadata_json=?,version=version+1 WHERE content_id=?",
            (json.dumps({"title": "new version"}), identity),
        )
    cache.hotkeys.clock.now += 46
    cache.hotkeys.refresh_one(identity, cache.hotkeys.claim(identity))
    keys = list(cache.redis.scan_iter(match="test:hot:content:*"))
    assert len(keys) == 1 and keys[0].endswith(":v2")
    assert json.loads(cache.redis.get(keys[0]))["data"]["title"] == "new version"
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET scope='user:legacy-owner',owner_id='legacy-owner',"
            "permission_version=permission_version+1 WHERE content_id=?",
            (identity,),
        )
    cache.hotkeys.clock.now += 46
    cache.hotkeys.refresh_one(identity, cache.hotkeys.claim(identity))
    assert cache.redis.zcard(cache.hotkeys.due) == 0


def test_redis_outage_does_not_break_local_reads_or_replay_old_demand(make_cache, monkeypatch):
    cache = make_cache()
    cache.get("content:song:acl:1:public", lambda: {"title": "ok"}, shared=True)
    original = cache.redis.eval

    def unavailable(*_args, **_kwargs):
        raise ConnectionError("injected Redis outage")

    monkeypatch.setattr(cache.redis, "eval", unavailable)
    for _ in range(10):
        assert (
            cache.get("content:song:acl:1:public", lambda: {}, shared=True, content_id="song")[
                "title"
            ]
            == "ok"
        )
    with pytest.raises(ConnectionError):
        cache.hotkeys.flush()
    assert not cache.hotkeys.pending
    monkeypatch.setattr(cache.redis, "eval", original)
    assert cache.health()
    cache.hotkeys.flush()
    assert cache.redis.zcard(cache.hotkeys.due) == 0


def test_slow_refresh_cannot_publish_past_execution_deadline(make_cache):
    cfg = replace(HotKeyConfig(enabled=True, promote=10, demote=3), timeout=0.01)

    def slow(_identity):
        time.sleep(0.03)
        return "test:hot:content:song:acl:1:public:v1", {}, 1

    cache = make_cache(config=cfg, loader=slow)
    promote(cache)
    cache.hotkeys.clock.now += 46
    cache.hotkeys.refresh_one("song", cache.hotkeys.claim("song"))
    assert cache.redis.get("test:hot:content:song:acl:1:public:v1") is None


def test_background_worker_refreshes_without_foreground_access(make_cache):
    cache = make_cache()
    promote(cache)
    cache.hotkeys.clock.now += 46
    cache.hotkeys.tick_once()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if cache.redis.get("test:hot:content:song:acl:1:public:v1"):
            break
        threading.Event().wait(0.01)
    assert cache.redis.get("test:hot:content:song:acl:1:public:v1")


def test_evicted_cache_does_not_leave_a_proactive_worker_alive():
    cache = FeedCache(
        None,
        client=fakeredis.FakeRedis(decode_responses=True),
        hotkey_config=HotKeyConfig(enabled=True),
        metadata_loader=lambda _: None,
    )
    manager, reference = cache.hotkeys, weakref.ref(cache)
    del cache
    gc.collect()
    assert reference() is None
    assert manager.stop.is_set() and not manager.thread.is_alive()


@pytest.mark.parametrize(
    "field,value",
    [
        ("demote", 600),
        ("tick", float("nan")),
        ("timeout", 10),
        ("workers", 0),
        ("capacity", 100000),
    ],
)
def test_invalid_limits_rejected(field, value):
    with pytest.raises(ValueError):
        replace(HotKeyConfig(), **{field: value})
