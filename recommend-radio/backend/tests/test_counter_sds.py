"""Counter storage contract against an owned Redis, including its actual encoding."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from feed_runtime.cache import FeedCache
from redis_fixture import NativeRedis


@pytest.fixture(scope="module")
def owned_redis(tmp_path_factory):
    server = NativeRedis(tmp_path_factory.mktemp("counter-sds"))
    try:
        yield server
    finally:
        server.close()


@pytest.mark.parametrize("path", ["single", "batch", "refill"])
def test_counter_is_sds_string_and_replay_cannot_regress_or_increment(owned_redis, path):
    cache = FeedCache(owned_redis.url, prefix="counter-sds:" + uuid4().hex)
    cid = "storage-contract-content"
    key = f"{cache.prefix}:counter:{cid}"
    current = {
        "content_id": cid,
        "likes": 7,
        "dislikes": 2,
        "version": 12,
        "updated_at": 1792930000.0,
    }

    def publish(value):
        if path == "batch":
            cache.publish_projection_batch({cid: value}, [])
        elif path == "refill":
            cache.counter(cid, lambda: value, min_version=value["version"])
        else:
            cache.publish_counter(cid, value)

    def assert_stored():
        assert cache.redis.type(key) == "string"
        # A JSON envelope must retain SDS, including Redis's compact SDS form;
        # a bare numeric String can instead use the integer optimization.
        assert cache.redis.object("ENCODING", key) in {"raw", "embstr"}
        assert json.loads(cache.redis.get(key)) == current
        assert 0 < cache.redis.ttl(key) <= 300

    try:
        publish(current)
        assert_stored()
        publish({**current, "likes": 1, "dislikes": 0, "version": 11})
        publish(current)
        assert_stored()
        assert cache.counter(cid, lambda: pytest.fail("Expected Redis hit")) == current
        # Expiry/eviction recovery uses the same String/SDS publication path.
        cache.redis.delete(key)
        assert cache.counter(cid, lambda: current, min_version=12) == current
        assert_stored()
    finally:
        cache.redis.delete(key)
        cache.close()
