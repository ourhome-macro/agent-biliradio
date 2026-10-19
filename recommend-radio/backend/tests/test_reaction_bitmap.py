from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import fakeredis
import pytest
from database import get_connection
from feed_runtime.bitmap import MAX_INTEGER, READ, SHARD_BITS, bitmap_keys, user_bitmap_id
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from models import Track


@pytest.fixture
def cache():
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    yield cache
    cache.close()


def no_load():
    raise AssertionError("Expected a bitmap hit, not a database fallback")


def test_boundaries_use_small_independent_shards(cache):
    for user in [1, SHARD_BITS, SHARD_BITS+1, MAX_INTEGER]:
        cache.publish_reaction(str(user), "content", "like", 1, bitmap_id=user)
        keys, offset = bitmap_keys(cache.prefix, "content", user)
        assert 0 <= offset < SHARD_BITS
        assert all(cache.redis.strlen(key) <= SHARD_BITS//8 for key in keys[:3])
        assert cache.reaction(str(user), "content", no_load, bitmap_id=user,
                              expected_version=1)["state"] == "like"
    first, _ = bitmap_keys(cache.prefix, "content", 1)
    second, _ = bitmap_keys(cache.prefix, "content", SHARD_BITS+1)
    assert first != second
    tags = {key.split("{",1)[1].split("}",1)[0] for key in first}
    assert len(tags) == 1  # All Lua keys share one Redis Cluster slot.


def test_duplicate_reordered_and_mutually_exclusive_states(cache):
    assert cache.publish_reaction("a", "c", "dislike", 3, bitmap_id=1)
    assert not cache.publish_reaction("a", "c", "like", 1, bitmap_id=1)
    assert cache.publish_reaction("a", "c", "dislike", 3, bitmap_id=1)
    keys, offset = bitmap_keys(cache.prefix, "c", 1)
    assert [cache.redis.getbit(key, offset) for key in keys[:3]] == [0, 1, 1]
    cache.publish_reaction("a", "c", "neutral", 4, bitmap_id=1)
    assert [cache.redis.getbit(key, offset) for key in keys[:3]] == [0, 0, 1]
    with pytest.raises(Exception, match="ReactionVersionConflict"):
        cache.publish_reaction("a", "c", "like", 4, bitmap_id=1)


def test_bigint_versions_do_not_round_through_lua_doubles(cache):
    cache.publish_reaction("a", "c", "like", 2**53+1, bitmap_id=1)
    assert not cache.publish_reaction("a", "c", "neutral", 2**53, bitmap_id=1)
    cache.publish_reaction("a", "c", "dislike", MAX_INTEGER, bitmap_id=1)
    assert not cache.publish_reaction("a", "c", "like", MAX_INTEGER-1, bitmap_id=1)
    assert cache.reaction("a", "c", no_load, bitmap_id=1,
                          expected_version=MAX_INTEGER)["state"] == "dislike"


@pytest.mark.parametrize("missing", range(4))
def test_partial_eviction_is_unknown_and_rehydrates(cache, missing):
    cache.publish_reaction("a", "c", "like", 1, bitmap_id=1)
    cache.publish_reaction("b", "c", "dislike", 1, bitmap_id=2)
    keys, _ = bitmap_keys(cache.prefix, "c", 1)
    cache.redis.delete(keys[missing])
    assert not cache.redis.eval(READ, 4, *keys, 0)
    assert cache.reaction("a", "c", lambda: {"state":"like","version":1},
                          bitmap_id=1, expected_version=1)["state"] == "like"
    # Repaired shard cannot misrepresent the other user as neutral.
    assert cache.reaction("b", "c", lambda: {"state":"dislike","version":1},
                          bitmap_id=2, expected_version=1)["state"] == "dislike"
    assert cache.reaction("a", "c", no_load, bitmap_id=1, expected_version=1)["state"] == "like"


def test_partial_write_or_corruption_does_not_pass_read(cache):
    cache.publish_reaction("a", "c", "like", 1, bitmap_id=1)
    keys, _ = bitmap_keys(cache.prefix, "c", 1)
    cache.redis.hset(keys[3], "0", "2:dislike")  # Metadata wrote, bit updates failed.
    assert not cache.redis.eval(READ, 4, *keys, 0)
    assert cache.reaction("a", "c", lambda: {"state":"dislike","version":2},
                          bitmap_id=1, expected_version=2)["state"] == "dislike"
    assert cache.reaction("a", "c", no_load, bitmap_id=1, expected_version=2)["state"] == "dislike"


def test_sql_version_is_required_even_for_cached_newer_state(cache):
    cache.publish_reaction("a", "c", "dislike", 3, bitmap_id=1)
    result = cache.reaction("a", "c", lambda: {"state":"like","version":2},
                            bitmap_id=1, expected_version=2)
    assert result == {"state":"like","version":2}
    assert cache.reaction("a", "c", no_load, bitmap_id=1, expected_version=3)["state"] == "dislike"


def test_mapping_is_persistent_and_allocated_once_concurrently(tmp_path):
    repo = FeedRepository(tmp_path/"database.sqlite3")
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(lambda _: user_bitmap_id(repo.db_path,"opaque-user-uuid",create=True),range(20)))
    assert len(set(ids)) == 1
    assert user_bitmap_id(repo.db_path,"opaque-user-uuid") == ids[0]
    assert user_bitmap_id(repo.db_path,"unknown-user") is None
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM reaction_bitmap_users WHERE user_id=?",
                            ("opaque-user-uuid",)).fetchone()[0] == 1


def test_projection_retry_rebuild_and_feed_read_use_bitmaps(tmp_path, cache, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED","true")
    monkeypatch.setenv("JOB_TRANSPORT","disabled")
    repo = FeedRepository(tmp_path/"database.sqlite3")
    content = repo.admit(Track(bvid="BVbitmaptest",cid=1,title="Music live"),user_id="legacy-owner")
    service = ReactionService(repo,cache)
    service.set("legacy-owner",content,"like","one")
    payload={"event_id":"bitmap-one","user_id":"legacy-owner","content_id":content,"state":"like","version":1}
    publish=cache.publish_reaction
    monkeypatch.setattr(cache,"publish_reaction",lambda *a,**k: (_ for _ in ()).throw(ConnectionError()))
    with pytest.raises(ConnectionError):
        service.project(payload)
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT likes FROM content_counters WHERE content_id=?",(content,)).fetchone()[0] == 1
    monkeypatch.setattr(cache,"publish_reaction",publish)
    service.project(payload)  # Redis failure must not recount a committed Inbox event.
    bid=user_bitmap_id(repo.db_path,"legacy-owner")
    assert cache.reaction("legacy-owner",content,no_load,bitmap_id=bid,expected_version=1)["state"] == "like"
    cache.redis.flushall()
    service.rebuild()
    assert cache.reaction("legacy-owner",content,no_load,bitmap_id=bid,expected_version=1)["state"] == "like"
    feed=FeedService(repo,replace(FeedConfig(),workdir=tmp_path/"work"),cache)
    page=feed.create("legacy-owner",request_key="page")
    assert page["items"][0]["reaction"] == "like"
    # Newer Redis state cannot bypass current SQL dislike/privacy authorization.
    cache.publish_reaction("legacy-owner",content,"neutral",99,bitmap_id=bid)
    service.set("legacy-owner",content,"dislike","two")
    assert feed.page("legacy-owner",page["sessionId"])["items"][0]["suppressed"]


def test_invalid_offsets_and_versions_never_allocate_huge_strings(cache):
    for bitmap_id in (0,-1,MAX_INTEGER+1,True):
        with pytest.raises(ValueError):
            cache.publish_reaction("a","c","like",1,bitmap_id=bitmap_id)
    for version in (-1,MAX_INTEGER+1,True):
        with pytest.raises(ValueError):
            cache.publish_reaction("a","c","like",version,bitmap_id=1)
    with pytest.raises(ValueError):
        cache.publish_reaction("a","c","like",0,bitmap_id=1)
    assert cache.redis.dbsize() == 0
