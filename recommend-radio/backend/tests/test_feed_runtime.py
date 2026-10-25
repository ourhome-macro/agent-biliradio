from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import fakeredis
import pytest
from database import get_connection
from error_code import APIError
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.media import MediaService
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from models import Track


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    monkeypatch.setenv("FEED_ENABLED", "false")
    repo = FeedRepository(tmp_path / "feed.sqlite3")
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "bob"):
            conn.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                (user, "2026-10-07", "2026-10-07"),
            )
    config = replace(FeedConfig(), workdir=tmp_path / "work", cancel_grace_seconds=0.01)
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    yield repo, config, cache
    cache.close()


def admit(repo, index=0, *, user="alice", scope="public"):
    return repo.admit(
        Track(bvid=f"BV{index:010d}", cid=index + 1, title=f"Live concert {index}", duration=12),
        user_id=user,
        scope=scope,
    )


def test_fixed_pages_are_idempotent_and_have_no_exposure(setup):
    repo, config, cache = setup
    for index in range(25):
        admit(repo, index)
    service = FeedService(repo, config, cache)
    first = service.create("alice", mode="new", request_key="start")
    assert first == service.create("alice", mode="new", request_key="start")
    second = service.page("alice", first["sessionId"], cursor=first["nextCursor"])
    assert second == service.page("alice", first["sessionId"], cursor=first["nextCursor"])
    assert not {i["contentId"] for i in first["items"]} & {i["contentId"] for i in second["items"]}
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_events").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM recommendation_impressions").fetchone()[0] == 0


def test_warm_page_has_bounded_sql_and_still_revokes_cached_content(setup, monkeypatch):
    import feed_runtime.feed as module

    repo, config, cache = setup
    for index in range(12):
        admit(repo, index)
    service = FeedService(repo, config, cache)
    first = service.create("alice", mode="new", request_key="batch")
    queries = []
    original = module.get_connection

    def traced(*args, **kwargs):
        conn = original(*args, **kwargs)
        conn.set_trace_callback(queries.append)
        return conn

    monkeypatch.setattr(module, "get_connection", traced)
    page = service.page("alice", first["sessionId"])
    assert len(page["items"]) == 10
    assert len([q for q in queries if q.lstrip().upper().startswith("SELECT")]) <= 3
    hidden = page["items"][0]["contentId"]
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET scope='user:bob',owner_id='bob',"
            "permission_version=permission_version+1 WHERE content_id=?",
            (hidden,),
        )
    refreshed = service.page("alice", first["sessionId"])
    item = next(i for i in refreshed["items"] if i["contentId"] == hidden)
    assert item["suppressed"] and "track" not in item


def test_page_generation_is_atomic_across_requests(setup):
    repo, config, cache = setup
    for index in range(30):
        admit(repo, index)
    service = FeedService(repo, config, cache)
    first = service.create("alice", request_key="start")
    with ThreadPoolExecutor(max_workers=5) as executor:
        pages = list(
            executor.map(
                lambda _: service.page("alice", first["sessionId"], cursor=first["nextCursor"]),
                range(5),
            )
        )
    assert all(p == pages[0] for p in pages)
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_pages").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM feed_items").fetchone()[0] == 20


def test_private_scope_cursor_expiry_and_retirement(setup):
    repo, config, cache = setup
    private = admit(repo, scope="user:alice")
    assert not repo.list_content("bob")
    with pytest.raises(APIError):
        repo.content(private, "bob")
    service = FeedService(repo, config, cache)
    page = service.create("alice", request_key="first")
    with pytest.raises(APIError):
        service.page("bob", page["sessionId"])
    with pytest.raises(APIError):
        service.page("alice", page["sessionId"], cursor="arbitrary")
    repo.retire(private, "alice")
    replay = service.page("alice", page["sessionId"])
    assert replay["items"][0]["suppressed"] and "track" not in replay["items"][0]
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE feed_sessions SET expires_at=0")
    with pytest.raises(APIError) as error:
        service.page("alice", page["sessionId"])
    assert error.value.status_code == 410


def test_reactions_are_commands_and_legacy_likes_stay_consistent(setup):
    repo, _, cache = setup
    content = admit(repo)
    service = ReactionService(repo, cache)
    liked = service.set("alice", content, "like", "one")
    assert liked == service.set("alice", content, "like", "one")
    assert service.set("alice", content, "like", "two")["changed"] is False
    with pytest.raises(APIError):
        service.set("alice", content, "neutral", "one")
    disliked = service.set("alice", content, "dislike", "three", expected_version=1)
    assert disliked["relationVersion"] == 2
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM likes WHERE user_id='alice'").fetchone()[0] == 0
        counter = conn.execute("SELECT * FROM content_counters").fetchone()
        assert (counter["likes"], counter["dislikes"]) == (0, 1)
    with pytest.raises(APIError):
        service.set("alice", content, "like", "four", expected_version=1)


def test_projection_handles_duplicate_reordered_and_redis_rebuild(setup, monkeypatch):
    repo, _, cache = setup
    content = admit(repo)
    monkeypatch.setenv("FEED_ENABLED", "true")
    service = ReactionService(repo, cache)
    for version, state in enumerate(["like", "neutral", "like"], 1):
        service.set("alice", content, state, f"cmd-{version}")

    def event(version, state):
        return {
            "event_id": f"event-{version}",
            "user_id": "alice",
            "content_id": content,
            "version": version,
            "state": state,
        }

    service.project(event(3, "like"))
    service.project(event(1, "like"))
    service.project(event(2, "neutral"))
    service.project(event(3, "like"))
    assert cache.counter(content, lambda: {})["likes"] == 1
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_inbox").fetchone()[0] == 3
    cache.redis.flushall()
    assert service.rebuild() == {"contents": 1, "relations": 1}
    assert cache.counter(content, lambda: {})["likes"] == 1
    with pytest.raises(ValueError):
        service.project(event(3, "neutral"))


def test_atomic_redis_versions_do_not_regress(setup):
    _, _, cache = setup
    cache.publish_counter("x", {"likes": 3, "version": 3})
    cache.publish_counter("x", {"likes": 1, "version": 1})
    assert cache.counter("x", lambda: {})["likes"] == 3
    cache.publish_reaction("alice", "x", "neutral", 3, bitmap_id=1)
    cache.publish_reaction("alice", "x", "like", 2, bitmap_id=1)
    assert (
        cache.reaction("alice", "x", lambda: {}, bitmap_id=1, expected_version=3)["state"]
        == "neutral"
    )


def test_cache_coalesces_and_falls_back_when_redis_fails(setup, monkeypatch):
    _, _, cache = setup
    lock = threading.Lock()
    calls = 0

    def load():
        nonlocal calls
        with lock:
            calls += 1
        time.sleep(0.03)
        return {"value": 1}

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: cache.get("hot", load, shared=True), range(8)))
    assert results == [{"value": 1}] * 8 and calls == 1
    monkeypatch.setattr(cache.redis, "get", lambda *_: (_ for _ in ()).throw(ConnectionError()))
    assert cache.get("other", lambda: {"value": 2}) == {"value": 2}


def test_playbacks_share_import_and_cancel_only_after_last_viewer(setup):
    repo, config, _ = setup
    content = admit(repo)
    service = MediaService(repo, config, None)
    first = service.create("alice", content, "a")
    second = service.create("bob", content, "b")
    assert first["assetId"] == second["assetId"]
    with get_connection(repo.db_path) as conn:
        jobs = conn.execute("SELECT * FROM media_import_jobs").fetchall()
        assert len(jobs) == 1
    job = repo.claim_import(jobs[0]["import_id"], config)
    service.release(first["playbackId"], "alice")
    assert repo.demand(job, config)
    service.release(second["playbackId"], "bob")
    assert repo.demand(job, config)
    time.sleep(0.02)
    assert not repo.demand(job, config)
    with pytest.raises(APIError):
        service.descriptor(first["playbackId"], "bob")


def test_source_completion_wins_and_partial_asset_cannot_publish(setup):
    repo, config, _ = setup
    content = admit(repo)
    service = MediaService(repo, config, None)
    playback = service.create("alice", content, "key")
    with get_connection(repo.db_path) as conn:
        import_id = conn.execute("SELECT import_id FROM media_playbacks").fetchone()[0]
    job = repo.claim_import(import_id, config)
    with pytest.raises(RuntimeError, match="IncompleteMedia"):
        repo.complete_import(job, {"bytes": 0})
    repo.progress(job, config, source_complete=True)
    service.release(playback["playbackId"], "alice")
    assert repo.demand(job, config)
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE media_import_jobs SET lease_token='replacement'")
    with pytest.raises(RuntimeError, match="LeaseLost"):
        repo.complete_import(job, {"bytes": 0})


def test_legacy_reactions_update_private_alias_without_touching_other_users(setup):
    repo, _, cache = setup
    from library_service import LibraryService

    track = Track(bvid="BV0000000000", cid=1, title="Music live")
    private = repo.admit(track, user_id="alice", scope="user:alice")
    other = repo.admit(track, user_id="bob", scope="user:bob")
    reactions = ReactionService(repo, cache)
    reactions.set("alice", private, "like", "feed-like")
    library = LibraryService(repo.db_path, user_id="alice")
    assert library.is_liked(track.bvid, track.cid)
    assert library.remove_like(track.bvid, track.cid) == 1
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT state FROM content_reactions WHERE content_id=? AND user_id='alice'",
                (private,),
            ).fetchone()[0]
            == "neutral"
        )
        assert not conn.execute(
            "SELECT 1 FROM content_reactions WHERE content_id=? AND user_id='alice'", (other,)
        ).fetchone()
    library.add_like(track)
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT state FROM content_reactions WHERE content_id=? AND user_id='alice'",
                (private,),
            ).fetchone()[0]
            == "like"
        )


def test_events_are_grounded_and_issued_is_not_impression(setup):
    repo, config, cache = setup
    admit(repo)
    feed = FeedService(repo, config, cache)
    page = feed.create("alice", request_key="x")
    item = page["items"][0]
    event = {
        "eventId": "exposure",
        "itemId": item["itemId"],
        "type": "impression",
        "visibleRatio": 0.8,
        "visibleMs": 1000,
    }
    assert feed.events("alice", [event]) == feed.events("alice", [event])
    with pytest.raises(APIError):
        feed.events("bob", [event])
    media = MediaService(repo, config, None)
    playback = media.create("alice", item["contentId"], "play")
    with pytest.raises(APIError):
        feed.events(
            "alice",
            [
                {
                    "eventId": "fake-completion",
                    "itemId": item["itemId"],
                    "type": "complete",
                    "playbackId": playback["playbackId"],
                    "watchMs": 12000,
                }
            ],
        )
    for event_type in ["swipe_next", "playback_error", "interrupt", "autoplay_blocked"]:
        feed.events(
            "alice", [{"eventId": event_type, "itemId": item["itemId"], "type": event_type}]
        )
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM feed_events WHERE event_type='impression'"
            ).fetchone()[0]
            == 1
        )
        assert conn.execute("SELECT COUNT(*) FROM recommendation_events").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM content_reactions").fetchone()[0] == 0
