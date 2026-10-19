from __future__ import annotations

import json

import pytest
from database import get_connection
from error_code import APIError
from feed_runtime.bitmap import bitmap_keys, user_bitmap_id
from feed_runtime.reactions import ReactionService
from test_ugc_lifecycle import make_studio, publish, source_media


@pytest.fixture
def studio(tmp_path, monkeypatch):
    yield from make_studio(tmp_path, monkeypatch)


def test_ugc_reactions_project_bitmap_without_legacy_track_or_alias_writes(studio, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED", "true")
    first = publish(
        studio,
        studio.posts.command(
            "alice", "create", {"title": "First", "visibility": "public"}, "first"
        ),
    )
    second = publish(
        studio,
        studio.posts.command(
            "alice", "create", {"title": "Second", "visibility": "public"}, "second"
        ),
    )
    reactions = ReactionService(studio.repo, studio.feed.cache)
    one = reactions.set("bob", first["id"], "like", "one", expected_version=0)
    assert reactions.set("bob", first["id"], "like", "one", expected_version=0) == one
    reactions.set("bob", first["id"], "dislike", "two", expected_version=1)
    with get_connection(studio.repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM likes").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM content_reactions WHERE content_id=?", (second["id"],)
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_jobs WHERE kind='behavior'").fetchone()[0]
            == 0
        )
        events = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM durable_jobs WHERE kind='feed_projection' ORDER BY id"
            )
        ]
    reactions.project(events[1])
    reactions.project(events[0])
    reactions.project(events[1])
    bitmap_id = user_bitmap_id(studio.repo.db_path, "bob")
    cache = studio.feed.cache
    result = cache.reaction(
        "bob",
        first["id"],
        lambda: pytest.fail("current bitmap must be readable"),
        bitmap_id=bitmap_id,
        expected_version=2,
    )
    assert result == {"state": "dislike", "version": 2}
    keys, offset = bitmap_keys(cache.prefix, first["id"], bitmap_id)
    assert cache.redis.getbit(keys[0], offset) == 0 and cache.redis.getbit(keys[1], offset) == 1
    with get_connection(studio.repo.db_path) as conn:
        row = conn.execute(
            "SELECT likes,dislikes FROM content_counters WHERE content_id=?", (first["id"],)
        ).fetchone()
        assert tuple(row) == (0, 1)
        assert (
            conn.execute("SELECT COUNT(*) FROM feed_inbox WHERE consumer='reaction'").fetchone()[0]
            == 2
        )
    reactions.rebuild()
    assert (
        cache.reaction(
            "bob",
            first["id"],
            lambda: pytest.fail("rebuilt bitmap missing"),
            bitmap_id=bitmap_id,
            expected_version=2,
        )["state"]
        == "dislike"
    )


def test_reaction_does_not_bypass_current_media_dependency_permission(studio):
    source, _ = source_media(studio)
    post = publish(
        studio,
        studio.posts.command(
            "alice",
            "create",
            {
                "kind": "video",
                "title": "Commentary",
                "visibility": "public",
                "mediaSourceContentId": source,
            },
            "post",
        ),
    )
    with get_connection(studio.repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET scope='user:alice',owner_id='alice' WHERE content_id=?",
            (source,),
        )
    assert not studio.policy.resolve_many("bob", [post["id"]], action="read")
    with pytest.raises(APIError):
        ReactionService(studio.repo, studio.feed.cache).set("bob", post["id"], "like", "forbidden")
