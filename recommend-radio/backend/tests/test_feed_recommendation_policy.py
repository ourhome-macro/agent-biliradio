from __future__ import annotations

import time

import pytest
from database import get_connection
from feed_runtime.recommendation import rank_catalog
from feed_runtime.repository import FeedRepository
from models import Track


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED", "false")
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(tmp_path / "feed.sqlite3")
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "bob"):
            conn.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                (user, "2026-10-07", "2026-10-07"),
            )

    def add(index, facets=None, *, scope="public"):
        content_id = repo.admit(
            Track(bvid=f"BV{index:010d}", cid=index + 1, title="Music live", duration=60),
            user_id="alice",
            scope=scope,
            facets=facets or {},
        )
        return next(row for row in repo.list_content("alice") if row["content_id"] == content_id)

    def rank(rows, **kwargs):
        return rank_catalog(
            rows, user_id="alice", session_id="new-session", db_path=repo.db_path, **kwargs
        )

    return repo, add, rank


def test_dedup_uses_explicit_work_identity_not_similar_titles(catalog):
    _, add, rank = catalog
    first = add(1, {"work_id": "canonical-work"})
    duplicate = add(2, {"work_id": "canonical-work"})
    other_performance = add(3)
    rows = rank([first, duplicate, other_performance], mode="new")
    assert [r["content_id"] for r in rows] == [first["content_id"], other_performance["content_id"]]


def test_same_source_recording_is_deduplicated_across_scopes(catalog):
    _, add, rank = catalog
    public, private = add(1), add(1, scope="user:alice")
    assert len(rank([public, private], mode="new")) == 1


def test_live_strong_feedback_changes_unissued_ranking_only_for_its_user(catalog):
    repo, add, rank = catalog
    neutral = [add(index, {"genres": ["pop"]}) for index in range(4)]
    liked = add(10, {"genres": ["rock"]})
    target = add(11, {"genres": ["rock"]})
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO content_reactions VALUES (?,?, 'like',1,?)",
            ("bob", liked["content_id"], time.time()),
        )
    assert rank(neutral + [target], exploration_ratio=0)[-1]["content_id"] == target["content_id"]
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO content_reactions VALUES (?,?, 'like',1,?)",
            ("alice", liked["content_id"], time.time()),
        )
    ranked = rank(neutral + [target], exploration_ratio=0)
    assert ranked[1]["content_id"] == target["content_id"]
    assert ranked[1]["_feed_policy"]["shortTermAffinity"] > 0.9
    assert rank(neutral + [target], mode="new")[-1]["content_id"] == target["content_id"]


def test_short_views_are_not_preferences_and_grounded_watch_is_weaker_than_like(catalog):
    repo, add, rank = catalog
    watched = add(1, {"genres": ["jazz"]})
    target = add(2, {"genres": ["jazz"]})
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO feed_user_content(user_id,content_id,last_watched_at,watch_ms) "
            "VALUES (?,?,?,?)",
            ("alice", watched["content_id"], time.time(), 3000),
        )
    assert rank([target])[0]["_feed_policy"]["shortTermAffinity"] == 0
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE feed_user_content SET watch_ms=45000")
    assert 0.2 < rank([target])[0]["_feed_policy"]["shortTermAffinity"] <= 0.25


def test_exploration_is_deterministic_bounded_and_never_admits_new_candidates(catalog):
    _, add, rank = catalog
    candidates = [add(i) for i in range(15)]
    first = rank(candidates)
    assert first == rank(candidates)
    assert {r["content_id"] for r in first} == {r["content_id"] for r in candidates}
    assert sum(r["_feed_policy"]["exploration"] for r in first[:10]) == 2
    assert not any(r["_feed_policy"]["exploration"] for r in rank(candidates, mode="new"))


def test_aged_feedback_does_not_permanently_lock_ranking(catalog):
    repo, add, rank = catalog
    liked, target = add(1, {"genres": ["rock"]}), add(2, {"genres": ["rock"]})
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO content_reactions VALUES (?,?, 'like',1,?)",
            ("alice", liked["content_id"], time.time() - 8 * 86400),
        )
    assert rank([target])[0]["_feed_policy"]["shortTermAffinity"] == 0
