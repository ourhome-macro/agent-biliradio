from __future__ import annotations

import json
from uuid import uuid4

import pytest
from database import get_connection
from error_code import APIError
from feed_runtime.config import FeedConfig
from feed_runtime.product import FeedProductService
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from models import Track


@pytest.fixture
def product(tmp_path, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED", "true")
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(tmp_path / "product.sqlite3")
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "bob"):
            conn.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                (user, "2026-10-07", "2026-10-07"),
            )
    return FeedProductService(repo, FeedConfig())


def admit(product, number, scope="public"):
    return product.repo.admit(
        Track(bvid=f"BV{number:010d}", cid=number + 1, title=f"Live concert {number}", duration=60),
        user_id="alice",
        scope=scope,
    )


def event(
    product, item, *, user="alice", watch=1000, at=1000, playback="play-one", kind="watch_progress"
):
    event_id = str(uuid4())
    with get_connection(product.repo.db_path) as conn:
        conn.execute(
            "INSERT INTO feed_events VALUES (?,?,?,?,?,?,?,?)",
            (
                event_id,
                user,
                item,
                kind,
                event_id,
                json.dumps({"watchMs": watch, "playbackId": playback}),
                at,
                at,
            ),
        )


def test_product_detail_exposes_projection_lag_and_scope(product):
    public, private = admit(product, 1), admit(product, 2, "user:alice")
    assert product.detail("alice", private)["sharePath"] is None
    with pytest.raises(APIError):
        product.detail("bob", private)
    reactions = ReactionService(product.repo)
    result = reactions.set("alice", public, "like", "like-command", expected_version=0)
    pending = product.detail("alice", public)
    assert pending["projectionPending"] and pending["counts"]["likes"] == 0
    assert pending["reaction"] == "like" and result["relationVersion"] == 1
    reactions.project(
        {
            "event_id": f"reaction:alice:{public}:1",
            "user_id": "alice",
            "content_id": public,
            "state": "like",
            "version": 1,
        }
    )
    detail = product.detail("alice", public)
    assert not detail["projectionPending"] and detail["counts"]["likes"] == 1
    reactions.set("alice", public, "dislike", "dislike-command", expected_version=1)
    with pytest.raises(APIError):
        reactions.set("alice", public, "like", "stale-undo", expected_version=1)
    restored = reactions.set("alice", public, "like", "undo-command", expected_version=2)
    assert restored == reactions.set("alice", public, "like", "undo-command", expected_version=2)


def test_explicit_open_creates_valid_event_item_without_exposure(product):
    content = admit(product, 3)
    opened = product.open("alice", content)
    assert not opened["hasMore"] and opened["items"][0]["contentId"] == content
    with get_connection(product.repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_events").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT user_id FROM feed_sessions WHERE session_id=?", (opened["sessionId"],)
            ).fetchone()[0]
            == "alice"
        )
    private = admit(product, 4, "user:alice")
    with pytest.raises(APIError):
        product.open("bob", private)
    product.repo.retire(content, "alice")
    with pytest.raises(APIError):
        product.open("alice", content)


def test_history_counts_cumulative_watch_once_and_scopes_records(product):
    public = admit(product, 5)
    item = product.open("alice", public)["items"][0]["itemId"]
    event(product, item, watch=1000)
    event(product, item, watch=3000, at=1001)
    event(product, item, watch=2000, at=1002)
    event(product, item, watch=4000, at=1003, playback="play-two")
    event(product, item, watch=60000, at=1004, kind="autoplay_blocked")
    history = product.history("alice")
    assert len(history["items"]) == 1 and history["items"][0]["watchMs"] == 7000
    assert product.history("bob")["items"] == []
    product.repo.retire(public, "alice")
    assert product.history("alice")["items"] == []


def test_history_cursor_breaks_timestamp_ties_and_rejects_malformed(product):
    for number in range(6, 9):
        item = product.open("alice", admit(product, number))["items"][0]["itemId"]
        event(product, item, at=2000)
    first = product.history("alice", limit=2)
    second = product.history("alice", limit=2, cursor=first["nextCursor"])
    assert len(first["items"]) == 2 and len(second["items"]) == 1
    assert not {i["contentId"] for i in first["items"]} & {i["contentId"] for i in second["items"]}
    assert second["nextCursor"] is None
    for cursor in ("bad", "!!", "W05hTiwgIngiXQ=="):
        with pytest.raises(APIError):
            product.history("alice", cursor=cursor)
