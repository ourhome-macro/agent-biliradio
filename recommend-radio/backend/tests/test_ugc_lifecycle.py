from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import fakeredis
import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from database import get_connection
from error_code import APIError
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.media import MediaService
from feed_runtime.product import FeedProductService
from feed_runtime.repository import FeedRepository
from models import Track
from resource_policy import ResourcePolicy
from ugc.service import UGCService


def prepare_isolated_pending_schema(path):
    """Exercise pending 009 DDL only in this temporary test database."""
    engine = sa.create_engine(sa.URL.create("sqlite", database=str(path)))
    try:
        with engine.begin() as conn:
            if "resource_kind" in {
                col["name"] for col in sa.inspect(conn).get_columns("feed_content")
            }:
                return
            operation = Operations(MigrationContext.configure(conn))
            with operation.batch_alter_table("feed_content") as batch:
                batch.alter_column("track_id", existing_type=sa.Text(), nullable=True)
                batch.add_column(
                    sa.Column("resource_kind", sa.Text(), nullable=False, server_default="video")
                )
                batch.add_column(
                    sa.Column(
                        "permission_version", sa.Integer(), nullable=False, server_default="1"
                    )
                )
            from domain_event_schema import STATEMENTS as domain_statements
            from ugc.schema import STATEMENTS

            for statement in (*domain_statements, *STATEMENTS):
                conn.exec_driver_sql(statement)
    finally:
        engine.dispose()


def make_studio(tmp_path, monkeypatch, *, db_path=None):
    monkeypatch.setenv("FEED_ENABLED", "false")
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(db_path or tmp_path / "ugc.sqlite3")
    if not db_path:
        prepare_isolated_pending_schema(repo.db_path)
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "bob"):
            conn.execute(
                "INSERT INTO app_users(id,display_name,created_at,updated_at) VALUES (?,?,?,?)",
                (user, user, "2026-10-08", "2026-10-08"),
            )
    config = replace(FeedConfig(), workdir=tmp_path / "media")
    storage = SimpleNamespace(presign=lambda key, **_: "https://fixture.invalid/" + key)
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    yield SimpleNamespace(
        repo=repo,
        posts=UGCService(repo.db_path),
        policy=ResourcePolicy(repo.db_path),
        media=MediaService(repo, config, storage),
        feed=FeedService(repo, config, cache),
        product=FeedProductService(repo, config),
    )
    cache.close()


@pytest.fixture
def studio(tmp_path, monkeypatch):
    yield from make_studio(tmp_path, monkeypatch)


def source_media(studio, private=False):
    content = studio.repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live concert", duration=12),
        user_id="alice",
        scope="user:alice" if private else "public",
    )
    descriptor = studio.media.create(
        "alice", content, "source-private" if private else "source-public"
    )
    with get_connection(studio.repo.db_path) as conn:
        conn.execute(
            "UPDATE media_assets SET status='ready',manifest_json=?,bytes=100 WHERE asset_id=?",
            (
                json.dumps({"duration": 12, "files": {"init.mp4": {"key": "assets/a/v/init.mp4"}}}),
                descriptor["assetId"],
            ),
        )
    return content, descriptor["assetId"]


def publish(studio, post):
    return studio.posts.command(
        "alice",
        "publish",
        {"expectedVersion": post["version"]},
        "publish:" + post["id"],
        post["id"],
    )


def test_text_draft_publish_edit_cas_outbox_and_no_fake_bvid(studio):
    payload = {
        "kind": "text",
        "title": "Music notes",
        "body": "A listening diary",
        "visibility": "public",
    }
    draft = studio.posts.command("alice", "create", payload, "create")
    assert draft == studio.posts.command("alice", "create", payload, "create")
    assert studio.posts.detail("alice", draft["id"])["status"] == "draft"
    assert not studio.policy.materialize("alice", [draft["id"]])
    with pytest.raises(APIError):
        studio.posts.detail("bob", draft["id"])
    public = publish(studio, draft)
    assert studio.posts.detail("bob", public["id"])["body"] == payload["body"]
    assert studio.policy.materialize("bob", [public["id"]])[0]["kind"] == "text"
    with pytest.raises(APIError):
        studio.media.create("alice", public["id"], "not-video")
    assert public["id"] not in {row["content_id"] for row in studio.repo.list_content("bob")}
    with pytest.raises(APIError):
        studio.posts.command(
            "alice", "edit", {"expectedVersion": 1, "body": "stale"}, "stale", public["id"]
        )
    with get_connection(studio.repo.db_path) as conn:
        row = conn.execute(
            "SELECT track_id,metadata_json FROM feed_content WHERE content_id=?", (public["id"],)
        ).fetchone()
        assert row[0] is None and "bvid" not in json.loads(row[1])
        assert conn.execute("SELECT COUNT(*) FROM domain_event_outbox").fetchone()[0] == 2


def test_public_to_private_revokes_old_feed_detail_history_share_and_playback(studio):
    source, asset = source_media(studio)
    draft = studio.posts.command(
        "alice",
        "create",
        {
            "kind": "video",
            "title": "My concert commentary",
            "body": "Public commentary",
            "visibility": "public",
            "mediaSourceContentId": source,
        },
        "video",
    )
    public = publish(studio, draft)
    page = studio.feed.create("bob", mode="new", request_key="feed")
    item = next(row for row in page["items"] if row["contentId"] == public["id"])
    viewer = studio.media.create("bob", public["id"], "watch-post")
    original = studio.media.create("bob", source, "watch-source")
    assert viewer["assetId"] == asset and viewer["mode"] == "stored"
    studio.feed.events(
        "bob",
        [
            {
                "eventId": "watch",
                "itemId": item["itemId"],
                "type": "watch_progress",
                "playbackId": viewer["playbackId"],
                "watchMs": 1000,
            }
        ],
    )
    assert studio.product.history("bob")["items"]
    private = studio.posts.command(
        "alice",
        "edit",
        {"expectedVersion": public["version"], "visibility": "private"},
        "hide",
        public["id"],
    )
    assert (
        private["id"] == public["id"] and private["permissionVersion"] > public["permissionVersion"]
    )
    assert not studio.policy.materialize("bob", [public["id"]])
    assert studio.posts.detail("alice", public["id"])["sharePath"] is None
    assert studio.product.history("bob")["items"] == []
    replay = studio.feed.page("bob", page["sessionId"])
    assert next(row for row in replay["items"] if row["contentId"] == public["id"])["suppressed"]
    for call in (
        lambda: studio.product.detail("bob", public["id"]),
        lambda: studio.media.descriptor(viewer["playbackId"], "bob"),
        lambda: studio.media.create("bob", public["id"], "old-link"),
    ):
        with pytest.raises(APIError):
            call()
    assert studio.media.descriptor(original["playbackId"], "bob")["status"] == "ready"
    with get_connection(studio.repo.db_path) as conn:
        assert (
            conn.execute("SELECT status FROM media_assets WHERE asset_id=?", (asset,)).fetchone()[0]
            == "ready"
        )


def test_private_media_cannot_be_published_and_nonowner_cannot_mutate(studio):
    source, _ = source_media(studio, private=True)
    draft = studio.posts.command(
        "alice",
        "create",
        {
            "kind": "video",
            "title": "Private video",
            "visibility": "public",
            "mediaSourceContentId": source,
        },
        "private-source",
    )
    with pytest.raises(APIError):
        publish(studio, draft)
    with pytest.raises(APIError):
        studio.posts.command(
            "bob", "edit", {"expectedVersion": 1, "title": "hijack"}, "hijack", draft["id"]
        )
    with pytest.raises(APIError):
        studio.posts.command(
            "alice",
            "create",
            {"kind": "video", "title": "url", "mediaUrl": "http://attacker.invalid"},
            "url",
        )


def test_delete_is_tombstone_and_failed_outbox_rolls_back(studio, monkeypatch):
    post = publish(
        studio,
        studio.posts.command(
            "alice", "create", {"title": "Delete me", "visibility": "public"}, "new"
        ),
    )
    import ugc.service as module

    original = module.enqueue_event
    monkeypatch.setattr(
        module,
        "enqueue_event",
        lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("outbox failure")),
    )
    with pytest.raises(RuntimeError):
        studio.posts.command(
            "alice",
            "edit",
            {"expectedVersion": post["version"], "title": "uncommitted"},
            "fail",
            post["id"],
        )
    assert studio.posts.detail("alice", post["id"])["title"] == "Delete me"
    monkeypatch.setattr(module, "enqueue_event", original)
    deleted = studio.posts.command(
        "alice", "delete", {"expectedVersion": post["version"]}, "delete", post["id"]
    )
    assert deleted["status"] == "deleted"
    assert (
        studio.posts.command(
            "alice", "delete", {"expectedVersion": post["version"]}, "delete", post["id"]
        )
        == deleted
    )
    assert not studio.policy.resolve_many("alice", [post["id"]])
    assert studio.posts.own_posts("alice")["items"] == []


def test_concurrent_cas_edit_has_one_winner_and_batch_scope_is_current(studio):
    private = studio.posts.command(
        "alice", "create", {"title": "Private text", "visibility": "private"}, "private"
    )
    private = publish(studio, private)
    public = publish(
        studio,
        studio.posts.command(
            "alice", "create", {"title": "Public text", "visibility": "public"}, "public"
        ),
    )

    def edit(number):
        try:
            return studio.posts.command(
                "alice",
                "edit",
                {"expectedVersion": public["version"], "body": f"revision {number}"},
                f"concurrent-{number}",
                public["id"],
            )["version"]
        except APIError as error:
            assert error.status_code == 409
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(edit, (1, 2)))
    assert results.count("conflict") == 1
    assert set(
        studio.policy.resolve_many("bob", [private["id"], public["id"]], action="search")
    ) == {public["id"]}
    assert set(
        studio.policy.resolve_many("alice", [private["id"], public["id"]], action="search")
    ) == {private["id"], public["id"]}
    with get_connection(studio.repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM domain_event_outbox").fetchone()[0] == 5


def test_source_revocation_blocks_media_but_owner_can_still_restrict_post(studio):
    source, asset = source_media(studio)
    post = publish(
        studio,
        studio.posts.command(
            "alice",
            "create",
            {
                "kind": "video",
                "title": "My video",
                "visibility": "public",
                "mediaSourceContentId": source,
            },
            "reference",
        ),
    )
    with get_connection(studio.repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_content SET scope='user:bob',owner_id='bob' WHERE content_id=?", (source,)
        )
    assert not studio.policy.resolve_many("alice", [post["id"]], action="play")
    hidden = studio.posts.command(
        "alice",
        "edit",
        {"expectedVersion": post["version"], "visibility": "private"},
        "restrict-revoked",
        post["id"],
    )
    assert hidden["visibility"] == "private" and hidden["assetId"] == asset
    assert not studio.policy.resolve_many("alice", [post["id"]], action="play")
    assert studio.posts.detail("alice", post["id"])["canEdit"]


def test_admin_post_retirement_never_retires_shared_asset_or_allows_owner_bypass(studio):
    from feed_runtime.operations import FeedOperations

    operations = FeedOperations(
        SimpleNamespace(repo=studio.repo, config=studio.media.config, media=studio.media)
    )
    source, asset = source_media(studio)
    posts = [
        publish(
            studio,
            studio.posts.command(
                "alice",
                "create",
                {
                    "kind": "video",
                    "title": f"Commentary {index}",
                    "visibility": "public",
                    "mediaSourceContentId": source,
                },
                f"commentary-{index}",
            ),
        )
        for index in (1, 2)
    ]
    original = studio.media.create("bob", source, "source-viewer")
    other = studio.media.create("bob", posts[1]["id"], "other-post-viewer")
    result = operations.set_status(posts[0]["id"], "retired", actor_id="admin", reason="moderation")
    assert result["affectedContents"] == 1
    assert studio.media.descriptor(original["playbackId"], "bob")["status"] == "ready"
    assert studio.media.descriptor(other["playbackId"], "bob")["status"] == "ready"
    with pytest.raises(APIError) as caught:
        studio.posts.command(
            "alice",
            "edit",
            {"expectedVersion": 3, "title": "restore myself"},
            "bypass",
            posts[0]["id"],
        )
    assert caught.value.status_code == 403
    operations.set_status(
        posts[0]["id"], "admitted", actor_id="admin", reason="reviewed restoration"
    )
    restored = studio.posts.detail("bob", posts[0]["id"])
    assert restored["version"] == 4 and restored["status"] == "published"
    assert studio.posts.detail("bob", posts[1]["id"])["version"] == 2
    with get_connection(studio.repo.db_path) as conn:
        assert (
            conn.execute("SELECT status FROM media_assets WHERE asset_id=?", (asset,)).fetchone()[0]
            == "ready"
        )
    operations.set_status(source, "retired", actor_id="admin", reason="source withdrawal")
    assert not studio.policy.resolve_many("bob", [post["id"] for post in posts], action="play")
    operations.set_status(source, "admitted", actor_id="admin", reason="source restoration")
    assert (
        len(studio.policy.resolve_many("bob", [post["id"] for post in posts], action="play")) == 2
    )
