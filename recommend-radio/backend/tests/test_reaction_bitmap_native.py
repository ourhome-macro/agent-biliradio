from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from database import get_connection, init_db
from feed_runtime.bitmap import MAX_INTEGER, SHARD_BITS, bitmap_keys, user_bitmap_id
from feed_runtime.cache import FeedCache
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from models import Track
from mysql_fixture import NativeMySQL, SecretURL
from mysql_storage.connection import engine_for
from mysql_storage.schema import canonical_schema, metadata_from_catalog
from mysql_storage.transfer import _row_hash
from mysql_storage.upgrade import upgrade_mysql
from redis_fixture import NativeRedis
from redis_stream_jobs import RedisJobBus, RedisJobWorker
from integration_settings import acceptance_settings


@pytest.fixture(scope="module")
def mysql(tmp_path_factory):
    settings = acceptance_settings()
    instance, admin, name = None, None, None
    if settings is not None:
        root = sa.engine.make_url(settings["KAFKA_TEST_ROOT_DATABASE_URL"])
        assert root.host in {"127.0.0.1", "localhost"} and root.port not in {None, 3306, 3307}
        name = "radio_bitmap_" + uuid4().hex[:14]
        admin = engine_for(SecretURL(root.set(database=None).render_as_string(hide_password=False)))
        with admin.begin() as conn:
            conn.execute(sa.schema.CreateSchema(name))
        url = SecretURL(root.set(database=name).render_as_string(hide_password=False))
    else:
        instance = NativeMySQL(tmp_path_factory.mktemp("bitmap-mysql"))
        url = instance.url
    # Recreate the prior canonical business schema, not merely a wrong version stamp.
    catalog = canonical_schema(revision="radio_007")
    metadata = metadata_from_catalog(catalog)
    engine = engine_for(url)
    metadata.create_all(engine)
    with engine.begin() as connection:
        for table in metadata.sorted_tables:
            if table.name in catalog and catalog[table.name]["rows"]:
                connection.execute(sa.insert(table), catalog[table.name]["rows"])
    yield url
    engine.dispose()
    if instance:
        instance.close()
    else:
        with admin.begin() as conn:
            conn.execute(sa.schema.DropSchema(name))


@pytest.fixture(scope="module")
def redis_server(tmp_path_factory):
    values = acceptance_settings()
    if values is not None:
        from redis import Redis

        url = values.get("KAFKA_TEST_REDIS_URL", "redis://127.0.0.1:16379/0")
        client = Redis.from_url(url, decode_responses=True)
        yield SimpleNamespace(url=url, client=client)
        client.close()
        return
    server = NativeRedis(tmp_path_factory.mktemp("bitmap-redis"))
    yield server
    server.close()


def test_explicit_upgrade_preserves_all_prior_business_records(mysql):
    from schema_migrations import HEAD

    engine = engine_for(mysql)
    names = [name for name in sa.inspect(engine).get_table_names() if name != "alembic_version"]

    def fingerprints():
        with engine.connect() as connection:
            return {
                name: _row_hash(
                    [
                        dict(row)
                        for row in connection.execute(sa.text(f"SELECT * FROM `{name}`")).mappings()
                    ]
                )
                for name in names
            }

    before = fingerprints()
    assert upgrade_mysql(mysql) == {"revision": HEAD, "changed": True}
    assert before == fingerprints()
    assert upgrade_mysql(mysql) == {"revision": HEAD, "changed": False}
    init_db(mysql)
    with get_connection(mysql) as connection:
        assert (
            connection.execute(
                "SELECT bitmap_id FROM reaction_bitmap_users WHERE user_id='legacy-owner'"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize("backend", ["sqlite", "mysql"])
def test_native_stream_retry_uses_bitmap_without_double_count(
    mysql, redis_server, tmp_path, monkeypatch, backend
):
    monkeypatch.setenv("FEED_ENABLED", "true")
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(mysql if backend == "mysql" else tmp_path / "database.sqlite3")
    cache = FeedCache(redis_server.url, prefix="bitmap-native:" + uuid4().hex)
    content = repo.admit(
        Track(bvid="BVbitmap" + uuid4().hex[:8], cid=1, title="Music live"), user_id="legacy-owner"
    )
    service = ReactionService(repo, cache)
    service.set("legacy-owner", content, "like", uuid4().hex)
    with get_connection(repo.db_path) as conn:
        row = conn.execute(
            "SELECT job_id FROM durable_jobs WHERE kind='feed_projection' AND user_id=? AND payload_json LIKE ?",
            ("legacy-owner", "%" + content + "%"),
        ).fetchone()
    bus = RedisJobBus(repo.db_path, redis_server.url)
    bus.ensure_groups(("events",))
    worker = RedisJobWorker(
        bus, dispatcher=lambda job: service.project(json.loads(job["payload_json"]))
    )
    publisher = cache.publish_reaction
    try:
        monkeypatch.setattr(
            cache,
            "publish_reaction",
            lambda *a, **k: (_ for _ in ()).throw(
                ConnectionError("fixture Redis publication failed")
            ),
        )
        bus.publish(row[0], "feed_projection")
        assert (
            worker.process(*bus.read(("events",), "bitmap", block_ms=10)[0], "bitmap") == "queued"
        )
        assert bus.redis.xpending(bus.key("events"), bus.group)["pending"] == 0
        with get_connection(repo.db_path) as conn:
            assert (
                conn.execute(
                    "SELECT likes FROM content_counters WHERE content_id=?", (content,)
                ).fetchone()[0]
                == 1
            )
            conn.execute(
                "UPDATE durable_jobs SET available_at=0,next_publish_at=0 WHERE job_id=?", (row[0],)
            )
        monkeypatch.setattr(cache, "publish_reaction", publisher)
        bus.publish(row[0], "feed_projection")
        assert (
            worker.process(*bus.read(("events",), "bitmap", block_ms=10)[0], "bitmap")
            == "completed"
        )
        bitmap_id = user_bitmap_id(repo.db_path, "legacy-owner")
        assert (
            cache.reaction(
                "legacy-owner",
                content,
                lambda: pytest.fail("Expected native bitmap hit"),
                bitmap_id=bitmap_id,
                expected_version=1,
            )["state"]
            == "like"
        )
        with get_connection(repo.db_path) as conn:
            assert (
                conn.execute(
                    "SELECT likes FROM content_counters WHERE content_id=?", (content,)
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM feed_inbox WHERE consumer='reaction' AND event_id=?",
                    (row[0],),
                ).fetchone()[0]
                == 1
            )
    finally:
        cache.close()
        bus.redis.close()


def test_native_lua_ordering_shards_and_partial_loss(redis_server):
    cache = FeedCache(redis_server.url, prefix="bitmap-native:" + uuid4().hex)
    try:

        def publish(version):
            return cache.publish_reaction(
                "user",
                "content",
                "like" if version % 2 else "dislike",
                version,
                bitmap_id=SHARD_BITS,
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(publish, range(1, 33)))
        keys, offset = bitmap_keys(cache.prefix, "content", SHARD_BITS)
        assert [cache.redis.getbit(key, offset) for key in keys[:3]] == [0, 1, 1]
        assert all(cache.redis.strlen(key) <= 512 for key in keys[:3])
        cache.publish_reaction("other", "content", "like", MAX_INTEGER, bitmap_id=SHARD_BITS + 1)
        assert not cache.publish_reaction(
            "other", "content", "neutral", MAX_INTEGER - 1, bitmap_id=SHARD_BITS + 1
        )
        cache.redis.delete(keys[0])
        assert (
            cache.reaction(
                "user",
                "content",
                lambda: {"state": "dislike", "version": 32},
                bitmap_id=SHARD_BITS,
                expected_version=32,
            )["state"]
            == "dislike"
        )
        assert (
            cache.reaction(
                "other",
                "content",
                lambda: pytest.fail("Other shard must survive"),
                bitmap_id=SHARD_BITS + 1,
                expected_version=MAX_INTEGER,
            )["state"]
            == "like"
        )
    finally:
        cache.close()


def test_mysql_user_mapping_concurrent_allocation(mysql):
    user = "opaque-user-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(lambda _: user_bitmap_id(mysql, user, create=True), range(20)))
    assert len(set(values)) == 1
    with get_connection(mysql) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM reaction_bitmap_users WHERE user_id=?", (user,)
            ).fetchone()[0]
            == 1
        )


def test_native_partial_script_error_is_not_served_as_valid_state(redis_server):
    cache = FeedCache(redis_server.url, prefix="bitmap-error:" + uuid4().hex)
    try:
        cache.publish_reaction("user", "content", "like", 1, bitmap_id=1)
        keys, offset = bitmap_keys(cache.prefix, "content", 1)
        with pytest.raises(Exception, match="bit is not an integer or out of range"):
            cache.redis.eval(
                "redis.call('hset',KEYS[1],ARGV[1],'2:dislike');"
                "redis.call('setbit',KEYS[2],ARGV[1],3)",
                2,
                keys[3],
                keys[0],
                offset,
            )
        assert cache.redis.hget(keys[3], str(offset)) == "2:dislike"
        assert (
            cache.reaction(
                "user",
                "content",
                lambda: {"state": "dislike", "version": 2},
                bitmap_id=1,
                expected_version=2,
            )["state"]
            == "dislike"
        )
        assert cache.redis.getbit(keys[0], offset) == 0
        assert cache.redis.getbit(keys[1], offset) == 1
    finally:
        cache.close()


@pytest.mark.parametrize("backend", ["sqlite", "mysql"])
def test_online_bitmap_only_rebuild_preserves_counts_and_newer_cache(
    mysql, redis_server, tmp_path, monkeypatch, backend
):
    monkeypatch.setenv("FEED_ENABLED", "false")
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(mysql if backend == "mysql" else tmp_path / "database.sqlite3")
    cache = FeedCache(redis_server.url, prefix="bitmap-rebuild:" + uuid4().hex)
    service = ReactionService(repo, cache)
    content = repo.admit(
        Track(bvid="BVrebuild" + uuid4().hex[:8], cid=1, title="Music live"), user_id="legacy-owner"
    )
    service.set("legacy-owner", content, "like", uuid4().hex)
    with get_connection(repo.db_path) as conn:
        before = _row_hash([dict(r) for r in conn.execute("SELECT * FROM content_counters")])
    try:
        report = service.rebuild_bitmaps(batch_size=1)
        assert report["relations"] >= 1 and report["published"] == report["relations"]
        bitmap_id = user_bitmap_id(repo.db_path, "legacy-owner")
        assert (
            cache.reaction(
                "legacy-owner",
                content,
                lambda: pytest.fail("Rebuild must fill bitmap"),
                bitmap_id=bitmap_id,
                expected_version=1,
            )["state"]
            == "like"
        )
        cache.publish_reaction("legacy-owner", content, "neutral", 99, bitmap_id=bitmap_id)
        assert service.rebuild_bitmaps(batch_size=1)["skippedNewerVersions"] >= 1
        with get_connection(repo.db_path) as conn:
            assert before == _row_hash(
                [dict(r) for r in conn.execute("SELECT * FROM content_counters")]
            )
    finally:
        cache.close()
