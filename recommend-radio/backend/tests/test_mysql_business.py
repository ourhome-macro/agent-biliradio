from __future__ import annotations

import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import fakeredis
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from database import begin_write, get_connection, init_db
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from library_service import LibraryService
from models import Track
from mysql_fixture import NativeMySQL


@pytest.fixture(scope="module")
def mysql(tmp_path_factory):
    instance = NativeMySQL(tmp_path_factory.mktemp("mysql"))
    init_db(instance.url)
    yield instance.url
    from mysql_storage.connection import engine_for

    engine_for(instance.url).dispose()
    instance.close()


def test_mysql_core_schema_and_row_compatibility(mysql):
    from schema_migrations import HEAD

    with get_connection(mysql) as connection:
        row = connection.execute("SELECT ? AS a, ? AS a", ("first", "second")).fetchone()
        assert row[0] == "first" and row[1] == "second" and row["a"] == "first"
        assert dict(row) == {"a": "first"}
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone()[0] == HEAD
        assert (
            connection.execute(
                "SELECT MAX(2,3),json_extract(?, '$.text')", ('{"text":"中文"}',)
            ).fetchone()[1]
            == "中文"
        )


def test_mysql_catalog_pages_and_reaction_replay(mysql, monkeypatch):
    monkeypatch.setenv("FEED_ENABLED", "false")
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(mysql)
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    try:
        for index in range(25):
            repo.admit(
                Track(
                    bvid=f"BV{index:010d}", cid=index + 1, title=f"Music live {index}", duration=60
                ),
                user_id="legacy-owner",
            )
        service = FeedService(repo, FeedConfig(), cache)
        first = service.create("legacy-owner", mode="new", request_key="mysql-feed")
        assert len(first["items"]) == 10
        with ThreadPoolExecutor(max_workers=8) as executor:
            following = list(
                executor.map(
                    lambda _: service.page(
                        "legacy-owner", first["sessionId"], cursor=first["nextCursor"]
                    ),
                    range(16),
                )
            )
        assert all(page == following[0] for page in following)
        assert len(following[0]["items"]) == 10
        reactions = ReactionService(repo, cache)
        item = first["items"][0]
        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(
                executor.map(
                    lambda _: reactions.set(
                        "legacy-owner", item["contentId"], "like", "mysql-like"
                    ),
                    range(16),
                )
            )
        assert all(outcome == outcomes[0] for outcome in outcomes)
        with get_connection(mysql) as connection:
            assert (
                connection.execute(
                    "SELECT likes FROM content_counters WHERE content_id=?", (item["contentId"],)
                ).fetchone()[0]
                == 1
            )
    finally:
        cache.close()


def test_mysql_playlist_append_and_independent_updates(mysql):
    library = LibraryService(mysql)
    playlist = library.create_playlist("concurrent")
    tracks = [
        Track(bvid=f"BV8{i:09d}", cid=i + 1, title=f"Music piano {i}", duration=60)
        for i in range(8)
    ]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda track: library.batch_add_playlist_items(playlist["id"], tracks=[track]),
                tracks,
            )
        )
    with get_connection(mysql) as connection:
        rows = connection.execute(
            "SELECT position FROM playlist_items WHERE playlist_id=? ORDER BY position",
            (playlist["id"],),
        ).fetchall()
        assert len(rows) == 8 and len({row[0] for row in rows}) == 8
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(library.update_playlist, playlist["id"], name="renamed"),
            executor.submit(
                library.update_playlist, playlist["id"], cover="https://example.test/cover"
            ),
        ]
        for future in futures:
            future.result()
    with get_connection(mysql) as connection:
        row = connection.execute(
            "SELECT name,cover FROM playlists WHERE id=?", (playlist["id"],)
        ).fetchone()
        assert tuple(row) == ("renamed", "https://example.test/cover")


def test_mysql_rollback_and_duplicate_only_ignore(mysql):
    with pytest.raises(RuntimeError):
        with get_connection(mysql) as connection:
            begin_write(connection, namespace="test", key="rollback")
            connection.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                ("rolled-back", "now", "now"),
            )
            raise RuntimeError("abort")
    with get_connection(mysql) as connection:
        assert (
            connection.execute("SELECT id FROM app_users WHERE id=?", ("rolled-back",)).fetchone()
            is None
        )
        assert (
            connection.execute(
                "INSERT OR IGNORE INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                ("legacy-owner", "now", "now"),
            ).rowcount
            == 0
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT OR IGNORE INTO likes(user_id,track_id,created_at) VALUES (?,?,?)",
                ("legacy-owner", "missing-track", "now"),
            )


def test_mysql_shared_media_acquisition_and_claim_fencing(mysql, tmp_path, monkeypatch):
    from durable_jobs import claim
    from feed_runtime.media import MediaService

    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(mysql)
    content = repo.admit(
        Track(bvid="BV9999999999", cid=99, title="Music live shared", duration=60),
        user_id="legacy-owner",
    )
    media = MediaService(repo, replace(FeedConfig(), workdir=tmp_path / "media"), None)
    with ThreadPoolExecutor(max_workers=8) as executor:
        values = list(
            executor.map(
                lambda index: media.create("legacy-owner", content, f"viewer-{index}"), range(8)
            )
        )
    assert len({value["assetId"] for value in values}) == 1
    with get_connection(mysql) as connection:
        rows = connection.execute(
            "SELECT * FROM media_import_jobs WHERE asset_id=?", (values[0]["assetId"],)
        ).fetchall()
        assert len(rows) == 1
        task = rows[0]["job_id"]

    def take(_index):
        with get_connection(mysql) as connection:
            return claim(connection, task)

    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(executor.map(take, range(8)))
    assert sum(claim is not None for claim in claims) == 1


def test_mysql_conditional_upsert_and_sqlite_scalar_functions(mysql):
    with get_connection(mysql) as connection:
        row = connection.execute(
            "SELECT strftime('%s',?),MAX(?,?),json_extract(?, '$.text')",
            ("2020-01-01T00:00:00Z", 3, 5, '{"text":"中文"}'),
        ).fetchone()
        assert int(row[0]) == 1577836800 and row[1] == 5 and row[2] == "中文"
        scalar_null = connection.execute(
            "SELECT json_extract(?, '$.a'),json_extract(?, '$.b')",
            ('{"a":null}', '{"b":"null"}'),
        ).fetchone()
        assert scalar_null[0] is None and scalar_null[1] == "null"
        connection.execute(
            "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
            ("conditional", "old", "old"),
        )
        result = connection.execute(
            """INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)
            ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at
            WHERE app_users.updated_at=?""",
            ("conditional", "new", "new", "not-old"),
        )
        assert result.rowcount == 0
        assert (
            connection.execute(
                "SELECT updated_at FROM app_users WHERE id=?", ("conditional",)
            ).fetchone()[0]
            == "old"
        )


def test_mysql_snapshot_uses_database_content_and_preserves_checksums(mysql, tmp_path):
    from mysql_storage.transfer import _row_hash, export_snapshot

    destination = tmp_path / "mysql-snapshot.sqlite3"
    report = export_snapshot(mysql, destination)
    assert report["backend"] == "mysql" and report["consistentSnapshot"]
    with sqlite3.connect(destination) as connection:
        connection.row_factory = sqlite3.Row
        rows = [dict(row) for row in connection.execute("SELECT * FROM tracks")]
        assert report["tables"]["tracks"]["count"] == len(rows) and len(rows) >= 25
        assert report["tables"]["tracks"]["sha256"] == _row_hash(rows)
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()


def test_mysql_operator_summary_under_full_group_by(mysql, tmp_path):
    from types import SimpleNamespace

    from feed_runtime.operations import FeedOperations

    repo = FeedRepository(mysql)
    runtime = SimpleNamespace(repo=repo, config=replace(FeedConfig(), workdir=tmp_path / "work"))
    summary = FeedOperations(runtime).summary()
    assert summary["inventory"]["contents"]["admitted"] >= 25
    # JSON must not expose MySQL aggregate Decimal values as numeric strings.
    json.dumps(summary)


def test_mysql_backup_uses_live_backend_not_old_sqlite(mysql, tmp_path):
    from types import SimpleNamespace

    from feed_runtime.backup import create_backup

    create_backup(
        FeedRepository(mysql),
        SimpleNamespace(config=SimpleNamespace(bucket="empty-test")),
        tmp_path / "backup",
    )
    with sqlite3.connect(tmp_path / "backup/database.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM tracks").fetchone()[0] >= 25
    manifest = json.loads((tmp_path / "backup/manifest.json").read_text(encoding="utf-8"))
    assert manifest["sourceDatabaseEngine"] == "mysql"


def test_mysql_real_personal_profile_and_feed_ranking(mysql, monkeypatch):
    from amem_bridge import NoopAmemBridge
    from recommendation_service import RecommendationService
    from user_profile_reader import UserProfileReader

    monkeypatch.setenv("RECOMMEND_LLM_ENABLED", "false")
    monkeypatch.setenv("RECOMMEND_TEXT_EMBEDDING_BASE_URL", "")
    monkeypatch.setenv("RECOMMEND_AUDIO_EMBEDDING_BASE_URL", "")
    profile = UserProfileReader(mysql, "legacy-owner")._load_user_profile()
    assert isinstance(profile.skipped_track_ids, set)
    recommendations = RecommendationService(
        mysql, amem_bridge=NoopAmemBridge(), auto_discovery=False
    )
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    try:
        page = FeedService(
            FeedRepository(mysql), FeedConfig(), cache, recommendations=recommendations
        ).create("legacy-owner", request_key="native-personal")
        assert page["items"]
    finally:
        cache.close()


def test_mysql_health_and_evaluation_json_contract(mysql, tmp_path, monkeypatch):
    from types import SimpleNamespace

    from feed_runtime.evaluation import evaluate
    from feed_runtime.services import FeedServices

    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    try:
        runtime = FeedServices(
            user_id="legacy-owner",
            db_path=mysql,
            config=replace(
                FeedConfig(),
                enabled=True,
                workdir=tmp_path / "work",
                minio_access_key="fixture",
                minio_secret_key="fixture",
            ),
            cache=cache,
            storage=SimpleNamespace(health=lambda: True),
        )
        assert runtime.health()["redis"] and runtime.health()["minio"]
        json.dumps(evaluate(runtime.repo))
    finally:
        cache.close()


def test_mysql_lock_keys_do_not_serialize_unrelated_aggregates(mysql):
    import threading

    held, release = threading.Event(), threading.Event()

    def writer():
        with get_connection(mysql) as connection:
            begin_write(connection, namespace="parallel-check", key="A")
            held.set()
            assert release.wait(5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = executor.submit(writer)
        assert held.wait(5)
        try:

            def independent():
                with get_connection(mysql) as connection:
                    begin_write(connection, namespace="parallel-check", key="B")
                    return connection.execute("SELECT 1").fetchone()[0]

            assert executor.submit(independent).result(timeout=2) == 1
        finally:
            release.set()
        pending.result()


def test_mysql_active_asset_unique_constraint_and_checks(mysql):
    from durable_jobs import enqueue

    with get_connection(mysql) as connection:
        job = connection.execute("SELECT * FROM media_import_jobs LIMIT 1").fetchone()
        assert job
        copy = dict(job)
        copy.pop("active_asset_key")
        copy["import_id"] = "should-not-be-second-active"
        copy["attempt"] = int(copy["attempt"]) + 1
        copy["job_id"] = enqueue(
            connection,
            kind="media_import",
            user_id="legacy-owner",
            payload={},
            job_id="second-active-job",
            retry_safe=True,
        )
        with pytest.raises(sqlite3.IntegrityError, match="idx_media_active_import"):
            connection.execute(
                "INSERT INTO media_import_jobs ("
                + ",".join(copy)
                + ") VALUES ("
                + ",".join("?" for _ in copy)
                + ")",
                tuple(copy.values()),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE content_counters SET likes=-1 LIMIT 1")


def test_mysql_transfer_refuses_unmapped_tables_without_touching_them(mysql, tmp_path):
    import sqlalchemy as sa
    from mysql_storage.connection import engine_for
    from mysql_storage.transfer import export_snapshot, import_snapshot

    source = tmp_path / "before-extra.sqlite3"
    export_snapshot(mysql, source)
    metadata = sa.MetaData()
    sa.Table("app_users", metadata, autoload_with=engine_for(mysql))
    table = sa.Table(
        "unmapped_audit",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "user_id",
            sa.String(191, collation="utf8mb4_0900_bin"),
            sa.ForeignKey("app_users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("payload", sa.String(100)),
    )
    table.create(engine_for(mysql))
    with engine_for(mysql).begin() as connection:
        connection.execute(
            sa.insert(table).values(id=1, user_id="legacy-owner", payload="must-survive")
        )
    with pytest.raises(RuntimeError, match="unmapped"):
        import_snapshot(source, mysql)
    refused = tmp_path / "must-not-create.sqlite3"
    with pytest.raises(RuntimeError, match="unmapped"):
        export_snapshot(mysql, refused)
    assert not refused.exists()
    with engine_for(mysql).connect() as connection:
        assert connection.execute(sa.select(table.c.payload)).scalar_one() == "must-survive"
