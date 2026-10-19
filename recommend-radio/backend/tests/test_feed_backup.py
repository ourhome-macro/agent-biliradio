from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from database import get_connection
from feed_runtime.backup import create_backup, object_path, restore_backup, verify_backup
from feed_runtime.config import FeedConfig
from feed_runtime.media import MediaService
from feed_runtime.product import FeedProductService
from feed_runtime.repository import FeedRepository
from models import Track


class Storage:
    def __init__(self):
        self.config = SimpleNamespace(bucket="fixture-media")
        self.client = self
        self.objects = {}
        self.types = {}

    def ensure_bucket(self):
        pass

    def prepare_restore_bucket(self):
        if self.objects:
            raise ValueError("Restore requires a new dedicated bucket")

    def stat(self, key):
        if key not in self.objects:
            return None
        value = self.objects[key]
        return {
            "bytes": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
            "versionId": "target-version",
        }

    def get_object(self, bucket, key, version_id=None):
        assert bucket == self.config.bucket
        assert version_id == "source-version"
        return SimpleNamespace(
            stream=lambda _: iter([self.objects[key]]),
            close=lambda: None,
            release_conn=lambda: None,
        )

    def put_file(self, key, path, *, content_type):
        self.objects[key] = path.read_bytes()
        self.types[key] = content_type
        return {"key": key, **self.stat(key)}


@pytest.fixture
def backup(tmp_path, monkeypatch):
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(tmp_path / "source.sqlite3")
    config = replace(FeedConfig(), workdir=tmp_path / "work")
    source = Storage()
    content = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music Live", duration=60), user_id="legacy-owner"
    )
    playback = MediaService(repo, config, source).create("legacy-owner", content, "play")
    FeedProductService(repo, config).open("legacy-owner", content)
    source.objects = {
        "assets/asset/version/index.m3u8": b"#EXTM3U\n#EXT-X-ENDLIST\n",
        "assets/asset/version/init.mp4": b"fixture init bytes",
    }
    entries = {
        key.rsplit("/", 1)[1]: {
            "key": key,
            "bytes": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
            "versionId": "source-version",
        }
        for key, value in source.objects.items()
    }
    with get_connection(repo.db_path) as conn:
        conn.execute("INSERT INTO creators VALUES ('fixture-creator','bili','202','Fixture','',0)")
        conn.execute("""INSERT INTO creator_follow_relations
            (user_id,bili_account_mid,creator_id,binding_key,desired_state,confirmed_state,
             sync_status,version,lease_token,lease_until,updated_at)
            VALUES ('legacy-owner','101','fixture-creator','fixture-binding',1,NULL,
                    'pending',1,'old-worker',9999999999,0)""")
        conn.execute("""INSERT INTO follow_sync_runs
            (sync_run_id,user_id,bili_account_mid,binding_key,command_id,status,created_at,updated_at)
            VALUES ('fixture-run','legacy-owner','101','fixture-binding','fixture-sync','running',0,0)""")
        conn.execute("""INSERT INTO creator_supply(creator_id,status,lease_token,lease_until,version)
            VALUES ('fixture-creator','running','old-worker',9999999999,1)""")
        conn.execute("""INSERT INTO app_sessions
                     (token_hash,user_id,created_at,expires_at,last_seen_at)
                     VALUES ('fixture-token','legacy-owner',
                     '2026-10-07','2099-01-01','2026-10-07')""")
        conn.execute(
            "UPDATE media_assets SET bytes=?,manifest_json=?,status='ready' WHERE asset_id=?",
            (
                sum(map(len, source.objects.values())),
                json.dumps({"files": entries}),
                playback["assetId"],
            ),
        )
    root = tmp_path / "backup"
    # Keep a live WAL writer while snapshotting; the backup must capture its committed data.
    writer = sqlite3.connect(repo.db_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("UPDATE feed_sessions SET revision='committed-in-wal'")
        writer.commit()
        assert (tmp_path / "source.sqlite3-wal").exists()
        create_backup(repo, source, root)
    finally:
        writer.close()
    return repo, source, root


def test_backup_restore_snapshot_and_media_integrity(backup, tmp_path):
    _, source, root = backup
    manifest = verify_backup(root)
    assert len(manifest["objects"]) == 2
    target_storage = Storage()
    target = tmp_path / "restored.sqlite3"
    result = restore_backup(root, target, target_storage)
    assert result["jobsRequireReconciliation"] and target_storage.objects == source.objects
    conn = sqlite3.connect(target)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM app_sessions").fetchone()[0] == 0
        assert conn.execute(
            "SELECT sync_status,lease_token,lease_until,error_type FROM creator_follow_relations"
        ).fetchone() == ("failed", None, 0, "BackupRestoredRequiresReconciliation")
        assert conn.execute("SELECT status FROM follow_sync_runs").fetchone()[0] == "failed"
        assert conn.execute(
            "SELECT status,lease_token,lease_until,version FROM creator_supply"
        ).fetchone() == ("idle", None, 0, 2)
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM media_playbacks WHERE active<>0 OR expires_at<>0"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM feed_sessions WHERE expires_at<>0").fetchone()[0]
            == 0
        )
        assert (
            conn.execute("SELECT revision FROM feed_sessions").fetchone()[0] == "committed-in-wal"
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE status='needs_reconciliation'"
            ).fetchone()[0]
            >= 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM media_import_jobs WHERE status IN ('queued','running')"
            ).fetchone()[0]
            == 0
        )
        files = json.loads(conn.execute("SELECT manifest_json FROM media_assets").fetchone()[0])[
            "files"
        ]
        assert all(entry["versionId"] == "target-version" for entry in files.values())
    finally:
        conn.close()
    assert not list(tmp_path.glob("restored.sqlite3.restoring-*"))
    assert not target.with_name(target.name + ".restore.lock").exists()
    assert (
        target_storage.types["assets/asset/version/index.m3u8"] == "application/vnd.apple.mpegurl"
    )
    assert target_storage.types["assets/asset/version/init.mp4"] == "video/mp4"


def test_corrupted_media_and_database_cannot_be_restored(backup, tmp_path):
    _, _, root = backup
    media = object_path(root, "assets/asset/version/init.mp4")
    original = media.read_bytes()
    media.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum"):
        restore_backup(root, tmp_path / "never-created.sqlite3", Storage())
    assert not (tmp_path / "never-created.sqlite3").exists()
    media.write_bytes(original)
    with (root / "database.sqlite3").open("ab") as stream:
        stream.write(b"corrupted trailer")
    with pytest.raises(ValueError, match="database checksum"):
        verify_backup(root)


def test_existing_target_and_object_conflict_are_not_overwritten(backup, tmp_path):
    _, _, root = backup
    target = tmp_path / "existing.sqlite3"
    target.write_bytes(b"existing target sentinel")
    with pytest.raises(ValueError, match="new database path"):
        restore_backup(root, target, Storage())
    assert target.read_bytes() == b"existing target sentinel"
    destination = Storage()
    destination.objects["assets/asset/version/index.m3u8"] = b"conflicting object"
    target = tmp_path / "conflict.sqlite3"
    with pytest.raises(ValueError, match="new dedicated bucket"):
        restore_backup(root, target, destination)
    assert not target.exists()
    assert destination.objects["assets/asset/version/index.m3u8"] == b"conflicting object"


@pytest.mark.parametrize(
    "key",
    [
        "../outside",
        "/assets/a/b",
        "assets/a/../../out",
        "assets/a\\b",
        "assets/a:stream/b",
        "assets//a/b",
        "assets/./a/b",
    ],
)
def test_object_keys_reject_traversal_and_windows_aliases(tmp_path, key):
    with pytest.raises(ValueError):
        object_path(tmp_path, key)


def test_objects_symlink_cannot_escape_backup_root(tmp_path):
    root, outside = tmp_path / "backup", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    try:
        (root / "objects").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Host does not allow test symlinks")
    with pytest.raises(ValueError, match="escaped"):
        object_path(root, "assets/a/b")
