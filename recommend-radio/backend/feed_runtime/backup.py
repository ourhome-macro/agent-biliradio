"""Immutable media backup with a portable snapshot of the active business database."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from contextlib import closing, contextmanager
from pathlib import Path, PurePosixPath
from uuid import uuid4

from .storage import file_hash


@contextmanager
def connection(*args, **kwargs):
    # sqlite's own context manager commits, but does not close its file handles.
    with closing(sqlite3.connect(*args, **kwargs)) as conn:
        with conn:
            yield conn


def content_type(key):
    return {
        ".m3u8": "application/vnd.apple.mpegurl",
        ".mp4": "video/mp4",
        ".m4s": "video/iso.segment",
        ".m4a": "audio/mp4",
        ".jpg": "image/jpeg",
        ".json": "application/json",
    }.get(PurePosixPath(key).suffix.lower(), "application/octet-stream")


def object_path(root, key):
    root = Path(root).resolve()
    if not isinstance(key, str) or not key or ":" in key:
        raise ValueError("Unsafe backup object key")
    parts = PurePosixPath(key)
    if (
        parts.is_absolute()
        or ".." in parts.parts
        or not parts.parts
        or parts.parts[0] != "assets"
        or "\\" in key
        or str(parts) != key
        or len(parts.parts) < 3
    ):
        raise ValueError("Unsafe backup object key")
    object_root = (root / "objects").resolve()
    target = (object_root / str(parts)).resolve()
    if not object_root.is_relative_to(root) or not target.is_relative_to(object_root):
        raise ValueError("Backup object escaped its directory")
    return target


def _references(database):
    with connection(f"{database.resolve().as_uri()}?mode=ro", uri=True) as conn:
        values = conn.execute("SELECT manifest_json FROM media_assets WHERE bytes>0").fetchall()
    refs = {}
    for (raw,) in values:
        for entry in json.loads(raw).get("files", {}).values():
            previous = refs.get(entry["key"])
            if previous and previous != entry:
                raise ValueError("Conflicting immutable object references")
            refs[entry["key"]] = entry
    return sorted(refs.values(), key=lambda entry: entry["key"])


def create_backup(repo, storage, directory):
    from database import mysql_target

    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=False)
    if os.name != "nt":
        root.chmod(0o700)
    snapshot = root / "database.sqlite3"
    mysql_url = mysql_target(repo.db_path)
    if mysql_url:
        from mysql_storage.transfer import export_snapshot

        export_snapshot(mysql_url, snapshot)
    else:
        with connection(f"{Path(repo.db_path).resolve().as_uri()}?mode=ro", uri=True) as source:
            with connection(snapshot) as destination:
                source.backup(destination, pages=128, sleep=0.05)
                destination.execute("PRAGMA journal_mode=DELETE")
    if os.name != "nt":
        snapshot.chmod(0o600)
    refs = _references(snapshot)
    for entry in refs:
        target = object_path(root, entry["key"])
        target.parent.mkdir(parents=True, exist_ok=True)
        response = storage.client.get_object(
            storage.config.bucket, entry["key"], version_id=entry.get("versionId")
        )
        try:
            with target.open("wb") as stream:
                for chunk in response.stream(1024 * 1024):
                    stream.write(chunk)
        finally:
            response.close()
            response.release_conn()
        if target.stat().st_size != entry["bytes"] or file_hash(target) != entry["sha256"]:
            raise ValueError("Backup object verification failed")
    manifest = {
        "schemaVersion": "radio.media.backup.v1",
        "createdAt": time.time(),
        "databaseSha256": file_hash(snapshot),
        "sourceDatabaseEngine": "mysql" if mysql_url else "sqlite",
        "objects": refs,
        "sourceBucket": storage.config.bucket,
        "complete": True,
    }
    temporary = root / "manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(root / "manifest.json")
    return {
        "directory": str(root),
        "objects": len(refs),
        "bytes": sum(e["bytes"] for e in refs),
        "complete": True,
    }


def verify_backup(directory):
    root = Path(directory).resolve()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if (
        manifest.get("schemaVersion") != "radio.media.backup.v1"
        or manifest.get("complete") is not True
    ):
        raise ValueError("Backup is incomplete or unsupported")
    database = (root / "database.sqlite3").resolve()
    if not database.is_relative_to(root):
        raise ValueError("Backup database escaped its directory")
    if file_hash(database) != manifest["databaseSha256"]:
        raise ValueError("Backup database checksum mismatch")
    with connection(f"{database.as_uri()}?mode=ro", uri=True) as conn:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Backup database failed integrity check")
        if conn.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Backup database failed foreign-key validation")
    expected = {e["key"]: e for e in _references(database)}
    declared = {e["key"]: e for e in manifest["objects"]}
    if expected != declared or len(declared) != len(manifest["objects"]):
        raise ValueError("Backup manifest does not match database references")
    for entry in declared.values():
        path = object_path(root, entry["key"])
        if (
            not path.is_file()
            or path.stat().st_size != entry["bytes"]
            or file_hash(path) != entry["sha256"]
        ):
            raise ValueError("Backup media checksum mismatch")
    return manifest


def restore_backup(directory, target_db, storage):
    if str(target_db).startswith(("mysql://", "mysql+pymysql://")):
        from database import database_identity, mysql_target
        from mysql_storage.transfer import import_snapshot

        with tempfile.TemporaryDirectory(prefix="radio-restore-") as temporary:
            snapshot = Path(temporary) / "restored.sqlite3"
            result = restore_backup(directory, snapshot, storage)
            report = import_snapshot(snapshot, mysql_target(target_db))
            result["database"] = "mysql:" + database_identity(target_db)
            result["migration"] = report
            return result
    root = Path(directory).resolve()
    manifest = verify_backup(root)
    target = Path(target_db).resolve()
    if target.exists():
        raise ValueError("Restore requires a new database path; existing data is never overwritten")
    target.parent.mkdir(parents=True, exist_ok=True)
    lock = target.with_name(target.name + ".restore.lock")
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    temporary = target.with_name(target.name + ".restoring-" + uuid4().hex)
    try:
        # A fresh dedicated namespace avoids overwriting another workload during restore.
        # Failed restores intentionally leave that bucket for operator inspection.
        storage.prepare_restore_bucket()
        for entry in manifest["objects"]:
            existing = storage.stat(entry["key"])
            if existing and (
                existing["bytes"] != entry["bytes"] or existing["sha256"] != entry["sha256"]
            ):
                raise ValueError("Target object conflicts with backup; restore stopped")
            storage.put_file(
                entry["key"],
                object_path(root, entry["key"]),
                content_type=content_type(entry["key"]),
            )
        shutil.copyfile(root / "database.sqlite3", temporary)
        with connection(temporary) as conn:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("UPDATE media_playbacks SET active=0,expires_at=0")
            conn.execute("UPDATE feed_sessions SET expires_at=0")
            conn.execute("""UPDATE media_import_jobs SET status='cancelled',stage='cancelled',
                complete_source=0,lease_token=NULL,lease_until=0,error_type='BackupRestored'
                WHERE status IN ('queued','running')""")
            conn.execute("DELETE FROM media_storage_reservations")
            conn.execute("""UPDATE durable_jobs SET status='needs_reconciliation',lease_token=NULL,
                lease_until=0,error='BackupRestoredRequiresReconciliation'
                WHERE status IN ('queued','running')""")
            # Session revocations cannot be replayed from an older snapshot.
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "app_sessions" in tables:
                conn.execute("DELETE FROM app_sessions")
            if "creator_follow_relations" in tables:
                conn.execute("""UPDATE creator_follow_relations SET lease_token=NULL,lease_until=0,
                    sync_status=CASE WHEN sync_status IN ('pending','unknown') THEN 'failed'
                    ELSE sync_status END,
                    error_type=CASE WHEN sync_status IN ('pending','unknown')
                    THEN 'BackupRestoredRequiresReconciliation' ELSE error_type END""")
            if "follow_sync_runs" in tables:
                conn.execute("""UPDATE follow_sync_runs SET status='failed',
                    error_type='BackupRestoredRequiresReconciliation'
                    WHERE status IN ('queued','running')""")
            if "creator_supply" in tables:
                conn.execute("""UPDATE creator_supply SET status='idle',lease_token=NULL,
                    lease_until=0,next_check_at=0,version=version+1
                    WHERE status IN ('queued','running')""")
            # Restore references use the newly created object versions in the target bucket.
            for row_id, raw in conn.execute(
                "SELECT asset_id,manifest_json FROM media_assets"
            ).fetchall():
                value = json.loads(raw)
                for entry in value.get("files", {}).values():
                    entry["versionId"] = (storage.stat(entry["key"]) or {}).get("versionId")
                conn.execute(
                    "UPDATE media_assets SET manifest_json=? WHERE asset_id=?",
                    (json.dumps(value, ensure_ascii=False), row_id),
                )
        # Publish without replacing a file concurrently created by another operator.
        with temporary.open("r+b") as stream:
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target)
        return {
            "database": str(target),
            "objects": len(manifest["objects"]),
            "jobsRequireReconciliation": True,
            "playbacksReleased": True,
            "targetBucket": storage.config.bucket,
        }
    finally:
        if temporary.exists():
            temporary.unlink()
        lock.unlink(missing_ok=True)
