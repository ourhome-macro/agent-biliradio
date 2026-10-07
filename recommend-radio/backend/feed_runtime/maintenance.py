from __future__ import annotations

import json
import re
import shutil
import time
from datetime import UTC, datetime

from database import get_connection

from .media import MediaService


def cleanup(repo, config, storage, *, apply=False, minimum_age=86400):
    """Only DB-known scratch paths and unreferenced managed object prefixes."""
    now = time.time()
    with get_connection(repo.db_path) as conn:
        jobs = [dict(r) for r in conn.execute("SELECT * FROM media_import_jobs")]
        assets = [dict(r) for r in conn.execute("SELECT * FROM media_assets")]
    live_paths = {job["work_key"] for job in jobs if job["status"] in {"queued", "running"}}
    live_assets = {job["asset_id"] for job in jobs if job["status"] in {"queued", "running"}}
    known_assets = {asset["asset_id"] for asset in assets}
    referenced = set()
    for asset in assets:
        for entry in json.loads(asset["manifest_json"]).get("files", {}).values():
            referenced.add(entry["key"].rsplit("/", 1)[0] + "/")
    media = MediaService(repo, config, storage)
    directories = []
    for job in jobs:
        age = 60 if job["status"] == "completed" else minimum_age
        if (
            job["status"] in {"queued", "running"}
            or job["work_key"] in live_paths
            or now - job["updated_at"] < age
        ):
            continue
        path = media.workdir(job)
        if path.exists() and path not in directories:
            directories.append(path)
            if apply:
                # workdir() resolves and checks the exact absolute deletion target.
                with get_connection(repo.db_path) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    live = conn.execute(
                        """SELECT 1 FROM media_import_jobs WHERE work_key=?
                        AND status IN ('queued','running')""",
                        (job["work_key"],),
                    ).fetchone()
                    if not live:
                        try:
                            shutil.rmtree(path)
                        except (FileNotFoundError, PermissionError):
                            pass
    objects = []
    if storage is not None and storage.health():
        for item in storage.client.list_objects(config.bucket, prefix="assets/", recursive=True):
            parts = item.object_name.split("/")
            if (
                len(parts) != 4
                or parts[1] not in known_assets
                or parts[1] in live_assets
                or not re.fullmatch(r"[0-9a-f]{64}", parts[2])
            ):
                continue
            prefix = "/".join(parts[:3]) + "/"
            if (
                prefix in referenced
                or (datetime.now(UTC) - item.last_modified).total_seconds() < minimum_age
            ):
                continue
            objects.append(item.object_name)
            if apply:
                with get_connection(repo.db_path) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    live = conn.execute(
                        """SELECT 1 FROM media_import_jobs WHERE asset_id=?
                        AND status IN ('queued','running')""",
                        (parts[1],),
                    ).fetchone()
                    current = conn.execute(
                        "SELECT manifest_json FROM media_assets WHERE asset_id=?", (parts[1],)
                    ).fetchone()
                    currently_referenced = current and any(
                        entry["key"] == item.object_name
                        for entry in json.loads(current[0]).get("files", {}).values()
                    )
                    if not live and not currently_referenced:
                        storage.remove(item.object_name)
    return {
        "applied": apply,
        "scratchDirectories": [str(path) for path in directories],
        "orphanObjects": objects,
    }
