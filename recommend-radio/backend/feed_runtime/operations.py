"""Operator-only catalog lifecycle and authoritative, cross-process Feed metrics."""

from __future__ import annotations

import json
import math
import shutil
import time

from database import begin_write, get_connection
from error_code import APIError

from .repository import ContentLinkChanged, encode, lock_content_asset


def distribution(values):
    ordered = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))

    def percentile(fraction):
        if not ordered:
            return None
        position = (len(ordered) - 1) * fraction
        lower, upper = math.floor(position), math.ceil(position)
        return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 2)

    return {"count": len(ordered), "p50": percentile(0.5), "p95": percentile(0.95)}


def media_measurement(repo, job, *, first_fragment=None, elapsed=None):
    try:
        with get_connection(repo.db_path) as conn:
            begin_write(conn, namespace="media-asset", key=job["asset_id"])
            current = conn.execute(
                "SELECT work_key,status FROM media_import_jobs WHERE import_id=?",
                (job["import_id"],),
            ).fetchone()
            if not current or current["work_key"] != job["work_key"]:
                return
            conn.execute(
                """INSERT INTO media_measurements
                (import_id,first_fragment_seconds,import_seconds,outcome,updated_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(import_id) DO UPDATE SET
                first_fragment_seconds=COALESCE(media_measurements.first_fragment_seconds,
                    excluded.first_fragment_seconds),
                import_seconds=COALESCE(excluded.import_seconds,media_measurements.import_seconds),
                outcome=excluded.outcome,updated_at=excluded.updated_at""",
                (job["import_id"], first_fragment, elapsed, current["status"], time.time()),
            )
    except Exception:
        # A measurement must not turn an already committed asset into a failed task.
        from .metrics import count

        count("measurement", "write_failed")


class FeedOperations:
    def __init__(self, runtime):
        self.runtime, self.repo, self.config = runtime, runtime.repo, runtime.config

    def summary(self):
        since = time.time() - 86400
        sample_limit = 10000
        with get_connection(self.repo.db_path) as conn:
            contents = {
                r[0]: r[1]
                for r in conn.execute("SELECT status,COUNT(*) FROM feed_content GROUP BY status")
            }
            assets = {
                r[0]: r[1]
                for r in conn.execute("SELECT status,COUNT(*) FROM media_assets GROUP BY status")
            }
            items = [
                {
                    "contentId": r["content_id"],
                    "title": json.loads(r["metadata_json"]).get("title", ""),
                    "status": r["status"],
                    "assetStatus": r["asset_status"],
                }
                for r in conn.execute(
                    """SELECT c.*,a.status AS asset_status FROM feed_content c
                    LEFT JOIN content_media_links l ON l.content_id=c.content_id
                    LEFT JOIN media_assets a ON a.asset_id=l.asset_id
                    ORDER BY c.admitted_at DESC LIMIT 200"""
                )
            ]
            imports = [
                {
                    "importId": r["import_id"],
                    "assetId": r["asset_id"],
                    "status": r["status"],
                    "stage": r["stage"],
                    "attempt": r["attempt"],
                    "errorType": r["error_type"],
                    "updatedAt": r["updated_at"],
                }
                for r in conn.execute(
                    "SELECT * FROM media_import_jobs ORDER BY updated_at DESC LIMIT 100"
                )
            ]
            progress = [
                dict(r)
                for r in conn.execute(
                    """SELECT q.*,p.import_id FROM feed_playback_progress q
                JOIN media_playbacks p ON p.playback_id=q.playback_id WHERE q.last_event_at>=?
                ORDER BY q.last_event_at DESC LIMIT ?""",
                    (since, sample_limit),
                )
            ]
            totals = conn.execute(
                """SELECT COALESCE(SUM(watch_ms),0),COALESCE(SUM(buffering_ms),0),COUNT(*)
                FROM feed_playback_progress WHERE last_event_at>=?""",
                (since,),
            ).fetchone()
            event_counts = {
                r[0]: r[1]
                for r in conn.execute(
                    """SELECT event_type,COUNT(*) FROM feed_events
                WHERE recorded_at>=? GROUP BY event_type""",
                    (since,),
                )
            }
            measurements = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM media_measurements WHERE updated_at>=? "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (since, sample_limit),
                )
            ]
            used = conn.execute(
                "SELECT COALESCE(SUM(bytes),0) FROM media_assets WHERE bytes>0"
            ).fetchone()[0]
            drift = conn.execute("""SELECT COUNT(*) FROM (
                SELECT k.content_id FROM content_counters k LEFT JOIN content_reactions r
                ON r.content_id=k.content_id GROUP BY k.content_id,k.likes,k.dislikes
                HAVING k.likes<>COALESCE(SUM(r.state='like'),0)
                OR k.dislikes<>COALESCE(SUM(r.state='dislike'),0)) AS counter_drift""").fetchone()[
                0
            ]
            oldest = conn.execute("""SELECT MIN(created_at) FROM durable_jobs
                WHERE kind='feed_projection' AND status IN ('queued','running')""").fetchone()[0]
            duplicates = conn.execute(
                """SELECT COALESCE(SUM(n-1),0),COALESCE(SUM(n),0)
                FROM (SELECT COUNT(*) n FROM feed_items WHERE issued_at>=?
                GROUP BY session_id,content_id) AS duplicate_groups""",
                (since,),
            ).fetchone()
            failures = conn.execute(
                """WITH attempts AS (
                SELECT json_extract(payload_json,'$.playbackId') AS playback_id,
                MAX(event_type='play_started') AS started,
                MAX(event_type='playback_error') AS failed FROM feed_events
                WHERE recorded_at>=? AND event_type IN ('play_started','playback_error')
                AND json_extract(payload_json,'$.playbackId') IS NOT NULL GROUP BY playback_id
                ) SELECT COALESCE(SUM(started),0),COALESCE(SUM(started AND failed),0),
                COALESCE(SUM(failed AND NOT started),0) FROM attempts""",
                (since,),
            ).fetchone()
            dead = conn.execute("""SELECT COUNT(*) FROM durable_jobs
                WHERE kind IN ('media_import','feed_projection','feed_asset_ready')
                AND status IN ('failed','needs_reconciliation')""").fetchone()[0]
        scratch = 0
        for path in self.config.workdir.rglob("*"):
            try:
                if path.is_file():
                    scratch += path.stat().st_size
            except FileNotFoundError:
                # Import cancellation and scratch GC may delete a file during the scan.
                continue
        watched, buffered, total_progress = totals
        starts, failed_starts, failed_before_start = failures
        projection_lag = max(0, time.time() - oldest) if oldest is not None else 0
        alerts = []
        for name, value, limit in (
            ("storage", used, self.config.storage_bytes),
            ("scratch", scratch, self.config.scratch_bytes),
        ):
            if value >= limit * 0.8:
                alerts.append(
                    {
                        "level": "critical" if value >= limit else "warning",
                        "code": name + "_watermark",
                        "message": f"{name} 已使用 {value / limit:.0%}",
                    }
                )
        if dead:
            alerts.append(
                {
                    "level": "warning",
                    "code": "failed_jobs",
                    "message": f"{dead} 个任务需要检查或重试",
                }
            )
        if drift:
            alerts.append(
                {
                    "level": "warning",
                    "code": "counter_drift",
                    "message": "计数投影与关系事实存在差异",
                }
            )
        if projection_lag > 60:
            alerts.append(
                {"level": "warning", "code": "projection_lag", "message": "计数投影积压超过 60 秒"}
            )
        root = self.config.workdir
        while not root.exists():
            root = root.parent
        free = shutil.disk_usage(root).free
        if free < min(self.config.max_bytes, 512 * 1024**2):
            alerts.append(
                {"level": "critical", "code": "disk_space", "message": "工作目录磁盘可用空间不足"}
            )
        return {
            "enabled": self.config.enabled,
            "inventory": {
                "contents": contents,
                "assets": assets,
                "contentsList": items,
                "imports": imports,
                "retainedBytes": used,
                "storageBudgetBytes": self.config.storage_bytes,
                "scratchBytes": scratch,
                "scratchBudgetBytes": self.config.scratch_bytes,
                "diskFreeBytes": free,
            },
            "metrics": {
                "windowHours": 24,
                "distributionSampleLimit": sample_limit,
                "startupSampleTruncated": total_progress > sample_limit,
                "startupMs": {
                    "cold": distribution(r["startup_ms"] for r in progress if r["import_id"]),
                    "stored": distribution(r["startup_ms"] for r in progress if not r["import_id"]),
                },
                "bufferingRatio": buffered / (buffered + watched) if buffered + watched else None,
                "playbackErrorRate": failed_starts / starts if starts else None,
                "failedBeforeStartCount": failed_before_start,
                "swipeCount": event_counts.get("swipe_next", 0),
                "startedCount": starts,
                "firstFragmentSeconds": distribution(
                    r["first_fragment_seconds"] for r in measurements
                ),
                "importSeconds": distribution(r["import_seconds"] for r in measurements),
                "duplicateRate": duplicates[0] / duplicates[1] if duplicates[1] else None,
                "counterDriftContents": drift,
                "projectionLagSeconds": projection_lag,
            },
            "alerts": alerts,
        }

    @staticmethod
    def reason(value):
        if not isinstance(value, str) or not value.strip() or len(value) > 500:
            raise APIError.validation_error("A bounded operational reason is required")
        return value.strip()

    def set_status(self, content_id, status, *, actor_id, reason):
        for attempt in range(3):
            try:
                return self._set_status_once(content_id, status, actor_id=actor_id, reason=reason)
            except ContentLinkChanged:
                if attempt == 2:
                    raise APIError.conflict("Media link changed; retry lifecycle command") from None

    def _set_status_once(self, content_id, status, *, actor_id, reason):
        reason = self.reason(reason)
        if not isinstance(status, str) or status not in {"admitted", "retired"}:
            raise APIError.validation_error("Unsupported content status")
        now = time.time()
        with get_connection(self.repo.db_path) as conn:
            locked_asset = lock_content_asset(conn, content_id)
            content = conn.execute(
                "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
            ).fetchone()
            if not content:
                raise APIError.not_found("Content not found")
            if content["status"] == "catalogued":
                raise APIError.conflict(
                    "Content must pass music admission before lifecycle changes"
                )
            if content["status"] == status:
                self.audit(
                    conn,
                    actor_id,
                    "content_status_noop",
                    content_id,
                    reason,
                    {"status": status, "affectedContents": 0},
                )
                return {"contentId": content_id, "status": status, "affectedContents": 0}
            if content["status"] not in {"admitted", "retired"}:
                raise APIError.conflict("Unsupported content lifecycle transition")
            link = (locked_asset,) if locked_asset else None
            identifiers = [content_id]
            if link:
                identifiers = [
                    r[0]
                    for r in conn.execute(
                        "SELECT content_id FROM content_media_links WHERE asset_id=?", (link[0],)
                    )
                ]
                asset = conn.execute(
                    "SELECT * FROM media_assets WHERE asset_id=?", (link[0],)
                ).fetchone()
                asset_status = (
                    "retired"
                    if status == "retired"
                    else "ready"
                    if json.loads(asset["manifest_json"]).get("files")
                    else "empty"
                )
                conn.execute(
                    "UPDATE media_assets SET status=?,updated_at=? WHERE asset_id=?",
                    (asset_status, now, link[0]),
                )
                if status == "retired":
                    conn.execute(
                        "UPDATE media_playbacks SET active=0,expires_at=? WHERE asset_id=?",
                        (now, link[0]),
                    )
                    conn.execute(
                        """UPDATE durable_jobs SET status='failed',lease_token=NULL,lease_until=0,
                        error='ContentRetired',updated_at=? WHERE job_id IN
                        (SELECT job_id FROM media_import_jobs WHERE asset_id=?
                        AND status IN ('queued','running'))""",
                        (now, link[0]),
                    )
                    conn.execute(
                        """UPDATE media_import_jobs SET status='cancelled',stage='cancelled',
                        lease_token=NULL,lease_until=0,error_type='ContentRetired',updated_at=?
                        WHERE asset_id=? AND status IN ('queued','running')""",
                        (now, link[0]),
                    )
                    conn.execute(
                        """DELETE FROM media_storage_reservations WHERE import_id IN
                        (SELECT import_id FROM media_import_jobs WHERE asset_id=?)""",
                        (link[0],),
                    )
            for identifier in identifiers:
                conn.execute(
                    "UPDATE feed_content SET status=?,version=version+1 WHERE content_id=?",
                    (status, identifier),
                )
            self.audit(
                conn,
                actor_id,
                "content_status",
                content_id,
                reason,
                {"status": status, "affectedContents": len(identifiers)},
            )
        return {"contentId": content_id, "status": status, "affectedContents": len(identifiers)}

    def retry(self, import_id, *, actor_id, reason):
        reason = self.reason(reason)
        if not self.config.enabled:
            raise APIError.conflict("Enable Feed before scheduling imports")
        now = time.time()
        with get_connection(self.repo.db_path) as conn:
            initial = conn.execute(
                "SELECT asset_id FROM media_import_jobs WHERE import_id=?", (import_id,)
            ).fetchone()
            if not initial:
                raise APIError.not_found("Import not found")
            begin_write(conn, namespace="media-asset", key=initial[0])
            row = conn.execute(
                """SELECT j.*,c.status AS content_status,a.status AS asset_status
                FROM media_import_jobs j JOIN media_assets a ON a.asset_id=j.asset_id
                JOIN feed_content c ON c.content_id=a.content_id
                WHERE j.import_id=?""",
                (import_id,),
            ).fetchone()
            if not row:
                raise APIError.not_found("Import not found")
            if row["status"] != "failed" or row["content_status"] != "admitted":
                raise APIError.conflict("Only failed imports of admitted content can be retried")
            if row["asset_status"] in {"ready", "retired", "unsupported"}:
                raise APIError.conflict("Asset state does not permit another import")
            if conn.execute(
                "SELECT 1 FROM media_import_jobs WHERE asset_id=? AND attempt>?",
                (row["asset_id"], row["attempt"]),
            ).fetchone():
                raise APIError.conflict("Only the latest import attempt can be retried")
            if row["complete_source"]:
                directory = self.runtime.media.workdir(row)
                if not all((directory / name).is_file() for name in ("source.json", "index.m3u8")):
                    raise APIError.conflict(
                        "Completed local source expired; start a new playback to acquire it again"
                    )
            if conn.execute(
                """SELECT 1 FROM media_import_jobs WHERE asset_id=?
                AND status IN ('queued','running')""",
                (row["asset_id"],),
            ).fetchone():
                raise APIError.conflict("A newer import is already active")
            conn.execute(
                """UPDATE media_import_jobs SET status='queued',stage='queued',error_type=NULL,
                idle_since=NULL,lease_until=0,lease_token=NULL,updated_at=? WHERE import_id=?""",
                (now, import_id),
            )
            conn.execute(
                """UPDATE durable_jobs SET status='queued',attempts=0,error=NULL,
                lease_token=NULL,lease_until=0,available_at=0,next_publish_at=0,updated_at=?
                WHERE job_id=?""",
                (now, row["job_id"]),
            )
            self.audit(
                conn,
                actor_id,
                "retry_import",
                import_id,
                reason,
                {"completeSource": bool(row["complete_source"])},
            )
        return {
            "importId": import_id,
            "status": "queued",
            "requiresViewer": not bool(row["complete_source"]),
        }

    @staticmethod
    def audit(conn, actor, action, target, reason, details):
        conn.execute(
            """INSERT INTO feed_ops_audit(actor_id,action,target_id,reason,details_json,created_at)
            VALUES (?,?,?,?,?,?)""",
            (actor, action, target, reason, encode(details), time.time()),
        )
