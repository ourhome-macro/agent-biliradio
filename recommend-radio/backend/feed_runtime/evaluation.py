"""Export aggregate evaluation facts without private memory or signed media URLs."""

from __future__ import annotations

import json
import time
from collections import Counter

from database import get_connection
from request_spec import RequestSpec


def evaluate(repo):
    with get_connection(repo.db_path) as conn:
        issued = conn.execute("""SELECT i.session_id,i.content_id,i.snapshot_json,s.spec_json,
            c.metadata_json,c.facets_json FROM feed_items i
            JOIN feed_sessions s ON s.session_id=i.session_id
            JOIN feed_content c ON c.content_id=i.content_id""").fetchall()
        events = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT event_type,COUNT(*) FROM feed_events GROUP BY event_type"
            )
        }
        imports = {
            row[0]: row[1]
            for row in conn.execute("SELECT status,COUNT(*) FROM media_import_jobs GROUP BY status")
        }
        assets = {
            row[0]: row[1]
            for row in conn.execute("SELECT status,COUNT(*) FROM media_assets GROUP BY status")
        }
        used = conn.execute(
            "SELECT COALESCE(SUM(bytes),0) FROM media_assets WHERE bytes>0"
        ).fetchone()[0]
        counters = conn.execute("""SELECT c.content_id,c.likes,c.dislikes,
            COALESCE(SUM(r.state='like'),0) AS actual_likes,
            COALESCE(SUM(r.state='dislike'),0) AS actual_dislikes
            FROM content_counters c LEFT JOIN content_reactions r ON r.content_id=c.content_id
            GROUP BY c.content_id,c.likes,c.dislikes""").fetchall()
        pending = conn.execute("""SELECT COUNT(*) AS count,MIN(created_at) AS oldest
            FROM durable_jobs WHERE kind='feed_projection'
            AND status IN ('queued','running')""").fetchone()
        duplicate_ready = conn.execute("""SELECT COUNT(*) FROM (
            SELECT asset_id FROM media_import_jobs WHERE status='completed'
            GROUP BY asset_id HAVING COUNT(*)>1) AS duplicate_assets""").fetchone()[0]
    constraints = [
        RequestSpec.from_dict(json.loads(row["spec_json"])).matches_candidate(
            json.loads(row["metadata_json"]), json.loads(row["facets_json"])
        )
        for row in issued
    ]
    identities = Counter((row["session_id"], row["content_id"]) for row in issued)

    def ratio(numerator, denominator):
        return round(numerator / denominator, 6) if denominator else None

    return {
        "schemaVersion": "radio.feed.evaluation.v1",
        "generatedAt": time.time(),
        "feed": {
            "issued": len(issued),
            "events": events,
            "duplicateWithinSessionRate": ratio(
                sum(n - 1 for n in identities.values()), len(issued)
            ),
            "currentMetadataConstraintCompliance": ratio(sum(constraints), len(constraints)),
            "completionPerStart": ratio(events.get("complete", 0), events.get("play_started", 0)),
            "completionDenominator": "all play_started; interrupts remain in denominator",
        },
        "media": {
            "imports": imports,
            "assets": assets,
            "retainedBytes": used,
            "multipleCompletedImportsForAsset": duplicate_ready,
        },
        "projections": {
            "counterDriftContents": sum(
                row["likes"] != row["actual_likes"] or row["dislikes"] != row["actual_dislikes"]
                for row in counters
            ),
            "pendingJobs": pending["count"],
            "oldestPendingSeconds": max(0, time.time() - pending["oldest"])
            if pending["oldest"]
            else 0,
        },
        "limits": [
            "Observed engineering facts do not measure causal recommendation uplift.",
            "Compliance uses current metadata; issued decisions retain a separate snapshot.",
            "Feed v1 impressions remain separate from historical recommendation impressions.",
        ],
    }
