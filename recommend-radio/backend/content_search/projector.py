from __future__ import annotations

import json

from database import get_connection
from resource_policy import current_record

from .elastic import ElasticClient


def document(row):
    metadata = json.loads(row["metadata_json"])
    ugc = metadata.get("source") == "ugc"
    live = row["status"] == "admitted" and (not ugc or row["publication_status"] == "published")
    return {
        "contentId": row["content_id"],
        "source": "ugc" if ugc else "bili",
        "ownerId": row["owner_id"] or "",
        "scope": row["scope"],
        "status": "published" if live else "hidden",
        "kind": row["resource_kind"],
        "title": (row["title"] if ugc else metadata.get("title", "")) if live else "",
        "body": (row["body"] if ugc else metadata.get("description", "")) if live else "",
        "completion": (row["title"] if ugc else metadata.get("title", "")) if live else "",
        "contentVersion": row["version"],
        "permissionVersion": row["permission_version"],
        "updatedAt": row.get("updated_at") or row["admitted_at"],
        "tombstone": not live,
    }


class SearchProjector:
    def __init__(self, db_path, *, client=None):
        self.db_path, self.client = db_path, client or ElasticClient(writer=True)

    def handle(self, conn, envelope):
        # The consumer records Inbox/head in this SQL transaction. ES work is
        # done by prepare, before the transaction starts, with external versions.
        return None

    def prepare(self, envelope):
        if envelope["aggregateType"] != "content":
            return
        with get_connection(self.db_path) as conn:
            row = current_record(conn, envelope["aggregateId"])
        if row is None:
            # Hard deletion isn't a supported lifecycle operation: retain SQL tombstones.
            raise RuntimeError("Content projection has no authoritative tombstone")
        self.client.index_document(row["content_id"], row["version"], document(row))

    def rebuild(self, *, batch_size=200):
        cursor, indexed = "", 0
        while True:
            with get_connection(self.db_path) as conn:
                ids = [
                    r[0]
                    for r in conn.execute(
                        "SELECT content_id FROM feed_content WHERE content_id>? "
                        "ORDER BY content_id LIMIT ?",
                        (cursor, batch_size),
                    )
                ]
                # Read material for a bounded batch; don't hold a DB transaction over ES calls.
                rows = [current_record(conn, content_id) for content_id in ids]
            if not ids:
                break
            for row in rows:
                self.client.index_document(row["content_id"], row["version"], document(row))
                indexed += 1
            cursor = ids[-1]
        return {"indexed": indexed}
