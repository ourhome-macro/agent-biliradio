"""Authoritative batch authorization. Caches and search results never grant access."""

from __future__ import annotations

import json

from database import get_connection
from error_code import APIError


def current_record(conn, content_id):
    """Internal projection input only; callers must authorize before user serialization."""
    row = conn.execute(
        """SELECT c.*,p.title,p.body,p.kind AS post_kind,
        p.status AS publication_status,p.visibility,p.author_id,
        p.media_source_content_id,p.media_asset_id,p.published_at,p.updated_at
        FROM feed_content c LEFT JOIN ugc_posts p ON p.content_id=c.content_id
        WHERE c.content_id=?""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


class ResourcePolicy:
    def __init__(self, db_path):
        self.db_path = db_path

    @staticmethod
    def base_visible(row, user_id, *, action="read"):
        if row is None:
            return False
        keys = row.keys()
        if row["scope"] != "public" and row["owner_id"] != user_id:
            return False
        if row["status"] != "admitted":
            if not (action == "read" and row["status"] == "draft" and row["owner_id"] == user_id):
                return False
        kind = row["resource_kind"] if "resource_kind" in keys else "video"
        if action in {"feed", "play"} and kind not in {'video', 'audio'}:
            return False
        if action == "share" and row["scope"] != "public":
            return False
        return True

    def resolve_many(self, user_id, ids, *, action="read", include_private=True, conn=None):
        identifiers = list(dict.fromkeys(str(value) for value in ids))
        if len(identifiers) > 500:
            raise APIError.validation_error("Resolve at most 500 resources per batch")
        if not identifiers:
            return {}
        if conn is None:
            with get_connection(self.db_path) as connection:
                return self.resolve_many(
                    user_id,
                    identifiers,
                    action=action,
                    include_private=include_private,
                    conn=connection,
                )
        loaded, visited, queue = {}, set(), identifiers
        # Referenced media can have its own visibility. Read those live facts too.
        for _depth in range(8):
            if not queue:
                break
            visited.update(queue)
            rows = conn.execute(
                """SELECT c.*,p.author_id AS post_author,p.kind AS post_kind,p.title AS post_title,
                p.body AS post_body,p.status AS post_status,p.visibility AS post_visibility,
                p.version AS post_version,p.permission_version AS post_permission_version,
                p.media_source_content_id,p.media_asset_id,p.created_at AS post_created_at,
                p.updated_at AS post_updated_at,p.published_at AS post_published_at,p.audio_upload_id,
                a.status AS asset_status,l.asset_id AS linked_asset_id
                FROM feed_content c LEFT JOIN ugc_posts p ON p.content_id=c.content_id
                LEFT JOIN content_media_links l ON l.content_id=c.content_id
                LEFT JOIN media_assets a ON a.asset_id=l.asset_id WHERE c.content_id IN ("""
                + ",".join("?" for _ in queue)
                + ")",
                tuple(queue),
            ).fetchall()
            queue = []
            for value in rows:
                row = dict(value)
                loaded[row["content_id"]] = row
            for row in list(loaded.values()):
                source = row.get("media_source_content_id")
                if source and source not in visited and source not in queue:
                    queue.append(source)
            if len(loaded) + len(queue) > 1000:
                break

        def allowed(row, operation, stack=()):
            if not self.base_visible(row, user_id, action=operation):
                return False
            if not include_private and row["scope"] != "public":
                return False
            if row["content_id"] in stack:
                return False
            metadata = json.loads(row["metadata_json"])
            if operation in {"play", "feed"} and row["asset_status"] in {"retired", "unsupported"}:
                return False
            if metadata.get("source") == "ugc":
                if not row["post_author"] or row["post_status"] == "deleted":
                    return False
                if row["post_status"] != "published" and not (
                    operation == "read" and row["post_author"] == user_id
                ):
                    return False
                if row["post_visibility"] != "public" and row["post_author"] != user_id:
                    return False
                if row['post_kind'] == 'audio' and operation in {'play', 'feed'} and (
                    row['asset_status'] != 'ready' or row['media_asset_id'] != row['linked_asset_id']
                ):
                    return False
                if row["post_kind"] == "video" and not (
                    operation == "read" and row["post_author"] == user_id
                ):
                    source = loaded.get(row["media_source_content_id"])
                    if not source or row["asset_status"] != "ready":
                        return False
                    if not allowed(source, "play", (*stack, row["content_id"])):
                        return False
                    if row["media_asset_id"] != row["linked_asset_id"]:
                        return False
            return True

        output = {}
        for content_id in identifiers:
            row = loaded.get(content_id)
            if not row or not allowed(row, action):
                continue
            metadata = json.loads(row["metadata_json"])
            own = row["owner_id"] == user_id
            post = row["post_author"] is not None
            output[content_id] = {
                "id": content_id,
                "contentId": content_id,
                "title": row["post_title"] if post else metadata.get("title", ""),
                "body": row["post_body"] if post else metadata.get("description", ""),
                "kind": row["resource_kind"],
                "ownerId": row["owner_id"],
                "isOwner": own,
                "author": {
                    "id": row["owner_id"] if post else str(metadata.get("ownerMid") or ""),
                    "name": metadata.get("owner", ""),
                },
                "visibility": row["post_visibility"]
                if post
                else "public"
                if row["scope"] == "public"
                else "private",
                "status": row["post_status"] if post else "published",
                "version": row["version"],
                "permissionVersion": row["permission_version"],
                "source": metadata.get("source", "unknown"),
                "track": metadata,
                "assetId": row["linked_asset_id"],
                "assetStatus": row["asset_status"] or "empty",
                "mediaSourceContentId": row["media_source_content_id"],
                "audioUploadId": row['audio_upload_id'],
                "createdAt": row["post_created_at"] if post else row["admitted_at"],
                "updatedAt": row["post_updated_at"] if post else row["admitted_at"],
                "publishedAt": row["post_published_at"] if post else row["admitted_at"],
                "canEdit": own and post,
                "sharePath": (
                    f"/#/content/{content_id}" if post else f"/#/feed?content={content_id}"
                )
                if row["scope"] == "public" and row["status"] == "admitted"
                else None,
            }
        return output

    def materialize(self, user_id, ids, *, include_private=True):
        values = self.resolve_many(user_id, ids, action="search", include_private=include_private)
        return [values[value] for value in ids if value in values]

    def require(self, user_id, content_id, *, action="read", conn=None):
        result = self.resolve_many(user_id, [content_id], action=action, conn=conn).get(content_id)
        if result is None:
            raise APIError.not_found("Content not found or unavailable")
        return result
