from __future__ import annotations

import hashlib
import json
import time
from uuid import uuid4

from database import begin_write, get_connection
from domain_outbox import enqueue_event
from error_code import APIError
from feed_runtime.repository import encode
from resource_policy import ResourcePolicy


class UGCService:
    def __init__(self, db_path):
        self.db_path, self.policy = db_path, ResourcePolicy(db_path)

    @staticmethod
    def _validated(payload, old=None):
        if not isinstance(payload, dict) or set(payload) - {
            "expectedVersion",
            "title",
            "body",
            "kind",
            "visibility",
            "mediaSourceContentId",
            "audioUploadId",
        }:
            raise APIError.validation_error("Unsupported post fields")
        values = {
            "kind": old["kind"] if old else "text",
            "title": old["title"] if old else "",
            "body": old["body"] if old else "",
            "visibility": old["visibility"] if old else "private",
            "mediaSourceContentId": old["media_source_content_id"] if old else None,
            "audioUploadId": old['audio_upload_id'] if old else None,
        }
        values.update({key: value for key, value in payload.items() if key != "expectedVersion"})
        if values["kind"] not in ("text", "video", "audio") or values["visibility"] not in (
            "public",
            "private",
        ):
            raise APIError.validation_error("Invalid post kind or visibility")
        if old and values["kind"] != old["kind"]:
            raise APIError.validation_error("Post kind cannot be changed")
        for name, maximum in (("title", 200), ("body", 50000)):
            if not isinstance(values[name], str) or len(values[name]) > maximum:
                raise APIError.validation_error(f"Invalid {name}")
        values["title"] = values["title"].strip()
        if not values["title"]:
            raise APIError.validation_error("Title is required")
        source = values["mediaSourceContentId"]
        upload = values['audioUploadId']
        if upload is not None and (not isinstance(upload, str) or not upload or len(upload) > 100):
            raise APIError.validation_error('Invalid audio upload ID')
        if upload and values['kind'] != 'audio' or source and values['kind'] != 'video':
            raise APIError.validation_error('Media attachment does not match post kind')
        if source is not None and (not isinstance(source, str) or not source or len(source) > 100):
            raise APIError.validation_error("Invalid media source content ID")
        if values["kind"] == "text" and source:
            raise APIError.validation_error("Text posts cannot attach video media")
        return values

    def _media(self, conn, user, values, content_id, *, publish):
        if values['kind'] == 'audio':
            upload = conn.execute(
                'SELECT u.*,a.manifest_json FROM ugc_audio_uploads u JOIN media_assets a '
                'ON a.asset_id=u.asset_id WHERE u.upload_id=? AND u.user_id=? AND u.content_id=?',
                (values['audioUploadId'], user, content_id),
            ).fetchone() if values['audioUploadId'] else None
            if values['audioUploadId'] and upload is None:
                raise APIError.not_found('Audio upload not found')
            if publish and (upload is None or upload['status'] != 'ready'):
                raise APIError.conflict('Audio validation must finish before publication')
            return {'assetId': upload['asset_id'], 'track': json.loads(upload['manifest_json'])} \
                if upload else None
        source_id = values["mediaSourceContentId"]
        if not source_id:
            if publish and values["kind"] == "video":
                raise APIError.validation_error(
                    "Video publication requires completed platform media"
                )
            return None
        if source_id == content_id:
            raise APIError.validation_error("A post cannot reference itself")
        # A fixed ready asset is retained, never a client URL or a synthetic upstream ID.
        source = self.policy.require(user, source_id, action="play", conn=conn)
        if source["source"] == "ugc":
            raise APIError.validation_error(
                "Select an original library video to avoid reference cycles"
            )
        if source["assetStatus"] != "ready" or not source["assetId"]:
            raise APIError.validation_error("Media must be completely stored before attachment")
        if publish and values["visibility"] == "public" and source["visibility"] != "public":
            raise APIError.forbidden("Personal-access media cannot be published publicly")
        return source

    def command(self, user_id, operation, payload, key, content_id=None):
        if not isinstance(key, str) or not key or len(key) > 180:
            raise APIError.validation_error("Idempotency-Key is required")
        if operation not in {"create", "edit", "publish", "delete"}:
            raise APIError.validation_error("Unsupported post command")
        if not isinstance(payload, dict):
            raise APIError.validation_error("JSON object is required")
        signature = hashlib.sha256(encode([operation, content_id, payload]).encode()).hexdigest()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="ugc-command", key=encode([user_id, key]))
            previous = conn.execute(
                "SELECT * FROM ugc_commands WHERE user_id=? AND command_id=?", (user_id, key)
            ).fetchone()
            if previous:
                if previous["payload_hash"] != signature:
                    raise APIError.conflict("Idempotency-Key is bound to another post command")
                return json.loads(previous["result_json"])
            identifier = content_id or "ugc-" + uuid4().hex
            begin_write(conn, namespace="content", key=identifier)
            old = conn.execute(
                "SELECT * FROM ugc_posts WHERE content_id=?", (identifier,)
            ).fetchone()
            if operation != "create":
                if not old or old["author_id"] != user_id or old["status"] == "deleted":
                    raise APIError.not_found("Post not found")
                catalog = conn.execute(
                    "SELECT status FROM feed_content WHERE content_id=?", (identifier,)
                ).fetchone()
                if catalog[0] == "retired" and operation != "delete":
                    raise APIError.forbidden(
                        "Administratively retired posts require explicit administrator restoration"
                    )
                if (
                    type(payload.get("expectedVersion")) is not int
                    or payload["expectedVersion"] != old["version"]
                ):
                    raise APIError.conflict("Post version changed; reload before editing")
            values = self._validated(payload if operation in {"create", "edit"} else {}, old)
            if operation in {"publish", "delete"} and set(payload) != {"expectedVersion"}:
                raise APIError.validation_error("Only expectedVersion is accepted")
            state = "draft" if not old else old["status"]
            if operation == "publish":
                state = "published"
            elif operation == "delete":
                state = "deleted"
            try:
                media = (
                    self._media(conn, user_id, values, identifier, publish=state == "published")
                    if state != "deleted"
                    else None
                )
            except APIError as error:
                if (
                    operation != "edit"
                    or values["visibility"] != "private"
                    or not old
                    or old["media_source_content_id"] != values["mediaSourceContentId"]
                    or error.status_code not in {403, 404}
                ):
                    raise
                # Restricting visibility never needs a fresh grant to a revoked source.
                # Keep the existing reference; policy still rejects playback of that source.
                previous_metadata = conn.execute(
                    "SELECT metadata_json FROM feed_content WHERE content_id=?", (identifier,)
                ).fetchone()
                media = {
                    "assetId": old["media_asset_id"],
                    "track": json.loads(previous_metadata[0]),
                }
            now, version = time.time(), (old["version"] + 1 if old else 1)
            permission_changed = (
                not old
                or old["visibility"] != values["visibility"]
                or old["status"] != state
                or old["media_source_content_id"] != values["mediaSourceContentId"]
                or old['audio_upload_id'] != values['audioUploadId']
            )
            permission_version = (old["permission_version"] if old else 0) + int(permission_changed)
            published = (old["published_at"] if old else None) or (
                now if state == "published" else None
            )
            account = conn.execute(
                "SELECT display_name FROM app_users WHERE id=?", (user_id,)
            ).fetchone()
            owner_name = account[0] if account and account[0] else user_id
            metadata = {
                "source": "ugc",
                "contentId": identifier,
                "trackId": identifier,
                "title": values["title"],
                "description": values["body"],
                "owner": owner_name,
                "ownerId": user_id,
                "duration": media["track"].get("duration", 0) if media else 0,
                "cover": "",
                "typeName": "音乐" if values["kind"] in {'video', 'audio'} else "文章",
            }
            scope = (
                "public"
                if state == "published" and values["visibility"] == "public"
                else f"user:{user_id}"
            )
            catalog_status = {"published": "admitted", "draft": "draft", "deleted": "retired"}[
                state
            ]
            if old:
                conn.execute(
                    "UPDATE ugc_posts SET title=?,body=?,status=?,visibility=?,version=?,"
                    "permission_version=?,media_source_content_id=?,media_asset_id=?,"
                    "updated_at=?,published_at=? WHERE content_id=?",
                    (
                        values["title"],
                        values["body"],
                        state,
                        values["visibility"],
                        version,
                        permission_version,
                        values["mediaSourceContentId"],
                        media["assetId"] if media else None,
                        now,
                        published,
                        identifier,
                    ),
                )
                conn.execute(
                    "UPDATE feed_content SET scope=?,metadata_json=?,status=?,version=?,"
                    "permission_version=?,"
                    "admitted_at=? WHERE content_id=?",
                    (
                        scope,
                        encode(metadata),
                        catalog_status,
                        version,
                        permission_version,
                        published or now,
                        identifier,
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO feed_content(content_id,track_id,scope,owner_id,metadata_json,
                    status,version,permission_version,resource_kind,admitted_at)
                    VALUES (?,NULL,?,?,?,?,?,?,?,?)""",
                    (
                        identifier,
                        scope,
                        user_id,
                        encode(metadata),
                        catalog_status,
                        version,
                        permission_version,
                        values["kind"],
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO ugc_posts (content_id,author_id,kind,title,body,status,visibility,"
                    "version,permission_version,media_source_content_id,media_asset_id,created_at,"
                    "updated_at,published_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        identifier,
                        user_id,
                        values["kind"],
                        values["title"],
                        values["body"],
                        state,
                        values["visibility"],
                        version,
                        permission_version,
                        values["mediaSourceContentId"],
                        media["assetId"] if media else None,
                        now,
                        now,
                        published,
                    ),
                )
                conn.execute(
                    "INSERT INTO content_counters(content_id,updated_at) VALUES (?,?)",
                    (identifier, now),
                )
            conn.execute('UPDATE ugc_posts SET audio_upload_id=? WHERE content_id=?',
                         (values['audioUploadId'], identifier))
            conn.execute("DELETE FROM content_media_links WHERE content_id=?", (identifier,))
            if media:
                conn.execute(
                    "INSERT INTO content_media_links VALUES (?,?)", (identifier, media["assetId"])
                )
            if permission_changed:
                # Scope revocation is per resource; never retire a shared asset or another viewer.
                conn.execute(
                    "UPDATE media_playbacks SET active=0,expires_at=0 WHERE content_id=?",
                    (identifier,),
                )
            event_type = (
                "content.created"
                if not old
                else "content.deleted"
                if state == "deleted"
                else "content.visibility_changed"
                if permission_changed
                else "content.updated"
            )
            enqueue_event(
                conn,
                aggregate_type="content",
                aggregate_id=identifier,
                event_type=event_type,
                payload={
                    "contentId": identifier,
                    "version": version,
                    "permissionVersion": permission_version,
                },
                owner_id=user_id,
                visibility_revision=permission_version,
            )
            result = self._owned_dto(conn, user_id, identifier)
            conn.execute(
                "INSERT INTO ugc_commands VALUES (?,?,?,?,?,?,?)",
                (user_id, key, identifier, operation, signature, encode(result), now),
            )
        return result

    def _owned_dto(self, conn, user_id, content_id):
        row = conn.execute(
            "SELECT * FROM ugc_posts WHERE content_id=? AND author_id=?", (content_id, user_id)
        ).fetchone()
        if not row:
            raise APIError.not_found("Post not found")
        account = conn.execute(
            "SELECT display_name FROM app_users WHERE id=?", (user_id,)
        ).fetchone()
        return {
            "id": content_id,
            "contentId": content_id,
            "version": row["version"],
            "permissionVersion": row["permission_version"],
            "kind": row["kind"],
            "status": row["status"],
            "visibility": row["visibility"],
            "title": row["title"],
            "body": row["body"],
            "ownerId": user_id,
            "isOwner": True,
            "canEdit": True,
            "author": {"id": user_id, "name": account[0] if account and account[0] else user_id},
            "source": "ugc",
            "mediaSourceContentId": row["media_source_content_id"],
            "audioUploadId": row['audio_upload_id'],
            "assetId": row["media_asset_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "publishedAt": row["published_at"],
            "sharePath": f"/#/content/{content_id}"
            if row["status"] == "published" and row["visibility"] == "public"
            else None,
        }

    def detail(self, user_id, content_id):
        value = self.policy.require(user_id, content_id)
        if value["source"] != "ugc":
            raise APIError.not_found("Post not found")
        return value

    def own_posts(self, user_id):
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT content_id FROM ugc_posts WHERE author_id=? AND status<>'deleted' "
                "ORDER BY updated_at DESC LIMIT 100",
                (user_id,),
            ).fetchall()
            return {"items": [self._owned_dto(conn, user_id, row[0]) for row in rows]}

    def media_candidates(self, user_id):
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT c.content_id FROM feed_content c JOIN content_media_links l "
                "ON l.content_id=c.content_id "
                "JOIN media_assets a ON a.asset_id=l.asset_id WHERE c.status='admitted' "
                "AND c.resource_kind='video' AND a.status='ready' "
                "AND (c.scope='public' OR c.owner_id=?) "
                "ORDER BY c.admitted_at DESC LIMIT 100",
                (user_id,),
            ).fetchall()
        values = self.policy.resolve_many(user_id, [row[0] for row in rows], action="play")
        return {
            "items": [
                {key: value[key] for key in ("contentId", "title", "assetId", "visibility")}
                for value in values.values()
                if value["source"] != "ugc"
            ]
        }
