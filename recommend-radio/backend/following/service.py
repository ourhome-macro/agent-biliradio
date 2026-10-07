from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager

from auth_service import AuthService
from bili_client import BiliClient
from database import begin_write, get_connection
from durable_jobs import enqueue
from error_code import APIError
from feed_runtime.repository import FeedRepository, encode, identity


def write(conn, namespace, key):
    begin_write(conn, namespace=namespace, key=key)


class FollowingService:
    def __init__(self, db_path, *, auth_factory=AuthService, client_factory=BiliClient):
        self.db_path, self.auth_factory, self.client_factory = db_path, auth_factory, client_factory

    @staticmethod
    def binding(row, epoch=None):
        if not row or not row["cookie_encrypted"] or not row["user_mid"]:
            return None
        if epoch and (not epoch["active"] or epoch["account_mid"] != str(row["user_mid"])):
            return None
        key = hashlib.sha256(
            encode(
                [row["user_id"], str(row["user_mid"]), epoch["generation"] if epoch else 1]
            ).encode()
        ).hexdigest()
        return {"mid": str(row["user_mid"]), "binding_key": key}

    def current_binding(self, user_id, conn=None):
        if conn is None:
            with get_connection(self.db_path) as connection:
                return self.current_binding(user_id, connection)
        epoch = conn.execute(
            "SELECT * FROM bili_binding_epochs WHERE user_id=?", (user_id,)
        ).fetchone()
        return self.binding(
            conn.execute(
                "SELECT * FROM bili_accounts WHERE user_id=? AND provider='bilibili'", (user_id,)
            ).fetchone(),
            epoch,
        )

    def require_binding(self, user_id, expected=None, conn=None):
        binding = self.current_binding(user_id, conn)
        if not binding:
            raise APIError.forbidden("请先绑定 B 站账号")
        if expected and binding != expected:
            raise APIError.conflict("Bilibili account binding changed")
        return binding

    @contextmanager
    def client(self, user_id, binding):
        auth = self.auth_factory(self.db_path, user_id=user_id)

        def cookie():
            self.require_binding(user_id, binding)
            value = auth.get_cookie_header()
            # Recheck after credential resolution so a switched binding cannot supply new cookies.
            self.require_binding(user_id, binding)
            return value

        client = self.client_factory(cookie_provider=cookie, timeout=8)
        try:
            yield client
        finally:
            for name in ("session", "auth_session"):
                session = getattr(client, name, None)
                if session:
                    session.close()

    @staticmethod
    def creator(conn, mid, name="", avatar=""):
        mid = str(mid)
        if not mid.isdecimal() or int(mid) <= 0:
            raise APIError.validation_error("Invalid trusted creator identity")
        creator_id = identity("creator", "bili", mid)
        write(conn, "creator-identity", creator_id)
        existing = conn.execute(
            "SELECT * FROM creators WHERE creator_id=?", (creator_id,)
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO creators VALUES (?,?,?,?,?,?)",
                (creator_id, "bili", mid, str(name)[:300], str(avatar)[:2000], time.time()),
            )
        elif name:
            conn.execute(
                "UPDATE creators SET display_name=?,avatar=?,verified_at=? WHERE creator_id=?",
                (str(name)[:300], str(avatar)[:2000], time.time(), creator_id),
            )
        return creator_id

    def content_creator(self, user_id, content_id):
        repo = FeedRepository(self.db_path)
        content = repo.content(content_id, user_id)
        metadata = json.loads(content["metadata_json"])
        # Explicit origin is required; storage location and client-provided MID are irrelevant.
        if metadata.get("source") != "bili":
            return {
                "supported": False,
                "provider": metadata.get("source") or "unknown",
                "creatorId": None,
                "following": False,
                "confirmedFollowing": None,
                "relationVersion": 0,
                "syncStatus": "unsupported",
                "accountMid": None,
            }
        with get_connection(self.db_path) as conn:
            linked = conn.execute(
                "SELECT creator_id FROM creator_content WHERE content_id=?", (content_id,)
            ).fetchone()
        if linked:
            return self.state(user_id, linked[0])
        # Verify source attribution from the upstream video record, never a request body.
        client = self.client_factory(timeout=8)
        try:
            source = client.get_video_detail(metadata["bvid"]).info
        finally:
            for name in ("session", "auth_session"):
                value = getattr(client, name, None)
                if value:
                    value.close()
        if not source.owner_mid:
            raise APIError.not_found("Video has no verified creator")
        with get_connection(self.db_path) as conn:
            write(conn, "creator-identity", str(source.owner_mid))
            repo.authorize(
                conn.execute(
                    "SELECT * FROM feed_content WHERE content_id=?", (content_id,)
                ).fetchone(),
                user_id,
            )
            creator_id = self.creator(conn, source.owner_mid, source.owner)
            if not conn.execute(
                "SELECT 1 FROM creator_content WHERE creator_id=? AND content_id=?",
                (creator_id, content_id),
            ).fetchone():
                conn.execute(
                    "INSERT INTO creator_content VALUES (?,?,?,?)",
                    (creator_id, content_id, 0, time.time()),
                )
        return self.state(user_id, creator_id)

    def state(self, user_id, creator_id, conn=None):
        if conn is None:
            with get_connection(self.db_path) as connection:
                return self.state(user_id, creator_id, connection)
        creator = conn.execute(
            "SELECT * FROM creators WHERE creator_id=?", (creator_id,)
        ).fetchone()
        if not creator:
            raise APIError.not_found("Creator not found")
        binding = self.current_binding(user_id, conn)
        row = (
            conn.execute(
                "SELECT * FROM creator_follow_relations WHERE user_id=? "
                "AND bili_account_mid=? AND creator_id=? AND binding_key=?",
                (user_id, binding["mid"], creator_id, binding["binding_key"]),
            ).fetchone()
            if binding
            else None
        )
        return {
            "creatorId": creator_id,
            "provider": creator["provider"],
            "externalId": creator["external_id"],
            "name": creator["display_name"],
            "supported": creator["provider"] == "bili",
            "following": bool(row["desired_state"]) if row else False,
            "confirmedFollowing": bool(row["confirmed_state"])
            if row and row["confirmed_state"] is not None
            else None,
            "syncStatus": row["sync_status"] if row else "unbound" if not binding else "unknown",
            "relationVersion": row["version"] if row else 0,
            "accountMid": binding["mid"] if binding else None,
            "errorType": row["error_type"] if row else None,
        }

    def author_creator(self, user_id, mid):
        if type(mid) is not int or mid <= 0:
            raise APIError.validation_error("Invalid creator MID")
        creator_id = identity("creator", "bili", str(mid))
        with get_connection(self.db_path) as conn:
            existing = conn.execute(
                "SELECT 1 FROM creators WHERE creator_id=?", (creator_id,)
            ).fetchone()
        if not existing:
            client = self.client_factory(timeout=8)
            try:
                profile = client.get_user_profile(mid)
            finally:
                for name in ("session", "auth_session"):
                    session = getattr(client, name, None)
                    if session:
                        session.close()
            if str(profile.get("mid")) != str(mid):
                raise APIError.not_found("Creator identity was not verified")
            with get_connection(self.db_path) as conn:
                self.creator(conn, mid, profile.get("name", ""), profile.get("face", ""))
        return self.state(user_id, creator_id)

    def set(self, user_id, creator_id, desired, key, *, expected_version):
        if type(desired) is not bool or type(expected_version) is not int or expected_version < 0:
            raise APIError.validation_error("following and expectedVersion are required")
        if not isinstance(key, str) or not key or len(key) > 180:
            raise APIError.validation_error("A bounded Idempotency-Key is required")
        now = time.time()
        with get_connection(self.db_path) as conn:
            write(conn, "follow-command", f"{user_id}:{key}")
            binding = self.require_binding(user_id, conn=conn)
            write(conn, "follow-relation", encode([user_id, binding["mid"], creator_id]))
            creator = conn.execute(
                "SELECT * FROM creators WHERE creator_id=?", (creator_id,)
            ).fetchone()
            if not creator:
                raise APIError.not_found("Creator not found")
            if creator["provider"] != "bili":
                raise APIError.validation_error(
                    "This creator source does not support Bilibili following"
                )
            if creator["external_id"] == binding["mid"]:
                raise APIError.validation_error("Cannot follow the current Bilibili account")
            old_command = conn.execute(
                "SELECT * FROM creator_follow_commands WHERE user_id=? AND command_id=?",
                (user_id, key),
            ).fetchone()
            if old_command:
                if (
                    old_command["creator_id"] != creator_id
                    or bool(old_command["desired_state"]) != desired
                    or old_command["binding_key"] != binding["binding_key"]
                ):
                    raise APIError.conflict("Idempotency-Key belongs to another follow command")
                return json.loads(old_command["result_json"])
            current = self.state(user_id, creator_id, conn)
            if current["relationVersion"] != expected_version:
                raise APIError.conflict("Follow relation version changed")
            if (
                current["syncStatus"] == "synced"
                and current["following"] == desired
                and current["confirmedFollowing"] == desired
            ):
                conn.execute(
                    "INSERT INTO creator_follow_commands VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        user_id,
                        key,
                        creator_id,
                        binding["mid"],
                        binding["binding_key"],
                        int(desired),
                        current["relationVersion"],
                        encode(current),
                        now,
                    ),
                )
                return current
            old = conn.execute(
                "SELECT * FROM creator_follow_relations WHERE user_id=? AND bili_account_mid=? "
                "AND creator_id=?",
                (user_id, binding["mid"], creator_id),
            ).fetchone()
            version = (old["version"] if old else 0) + 1
            confirmed = current["confirmedFollowing"]
            if old:
                conn.execute(
                    "UPDATE creator_follow_relations SET "
                    "binding_key=?,desired_state=?,confirmed_state=?,"
                    "sync_status='pending',version=?,error_type=NULL,updated_at=? "
                    "WHERE user_id=? AND bili_account_mid=? AND creator_id=?",
                    (
                        binding["binding_key"],
                        int(desired),
                        int(confirmed) if confirmed is not None else None,
                        version,
                        now,
                        user_id,
                        binding["mid"],
                        creator_id,
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO creator_follow_relations
                    (user_id,bili_account_mid,creator_id,binding_key,desired_state,confirmed_state,
                    sync_status,version,updated_at) VALUES (?,?,?,?,?,NULL,'pending',?,?)""",
                    (
                        user_id,
                        binding["mid"],
                        creator_id,
                        binding["binding_key"],
                        int(desired),
                        version,
                        now,
                    ),
                )
            payload = {
                "creator_id": creator_id,
                "account_mid": binding["mid"],
                "binding_key": binding["binding_key"],
                "version": version,
            }
            enqueue(
                conn,
                kind="creator_follow",
                user_id=user_id,
                payload=payload,
                retry_safe=True,
                job_id=f"follow:{identity('follow-command', user_id, key)}",
            )
            result = self.state(user_id, creator_id, conn)
            conn.execute(
                "INSERT INTO creator_follow_commands VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    user_id,
                    key,
                    creator_id,
                    binding["mid"],
                    binding["binding_key"],
                    int(desired),
                    version,
                    encode(result),
                    now,
                ),
            )
        return result

    def snapshot(self, user_id):
        with get_connection(self.db_path) as conn:
            binding = self.current_binding(user_id, conn)
            if not binding:
                return {
                    "accountMid": None,
                    "bindingKey": None,
                    "creatorIds": [],
                    "version": "unbound",
                    "lastSyncedAt": None,
                    "syncStatus": "unbound",
                    "reason": "bilibili_login_required",
                }
            rows = conn.execute(
                "SELECT creator_id,desired_state,confirmed_state,sync_status,version "
                "FROM creator_follow_relations WHERE user_id=? AND bili_account_mid=? AND "
                "binding_key=? ORDER BY creator_id",
                (user_id, binding["mid"], binding["binding_key"]),
            ).fetchall()
            sync = conn.execute(
                "SELECT status,updated_at,error_type FROM follow_sync_runs WHERE user_id=? AND "
                "bili_account_mid=? "
                "AND binding_key=? ORDER BY created_at DESC LIMIT 1",
                (user_id, binding["mid"], binding["binding_key"]),
            ).fetchone()
            last_success = conn.execute(
                "SELECT MAX(updated_at) FROM follow_sync_runs WHERE user_id=? AND "
                "bili_account_mid=? "
                "AND binding_key=? AND status='completed'",
                (user_id, binding["mid"], binding["binding_key"]),
            ).fetchone()[0]
        active = [
            row["creator_id"]
            for row in rows
            if row["desired_state"] and row["confirmed_state"] and row["sync_status"] == "synced"
        ]
        version = hashlib.sha256(
            encode([binding, [dict(row) for row in rows]]).encode()
        ).hexdigest()
        return {
            "accountMid": binding["mid"],
            "bindingKey": binding["binding_key"],
            "creatorIds": active,
            "version": version,
            "lastSyncedAt": last_success,
            "syncStatus": sync["status"] if sync else "idle",
            "errorType": sync["error_type"] if sync else None,
            "reason": ""
            if active
            else "syncing"
            if (sync and sync["status"] in {"queued", "running"})
            or any(row["sync_status"] in {"pending", "unknown"} for row in rows)
            else "login_required"
            if sync and sync["error_type"] == "BilibiliAuthExpired"
            else "upstream_error"
            if sync and sync["status"] in {"failed", "partial"}
            else "no_following",
        }

    def list(self, user_id):
        snapshot = self.snapshot(user_id)
        if not snapshot["accountMid"]:
            return {
                "items": [],
                **snapshot,
                "relationRevision": snapshot["version"],
                "sync": {"status": "unbound"},
            }
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT creator_id FROM creator_follow_relations WHERE user_id=? AND "
                "bili_account_mid=? "
                "AND binding_key=? AND (desired_state=1 OR confirmed_state=1 OR "
                "sync_status<>'synced') ORDER BY updated_at DESC LIMIT 1000",
                (user_id, snapshot["accountMid"], snapshot["bindingKey"]),
            ).fetchall()
            items = [self.state(user_id, row[0], conn) for row in rows]
        return {
            "items": items,
            "accountMid": snapshot["accountMid"],
            "relationRevision": snapshot["version"],
            "sync": {
                "status": snapshot["syncStatus"],
                "lastSyncedAt": snapshot["lastSyncedAt"],
                "errorType": snapshot.get("errorType"),
            },
        }
