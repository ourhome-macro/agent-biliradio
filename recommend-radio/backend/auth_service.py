from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import os
import secrets
import time
from ctypes import wintypes
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from constant import BilibiliAPI as APIConst
from constant import HttpHeader
from database import DEFAULT_DB_PATH, LEGACY_OWNER_USER_ID, get_connection, init_db
from error_code import APIError
from models import BiliUserProfile
from monitoring import record_bilibili_request
from track_service import normalize_user_profile

BILI_PROVIDER = "bilibili"
QR_EXPIRES_SECONDS = 180
QR_STATUS_MAP = {
    0: "confirmed",
    86038: "expired",
    86090: "scanned",
    86101: "waiting",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


class LocalCookieCipher:
    def __init__(self, key_path: Path):
        self.key_path = key_path

    def encrypt(self, value: str) -> str:
        raw = value.encode("utf-8")
        if os.name == "nt":
            try:
                return "dpapi:" + base64.b64encode(self._dpapi_protect(raw)).decode("ascii")
            except Exception:
                pass

        key = self._local_key()
        nonce = secrets.token_bytes(16)
        encrypted = self._xor(raw, key, nonce)
        tag = hmac.new(key, nonce + encrypted, hashlib.sha256).digest()
        return "local:" + base64.b64encode(nonce + tag + encrypted).decode("ascii")

    def decrypt(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if value.startswith("dpapi:"):
            raw = base64.b64decode(value[6:])
            return self._dpapi_unprotect(raw).decode("utf-8")
        if value.startswith("local:"):
            raw = base64.b64decode(value[6:])
            nonce = raw[:16]
            tag = raw[16:48]
            encrypted = raw[48:]
            key = self._local_key()
            expected = hmac.new(key, nonce + encrypted, hashlib.sha256).digest()
            if not hmac.compare_digest(tag, expected):
                raise APIError.api_error("Stored auth cookie failed integrity check")
            return self._xor(encrypted, key, nonce).decode("utf-8")
        return None

    def _local_key(self) -> bytes:
        if self.key_path.exists():
            return self.key_path.read_bytes()
        self.key_path.parent.mkdir(parents=True, exist_ok=True)
        key = secrets.token_bytes(32)
        self.key_path.write_bytes(key)
        return key

    @staticmethod
    def _xor(data: bytes, key: bytes, nonce: bytes) -> bytes:
        output = bytearray()
        counter = 0
        while len(output) < len(data):
            block = hmac.new(
                key,
                nonce + counter.to_bytes(8, "big"),
                hashlib.sha256,
            ).digest()
            output.extend(block)
            counter += 1
        return bytes(source ^ mask for source, mask in zip(data, output))

    @staticmethod
    def _dpapi_protect(data: bytes) -> bytes:
        in_blob, in_buffer = _blob_from_bytes(data)
        out_blob = DATA_BLOB()
        try:
            ok = ctypes.windll.crypt32.CryptProtectData(
                ctypes.byref(in_blob),
                None,
                None,
                None,
                None,
                CRYPTPROTECT_UI_FORBIDDEN,
                ctypes.byref(out_blob),
            )
            if not ok:
                raise ctypes.WinError()
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            del in_buffer
            if out_blob.pbData:
                ctypes.windll.kernel32.LocalFree(out_blob.pbData)

    @staticmethod
    def _dpapi_unprotect(data: bytes) -> bytes:
        in_blob, in_buffer = _blob_from_bytes(data)
        out_blob = DATA_BLOB()
        try:
            ok = ctypes.windll.crypt32.CryptUnprotectData(
                ctypes.byref(in_blob),
                None,
                None,
                None,
                None,
                CRYPTPROTECT_UI_FORBIDDEN,
                ctypes.byref(out_blob),
            )
            if not ok:
                raise ctypes.WinError()
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            del in_buffer
            if out_blob.pbData:
                ctypes.windll.kernel32.LocalFree(out_blob.pbData)


class DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


CRYPTPROTECT_UI_FORBIDDEN = 0x01


def _blob_from_bytes(data: bytes) -> tuple[DATA_BLOB, Any]:
    buffer = ctypes.create_string_buffer(data)
    blob = DATA_BLOB(
        len(data),
        ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)),
    )
    return blob, buffer


class AuthService:
    def __init__(
        self,
        db_path: Optional[Path | str] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 10,
        user_id: str = LEGACY_OWNER_USER_ID,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.timeout = timeout
        self.user_id = user_id
        self.session = session or requests.Session()
        self.session.headers.update(HttpHeader.default_headers())
        init_db(self.db_path)

        from database import mysql_target
        configured_key = os.getenv("AUTH_KEY_PATH", "").strip()
        key_path = Path(configured_key) if configured_key else (
            DEFAULT_DB_PATH if mysql_target(self.db_path) else Path(self.db_path)
        ).with_suffix(".auth.key")
        self.cipher = LocalCookieCipher(key_path)

    def qr_login_enabled(self) -> bool:
        return True

    def create_qrcode(self) -> dict[str, Any]:
        response = self._get(APIConst.QR_GENERATE_URL, "QR code")
        payload = self._json_payload(response, "QR code")
        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili QR code failed")

        data = payload.get("data") or {}
        qrcode_key = str(data.get("qrcode_key") or "").strip()
        url = str(data.get("url") or "").strip()
        if not qrcode_key or not url:
            raise APIError.api_error("Bilibili QR code response missing qrcode_key or url")

        now = utc_now()
        expires_at = (
            datetime.now(timezone.utc).astimezone() + timedelta(seconds=QR_EXPIRES_SECONDS)
        ).isoformat()
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO auth_qr_sessions (
                    user_id, qrcode_key, url, status, message,
                    created_at, updated_at, expires_at
                )
                VALUES (?, ?, ?, 'waiting', NULL, ?, ?, ?)
                ON CONFLICT(user_id, qrcode_key) DO UPDATE SET
                    url = excluded.url,
                    status = excluded.status,
                    message = excluded.message,
                    updated_at = excluded.updated_at,
                    expires_at = excluded.expires_at
                """,
                (self.user_id, qrcode_key, url, now, now, expires_at),
            )

        return {
            "qrcodeKey": qrcode_key,
            "url": url,
            "expiresAt": expires_at,
            "pollIntervalMs": 2000,
        }

    def poll_qrcode(self, qrcode_key: str) -> dict[str, Any]:
        qrcode_key = (qrcode_key or "").strip()
        if not qrcode_key:
            raise APIError.validation_error("qrcodeKey is required")

        with get_connection(self.db_path) as conn:
            owned_qr = conn.execute(
                "SELECT 1 FROM auth_qr_sessions WHERE user_id = ? AND qrcode_key = ?",
                (self.user_id, qrcode_key),
            ).fetchone()
        if not owned_qr:
            raise APIError.not_found("QR code session not found")

        response = self._get(
            APIConst.QR_POLL_URL,
            "QR code poll",
            params={"qrcode_key": qrcode_key},
        )
        payload = self._json_payload(response, "QR code poll")
        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili QR poll failed")

        data = payload.get("data") or {}
        bili_code = int(data.get("code") or 0)
        status = QR_STATUS_MAP.get(bili_code, "unknown")
        message = str(data.get("message") or payload.get("message") or "")
        user = None

        if status == "confirmed":
            cookie_header = self._cookie_header_from_response(response)
            if not cookie_header:
                raise APIError.api_error("Bilibili QR poll succeeded without Set-Cookie")
            refresh_token = data.get("refresh_token")
            user = self._refresh_profile(cookie_header=cookie_header)
            self._save_auth(cookie_header, refresh_token, user)

        self._save_qr_status(qrcode_key, status, message)
        return {
            "qrcodeKey": qrcode_key,
            "status": status,
            "code": bili_code,
            "message": message,
            "isLoggedIn": status == "confirmed",
            "user": user.to_dict() if user else self.get_status().get("user"),
        }

    def get_status(self, refresh: bool = False) -> dict[str, Any]:
        row = self._auth_row()
        cookie_header = self.cipher.decrypt(row["cookie_encrypted"]) if row else None
        user = None
        if cookie_header and refresh:
            user = self._refresh_profile(cookie_header=cookie_header)
            self._save_auth(cookie_header, self._decrypt_refresh_token(row), user,
                            expected_cookie=row["cookie_encrypted"])
        elif row and row["user_mid"]:
            user = BiliUserProfile(
                mid=int(row["user_mid"]),
                name=row["user_name"] or "",
                face=row["user_face"] or "",
            )

        return {
            "qrLoginEnabled": self.qr_login_enabled(),
            "isLoggedIn": bool(cookie_header),
            "user": user.to_dict() if user else None,
            "cookieUpdatedAt": row["cookie_updated_at"] if row else None,
        }

    def get_profile(self, refresh: bool = True) -> dict[str, Any]:
        row = self._auth_row()
        cookie_header = self.cipher.decrypt(row["cookie_encrypted"]) if row else None
        if not cookie_header:
            raise APIError.auth_required("Bilibili login is required")
        user = self._refresh_profile(cookie_header=cookie_header) if refresh else None
        if user:
            self._save_auth(cookie_header, self._decrypt_refresh_token(row), user,
                            expected_cookie=row["cookie_encrypted"])
            return user.to_dict()
        status = self.get_status(refresh=False)
        if not status["user"]:
            raise APIError.auth_required("Bilibili login is required")
        return status["user"]

    def get_cookie_header(self) -> Optional[str]:
        row = self._auth_row()
        if not row:
            return None
        return self.cipher.decrypt(row["cookie_encrypted"])

    def logout(self) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            from database import begin_write
            begin_write(conn, namespace="bili-binding", key=self.user_id)
            conn.execute("UPDATE bili_binding_epochs SET active=0,generation=generation+1,updated_at=? WHERE user_id=?",
                         (time.time(), self.user_id))
            cursor = conn.execute(
                "DELETE FROM bili_accounts WHERE user_id = ? AND provider = ?",
                (self.user_id, BILI_PROVIDER),
            )
        return {"loggedOut": cursor.rowcount > 0}

    def _refresh_profile(self, cookie_header: str) -> BiliUserProfile:
        headers = {
            **HttpHeader.default_headers(),
            "Cookie": cookie_header,
        }
        response = self._get(APIConst.NAV_URL, "Bilibili nav", headers=headers)
        payload = self._json_payload(response, "Bilibili nav")
        data = payload.get("data") or {}
        if payload.get("code") != 0 or not data.get("isLogin"):
            raise APIError.auth_required(payload.get("message") or "Bilibili login is required")
        return normalize_user_profile(data)

    def _save_auth(
        self,
        cookie_header: str,
        refresh_token: Optional[str],
        user: BiliUserProfile,
        *, expected_cookie: str | None = None,
    ) -> None:
        now = utc_now()
        encrypted_cookie = self.cipher.encrypt(cookie_header)
        encrypted_refresh_token = self.cipher.encrypt(str(refresh_token)) if refresh_token else None
        with get_connection(self.db_path) as conn:
            from database import begin_write
            begin_write(conn, namespace="bili-binding", key=self.user_id)
            if expected_cookie is not None:
                current = conn.execute("SELECT cookie_encrypted,user_mid FROM bili_accounts WHERE user_id=?", (self.user_id,)).fetchone()
                if not current or current[0] != expected_cookie:
                    if current and str(current[1]) == str(user.mid):
                        return  # A newer credential for the same account wins over this refresh.
                    raise APIError.conflict("Bilibili account changed during profile refresh")
            binding = conn.execute("SELECT * FROM bili_binding_epochs WHERE user_id=?", (self.user_id,)).fetchone()
            if binding is None:
                conn.execute("INSERT INTO bili_binding_epochs(user_id,account_mid,generation,active,updated_at) VALUES (?,?,1,1,?)",
                             (self.user_id, str(user.mid), time.time()))
            elif not binding["active"] or binding["account_mid"] != str(user.mid):
                conn.execute("UPDATE bili_binding_epochs SET account_mid=?,generation=generation+1,active=1,updated_at=? WHERE user_id=?",
                             (str(user.mid), time.time(), self.user_id))
            import json
            epoch = conn.execute("SELECT generation FROM bili_binding_epochs WHERE user_id=?", (self.user_id,)).fetchone()[0]
            binding_key = hashlib.sha256(json.dumps([self.user_id, str(user.mid), epoch],
                ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            conn.execute("UPDATE bili_binding_epochs SET binding_key=? WHERE user_id=?",
                         (binding_key, self.user_id))
            conn.execute(
                """
                INSERT INTO bili_accounts (
                    user_id, provider, cookie_encrypted, refresh_token_encrypted, user_mid,
                    user_name, user_face, cookie_updated_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    cookie_encrypted = excluded.cookie_encrypted,
                    refresh_token_encrypted = COALESCE(
                        excluded.refresh_token_encrypted,
                        bili_accounts.refresh_token_encrypted
                    ),
                    user_mid = excluded.user_mid,
                    user_name = excluded.user_name,
                    user_face = excluded.user_face,
                    cookie_updated_at = excluded.cookie_updated_at,
                    updated_at = excluded.updated_at
                """,
                (
                    self.user_id,
                    BILI_PROVIDER,
                    encrypted_cookie,
                    encrypted_refresh_token,
                    user.mid,
                    user.name,
                    user.face,
                    now,
                    now,
                ),
            )

    def _save_qr_status(self, qrcode_key: str, status: str, message: str) -> None:
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE auth_qr_sessions
                SET status = ?, message = ?, updated_at = ?
                WHERE user_id = ? AND qrcode_key = ?
                """,
                (status, message, utc_now(), self.user_id, qrcode_key),
            )

    def _auth_row(self):
        with get_connection(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM bili_accounts WHERE user_id = ? AND provider = ?",
                (self.user_id, BILI_PROVIDER),
            ).fetchone()

    def _decrypt_refresh_token(self, row: Any) -> Optional[str]:
        if not row:
            return None
        return self.cipher.decrypt(row["refresh_token_encrypted"])

    def _get(
        self,
        url: str,
        context: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> requests.Response:
        started_at = time.perf_counter()
        outcome = "success"
        operation = "auth_qr" if context.startswith("QR code") else "auth"
        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers or HttpHeader.default_headers(),
                timeout=self.timeout,
            )
            if response.status_code in {412, 429}:
                outcome = "rate_limited"
            elif response.status_code in {401, 403}:
                outcome = "auth_error"
            elif response.status_code >= 400:
                outcome = "upstream_error"
            response.raise_for_status()
            return response
        except requests.Timeout:
            outcome = "timeout"
            raise APIError.request_timeout(context)
        except requests.RequestException as exc:
            outcome = "upstream_error"
            raise APIError.network_error(str(exc))
        finally:
            record_bilibili_request(operation, outcome, time.perf_counter() - started_at)

    @staticmethod
    def _json_payload(response: requests.Response, context: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            raise APIError.api_error(f"Bilibili {context} returned non-JSON response")
        if not isinstance(payload, dict):
            raise APIError.api_error(f"Bilibili {context} returned invalid JSON payload")
        return payload

    @staticmethod
    def _cookie_header_from_response(response: requests.Response) -> str:
        cookies = getattr(response, "cookies", None)
        if not cookies:
            return ""
        return "; ".join(f"{cookie.name}={cookie.value}" for cookie in cookies)
