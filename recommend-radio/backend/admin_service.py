from __future__ import annotations

import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from database import DEFAULT_DB_PATH, begin_write, get_connection, init_db
from error_code import APIError

_RANGES = {
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "90d": timedelta(days=90),
}


class AdminService:
    def __init__(
        self,
        db_path: Optional[Path | str] = None,
        *,
        prometheus_url: Optional[str] = None,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.prometheus_url = (
            prometheus_url
            if prometheus_url is not None
            else os.getenv("PROMETHEUS_QUERY_URL", "")
        ).strip().rstrip("/")
        self._traffic_cache: dict[str, tuple[float, dict[str, Optional[float]]]] = {}
        self._traffic_cache_lock = threading.Lock()
        init_db(self.db_path)

    def summary(self, range_name: str = "7d") -> dict[str, Any]:
        normalized_range = range_name if range_name in _RANGES else "7d"
        now = datetime.now(timezone.utc)
        since = (now - _RANGES[normalized_range]).isoformat()
        idle_since = (
            now - timedelta(seconds=int(os.getenv("APP_SESSION_IDLE_SECONDS", str(24 * 60 * 60))))
        ).isoformat()

        with get_connection(self.db_path) as conn:
            users = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS enabled,
                    SUM(CASE WHEN role = 'admin' AND status = 'active' THEN 1 ELSE 0 END) AS admins,
                    SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS new_users
                FROM app_users
                """,
                (since,),
            ).fetchone()
            active_users = conn.execute(
                """
                SELECT COUNT(*)
                FROM app_users AS u
                WHERE u.status = 'active'
                  AND (
                    u.last_login_at >= ?
                    OR EXISTS (
                        SELECT 1 FROM playback_events AS pe
                        WHERE pe.user_id = u.id AND pe.created_at >= ?
                    )
                  )
                """,
                (since, since),
            ).fetchone()[0]
            linked_users = conn.execute(
                """
                SELECT COUNT(*) FROM bili_accounts
                WHERE cookie_encrypted IS NOT NULL AND cookie_encrypted <> ''
                """
            ).fetchone()[0]
            active_sessions = conn.execute(
                """
                SELECT COUNT(*)
                FROM app_sessions AS s
                JOIN app_users AS u ON u.id = s.user_id
                WHERE s.revoked_at IS NULL
                  AND s.expires_at > ?
                  AND s.last_seen_at > ?
                  AND u.status = 'active'
                """,
                (now.isoformat(), idle_since),
            ).fetchone()[0]
            playback = conn.execute(
                """
                SELECT
                    COUNT(*) AS plays,
                    COALESCE(SUM(skipped), 0) AS skips,
                    COALESCE(SUM(listen_ms), 0) AS listen_ms,
                    COALESCE(SUM(completed), 0) AS completed
                FROM playback_sessions
                WHERE started_at >= ?
                """,
                (since,),
            ).fetchone()

        traffic = self._traffic_summary(normalized_range)
        return {
            "range": normalized_range,
            "generatedAt": now.isoformat(),
            "monitoringUrl": os.getenv("GRAFANA_PUBLIC_URL", "").strip() or None,
            "users": {
                "total": int(users["total"] or 0),
                "enabled": int(users["enabled"] or 0),
                "active": int(active_users or 0),
                "newUsers": int(users["new_users"] or 0),
                "admins": int(users["admins"] or 0),
                "biliConnected": int(linked_users or 0),
                "activeSessions": int(active_sessions or 0),
            },
            "traffic": traffic,
            "playback": {
                "plays": int(playback["plays"] or 0),
                "skips": int(playback["skips"] or 0),
                "listenSeconds": int((playback["listen_ms"] or 0) // 1000),
                "completed": int(playback["completed"] or 0),
            },
        }

    def _traffic_summary(self, range_name: str) -> dict[str, Optional[float]]:
        empty = {"requests": None, "errorRate": None, "p95LatencyMs": None}
        parsed_url = urlparse(self.prometheus_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            return empty

        now = time.monotonic()
        with self._traffic_cache_lock:
            cached = self._traffic_cache.get(range_name)
            if cached and now - cached[0] < 30:
                return dict(cached[1])

        window = range_name
        total = f"sum(increase(bilibili_radio_http_requests_total[{window}]))"
        expressions = {
            "requests": total,
            "errorRate": (
                "sum(increase(bilibili_radio_http_requests_total{status=~\"5..\"}"
                f"[{window}])) / clamp_min({total}, 1)"
            ),
            "p95LatencyMs": (
                "histogram_quantile(0.95, sum by (le) "
                "(increase(bilibili_radio_http_request_duration_seconds_bucket"
                f"[{window}]))) * 1000"
            ),
        }
        values: dict[str, Optional[float]] = {}
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="prometheus-admin") as pool:
            futures = {
                name: pool.submit(self._prometheus_query_value, expression)
                for name, expression in expressions.items()
            }
            for name, future in futures.items():
                try:
                    values[name] = future.result()
                except Exception:
                    values[name] = None

        if values["requests"] is not None:
            values["requests"] = float(max(0, round(values["requests"])))
        if values["errorRate"] is not None:
            values["errorRate"] = max(0.0, min(1.0, values["errorRate"]))
        if values["p95LatencyMs"] is not None:
            values["p95LatencyMs"] = max(0.0, values["p95LatencyMs"])

        with self._traffic_cache_lock:
            self._traffic_cache[range_name] = (now, dict(values))
        return values

    def _prometheus_query_value(self, expression: str) -> Optional[float]:
        response = requests.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": expression},
            timeout=(0.5, 1.5),
        )
        response.raise_for_status()
        payload = response.json()
        results = ((payload.get("data") or {}).get("result") or []) if isinstance(payload, dict) else []
        if not results:
            return None
        raw_value = (results[0].get("value") or [None, None])[-1]
        value = float(raw_value)
        return value if math.isfinite(value) else None

    def monitoring_user_stats(self) -> tuple[int, dict[str, int]]:
        now = datetime.now(timezone.utc)
        thresholds = {
            "1d": (now - timedelta(days=1)).isoformat(),
            "7d": (now - timedelta(days=7)).isoformat(),
            "30d": (now - timedelta(days=30)).isoformat(),
        }
        with get_connection(self.db_path) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM app_users").fetchone()[0])
            active: dict[str, int] = {}
            for period, since in thresholds.items():
                active[period] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM app_users AS u
                        WHERE u.status = 'active'
                          AND (
                            u.last_login_at >= ?
                            OR EXISTS (
                                SELECT 1 FROM playback_events AS pe
                                WHERE pe.user_id = u.id AND pe.created_at >= ?
                            )
                          )
                        """,
                        (since, since),
                    ).fetchone()[0]
                )
        return total, active

    def list_users(self, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        safe_page = max(int(page), 1)
        safe_page_size = min(max(int(page_size), 1), 100)
        offset = (safe_page - 1) * safe_page_size
        with get_connection(self.db_path) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM app_users").fetchone()[0])
            rows = conn.execute(
                """
                SELECT u.*,
                       CASE WHEN b.cookie_encrypted IS NOT NULL AND b.cookie_encrypted <> ''
                            THEN 1 ELSE 0 END AS bili_connected
                FROM app_users AS u
                LEFT JOIN bili_accounts AS b ON b.user_id = u.id
                ORDER BY u.created_at ASC, u.id ASC
                LIMIT ? OFFSET ?
                """,
                (safe_page_size, offset),
            ).fetchall()

        return {
            "items": [
                {
                    "id": row["id"],
                    "displayName": row["display_name"],
                    "email": row["email"],
                    "avatarUrl": row["avatar_url"],
                    "role": row["role"],
                    "status": row["status"],
                    "roleSource": row["role_source"],
                    "biliConnected": bool(row["bili_connected"]),
                    "createdAt": row["created_at"],
                    "lastLoginAt": row["last_login_at"],
                }
                for row in rows
            ],
            "total": total,
            "page": safe_page,
            "pageSize": safe_page_size,
        }

    def set_role(
        self,
        user_id: str,
        role: str,
        *,
        actor_user_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        normalized_role = role.strip().lower()
        if normalized_role not in {"user", "admin"}:
            raise APIError.validation_error("role must be user or admin")
        now = datetime.now(timezone.utc).isoformat()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="admin-membership", key="active-administrators")
            target = conn.execute(
                "SELECT id, role, status FROM app_users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if target is None:
                raise APIError.not_found("User not found")
            if target["role"] == "admin" and normalized_role != "admin":
                admin_count = conn.execute(
                    "SELECT COUNT(*) FROM app_users WHERE role = 'admin' AND status = 'active'"
                ).fetchone()[0]
                if int(admin_count) <= 1:
                    raise APIError.conflict("Cannot remove the last active administrator")
            conn.execute(
                """
                UPDATE app_users
                SET role = ?, role_source = 'local', updated_at = ?
                WHERE id = ?
                """,
                (normalized_role, now, user_id),
            )
            if target["role"] != normalized_role:
                conn.execute(
                    "UPDATE app_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
                    (now, user_id),
                )
            if actor_user_id:
                self._insert_audit(
                    conn,
                    actor_user_id=actor_user_id,
                    action="user.role_updated",
                    target_id=user_id,
                    request_id=request_id,
                    details={"role": normalized_role},
                    created_at=now,
                )
            row = conn.execute("SELECT * FROM app_users WHERE id = ?", (user_id,)).fetchone()
        return {
            "id": row["id"],
            "displayName": row["display_name"],
            "role": row["role"],
            "status": row["status"],
            "roleSource": row["role_source"],
        }

    def toggle_owner_admin(
        self,
        user_id: str,
        *,
        actor_user_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        with get_connection(self.db_path) as conn:
            begin_write(conn, namespace="admin-membership", key="active-administrators")
            row = conn.execute(
                "SELECT id, role, status FROM app_users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None or row["status"] != "active":
                raise APIError.not_found("Active user not found")
            next_role = "user" if row["role"] == "admin" else "admin"
            conn.execute(
                """
                UPDATE app_users
                SET role = ?, role_source = 'local', updated_at = ?
                WHERE id = ?
                """,
                (next_role, now, user_id),
            )
            conn.execute(
                "UPDATE app_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
                (now, user_id),
            )
            if actor_user_id:
                self._insert_audit(
                    conn,
                    actor_user_id=actor_user_id,
                    action="owner.admin_toggled",
                    target_id=user_id,
                    request_id=request_id,
                    details={"role": next_role, "source": "admin/genshin"},
                    created_at=now,
                )
        return {"id": user_id, "role": next_role}

    @staticmethod
    def _insert_audit(
        conn,
        *,
        actor_user_id: str,
        action: str,
        target_id: str,
        request_id: Optional[str],
        details: dict[str, Any],
        created_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO admin_audit_log (
                actor_user_id, action, target_type, target_id,
                request_id, details_json, created_at
            ) VALUES (?, ?, 'app_user', ?, ?, ?, ?)
            """,
            (
                actor_user_id,
                action,
                target_id,
                request_id,
                json.dumps(details, ensure_ascii=False),
                created_at,
            ),
        )
