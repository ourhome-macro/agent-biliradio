"""Bounded maintenance; replicas share persisted sync/supply reservations."""

import threading
import time

from database import database_identity, get_connection

from .service import FollowingService
from .sync import start_sync

_last = {}
_lock = threading.Lock()


def periodic_tick(db_path):
    identity, now = database_identity(db_path), time.time()
    with _lock:
        if now - _last.get(identity, 0) < 60:
            return
        _last[identity] = now
    service = FollowingService(db_path)
    with get_connection(db_path) as conn:
        accounts = conn.execute("""SELECT b.user_id,MAX(s.updated_at) AS last_sync
            FROM bili_accounts b LEFT JOIN follow_sync_runs s ON s.user_id=b.user_id
            AND s.bili_account_mid=CAST(b.user_mid AS TEXT)
            WHERE b.cookie_encrypted IS NOT NULL AND b.user_mid IS NOT NULL
            GROUP BY b.user_id ORDER BY COALESCE(MAX(s.updated_at),0) LIMIT 100""").fetchall()
    for row in accounts:
        binding = service.current_binding(row["user_id"])
        if binding and (row["last_sync"] is None or row["last_sync"] < now - 900):
            start_sync(
                service, row["user_id"], f"periodic:{binding['binding_key']}:{int(now // 900)}"
            )
    from feed_runtime.reactions import feed_enabled

    if not feed_enabled():
        return
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """SELECT DISTINCT r.creator_id,COALESCE(s.next_check_at,0) AS due
            FROM creator_follow_relations r
            JOIN bili_binding_epochs e ON e.user_id=r.user_id AND e.binding_key=r.binding_key
            AND e.active=1 AND e.account_mid=r.bili_account_mid
            JOIN bili_accounts b ON b.user_id=r.user_id AND CAST(b.user_mid AS TEXT)=e.account_mid
            LEFT JOIN creator_supply s ON s.creator_id=r.creator_id
            WHERE r.desired_state=1 AND r.confirmed_state=1 AND r.sync_status='synced'
            AND b.cookie_encrypted IS NOT NULL
            AND (s.next_check_at IS NULL OR s.next_check_at<=?)
            ORDER BY due,r.creator_id LIMIT 100""",
            (now,),
        ).fetchall()
    creators = {row["creator_id"] for row in rows}
    if creators:
        from feed_runtime.following_feed import schedule_supply

        schedule_supply(db_path, creators, limit=100)
