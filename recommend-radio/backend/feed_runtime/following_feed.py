"""Author timeline: shared public supply, account-scoped membership, fixed pages."""

from __future__ import annotations

import json
import math
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

from database import begin_write, get_connection
from durable_jobs import enqueue
from error_code import APIError, ErrorCode

from .repository import FeedRepository, encode


def following_snapshot(db_path, user_id):
    from following.service import FollowingService

    result = FollowingService(db_path).snapshot(user_id)
    result["watermark"] = time.time()
    return result


def schedule_supply(db_path, creator_ids, *, limit=20):
    """Reserve each author once across users; a failed transport is recoverable."""
    if not creator_ids:
        return 0
    now = time.time()
    scheduled = 0
    # Order due creators first so a large follow set doesn't starve behind its head.
    with get_connection(db_path) as conn:
        ids = sorted(set(creator_ids))
        states = {}
        for offset in range(0, len(ids), 500):
            chunk = ids[offset : offset + 500]
            states.update(
                {
                    r["creator_id"]: dict(r)
                    for r in conn.execute(
                        "SELECT * FROM creator_supply WHERE creator_id IN ("
                        + ",".join("?" for _ in chunk)
                        + ")",
                        tuple(chunk),
                    )
                }
            )
    due = sorted(set(creator_ids), key=lambda c: (states.get(c, {}).get("next_check_at", 0), c))
    for creator_id in due:
        if scheduled >= limit:
            break
        with get_connection(db_path) as conn:
            begin_write(conn, namespace="creator-supply", key=creator_id)
            creator = conn.execute(
                "SELECT provider FROM creators WHERE creator_id=?", (creator_id,)
            ).fetchone()
            if not creator or creator[0] != "bili":
                continue
            row = conn.execute(
                "SELECT * FROM creator_supply WHERE creator_id=?", (creator_id,)
            ).fetchone()
            if row and row["next_check_at"] > now:
                continue
            if row and row["status"] in {"queued", "running"} and row["lease_until"] > now:
                continue
            version = (row["version"] if row else 0) + 1
            if row:
                conn.execute(
                    "UPDATE creator_supply SET status='queued',version=?,lease_token=NULL,"
                    "lease_until=?,next_check_at=? WHERE creator_id=?",
                    (version, now + 300, now + 300, creator_id),
                )
            else:
                conn.execute(
                    "INSERT INTO creator_supply"
                    "(creator_id,status,version,lease_until,next_check_at) "
                    "VALUES (?,'queued',?,?,?)",
                    (creator_id, version, now + 300, now + 300),
                )
            enqueue(
                conn,
                kind="creator_supply",
                user_id="creator-supply",
                job_id=f"creator-supply:{creator_id}:{version}",
                lane=f"creator-supply:{creator_id}",
                payload={"creator_id": creator_id, "version": version},
                retry_safe=True,
            )
            scheduled += 1
    return scheduled


def published_timestamp(value):
    if isinstance(value, (int, float)):
        return max(0.0, float(value)) if math.isfinite(value) else 0.0
    try:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.replace(tzinfo=value.tzinfo or UTC).timestamp()
    except (TypeError, ValueError, OverflowError):
        return 0.0


def index_content(conn, creator_id, content_id, published_at, *, now=None):
    """Stable source identity; overlapping scans never duplicate an item."""
    now = time.time() if now is None else now
    row = conn.execute(
        "SELECT published_at FROM creator_content WHERE creator_id=? AND content_id=?",
        (creator_id, content_id),
    ).fetchone()
    if row:
        if row["published_at"] != published_at:
            conn.execute(
                "UPDATE creator_content SET published_at=?,indexed_at=? "
                "WHERE creator_id=? AND content_id=?",
                (published_at, now, creator_id, content_id),
            )
    else:
        conn.execute(
            "INSERT INTO creator_content(creator_id,content_id,published_at,indexed_at) "
            "VALUES (?,?,?,?)",
            (creator_id, content_id, published_at, now),
        )


def refresh_creator(db_path, creator_id, *, version=None, client_factory=None):
    from bili_client import BiliClient
    from candidate_pool import infer_facets
    from models import Track

    token, now = uuid4().hex, time.time()
    with get_connection(db_path) as conn:
        begin_write(conn, namespace="creator-supply", key=creator_id)
        creator = conn.execute(
            "SELECT * FROM creators WHERE creator_id=?", (creator_id,)
        ).fetchone()
        state = conn.execute(
            "SELECT * FROM creator_supply WHERE creator_id=?", (creator_id,)
        ).fetchone()
        if not creator or creator["provider"] != "bili" or not state:
            return {"status": "unsupported"}
        if version is not None and state["version"] != version:
            return {"status": "superseded"}
        if state["status"] == "running" and state["lease_until"] > now:
            return {"status": "owned"}
        if state["status"] == "ready":
            return {"status": "ready"}
        conn.execute(
            "UPDATE creator_supply SET status='running',lease_token=?,lease_until=? "
            "WHERE creator_id=?",
            (token, now + 240, creator_id),
        )
        mid = int(creator["external_id"])
        next_page = max(1, int(state["next_page"]))
    client, imported, complete = None, 0, False

    def fence(conn):
        stamp = time.time()
        changed = conn.execute(
            "UPDATE creator_supply SET lease_until=? WHERE creator_id=? AND lease_token=? "
            "AND status='running' AND lease_until>?",
            (stamp + 240, creator_id, token, stamp),
        ).rowcount
        if not changed:
            raise RuntimeError("CreatorSupplyLeaseLost")

    try:
        # Public author supply always uses a visitor, never a user's private credentials.
        client = (client_factory or BiliClient)(cookie_provider=lambda: None)
        repo = FeedRepository(db_path)
        # Reserve one head page on every attempt while allowing the durable backlog
        # cursor to progress. A cap of one could otherwise starve every older page.
        max_pages = min(10, max(2, int(os.getenv("FOLLOWING_AUTHOR_MAX_PAGES", "3"))))
        backlog = max(2, next_page)
        pages = [1, *range(backlog, backlog + max_pages - 1)]
        for page in pages:
            with get_connection(db_path) as conn:
                fence(conn)
            result = client.list_user_tracks(mid, page=page, page_size=50, order="pubdate")
            if not isinstance(result, dict) or not isinstance(result.get("tracks"), list):
                raise ValueError("InvalidCreatorPage")
            for payload in result["tracks"]:
                with get_connection(db_path) as conn:
                    fence(conn)
                track = Track.from_dict(payload)
                if track.source != "bili" or track.owner_mid != mid:
                    continue
                facets, evidence = infer_facets(track, query="")
                content_id = repo.admit(
                    track, user_id="legacy-owner", scope="public", facets=facets, evidence=evidence
                )
                with get_connection(db_path) as conn:
                    begin_write(conn, namespace="creator-supply", key=creator_id)
                    fence(conn)
                    index_content(
                        conn, creator_id, content_id, published_timestamp(track.published_at)
                    )
                imported += 1
            complete = not bool(result.get("hasMore"))
            next_page = 1 if complete else max(next_page, page + 1)
            with get_connection(db_path) as conn:
                fence(conn)
                conn.execute(
                    "UPDATE creator_supply SET next_page=?,scan_complete=? "
                    "WHERE creator_id=? AND lease_token=?",
                    (next_page, int(complete), creator_id, token),
                )
            if complete:
                break
        now = time.time()
        status = "ready" if complete else "partial"
        with get_connection(db_path) as conn:
            begin_write(conn, namespace="creator-supply", key=creator_id)
            fence(conn)
            changed = conn.execute(
                "UPDATE creator_supply SET status=?,last_success_at=?,next_check_at=?,"
                "lease_until=0,error=NULL "
                "WHERE creator_id=? AND lease_token=? AND status='running'",
                (status, now, now + (300 if complete else 1), creator_id, token),
            ).rowcount
            if not changed:
                raise RuntimeError("CreatorSupplyLeaseLost")
        return {
            "status": status,
            "indexed": imported,
            "nextPage": next_page,
            "scanComplete": complete,
        }
    except Exception as error:
        with get_connection(db_path) as conn:
            conn.execute(
                "UPDATE creator_supply SET status='failed',next_check_at=?,lease_until=0,error=? "
                "WHERE creator_id=? AND lease_token=? AND status='running'",
                (time.time() + 30, type(error).__name__, creator_id, token),
            )
        raise
    finally:
        if client is not None:
            client.close()


def render_state(db_path, user_id, session):
    saved = json.loads(session["profile_json"])["following"]
    current = following_snapshot(db_path, user_id)
    same_account = (saved.get("accountMid"), saved.get("bindingKey")) == (
        current.get("accountMid"),
        current.get("bindingKey"),
    )
    # Version can change through confirmation or sync; removal is enforced on every render.
    allowed = set(saved["creatorIds"]) & set(current["creatorIds"]) if same_account else set()
    reason = current.get("reason") or ("available" if allowed else "no_follows")
    reason = {"bilibili_login_required": "login_required", "no_following": "no_follows"}.get(
        reason, reason
    )
    if not same_account:
        reason = "account_changed"
    latest, error_type = None, None
    last_synced = current.get("lastSyncedAt")
    if allowed:
        with get_connection(db_path) as conn:
            latest = conn.execute(
                "SELECT MAX(cc.published_at) FROM creator_content cc JOIN feed_content c "
                "ON c.content_id=cc.content_id WHERE cc.creator_id IN ("
                + ",".join("?" for _ in allowed)
                + ") AND c.status='admitted' "
                "AND (c.scope='public' OR c.owner_id=?)",
                (*sorted(allowed), user_id),
            ).fetchone()[0]
            supplies = conn.execute(
                "SELECT status,last_success_at,error FROM creator_supply WHERE creator_id IN ("
                + ",".join("?" for _ in allowed)
                + ")",
                tuple(sorted(allowed)),
            ).fetchall()
            if any(r["status"] == "failed" for r in supplies):
                error_type = next(r["error"] for r in supplies if r["status"] == "failed")
                reason = "upstream_error"
            times = [r["last_success_at"] for r in supplies if r["last_success_at"]]
            if times:
                last_synced = max([last_synced or 0, *times])
    return {
        "allowed": allowed,
        "state": {
            "reason": reason,
            "snapshotVersion": saved["version"],
            "hasUpdates": current["version"] != saved["version"]
            or bool(latest and latest > saved["watermark"]),
            "lastSyncedAt": last_synced,
            "errorType": error_type,
        },
    }


def following_page(service, user_id, session, number, cursor, spec):
    saved = json.loads(session["profile_json"])["following"]
    membership = render_state(service.repo.db_path, user_id, session)
    allowed = sorted(membership["allowed"])
    schedule_supply(service.repo.db_path, allowed)
    page_id, now = uuid4().hex, time.time()
    pending = False
    with get_connection(service.repo.db_path) as conn:
        begin_write(conn, namespace="feed-page", key=session["session_id"])
        existing = conn.execute(
            "SELECT * FROM feed_pages WHERE session_id=? AND page_number=?",
            (session["session_id"], number),
        ).fetchone()
        if existing:
            page = dict(existing)
            if allowed:
                pending = bool(
                    conn.execute(
                        "SELECT 1 FROM creator_supply WHERE creator_id IN ("
                        + ",".join("?" for _ in allowed)
                        + ") AND status IN ('queued','running','partial') LIMIT 1",
                        tuple(allowed),
                    ).fetchone()
                )
        else:
            if session["expires_at"] <= now:
                raise APIError(ErrorCode.CONFLICT, "Feed session expired", 410)
            profile = json.loads(
                conn.execute(
                    "SELECT profile_json FROM feed_sessions WHERE session_id=?",
                    (session["session_id"],),
                ).fetchone()[0]
            )
            after = profile.get("followingCursor")
            scan_started = profile.get("followingScanStarted", now)
            if allowed and after:
                # Backfilled history can arrive above an already-issued keyset
                # cursor. Restart a bounded scan only for newly indexed supply;
                # issued IDs remain excluded and existing pages remain immutable.
                latest_index = conn.execute(
                    "SELECT MAX(cc.indexed_at) FROM creator_content cc JOIN feed_content c "
                    "ON c.content_id=cc.content_id WHERE cc.creator_id IN ("
                    + ",".join("?" for _ in allowed)
                    + ") AND c.status='admitted' AND (c.scope='public' OR c.owner_id=?) "
                    "AND cc.published_at<=? AND NOT EXISTS "
                    "(SELECT 1 FROM feed_items i "
                    "WHERE i.session_id=? AND i.content_id=c.content_id)",
                    (*allowed, user_id, saved["watermark"], session["session_id"]),
                ).fetchone()[0]
                if (
                    latest_index
                    and latest_index > scan_started
                    and profile.get("followingRoundExhausted")
                ):
                    after = None
                    scan_started = now
            selected, used, rows = [], set(), []
            # At most 1000 metadata rows per page. No upstream network or LLM here.
            if allowed:
                marks = ",".join("?" for _ in allowed)
                params = [user_id, *allowed, user_id, saved["watermark"], session["session_id"]]
                keyset = ""
                if after:
                    keyset = " AND (cc.published_at<? OR (cc.published_at=? AND c.content_id<?))"
                    params.extend((after[0], after[0], after[1]))
                rows = conn.execute(
                    "SELECT c.*,cc.creator_id,cc.published_at FROM creator_content cc "
                    "JOIN feed_content c ON c.content_id=cc.content_id "
                    "LEFT JOIN content_media_links l ON l.content_id=c.content_id "
                    "LEFT JOIN media_assets a ON a.asset_id=l.asset_id "
                    "LEFT JOIN content_reactions r ON r.content_id=c.content_id AND r.user_id=? "
                    f"WHERE cc.creator_id IN ({marks}) AND c.status='admitted' "
                    "AND (c.scope='public' OR c.owner_id=?) AND cc.published_at<=? "
                    "AND COALESCE(a.status,'empty') NOT IN ('retired','unsupported') "
                    "AND COALESCE(r.state,'neutral')<>'dislike' "
                    "AND NOT EXISTS (SELECT 1 FROM feed_items i WHERE i.session_id=? "
                    "AND i.content_id=c.content_id)"
                    + keyset
                    + " ORDER BY cc.published_at DESC,c.content_id DESC LIMIT 1000",
                    tuple(params),
                ).fetchall()
                prior = conn.execute(
                    "SELECT c.metadata_json FROM feed_items i "
                    "JOIN feed_content c ON c.content_id=i.content_id "
                    "WHERE i.session_id=?",
                    (session["session_id"],),
                ).fetchall()
                for item in prior:
                    metadata = json.loads(item[0])
                    used.add((metadata.get("bvid"), metadata.get("cid")))
                for row in rows:
                    metadata = json.loads(row["metadata_json"])
                    recording = (metadata.get("bvid"), metadata.get("cid"))
                    if recording in used or not spec.matches_candidate(
                        metadata, json.loads(row["facets_json"])
                    ):
                        continue
                    used.add(recording)
                    selected.append(row)
                    if len(selected) == 11:
                        break
            pending = False
            if allowed:
                pending = bool(
                    conn.execute(
                        f"SELECT 1 FROM creator_supply WHERE creator_id IN ({marks}) "
                        "AND status IN ('queued','running','partial') LIMIT 1",
                        tuple(allowed),
                    ).fetchone()
                )
            scan_more = len(rows) == 1000
            supply = (
                "available"
                if len(selected) > 10 or scan_more
                else "replenishing"
                if pending
                else "exhausted"
            )
            if len(selected) >= 10:
                last = selected[9]
            elif rows and scan_more:
                last = rows[-1]
            elif selected:
                last = selected[-1]
            else:
                last = None
            profile["followingCursor"] = (
                [last["published_at"], last["content_id"]] if last else after
            )
            profile["followingScanStarted"] = scan_started
            profile["followingRoundExhausted"] = len(selected) <= 10 and not scan_more
            conn.execute(
                "UPDATE feed_sessions SET profile_json=? WHERE session_id=?",
                (encode(profile), session["session_id"]),
            )
            conn.execute(
                "INSERT INTO feed_pages VALUES (?,?,?,?,?,?,?)",
                (
                    page_id,
                    session["session_id"],
                    number,
                    cursor or uuid4().hex,
                    uuid4().hex,
                    supply,
                    now,
                ),
            )
            for rank, row in enumerate(selected[:10], 1):
                snapshot = {
                    "decisionTraceId": f"feed:{session['session_id']}:{number}",
                    "timelineKey": [row["published_at"], row["content_id"]],
                    "creatorId": row["creator_id"],
                    "followVersion": saved["version"],
                }
                conn.execute(
                    "INSERT INTO feed_items VALUES (?,?,?,?,?,?,?)",
                    (
                        uuid4().hex,
                        session["session_id"],
                        page_id,
                        row["content_id"],
                        rank,
                        encode(snapshot),
                        now,
                    ),
                )
            page = dict(
                conn.execute("SELECT * FROM feed_pages WHERE page_id=?", (page_id,)).fetchone()
            )
    result = service._render(user_id, session, page)
    if not result["items"] and result["followingState"]["reason"] == "available":
        result["followingState"]["reason"] = "syncing" if pending else "no_music"
    return result
