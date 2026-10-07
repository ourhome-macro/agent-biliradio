"""Bounded, deterministic Feed reranking over already admitted candidates.

The caller enforces request constraints and visibility before this policy runs.
Only explicit reactions and grounded watch projections affect short-term taste;
navigation, buffer errors and issued-but-unseen cards never become preferences.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict

from database import get_connection


def _object(value):
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _identities(row):
    metadata = _object(row.get("metadata_json"))
    facets = _object(row.get("facets_json"))
    evidence = _object(row.get("evidence_json"))
    keys = set()
    if row.get("asset_id"):
        keys.add("asset:" + str(row["asset_id"]))
    bvid, cid = metadata.get("bvid"), metadata.get("cid")
    if bvid:
        keys.add(f"recording:{bvid}:{cid or 'default'}")
    else:
        keys.add("track:" + str(row["track_id"]))
    # Titles are not identifiers: covers and different performances can share a
    # title. Work suppression requires an explicitly catalogued identity.
    work = (
        facets.get("work_id")
        or facets.get("workId")
        or evidence.get("work_id")
        or evidence.get("workId")
    )
    if work:
        keys.add("work:" + str(work))
    return keys


def _features(row):
    facets, metadata = _object(row.get("facets_json")), _object(row.get("metadata_json"))
    result = set()
    for canonical, fields in {
        "genre": ("genre", "genres"),
        "language": ("language", "languages"),
        "region": ("region", "regions"),
        "scene": ("scene", "scenes", "scene_tags"),
    }.items():
        for field in fields:
            values = facets.get(field, [])
            if isinstance(values, str):
                values = [values]
            if isinstance(values, (list, tuple)):
                for value in values:
                    if isinstance(value, str) and value.strip():
                        result.add(f"{canonical}:{value.strip().casefold()}")
    owner = metadata.get("ownerMid") or metadata.get("owner_mid") or metadata.get("owner")
    if isinstance(owner, (str, int)) and str(owner).strip():
        result.add("owner:" + str(owner).strip().casefold())
    return result


def deduplicate_catalog(rows, *, excluded=()):
    """Keep the first representation of each recording or explicit work."""
    used, output = set(excluded), []
    for row in rows:
        identities = _identities(row)
        duplicate = bool(identities & used)
        used.update(identities)
        if not duplicate:
            output.append(row)
    return output


def distinct_catalog_count(rows, *, excluded=()):
    return len(deduplicate_catalog(rows, excluded=excluded))


def _catalog_exclusions(conn, user_id, session_id, seen_since):
    issued = conn.execute(
        """SELECT c.*,l.asset_id FROM feed_items i
        JOIN feed_content c ON c.content_id=i.content_id
        LEFT JOIN content_media_links l ON l.content_id=c.content_id
        WHERE i.session_id=?""",
        (session_id,),
    ).fetchall()
    if seen_since is not None:
        issued += conn.execute(
            """SELECT c.*,l.asset_id FROM feed_user_content h
            JOIN feed_content c ON c.content_id=h.content_id
            LEFT JOIN content_media_links l ON l.content_id=c.content_id
            WHERE h.user_id=? AND MAX(h.last_exposed_at,h.last_watched_at)>?
              AND (c.scope='public' OR c.owner_id=?)""",
            (user_id, seen_since, user_id),
        ).fetchall()
    excluded = set()
    for row in issued:
        excluded.update(_identities(dict(row)))
    return excluded


def catalog_exclusions(*, user_id, session_id, db_path, seen_since=None):
    """Load recording/work exclusions before a bounded candidate scan."""
    with get_connection(db_path) as conn:
        return _catalog_exclusions(conn, user_id, session_id, seen_since)


def _recent_affinity(conn, user_id, now):
    weights = defaultdict(float)
    # Latest desired reaction states are authoritative. No duplicate event sum.
    signals = conn.execute(
        """SELECT c.metadata_json,c.facets_json,r.state,r.updated_at
        FROM content_reactions r JOIN feed_content c ON c.content_id=r.content_id
        WHERE r.user_id=? AND r.state IN ('like','dislike') AND r.updated_at>=?
          AND (c.scope='public' OR c.owner_id=?)
        ORDER BY r.updated_at DESC LIMIT 100""",
        (user_id, now - 7 * 86400, user_id),
    ).fetchall()
    for signal in signals:
        decay = 0.5 ** (max(0, now - signal["updated_at"]) / 86400)
        weight = (1.0 if signal["state"] == "like" else -1.0) * decay
        for feature in _features(dict(signal)):
            weights[feature] += weight
    signals = conn.execute(
        """SELECT c.metadata_json,c.facets_json,u.last_watched_at,u.completed
        FROM feed_user_content u JOIN feed_content c ON c.content_id=u.content_id
        LEFT JOIN content_reactions r ON r.user_id=u.user_id AND r.content_id=u.content_id
        WHERE u.user_id=? AND u.last_watched_at>=? AND (u.watch_ms>=30000 OR u.completed=1)
          AND COALESCE(r.state,'neutral')='neutral' AND (c.scope='public' OR c.owner_id=?)
        ORDER BY u.last_watched_at DESC LIMIT 100""",
        (user_id, now - 86400, user_id),
    ).fetchall()
    for signal in signals:
        decay = 0.5 ** (max(0, now - signal["last_watched_at"]) / (6 * 3600))
        for feature in _features(dict(signal)):
            weights[feature] += 0.25 * decay
    return {key: max(-2.0, min(2.0, value)) for key, value in weights.items()}


def rank_catalog(
    rows,
    *,
    user_id,
    session_id,
    db_path,
    mode="personal",
    page_number=0,
    exploration_ratio=0.2,
    seen_since=None,
):
    """Return rows in policy order, with a reviewable ``_feed_policy`` snapshot.

    Base ranking dominates: a single strong feedback signal moves a matching
    item by at most four rank positions. Exploration reserves at most two slots
    in the first ten, deterministically by session/page/content. It never adds a
    candidate the caller filtered out and never changes a persisted page.
    """
    with get_connection(db_path) as conn:
        used = _catalog_exclusions(conn, user_id, session_id, seen_since)
        weights = _recent_affinity(conn, user_id, time.time()) if mode == "personal" else {}
    ranked = []
    for base_rank, source in enumerate(rows):
        row = dict(source)
        features = _features(row)
        affinity = max(-2.0, min(2.0, sum(weights.get(key, 0.0) for key in features)))
        row["_feed_policy"] = {
            "version": "feed-short-term-v1",
            "baseRank": base_rank,
            "shortTermAffinity": round(affinity, 4),
            "exploration": False,
        }
        ranked.append((base_rank - 4 * affinity, base_rank, row))
    ranked.sort(key=lambda entry: (entry[0], entry[1]))
    unique = []
    for _, _, row in ranked:
        identities = _identities(row)
        duplicate = bool(identities & used)
        used.update(identities)
        if duplicate:
            continue
        unique.append(row)
    if mode != "personal" or len(unique) < 5:
        return unique
    ratio = max(0.0, min(0.2, exploration_ratio))
    slots = int(10 * ratio)
    if not slots:
        return unique
    # Reserve exploration inside the admitted top-40 pool; do not turn broad
    # recall into unbounded random recommendations. Weak affinity is exploratory.
    pool = [row for row in unique[:40] if row["_feed_policy"]["shortTermAffinity"] <= 0]
    pool.sort(
        key=lambda row: hashlib.sha256(
            f"{session_id}:{page_number}:{row['content_id']}".encode()
        ).digest()
    )
    exploration = pool[:slots]
    for row in exploration:
        unique.remove(row)
    for index, row in enumerate(exploration):
        row["_feed_policy"]["exploration"] = True
        unique.insert(min(4 + index * 5, len(unique)), row)
    return unique
