from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import requests
from database import get_connection
from models import Track

DEFAULT_KEYWORD_LIMIT = 3
DEFAULT_VARIANT_COOLDOWN_HOURS = 6
LOW_NOVELTY_COOLDOWN_HOURS = 24
LOW_NOVELTY_THRESHOLD = 0.20
DEFAULT_FEEDBACK_HALF_LIFE_DAYS = 14.0
DEFAULT_YIELD_HALF_LIFE_DAYS = 7.0
DEFAULT_EXPLORATION_MIN_EXPOSURES = 20.0
DEFAULT_UCB_COEFFICIENT = 0.18


class KeywordGovernance:
    """Govern semantic keyword families, concrete variants and their marginal yield."""

    def __init__(self, db_path: str, *, user_id: str) -> None:
        self.db_path = db_path
        self.user_id = user_id

    def prepare(
        self,
        queries: list[str],
        *,
        source: str,
        preserve_order: bool,
        family_specs: dict[str, dict[str, object]] | None = None,
        limit: int = DEFAULT_KEYWORD_LIMIT,
        apply_governance: bool = True,
    ) -> list[dict[str, Any]]:
        now = _utc_now()
        specs = family_specs or {}
        prepared: list[dict[str, Any]] = []
        with get_connection(self.db_path) as conn:
            for order, query in enumerate(
                dict.fromkeys(item.strip() for item in queries if item.strip())
            ):
                (
                    canonical_spec,
                    exploration_axis,
                    keyword_kind,
                    origin,
                    parent_keyword_id,
                ) = _canonical_spec(specs.get(query), query=query, source=source)
                canonical_json = json.dumps(
                    canonical_spec, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                family_id = _family_id(self.user_id, canonical_json)
                keyword_id = _keyword_id(self.user_id, query)
                conn.execute(
                    """
                    INSERT INTO discovery_keyword_families (
                        family_id, user_id, canonical_spec_json, exploration_axis,
                        keyword_kind, origin, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id, platform, canonical_spec_json) DO UPDATE SET
                        exploration_axis=excluded.exploration_axis,
                        keyword_kind=CASE
                            WHEN discovery_keyword_families.keyword_kind='anchor' THEN 'anchor'
                            ELSE excluded.keyword_kind
                        END,
                        origin=CASE
                            WHEN discovery_keyword_families.keyword_kind='anchor' THEN discovery_keyword_families.origin
                            ELSE excluded.origin
                        END,
                        updated_at=excluded.updated_at
                    """,
                    (
                        family_id,
                        self.user_id,
                        canonical_json,
                        exploration_axis,
                        keyword_kind,
                        origin,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO discovery_keywords (
                        keyword_id, user_id, keyword, source, family_id,
                        canonical_spec_json, exploration_axis, keyword_kind,
                        origin, parent_keyword_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id, platform, keyword) DO UPDATE SET
                        source=excluded.source,
                        family_id=excluded.family_id,
                        canonical_spec_json=excluded.canonical_spec_json,
                        exploration_axis=excluded.exploration_axis,
                        keyword_kind=CASE
                            WHEN discovery_keywords.keyword_kind='anchor' THEN 'anchor'
                            ELSE excluded.keyword_kind
                        END,
                        origin=CASE
                            WHEN discovery_keywords.keyword_kind='anchor' THEN discovery_keywords.origin
                            ELSE excluded.origin
                        END,
                        parent_keyword_id=COALESCE(excluded.parent_keyword_id, discovery_keywords.parent_keyword_id)
                    """,
                    (
                        keyword_id,
                        self.user_id,
                        query,
                        source,
                        family_id,
                        canonical_json,
                        exploration_axis,
                        keyword_kind,
                        origin,
                        parent_keyword_id,
                        now,
                    ),
                )
                row = conn.execute(
                    """
                    SELECT k.*, f.quality_score AS family_quality,
                           f.affinity_score AS family_affinity,
                           f.yield_score AS family_yield,
                           f.status AS family_status, f.cooldown_until AS family_cooldown_until
                    FROM discovery_keywords k
                    JOIN discovery_keyword_families f ON f.family_id=k.family_id
                    WHERE k.keyword_id=?
                    """,
                    (keyword_id,),
                ).fetchone()
                if row is None:
                    continue
                if apply_governance and (
                    row["status"] == "retired" or row["family_status"] == "retired"
                ):
                    continue
                cooldown_active = _is_future(row["cooldown_until"], now) or _is_future(
                    row["family_cooldown_until"], now
                )
                prepared.append(
                    {
                        "keywordId": row["keyword_id"],
                        "familyId": row["family_id"],
                        "query": row["keyword"],
                        "quality": float(row["quality_score"] or 0.0),
                        "familyQuality": float(row["family_quality"] or 0.0),
                        "searchCount": int(row["search_count"] or 0),
                        "marginalYield": float(row["last_marginal_yield"] or 0.0),
                        "resultOverlap": float(row["last_result_overlap"] or 0.0),
                        "cooldownActive": cooldown_active,
                        "status": row["status"],
                        "canonicalSpec": canonical_spec,
                        "explorationAxis": exploration_axis,
                        "keywordKind": str(row["keyword_kind"] or keyword_kind),
                        "origin": str(row["origin"] or origin),
                        "affinity": max(
                            float(row["affinity_score"] or 0.5),
                            float(row["family_affinity"] or 0.5),
                        ),
                        "affinityExposure": float(row["affinity_exposure"] or 0.0),
                        "yield": max(
                            float(row["yield_score"] or 0.0),
                            float(row["family_yield"] or 0.0),
                        ),
                        "evolutionAction": str(row["evolution_action"] or "observe"),
                        "order": order,
                    }
                )
        if not apply_governance:
            return [
                {key: value for key, value in item.items() if key != "order"}
                for item in sorted(prepared, key=lambda item: int(item["order"]))[
                    : max(int(limit), 0)
                ]
            ]
        return self._select(prepared, limit=max(int(limit), 0), preserve_order=preserve_order)

    def record_discovery(
        self,
        keyword_id: str,
        *,
        tracks: list[Track],
        admitted_count: int,
        discovery_job_id: str = "",
        admitted_track_ids: list[str] | None = None,
        scope_kind: str = "default",
        scope_key: str = "",
    ) -> None:
        now = _utc_now()
        current_ids = list(dict.fromkeys(track.track_id for track in tracks if track.track_id))
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM discovery_keywords WHERE keyword_id=? AND user_id=?",
                (keyword_id, self.user_id),
            ).fetchone()
            if row is None:
                return
            family_id = str(row["family_id"] or "")
            previous_family_ids = {
                str(item["track_id"])
                for item in conn.execute(
                    """
                    SELECT DISTINCT c.track_id
                    FROM discovery_keyword_candidates c
                    JOIN discovery_keywords k ON k.keyword_id=c.keyword_id
                    WHERE c.user_id=? AND k.family_id=?
                    """,
                    (self.user_id, family_id),
                ).fetchall()
            }
            last_ids = set(_json_strings(row["last_result_track_ids_json"]))
            new_ids = [track_id for track_id in current_ids if track_id not in previous_family_ids]
            duplicate_count = max(len(current_ids) - len(new_ids), 0)
            overlap = _jaccard(set(current_ids), last_ids)
            marginal_yield = len(new_ids) / max(len(current_ids), 1)
            low_novelty_count = (
                int(row["consecutive_low_novelty"] or 0) + 1
                if marginal_yield < LOW_NOVELTY_THRESHOLD
                else 0
            )
            searches = int(row["search_count"] or 0) + 1
            total_new = int(row["new_candidate_count"] or 0) + len(new_ids)
            if searches >= 5 and total_new == 0:
                status = "retired"
            elif low_novelty_count >= 2:
                status = "cooldown"
            else:
                status = "active"
            cooldown_hours = (
                LOW_NOVELTY_COOLDOWN_HOURS
                if low_novelty_count >= 2
                else DEFAULT_VARIANT_COOLDOWN_HOURS
            )
            cooldown_until = (
                datetime.now(timezone.utc) + timedelta(hours=cooldown_hours)
            ).isoformat()
            conn.execute(
                """
                UPDATE discovery_keywords
                SET search_count=search_count+1,
                    candidate_count=candidate_count+?, admitted_count=admitted_count+?,
                    new_candidate_count=new_candidate_count+?,
                    duplicate_candidate_count=duplicate_candidate_count+?,
                    last_marginal_yield=?, last_result_overlap=?,
                    consecutive_low_novelty=?, last_result_track_ids_json=?,
                    status=?, cooldown_until=?, last_used_at=?, last_evaluated_at=?
                WHERE keyword_id=? AND user_id=?
                """,
                (
                    len(tracks),
                    admitted_count,
                    len(new_ids),
                    duplicate_count,
                    round(marginal_yield, 4),
                    round(overlap, 4),
                    low_novelty_count,
                    json.dumps(current_ids, ensure_ascii=False),
                    status,
                    cooldown_until,
                    now,
                    now,
                    keyword_id,
                    self.user_id,
                ),
            )
            for track_id in current_ids:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO discovery_keyword_candidates (
                        keyword_id, user_id, track_id, discovered_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (keyword_id, self.user_id, track_id, now),
                )
            admitted_ids = set(admitted_track_ids or [])
            if discovery_job_id:
                for rank, track_id in enumerate(current_ids, start=1):
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO discovery_item_sources (
                            discovery_job_id, user_id, keyword_id, family_id,
                            track_id, search_rank, admitted, scope_kind, scope_key, discovered_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            discovery_job_id,
                            self.user_id,
                            keyword_id,
                            family_id,
                            track_id,
                            rank,
                            int(track_id in admitted_ids),
                            "request" if scope_kind == "request" else "default",
                            scope_key if scope_kind == "request" else "",
                            now,
                        ),
                    )
            conn.execute(
                """
                UPDATE discovery_keyword_families
                SET search_count=search_count+1,
                    candidate_count=candidate_count+?,
                    new_candidate_count=new_candidate_count+?,
                    last_used_at=?, updated_at=?
                WHERE family_id=? AND user_id=?
                """,
                (len(tracks), len(new_ids), now, now, family_id, self.user_id),
            )
            conn.execute(
                """
                INSERT INTO discovery_search_observations (
                    observation_id, keyword_id, family_id, user_id,
                    candidate_count, new_candidate_count, admitted_count,
                    result_overlap, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"search-observation:{uuid4().hex}",
                    keyword_id,
                    family_id,
                    self.user_id,
                    len(tracks),
                    len(new_ids),
                    admitted_count,
                    round(overlap, 4),
                    now,
                ),
            )
            self._recalculate(conn, keyword_id)
            self._recalculate_family(conn, family_id)

    def record_feedback(
        self,
        track_id: str,
        event: str,
        *,
        recommendation_trace_id: str = "",
        source_keyword_ids: list[str] | None = None,
    ) -> None:
        if event not in {
            "shown",
            "played",
            "accepted",
            "completed",
            "liked",
            "skipped",
            "dismissed",
            "dislike",
        }:
            return
        with get_connection(self.db_path) as conn:
            trace_id = recommendation_trace_id.strip()
            if not trace_id:
                trace_row = conn.execute(
                    """
                    SELECT recommendation_trace_id
                    FROM recommendation_keyword_attributions
                    WHERE user_id=? AND track_id=?
                    ORDER BY shown_at DESC LIMIT 1
                    """,
                    (self.user_id, track_id),
                ).fetchone()
                trace_id = "" if trace_row is None else str(trace_row["recommendation_trace_id"])
            keyword_filter = list(
                dict.fromkeys(str(item) for item in source_keyword_ids or [] if str(item))
            )
            parameters: list[object] = [self.user_id, track_id]
            where = "user_id=? AND track_id=?"
            if trace_id:
                where += " AND recommendation_trace_id=?"
                parameters.append(trace_id)
            if keyword_filter:
                placeholders = ",".join("?" for _ in keyword_filter)
                where += f" AND keyword_id IN ({placeholders})"
                parameters.extend(keyword_filter)
            rows = conn.execute(
                f"""
                SELECT keyword_id, family_id
                FROM recommendation_keyword_attributions
                WHERE {where}
                """,
                tuple(parameters),
            ).fetchall()
            if rows:
                assignments = {
                    "shown": "shown=1",
                    "played": "clicked=1",
                    "accepted": "clicked=1",
                    "completed": "clicked=1, completed=1",
                    "liked": "liked=1",
                    "skipped": "negative=1",
                    "dismissed": "negative=1",
                    "dislike": "negative=1",
                }[event]
                conn.execute(
                    f"""
                    UPDATE recommendation_keyword_attributions
                    SET {assignments}, updated_at=?
                    WHERE {where}
                    """,
                    (_utc_now(), *parameters),
                )
            else:
                rows = conn.execute(
                    """
                    SELECT c.keyword_id, k.family_id
                    FROM discovery_keyword_candidates c
                    JOIN discovery_keywords k ON k.keyword_id=c.keyword_id
                    WHERE c.user_id=? AND c.track_id=?
                    GROUP BY c.keyword_id, k.family_id
                    ORDER BY MAX(c.discovered_at) DESC, c.keyword_id LIMIT 3
                    """,
                    (self.user_id, track_id),
                ).fetchall()
            families: set[str] = set()
            for row in rows:
                increments = {
                    "shown": {"shown_count": 1},
                    "played": {"clicked_count": 1},
                    "accepted": {"clicked_count": 1},
                    "completed": {"clicked_count": 1, "completed_count": 1},
                    "liked": {"liked_count": 1},
                    "skipped": {"dismissed_count": 1},
                    "dismissed": {"dismissed_count": 1},
                    "dislike": {"dismissed_count": 1},
                }[event]
                assignments = ", ".join(
                    f"{field}={field}+{amount}" for field, amount in increments.items()
                )
                conn.execute(
                    f"UPDATE discovery_keywords SET {assignments}, last_evaluated_at=? WHERE keyword_id=?",
                    (_utc_now(), row["keyword_id"]),
                )
                self._refresh_affinity(conn, str(row["keyword_id"]))
                self._recalculate(conn, row["keyword_id"])
                families.add(str(row["family_id"] or ""))
            for family_id in families:
                self._recalculate_family(conn, family_id)

    def reusable_keywords(self, *, limit: int = 24) -> list[dict[str, Any]]:
        now = _utc_now()
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT keyword, canonical_spec_json, keyword_kind, origin,
                       parent_keyword_id, exploration_axis, affinity_score,
                       yield_score, status, cooldown_until
                FROM discovery_keywords
                WHERE user_id=? AND status<>'retired' AND origin<>'negative_probe'
                ORDER BY
                    CASE keyword_kind WHEN 'anchor' THEN 0 ELSE 1 END,
                    affinity_score DESC, yield_score DESC,
                    COALESCE(last_used_at, '') ASC
                LIMIT ?
                """,
                (self.user_id, max(1, min(int(limit), 80))),
            ).fetchall()
        return [
            {
                "query": str(row["keyword"]),
                "canonicalSpec": {
                    **_json_object(row["canonical_spec_json"]),
                    "keyword_kind": str(row["keyword_kind"] or "probe"),
                    "origin": str(row["origin"] or "legacy"),
                    "parent_keyword_id": str(row["parent_keyword_id"] or ""),
                    "exploration_axis": str(row["exploration_axis"] or "base"),
                },
                "cooldownActive": _is_future(row["cooldown_until"], now),
            }
            for row in rows
        ]

    def evolution_due(self) -> bool:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        with get_connection(self.db_path) as conn:
            last = conn.execute(
                "SELECT MAX(created_at) AS created_at FROM discovery_keyword_evolution_runs WHERE user_id=?",
                (self.user_id,),
            ).fetchone()
            if last and last["created_at"] and str(last["created_at"]) > cutoff:
                return False
            actionable = conn.execute(
                """
                SELECT COUNT(*)
                FROM discovery_keywords
                WHERE user_id=? AND evolution_action IN ('rewrite', 'downweight', 'retire')
                """,
                (self.user_id,),
            ).fetchone()[0]
            completed_jobs = conn.execute(
                "SELECT COUNT(*) FROM discovery_jobs WHERE user_id=? AND status='completed'",
                (self.user_id,),
            ).fetchone()[0]
        return bool(actionable or (completed_jobs and completed_jobs % 3 == 0))

    def record_evolution_run(
        self,
        *,
        status: str,
        proposed_count: int = 0,
        accepted_count: int = 0,
        error: str = "",
        run_id: str = "",
    ) -> str:
        resolved_run_id = run_id or f"keyword-evolution:{uuid4().hex}"
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO discovery_keyword_evolution_runs (
                    run_id, user_id, status, proposed_count, accepted_count, error, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status,
                    proposed_count=excluded.proposed_count,
                    accepted_count=excluded.accepted_count,
                    error=excluded.error
                """,
                (
                    resolved_run_id,
                    self.user_id,
                    status[:32],
                    max(int(proposed_count), 0),
                    max(int(accepted_count), 0),
                    str(error)[:300],
                    _utc_now(),
                ),
            )
        return resolved_run_id

    def evolution_snapshot(self, *, limit: int = 24) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT keyword_id, family_id, keyword, keyword_kind, origin,
                       canonical_spec_json, affinity_score, yield_score,
                       evolution_action, search_count, status
                FROM discovery_keywords
                WHERE user_id=?
                ORDER BY
                    CASE evolution_action
                        WHEN 'rewrite' THEN 0 WHEN 'downweight' THEN 1
                        WHEN 'retire' THEN 2 ELSE 3 END,
                    affinity_score DESC, yield_score DESC
                LIMIT ?
                """,
                (self.user_id, max(1, min(int(limit), 60))),
            ).fetchall()
        return {
            "keywords": [
                {
                    "keywordId": row["keyword_id"],
                    "familyId": row["family_id"],
                    "query": row["keyword"],
                    "kind": row["keyword_kind"],
                    "origin": row["origin"],
                    "canonicalSpec": _json_object(row["canonical_spec_json"]),
                    "affinity": round(float(row["affinity_score"] or 0.5), 4),
                    "yield": round(float(row["yield_score"] or 0.0), 4),
                    "action": row["evolution_action"],
                    "searchCount": int(row["search_count"] or 0),
                    "status": row["status"],
                }
                for row in rows
            ]
        }

    def register_evolution_proposals(
        self,
        proposals: list[dict[str, Any]],
        *,
        evolution_run_id: str = "",
    ) -> dict[str, int]:
        accepted = 0
        rejected = 0
        for proposal in proposals[:8]:
            action = str(proposal.get("action") or "").strip().lower()
            query = " ".join(str(proposal.get("query") or "").split())[:180]
            parent_keyword_id = str(proposal.get("parentKeywordId") or "").strip()
            reason = str(proposal.get("reason") or "")[:300]
            spec = (
                proposal.get("canonicalSpec")
                if isinstance(proposal.get("canonicalSpec"), dict)
                else {}
            )
            status = "accepted"
            if action not in {"rewrite", "explore"} or len(query) < 3:
                status = "rejected"
            with get_connection(self.db_path) as conn:
                parent = (
                    conn.execute(
                        """
                        SELECT keyword_id, family_id, canonical_spec_json,
                               evolution_action, affinity_score, yield_score
                        FROM discovery_keywords
                        WHERE keyword_id=? AND user_id=?
                        """,
                        (parent_keyword_id, self.user_id),
                    ).fetchone()
                    if parent_keyword_id
                    else None
                )
                if action == "rewrite" and parent is None:
                    status = "rejected"
                if (
                    action == "rewrite"
                    and parent is not None
                    and parent["evolution_action"] != "rewrite"
                ):
                    status = "rejected"
                if action == "rewrite" and parent is not None:
                    spec = _json_object(parent["canonical_spec_json"])
                elif not spec and parent is not None:
                    spec = _json_object(parent["canonical_spec_json"])
                canonical, _axis, _kind, _origin, _parent = _canonical_spec(
                    {
                        **spec,
                        "exploration_axis": "llm_rewrite" if action == "rewrite" else "llm_explore",
                        "keyword_kind": "probe",
                        "origin": "llm_evolution",
                        "parent_keyword_id": parent_keyword_id,
                    },
                    query=query,
                    source="llm_evolution",
                )
                canonical_json = json.dumps(
                    canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                family_id = _family_id(self.user_id, canonical_json)
                family = conn.execute(
                    "SELECT status FROM discovery_keyword_families WHERE family_id=?",
                    (family_id,),
                ).fetchone()
                duplicate = conn.execute(
                    "SELECT 1 FROM discovery_keywords WHERE user_id=? AND keyword=?",
                    (self.user_id, query),
                ).fetchone()
                if duplicate is not None or (family is not None and family["status"] == "retired"):
                    status = "rejected"
                if action == "explore" and family is not None:
                    status = "rejected"
                proposal_id = f"keyword-proposal:{uuid4().hex}"
                conn.execute(
                    """
                    INSERT INTO discovery_keyword_proposals (
                        proposal_id, user_id, action, query_text, canonical_spec_json,
                        parent_keyword_id, family_id, reason, status, created_at, evolution_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        proposal_id,
                        self.user_id,
                        action or "invalid",
                        query,
                        canonical_json,
                        parent_keyword_id or None,
                        family_id,
                        reason,
                        status,
                        _utc_now(),
                        evolution_run_id,
                    ),
                )
            if status == "accepted":
                self.prepare(
                    [query],
                    source="llm_evolution",
                    preserve_order=True,
                    family_specs={
                        query: {
                            **canonical,
                            "exploration_axis": "llm_rewrite"
                            if action == "rewrite"
                            else "llm_explore",
                            "keyword_kind": "probe",
                            "origin": "llm_evolution",
                            "parent_keyword_id": parent_keyword_id,
                        }
                    },
                    limit=0,
                )
                accepted += 1
            else:
                rejected += 1
        return {"accepted": accepted, "rejected": rejected}

    @staticmethod
    def _refresh_affinity(conn: Any, keyword_id: str) -> None:
        rows = conn.execute(
            """
            SELECT credit_weight, shown, clicked, completed, liked, negative, updated_at
            FROM recommendation_keyword_attributions
            WHERE keyword_id=?
            """,
            (keyword_id,),
        ).fetchall()
        exposure = 0.0
        positive = 0.0
        negative = 0.0
        now = datetime.now(timezone.utc)
        half_life_days = _env_float(
            "RECOMMEND_KEYWORD_FEEDBACK_HALF_LIFE_DAYS",
            DEFAULT_FEEDBACK_HALF_LIFE_DAYS,
        )
        for row in rows:
            decay = _time_decay(
                row["updated_at"],
                now=now,
                half_life_days=half_life_days,
            )
            weight = float(row["credit_weight"] or 0.0) * decay
            exposure += weight * int(row["shown"] or 0)
            reward = min(
                1.0,
                0.20 * int(row["clicked"] or 0)
                + 0.80 * int(row["completed"] or 0)
                + 1.00 * int(row["liked"] or 0),
            )
            positive += weight * reward
            negative += weight * int(row["negative"] or 0)
        affinity = (positive + 2.0) / max(positive + negative + 4.0, 1e-9)
        conn.execute(
            """
            UPDATE discovery_keywords
            SET affinity_exposure=?, affinity_positive=?, affinity_negative=?, affinity_score=?
            WHERE keyword_id=?
            """,
            (
                round(exposure, 4),
                round(positive, 4),
                round(negative, 4),
                round(affinity, 4),
                keyword_id,
            ),
        )

    def _select(
        self,
        candidates: list[dict[str, Any]],
        *,
        limit: int,
        preserve_order: bool,
    ) -> list[dict[str, Any]]:
        if limit <= 0 or not candidates:
            return []
        vectors = _embed_queries([item["query"] for item in candidates])
        pending = list(range(len(candidates)))
        selected: list[int] = []
        family_counts: dict[str, int] = {}
        max_per_family = 2 if preserve_order else 1
        anchor_target = limit if preserve_order else min(limit, round(limit * 0.75))
        probe_target = 0 if preserve_order else max(limit - anchor_target, 0)

        def base_score(index: int) -> float:
            item = candidates[index]
            quality = max(float(item["quality"]), float(item["familyQuality"]))
            affinity = float(item["affinity"])
            yield_score = float(item["yield"])
            unseen_bonus = 0.20 if int(item["searchCount"]) == 0 else 0.0
            exploration_bonus = 0.08 if item["explorationAxis"] == "adjacent_genre" else 0.0
            cooldown_penalty = 0.55 if item["cooldownActive"] else 0.0
            downweight_penalty = 0.25 if item["evolutionAction"] == "downweight" else 0.0
            anchor_bonus = 0.08 if item["keywordKind"] == "anchor" else 0.0
            order_bonus = max(0.0, 0.08 - 0.01 * int(item["order"])) if preserve_order else 0.0
            exposure = max(float(item.get("affinityExposure") or 0.0), 0.0)
            total_exposure = sum(
                max(float(value.get("affinityExposure") or 0.0), 0.0) for value in candidates
            )
            min_exposures = _env_float(
                "RECOMMEND_KEYWORD_MIN_EXPOSURES",
                DEFAULT_EXPLORATION_MIN_EXPOSURES,
            )
            cold_start_bonus = (
                0.20 * (1.0 - exposure / min_exposures)
                if min_exposures > 0 and exposure < min_exposures
                else 0.0
            )
            ucb_bonus = _env_float(
                "RECOMMEND_KEYWORD_UCB_COEFFICIENT",
                DEFAULT_UCB_COEFFICIENT,
            ) * math.sqrt(math.log(total_exposure + 2.0) / (exposure + 1.0))
            return (
                0.45 * affinity
                + 0.35 * yield_score
                + 0.10 * quality
                + unseen_bonus
                + exploration_bonus
                + anchor_bonus
                + order_bonus
                + cold_start_bonus
                + ucb_bonus
                - 0.22 * float(item["resultOverlap"])
                - cooldown_penalty
                - downweight_penalty
            )

        while pending and len(selected) < limit:
            eligible_all = [
                index
                for index in pending
                if family_counts.get(str(candidates[index]["familyId"]), 0) < max_per_family
                and (
                    preserve_order
                    or candidates[index]["explorationAxis"] != "adjacent_genre"
                    or not any(
                        candidates[item]["explorationAxis"] == "adjacent_genre" for item in selected
                    )
                )
            ]
            if preserve_order:
                eligible = eligible_all
            else:
                anchor_count = sum(
                    candidates[index]["keywordKind"] == "anchor" for index in selected
                )
                probe_count = sum(candidates[index]["keywordKind"] == "probe" for index in selected)
                anchor_pool = [
                    index for index in eligible_all if candidates[index]["keywordKind"] == "anchor"
                ]
                probe_pool = [
                    index for index in eligible_all if candidates[index]["keywordKind"] == "probe"
                ]
                if anchor_count < anchor_target and anchor_pool:
                    eligible = anchor_pool
                elif probe_count < probe_target and probe_pool:
                    eligible = probe_pool
                else:
                    eligible = eligible_all
            if not eligible:
                break

            def mmr_score(index: int) -> float:
                similarity = max(
                    (
                        _keyword_similarity(
                            candidates[index],
                            candidates[other],
                            vectors=vectors,
                            left_index=index,
                            right_index=other,
                        )
                        for other in selected
                    ),
                    default=0.0,
                )
                return base_score(index) - 0.38 * similarity

            best = max(eligible, key=mmr_score)
            pending.remove(best)
            selected.append(best)
            family_id = str(candidates[best]["familyId"])
            family_counts[family_id] = family_counts.get(family_id, 0) + 1

        if not preserve_order and len(selected) >= 3:
            has_exploration = any(
                candidates[index]["explorationAxis"] == "adjacent_genre" for index in selected
            )
            exploration_candidates = [
                index
                for index in pending
                if candidates[index]["explorationAxis"] == "adjacent_genre"
                and str(candidates[index]["familyId"])
                not in {str(candidates[item]["familyId"]) for item in selected[:-1]}
            ]
            if not has_exploration and exploration_candidates:
                selected[-1] = max(exploration_candidates, key=base_score)
        mode = "bge_m3_mmr" if vectors is not None else "structured_mmr"
        return [
            {
                **{key: value for key, value in candidates[index].items() if key != "order"},
                "selectionMode": mode,
                "governanceScore": round(base_score(index), 4),
            }
            for index in selected
        ]

    @staticmethod
    def _recalculate(conn: Any, keyword_id: str) -> None:
        row = conn.execute(
            "SELECT * FROM discovery_keywords WHERE keyword_id=?", (keyword_id,)
        ).fetchone()
        if row is None:
            return
        decayed = _decayed_search_metrics(conn, keyword_id)
        candidates = max(float(decayed["candidate_count"]), 1.0)
        shown = int(row["shown_count"])
        dismissed = int(row["dismissed_count"])
        completed = int(row["completed_count"])
        likes = int(row["liked_count"])
        clicks = int(row["clicked_count"])
        acceptance = min((0.25 * clicks + completed + 1.2 * likes) / max(shown, 1), 1.0)
        dismiss_rate = dismissed / max(shown, 1)
        novelty = float(decayed["new_candidate_count"]) / candidates
        admission = float(decayed["admitted_count"]) / candidates
        per_search_yield = min(
            float(decayed["new_candidate_count"]) / max(float(decayed["search_count"]), 1.0) / 8.0,
            1.0,
        )
        yield_score = min(
            max(0.45 * novelty + 0.35 * per_search_yield + 0.20 * admission, 0.0), 1.0
        )
        affinity = float(row["affinity_score"] or 0.5)
        feedback_evidence = float(row["affinity_positive"] or 0.0) + float(
            row["affinity_negative"] or 0.0
        )
        decision_ready = float(row["affinity_exposure"] or 0.0) >= _env_float(
            "RECOMMEND_KEYWORD_MIN_EXPOSURES",
            DEFAULT_EXPLORATION_MIN_EXPOSURES,
        )
        quality = max(
            0.0,
            min(
                1.0,
                0.25 * admission
                + 0.35 * acceptance
                + 0.25 * novelty
                + 0.15 * (1.0 - dismiss_rate)
                - 0.15 * float(row["last_result_overlap"] or 0.0),
            ),
        )
        status = str(row["status"])
        keyword_kind = str(row["keyword_kind"] or "probe")
        high_affinity = decision_ready and feedback_evidence >= 3 and affinity >= 0.62
        low_affinity = decision_ready and feedback_evidence >= 3 and affinity <= 0.40
        high_yield = yield_score >= 0.45
        low_yield = int(row["search_count"]) >= 2 and yield_score <= 0.20
        if high_affinity and high_yield:
            evolution_action = "anchor"
            if keyword_kind == "probe" and int(row["search_count"]) >= 3 and feedback_evidence >= 5:
                keyword_kind = "anchor"
            status = "active"
        elif high_affinity and low_yield:
            evolution_action = "rewrite"
            status = "active" if keyword_kind == "anchor" else "cooldown"
        elif low_affinity and high_yield:
            evolution_action = "downweight"
            status = "cooldown"
        elif low_affinity and low_yield:
            evolution_action = "retire"
            retire_ready = int(row["search_count"]) >= (
                12 if keyword_kind == "anchor" else 3
            ) and feedback_evidence >= (20 if keyword_kind == "anchor" else 5)
            status = "retired" if retire_ready else "cooldown"
        else:
            evolution_action = "observe"
            if (
                int(row["search_count"]) >= (12 if keyword_kind == "anchor" else 5)
                and int(row["new_candidate_count"]) == 0
            ):
                status = "retired" if keyword_kind == "probe" else "cooldown"
            elif shown >= 8 and dismiss_rate >= 0.7 and completed == 0 and likes == 0:
                status = "cooldown"
        conn.execute(
            """
            UPDATE discovery_keywords
            SET quality_score=?, affinity_score=?, yield_score=?,
                keyword_kind=?, evolution_action=?, status=?
            WHERE keyword_id=?
            """,
            (
                round(quality, 4),
                round(affinity, 4),
                round(yield_score, 4),
                keyword_kind,
                evolution_action,
                status,
                keyword_id,
            ),
        )

    @staticmethod
    def _recalculate_family(conn: Any, family_id: str) -> None:
        if not family_id:
            return
        rows = conn.execute(
            """
            SELECT quality_score, status, cooldown_until, keyword_kind,
                   affinity_exposure, affinity_positive, affinity_negative,
                   admitted_count
            FROM discovery_keywords WHERE family_id=?
            """,
            (family_id,),
        ).fetchall()
        if not rows:
            return
        active = [row for row in rows if row["status"] == "active"]
        status = (
            "active"
            if active
            else "cooldown"
            if any(row["status"] == "cooldown" for row in rows)
            else "retired"
        )
        quality = max(float(row["quality_score"] or 0.0) for row in rows)
        positive = sum(float(row["affinity_positive"] or 0.0) for row in rows)
        negative = sum(float(row["affinity_negative"] or 0.0) for row in rows)
        affinity = (positive + 2.0) / max(positive + negative + 4.0, 1e-9)
        family = conn.execute(
            "SELECT search_count, candidate_count, new_candidate_count FROM discovery_keyword_families WHERE family_id=?",
            (family_id,),
        ).fetchone()
        candidate_count = max(int(family["candidate_count"] or 0), 1)
        novelty = int(family["new_candidate_count"] or 0) / candidate_count
        per_search = min(
            int(family["new_candidate_count"] or 0)
            / max(int(family["search_count"] or 0), 1)
            / 8.0,
            1.0,
        )
        admission = sum(int(row["admitted_count"] or 0) for row in rows) / candidate_count
        yield_score = min(max(0.45 * novelty + 0.35 * per_search + 0.20 * admission, 0.0), 1.0)
        keyword_kind = "anchor" if any(row["keyword_kind"] == "anchor" for row in rows) else "probe"
        evidence = positive + negative
        exposure = sum(float(row["affinity_exposure"] or 0.0) for row in rows)
        decision_ready = exposure >= _env_float(
            "RECOMMEND_KEYWORD_MIN_EXPOSURES",
            DEFAULT_EXPLORATION_MIN_EXPOSURES,
        )
        if decision_ready and evidence >= 3 and affinity >= 0.62 and yield_score >= 0.45:
            action = "anchor"
        elif decision_ready and evidence >= 3 and affinity >= 0.62 and yield_score <= 0.20:
            action = "rewrite"
        elif decision_ready and evidence >= 3 and affinity <= 0.40 and yield_score >= 0.45:
            action = "downweight"
        elif decision_ready and evidence >= 3 and affinity <= 0.40 and yield_score <= 0.20:
            action = "retire"
        else:
            action = "observe"
        cooldowns = [str(row["cooldown_until"]) for row in rows if row["cooldown_until"]]
        conn.execute(
            """
            UPDATE discovery_keyword_families
            SET quality_score=?, affinity_score=?, yield_score=?, keyword_kind=?,
                evolution_action=?, status=?, cooldown_until=?, updated_at=?
            WHERE family_id=?
            """,
            (
                round(quality, 4),
                round(affinity, 4),
                round(yield_score, 4),
                keyword_kind,
                action,
                status,
                max(cooldowns) if cooldowns else None,
                _utc_now(),
                family_id,
            ),
        )


def _canonical_spec(
    value: dict[str, object] | None,
    *,
    query: str,
    source: str,
) -> tuple[dict[str, object], str, str, str, str | None]:
    raw = dict(value or _infer_query_spec(query))
    exploration_axis = str(raw.pop("exploration_axis", "") or source or "base")[:40]
    keyword_kind = str(
        raw.pop("keyword_kind", "")
        or ("anchor" if source in {"profile", "explicit", "l3_profile"} else "probe")
    )
    keyword_kind = "anchor" if keyword_kind == "anchor" else "probe"
    origin = str(raw.pop("origin", "") or source or "unknown")[:40]
    parent_keyword_id = str(raw.pop("parent_keyword_id", "") or "").strip() or None
    canonical: dict[str, object] = {}
    for key, item in sorted(raw.items()):
        if isinstance(item, (list, tuple, set)):
            values = sorted(
                set(str(entry).strip().casefold() for entry in item if str(entry).strip())
            )
            if values:
                canonical[key] = values
        else:
            text = str(item or "").strip().casefold()
            if text:
                canonical[key] = text
    if not canonical:
        canonical["topic"] = " ".join(query.casefold().split())[:160]
    return canonical, exploration_axis, keyword_kind, origin, parent_keyword_id


def _infer_query_spec(query: str) -> dict[str, object]:
    normalized = " ".join(query.casefold().split())
    subgenres = [
        subgenre
        for subgenre, terms in {
            "neo_soul": ("neo soul",),
            "funk_soul": ("funk soul",),
            "classic_blues": ("classic blues",),
            "blues_rock": ("blues rock", "布鲁斯摇滚", "蓝调摇滚"),
            "soul_rock": ("soul rock",),
            "indie_rock": ("indie rock",),
            "indie_pop": ("indie pop", "独立流行"),
            "synth_pop": ("synth pop",),
            "city_pop": ("city pop", "城市流行"),
            "jazz_rap": ("jazz rap",),
            "melodic_rap": ("melodic rap",),
            "ska": ("ska",),
            "dub_reggae": ("dub reggae",),
        }.items()
        if any(term in normalized for term in terms)
    ]
    genres = [
        genre
        for genre, terms in {
            "rnb": ("r&b", "rnb", "节奏布鲁斯"),
            "rock": ("rock", "摇滚"),
            "rap": ("rap", "hip-hop", "hiphop", "说唱", "嘻哈"),
            "reggae": ("reggae", "雷鬼", "dub", "ska"),
            "pop": ("pop", "流行", "city pop"),
            "soul": ("soul", "灵魂乐"),
            "funk": ("funk",),
        }.items()
        if any(term in normalized for term in terms)
    ]
    languages = [
        language
        for language, terms in {
            "english": ("英文", "english", "western", "欧美"),
            "chinese": ("中文", "华语", "国语"),
            "japanese": ("日语", "日文", "j-pop", "jpop"),
            "korean": ("韩语", "韩文", "k-pop", "kpop"),
        }.items()
        if any(term in normalized for term in terms)
    ]
    vocals = (
        ["female"]
        if any(term in normalized for term in ("女声", "女歌手", "female vocal", "female"))
        else []
    )
    moods = [
        mood
        for mood, terms in {
            "calm": ("安静", "轻柔", "calm"),
            "relaxed": ("放松", "chill", "relax"),
            "lyrical": ("抒情", "ballad"),
            "energetic": ("热血", "动感", "energetic"),
        }.items()
        if any(term in normalized for term in terms)
    ]
    if genres or subgenres or languages or vocals or moods:
        return {
            "genres": genres,
            "subgenres": subgenres,
            "languages": languages,
            "vocals": vocals,
            "moods": moods,
        }
    return {"topic": normalized[:160]}


def _embed_queries(queries: list[str]) -> list[list[float]] | None:
    base_url = os.getenv("AMEM_EMBEDDING_BASE_URL", "").rstrip("/")
    if not base_url or len(queries) < 2:
        return None
    try:
        response = requests.post(
            f"{base_url}/embeddings",
            json={"model": os.getenv("AMEM_EMBEDDING_MODEL", "bge-m3"), "input": queries},
            headers={"Authorization": f"Bearer {os.getenv('BGE_M3_API_KEY', 'local-embedding')}"},
            timeout=max(float(os.getenv("AMEM_EMBEDDING_TIMEOUT_SECONDS", "15")), 5.0),
        )
        response.raise_for_status()
        values = [
            item["embedding"]
            for item in sorted(response.json()["data"], key=lambda item: item["index"])
        ]
        return values if len(values) == len(queries) else None
    except Exception:
        return None


def _keyword_similarity(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    vectors: list[list[float]] | None,
    left_index: int,
    right_index: int,
) -> float:
    if left["familyId"] == right["familyId"]:
        return 1.0
    if vectors is not None:
        return max(0.0, _cosine(vectors[left_index], vectors[right_index]))
    left_tokens = _spec_tokens(left["canonicalSpec"])
    right_tokens = _spec_tokens(right["canonicalSpec"])
    return _jaccard(left_tokens, right_tokens)


def _spec_tokens(value: dict[str, object]) -> set[str]:
    tokens: set[str] = set()
    for key, item in value.items():
        tokens.add(str(key))
        if isinstance(item, list):
            tokens.update(str(entry) for entry in item)
        else:
            tokens.add(str(item))
    return tokens


def _cosine(left: list[float], right: list[float]) -> float:
    dot = sum(float(a) * float(b) for a, b in zip(left, right, strict=False))
    left_norm = math.sqrt(sum(float(value) ** 2 for value in left))
    right_norm = math.sqrt(sum(float(value) ** 2 for value in right))
    return dot / max(left_norm * right_norm, 1e-12)


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _json_strings(value: Any) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_future(value: Any, now: str) -> bool:
    return bool(value and str(value) > now)


def _decayed_search_metrics(conn: Any, keyword_id: str) -> dict[str, float]:
    rows = conn.execute(
        """
        SELECT candidate_count, new_candidate_count, admitted_count, observed_at
        FROM discovery_search_observations WHERE keyword_id=?
        """,
        (keyword_id,),
    ).fetchall()
    if not rows:
        current = conn.execute(
            """
            SELECT search_count, candidate_count, new_candidate_count, admitted_count
            FROM discovery_keywords WHERE keyword_id=?
            """,
            (keyword_id,),
        ).fetchone()
        return {
            "search_count": float(current["search_count"] or 0) if current else 0.0,
            "candidate_count": float(current["candidate_count"] or 0) if current else 0.0,
            "new_candidate_count": float(current["new_candidate_count"] or 0) if current else 0.0,
            "admitted_count": float(current["admitted_count"] or 0) if current else 0.0,
        }
    now = datetime.now(timezone.utc)
    half_life_days = _env_float(
        "RECOMMEND_KEYWORD_YIELD_HALF_LIFE_DAYS",
        DEFAULT_YIELD_HALF_LIFE_DAYS,
    )
    result = {
        "search_count": 0.0,
        "candidate_count": 0.0,
        "new_candidate_count": 0.0,
        "admitted_count": 0.0,
    }
    for row in rows:
        weight = _time_decay(
            row["observed_at"],
            now=now,
            half_life_days=half_life_days,
        )
        result["search_count"] += weight
        result["candidate_count"] += weight * int(row["candidate_count"] or 0)
        result["new_candidate_count"] += weight * int(row["new_candidate_count"] or 0)
        result["admitted_count"] += weight * int(row["admitted_count"] or 0)
    return result


def _time_decay(value: Any, *, now: datetime, half_life_days: float) -> float:
    try:
        observed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return 1.0
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    age_days = (
        max(
            (now - observed.astimezone(timezone.utc)).total_seconds(),
            0.0,
        )
        / 86400.0
    )
    return math.exp(-math.log(2.0) * age_days / max(half_life_days, 0.01))


def _env_float(name: str, default: float) -> float:
    try:
        return max(float(os.getenv(name, str(default))), 0.0)
    except ValueError:
        return default


def _keyword_id(user_id: str, query: str) -> str:
    digest = hashlib.sha1(f"{user_id}:bilibili:{query.casefold()}".encode("utf-8")).hexdigest()[:20]
    return f"keyword:{digest}"


def _family_id(user_id: str, canonical_json: str) -> str:
    digest = hashlib.sha1(
        f"{user_id}:bilibili-family:{canonical_json}".encode("utf-8")
    ).hexdigest()[:20]
    return f"keyword-family:{digest}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
