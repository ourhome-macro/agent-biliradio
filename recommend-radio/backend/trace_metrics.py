from __future__ import annotations
import json
import math
import statistics
from collections import Counter, defaultdict
from itertools import combinations
from typing import Any

from database import get_connection


def _token_count(value):
    # Older traces redacted every key containing 'token', including numeric usage.
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def summarize_traces(db_path: str, *, since: str | None = None) -> dict[str, Any]:
    trace_where = "WHERE started_at>=?" if since else ""
    parameters: tuple[object, ...] = (since,) if since else ()
    with get_connection(db_path) as conn:
        traces = conn.execute(
            f"SELECT * FROM evaluation_traces {trace_where} ORDER BY started_at",
            parameters,
        ).fetchall()
        spans = conn.execute(
            f"""
            SELECT s.* FROM evaluation_trace_spans s
            JOIN evaluation_traces t ON t.trace_id=s.trace_id
            {trace_where.replace("started_at", "t.started_at")}
            ORDER BY s.started_at
            """,
            parameters,
        ).fetchall()
        impressions = conn.execute(
            f"""
            SELECT * FROM recommendation_impressions
            {"WHERE shown_at>=?" if since else ""}
            ORDER BY shown_at
            """,
            parameters,
        ).fetchall()
        feedback = conn.execute(
            """
            SELECT recommendation_trace_id, track_id,
                   MAX(completed) AS completed,
                   MAX(negative) AS negative,
                   MAX(CASE WHEN event='liked' THEN 1 ELSE 0 END) AS liked,
                   MAX(CASE WHEN event='collection_added' THEN 1 ELSE 0 END) AS collected
            FROM recommendation_events
            WHERE recommendation_trace_id<>''
            GROUP BY recommendation_trace_id, track_id
            """
        ).fetchall()
        entity = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN l.track_id IS NOT NULL THEN 1 ELSE 0 END) AS grounded,
                   SUM(CASE WHEN COALESCE(l.confidence, 0)<0.5 THEN 1 ELSE 0 END) AS low_confidence
            FROM tracks t LEFT JOIN track_entity_links l ON l.track_id=t.track_id
            """
        ).fetchone()
        governance_rows = conn.execute(
            """
            SELECT evolution_action, COUNT(*) AS count
            FROM discovery_keywords GROUP BY evolution_action
            """
        ).fetchall()
        ungrounded_impressions = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM recommendation_impressions i
            LEFT JOIN tracks t ON t.track_id=i.track_id
            WHERE t.track_id IS NULL {"AND i.shown_at>=?" if since else ""}
            """,
            parameters,
        ).fetchone()[0]

    status_counts = Counter(str(row["status"]) for row in traces)
    terminal = sum(status_counts[value] for value in ("completed", "failed", "cancelled"))
    durations_by_type: dict[str, list[float]] = defaultdict(list)
    for row in traces:
        if row["duration_ms"] is not None:
            durations_by_type[str(row["trace_type"])].append(float(row["duration_ms"]))

    span_latencies: dict[str, list[float]] = defaultdict(list)
    tool_spans = []
    candidate_pool_spans = []
    admission_spans = []
    ranking_spans = []
    input_tokens = 0
    output_tokens = 0
    unknown_usage_attempts = 0
    sampling_attempts = 0
    known_cost_usd = 0.0
    unknown_cost_attempts = 0
    authoritative_trace_ids = {row["trace_id"] for row in spans if row["name"] == "llm.sampling"}
    authoritative_calls = {_json_object(row["output_json"]).get("modelCallId") for row in spans
                           if row["name"] == "llm.sampling"}
    authoritative_calls.discard(None)
    for row in spans:
        name = str(row["name"])
        span_latencies[name].append(float(row["duration_ms"] or 0.0))
        if row["kind"] == "tool":
            tool_spans.append(row)
        if name == "candidate_pool.read":
            candidate_pool_spans.append(row)
        if name == "candidate.admit":
            admission_spans.append(row)
        if name == "candidate.rank_select":
            ranking_spans.append(row)
        span_metrics = _json_object(row["metrics_json"])
        model_call_id = _json_object(row["output_json"]).get("model_call_id")
        fallback_attempt = (name in {"model.completed", "sampling.failed"}
                            and span_metrics.get("executionKind") == "model"
                            and model_call_id not in authoritative_calls)
        if name == "llm.sampling" and not span_metrics.get("usageKnown"):
            unknown_usage_attempts += 1
        if name == "llm.sampling" or fallback_attempt:
            sampling_attempts += 1
            value = span_metrics.get("costUsd")
            if span_metrics.get("costKnown") and type(value) in (int, float) and math.isfinite(value):
                known_cost_usd += value
            else:
                unknown_cost_attempts += 1
        if name == "llm.sampling" or (row["trace_id"] not in authoritative_trace_ids
                and model_call_id not in authoritative_calls
                and span_metrics.get("executionKind") != "control"):
            input_tokens += _token_count(span_metrics.get("inputTokens"))
            output_tokens += _token_count(span_metrics.get("outputTokens"))

    pool_hits = sum(
        int(int(_json_object(row["output_json"]).get("candidateCount") or 0) > 0)
        for row in candidate_pool_spans
    )
    recalled = sum(
        int(_json_object(row["output_json"]).get("resultCount") or 0) for row in admission_spans
    )
    admitted = sum(
        int(_json_object(row["output_json"]).get("admittedCount") or 0) for row in admission_spans
    )
    fallback_count = sum(
        _json_object(row["metrics_json"]).get("mode") == "lexical_fallback" for row in ranking_spans
    )
    ttfr_values = [
        float(value)
        for row in ranking_spans
        if (value := _json_object(row["metrics_json"]).get("ttfrMs")) is not None
    ]
    trace_roots = {str(row["trace_id"]): str(row["root_trace_id"]) for row in traces}
    failed_tool_roots = {
        trace_roots.get(str(row["trace_id"]), str(row["trace_id"]))
        for row in tool_spans
        if row["status"] != "completed"
    }
    completed_recommendation_roots = {
        str(row["root_trace_id"])
        for row in traces
        if row["trace_type"] == "recommendation" and row["status"] == "completed"
    }

    feedback_by_item = {
        (str(row["recommendation_trace_id"]), str(row["track_id"])): row for row in feedback
    }
    source_counts: Counter[str] = Counter()
    exploration = []
    positive = []
    slates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in impressions:
        snapshot = _json_object(row["candidate_snapshot_json"])
        source = str(snapshot.get("source") or "unknown")
        source_counts[source] += 1
        outcome = feedback_by_item.get((str(row["recommendation_trace_id"]), str(row["track_id"])))
        is_positive = bool(
            outcome and (outcome["completed"] or outcome["liked"] or outcome["collected"])
        )
        positive.append(int(is_positive))
        exploration.append(int(source == "explore"))
        slates[str(row["recommendation_trace_id"])].append(snapshot)

    diversity_values = [_slate_diversity(items) for items in slates.values() if len(items) > 1]
    explore_positive = [value for value, flag in zip(positive, exploration, strict=False) if flag]
    exploit_positive = [
        value for value, flag in zip(positive, exploration, strict=False) if not flag
    ]
    explore_rate = _optional_mean(explore_positive)
    exploit_rate = _optional_mean(exploit_positive)
    total_entities = int(entity["total"] or 0)

    return {
        "trace": {
            "count": len(traces),
            "statusCounts": dict(sorted(status_counts.items())),
            "successRate": _optional_ratio(status_counts["completed"], terminal),
            "latencyMs": {
                name: _distribution(values) for name, values in sorted(durations_by_type.items())
            },
            "spanLatencyMs": {
                name: _distribution(values) for name, values in sorted(span_latencies.items())
            },
            "meanSpansPerTrace": _ratio(len(spans), len(traces)),
            "timeToFirstResultMs": _distribution(ttfr_values),
            "tokenUsage": {"input": input_tokens, "output": output_tokens,
                           "unknownUsageAttempts": unknown_usage_attempts},
            "modelUsage": {"attempts": sampling_attempts, "knownCostUsd": round(known_cost_usd, 8),
                           "unknownCostAttempts": unknown_cost_attempts},
        },
        "process": {
            "toolCallSuccessRate": _optional_ratio(
                sum(row["status"] == "completed" for row in tool_spans),
                len(tool_spans),
            ),
            "candidatePoolHitRate": _optional_ratio(pool_hits, len(candidate_pool_spans)),
            "invalidRecallRate": _optional_ratio(max(recalled - admitted, 0), recalled),
            "mmrFallbackRate": _optional_ratio(fallback_count, len(ranking_spans)),
            "failureRecoveryRate": _optional_ratio(
                len(failed_tool_roots & completed_recommendation_roots),
                len(failed_tool_roots),
            ),
            "recallSourceContribution": _normalized_counts(source_counts),
            "governanceActionDistribution": {
                str(row["evolution_action"] or "observe"): int(row["count"])
                for row in governance_rows
            },
        },
        "result": {
            "impressionCount": len(impressions),
            "completionOrLikeRate": _optional_mean(positive),
            "intraListDiversity": _optional_mean(diversity_values),
            "explorationRatio": _optional_mean(exploration),
            "explorationPositiveRate": explore_rate,
            "exploitationPositiveRate": exploit_rate,
            "explorationGain": (
                round(explore_rate / exploit_rate, 4)
                if explore_rate is not None and exploit_rate is not None and exploit_rate > 0
                else None
            ),
            "propensityCoverage": _optional_ratio(
                sum(row["selection_propensity"] is not None for row in impressions),
                len(impressions),
            ),
        },
        "trust": {
            "entityGroundingRate": _optional_ratio(int(entity["grounded"] or 0), total_entities),
            "lowConfidenceEntityRate": _optional_ratio(
                int(entity["low_confidence"] or 0), total_entities
            ),
            "databaseUngroundedImpressionCount": int(ungrounded_impressions or 0),
            "hallucinatedAssetCount": None,
            "deadLinkRate": None,
            "hallucinatedAssetMetricNote": "Database grounding is enforced by foreign keys; hallucination and dead-link rates require a scheduled live BVID probe.",
        },
    }


def _slate_diversity(items: list[dict[str, Any]]) -> float:
    distances = []
    for left, right in combinations(items, 2):
        left_tokens = _candidate_tokens(left)
        right_tokens = _candidate_tokens(right)
        if not left_tokens and not right_tokens:
            continue
        similarity = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
        distances.append(1.0 - similarity)
    return _mean(distances)


def _candidate_tokens(value: dict[str, Any]) -> set[str]:
    fields = [
        value.get("workId"),
        value.get("canonicalArtist"),
        value.get("versionType"),
        *(value.get("tags") or []),
        *((value.get("facets") or {}).get("genres") or []),
    ]
    return {str(item).casefold() for item in fields if str(item or "").strip()}


def _distribution(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
    }


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    offset = (len(ordered) - 1) * percentile / 100
    lower = math.floor(offset)
    upper = math.ceil(offset)
    if lower == upper:
        return round(ordered[lower], 4)
    return round(
        ordered[lower] + (ordered[upper] - ordered[lower]) * (offset - lower),
        4,
    )


def _mean(values: list[float] | list[int]) -> float:
    return round(statistics.mean(values), 4) if values else 0.0


def _optional_mean(values: list[float] | list[int]) -> float | None:
    return round(statistics.mean(values), 4) if values else None


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return round(float(numerator) / float(denominator), 4) if denominator else 0.0


def _optional_ratio(
    numerator: int | float,
    denominator: int | float,
) -> float | None:
    return round(float(numerator) / float(denominator), 4) if denominator else None


def _normalized_counts(values: Counter[str]) -> dict[str, float]:
    total = sum(values.values())
    return {key: _ratio(count, total) for key, count in sorted(values.items())}


def _json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
