"""Run deterministic Agent golden sets and aggregate persisted full-trace metrics."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from amem_bridge import _ensure_amem_import_path  # noqa: E402

_ensure_amem_import_path()

from dialogue_rules import _route_message  # noqa: E402
from discovery_planner import DiscoveryPlanner  # noqa: E402
from full_trace import _safe_json  # noqa: E402
from models import Track  # noqa: E402
from music_entity import resolve_track_entity  # noqa: E402
from music_profile import MusicProfile  # noqa: E402
from recommendation_engine import RecommendationEngine, RecommendationRequest  # noqa: E402
from recommendation_service import RecommendationCandidate, RecommendationService  # noqa: E402
from recommendation_contracts import UserProfile  # noqa: E402
from recommendation_policy import RecommendationPolicy  # noqa: E402
from request_spec import RequestInterpreter, RequestSpec  # noqa: E402
from trace_metrics import summarize_traces  # noqa: E402

from agent_memory_runtime.agent.orchestration.models import (  # noqa: E402
    AgentGraph,
    DelegatedTask,
)
from agent_memory_runtime.domain.memory import MemoryRecord  # noqa: E402
from agent_memory_runtime.domain.query import MemoryQuery  # noqa: E402
from agent_memory_runtime.memory.retrieval.pipeline import RetrievalPipeline  # noqa: E402

DEFAULT_GOLDEN_DIR = ROOT / "evals" / "golden"
DEFAULT_GATES = ROOT / "evals" / "gates.v1.json"


class _GoldenEmbeddingService:
    text_model = "golden-vector"

    def __init__(self, query_vector: list[float]) -> None:
        self.query_vector = query_vector

    def embed_query(self, _text: str) -> list[float]:
        return list(self.query_vector)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--golden-dir", default=str(DEFAULT_GOLDEN_DIR))
    parser.add_argument("--gates", default=str(DEFAULT_GATES))
    parser.add_argument("--db-path", default="")
    parser.add_argument("--since", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--fail-on-gate", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="Print summary; retain full JSON output")
    args = parser.parse_args()

    cases, golden_hash = _load_cases(Path(args.golden_dir))
    report = evaluate_cases(cases, golden_hash=golden_hash)
    if args.db_path:
        report["observedTraceMetrics"] = summarize_traces(
            str(Path(args.db_path).resolve()),
            since=args.since or None,
        )
    gates = _json_file(Path(args.gates)) if args.gates else {}
    report["gates"] = evaluate_gates(report, gates)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(
        json.dumps({key: report[key] for key in ("caseCount", "passedCount", "failedCount", "gates")})
        if args.quiet else rendered
    )
    if args.fail_on_gate and not report["gates"]["passed"]:
        raise SystemExit(1)


def evaluate_cases(cases: list[dict[str, Any]], *, golden_hash: str) -> dict[str, Any]:
    results = []
    layer_passes: dict[str, list[int]] = defaultdict(list)
    task_passes: dict[str, list[int]] = defaultdict(list)
    ranking_ndcg = []
    ranking_hits = []
    ranking_diversity = []
    grounding_values = []
    forbidden_violations = 0
    selected_count = 0

    for case in cases:
        try:
            result = _evaluate_case(case)
        except Exception as exc:
            result = {
                "id": case.get("id"),
                "layer": case.get("layer"),
                "task": case.get("task"),
                "passed": False,
                "error": type(exc).__name__,
                "message": str(exc)[:300],
            }
        results.append(result)
        layer = str(result.get("layer") or "unknown")
        task = str(result.get("task") or "unknown")
        passed = int(bool(result.get("passed")))
        layer_passes[layer].append(passed)
        task_passes[task].append(passed)
        metrics = result.get("metrics") or {}
        if task == "ranking":
            ranking_ndcg.append(float(metrics.get("ndcgAt8") or 0.0))
            ranking_hits.append(float(metrics.get("hitAt8") or 0.0))
            ranking_diversity.append(float(metrics.get("intraListDiversity") or 0.0))
            forbidden_violations += int(metrics.get("forbiddenViolations") or 0)
            selected_count += int(metrics.get("selectedCount") or 0)
        if task == "ranking" and "groundingRate" in metrics:
            grounding_values.append(float(metrics["groundingRate"]))

    passed_count = sum(int(result["passed"]) for result in results)
    metrics = {
        "overallPassRate": _ratio(passed_count, len(results)),
        "capabilityPassRate": _mean(layer_passes.get("capability", [])),
        "businessPassRate": _mean(layer_passes.get("business", [])),
        "adversarialPassRate": _mean(layer_passes.get("adversarial", [])),
        "routeAccuracy": _mean(task_passes.get("dialogue_route", [])),
        "requestSpecAccuracy": _mean(task_passes.get("request_spec", [])),
        "entityResolutionAccuracy": _mean(task_passes.get("entity_resolution", [])),
        "sceneRestorationAccuracy": _mean(task_passes.get("scene_restore", [])),
        "groundingRate": _mean(grounding_values),
        "groundingDetectionAccuracy": _mean(task_passes.get("asset_grounding", [])),
        "memoryRetrievalAccuracy": _mean(task_passes.get("memory_retrieval", [])),
        "rankingNdcgAt8": _mean(ranking_ndcg),
        "rankingHitAt8": _mean(ranking_hits),
        "intraListDiversity": _mean(ranking_diversity),
        "forbiddenViolationRate": _ratio(forbidden_violations, selected_count),
    }
    return {
        "suiteVersion": "music-agent-golden-v1",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "goldenHash": golden_hash,
        "caseCount": len(results),
        "passedCount": passed_count,
        "failedCount": len(results) - passed_count,
        "metrics": metrics,
        "layers": {
            layer: {
                "count": len(values),
                "passed": sum(values),
                "passRate": _mean(values),
            }
            for layer, values in sorted(layer_passes.items())
        },
        "tasks": {
            task: {"count": len(values), "passRate": _mean(values)}
            for task, values in sorted(task_passes.items())
        },
        "cases": results,
        "limitations": [
            "Programmatic golden cases measure deterministic contracts, not online retention.",
            "Ranking cases use frozen candidate slates and cannot estimate unexposed counterfactual reward.",
            "LLM-as-judge is intentionally excluded until calibrated against human labels.",
        ],
    }


def evaluate_gates(report: dict[str, Any], gates: dict[str, Any]) -> dict[str, Any]:
    failures = []
    metrics = report.get("metrics") or {}
    for name, expected in (gates.get("minimum") or {}).items():
        actual = metrics.get(name)
        if not math.isfinite(float(expected)):
            raise ValueError("Gate thresholds must be finite numbers")
        if actual is None or not math.isfinite(float(actual)) or float(actual) < float(expected):
            failures.append(
                {"metric": name, "operator": ">=", "expected": expected, "actual": actual}
            )
    for name, expected in (gates.get("maximum") or {}).items():
        actual = metrics.get(name)
        if not math.isfinite(float(expected)):
            raise ValueError("Gate thresholds must be finite numbers")
        if actual is None or not math.isfinite(float(actual)) or float(actual) > float(expected):
            failures.append(
                {"metric": name, "operator": "<=", "expected": expected, "actual": actual}
            )
    return {"passed": not failures, "failures": failures}


def _evaluate_case(case: dict[str, Any]) -> dict[str, Any]:
    task = str(case["task"])
    handlers = {
        "request_spec": _request_spec_case,
        "dialogue_route": _dialogue_route_case,
        "entity_resolution": _entity_resolution_case,
        "scene_restore": _scene_restore_case,
        "asset_grounding": _asset_grounding_case,
        "trace_redaction": _trace_redaction_case,
        "discovery_plan": _discovery_plan_case,
        "ranking": _ranking_case,
        "agent_graph": _agent_graph_case,
        "memory_retrieval": _memory_retrieval_case,
    }
    if task not in handlers:
        raise ValueError(f"unsupported golden task: {task}")
    details = handlers[task](case.get("input") or {}, case.get("expected") or {})
    return {
        "id": case["id"],
        "layer": case["layer"],
        "task": task,
        **details,
    }


def _request_spec_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    actual = RequestInterpreter().interpret(str(inputs.get("message") or "")).to_dict()
    mismatches = _subset_mismatches(actual, expected)
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _dialogue_route_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    route = _route_message(str(inputs.get("message") or ""), None)
    actual = {
        "tool": route.tool,
        "primaryIntent": route.primary_intent,
        "needProfile": route.need_profile,
        "needMemory": route.need_memory,
        "needRecommendationSearch": route.need_recommendation_search,
        "controlAction": route.control_action,
    }
    mismatches = _subset_mismatches(actual, expected)
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _entity_resolution_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    entities = [resolve_track_entity(Track.from_dict(item)) for item in inputs.get("tracks") or []]
    actual = [
        {
            "canonicalTitle": item.canonical_title,
            "canonicalArtist": item.canonical_artist,
            "versionType": item.version_type,
            "confidence": item.confidence,
        }
        for item in entities
    ]
    mismatches = []
    for index, value in enumerate(expected.get("entities") or []):
        mismatches.extend(
            f"entities[{index}].{item}" for item in _subset_mismatches(actual[index], value)
        )
    for left, right in expected.get("sameWorkPairs") or []:
        if entities[left].work_id != entities[right].work_id:
            mismatches.append(f"sameWorkPairs:{left},{right}")
    for left, right in expected.get("differentWorkPairs") or []:
        if entities[left].work_id == entities[right].work_id:
            mismatches.append(f"differentWorkPairs:{left},{right}")
    for left, right in expected.get("differentRecordingPairs") or []:
        if entities[left].recording_id == entities[right].recording_id:
            mismatches.append(f"differentRecordingPairs:{left},{right}")
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _scene_restore_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    incoming = RequestInterpreter().interpret(str(inputs.get("message") or ""))
    active = RequestSpec.from_dict(inputs.get("activeRequestSpec") or {})
    resolved, restored = RecommendationService._resolve_request_spec(incoming, active)
    actual = {"restored": restored, "requestSpec": resolved.to_dict()}
    mismatches = _subset_mismatches(actual, expected)
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _asset_grounding_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    bvid = str(inputs.get("bvid") or "")
    track_id = str(inputs.get("trackId") or "")
    grounded = bool(re.fullmatch(r"BV[0-9A-Za-z]{10}", bvid)) and track_id.startswith(
        f"bili:{bvid}"
    )
    actual = {"grounded": grounded}
    mismatches = _subset_mismatches(actual, expected)
    return {
        "passed": not mismatches,
        "actual": actual,
        "mismatches": mismatches,
        "metrics": {"groundingRate": float(grounded)},
    }


def _trace_redaction_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    rendered = _safe_json(inputs.get("payload") or {})
    forbidden = [str(item) for item in expected.get("forbiddenSubstrings") or []]
    required = [str(item) for item in expected.get("requiredSubstrings") or []]
    mismatches = [f"leaked:{item}" for item in forbidden if item in rendered]
    mismatches.extend(f"missing:{item}" for item in required if item not in rendered)
    return {"passed": not mismatches, "actual": rendered, "mismatches": mismatches}


def _discovery_plan_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    profile = MusicProfile.from_dict(inputs.get("profile") or {}, source="golden")
    request = RequestSpec.from_dict(inputs.get("requestSpec") or {})
    plan = DiscoveryPlanner(search_budget=int(inputs.get("searchBudget") or 3)).plan(
        profile=profile,
        request_spec=request,
        scene=str(inputs.get("scene") or "home"),
    )
    semantic_external = sum(query in plan.semantic_queries for query in plan.search_queries)
    actual = {
        "externalQueryCount": len(plan.search_queries),
        "semanticQueryCount": len(plan.semantic_queries),
        "semanticExternalCount": semantic_external,
        "requestFirst": plan.request_first,
        "queries": plan.search_queries,
    }
    mismatches = []
    if len(plan.search_queries) > int(expected.get("maxExternalQueries", 10**6)):
        mismatches.append("maxExternalQueries")
    if len(plan.semantic_queries) < int(expected.get("minSemanticQueries", 0)):
        mismatches.append("minSemanticQueries")
    if semantic_external > int(expected.get("maxSemanticExternal", 10**6)):
        mismatches.append("maxSemanticExternal")
    if "requestFirst" in expected and plan.request_first != bool(expected["requestFirst"]):
        mismatches.append("requestFirst")
    for blocked in expected.get("blockedTermsAbsent") or []:
        if any(str(blocked).casefold() in query.casefold() for query in plan.search_queries):
            mismatches.append(f"blocked:{blocked}")
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _ranking_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    query_vector = [float(item) for item in inputs.get("queryVector") or [1.0, 0.0]]
    profile = MusicProfile.from_dict(inputs.get("profile") or {}, source="golden")
    request_spec = RequestSpec.from_dict(inputs.get("requestSpec") or {})
    candidates = []
    for value in inputs.get("candidates") or []:
        candidates.append(
            RecommendationCandidate(
                track=dict(value["track"]),
                score=float(value.get("score") or 0.0),
                source=str(value.get("source") or "golden"),
                reason="golden",
                tags=[str(item) for item in value.get("tags") or []],
                facets={
                    str(key): [str(item) for item in items]
                    for key, items in (value.get("facets") or {}).items()
                },
                text_embedding=[float(item) for item in value.get("textEmbedding") or []],
                audio_embedding=[float(item) for item in value.get("audioEmbedding") or []],
            )
        )
    # Serving receives only scenario inputs. Expected labels never select/filter candidates.
    limit = int(inputs.get("limit") or 8)
    profile.same_uploader_limit = int(inputs.get("sameUploaderLimit") or profile.same_uploader_limit)
    legacy_profile = UserProfile(
        **{name: set(values) for name, values in (inputs.get("legacyProfile") or {}).items()}
    )
    engine = RecommendationEngine(
        embedding_service=_GoldenEmbeddingService(query_vector), policy=RecommendationPolicy("golden")
    )
    _reranked, selected, diagnostics = engine.rank_and_select(
        candidates,
        request=RecommendationRequest(
            scene=str(inputs.get("scene") or "home"),
            limit=limit,
            request_spec=request_spec,
            profile=profile,
            exclude_track_ids=set(),
            recent_context={},
        ),
        legacy_profile=legacy_profile,
    )
    selected_ids = [str(item.track.get("trackId") or "") for item in selected]
    forbidden = {str(item) for item in expected.get("forbiddenIds") or []}
    relevance = {str(key): int(value) for key, value in (expected.get("relevance") or {}).items()}
    ndcg = _ndcg(selected_ids, relevance, limit)
    hit = float(any(relevance.get(item, 0) >= 2 for item in selected_ids))
    diversity = _candidate_diversity(selected)
    violations = len(forbidden & set(selected_ids))
    required = {str(item) for item in expected.get("requiredIds") or []}
    grounding = _mean(
        [
            int(bool(re.fullmatch(r"BV[0-9A-Za-z]{10}", str(item.track.get("bvid") or ""))))
            for item in selected
        ]
    )
    mismatches = []
    if not required <= set(selected_ids):
        mismatches.append("requiredIds")
    if violations:
        mismatches.append("forbiddenIds")
    if ndcg < float(expected.get("minNdcgAt8", 0.0)):
        mismatches.append("minNdcgAt8")
    if diversity < float(expected.get("minDiversity", 0.0)):
        mismatches.append("minDiversity")
    return {
        "passed": not mismatches,
        "actual": {"selectedIds": selected_ids, "diagnostics": diagnostics},
        "mismatches": mismatches,
        "metrics": {
            "ndcgAt8": ndcg,
            "hitAt8": hit,
            "intraListDiversity": diversity,
            "forbiddenViolations": violations,
            "selectedCount": len(selected),
            "groundingRate": grounding,
        },
    }


def _agent_graph_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    try:
        graph = AgentGraph(
            tasks=tuple(
                DelegatedTask(
                    task_id=str(item["taskId"]),
                    agent_id=str(item["agentId"]),
                    message=str(item.get("message") or item["taskId"]),
                    depends_on=tuple(str(value) for value in item.get("dependsOn") or []),
                )
                for item in inputs.get("tasks") or []
            ),
            output_task_ids=tuple(str(value) for value in inputs.get("outputTaskIds") or []),
        )
    except ValueError as exc:
        actual = {"valid": False, "error": str(exc)}
    else:
        actual = {
            "valid": True,
            "maximumDepth": graph.maximum_depth,
            "maximumFanOut": graph.maximum_fan_out,
            "outputTaskIds": list(graph.resolved_output_task_ids),
        }
    mismatches = _subset_mismatches(actual, expected)
    return {"passed": not mismatches, "actual": actual, "mismatches": mismatches}


def _memory_retrieval_case(inputs: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    records = [MemoryRecord.from_dict(item) for item in inputs.get("records") or []]
    query_value = dict(inputs.get("query") or {})
    for name in (
        "memory_types",
        "tags",
        "source_memory_ids",
        "levels",
        "statuses",
        "visibilities",
        "temperatures",
    ):
        if name in query_value:
            query_value[name] = tuple(str(item) for item in query_value[name])
    query = MemoryQuery(**query_value)
    selected, trace = RetrievalPipeline().retrieve(records, query)
    selected_ids = [item.memory_id for item in selected]
    required = {str(item) for item in expected.get("selectedContains") or []}
    forbidden = {str(item) for item in expected.get("forbiddenAbsent") or []}
    mismatches = []
    if not required <= set(selected_ids):
        mismatches.append("selectedContains")
    forbidden_hits = forbidden & set(selected_ids)
    if forbidden_hits:
        mismatches.append("forbiddenAbsent")
    return {
        "passed": not mismatches,
        "actual": {
            "selectedIds": selected_ids,
            "candidateCount": trace.candidate_count,
            "blockedCount": trace.blocked_count,
        },
        "mismatches": mismatches,
        "metrics": {
            "retrievalRecall": _ratio(len(required & set(selected_ids)), len(required)),
            "forbiddenHits": len(forbidden_hits),
        },
    }


def _load_cases(directory: Path) -> tuple[list[dict[str, Any]], str]:
    cases = []
    digest = hashlib.sha256()
    for path in sorted(directory.glob("*.jsonl")):
        content = path.read_bytes()
        digest.update(path.name.encode("utf-8"))
        digest.update(content)
        for line_number, line in enumerate(content.decode("utf-8").splitlines(), start=1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            value = json.loads(line)
            if (
                not isinstance(value, dict)
                or not {"id", "layer", "task", "input", "expected"} <= value.keys()
                or not isinstance(value["input"], dict)
                or not isinstance(value["expected"], dict)
                or value["layer"] not in {"capability", "business", "adversarial"}
            ):
                raise ValueError(f"invalid golden case at {path}:{line_number}")
            cases.append(value)
    if not cases:
        raise ValueError(f"no golden cases found in {directory}")
    ids = [str(case["id"]) for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("golden case IDs must be unique")
    manifest_path = directory.parent / "manifest.v1.json"
    if manifest_path.is_file():
        manifest = _json_file(manifest_path)
        required = set(manifest["requiredCaseIds"])
        if not required or not required <= set(ids) or len(cases) < manifest["minimumCaseCount"]:
            raise ValueError("Golden suite is incomplete: required cases are missing")
    return cases, digest.hexdigest()


def _subset_mismatches(actual: Any, expected: Any, *, prefix: str = "") -> list[str]:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return [prefix or "value"]
        result = []
        for key, value in expected.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in actual:
                result.append(path)
            else:
                result.extend(_subset_mismatches(actual[key], value, prefix=path))
        return result
    if isinstance(expected, list):
        actual_values = list(actual) if isinstance(actual, (list, tuple)) else []
        return [] if actual_values == expected else [prefix or "value"]
    return [] if actual == expected else [prefix or "value"]


def _candidate_diversity(candidates: list[RecommendationCandidate]) -> float:
    distances = []
    for left, right in combinations(candidates, 2):
        left_tokens = _candidate_tokens(left)
        right_tokens = _candidate_tokens(right)
        similarity = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
        distances.append(1.0 - similarity)
    return _mean(distances)


def _candidate_tokens(candidate: RecommendationCandidate) -> set[str]:
    values = [
        candidate.track.get("workId"),
        candidate.track.get("canonicalArtist"),
        candidate.track.get("versionType"),
        *candidate.tags,
        *(candidate.facets.get("genres") or []),
    ]
    return {str(value).casefold() for value in values if str(value or "").strip()}


def _ndcg(selected: list[str], relevance: dict[str, int], k: int) -> float:
    dcg = sum(
        (2 ** relevance.get(item, 0) - 1) / math.log2(index + 2)
        for index, item in enumerate(selected[:k])
    )
    ideal = sorted(relevance.values(), reverse=True)[:k]
    idcg = sum((2**grade - 1) / math.log2(index + 2) for index, grade in enumerate(ideal))
    return round(dcg / idcg, 4) if idcg else 0.0


def _json_file(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _mean(values: list[int] | list[float]) -> float:
    return round(statistics.mean(values), 4) if values else 0.0


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


if __name__ == "__main__":
    main()
