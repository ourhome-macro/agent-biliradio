from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from recommendation_policy import RecommendationPolicy

from request_spec import RequestSpec


@dataclass(frozen=True)
class RecommendationRequest:
    scene: str
    limit: int
    request_spec: RequestSpec
    profile: Any
    exclude_track_ids: set[str]
    recent_context: dict[str, Any]


class RecommendationEngine:
    """Serving-only engine: scope filtering, score ordering, diversity and final selection."""

    def __init__(
        self,
        *,
        embedding_service: Any | None = None,
        user_id: str = "legacy-owner",
        policy: RecommendationPolicy | None = None,
    ) -> None:
        self.embedding_service = embedding_service
        self.policy = policy or RecommendationPolicy(user_id)

    def score(self, draft, legacy_profile, profile, trace_id, request_spec):
        return self.policy._score_candidate(draft, legacy_profile, profile, trace_id, request_spec)

    def rank_and_select(
        self,
        candidates: list[Any],
        *,
        request: RecommendationRequest,
        legacy_profile: Any,
    ) -> tuple[list[Any], list[Any], dict[str, Any]]:
        hard_rejected = []
        facet_rejected = []
        eligible = []
        for item in candidates:
            if (
                not re.fullmatch(r"BV[0-9A-Za-z]{10}", str(item.track.get("bvid") or ""))
                or self.policy._is_hard_filtered(item, request.profile, legacy_profile)
                or item.track["trackId"] in legacy_profile.recently_recommended_track_ids
            ):
                hard_rejected.append(item)
                continue
            if not request.request_spec.matches_candidate(
                item.track, getattr(item, "facets", {})
            ):
                facet_rejected.append(item)
                continue
            eligible.append(item)
        eligible.sort(key=lambda item: float(getattr(item, "score", 0)), reverse=True)
        reranked = eligible[: min(len(eligible), max(request.limit * 4, 16))]
        mmr_pool = eligible[: min(len(eligible), max(request.limit * 2, 16))]
        mmr_ranked, diagnostics = self._vector_mmr(
            mmr_pool,
            request,
            output_limit=min(len(mmr_pool), max(request.limit * 4, 16)),
        )
        diagnostics.update(
            {
                "inputCandidateCount": len(candidates),
                "hardFilteredCount": len(hard_rejected),
                "requestFacetFilteredCount": len(facet_rejected),
                "eligibleCount": len(eligible),
            }
        )

        def diversity(values):
            return self.policy._apply_diversity_limits(
                values,
                request.profile.same_uploader_limit,
                request.limit,
                request_scoped_limit=None if request.request_spec.constrained else 2,
            )

        if request.request_spec.required_scenes:
            selected = diversity(mmr_ranked)
        else:
            selected = diversity(
                self.policy._select_epsilon_greedy(
                    mmr_ranked,
                    request.limit,
                    request.scene,
                    legacy_profile,
                    request.profile,
                )
            )
        if len(selected) < request.limit:
            selected_ids = {
                str((getattr(item, "track", {}) or {}).get("trackId") or "") for item in selected
            }
            fill_pool = list(selected)
            for item in [*mmr_ranked, *eligible]:
                track_id = str((getattr(item, "track", {}) or {}).get("trackId") or "")
                if not track_id or track_id in selected_ids:
                    continue
                selected_ids.add(track_id)
                fill_pool.append(item)
            selected = diversity(fill_pool)
        return reranked, selected[: request.limit], diagnostics

    def _vector_mmr(
        self,
        candidates: list[Any],
        request: RecommendationRequest,
        *,
        output_limit: int,
    ) -> tuple[list[Any], dict[str, Any]]:
        if len(candidates) <= 1:
            return candidates[:output_limit], {
                "mode": "none",
                "candidateCount": len(candidates),
                "embeddingMs": 0.0,
            }
        document_vectors = [list(getattr(item, "text_embedding", []) or []) for item in candidates]
        dimensions = {len(vector) for vector in document_vectors if vector}
        if (
            self.embedding_service is None
            or len(dimensions) != 1
            or any(not vector for vector in document_vectors)
        ):
            return self._lexical_mmr(candidates, output_limit), {
                "mode": "lexical_fallback",
                "candidateCount": len(candidates),
                "embeddingMs": 0.0,
                "reason": "persisted_candidate_vectors_incomplete",
            }
        profile = request.profile
        query = request.request_spec.raw_text if request.request_spec.required_scenes else (
            " ".join(
                item
                for item in [
                    request.request_spec.raw_text,
                    " ".join(getattr(profile, "positive_topics", {}).keys()),
                    " ".join(getattr(profile, "mood_weights", {}).keys()),
                    getattr(profile, "music_persona", ""),
                    getattr(profile, "current_music_phase", ""),
                    " ".join(getattr(profile, "core_traits", [])),
                ]
                if item
            )
            or "personalized music recommendation"
        )
        negative_query = "" if request.request_spec.required_scenes else " ".join(
            [
                " ".join(getattr(profile, "negative_topics", {}).keys()),
                " ".join(getattr(profile, "negative_interest_texts", [])),
                " ".join(
                    str(item) for item in request.recent_context.get("negativeSamples", [])[:12]
                ),
            ]
        ).strip()
        query_vector = self.embedding_service.embed_query(query)
        negative_vector = (
            self.embedding_service.embed_query(negative_query) if negative_query else None
        )
        if not query_vector or len(query_vector) not in dimensions:
            return self._lexical_mmr(candidates, output_limit), {
                "mode": "lexical_fallback",
                "candidateCount": len(candidates),
                "negativeSampleCount": len(request.recent_context.get("negativeSamples", [])),
                "embeddingMs": 0.0,
                "reason": "query_embedding_unavailable",
            }
        if negative_vector and len(negative_vector) not in dimensions:
            negative_vector = None
        audio_vectors = [list(getattr(item, "audio_embedding", []) or []) for item in candidates]
        audio_dimensions = {len(vector) for vector in audio_vectors if vector}
        diversity_vectors = (
            audio_vectors if len(audio_dimensions) == 1 and all(audio_vectors) else document_vectors
        )
        diversity_space = "audio" if diversity_vectors is audio_vectors else "text"
        pending = list(range(len(candidates)))
        selected: list[int] = []
        scores = [float(getattr(item, "score", 0.0)) for item in candidates]
        low, high = min(scores), max(scores)
        while pending and len(selected) < output_limit:

            def mmr(index: int) -> float:
                relevance = 0.5 if high == low else (scores[index] - low) / (high - low)
                query_similarity = _cosine(query_vector, document_vectors[index])
                negative_similarity = (
                    max(_cosine(negative_vector, document_vectors[index]), 0.0)
                    if negative_vector is not None
                    else 0.0
                )
                diversity = max(
                    (
                        _cosine(diversity_vectors[index], diversity_vectors[other])
                        for other in selected
                    ),
                    default=0.0,
                )
                return (
                    0.45 * relevance
                    + 0.35 * query_similarity
                    - 0.20 * diversity
                    - 0.20 * negative_similarity
                )

            best = max(pending, key=mmr)
            pending.remove(best)
            selected.append(best)
        return [candidates[index] for index in selected], {
            "mode": "bge_m3_vector",
            "model": self.embedding_service.text_model,
            "candidateCount": len(candidates),
            "negativeSampleCount": len(request.recent_context.get("negativeSamples", [])),
            "embeddingMs": 0.0,
            "candidateVectors": "persisted",
            "diversitySpace": diversity_space,
        }

    @staticmethod
    def _lexical_mmr(candidates: list[Any], limit: int) -> list[Any]:
        """Deterministic fallback used only when bge-m3 is unavailable."""
        pending = list(candidates)
        selected: list[Any] = []
        while pending and len(selected) < limit:
            best = max(pending, key=lambda item: RecommendationEngine._mmr_score(item, selected))
            pending.remove(best)
            selected.append(best)
        return selected

    @staticmethod
    def _mmr_score(candidate: Any, selected: list[Any]) -> float:
        relevance = float(getattr(candidate, "score", 0.0))
        if not selected:
            return relevance
        similarity = max(_jaccard(_tokens(candidate), _tokens(other)) for other in selected)
        return relevance - 0.35 * similarity


def _tokens(candidate: Any) -> set[str]:
    track = getattr(candidate, "track", {}) or {}
    text = " ".join(
        str(value or "")
        for value in (
            track.get("title"),
            track.get("owner"),
            " ".join(getattr(candidate, "tags", [])),
        )
    ).casefold()
    return set(re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]{1,3}", text))


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _cosine(left: list[float], right: list[float]) -> float:
    dot = sum(float(a) * float(b) for a, b in zip(left, right, strict=False))
    left_norm = math.sqrt(sum(float(value) ** 2 for value in left))
    right_norm = math.sqrt(sum(float(value) ** 2 for value in right))
    return dot / max(left_norm * right_norm, 1e-12)
