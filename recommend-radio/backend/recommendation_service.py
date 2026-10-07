from __future__ import annotations

from music_agent import music_operation
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from amem_bridge import NoopAmemBridge, record_music_behavior
from bili_client import BiliClient
from candidate_pool import CandidatePool
from content_embeddings import ContentEmbeddingService
from database import DEFAULT_DB_PATH, LEGACY_OWNER_USER_ID, get_connection, init_db
from discovery_planner import DiscoveryPlanner
from discovery_service import DiscoveryService
from experiments import ExperimentAssignments
from full_trace import FullTrace, current_trace_id, hash_text
from keyword_governance import KeywordGovernance
from library_service import LibraryService
from memory_lifecycle import SceneMemoryService
from models import Track
from music_profile import MusicProfile
from profile_projector import ProfileProjection, ProfileProjector
from profile_statement_service import ProfileStatementService
from profile_update import MusicProfileUpdatePipeline
from recommendation_engine import RecommendationEngine, RecommendationRequest
from request_spec import RequestSpec
from settings_service import SettingsService
from recommendation_contracts import (
    CandidateDraft,
    DEFAULT_POOL_TARGET,
    DEFAULT_RECOMMENDATION_LIMIT,
    MAX_RECOMMENDATION_LIMIT,
    MEMORY_EVIDENCE_EVENTS,
    PROFILE_LIFECYCLE_EVENTS,
    RecommendationCandidate,
    UserProfile,
    _candidate_to_trace,
    _coerce_tracks,
    _env_bool,
    _json_loads,
    _payload_string_list,
    _profile_summary,
    _profile_version,
    _utc_now,
)
from user_profile_reader import UserProfileReader


class RecommendationService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        user_id: str = LEGACY_OWNER_USER_ID,
        bili_client: Any | None = None,
        amem_bridge: Any | None = None,
        profile_projector: ProfileProjector | None = None,
        discovery_planner: DiscoveryPlanner | None = None,
        auto_discovery: bool | None = None,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.user_id = user_id
        init_db(self.db_path)
        self.library = LibraryService(self.db_path, user_id=self.user_id)
        self.bili_client = bili_client if bili_client is not None else BiliClient()
        self.amem_bridge = amem_bridge if amem_bridge is not None else NoopAmemBridge()
        self.profile_projector = profile_projector or ProfileProjector(self.amem_bridge)
        self.candidate_pool = CandidatePool(str(self.db_path), user_id=self.user_id)
        self.content_embeddings = ContentEmbeddingService(
            str(self.db_path),
            user_id=self.user_id,
        )
        self.experiments = ExperimentAssignments(str(self.db_path), user_id=self.user_id)
        self.discovery_service = DiscoveryService(
            str(self.db_path),
            user_id=self.user_id,
            bili_client=self.bili_client,
            planner=discovery_planner,
        )
        self.recommendation_engine = RecommendationEngine(
            embedding_service=self.content_embeddings, user_id=self.user_id
        )
        self.profile_reader = UserProfileReader(self.db_path, self.user_id)
        self.profile_statement_service = ProfileStatementService(self.amem_bridge, db_path=str(self.db_path))
        self.profile_update_pipeline = MusicProfileUpdatePipeline(
            str(self.db_path),
            user_id=self.user_id,
            amem_bridge=self.amem_bridge,
        )
        self.scene_memory_service = SceneMemoryService(str(self.db_path), user_id=self.user_id)
        self.keyword_governance = KeywordGovernance(str(self.db_path), user_id=self.user_id)
        self.auto_discovery = (
            _env_bool("RECOMMEND_AUTO_DISCOVERY_ENABLED", False)
            if auto_discovery is None
            else auto_discovery
        )

    def feed_legacy_snapshot(self) -> dict:
        from dataclasses import fields
        legacy = self.profile_reader._load_user_profile()
        return {field.name: sorted(getattr(legacy, field.name)) for field in fields(legacy)}

    def feed_snapshot(self, *, legacy_snapshot=None) -> MusicProfile:
        """Committed profile only: feed pagination never invokes a chat model."""
        legacy = UserProfile(**{key: set(value) for key, value in legacy_snapshot.items()}) if legacy_snapshot is not None else self.profile_reader._load_user_profile()
        return self.profile_projector.project_committed(
            user_id=self.user_id,scene="feed",
            fallback_profile=self.profile_reader._fallback_music_profile(legacy)
        ).profile

    def decide_feed(self, *, profile: MusicProfile, request_spec: RequestSpec,
                      limit: int, excluded: set[str], trace_id: str,
                      legacy_snapshot=None, catalog=()):
        """Serving decision without shown events, recommendation writes or LLM calls."""
        legacy = UserProfile(**{key: set(value) for key, value in legacy_snapshot.items()}) if legacy_snapshot is not None else self.profile_reader._load_user_profile()
        drafts = self._generate_candidates(legacy, request_spec)
        for track, facets in catalog:
            drafts.setdefault(track.track_id, CandidateDraft(track=track, sources={"feed_catalog"},
                              tags=set(track.tags), facets=facets))
        candidates = [self.recommendation_engine.score(draft, legacy, profile, trace_id,
                       request_spec=request_spec) for draft in drafts.values()]
        _ranked, selected, diagnostics = self.recommendation_engine.rank_and_select(
            candidates, request=RecommendationRequest(scene="feed",limit=limit,
            request_spec=request_spec,profile=profile,exclude_track_ids=excluded,recent_context={}),
            legacy_profile=legacy)
        return [item for item in selected if item.track["trackId"] not in excluded], diagnostics

    @music_operation("recommendation")
    def list_recommendations(
        self,
        scene: str = "home",
        limit: int = DEFAULT_RECOMMENDATION_LIMIT,
        request_spec: RequestSpec | None = None,
        *,
        parent_trace_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_scene = self._normalize_scene(scene)
        trace = FullTrace(
            str(self.db_path),
            trace_type="recommendation",
            user_id=self.user_id,
            parent_trace_id=parent_trace_id or current_trace_id(),
            attributes={
                "scene": normalized_scene,
                "limit": min(
                    max(int(limit or DEFAULT_RECOMMENDATION_LIMIT), 1), MAX_RECOMMENDATION_LIMIT
                ),
                "requestTextHash": hash_text("" if request_spec is None else request_spec.raw_text),
            },
        )
        with trace:
            result = self._list_recommendations(
                scene=normalized_scene,
                limit=limit,
                request_spec=request_spec,
                full_trace=trace,
            )
            result["fullTraceId"] = trace.trace_id
            trace.event(
                "recommendation.completed",
                {
                    "resultCount": len(result.get("items") or []),
                    "trackIds": [
                        str((item.get("track") or {}).get("trackId") or "")
                        for item in result.get("items") or []
                    ],
                },
            )
            return result

    def _list_recommendations(
        self,
        scene: str,
        limit: int,
        request_spec: RequestSpec | None,
        *,
        full_trace: FullTrace,
    ) -> dict[str, Any]:
        started_at = time.perf_counter()
        timings: dict[str, Any] = {}
        normalized_scene = self._normalize_scene(scene)
        memory_variant = self.experiments.memory_variant()
        span_started = time.perf_counter()
        scene_memories = (
            []
            if memory_variant == "control"
            else self.scene_memory_service.active(scene=normalized_scene)
        )
        timings["l2SceneMemoryMs"] = round((time.perf_counter() - span_started) * 1000, 3)
        timings["l2SceneMemorySource"] = (
            scene_memories[0].get("source") if scene_memories else "sqlite_empty"
        )
        full_trace.record_span(
            "scene_memory.retrieve",
            timings["l2SceneMemoryMs"],
            kind="memory",
            inputs={"scene": normalized_scene, "memoryVariant": memory_variant},
            outputs={
                "memoryCount": len(scene_memories),
                "source": timings["l2SceneMemorySource"],
            },
        )
        active_scene_spec = (
            RequestSpec.from_dict(scene_memories[0]["requestSpec"])
            if scene_memories and isinstance(scene_memories[0].get("requestSpec"), dict)
            else None
        )
        resolved_request_spec, restore_scene_context = self._resolve_request_spec(
            request_spec,
            active_scene_spec,
        )
        bounded_limit = min(
            max(int(limit or DEFAULT_RECOMMENDATION_LIMIT), 1),
            MAX_RECOMMENDATION_LIMIT,
        )
        legacy_profile = self.profile_reader._load_user_profile()
        fallback_profile = self.profile_reader._fallback_music_profile(legacy_profile)
        has_personal_key = SettingsService(
            db_path=self.db_path, user_id=self.user_id
        ).has_deepseek_api_key()
        span_started = time.perf_counter()
        if memory_variant == "control":
            projection = ProfileProjection(
                profile=MusicProfile.empty(),
                memories=[],
                trace_id=f"profile-control:{self.user_id}:{normalized_scene}",
            )
        elif not has_personal_key:
            projection = ProfileProjection(
                profile=fallback_profile,
                memories=[],
                trace_id=f"profile-rules:{self.user_id}:{normalized_scene}",
            )
        else:
            projection = self.profile_projector.project(
                user_id=self.user_id,
                scene=normalized_scene,
                fallback_profile=fallback_profile,
            )
        timings["profileProjectionMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        timings["profileCacheHit"] = bool(getattr(projection, "cache_hit", False))
        if getattr(projection, "llm_latency_ms", 0):
            timings["profileLlmApiMs"] = round(float(projection.llm_latency_ms), 2)
        music_profile = projection.profile
        full_trace.record_span(
            "profile.project",
            timings["profileProjectionMs"],
            kind="memory",
            outputs={
                "traceId": projection.trace_id,
                "memoryCount": len(projection.memories),
                "cacheHit": timings["profileCacheHit"],
                "profileSource": music_profile.source,
            },
            metrics={"llmApiMs": timings.get("profileLlmApiMs", 0.0)},
        )
        negative_samples = self.candidate_pool.list_negative_sample_texts(limit=12)
        recommendation_request = RecommendationRequest(
            scene=normalized_scene,
            limit=bounded_limit,
            request_spec=resolved_request_spec,
            profile=music_profile,
            exclude_track_ids=(
                legacy_profile.recently_heard_track_ids
                | legacy_profile.recently_recommended_track_ids
                | legacy_profile.skipped_track_ids
            ),
            recent_context={
                "sceneMemories": scene_memories,
                "l1": music_profile.to_dict(),
                "negativeSamples": negative_samples,
            },
        )
        profile_version = _profile_version(projection.trace_id, music_profile)

        context_specs = []
        if (
            normalized_scene == "conversation"
            and restore_scene_context
            and active_scene_spec is not None
        ):
            context_specs = [active_scene_spec]
        span_started = time.perf_counter()
        drafts = self._generate_candidates(
            legacy_profile, resolved_request_spec, context_specs=context_specs
        )
        timings["candidatePoolReadMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        full_trace.record_span(
            "candidate_pool.read",
            timings["candidatePoolReadMs"],
            kind="retrieval",
            inputs={"requestSpec": resolved_request_spec.to_dict()},
            outputs={"candidateCount": len(drafts)},
            metrics={"poolHit": int(bool(drafts))},
        )
        span_started = time.perf_counter()
        discovery_plan = self.discovery_service.planner.plan(
            profile=music_profile,
            request_spec=resolved_request_spec,
            scene=normalized_scene,
        )
        timings["discoveryPlanMs"] = round((time.perf_counter() - span_started) * 1000, 3)
        full_trace.record_span(
            "discovery.plan",
            timings["discoveryPlanMs"],
            kind="planning",
            outputs={
                "entityQueries": [
                    query
                    for query in discovery_plan.search_queries
                    if query not in discovery_plan.semantic_queries
                ],
                "semanticQueries": discovery_plan.semantic_queries,
                "externalQueryCount": len(discovery_plan.search_queries),
            },
        )
        discovery_job_id = None
        if (
            self.auto_discovery
            and normalized_scene == "home"
            and not resolved_request_spec.constrained
            and len(drafts) < DEFAULT_POOL_TARGET
        ):
            discovery_job_id = self.discovery_service.enqueue(
                profile=music_profile,
                request_spec=resolved_request_spec,
                scene=normalized_scene,
                limit=bounded_limit,
                parent_trace_id=full_trace.trace_id,
            )

        span_started = time.perf_counter()
        candidates = [
            self.recommendation_engine.score(
                draft,
                legacy_profile,
                music_profile,
                discovery_plan.trace_id,
                request_spec=resolved_request_spec,
            )
            for draft in drafts.values()
        ]
        timings["candidateScoringMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        full_trace.record_span(
            "candidate.score",
            timings["candidateScoringMs"],
            kind="ranking",
            outputs={"candidateCount": len(candidates)},
        )
        span_started = time.perf_counter()
        reranked, selected, mmr_diagnostics = self.recommendation_engine.rank_and_select(
            candidates,
            request=recommendation_request,
            legacy_profile=legacy_profile,
        )
        timings["selectionMmrMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        timings["mmr"] = mmr_diagnostics
        timings["ttfrMs"] = round(full_trace.elapsed_ms(), 2) if selected else None
        full_trace.record_span(
            "candidate.rank_select",
            timings["selectionMmrMs"],
            kind="ranking",
            outputs={
                "rerankedCount": len(reranked),
                "selectedCount": len(selected),
                "selectedTrackIds": [item.track.get("trackId") for item in selected],
            },
            metrics={**mmr_diagnostics, "ttfrMs": timings["ttfrMs"]},
        )
        if selected:
            full_trace.event(
                "recommendation.first_valid_result",
                {
                    "ttfrMs": timings["ttfrMs"],
                    "trackId": selected[0].track.get("trackId"),
                },
            )
        timings["servingMs"] = round((time.perf_counter() - started_at) * 1000, 2)

        span_started = time.perf_counter()
        self._upsert_candidate_tracks(selected)
        timings["candidatePersistenceMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        full_trace.record_span(
            "candidate.persist",
            timings["candidatePersistenceMs"],
            kind="storage",
            metrics={"candidateCount": len(selected)},
        )
        span_started = time.perf_counter()
        trace_id = self._store_recommendation_trace(
            scene=normalized_scene,
            profile_trace_id=projection.trace_id,
            agent_trace_id=discovery_plan.trace_id,
            memories=projection.memories,
            music_profile=music_profile,
            profile_version=profile_version,
            agent_queries=discovery_plan.search_queries,
            local_candidate_count=len(drafts),
            agent_candidates=[],
            reranked=reranked,
            selected=selected,
            request_spec=resolved_request_spec,
            timing=timings,
            experiments={"memory_context_v1": memory_variant},
            trace_id=full_trace.trace_id,
        )
        timings["tracePersistenceMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        full_trace.record_span(
            "recommendation_trace.persist",
            timings["tracePersistenceMs"],
            kind="storage",
            outputs={"traceId": trace_id},
        )
        for item in selected:
            item.recommendation_trace_id = trace_id
        span_started = time.perf_counter()
        self.record_events(
            [
                {
                    "trackId": item.track["trackId"],
                    "event": "shown",
                    "scene": normalized_scene,
                    "source": item.source,
                    "reason": item.reason,
                    "score": item.score,
                    "recommendationTraceId": trace_id,
                    "sourceKeywordIds": item.source_keyword_ids,
                }
                for item in selected
            ]
        )
        timings["feedbackWriteMs"] = round((time.perf_counter() - span_started) * 1000, 2)
        full_trace.record_span(
            "impression.write",
            timings["feedbackWriteMs"],
            kind="feedback",
            metrics={"impressionCount": len(selected)},
        )
        timings["totalMs"] = round((time.perf_counter() - started_at) * 1000, 2)
        self._update_recommendation_trace_timing(trace_id, timings)
        return {
            "scene": normalized_scene,
            "items": [item.to_dict() for item in selected],
            "profile": music_profile.to_dict(),
            "profileTraceId": projection.trace_id,
            "profileVersion": profile_version,
            "discoveryTraceId": discovery_plan.trace_id,
            "discoveryJobId": discovery_job_id,
            "requestSpec": resolved_request_spec.to_dict(),
            "debugTraceId": trace_id,
            "timing": timings,
            "experiments": {"memory_context_v1": memory_variant},
        }

    @staticmethod
    def _resolve_request_spec(
        request_spec: RequestSpec | None,
        active_scene_spec: RequestSpec | None,
    ) -> tuple[RequestSpec, bool]:
        """Resolve RequestSpec without allowing L2 to leak into fresh requests."""
        restore_scene_context = request_spec is None or request_spec.should_restore_scene_context
        if restore_scene_context and active_scene_spec is not None:
            return active_scene_spec, True
        return request_spec or RequestSpec(), False

    def latest_debug_trace(self, scene: str = "home") -> dict[str, Any]:
        normalized_scene = self._normalize_scene(scene)
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT trace_id, scene, profile_trace_id, agent_trace_id, payload_json, created_at
                FROM recommendation_traces
                WHERE user_id = ? AND scene = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (self.user_id, normalized_scene),
            ).fetchone()
        if row is None:
            return {
                "traceId": None,
                "scene": normalized_scene,
                "available": False,
                "message": "No recommendation trace has been recorded for this scene.",
            }
        payload = _json_loads(row["payload_json"])
        payload.update(
            {
                "traceId": row["trace_id"],
                "scene": row["scene"],
                "profileTraceId": row["profile_trace_id"],
                "agentTraceId": row["agent_trace_id"],
                "createdAt": row["created_at"],
                "available": True,
            }
        )
        return payload

    def music_profile_analysis(self, scene: str = "home") -> dict[str, Any]:
        normalized_scene = self._normalize_scene(scene)
        legacy_profile = self.profile_reader._load_user_profile()
        fallback_profile = self.profile_reader._fallback_music_profile(legacy_profile)
        projection = self.profile_projector.project(
            user_id=self.user_id,
            scene=normalized_scene,
            fallback_profile=fallback_profile,
        )
        profile = projection.profile
        return {
            "scene": normalized_scene,
            "profile": profile.to_dict(),
            "profileTraceId": projection.trace_id,
            "memories": [memory.to_prompt_dict() for memory in projection.memories],
            "summary": _profile_summary(profile),
            "sceneMemories": self.scene_memory_service.active(scene=normalized_scene),
        }

    def backfill_music_memories(self, limit: int = 80) -> dict[str, Any]:
        bounded_limit = min(max(int(limit or 80), 1), 200)
        events: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for payload in self.library.list_recent(limit=bounded_limit):
            track = Track.from_dict(payload)
            event = "completed" if payload.get("completed") else "played"
            key = (event, track.track_id or "")
            if key in seen:
                continue
            seen.add(key)
            events.append({"event": event, "track": track, "scene": "backfill"})

        for payload in self.library.list_likes()[:bounded_limit]:
            track = Track.from_dict(payload)
            key = ("liked", track.track_id or "")
            if key in seen:
                continue
            seen.add(key)
            events.append({"event": "liked", "track": track, "scene": "backfill"})

        for item in self._review_backfill_items(bounded_limit):
            track = item["track"]
            key = ("track_reviewed", track.track_id or "")
            if key in seen:
                continue
            seen.add(key)
            events.append(
                {
                    "event": "track_reviewed",
                    "track": track,
                    "scene": "backfill",
                    "payload": {
                        "rating": item["rating"],
                        "mood": item["mood"],
                        "note": item["note"],
                    },
                }
            )

        memory_ids: list[str] = []
        recorded = 0
        for item in events[: bounded_limit * 3]:
            result = record_music_behavior(
                self.amem_bridge,
                user_id=self.user_id,
                event=item["event"],
                track=item["track"],
                scene=item["scene"],
                payload=item.get("payload") or {},
            )
            ids = result.get("memoryIds") or []
            if result.get("eventId"):
                recorded += 1
            memory_ids.extend(str(memory_id) for memory_id in ids)

        return {
            "userId": self.user_id,
            "eventsConsidered": len(events),
            "eventsRecorded": recorded,
            "memoryIds": memory_ids,
            "memoryCount": len(memory_ids),
        }

    def submit_profile_statement(self, description: str) -> dict[str, Any]:
        result = self.profile_statement_service.submit(
            user_id=self.user_id,
            description=description,
        )
        if hasattr(self.profile_projector, "clear_cache"):
            self.profile_projector.clear_cache(user_id=self.user_id)
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO music_profile_snapshots (user_id, profile_json, source, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET profile_json = excluded.profile_json,
                    source = excluded.source, updated_at = excluded.updated_at
                """,
                (
                    self.user_id,
                    json.dumps(result.get("profile") or {}, ensure_ascii=False),
                    "profile_statement",
                    _utc_now(),
                ),
            )
        try:
            submitted_profile = MusicProfile.from_dict(
                result.get("profile") or {}, source="profile_statement"
            )
            result["discovery"] = self.discovery_service.discover_now(
                profile=submitted_profile,
                request_spec=RequestSpec(),
                scene="home",
                limit=MAX_RECOMMENDATION_LIMIT,
            )
        except Exception:
            result["discovery"] = {
                "available": self.candidate_pool.availability(RequestSpec()),
                "queries": [],
            }
        result["analysis"] = self.music_profile_analysis(scene="home")
        return result

    def enqueue_discovery(
        self,
        *,
        scene: str = "home",
        limit: int = DEFAULT_RECOMMENDATION_LIMIT,
        request_spec: RequestSpec | None = None,
    ) -> str | None:
        normalized_scene = self._normalize_scene(scene)
        legacy_profile = self.profile_reader._load_user_profile()
        projection = self.profile_projector.project(
            user_id=self.user_id,
            scene=normalized_scene,
            fallback_profile=self.profile_reader._fallback_music_profile(legacy_profile),
        )
        return self.discovery_service.enqueue(
            profile=projection.profile,
            request_spec=request_spec or RequestSpec(),
            scene=normalized_scene,
            limit=min(max(int(limit or DEFAULT_RECOMMENDATION_LIMIT), 1), MAX_RECOMMENDATION_LIMIT),
        )

    def bootstrap_discovery(
        self,
        *,
        scene: str = "conversation",
        limit: int = DEFAULT_RECOMMENDATION_LIMIT,
        request_spec: RequestSpec | None = None,
    ) -> dict[str, Any]:
        normalized_scene = self._normalize_scene(scene)
        legacy_profile = self.profile_reader._load_user_profile()
        projection = self.profile_projector.project(
            user_id=self.user_id,
            scene=normalized_scene,
            fallback_profile=self.profile_reader._fallback_music_profile(legacy_profile),
        )
        return self.discovery_service.discover_now(
            profile=projection.profile,
            request_spec=request_spec or RequestSpec(),
            scene=normalized_scene,
            limit=min(max(int(limit or DEFAULT_RECOMMENDATION_LIMIT), 1), MAX_RECOMMENDATION_LIMIT),
        )

    def discovery_status(self, job_id: str) -> dict[str, Any]:
        return self.discovery_service.job_status(job_id)

    def remember_request(self, *, scene: str, request_spec: RequestSpec) -> str | None:
        return self.scene_memory_service.remember_request(
            scene=self._normalize_scene(scene),
            request_spec=request_spec,
        )

    def record_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        events = self.record_events([payload])
        return events[0] if events else {}

    def record_events(self, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parent_ids = {
            str(
                payload.get("recommendationTraceId") or payload.get("recommendation_trace_id") or ""
            )
            for payload in payloads
        } - {""}
        parent_trace_id = next(iter(parent_ids)) if len(parent_ids) == 1 else current_trace_id()
        trace = FullTrace(
            str(self.db_path),
            trace_type="feedback",
            user_id=self.user_id,
            parent_trace_id=parent_trace_id,
            attributes={"inputEventCount": len(payloads)},
        )
        with trace:
            result = self._record_events(payloads, full_trace=trace)
            for item in result:
                item["fullTraceId"] = trace.trace_id
            trace.event("feedback.completed", {"acceptedEventCount": len(result)})
            return result

    def _record_events(
        self,
        payloads: list[dict[str, Any]],
        *,
        full_trace: FullTrace,
    ) -> list[dict[str, Any]]:
        normalized_started = time.perf_counter()
        now = _utc_now()
        normalized = []
        for payload in payloads:
            track_id = str(payload.get("trackId") or payload.get("track_id") or "").strip()
            track = self.library.get_track(track_id) if track_id else None
            if not track_id or track is None:
                continue
            event = self._normalize_event(payload.get("event"))
            normalized.append(
                {
                    "trackId": track_id,
                    "track": track,
                    "event": event,
                    "scene": self._normalize_scene(str(payload.get("scene") or "home")),
                    "source": str(payload.get("source") or "")[:64],
                    "reason": str(payload.get("reason") or "")[:240],
                    "score": float(payload.get("score") or 0),
                    "recommendationTraceId": str(
                        payload.get("recommendationTraceId")
                        or payload.get("recommendation_trace_id")
                        or ""
                    )[:180],
                    "sourceKeywordIds": _payload_string_list(
                        payload,
                        "sourceKeywordIds",
                        "source_keyword_ids",
                        limit=12,
                    ),
                    "playedSeconds": max(
                        int(payload.get("playedSeconds") or payload.get("played_seconds") or 0),
                        0,
                    ),
                    "completed": bool(payload.get("completed")) or event == "completed",
                    "liked": event == "liked",
                    "skipped": bool(payload.get("skipped"))
                    or event in {"skipped", "dismissed", "dislike"},
                    "createdAt": now,
                    "behaviorPayload": dict(payload),
                }
            )
        full_trace.record_span(
            "feedback.normalize",
            (time.perf_counter() - normalized_started) * 1000,
            kind="feedback",
            outputs={
                "acceptedEventCount": len(normalized),
                "eventTypes": sorted({item["event"] for item in normalized}),
            },
        )

        storage_started = time.perf_counter()
        if normalized:
            with get_connection(self.db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO recommendation_events (
                        user_id, track_id, event, scene, source, reason, score,
                        recommendation_trace_id, source_keyword_ids_json,
                        played_seconds, completed, negative, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            self.user_id,
                            item["trackId"],
                            item["event"],
                            item["scene"],
                            item["source"],
                            item["reason"],
                            item["score"],
                            item["recommendationTraceId"],
                            json.dumps(item["sourceKeywordIds"], ensure_ascii=False),
                            item["playedSeconds"],
                            int(item["completed"]),
                            int(item["skipped"]),
                            item["createdAt"],
                        )
                        for item in normalized
                    ],
                )
                for item in normalized:
                    self._write_history(conn, item)
                    if item["event"] in MEMORY_EVIDENCE_EVENTS:
                        from durable_jobs import enqueue_behavior

                        behavior_payload = dict(item.get("behaviorPayload") or {})
                        behavior_payload.update(
                            {
                                name: item[name]
                                for name in (
                                    "source",
                                    "reason",
                                    "score",
                                    "playedSeconds",
                                    "completed",
                                    "skipped",
                                )
                            }
                        )
                        enqueue_behavior(
                            conn,
                            user_id=self.user_id,
                            event=item["event"],
                            scene=item["scene"],
                            track=item["track"],
                            payload=behavior_payload,
                        )
        full_trace.record_span(
            "feedback.persist",
            (time.perf_counter() - storage_started) * 1000,
            kind="storage",
            metrics={"eventCount": len(normalized)},
        )

        learning_started = time.perf_counter()
        memory_evidence_count = 0
        for item in normalized:
            behavior_payload = dict(item.get("behaviorPayload") or {})
            behavior_payload.update(
                {
                    "source": item["source"],
                    "reason": item["reason"],
                    "score": item["score"],
                    "playedSeconds": item["playedSeconds"],
                    "completed": item["completed"],
                    "skipped": item["skipped"],
                }
            )
            # Exposure is stored locally for fatigue and keyword attribution,
            # but it is not user-preference evidence and must not enter AMEM.
            if item["event"] in MEMORY_EVIDENCE_EVENTS:
                memory_evidence_count += 1
                from job_transport import async_jobs_enabled

                if not async_jobs_enabled():
                    record_music_behavior(
                        self.amem_bridge,
                        user_id=self.user_id,
                        event=item["event"],
                        track=item["track"],
                        scene=item["scene"],
                        payload=behavior_payload,
                    )
            self.keyword_governance.record_feedback(
                item["trackId"],
                item["event"],
                recommendation_trace_id=item["recommendationTraceId"],
                source_keyword_ids=item["sourceKeywordIds"],
            )
            if item["event"] in {"completed", "liked", "collection_added"}:
                self.content_embeddings.enqueue_audio_embeddings(
                    [item["track"]],
                    force=True,
                )
        full_trace.record_span(
            "feedback.learn",
            (time.perf_counter() - learning_started) * 1000,
            kind="memory",
            metrics={
                "eventCount": len(normalized),
                "memoryEvidenceCount": memory_evidence_count,
            },
        )
        profile_started = time.perf_counter()
        if any(item["event"] in PROFILE_LIFECYCLE_EVENTS for item in normalized):
            self.profile_update_pipeline.process()
        full_trace.record_span(
            "profile.lifecycle.update",
            (time.perf_counter() - profile_started) * 1000,
            kind="memory",
            metrics={
                "triggered": int(
                    any(item["event"] in PROFILE_LIFECYCLE_EVENTS for item in normalized)
                )
            },
        )
        return [
            {key: value for key, value in item.items() if key not in {"track", "behaviorPayload"}}
            for item in normalized
        ]

    def _generate_candidates(
        self,
        profile: UserProfile,
        request_spec: RequestSpec | None = None,
        *,
        context_specs: list[RequestSpec] | None = None,
    ) -> dict[str, CandidateDraft]:
        """Load only previously admitted content. This serving path never calls Bilibili search."""
        candidates: dict[str, CandidateDraft] = {}
        for candidate in self.candidate_pool.list_ready(
            request_spec or RequestSpec(), context_specs=context_specs
        ):
            self._add_candidate(candidates, candidate.track, candidate.source)
            draft = candidates[candidate.track.track_id]
            draft.facets = candidate.facets
            draft.scope_evidence = candidate.evidence
            draft.scope_kind = candidate.scope_kind
            draft.source_keyword_ids.update(candidate.source_keyword_ids)
            draft.source_keyword_family_ids.update(candidate.source_keyword_family_ids)
            draft.source_discovery_job_ids.update(candidate.source_discovery_job_ids)
            draft.tags.update(
                evidence.removeprefix("discovery_query:")
                for evidence in candidate.evidence
                if evidence.startswith("discovery_query:")
            )
        vectors = self.content_embeddings.text_vectors(candidates)
        audio_vectors = self.content_embeddings.audio_vectors(candidates)
        for track_id, vector in vectors.items():
            if track_id in candidates:
                candidates[track_id].text_embedding = vector
        for track_id, vector in audio_vectors.items():
            if track_id in candidates:
                candidates[track_id].audio_embedding = vector
        return candidates

    def _safe_list_user_tracks(self, mid: int, order: str, page_size: int) -> list[Track]:
        try:
            payload = self.bili_client.list_user_tracks(
                mid,
                page=1,
                page_size=page_size,
                order=order,
            )
        except Exception:
            return []
        tracks = payload.get("tracks") if isinstance(payload, dict) else []
        return _coerce_tracks(tracks or [])

    def _safe_search_tracks(self, keyword: str, page_size: int) -> list[Track]:
        try:
            tracks = self.bili_client.search(keyword, page=1, page_size=page_size)
        except Exception:
            return []
        return _coerce_tracks(tracks or [])

    def _local_tracks_with_common_tags(self, tags: set[str]) -> list[tuple[Track, str]]:
        if not tags:
            return []
        placeholders = ",".join("?" for _ in tags)
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT t.*, tr.mood
                FROM track_reviews tr
                JOIN tracks t ON t.track_id = tr.track_id
                WHERE tr.user_id = ?
                  AND tr.mood IN ({placeholders})
                ORDER BY tr.updated_at DESC
                LIMIT 50
                """,
                (self.user_id, *tags),
            ).fetchall()
        return [(self.library._track_from_row(row), str(row["mood"])) for row in rows]

    def _local_fallback_tracks(self) -> list[Track]:
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT t.*
                FROM tracks t
                LEFT JOIN recent r ON r.user_id = ? AND r.track_id = t.track_id
                LEFT JOIN likes l ON l.user_id = ? AND l.track_id = t.track_id
                LEFT JOIN track_reviews tr ON tr.user_id = ? AND tr.track_id = t.track_id
                LEFT JOIN playlist_items pi ON pi.user_id = ? AND pi.track_id = t.track_id
                WHERE r.track_id IS NOT NULL
                   OR l.track_id IS NOT NULL
                   OR tr.track_id IS NOT NULL
                   OR pi.track_id IS NOT NULL
                ORDER BY COALESCE(r.last_played_at, l.created_at, tr.updated_at, t.updated_at) DESC
                LIMIT 50
                """,
                (self.user_id, self.user_id, self.user_id, self.user_id),
            ).fetchall()
        return [self.library._track_from_row(row) for row in rows]

    def _review_backfill_items(self, limit: int) -> list[dict[str, Any]]:
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT tr.rating, tr.mood, tr.note, t.*
                FROM track_reviews tr
                JOIN tracks t ON t.track_id = tr.track_id
                WHERE tr.user_id = ?
                ORDER BY tr.updated_at DESC
                LIMIT ?
                """,
                (self.user_id, limit),
            ).fetchall()
        return [
            {
                "track": self.library._track_from_row(row),
                "rating": int(row["rating"]),
                "mood": str(row["mood"] or ""),
                "note": str(row["note"] or ""),
            }
            for row in rows
        ]

    @staticmethod
    def _add_candidate(
        candidates: dict[str, CandidateDraft],
        track: Track,
        source: str,
        tag: str | None = None,
    ) -> None:
        if not track.track_id:
            return
        draft = candidates.get(track.track_id)
        if not draft:
            draft = CandidateDraft(track=track)
            candidates[track.track_id] = draft
        draft.sources.add(source)
        if tag:
            draft.tags.add(tag)

    def _upsert_draft_tracks(self, drafts: Any) -> None:
        tracks = [draft.track for draft in drafts]
        if tracks:
            self.library.upsert_tracks(tracks)

    def _upsert_candidate_tracks(self, candidates: list[RecommendationCandidate]) -> None:
        tracks = []
        for candidate in candidates:
            try:
                tracks.append(Track.from_dict(candidate.track))
            except Exception:
                continue
        if tracks:
            self.library.upsert_tracks(tracks)

    def _update_recommendation_trace_timing(self, trace_id: str, timing: dict[str, Any]) -> None:
        """Persist the final timing after shown events and keyword feedback are recorded."""
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT payload_json FROM recommendation_traces WHERE trace_id = ?",
                (trace_id,),
            ).fetchone()
            if row is None:
                return
            payload = _json_loads(row["payload_json"])
            payload["timing"] = timing
            conn.execute(
                "UPDATE recommendation_traces SET payload_json = ? WHERE trace_id = ?",
                (json.dumps(payload, ensure_ascii=False), trace_id),
            )

    def _store_recommendation_trace(
        self,
        *,
        scene: str,
        profile_trace_id: str,
        agent_trace_id: str,
        memories: list[Any],
        music_profile: MusicProfile,
        profile_version: str,
        agent_queries: list[str],
        local_candidate_count: int,
        agent_candidates: list[Any],
        reranked: list[RecommendationCandidate],
        selected: list[RecommendationCandidate],
        request_spec: RequestSpec,
        timing: dict[str, Any],
        experiments: dict[str, str],
        trace_id: str | None = None,
    ) -> str:
        created_at = _utc_now()
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        trace_id = trace_id or f"recommend:{self.user_id}:{scene}:{timestamp_ms}:{uuid4().hex[:8]}"
        for candidate in selected:
            candidate.recommendation_trace_id = trace_id
        payload = {
            "memoryRetrieval": {
                "count": len(memories),
                "memories": [memory.to_prompt_dict() for memory in memories],
            },
            "profileVersion": profile_version,
            "profileSnapshot": {
                "traceId": profile_trace_id,
                "version": profile_version,
                "profile": music_profile.to_dict(),
            },
            "musicProfile": music_profile.to_dict(),
            "requestSpec": request_spec.to_dict(),
            "timing": timing,
            "experiments": experiments,
            "candidatePool": {
                "searchQueries": agent_queries,
                "availableCandidateCount": max(local_candidate_count, 0),
                "newlyDiscoveredCandidateCount": len(agent_candidates),
            },
            "agent": {
                "searchQueries": agent_queries,
                "localCandidateCount": max(local_candidate_count, 0),
                "agentCandidateCount": 0,
                "agentCandidates": [],
            },
            "rerankedCandidates": [_candidate_to_trace(candidate) for candidate in reranked[:40]],
            "finalResults": [_candidate_to_trace(candidate) for candidate in selected],
        }
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO recommendation_traces (
                    trace_id, user_id, scene, profile_trace_id, agent_trace_id,
                    payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    self.user_id,
                    scene,
                    profile_trace_id,
                    agent_trace_id,
                    json.dumps(payload, ensure_ascii=False),
                    created_at,
                ),
            )
            policy_json = json.dumps(
                {
                    "experiments": experiments,
                    "scene": scene,
                    "profileVersion": profile_version,
                    "requestSpec": request_spec.to_dict(),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            for rank_position, candidate in enumerate(selected, start=1):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO recommendation_impressions (
                        recommendation_trace_id, user_id, track_id, rank_position,
                        score, score_signals_json, candidate_snapshot_json,
                        policy_json, selection_propensity, shown_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trace_id,
                        self.user_id,
                        str(candidate.track.get("trackId") or ""),
                        rank_position,
                        float(candidate.score),
                        json.dumps(candidate.score_signals, ensure_ascii=False, sort_keys=True),
                        json.dumps(_candidate_to_trace(candidate), ensure_ascii=False),
                        policy_json,
                        None,
                        created_at,
                    ),
                )
            for candidate in selected:
                keyword_ids = list(dict.fromkeys(candidate.source_keyword_ids))
                if not keyword_ids:
                    continue
                placeholders = ",".join("?" for _ in keyword_ids)
                family_rows = conn.execute(
                    f"SELECT keyword_id, family_id FROM discovery_keywords WHERE keyword_id IN ({placeholders})",
                    tuple(keyword_ids),
                ).fetchall()
                family_by_keyword = {
                    str(row["keyword_id"]): str(row["family_id"] or "") for row in family_rows
                }
                credit_weight = 1.0 / max(len(keyword_ids), 1)
                for keyword_id in keyword_ids:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO recommendation_keyword_attributions (
                            recommendation_trace_id, user_id, track_id, keyword_id,
                            family_id, credit_weight, shown, shown_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                        """,
                        (
                            trace_id,
                            self.user_id,
                            str(candidate.track.get("trackId") or ""),
                            keyword_id,
                            family_by_keyword.get(keyword_id, ""),
                            credit_weight,
                            created_at,
                            created_at,
                        ),
                    )
        return trace_id

    def _write_history(self, conn: Any, item: dict[str, Any]) -> None:
        if item["event"] == "shown":
            conn.execute(
                """
                INSERT INTO recommendation_history (
                    user_id, track_id, recommended_at, clicked, played_seconds,
                    completed, liked, skipped, scene, source, score, reason
                )
                VALUES (?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?, ?)
                """,
                (
                    self.user_id,
                    item["trackId"],
                    item["createdAt"],
                    item["scene"],
                    item["source"],
                    item["score"],
                    item["reason"],
                ),
            )
            return

        latest = conn.execute(
            """
            SELECT id FROM recommendation_history
            WHERE user_id = ? AND track_id = ?
            ORDER BY recommended_at DESC
            LIMIT 1
            """,
            (self.user_id, item["trackId"]),
        ).fetchone()
        if latest:
            conn.execute(
                """
                UPDATE recommendation_history
                SET clicked = MAX(clicked, ?),
                    played_seconds = MAX(played_seconds, ?),
                    completed = MAX(completed, ?),
                    liked = MAX(liked, ?),
                    skipped = MAX(skipped, ?)
                WHERE id = ?
                """,
                (
                    int(item["event"] in {"played", "accepted", "completed"}),
                    item["playedSeconds"],
                    int(item["completed"]),
                    int(item["liked"]),
                    int(item["skipped"]),
                    latest["id"],
                ),
            )
            return

        conn.execute(
            """
            INSERT INTO recommendation_history (
                user_id, track_id, recommended_at, clicked, played_seconds,
                completed, liked, skipped, scene, source, score, reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.user_id,
                item["trackId"],
                item["createdAt"],
                int(item["event"] in {"played", "accepted", "completed"}),
                item["playedSeconds"],
                int(item["completed"]),
                int(item["liked"]),
                int(item["skipped"]),
                item["scene"],
                item["source"],
                item["score"],
                item["reason"],
            ),
        )

    @staticmethod
    def _normalize_scene(scene: str) -> str:
        value = (scene or "home").strip().lower()
        return value[:32] or "home"

    @staticmethod
    def _normalize_event(event: Any) -> str:
        value = str(event or "shown").strip().lower()
        allowed = {
            "shown",
            "played",
            "accepted",
            "dismissed",
            "dislike",
            "skipped",
            "completed",
            "liked",
            "unliked",
            "collection_added",
            "track_reviewed",
        }
        return value if value in allowed else "shown"
