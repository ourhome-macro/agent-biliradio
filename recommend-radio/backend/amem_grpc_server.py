from __future__ import annotations

import logging
import json
import os
import signal
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc

from amem_bridge import AmemBridge
from music_profile import MusicProfile
from profile_projector import ProfileProjector

_GEN_DIR = Path(__file__).resolve().parent / "amem_gen"
if str(_GEN_DIR) not in sys.path:
    sys.path.insert(0, str(_GEN_DIR))

from amem.v1 import amem_pb2, amem_pb2_grpc  # noqa: E402


LOGGER = logging.getLogger("amem.grpc")


class AmemGrpcService(amem_pb2_grpc.AmemServiceServicer):
    def __init__(
        self,
        *,
        bridge: Any | None = None,
        projector: ProfileProjector | None = None,
    ) -> None:
        self.bridge = bridge if bridge is not None else AmemBridge.from_env()
        self.projector = projector if projector is not None else ProfileProjector(self.bridge)
        bind_invalidator = getattr(self.bridge, "bind_dream_cache_invalidator", None)
        if callable(bind_invalidator):
            bind_invalidator(self.projector.clear_cache)
        self._embedding_stop = threading.Event()
        self._embedding_thread: threading.Thread | None = None
        if callable(getattr(self.bridge, "process_embedding_jobs", None)):
            self._embedding_thread = threading.Thread(
                target=self._run_embedding_worker,
                name="amem-embedding-worker",
                daemon=True,
            )
            self._embedding_thread.start()

    def _run_embedding_worker(self) -> None:
        while not self._embedding_stop.is_set():
            try:
                report = self.bridge.process_embedding_jobs(max_jobs=64)
                processed = int(getattr(report, "processed", 0) or 0)
            except Exception as exc:
                LOGGER.warning("AMEM embedding worker iteration failed: %s", exc)
                processed = 0
            self._embedding_stop.wait(0.1 if processed else 1.0)

    def close(self) -> None:
        self._embedding_stop.set()
        if self._embedding_thread is not None:
            self._embedding_thread.join(timeout=5)
        close_bridge = getattr(self.bridge, "close", None)
        if callable(close_bridge):
            close_bridge()

    def RecordBehavior(self, request: Any, context: grpc.ServicerContext) -> Any:
        payload = _payload_from_json(request.payload_json)
        payload.setdefault("event_id", request.event_id)
        payload.setdefault("eventId", request.event_id)
        payload.setdefault("userId", _normalize_user_id(request.user_id))
        payload.setdefault("event", request.event)
        payload.setdefault("scene", _normalize_scene(request.scene))
        if request.track_id:
            payload.setdefault("trackId", request.track_id)
        result = self.bridge.record_behavior(payload)
        return amem_pb2.RecordBehaviorResponse(
            accepted=True,
            amem_event_id=str(result.get("eventId") or ""),
            memory_ids=[str(item) for item in result.get("memoryIds") or []],
        )

    def RecordProfileStatement(self, request: Any, context: grpc.ServicerContext) -> Any:
        profile_payload = _payload_from_json(request.profile_json)
        user_id = _normalize_user_id(request.user_id)
        scene = _normalize_scene(request.scene)
        description = str(request.description or "").strip()
        source = str(request.source or "go-backend").strip() or "go-backend"
        profile = MusicProfile.from_dict(profile_payload, source=source)
        if source == "event_l3":
            support_counts = _support_counts_from_description(description)
            result = self.bridge.promote_music_profile(
                user_id=user_id,
                profile=profile,
                support_counts=support_counts,
            )
        elif source == "event_l3_demote":
            result = self.bridge.demote_music_profile(
                user_id=user_id,
                profile=profile,
                reasons=_demotion_reasons_from_description(description),
            )
        else:
            result = self.bridge.record_profile_statement(
                user_id=user_id,
                description=description,
                profile=profile,
                source=source,
            )
        # A profile statement affects every recommendation scene. Clearing only
        # the caller's scene left home/conversation projections disagreeing for the TTL.
        self.projector.clear_cache(user_id=user_id)
        return amem_pb2.RecordProfileStatementResponse(
            accepted=True,
            amem_event_id=str(result.get("eventId") or ""),
            memory_ids=[str(item) for item in result.get("memoryIds") or []],
        )

    def GetMusicProfile(self, request: Any, context: grpc.ServicerContext) -> Any:
        started = time.perf_counter()
        user_id = _normalize_user_id(request.user_id)
        scene = _normalize_scene(request.scene)
        projection = self.projector.project(
            user_id=user_id,
            scene=scene,
            fallback_profile=MusicProfile.empty(),
        )
        return _profile_to_response(
            projection.profile,
            profile_llm_api_ms=float(getattr(projection, "llm_latency_ms", 0.0)),
            profile_total_ms=(time.perf_counter() - started) * 1000,
        )

    def ExplainRecommendation(self, request: Any, context: grpc.ServicerContext) -> Any:
        user_id = _normalize_user_id(request.user_id)
        scene = _normalize_scene(request.scene)
        projection = self.projector.project(
            user_id=user_id,
            scene=scene,
            fallback_profile=MusicProfile.empty(),
        )
        trace_id = request.trace_id or projection.trace_id
        reason = _profile_reason(projection.profile)
        reasons = {
            track_id: reason
            for track_id in request.candidate_track_ids
            if str(track_id).strip()
        }
        return amem_pb2.ExplainRecommendationResponse(
            trace_id=trace_id,
            reasons=reasons,
            evidence_memory_ids=projection.profile.evidence_memory_ids,
        )

    def Health(self, request: Any, context: grpc.ServicerContext) -> Any:
        enabled = bool(getattr(self.bridge, "enabled", False))
        return amem_pb2.HealthResponse(status="serving" if enabled else "noop")


def build_server(service: AmemGrpcService | None = None) -> grpc.Server:
    max_workers = _env_int("AMEM_GRPC_WORKERS", 8)
    from telemetry_setup import setup
    setup('amem-grpc')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    amem_pb2_grpc.add_AmemServiceServicer_to_server(service or AmemGrpcService(), server)
    return server


def serve() -> None:
    logging.basicConfig(level=_log_level(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    addr = os.getenv("AMEM_GRPC_BIND", "0.0.0.0:9090").strip() or "0.0.0.0:9090"
    service = AmemGrpcService()
    server = build_server(service)
    bound_port = server.add_insecure_port(addr)
    if bound_port == 0:
        raise RuntimeError(f"failed to bind AMEM gRPC server on {addr}")

    stopped = threading.Event()

    def _stop(_signum: int, _frame: Any) -> None:
        LOGGER.info("stopping AMEM gRPC server")
        server.stop(_env_int("AMEM_GRPC_GRACE_SECONDS", 10))
        stopped.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    server.start()
    LOGGER.info("AMEM gRPC server listening", extra={"addr": addr})
    try:
        while not stopped.wait(timeout=3600):
            time.sleep(0)
    finally:
        server.stop(0)
        service.close()


def _profile_to_response(profile: MusicProfile, *, profile_llm_api_ms: float = 0.0, profile_total_ms: float = 0.0) -> Any:
    return amem_pb2.MusicProfileResponse(
        positive_topics=_score_map(profile.positive_topics),
        negative_topics=_score_map(profile.negative_topics),
        preferred_uploaders=_score_map(profile.preferred_uploaders),
        avoid_uploaders=_score_map(profile.avoid_uploaders),
        blocked_uploaders=_score_map(profile.blocked_uploaders),
        mood_weights=_score_map(profile.mood_weights),
        recent_intents=list(profile.recent_intents),
        same_uploader_limit=int(profile.same_uploader_limit or 0),
        exploration_ratio=float(profile.exploration_ratio or 0.0),
        evidence_memory_ids=list(profile.evidence_memory_ids),
        confidence=float(profile.confidence or 0.0),
        source=_profile_source(profile.source),
        positive_interest_texts=list(profile.positive_interest_texts),
        negative_interest_texts=list(profile.negative_interest_texts),
        profile_llm_api_ms=profile_llm_api_ms,
        profile_total_ms=profile_total_ms,
        mbti=profile.mbti,
        music_persona=profile.music_persona,
        current_music_phase=profile.current_music_phase,
        core_traits=list(profile.core_traits),
        psychological_needs=list(profile.psychological_needs),
        persona_evidence=list(profile.persona_evidence),
        persona_confidence=float(profile.persona_confidence),
    )


def _payload_from_json(value: bytes) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _support_counts_from_description(value: str) -> dict[str, int]:
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return {}
    values = payload.get("supportCounts") if isinstance(payload, dict) else {}
    if not isinstance(values, dict):
        return {}
    result: dict[str, int] = {}
    for topic, count in values.items():
        try:
            result[str(topic)] = int(count)
        except (TypeError, ValueError):
            continue
    return result


def _demotion_reasons_from_description(value: str) -> dict[str, str]:
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return {}
    values = payload.get("reasons") if isinstance(payload, dict) else {}
    if not isinstance(values, dict):
        return {}
    return {
        str(key)[:180]: str(reason)[:300]
        for key, reason in values.items()
        if str(key).strip() and str(reason).strip()
    }


def _score_map(values: dict[str, float]) -> dict[str, float]:
    result: dict[str, float] = {}
    for key, value in (values or {}).items():
        name = str(key).strip()
        if not name:
            continue
        try:
            score = float(value)
        except (TypeError, ValueError):
            score = 0.0
        result[name] = min(max(score, 0.0), 1.0)
    return result


def _profile_source(source: str) -> str:
    normalized = str(source or "").strip()
    if normalized == "fallback":
        return "amem_fallback"
    return normalized or "amem"


def _profile_reason(profile: MusicProfile) -> str:
    topics = _top_keys(profile.positive_topics, 3)
    moods = _top_keys(profile.mood_weights, 2)
    uploaders = _top_keys(profile.preferred_uploaders, 2)
    parts = []
    if topics:
        parts.append("positive topics: " + ", ".join(topics))
    if moods:
        parts.append("moods: " + ", ".join(moods))
    if uploaders:
        parts.append("preferred uploaders: " + ", ".join(uploaders))
    if not parts:
        return "AMEM has no strong evidence for this candidate yet."
    return "AMEM evidence matched " + "; ".join(parts) + "."


def _top_keys(values: dict[str, float], limit: int) -> list[str]:
    return [
        key
        for key, _value in sorted(
            values.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:limit]
    ]


def _normalize_user_id(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized[:128] or "legacy-owner"


def _normalize_scene(value: str) -> str:
    normalized = str(value or "home").strip().lower()
    return normalized[:32] or "home"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _log_level() -> int:
    return {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }.get(os.getenv("LOG_LEVEL", "info").strip().lower(), logging.INFO)


if __name__ == "__main__":
    serve()
