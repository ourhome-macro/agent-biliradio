from __future__ import annotations

import os
import logging
import re
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from music_profile import RelevantMemory
from models import Track


AGENT_ID = "recommend-radio"
LOGGER = logging.getLogger("recommend-radio.amem-bridge")
TENANT_ID = "recommend-radio"
PROFILE_SESSION_ID = "music-profile"
MUSIC_TAGS = ("music", "recommend-radio")
POSITIVE_EVENTS = {"played", "accepted", "completed", "liked", "collection_added"}
NEGATIVE_EVENTS = {"skipped", "dismissed", "dislike"}
DEFAULT_CORE_PROMOTION_MIN_AGE_DAYS = 2
MAX_TOPIC_MEMORIES_PER_EVENT = 5
EVENT_ALIASES = {
    "play": "played",
    "played": "played",
    "recommendation.clicked": "accepted",
    "clicked": "accepted",
    "accepted": "accepted",
    "complete": "completed",
    "completed": "completed",
    "like": "liked",
    "liked": "liked",
    "skip": "skipped",
    "skipped": "skipped",
}
DEFAULT_RETRIEVAL_ROUTES = {
    "music_recommendation": "lexical_first",
    "home": "lexical_first",
    "recommendation": "lexical_first",
    "conversation": "hybrid",
}
GENERIC_TOPIC_STOPWORDS = {
    "beat",
    "beats",
    "playlist",
    "music",
    "song",
    "songs",
    "cover",
    "instrumental",
    "official",
    "type-beat",
    "typebeat",
    "video",
    "remix",
    "audio",
    "\u97f3\u4e50",
    "\u6b4c\u66f2",
    "\u6b4c\u5355",
    "\u5408\u96c6",
    "\u63a8\u8350",
    "\u5b8c\u6574\u7248",
    "\u5b98\u65b9",
    "\u7ffb\u5531",
}
MUSIC_ENTITY_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Vocaloid", ("vocaloid", "ボカロ", "ボーカロイド", "术力口")),
    ("初音未来", ("初音未来", "初音ミク", "hatsune miku", "miku")),
    ("洛天依", ("洛天依", "luo tianyi")),
    ("J-Pop", ("j-pop", "jpop", "日语歌", "日文歌", "日语", "日系")),
    ("K-Pop", ("k-pop", "kpop", "韩语歌", "韩文歌", "韩语")),
    ("华语", ("华语", "中文歌", "国语", "粤语", "华语女声", "华语男声")),
    ("粤语", ("粤语", "港乐", "陈奕迅", "eason")),
    ("纯音乐", ("纯音乐", "instrumental", "piano", "钢琴", "吉他", "古典", "bgm")),
    ("钢琴", ("piano", "钢琴")),
    ("吉他", ("guitar", "吉他")),
    ("治愈系", ("治愈", "慰藉", "温柔", "放松", "舒缓", "静心")),
    ("Lo-fi", ("lo-fi", "lofi", "lo fi")),
    ("ACG", ("acg", "动漫", "动画", "番剧", "mad", "op", "ed")),
    ("Rap", ("rap", "说唱", "嘻哈", "hiphop", "hip-hop")),
)
ARTIST_MARKERS = (" - ", "《", "》")


@dataclass(frozen=True)
class MusicEntity:
    name: str
    kind: str
    confidence: float = 0.72


class NoopAmemBridge:
    enabled = False

    def record_behavior(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"enabled": False, "eventId": None, "memoryIds": []}

    def record_profile_statement(
        self,
        *,
        user_id: str,
        description: str,
        profile: Any,
        source: str,
    ) -> dict[str, Any]:
        return {"enabled": False, "eventId": None, "memoryIds": []}

    def promote_music_profile(
        self,
        *,
        user_id: str,
        profile: Any,
        support_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {"enabled": False, "eventId": None, "memoryIds": []}

    def demote_music_profile(
        self,
        *,
        user_id: str,
        profile: Any,
        reasons: dict[str, str],
    ) -> dict[str, Any]:
        return {"enabled": False, "eventId": None, "memoryIds": []}

    def retrieve_memories(self, user_id: str, scene: str, *, limit: int = 12) -> list[RelevantMemory]:
        return []


@dataclass(frozen=True)
class AmemRuntimeHandle:
    runtime: Any
    intake: Any


class AmemBridge:
    enabled = True

    def __init__(self, db_path: str | Path | None = None) -> None:
        _ensure_amem_import_path()
        from agent_memory_runtime.config import RuntimeConfig
        from agent_memory_runtime.exceptions import StoreError
        from agent_memory_runtime.memory.embeddings.environment import load_embedding_environment
        from agent_memory_runtime.memory.intake import MemoryIntakeService
        from agent_memory_runtime.memory.intake.triggers import AutoDreamTriggers
        from agent_memory_runtime.memory.stores import SQLiteStoreBundle
        from agent_memory_runtime.runtime import AgentMemoryRuntime

        path = Path(db_path or _default_amem_db_path()).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        embedding_env = load_embedding_environment(required_provider=False, require_online_threshold=False)
        try:
            bundle = SQLiteStoreBundle(path, embedding_provider=embedding_env.provider)
        except StoreError as exc:
            if embedding_env.provider is None or "embedding generation" not in str(exc):
                raise
            LOGGER.info("Bootstrapping AMEM embedding generation %s", embedding_env.provider.spec.generation)
            bootstrap = SQLiteStoreBundle(path)
            bootstrap.embedding_generations.register(embedding_env.provider.spec, status="backfill")
            bootstrap.enqueue_embedding_backfill()
            report = bootstrap.embedding_worker(embedding_env.provider).run_until_idle()
            coverage = bootstrap.vector_index.coverage(generation=embedding_env.provider.spec.generation)
            if report.failed or report.dead_lettered or coverage < 1.0:
                raise StoreError(
                    "embedding backfill failed: "
                    f"coverage={coverage:.4f}, failed={report.failed}, dead_lettered={report.dead_lettered}"
                ) from exc
            bootstrap.activate_embedding_generation(
                embedding_env.provider.spec.generation,
                minimum_coverage=1.0,
            )
            bundle = SQLiteStoreBundle(path, embedding_provider=embedding_env.provider)
        runtime_config = RuntimeConfig()
        semantic_timeout = os.getenv("AMEM_SEMANTIC_TIMEOUT_MS", "").strip()
        if embedding_env.min_similarity is not None or semantic_timeout:
            timeout_ms = (
                int(semantic_timeout)
                if semantic_timeout else runtime_config.hybrid_retrieval.semantic_timeout_ms
            )
            if not 1 <= timeout_ms <= 30_000:
                raise ValueError("AMEM_SEMANTIC_TIMEOUT_MS must be between 1 and 30000")
            runtime_config = replace(
                runtime_config,
                hybrid_retrieval=replace(
                    runtime_config.hybrid_retrieval,
                    min_semantic_similarity=(
                        embedding_env.min_similarity
                        if embedding_env.min_similarity is not None
                        else runtime_config.hybrid_retrieval.min_semantic_similarity
                    ),
                    semantic_timeout_ms=timeout_ms,
                ),
            )
        runtime = AgentMemoryRuntime(
            config=runtime_config,
            event_store=bundle.event_store,
            memory_store=bundle.memory_store,
            snapshot_store=bundle.snapshot_store,
            audit_store=bundle.audit_store,
            tombstone_store=bundle.tombstone_store,
            transaction_manager=bundle,
            dream_store=bundle.dream_store,
        )
        self.db_path = path
        self.store_bundle = bundle
        self.embedding_worker = (
            bundle.embedding_worker(embedding_env.provider)
            if embedding_env.provider is not None
            else None
        )
        self.handle = AmemRuntimeHandle(
            runtime=runtime,
            intake=MemoryIntakeService(runtime),
        )
        self.dream_triggers = AutoDreamTriggers(
            bundle.dream_store,
            timezone=os.getenv("AUTO_DREAM_TIMEZONE", "Asia/Shanghai"),
        )
        self._dream_last_due_scan = 0.0
        self._dream_cache_invalidator: Any | None = None
        self._dream_worker_started = False

    def _start_dream_worker(self) -> None:
        if self._dream_worker_started:
            return
        if os.getenv("AUTO_DREAM_ENABLED", "true").strip().lower() not in {
            "0", "false", "no", "off",
        }:
            from agent_memory_runtime.memory.intake.dream import AutoDreamAnalyzer

            self.handle.runtime.start_auto_dream_background(
                analyzer=AutoDreamAnalyzer(llm_client_factory=self._dream_llm_client),
                before_poll=self._schedule_due_dreams,
                on_applied=self._dream_applied,
            )
            self._dream_worker_started = True

    def bind_dream_cache_invalidator(self, callback: Any) -> None:
        self._dream_cache_invalidator = callback
        self._start_dream_worker()

    def _dream_applied(self, user_id: str | None) -> None:
        if self._dream_cache_invalidator is not None and user_id is not None:
            self._dream_cache_invalidator(user_id=user_id)

    def _dream_llm_client(self, user_id: str) -> Any | None:
        if os.getenv("AUTO_DREAM_LLM_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
            return None
        from profile_projector import _default_llm_client
        from settings_service import SettingsService

        provider = os.getenv("RECOMMEND_LLM_PROVIDER", "deepseek").strip().lower()
        if provider != "deepseek":
            return None
        if SettingsService(user_id=user_id).has_deepseek_api_key():
            client = _default_llm_client(user_id=user_id)
            client.max_tokens = min(client.max_tokens, 800)
            client.timeout_seconds = min(client.timeout_seconds, 12)
            return client
        return None

    def _schedule_due_dreams(self) -> None:
        import time

        now = time.monotonic()
        if now - self._dream_last_due_scan < 30:
            return
        try:
            self.dream_triggers.schedule_due()
            self._dream_last_due_scan = now
        except Exception:
            LOGGER.exception("Auto Dream due scan failed")

    def process_embedding_jobs(self, *, max_jobs: int = 64) -> Any | None:
        if self.embedding_worker is None:
            return None
        return self.embedding_worker.run_until_idle(max_jobs=max(max_jobs, 1))

    def close(self) -> None:
        self.handle.runtime.close()

    @classmethod
    def from_env(cls) -> "AmemBridge | NoopAmemBridge":
        if os.getenv("AMEM_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
            return NoopAmemBridge()
        try:
            return cls(os.getenv("AMEM_DB_PATH") or None)
        except Exception:
            return NoopAmemBridge()

    def record_behavior(self, payload: dict[str, Any]) -> dict[str, Any]:
        event = _normalize_event(payload)
        user_id = str(payload.get("userId") or payload.get("user_id") or "legacy-owner")
        session_id = str(payload.get("sessionId") or payload.get("session_id") or PROFILE_SESSION_ID)
        with self.store_bundle.transaction():
            stored = self._record_event(event, user_id=user_id, session_id=session_id, payload=payload)
            modes = self.dream_triggers.record_event(stored)
        if "micro" in modes:
            self._dream_applied(user_id)
        return {"enabled": True, "eventId": stored.event_id, "memoryIds": []}

    def record_profile_statement(
        self,
        *,
        user_id: str,
        description: str,
        profile: Any,
        source: str,
    ) -> dict[str, Any]:
        stored = self._record_event(
            "profile_statement",
            user_id=user_id,
            session_id=PROFILE_SESSION_ID,
            payload={
                "event": "profile_statement",
                "description": description,
                "profile": profile.to_dict() if hasattr(profile, "to_dict") else profile,
                "source": source,
            },
        )
        memory_ids = self._write_profile_statement_memories(
            user_id=user_id,
            source_event_id=stored.event_id,
            description=description,
            profile=profile,
            source=source,
        )
        return {"enabled": True, "eventId": stored.event_id, "memoryIds": memory_ids}

    def promote_music_profile(
        self,
        *,
        user_id: str,
        profile: Any,
        support_counts: dict[str, int],
    ) -> dict[str, Any]:
        stored = self._record_event(
            "profile_promoted",
            user_id=user_id,
            session_id=PROFILE_SESSION_ID,
            payload={
                "event": "profile_promoted",
                "profile": profile.to_dict() if hasattr(profile, "to_dict") else profile,
                "supportCounts": support_counts,
            },
        )
        memory_ids: list[str] = []
        for polarity, topics in (
            ("positive", getattr(profile, "positive_topics", {})),
            ("negative", getattr(profile, "negative_topics", {})),
        ):
            for topic, weight in _score_items(topics):
                memory_id = self._save_memory(
                    user_id=user_id,
                    key=f"music:l3:topic:{polarity}:{topic}",
                    content=(
                        f"User has a stable music preference for topic: {topic}."
                        if polarity == "positive"
                        else f"User has a stable music avoidance for topic: {topic}."
                    ),
                    event_kind="belief.stated",
                    layer="core",
                    source_event_id=stored.event_id,
                    confidence=max(float(weight), 0.84),
                    salience=max(float(weight), 0.84),
                    metadata={
                        "signal": f"profile_l3_{polarity}_topic",
                        "topic": topic,
                        "supportCount": int(support_counts.get(topic, 0)),
                        "level": "L3",
                    },
                )
                if memory_id:
                    memory_ids.append(memory_id)
        return {"enabled": True, "eventId": stored.event_id, "memoryIds": memory_ids}

    def demote_music_profile(
        self,
        *,
        user_id: str,
        profile: Any,
        reasons: dict[str, str],
    ) -> dict[str, Any]:
        """Supersede active L3 memories invalidated by reverse evidence or time decay."""
        from agent_memory_runtime.memory.intake.models import MemoryToolIdentity

        stored = self._record_event(
            "profile_demoted",
            user_id=user_id,
            session_id=PROFILE_SESSION_ID,
            payload={
                "event": "profile_demoted",
                "profile": profile.to_dict() if hasattr(profile, "to_dict") else profile,
                "reasons": reasons,
            },
        )
        targets = {
            (polarity, str(topic).strip())
            for polarity, topics in (
                ("positive", getattr(profile, "positive_topics", {})),
                ("negative", getattr(profile, "negative_topics", {})),
            )
            for topic in topics
            if str(topic).strip()
        }
        if not targets:
            return {"enabled": True, "eventId": stored.event_id, "memoryIds": []}
        identity = MemoryToolIdentity(
            actor_id=user_id,
            agent_id=AGENT_ID,
            session_id=PROFILE_SESSION_ID,
            tenant_id=TENANT_ID,
            user_id=user_id,
            labels=("private",),
            tags=MUSIC_TAGS,
        )
        demoted: list[str] = []
        for record in self.handle.runtime.memory_store.list_records():
            metadata = getattr(record, "metadata", {}) or {}
            signal = str(metadata.get("signal") or "")
            polarity = (
                "positive"
                if signal == "profile_l3_positive_topic"
                else "negative" if signal == "profile_l3_negative_topic" else ""
            )
            topic = str(metadata.get("topic") or "").strip()
            if (
                (polarity, topic) not in targets
                or getattr(record, "user_id", None) != user_id
                or getattr(record, "status", "") != "active"
            ):
                continue
            reason = reasons.get(f"{polarity}:{topic}") or "l3_evidence_invalidated"
            result = self.handle.intake.revise_memory(
                {
                    "kind": "belief.stated",
                    "operation": "supersede",
                    "target_memory_id": record.memory_id,
                    "key": str(metadata.get("key") or record.memory_id),
                    "content": record.content,
                    "layer": "core",
                    "scope": "private",
                    "confidence": record.confidence,
                    "salience": record.salience,
                    "source_memory_ids": [record.memory_id],
                    "evidence_event_ids": [stored.event_id],
                    "reason": reason,
                },
                identity=identity,
                idempotency_key=f"demote:{record.memory_id}:{stored.event_id}",
            )
            if result.status == "succeeded":
                demoted.append(record.memory_id)
        return {"enabled": True, "eventId": stored.event_id, "memoryIds": demoted}

    def retrieve_memories(self, user_id: str, scene: str, *, limit: int = 12) -> list[RelevantMemory]:
        from agent_memory_runtime.domain.query import MemoryQuery

        query = MemoryQuery(
            agent_id=AGENT_ID,
            tenant_id=TENANT_ID,
            user_id=user_id,
            session_id=PROFILE_SESSION_ID,
            session_policy="profile",
            visibilities=("private",),
            tags=MUSIC_TAGS,
            limit=limit,
            text=(
                "Music recommendation profile for scene "
                f"{scene}: long-term preferences, recent interests, negative feedback, "
                "uploader preferences, mood preferences, and recommendation strategy."
            ),
        )
        try:
            if _retrieval_route(scene) == "lexical_first":
                candidates = self.handle.runtime.memory_store.query_records(
                    query,
                    limit=max(limit * 4, limit),
                    offset=0,
                )
                records, _trace = self.handle.runtime.retrieval.retrieve(candidates, query)
            else:
                records, _trace = self.handle.runtime.retrieve(query)
        except Exception:
            records = []
        records = [*records, *self._profile_statement_records(user_id)]
        seen: set[str] = set()
        unique_records = []
        for record in records:
            memory_id = str(getattr(record, "memory_id", ""))
            if not memory_id or memory_id in seen:
                continue
            seen.add(memory_id)
            unique_records.append(record)
        memories = [RelevantMemory.from_record(record) for record in unique_records]
        memories.sort(key=_retrieved_memory_priority, reverse=True)
        return memories[:limit]

    def _profile_statement_records(self, user_id: str) -> list[Any]:
        try:
            records = self.handle.runtime.memory_store.list_records()
        except Exception:
            return []
        result = []
        for record in records:
            if getattr(record, "tenant_id", "") != TENANT_ID:
                continue
            if getattr(record, "user_id", None) != user_id:
                continue
            metadata = getattr(record, "metadata", {}) or {}
            signal = str(metadata.get("signal") or "")
            if signal.startswith("profile_statement_"):
                result.append(record)
        return result

    def _record_event(self, event: str, *, user_id: str, session_id: str, payload: dict[str, Any]) -> Any:
        from agent_memory_runtime.domain.event import Event

        event_id = str(payload.get("event_id") or payload.get("eventId") or uuid4())
        occurred_at = str(
            payload.get("occurred_at")
            or payload.get("occurredAt")
            or datetime.now(timezone.utc).isoformat()
        )
        return self.handle.runtime.ingest(
            Event(
                event_id=event_id,
                kind="observation.created",
                actor_id=user_id,
                session_id=session_id,
                tenant_id=TENANT_ID,
                user_id=user_id,
                agent_id=AGENT_ID,
                labels=("private",),
                tags=(*MUSIC_TAGS, "behavior", event),
                payload={**payload, "event": event},
                occurred_at=occurred_at,
            )
        ).event

    def _write_aggregate_memories(
        self,
        event: str,
        *,
        user_id: str,
        source_event_id: str,
        payload: dict[str, Any],
    ) -> list[str]:
        track = _track_from_payload(payload)
        entities = _entities_from_track(track, payload)
        uploader_key = _uploader_key(track)
        memory_ids: list[str] = []

        if event in POSITIVE_EVENTS:
            for entity in entities[:MAX_TOPIC_MEMORIES_PER_EVENT]:
                key = f"music:topic:positive:{entity.name}"
                layer = "core" if self._is_core_ready(user_id=user_id, key=key) else "working"
                memory_id = self._save_memory(
                    user_id=user_id,
                    key=key,
                    content=(
                        f"User has a stable music preference for topic: {entity.name}."
                        if layer == "core"
                        else f"User shows recent positive music preference for topic: {entity.name}."
                    ),
                    event_kind="belief.stated",
                    layer=layer,
                    source_event_id=source_event_id,
                    confidence=0.84 if layer == "core" else entity.confidence,
                    salience=0.84 if layer == "core" else entity.confidence,
                    metadata={
                        "signal": "positive_topic",
                        "topic": entity.name,
                        "entityKind": entity.kind,
                        "event": event,
                    },
                )
                if memory_id:
                    memory_ids.append(memory_id)
            if uploader_key:
                key = f"music:uploader:preferred:{uploader_key}"
                layer = "core" if self._is_core_ready(user_id=user_id, key=key) else "working"
                memory_id = self._save_memory(
                    user_id=user_id,
                    key=key,
                    content=f"User prefers music from uploader {uploader_key}.",
                    event_kind="belief.stated",
                    layer=layer,
                    source_event_id=source_event_id,
                    confidence=0.78 if layer == "core" else 0.68,
                    salience=0.78 if layer == "core" else 0.68,
                    metadata={
                        "signal": "preferred_uploader",
                        "uploader": uploader_key,
                        "event": event,
                        "owner": "" if track is None else track.owner,
                    },
                )
                if memory_id:
                    memory_ids.append(memory_id)

        if event in NEGATIVE_EVENTS:
            for entity in entities[:MAX_TOPIC_MEMORIES_PER_EVENT]:
                confidence, salience = _negative_topic_strength(entity.name)
                memory_id = self._save_memory(
                    user_id=user_id,
                    key=f"music:topic:negative:{entity.name}",
                    content=f"User shows recent negative music signal for topic: {entity.name}.",
                    event_kind="belief.stated",
                    layer="working",
                    source_event_id=source_event_id,
                    confidence=confidence,
                    salience=salience,
                    metadata={
                        "signal": "negative_topic",
                        "topic": entity.name,
                        "entityKind": entity.kind,
                        "event": event,
                    },
                )
                if memory_id:
                    memory_ids.append(memory_id)

        if event == "track_reviewed":
            rating = int(payload.get("rating") or 0)
            mood = str(payload.get("mood") or "").strip()
            if mood:
                polarity = "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral"
                memory_id = self._save_memory(
                    user_id=user_id,
                    key=f"music:mood:{polarity}:{mood}",
                    content=f"User rated music mood '{mood}' as {polarity} with rating {rating}.",
                    event_kind="preference.updated",
                    layer="working",
                    source_event_id=source_event_id,
                    confidence=0.82,
                    salience=0.8,
                    metadata={"signal": "mood", "mood": mood, "rating": rating, "polarity": polarity},
                )
                if memory_id:
                    memory_ids.append(memory_id)
            if rating >= 4:
                for entity in entities[:MAX_TOPIC_MEMORIES_PER_EVENT]:
                    key = f"music:topic:positive:{entity.name}"
                    layer = "core" if self._is_core_ready(user_id=user_id, key=key) else "working"
                    memory_id = self._save_memory(
                        user_id=user_id,
                        key=key,
                        content=(
                            f"User has a stable music preference for topic: {entity.name}."
                            if layer == "core"
                            else f"User's private review supports recent preference for topic: {entity.name}."
                        ),
                        event_kind="belief.stated",
                        layer=layer,
                        source_event_id=source_event_id,
                        confidence=0.84 if layer == "core" else max(entity.confidence, 0.74),
                        salience=0.84 if layer == "core" else max(entity.confidence, 0.74),
                        metadata={
                            "signal": "review_positive_topic",
                            "topic": entity.name,
                            "entityKind": entity.kind,
                            "event": event,
                            "rating": rating,
                        },
                    )
                    if memory_id:
                        memory_ids.append(memory_id)

        return memory_ids

    def _write_profile_statement_memories(
        self,
        *,
        user_id: str,
        source_event_id: str,
        description: str,
        profile: Any,
        source: str,
    ) -> list[str]:
        memory_ids: list[str] = []
        for topic, weight in _score_items(getattr(profile, "positive_topics", {})):
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:topic:positive:{topic}",
                content=f"User explicitly states a music preference for topic: {topic}.",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=max(weight, 0.82),
                salience=max(weight, 0.82),
                metadata={
                    "signal": "profile_statement_positive_topic",
                    "topic": topic,
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        for topic, weight in _score_items(getattr(profile, "negative_topics", {})):
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:topic:negative:{topic}",
                content=f"User explicitly states a negative music preference for topic: {topic}.",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=max(weight, 0.82),
                salience=max(weight, 0.82),
                metadata={
                    "signal": "profile_statement_negative_topic",
                    "topic": topic,
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        for mood, weight in _score_items(getattr(profile, "mood_weights", {})):
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:mood:positive:{mood}",
                content=f"User explicitly states a music mood preference for mood '{mood}'.",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=max(weight, 0.74),
                salience=max(weight, 0.74),
                metadata={
                    "signal": "profile_statement_mood",
                    "mood": mood,
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        for uploader, weight in _score_items(getattr(profile, "preferred_uploaders", {})):
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:uploader:preferred:{uploader}",
                content=f"User explicitly states a preference for artist or uploader {uploader}.",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=max(weight, 0.78),
                salience=max(weight, 0.78),
                metadata={
                    "signal": "profile_statement_preferred_uploader",
                    "uploader": uploader,
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        for intent in getattr(profile, "recent_intents", []) or []:
            normalized_intent = str(intent or "").strip()
            if not normalized_intent:
                continue
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:intent:{normalized_intent[:80]}",
                content=f"User recent music search intent: {normalized_intent[:120]}.",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=0.74,
                salience=0.74,
                metadata={
                    "signal": "profile_statement_recent_intent",
                    "intent": normalized_intent[:120],
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        persona_parts = []
        if getattr(profile, "music_persona", ""):
            persona_parts.append(f"music personality: {profile.music_persona}")
        if getattr(profile, "mbti", ""):
            persona_parts.append(f"tentative MBTI: {profile.mbti}")
        if getattr(profile, "current_music_phase", ""):
            persona_parts.append(f"current music phase: {profile.current_music_phase}")
        if getattr(profile, "core_traits", None):
            persona_parts.append("core traits: " + ", ".join(profile.core_traits))
        if getattr(profile, "psychological_needs", None):
            persona_parts.append("psychological needs: " + ", ".join(profile.psychological_needs))
        if persona_parts:
            persona_confidence = max(float(getattr(profile, "persona_confidence", 0.0) or 0.0), 0.55)
            memory_id = self._save_memory(
                user_id=user_id,
                key="music:persona:current",
                content="User inferred music persona (tentative): " + "; ".join(persona_parts),
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=persona_confidence,
                salience=max(persona_confidence, 0.65),
                metadata={
                    "signal": "profile_statement_persona",
                    "mbti": str(getattr(profile, "mbti", ""))[:8],
                    "musicPersona": str(getattr(profile, "music_persona", ""))[:500],
                    "currentMusicPhase": str(getattr(profile, "current_music_phase", ""))[:300],
                    "coreTraits": list(getattr(profile, "core_traits", []) or [])[:8],
                    "psychologicalNeeds": list(getattr(profile, "psychological_needs", []) or [])[:8],
                    "personaConfidence": persona_confidence,
                    "source": source,
                },
            )
            if memory_id:
                memory_ids.append(memory_id)

        if description.strip():
            memory_id = self._save_memory(
                user_id=user_id,
                key=f"music:profile_statement:{source_event_id}",
                content=f"User music profile statement: {description.strip()[:1000]}",
                event_kind="preference.updated",
                layer="working",
                source_event_id=source_event_id,
                confidence=0.72,
                salience=0.72,
                metadata={"signal": "profile_statement_raw", "source": source},
            )
            if memory_id:
                memory_ids.append(memory_id)

        return memory_ids

    def _is_core_ready(self, *, user_id: str, key: str) -> bool:
        min_age_days = _env_int("AMEM_CORE_PROMOTION_MIN_AGE_DAYS", DEFAULT_CORE_PROMOTION_MIN_AGE_DAYS)
        min_count = _env_int("AMEM_CORE_PROMOTION_MIN_COUNT", 2)
        if min_age_days <= 0:
            return self._reinforcement_count(user_id=user_id, key=key) >= min_count

        cutoff = datetime.now(timezone.utc) - timedelta(days=min_age_days)
        try:
            records = self.handle.runtime.memory_store.list_records()
        except Exception:
            return False
        for record in records:
            if getattr(record, "tenant_id", "") != TENANT_ID:
                continue
            if getattr(record, "user_id", None) != user_id:
                continue
            if getattr(record, "metadata", {}).get("key") != key:
                continue
            created_at = _record_created_at(record)
            if created_at is not None and created_at <= cutoff:
                return True
        return False

    def _reinforcement_count(self, *, user_id: str, key: str) -> int:
        try:
            records = self.handle.runtime.memory_store.list_records()
        except Exception:
            return 0
        count = 0
        for record in records:
            if getattr(record, "tenant_id", "") != TENANT_ID:
                continue
            if getattr(record, "user_id", None) != user_id:
                continue
            if getattr(record, "metadata", {}).get("key") != key:
                continue
            count = max(count, int(getattr(record, "reinforcement_count", 0) or 0))
        return count

    def _save_memory(
        self,
        *,
        user_id: str,
        key: str,
        content: str,
        event_kind: str,
        layer: str,
        source_event_id: str,
        confidence: float,
        salience: float,
        metadata: dict[str, Any],
    ) -> str | None:
        from agent_memory_runtime.memory.intake.models import MemoryToolIdentity

        identity = MemoryToolIdentity(
            actor_id=user_id,
            agent_id=AGENT_ID,
            session_id=PROFILE_SESSION_ID,
            tenant_id=TENANT_ID,
            user_id=user_id,
            labels=("private",),
            tags=MUSIC_TAGS,
        )
        try:
            result = self.handle.intake.save_memory(
                {
                    "kind": event_kind,
                    "key": key,
                    "content": content,
                    "layer": layer,
                    "scope": "private",
                    "confidence": confidence,
                    "salience": salience,
                    "evidence_event_ids": [source_event_id],
                    "metadata": metadata,
                },
                identity=identity,
                idempotency_key=f"{key}:{source_event_id}:{uuid4()}",
            )
        except Exception as exc:
            LOGGER.warning("AMEM memory write failed for key %s: %s", key, exc)
            return None
        return result.memory_ids[0] if result.memory_ids else None


def record_music_behavior(
    bridge: Any,
    *,
    user_id: str,
    event: str,
    track: Track | dict[str, Any] | None = None,
    scene: str = "home",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if bridge is None:
        return {"enabled": False, "eventId": None, "memoryIds": []}
    values = dict(payload or {})
    values.update({"userId": user_id, "event": event, "scene": scene})
    if track is not None:
        values["track"] = track.to_dict() if isinstance(track, Track) else dict(track)
    return bridge.record_behavior(values)


def _normalize_event(payload: dict[str, Any]) -> str:
    event = str(payload.get("event") or "shown").strip().lower()[:64] or "shown"
    return EVENT_ALIASES.get(event, event)


def _retrieval_route(scene: str) -> str:
    normalized_scene = str(scene or "music_recommendation").strip().lower()[:64]
    env_key = f"AMEM_RETRIEVAL_ROUTE_{normalized_scene.upper().replace('-', '_')}"
    route = os.getenv(env_key, "").strip().lower()
    if not route:
        route = DEFAULT_RETRIEVAL_ROUTES.get(normalized_scene, "")
    if not route:
        route = os.getenv("AMEM_RETRIEVAL_ROUTE_DEFAULT", "hybrid").strip().lower()
    if route in {"lexical", "lexical_first", "fts5", "fts5_only"}:
        return "lexical_first"
    return "hybrid"


def _track_from_payload(payload: dict[str, Any]) -> Track | None:
    raw = payload.get("track")
    if isinstance(raw, Track):
        return raw
    if isinstance(raw, dict) and raw.get("bvid"):
        try:
            return Track.from_dict(raw)
        except Exception:
            return None
    return None


def _topics_from_track(track: Track | None, payload: dict[str, Any]) -> list[str]:
    return [entity.name for entity in _entities_from_track(track, payload)]


def _entities_from_track(track: Track | None, payload: dict[str, Any]) -> list[MusicEntity]:
    entities: list[MusicEntity] = []
    mood = str(payload.get("mood") or "").strip()
    if mood:
        entities.append(MusicEntity(mood, "mood", 0.68))
    if track is not None:
        text_parts = [track.title, track.page_title or "", track.owner or ""]
        text = " ".join(part for part in text_parts if part)
        entities.extend(_semantic_entities(text))
        entities.extend(_artist_entities(track.title))
    return _dedupe_entities(entities)


def _semantic_entities(text: str) -> list[MusicEntity]:
    normalized = text or ""
    lowered = normalized.casefold()
    entities: list[MusicEntity] = []
    for name, patterns in MUSIC_ENTITY_PATTERNS:
        if any(_pattern_matches(lowered, pattern.casefold()) for pattern in patterns):
            entities.append(MusicEntity(name, _entity_kind_for_name(name), 0.78))

    has_rap = _has_english_token(lowered, "rap") or any(term in lowered for term in ("说唱", "嘻哈"))
    if has_rap and (_has_english_token(lowered, "freestyle") or "freestyle" in lowered):
        entities.insert(0, MusicEntity("Freestyle Rap", "genre", 0.72))

    keywords = _keywords(normalized)
    if has_rap:
        keywords = [
            item
            for item in keywords
            if item.strip().casefold() not in {"rap", "freestyle"}
        ]
    for keyword in keywords:
        if not _is_low_value_keyword(keyword):
            entities.append(MusicEntity(keyword, "keyword", 0.62))
    return _dedupe_entities(entities)


def _artist_entities(title: str) -> list[MusicEntity]:
    artists: list[MusicEntity] = []
    for artist in _artist_candidates(title):
        artists.append(MusicEntity(artist, "artist", 0.76))
    return artists


def _artist_candidates(title: str) -> list[str]:
    text = _strip_bracketed_noise(title or "")
    candidates: list[str] = []
    if " - " in text:
        before, after = text.split(" - ", 1)
        left = before.strip()
        right = after.strip()
        if _looks_like_artist(left):
            candidates.append(left)
        if _looks_like_artist(right):
            candidates.append(right)

    for match in re.finditer(r"《([^》]{1,32})》", text):
        before = text[: match.start()].strip(" -_｜|【】[]()（）")
        if _looks_like_artist(before):
            candidates.append(before)

    for marker in ("feat.", "feat ", "ft.", "ft ", "cover by", "翻唱"):
        lowered = text.casefold()
        index = lowered.find(marker)
        if index >= 0:
            after = text[index + len(marker):].strip(" :-_｜|")
            candidate = re.split(r"[，,。/｜|【】\[\]()（）]", after, maxsplit=1)[0].strip()
            if _looks_like_artist(candidate):
                candidates.append(candidate)

    return list(dict.fromkeys(candidate[:40] for candidate in candidates))


def _strip_bracketed_noise(text: str) -> str:
    return re.sub(r"^[【\[].*?[】\]]", "", text).strip()


def _looks_like_artist(value: str) -> bool:
    normalized = value.strip()
    if not normalized or len(normalized) > 40:
        return False
    lowered = normalized.casefold()
    if lowered in GENERIC_TOPIC_STOPWORDS:
        return False
    if any(word in lowered for word in ("official", "mv", "music video", "完整版", "合集", "歌单")):
        return False
    return bool(re.search(r"[A-Za-z\u4e00-\u9fff]", normalized))


def _pattern_matches(text: str, pattern: str) -> bool:
    if pattern.isascii() and pattern.replace("-", "").replace(" ", "").isalnum():
        parts = [part for part in re.split(r"[\s-]+", pattern) if part]
        escaped = r"[\s-]+".join(re.escape(part) for part in parts)
        return re.search(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", text) is not None
    return pattern in text


def _entity_kind_for_name(name: str) -> str:
    if name in {"Vocaloid", "初音未来", "洛天依", "ACG"}:
        return "culture"
    if name in {"J-Pop", "K-Pop", "华语", "粤语"}:
        return "language_scene"
    if name in {"纯音乐", "钢琴", "吉他", "Lo-fi", "Rap", "Freestyle Rap", "治愈系"}:
        return "genre"
    return "keyword"


def _dedupe_entities(entities: list[MusicEntity]) -> list[MusicEntity]:
    result: dict[str, MusicEntity] = {}
    for entity in entities:
        name = entity.name.strip()
        if not name:
            continue
        existing = result.get(name)
        if existing is None or entity.confidence > existing.confidence:
            result[name] = MusicEntity(name, entity.kind, entity.confidence)
    return list(result.values())


def _is_low_value_keyword(keyword: str) -> bool:
    lowered = keyword.strip().casefold()
    if lowered in GENERIC_TOPIC_STOPWORDS:
        return True
    if lowered in {"feat", "cover", "live", "mv", "mad", "ost", "op", "ed"}:
        return True
    return False


def _legacy_topics_from_track(track: Track | None, payload: dict[str, Any]) -> list[str]:
    topics: list[str] = []
    mood = str(payload.get("mood") or "").strip()
    if mood:
        topics.append(mood)
    if track is not None:
        topics.extend(_semantic_topics(track.title))
        if track.page_title:
            topics.extend(_semantic_topics(track.page_title))
    return list(dict.fromkeys(topic for topic in topics if topic))


def _semantic_topics(text: str) -> list[str]:
    normalized = text or ""
    lowered = normalized.casefold()
    topics: list[str] = []
    has_rap = _has_english_token(lowered, "rap")
    if has_rap:
        if _has_english_token(lowered, "freestyle"):
            topics.append("Freestyle Rap")
        topics.append("Rap")

    keywords = _keywords(normalized)
    if has_rap:
        keywords = [
            item
            for item in keywords
            if item.strip().casefold() not in {"rap", "freestyle"}
        ]
    topics.extend(keywords)
    return list(dict.fromkeys(topic for topic in topics if topic))


def _keywords(text: str) -> list[str]:
    result = []
    for item in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}|[\u4e00-\u9fff]{2,}", text or ""):
        if len(item) > 24:
            continue
        if item.strip().casefold() in GENERIC_TOPIC_STOPWORDS:
            continue
        result.append(item)
    return result[:4]


def _has_english_token(text: str, token: str) -> bool:
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(token.casefold())}(?![A-Za-z0-9])", text) is not None


def _negative_topic_strength(topic: str) -> tuple[float, float]:
    if " " in topic.strip():
        return 0.55, 0.55
    return 0.48, 0.5


def _record_created_at(record: Any) -> datetime | None:
    for attr in ("created_at", "createdAt", "updated_at", "updatedAt"):
        raw = getattr(record, attr, None)
        if raw is None:
            continue
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, str):
            try:
                value = raw.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(value)
            except ValueError:
                continue
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _score_items(values: Any) -> list[tuple[str, float]]:
    if not isinstance(values, dict):
        return []
    items: list[tuple[str, float]] = []
    for key, value in values.items():
        name = str(key).strip()
        if not name:
            continue
        try:
            weight = float(value)
        except (TypeError, ValueError):
            weight = 0.72
        items.append((name[:80], min(max(weight, 0.0), 1.0)))
    return sorted(items, key=lambda item: item[1], reverse=True)[:16]


def _retrieved_memory_priority(memory: RelevantMemory) -> tuple[int, float, float]:
    signal = str((memory.metadata or {}).get("signal") or "")
    layer_weight = {"core": 3, "working": 2, "archival": 1}.get(memory.layer, 0)
    statement_weight = 2 if signal.startswith("profile_statement_") else 0
    return (statement_weight, layer_weight, memory.salience, memory.confidence)


def _uploader_key(track: Track | None) -> str | None:
    if track is None:
        return None
    if track.owner_mid:
        return str(track.owner_mid)
    owner = track.owner.strip()
    return owner or None


def _default_amem_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "amem.sqlite3"


def _ensure_amem_import_path() -> None:
    # The AMEM package targets Python 3.11 and imports ``datetime.UTC``.
    # Local recommend-radio development may still use Python 3.10, so provide
    # the equivalent alias before importing AMEM modules.
    import datetime as _datetime
    import enum as _enum

    if not hasattr(_datetime, "UTC"):
        _datetime.UTC = _datetime.timezone.utc
    if not hasattr(_enum, "StrEnum"):
        class _StrEnum(str, _enum.Enum):
            pass

        _enum.StrEnum = _StrEnum
    current = Path(__file__).resolve()
    for parent in (current.parent, *current.parents):
        root_src = parent / "src"
        if root_src.exists() and str(root_src) not in sys.path:
            sys.path.insert(0, str(root_src))
            break
