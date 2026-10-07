from __future__ import annotations

from agent_memory_runtime.telemetry import traced
from agent_memory_runtime.agent.sampling import sample_sync

import json
import os
import re
import time
from dataclasses import dataclass, replace
from typing import Any

from amem_bridge import _ensure_amem_import_path
from env_loader import load_recommend_radio_env
from music_profile import MusicProfile, RelevantMemory, overlay_profile_snapshot

load_recommend_radio_env()

DEFAULT_PROFILE_TTL_SECONDS = 600
DEFAULT_LLM_ATTEMPTS = 2


@dataclass
class ProfileProjection:
    profile: MusicProfile
    memories: list[RelevantMemory]
    trace_id: str
    llm_latency_ms: float = 0.0
    cache_hit: bool = False


@dataclass
class _ChatResponse:
    content: str
    latency_ms: float = 0.0


@dataclass
class _ToolCallResponse:
    name: str
    arguments: dict[str, Any]
    latency_ms: float = 0.0


class RecommendationOpenAIChatClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key_env: str,
        model: str,
        timeout_seconds: int,
        temperature: float,
        top_p: float,
        max_tokens: int,
        extra_body: dict[str, Any] | None = None,
        json_response: bool = True,
        user_id: str | None = None,
        db_path: str | None = None,
        deepseek_user_key: bool = False,
    ) -> None:
        self.base_url = base_url
        self.api_key_env = api_key_env
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.temperature = temperature
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.extra_body = extra_body or {}
        self.json_response = json_response
        self.user_id = user_id
        self.db_path = db_path
        self.deepseek_user_key = deepseek_user_key

    def _api_key(self) -> str:
        if self.deepseek_user_key and self.user_id is not None:
            from settings_service import SettingsService

            stored = SettingsService(
                db_path=self.db_path, user_id=self.user_id
            ).get_deepseek_api_key()
            if not stored:
                raise RuntimeError("Personal DeepSeek API Key is required")
            return stored
        key = os.getenv(self.api_key_env, "").strip()
        if not key:
            raise RuntimeError(f"{self.api_key_env} is required for recommendation LLM")
        return key

    @traced('llm.chat')
    def complete(self, *, system_prompt: str, user_prompt: str) -> _ChatResponse:
        api_key = self._api_key()

        from agent_memory_runtime.llm.transport import get_openai_client

        client = get_openai_client(
            base_url=self.base_url,
            api_key_env=self.api_key_env,
            timeout_seconds=self.timeout_seconds,
            api_key=api_key,
        )
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.temperature,
            "top_p": self.top_p,
            "max_tokens": self.max_tokens,
            "extra_body": self.extra_body,
            "stream": False,
        }
        if self.json_response:
            kwargs["response_format"] = {"type": "json_object"}
        started = time.perf_counter()
        completion = sample_sync(lambda:client.chat.completions.create(**kwargs),model=self.model,provider="recommendation")
        return _ChatResponse(content=completion.choices[0].message.content or "", latency_ms=(time.perf_counter() - started) * 1000)

    @traced('llm.route')
    def complete_tool(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        tools: list[dict[str, Any]],
    ) -> _ToolCallResponse:
        api_key = self._api_key()
        from agent_memory_runtime.llm.transport import get_openai_client

        client = get_openai_client(
            base_url=self.base_url,
            api_key_env=self.api_key_env,
            timeout_seconds=self.timeout_seconds,
            api_key=api_key,
        )
        started = time.perf_counter()
        completion = sample_sync(lambda:client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tools=tools,
            tool_choice="required",
            temperature=0,
            top_p=1,
            max_tokens=min(self.max_tokens, 800),
            extra_body=self.extra_body,
            stream=False,
        ),model=self.model,provider="recommendation")
        calls = completion.choices[0].message.tool_calls or []
        if len(calls) != 1 or calls[0].type != "function":
            raise ValueError("router LLM did not return exactly one function call")
        try:
            arguments = json.loads(calls[0].function.arguments or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("router LLM returned invalid tool arguments") from exc
        if not isinstance(arguments, dict):
            raise ValueError("router LLM tool arguments must be an object")
        return _ToolCallResponse(name=calls[0].function.name, arguments=arguments, latency_ms=(time.perf_counter() - started) * 1000)


class NvidiaOpenAIChatClient(RecommendationOpenAIChatClient):
    pass


class ProfileProjector:
    def __init__(
        self,
        memory_retriever: Any,
        *,
        llm_client: Any | None = None,
        enabled: bool | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        self.memory_retriever = memory_retriever
        self.llm_client = llm_client
        self.enabled = _env_bool("RECOMMEND_LLM_ENABLED", False) if enabled is None else enabled
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else _env_int(
            "RECOMMEND_PROFILE_TTL_SECONDS",
            DEFAULT_PROFILE_TTL_SECONDS,
        )
        self._cache: dict[tuple[str, str, bool], tuple[float, ProfileProjection]] = {}
        self._last_llm_latency_ms = 0.0

    def clear_cache(self, user_id: str | None = None, scene: str | None = None) -> None:
        if user_id is None and scene is None:
            self._cache.clear()
            return
        for key in list(self._cache):
            key_user, key_scene = key[:2]
            if user_id is not None and key_user != user_id:
                continue
            if scene is not None and key_scene != scene:
                continue
            self._cache.pop(key, None)

    def project(
        self,
        *,
        user_id: str,
        scene: str,
        fallback_profile: MusicProfile,
    ) -> ProfileProjection:
        from settings_service import SettingsService

        llm_allowed = self.enabled and (
            self.llm_client is not None
            or SettingsService(user_id=user_id).has_deepseek_api_key()
        )
        cache_key = (user_id, scene, llm_allowed)
        cached = self._cache.get(cache_key)
        if cached and time.time() - cached[0] < self.ttl_seconds:
            return replace(cached[1], llm_latency_ms=0.0, cache_hit=True)

        memories = self.memory_retriever.retrieve_memories(user_id, scene, limit=16)
        trace_id = f"profile:{user_id}:{scene}:{int(time.time())}"
        if not llm_allowed:
            projection = ProfileProjection(
                profile=overlay_profile_snapshot(
                    self._fallback_with_memories(fallback_profile, memories), fallback_profile
                ),
                memories=memories,
                trace_id=trace_id,
            )
            self._cache[cache_key] = (time.time(), projection)
            return projection

        try:
            self._last_llm_latency_ms = 0.0
            profile = self._project_with_llm(
                memories, scene=scene, fallback_profile=fallback_profile, user_id=user_id
            )
        except Exception:
            profile = self._fallback_with_memories(fallback_profile, memories)
        profile = overlay_profile_snapshot(profile, fallback_profile)

        projection = ProfileProjection(profile=profile, memories=memories, trace_id=trace_id, llm_latency_ms=self._last_llm_latency_ms)
        self._cache[cache_key] = (time.time(), projection)
        return projection

    def project_committed(self, *, user_id: str, scene: str,
                          fallback_profile: MusicProfile) -> ProfileProjection:
        """Serving snapshot from governed memories without model synthesis."""
        memories = self.memory_retriever.retrieve_memories(user_id, scene, limit=16)
        profile = overlay_profile_snapshot(
            self._fallback_with_memories(fallback_profile, memories), fallback_profile
        )
        return ProfileProjection(profile=profile, memories=memories,
                                 trace_id=f"profile:committed:{user_id}:{scene}")

    def _project_with_llm(
        self,
        memories: list[RelevantMemory],
        *,
        scene: str,
        fallback_profile: MusicProfile,
        user_id: str | None = None,
    ) -> MusicProfile:
        client = self.llm_client or _default_llm_client(user_id=user_id)
        system_prompt = (
            "Extract a music recommendation profile from memories. "
            "Infer a best-effort tentative MBTI, current music phase, core traits and psychological needs from explicit "
            "profile statements and aggregated listening behavior. Keep confidence conservative. "
            "Return exactly one JSON object. No markdown, no code, no explanation. "
            "Use real values from memory text as map keys. "
            "positive preference -> positive_topics; skipped/negative -> negative_topics; "
            "preferred uploader -> preferred_uploaders; rated mood -> mood_weights."
        )
        user_prompt = json.dumps(
            {
                "scene": scene,
                "output_schema": {
                    "positive_topics": {"actual_extracted_topic_name": 0.0},
                    "negative_topics": {"actual_extracted_topic_name": 0.0},
                    "mbti": "best-effort four-letter tentative MBTI when evidence exists",
                    "music_persona": "music personality narrative",
                    "current_music_phase": "recent listening phase",
                    "core_traits": ["trait"],
                    "psychological_needs": ["need"],
                    "persona_evidence": ["memory-backed evidence"],
                    "persona_confidence": 0.0,
                    "preferred_uploaders": {"actual_uploader_mid_or_name": 0.0},
                    "avoid_uploaders": {"actual_uploader_mid_or_name": 0.0},
                    "blocked_uploaders": {"actual_uploader_mid_or_name": 0.0},
                    "mood_weights": {"actual_mood_name": 0.0},
                    "recent_intents": ["search intent"],
                    "same_uploader_limit": 0,
                    "exploration_ratio": 0.0,
                    "evidence_memory_ids": ["memory id"],
                    "confidence": 0.0,
                },
                "retrieved_memories": [memory.to_prompt_dict() for memory in memories],
                "fallback_profile": fallback_profile.to_dict(),
                "examples": {
                    "topic: Vocaloid preference": {"positive_topics": {"Vocaloid": 0.9}},
                    "topic: Rap negative": {"negative_topics": {"Rap": 0.8}},
                    "mood 'calm' positive": {"mood_weights": {"calm": 0.8}},
                },
            },
            ensure_ascii=False,
        )
        last_error: Exception | None = None
        for _attempt in range(_llm_attempts()):
            try:
                response = client.complete(system_prompt=system_prompt, user_prompt=user_prompt)
                self._last_llm_latency_ms = float(getattr(response, "latency_ms", 0.0))
                parsed = _parse_json_object(response.content)
                profile = MusicProfile.from_dict(parsed, source="llm")
                _remove_placeholder_intents(profile)
                _repair_placeholder_topics(profile, memories)
                _align_profile_with_memory_evidence(profile, memories)
                _separate_mood_only_topics(profile, memories)
                _resolve_topic_conflicts(profile, memories)
                _ensure_interest_texts(profile)
                memory_ids = {memory.memory_id for memory in memories}
                profile.evidence_memory_ids = [
                    memory_id for memory_id in profile.evidence_memory_ids if memory_id in memory_ids
                ]
                if not profile.evidence_memory_ids:
                    profile.evidence_memory_ids = [memory.memory_id for memory in memories[:8]]
                if memories and not _profile_has_signals(profile):
                    raise ValueError("LLM returned an empty MusicProfile despite retrieved memories")
                return profile
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("LLM profile projection failed without an exception")

    @staticmethod
    def _fallback_with_memories(
        fallback_profile: MusicProfile,
        memories: list[RelevantMemory],
    ) -> MusicProfile:
        profile = MusicProfile.from_dict(fallback_profile.to_dict(), source="fallback")
        for memory in memories:
            metadata = memory.metadata or {}
            signal = str(metadata.get("signal") or "")
            weight = max(memory.salience, memory.confidence)
            if signal in {"profile_statement_positive_topic", "profile_l3_positive_topic"}:
                topic = str(metadata.get("topic") or "").strip()
                if topic:
                    profile.positive_topics[topic] = max(profile.positive_topics.get(topic, 0.0), weight)
                    continue
            if signal in {"profile_statement_negative_topic", "profile_l3_negative_topic"}:
                topic = str(metadata.get("topic") or "").strip()
                if topic:
                    profile.negative_topics[topic] = max(profile.negative_topics.get(topic, 0.0), weight)
                    continue
            if signal == "profile_statement_mood":
                mood = str(metadata.get("mood") or "").strip()
                if mood:
                    profile.mood_weights[mood] = max(profile.mood_weights.get(mood, 0.0), weight)
                    continue
            if signal == "profile_statement_preferred_uploader":
                uploader = str(metadata.get("uploader") or "").strip()
                if uploader:
                    profile.preferred_uploaders[uploader] = max(profile.preferred_uploaders.get(uploader, 0.0), weight)
                    continue
            if signal == "profile_statement_recent_intent":
                intent = str(metadata.get("intent") or "").strip()
                if intent and intent not in profile.recent_intents:
                    profile.recent_intents.append(intent[:120])
                    continue
            if signal == "profile_statement_persona":
                profile.mbti = str(metadata.get("mbti") or profile.mbti)[:8]
                profile.music_persona = str(metadata.get("musicPersona") or profile.music_persona)[:500]
                profile.current_music_phase = str(
                    metadata.get("currentMusicPhase") or profile.current_music_phase
                )[:300]
                profile.core_traits = [str(item)[:120] for item in metadata.get("coreTraits") or []][:8]
                profile.psychological_needs = [
                    str(item)[:120] for item in metadata.get("psychologicalNeeds") or []
                ][:8]
                profile.persona_confidence = max(
                    profile.persona_confidence,
                    float(metadata.get("personaConfidence") or 0.0),
                )
                continue

            content = memory.content.casefold()
            topic = _mood_from_content(memory.content) if "mood" in content else _topic_from_content(memory.content)
            if not topic:
                continue
            if "negative" in content:
                profile.negative_topics[topic] = max(profile.negative_topics.get(topic, 0.0), weight)
            elif "mood" in content:
                profile.mood_weights[topic] = max(profile.mood_weights.get(topic, 0.0), weight)
            elif "preference" in content or "prefers" in content:
                profile.positive_topics[topic] = max(profile.positive_topics.get(topic, 0.0), weight)
        profile.evidence_memory_ids = [memory.memory_id for memory in memories[:8]]
        profile.confidence = max(profile.confidence, 0.45 if memories else 0.0)
        profile.source = "fallback"
        _ensure_interest_texts(profile)
        return profile


def _default_llm_client(*, user_id: str | None = None, db_path: str | None = None) -> Any:
    _ensure_amem_import_path()
    from agent_memory_runtime.config import LLMConfig
    from agent_memory_runtime.llm import OpenAICompatibleChatClient

    provider = os.getenv("RECOMMEND_LLM_PROVIDER", "").strip() or os.getenv("AMEM_LLM_PROVIDER", "").strip() or "deepseek"
    provider_id = provider.casefold()
    if provider_id == "deepseek":
        return RecommendationOpenAIChatClient(
            base_url=os.getenv("RECOMMEND_LLM_BASE_URL", "").strip() or "https://api.deepseek.com",
            api_key_env=os.getenv("RECOMMEND_LLM_API_KEY_ENV", "").strip() or "DEEPSEEK_API_KEY",
            model=os.getenv("RECOMMEND_LLM_MODEL", "").strip() or "deepseek-chat",
            timeout_seconds=_env_int("RECOMMEND_LLM_TIMEOUT_SECONDS", 8),
            temperature=_env_float("RECOMMEND_LLM_TEMPERATURE", 0.2),
            top_p=_env_float("RECOMMEND_LLM_TOP_P", 0.95),
            max_tokens=_env_int("RECOMMEND_LLM_MAX_TOKENS", 3000),
            extra_body=_json_env(
                "RECOMMEND_LLM_EXTRA_BODY",
                {"chat_template_kwargs": {"thinking": False}},
            ),
            json_response=_env_bool("RECOMMEND_LLM_JSON_RESPONSE", True),
            user_id=user_id,
            db_path=db_path,
            deepseek_user_key=True,
        )

    if provider_id in {"nvidia", "nvidia-nim"} or os.getenv("RECOMMEND_LLM_BASE_URL", "").strip():
        return NvidiaOpenAIChatClient(
            base_url=os.getenv("RECOMMEND_LLM_BASE_URL", "").strip() or "https://integrate.api.nvidia.com/v1",
            api_key_env=os.getenv("RECOMMEND_LLM_API_KEY_ENV", "").strip() or "NVIDIA_API_KEY",
            model=os.getenv("RECOMMEND_LLM_MODEL", "").strip() or "meta/llama-3.1-8b-instruct",
            timeout_seconds=_env_int("RECOMMEND_LLM_TIMEOUT_SECONDS", 8),
            temperature=_env_float("RECOMMEND_LLM_TEMPERATURE", 1.0),
            top_p=_env_float("RECOMMEND_LLM_TOP_P", 0.95),
            max_tokens=_env_int("RECOMMEND_LLM_MAX_TOKENS", 1200),
            extra_body=_json_env(
                "RECOMMEND_LLM_EXTRA_BODY",
                {"chat_template_kwargs": {"thinking": False}},
            ),
            json_response=_env_bool("RECOMMEND_LLM_JSON_RESPONSE", True),
        )

    model = os.getenv("RECOMMEND_LLM_MODEL", "").strip() or None
    timeout = _env_int("RECOMMEND_LLM_TIMEOUT_SECONDS", 8)
    extra_body = _json_env("RECOMMEND_LLM_EXTRA_BODY", {})
    if provider_id == "deepseek" and not extra_body:
        extra_body = {"chat_template_kwargs": {"thinking": False}}
    config = LLMConfig.for_provider(
        provider,
        model=model,
        timeout_seconds=timeout,
        max_tokens=_env_int("RECOMMEND_LLM_MAX_TOKENS", 1200),
        extra_body=extra_body,
    )
    return OpenAICompatibleChatClient(config)


def _llm_attempts() -> int:
    return max(_env_int("RECOMMEND_LLM_ATTEMPTS", DEFAULT_LLM_ATTEMPTS), 1)


def _parse_json_object(content: str) -> dict[str, Any]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("LLM response does not contain a JSON object")
    value = json.loads(text[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("MusicProfile response must be a JSON object")
    return value


def _topic_from_content(content: str) -> str | None:
    match = re.search(r"(?:topic|mood):\s*([^.\n]+)", content, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip("'\" ")[:80]
    return None


def _mood_from_content(content: str) -> str | None:
    match = re.search(r"mood\s+['\"]([^'\"]+)['\"]", content, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()[:80]
    match = re.search(r"mood:\s*([^.\n]+)", content, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip("'\" ")[:80]
    return None


def _profile_has_signals(profile: MusicProfile) -> bool:
    return any(
        [
            profile.positive_topics,
            profile.negative_topics,
            profile.preferred_uploaders,
            profile.avoid_uploaders,
            profile.blocked_uploaders,
            profile.mood_weights,
            profile.recent_intents,
            profile.mbti,
            profile.music_persona,
            profile.current_music_phase,
            profile.core_traits,
            profile.psychological_needs,
            profile.confidence > 0,
        ]
    )


def _ensure_interest_texts(profile: MusicProfile) -> None:
    if not profile.positive_interest_texts:
        texts: list[str] = []
        topics = _top_score_names(profile.positive_topics, 8)
        moods = _top_score_names(profile.mood_weights, 6)
        uploaders = _top_score_names(profile.preferred_uploaders, 4)
        if topics or moods:
            parts = []
            if topics:
                parts.append("偏好的音乐主题: " + "、".join(topics))
            if moods:
                parts.append("偏好的收听氛围: " + "、".join(moods))
            texts.append("用户适合推荐" + "；".join(parts))
        for intent in profile.recent_intents[:6]:
            texts.append("用户近期音乐搜索意图: " + intent)
        for uploader in uploaders:
            texts.append("用户偏好的歌手或UP主: " + uploader)
        profile.positive_interest_texts = _dedupe_texts(texts, limit=12)

    if not profile.negative_interest_texts:
        texts = []
        for topic in _top_score_names(profile.negative_topics, 8):
            texts.append("用户不适合推荐的音乐主题: " + topic)
        for uploader in _top_score_names(profile.avoid_uploaders, 4):
            texts.append("用户应减少推荐的歌手或UP主: " + uploader)
        for uploader in _top_score_names(profile.blocked_uploaders, 4):
            texts.append("用户明确屏蔽的歌手或UP主: " + uploader)
        profile.negative_interest_texts = _dedupe_texts(texts, limit=12)


def _top_score_names(values: dict[str, float], limit: int) -> list[str]:
    return [
        key
        for key, _score in sorted(
            (values or {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )[:limit]
        if str(key).strip()
    ]


def _dedupe_texts(values: list[str], *, limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized[:240])
        if len(out) >= limit:
            break
    return out


def _repair_placeholder_topics(profile: MusicProfile, memories: list[RelevantMemory]) -> None:
    placeholders = {
        "topic",
        "actual_topic",
        "actual_extracted_topic_name",
        "extracted_topic",
        "music_topic",
    }
    for memory in memories:
        content = memory.content.casefold()
        topic = _mood_from_content(memory.content) if "mood" in content else _topic_from_content(memory.content)
        if not topic:
            continue
        weight = max(memory.salience, memory.confidence)
        if "negative" in content or "skip" in content or "skipped" in content:
            _replace_placeholder_score(profile.negative_topics, placeholders, topic, weight)
        elif "mood" in content:
            _replace_placeholder_score(profile.mood_weights, placeholders, topic, weight)
        elif "preference" in content or "prefers" in content or "stable" in content:
            _replace_placeholder_score(profile.positive_topics, placeholders, topic, weight)


def _replace_placeholder_score(
    scores: dict[str, float],
    placeholders: set[str],
    replacement: str,
    default_weight: float,
) -> None:
    placeholder_values = [
        scores.pop(key)
        for key in list(scores)
        if key.strip().casefold() in placeholders
    ]
    if not placeholder_values:
        return
    scores[replacement] = max([scores.get(replacement, 0.0), default_weight, *placeholder_values])


def _remove_placeholder_intents(profile: MusicProfile) -> None:
    placeholders = {
        "search intent",
        "intent",
        "actual search intent",
        "query",
        "keyword",
    }
    profile.recent_intents = [
        intent for intent in profile.recent_intents
        if intent.strip().casefold() not in placeholders
    ]


def _align_profile_with_memory_evidence(profile: MusicProfile, memories: list[RelevantMemory]) -> None:
    positive: dict[str, float] = {}
    negative: dict[str, float] = {}
    moods: dict[str, float] = {}

    for memory in memories:
        content = memory.content.casefold()
        weight = max(memory.salience, memory.confidence)
        if "mood" in content:
            mood = _mood_from_content(memory.content)
            if mood:
                moods[mood] = max(moods.get(mood, 0.0), weight)
            continue

        topic = _topic_from_content(memory.content)
        if not topic:
            continue
        if "negative" in content or "skip" in content or "skipped" in content:
            if _memory_entity_kind(memory) == "keyword":
                continue
            negative[topic] = max(negative.get(topic, 0.0), weight)
        elif "preference" in content or "prefers" in content or "stable" in content:
            positive[topic] = max(positive.get(topic, 0.0), weight)

    for topic, weight in positive.items():
        profile.positive_topics[topic] = max(profile.positive_topics.get(topic, 0.0), weight)
        if topic not in negative:
            profile.negative_topics.pop(topic, None)
    for topic, weight in negative.items():
        profile.negative_topics[topic] = max(profile.negative_topics.get(topic, 0.0), weight)
        if topic not in positive:
            profile.positive_topics.pop(topic, None)
    for mood, weight in moods.items():
        profile.mood_weights[mood] = max(profile.mood_weights.get(mood, 0.0), weight)


def _separate_mood_only_topics(profile: MusicProfile, memories: list[RelevantMemory]) -> None:
    memory_topics: set[str] = set()
    memory_moods: set[str] = set()
    for memory in memories:
        content = memory.content.casefold()
        if "mood" in content:
            mood = _mood_from_content(memory.content)
            if mood:
                memory_moods.add(mood)
            continue
        topic = _topic_from_content(memory.content)
        if topic and ("preference" in content or "prefers" in content or "stable" in content):
            memory_topics.add(topic)

    for topic in list(profile.positive_topics):
        if topic in memory_moods:
            profile.mood_weights[topic] = max(profile.mood_weights.get(topic, 0.0), profile.positive_topics[topic])
            profile.positive_topics.pop(topic, None)
    for topic in list(profile.negative_topics):
        if _topic_only_supported_by_keyword_negative(topic, memories):
            profile.negative_topics.pop(topic, None)


def _resolve_topic_conflicts(profile: MusicProfile, memories: list[RelevantMemory]) -> None:
    explicit_negative_topics = {
        str((memory.metadata or {}).get("topic") or _topic_from_content(memory.content) or "").strip()
        for memory in memories
        if str((memory.metadata or {}).get("signal") or "") == "profile_statement_negative_topic"
    }
    explicit_negative_topics.discard("")
    for topic in explicit_negative_topics:
        profile.positive_topics.pop(topic, None)

    for topic in set(profile.positive_topics).intersection(profile.negative_topics):
        if profile.negative_topics[topic] >= profile.positive_topics[topic]:
            profile.positive_topics.pop(topic, None)
        else:
            profile.negative_topics.pop(topic, None)


def _memory_entity_kind(memory: RelevantMemory) -> str:
    raw = getattr(memory, "metadata", None)
    if isinstance(raw, dict):
        return str(raw.get("entityKind") or "")
    return ""


def _topic_only_supported_by_keyword_negative(topic: str, memories: list[RelevantMemory]) -> bool:
    supporting = []
    for memory in memories:
        content = memory.content.casefold()
        if "negative" not in content and "skip" not in content and "skipped" not in content:
            continue
        memory_topic = _topic_from_content(memory.content)
        if memory_topic == topic:
            supporting.append(memory)
    return bool(supporting) and all(_memory_entity_kind(memory) == "keyword" for memory in supporting)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _json_env(name: str, default: dict[str, Any]) -> dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return default
    return value if isinstance(value, dict) else default
