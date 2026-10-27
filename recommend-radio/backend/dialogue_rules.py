from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable
from music_keyword_pool import (
    detect_emotion,
    has_negative_intent,
    has_positive_intent,
    is_recall_request,
    is_recommendation_request,
    match_topics,
    matched_artist_names,
    topic_phrase,
)
from profile_projector import _default_llm_client, _parse_json_object
from request_spec import RequestInterpreter, RequestSpec


MAX_MESSAGE_LENGTH = 1000

SESSION_TURN_LIMIT = 60

VISIBLE_CARD_LIMIT = 12

RECALL_RESULT_LIMIT = 8

RECOMMENDATION_CARD_LIMIT = 8

CHAT_LLM_HISTORY_LIMIT = 10

INTENT_CHAT = "CHAT"

INTENT_GREETING = "GREETING"

INTENT_RECOMMEND = "RECOMMEND"

INTENT_PROFILE_UPDATE = "PROFILE_UPDATE"

INTENT_CONTROL = "CONTROL"

HIGH_CONFIDENCE_RECOMMEND_PHRASES = (
    "给我推荐",
    "给我推",
    "帮我推荐",
    "帮我找",
    "推荐几首",
    "推荐点",
    "推荐一点",
    "推荐一些",
    "推荐一下",
    "推荐下",
    "推几首",
    "推点",
    "推一点",
    "推一些",
    "来几首",
    "来点",
    "来一点",
    "来一些",
    "来一批",
    "来一轮",
    "来一组",
    "找几首",
    "找点",
    "找一点",
    "找一些",
    "放几首",
    "放点",
    "放一点",
    "放一些",
    "整点",
    "整一点",
    "整一些",
    "我想听",
    "想听点",
    "想听一点",
    "想听一些",
    "我要听",
    "现在想听",
    "今晚想听",
    "今天想听",
    "随便推荐",
    "随便来",
    "随便放",
    "随机推荐",
    "随机来",
    "换一批",
    "再来一批",
    "下一批",
    "换点别的",
    "再来点",
    "再来几首",
    "再推荐",
    "歌单",
    "recommend some",
    "recommend me",
    "play some",
)

RECOMMEND_DISCUSSION_PHRASES = (
    "推荐系统",
    "推荐算法",
    "推荐逻辑",
    "推荐机制",
    "推荐链路",
    "推荐架构",
    "怎么推荐",
    "如何推荐",
    "为什么推荐",
    "为啥推荐",
    "推荐得怎么样",
    "推荐准确",
    "推荐的时候",
    "推荐音乐时",
)

CONFIRM_ACTIONS = {"confirm", "accurate"}

REJECT_ACTIONS = {"reject", "inaccurate"}

VALID_ACTIONS = CONFIRM_ACTIONS | REJECT_ACTIONS | {"discuss", "later"}

GENERIC_PROBES = (
    {
        "kind": "interest_probe",
        "title": "今晚的听感",
        "prompt": "这轮想让我把歌放轻一点，还是保留一点律动感？",
        "statement": "用户希望当前推荐更偏向轻松、舒服、适合持续播放的音乐。",
        "topic": "轻松听感",
        "polarity": "positive",
    },
    {
        "kind": "avoid_probe",
        "title": "推荐边界",
        "prompt": "如果我连续推同一类歌，你希望我主动换口味吗？",
        "statement": "用户希望推荐列表减少同一类音乐连续出现。",
        "topic": "换口味",
        "polarity": "negative",
    },
    {
        "kind": "interest_probe",
        "title": "探索口味",
        "prompt": "要不要给你留一点没听过但气质接近的歌？",
        "statement": "用户愿意接受少量气质接近的新音乐探索。",
        "topic": "探索推荐",
        "polarity": "positive",
    },
)


@dataclass(frozen=True)
class ExtractedSignal:
    polarity: str
    topic: str
    statement: str
    confidence: float
    kind: str = "preference_hypothesis"
    commit_policy: str = "shadow"


@dataclass(frozen=True)
class DialogueRoute:
    tool: str
    reason: str
    signal: ExtractedSignal | None = None
    emotion: str = ""
    primary_intent: str = INTENT_CHAT
    intents: tuple[str, ...] = (INTENT_CHAT,)
    need_profile: bool = False
    need_memory: bool = False
    need_recommendation_search: bool = False
    control_action: str = ""
    request_spec: RequestSpec | None = None
    route_source: str = "rule"
    confidence: float = 1.0


def _serialize_session_summary(row: Any) -> dict[str, Any]:
    first_user_message = str(row["first_user_message"] or "").strip()
    focus = str(row["focus"] or "").strip()
    title = first_user_message or focus or "新的聊天"
    latest_message = str(row["latest_message"] or "").strip()
    return {
        "sessionId": row["session_id"],
        "title": _compact_text(title, 28),
        "preview": _compact_text(latest_message or title, 56),
        "state": row["state"],
        "focus": focus,
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "messageCount": int(row["message_count"] or 0),
    }


def _serialize_turn(row: Any, cards_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    payload = _json_loads(row["payload_json"])
    card_id = row["card_id"]
    return {
        "id": str(row["id"]),
        "role": row["role"],
        "content": row["content"],
        "cardId": card_id,
        "createdAt": row["created_at"],
        "quotedContext": payload.get("quotedContext"),
        "confirmedContext": payload.get("confirmedContext"),
        "card": cards_by_id.get(card_id) if card_id else None,
    }


def _serialize_card(row: Any) -> dict[str, Any]:
    payload = _json_loads(row["payload_json"])
    return {
        "cardId": row["card_id"],
        "kind": row["kind"],
        "status": row["status"],
        "title": row["title"],
        "prompt": row["prompt"],
        "statement": row["statement"],
        "topic": row["topic"],
        "polarity": row["polarity"],
        "sourceText": row["source_text"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "actions": _actions_for_kind(row["kind"]),
        "memoryIds": payload.get("memoryIds") or [],
        "eventId": payload.get("eventId"),
        "error": payload.get("error"),
        "note": payload.get("note") or "",
        "requestSpec": payload.get("requestSpec") or {},
        "sceneMemoryId": payload.get("sceneMemoryId"),
        "discoveryJobId": payload.get("discoveryJobId"),
        "discoveryStatus": payload.get("discoveryStatus"),
        "recommendations": payload.get("recommendations") or [],
        "tracks": payload.get("tracks") or [],
    }


def _actions_for_kind(kind: str) -> list[str]:
    if kind == "pending_confirmation":
        return ["accurate", "inaccurate", "discuss"]
    if kind in {"recommendation_carousel", "memory_recall"}:
        return []
    return ["confirm", "reject", "discuss", "later"]


def _card_context(card: Any) -> dict[str, Any]:
    return {
        "cardId": card["card_id"],
        "kind": card["kind"],
        "statement": card["statement"],
        "sourceText": card["source_text"] or card["statement"],
        "topic": card["topic"],
        "polarity": card["polarity"],
        "trackId": card.get("track_id") if isinstance(card, dict) else None,
    }


def _context_card_with_track(card: Any, track_id: str) -> dict[str, Any]:
    payload = _json_loads(card["payload_json"])
    for item in payload.get("recommendations") or []:
        track = item.get("track") if isinstance(item, dict) else None
        if isinstance(track, dict) and str(track.get("trackId") or "") == track_id:
            title = str(track.get("title") or "这首歌")
            owner = str(track.get("owner") or "")
            return {
                "card_id": card["card_id"],
                "kind": card["kind"],
                "statement": f"用户正在引用《{title}》{f'（{owner}）' if owner else ''}并讨论这首歌。",
                "source_text": title,
                "topic": title,
                "polarity": "neutral",
                "track_id": track_id,
                "payload_json": card["payload_json"],
            }
    return dict(card)


def _merge_recommendations(
    existing: list[dict[str, Any]],
    discovered: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*existing, *discovered]:
        if not isinstance(item, dict):
            continue
        track = item.get("track") if isinstance(item.get("track"), dict) else {}
        track_id = str(track.get("trackId") or track.get("bvid") or "")
        if not track_id or track_id in seen:
            continue
        seen.add(track_id)
        merged.append(item)
        if len(merged) >= limit:
            break
    return merged


def _route_payload(route: DialogueRoute, *, reply_engine: str | None = None) -> dict[str, Any]:
    template = _route_template(route)
    payload = {
        "route": route.tool,
        "routeReason": route.reason,
        "intent": route.primary_intent,
        "intents": list(route.intents),
        "needProfile": route.need_profile,
        "needMemory": route.need_memory,
        "needRecommendationSearch": route.need_recommendation_search,
        "routeTemplate": template,
    }
    if route.control_action:
        payload["controlAction"] = route.control_action
    if reply_engine:
        payload["replyEngine"] = reply_engine
    return payload


def _route_template(route: DialogueRoute) -> dict[str, Any]:
    return {
        "schemaVersion": "dialogue-route/v1",
        "tool": route.tool,
        "intent": route.primary_intent,
        "intents": list(route.intents),
        "needProfile": route.need_profile,
        "needMemory": route.need_memory,
        "needRecommendationSearch": route.need_recommendation_search,
        "controlAction": route.control_action or None,
        "emotion": route.emotion or None,
        "signal": None
        if route.signal is None
        else {
            "polarity": route.signal.polarity,
            "topic": route.signal.topic,
            "statement": route.signal.statement,
            "confidence": route.signal.confidence,
            "kind": route.signal.kind,
            "commitPolicy": route.signal.commit_policy,
        },
        "requestSpec": (route.request_spec or RequestSpec()).to_dict(),
        "source": route.route_source,
        "confidence": round(route.confidence, 3),
    }


def _canonical_route(
    route: DialogueRoute, message: str, *, source: str, confidence: float
) -> DialogueRoute:
    return replace(
        route,
        request_spec=RequestInterpreter().interpret(message),
        route_source=source,
        confidence=confidence,
    )


def _is_high_confidence_route(route: DialogueRoute, message: str) -> bool:
    if route.tool in {
        "control",
        "direct_chat",
        "explain_recommendation",
        "recall_memory",
        "confirm_signal",
        "profile_update",
    }:
        return True
    if route.tool == "recommend_music":
        return _is_high_confidence_recommendation_command(message)
    return False


def _is_high_confidence_recommendation_command(message: str) -> bool:
    normalized = _normalize_message(message).casefold()
    if not normalized or _has_any(normalized, RECOMMEND_DISCUSSION_PHRASES):
        return False

    # Negated meta statements must continue through the semantic router. A
    # scoped exclusion such as “不要中文，推荐欧美” is intentionally not blocked.
    compact = re.sub(r"\s+", "", normalized)
    if "不是让你推荐" in compact or "没让你推荐" in compact:
        return False
    if re.fullmatch(
        r"(?:请)?(?:不要|别|不用|无需|不需要)(?:再)?(?:给我|帮我)?(?:推荐|推|放)(?:歌|音乐)?(?:了|啦|吧|呀|啊|[。！!])?",
        compact,
    ):
        return False
    if _has_any(normalized, HIGH_CONFIDENCE_RECOMMEND_PHRASES):
        return True
    if re.search(r"(?:推荐|推|来|找|放|整)\s*\d+\s*首", normalized):
        return True
    return normalized.startswith(("推荐", "请推荐", "能推荐", "可以推荐", "帮忙推荐"))


_ROUTER_TOOL_DEFAULTS: dict[str, dict[str, Any]] = {
    "recommend_music": {
        "intent": INTENT_RECOMMEND,
        "profile": True,
        "memory": True,
        "search": True,
    },
    "profile_chat": {"intent": INTENT_CHAT, "profile": True, "memory": True, "search": False},
    "profile_update": {
        "intent": INTENT_PROFILE_UPDATE,
        "profile": True,
        "memory": False,
        "search": False,
    },
    "chat_with_signal": {"intent": INTENT_CHAT, "profile": False, "memory": True, "search": False},
    "casual_chat": {"intent": INTENT_CHAT, "profile": True, "memory": True, "search": False},
    "explain_recommendation": {
        "intent": INTENT_CHAT,
        "profile": True,
        "memory": True,
        "search": False,
    },
    "recall_memory": {"intent": INTENT_CHAT, "profile": False, "memory": True, "search": False},
}


def _llm_route_message(
    message: str,
    *,
    context_card: Any | None,
    recent_turns: list[dict[str, str]],
    client: Any,
) -> DialogueRoute:
    if not hasattr(client, "complete_tool"):
        raise ValueError("router LLM client does not support tool calls")
    system_prompt = (
        "You are a music dialogue router. Call exactly one route_dialogue tool. "
        "Never write a reply. Select recommend_music only when the user wants playable music now; "
        "select profile_update only for an explicit durable preference. A temporary request is not durable. "
        "Return a signal only when the message states a preference with a concrete topic."
    )
    tool = {
        "type": "function",
        "function": {
            "name": "route_dialogue",
            "description": "Return the normalized dialogue route template.",
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["tool"],
                "properties": {
                    "tool": {"type": "string", "enum": list(_ROUTER_TOOL_DEFAULTS)},
                    "emotion": {"type": "string", "maxLength": 32},
                    "signal": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["polarity", "topic", "kind", "commitPolicy"],
                        "properties": {
                            "polarity": {"type": "string", "enum": ["positive", "negative"]},
                            "topic": {"type": "string", "maxLength": 80},
                            "kind": {
                                "type": "string",
                                "enum": [
                                    "stable_preference",
                                    "preference_hypothesis",
                                    "recent_state",
                                ],
                            },
                            "commitPolicy": {
                                "type": "string",
                                "enum": ["commit", "shadow", "confirm"],
                            },
                        },
                    },
                },
            },
        },
    }
    response = client.complete_tool(
        system_prompt=system_prompt,
        user_prompt=json.dumps(
            {
                "message": message,
                "contextCard": _card_context(context_card) if context_card is not None else None,
                "recentTurns": _compact_recent_turns(recent_turns, limit=6),
            },
            ensure_ascii=False,
        ),
        tools=[tool],
    )
    if response.name != "route_dialogue":
        raise ValueError("router returned an unsupported tool name")
    args = response.arguments
    tool_name = str(args.get("tool") or "")
    defaults = _ROUTER_TOOL_DEFAULTS.get(tool_name)
    if defaults is None:
        raise ValueError("router returned an unsupported route")
    signal = _signal_from_tool_args(args.get("signal"), message)
    return DialogueRoute(
        tool=tool_name,
        reason="LLM tool route",
        signal=signal,
        emotion=str(args.get("emotion") or "")[:32],
        primary_intent=defaults["intent"],
        intents=_route_intents(defaults["intent"], signal),
        need_profile=defaults["profile"],
        need_memory=defaults["memory"],
        need_recommendation_search=defaults["search"],
    )


def _signal_from_tool_args(value: Any, message: str) -> ExtractedSignal | None:
    if not isinstance(value, dict):
        return None
    topic = _clean_topic(str(value.get("topic") or ""))
    polarity = str(value.get("polarity") or "")
    kind = str(value.get("kind") or "")
    policy = str(value.get("commitPolicy") or "")
    if (
        not topic
        or polarity not in {"positive", "negative"}
        or kind not in {"stable_preference", "preference_hypothesis", "recent_state"}
        or policy not in {"commit", "shadow", "confirm"}
    ):
        return None
    return ExtractedSignal(
        polarity=polarity,
        topic=topic,
        statement=_preference_statement(topic, message, kind),
        confidence=0.72,
        kind=kind,
        commit_policy=policy,
    )


def _route_message(message: str, context_card: Any | None) -> DialogueRoute:
    text = _normalize_message(message)
    control_action = _control_action(text)
    if control_action:
        return DialogueRoute(
            "control",
            "user sends playback control command",
            signal=None,
            emotion="",
            primary_intent=INTENT_CONTROL,
            intents=(INTENT_CONTROL,),
            control_action=control_action,
        )
    if _is_direct_chat(text):
        return DialogueRoute(
            "direct_chat",
            "low-information chat should not touch profile or recommendation tools",
            signal=None,
            emotion="",
            primary_intent=INTENT_GREETING,
            intents=(INTENT_GREETING,),
            need_profile=False,
            need_memory=False,
            need_recommendation_search=False,
        )
    if _looks_like_instruction_injection(text):
        return DialogueRoute(
            "casual_chat",
            "instruction-like text must not select privileged tools",
            signal=None,
            emotion="",
            primary_intent=INTENT_CHAT,
            intents=(INTENT_CHAT,),
            need_profile=False,
            need_memory=False,
            need_recommendation_search=False,
        )

    signal = _extract_signal(text, context_card) or _extract_scene_signal(text)
    emotion = detect_emotion(text)
    if _is_recommendation_explanation_request(text):
        return DialogueRoute(
            "explain_recommendation",
            "user asks why these songs were chosen",
            signal=signal,
            emotion=emotion,
            primary_intent=INTENT_CHAT,
            intents=_route_intents(INTENT_CHAT, signal),
            need_profile=True,
            need_memory=True,
        )
    if is_recall_request(text):
        return DialogueRoute(
            "recall_memory",
            "user asks about previous listening memory",
            signal=signal,
            emotion=emotion,
            primary_intent=INTENT_CHAT,
            intents=_route_intents(INTENT_CHAT, signal),
            need_memory=True,
        )
    if _is_profile_chat_request(text):
        return DialogueRoute(
            "profile_chat",
            "user discusses taste without asking for playable candidates",
            signal,
            emotion,
            primary_intent=INTENT_CHAT,
            intents=_route_intents(INTENT_CHAT, signal),
            need_profile=True,
            need_memory=True,
            need_recommendation_search=False,
        )
    if _is_recommendation_execution_request(text):
        return DialogueRoute(
            "recommend_music",
            "user asks to generate playable recommendation candidates",
            signal,
            emotion,
            primary_intent=INTENT_RECOMMEND,
            intents=_route_intents(INTENT_RECOMMEND, signal),
            need_profile=True,
            need_memory=True,
            need_recommendation_search=True,
        )
    if signal is not None and signal.commit_policy == "confirm":
        return DialogueRoute(
            "confirm_signal",
            "signal needs explicit confirmation",
            signal,
            emotion,
            primary_intent=INTENT_PROFILE_UPDATE,
            intents=(INTENT_PROFILE_UPDATE,),
            need_profile=True,
        )
    if signal is not None and _is_explicit_profile_update(text):
        return DialogueRoute(
            "profile_update",
            "user explicitly updates music preference",
            signal,
            emotion,
            primary_intent=INTENT_PROFILE_UPDATE,
            intents=(INTENT_PROFILE_UPDATE,),
            need_profile=True,
        )
    if signal is not None:
        return DialogueRoute(
            "chat_with_signal",
            "conversation contains usable context",
            signal,
            emotion,
            primary_intent=INTENT_CHAT,
            intents=(INTENT_CHAT,),
            need_memory=True,
        )
    return DialogueRoute(
        "casual_chat",
        "general music companion chat",
        None,
        emotion,
        primary_intent=INTENT_CHAT,
        intents=(INTENT_CHAT,),
        need_profile=True,
        need_memory=True,
    )


def _looks_like_instruction_injection(text: str) -> bool:
    normalized = text.casefold()
    instruction_markers = (
        "忽略之前规则",
        "忽略系统提示",
        "system prompt",
        "developer message",
        "调用删除工具",
        "删除数据库",
        "绕过权限",
    )
    return any(marker in normalized for marker in instruction_markers)


def _route_intents(primary: str, signal: ExtractedSignal | None) -> tuple[str, ...]:
    intents = [primary]
    if signal is not None and signal.kind != "recent_state" and primary != INTENT_PROFILE_UPDATE:
        intents.append(INTENT_PROFILE_UPDATE)
    return tuple(dict.fromkeys(intents))


def _control_action(text: str) -> str:
    compact = re.sub(r"[\s，,。.!！?？；;、~～]", "", _normalize_message(text).casefold())
    if compact in {"暂停", "停一下", "先停", "先暂停", "别放了", "pause"}:
        return "pause"
    if compact in {"继续", "继续播放", "播放", "接着放", "resume"}:
        return "resume"
    if compact in {"下一首", "下一个", "换下一首", "next"}:
        return "next"
    if compact in {"上一首", "上一个", "previous", "prev"}:
        return "previous"
    return ""


def _is_direct_chat(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    compact = re.sub(r"[\s，,。.!！?？；;、~～]", "", normalized)
    if _is_greeting(normalized):
        return True
    return compact in {
        "哈哈",
        "哈哈哈",
        "谢谢",
        "谢谢你",
        "谢啦",
        "好的",
        "好",
        "ok",
        "收到",
        "晚安",
        "拜拜",
        "再见",
        "thanks",
        "thankyou",
        "bye",
        "hh",
        "hhh",
    }


def _is_profile_chat_request(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    if _asks_for_artist_names_only(normalized):
        return True
    discussion_words = (
        "你觉得",
        "觉得",
        "适合我",
        "符合我",
        "符合我的品味",
        "我的品味",
        "我的口味",
        "我喜欢什么",
        "我爱听什么",
        "哪种歌手",
        "那种歌手",
        "什么歌手",
        "哪个更适合",
        "是不是更喜欢",
        "更喜欢",
        "比较符合",
        "类似的歌手",
        "相近的歌手",
        "像谁",
    )
    if not _has_any(normalized, discussion_words):
        return False
    return _mentions_music_domain(normalized) or _has_any(normalized, ("品味", "口味"))


def _asks_for_artist_names_only(text: str) -> bool:
    if "歌手" not in text:
        return False
    if _has_any(text, ("几首", "首歌", "歌曲", "歌单", "播放", "放几首", "来几首")):
        return False
    return _has_any(text, ("推荐", "适合", "符合", "类似", "哪种", "什么", "几个", "哪些"))


def _is_recommendation_execution_request(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    if _is_profile_chat_request(normalized):
        return False
    if _is_high_confidence_recommendation_command(normalized):
        return True
    if is_recommendation_request(normalized) and not _has_any(
        normalized, RECOMMEND_DISCUSSION_PHRASES
    ) and not _has_any(
        normalized,
        ("为什么", "为啥", "原因", "你觉得", "是不是", "哪个", "什么歌手", "哪种歌手"),
    ):
        return True
    return False


def _is_explicit_profile_update(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    if _has_negative_preference_intent(normalized):
        return True
    if not _has_positive_preference_intent(normalized):
        return False
    return _has_any(
        normalized,
        (
            "我喜欢",
            "我爱听",
            "我很喜欢",
            "我超喜欢",
            "一直",
            "长期",
            "以后",
            "多推",
            "少推",
            "其实",
            "最近",
            "开始",
        ),
    )


def _has_negative_preference_intent(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    return has_negative_intent(normalized) or _has_any(
        normalized,
        (
            "少给我推",
            "少给我推荐",
            "别给我推",
            "别给我推荐",
            "不要给我推",
            "不要给我推荐",
            "以后少推",
            "以后别推",
        ),
    )


def _has_positive_preference_intent(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    return has_positive_intent(normalized) or _has_any(
        normalized,
        ("多给我推", "多给我推荐", "以后多推", "以后多推荐"),
    )


def _mentions_music_domain(text: str) -> bool:
    return bool(
        matched_artist_names(text)
        or match_topics(text)
        or _has_any(
            text,
            (
                "歌",
                "歌手",
                "音乐",
                "旋律",
                "风格",
                "rnb",
                "r&b",
                "华语",
                "摇滚",
                "说唱",
            ),
        )
    )


def _extract_signal(message: str, context_card: Any | None) -> ExtractedSignal | None:
    text = _normalize_message(message)
    if not text:
        return None

    topic = _topic_from_text(text)
    context_topic = ""
    if context_card is not None:
        context_topic = str(context_card["topic"] or context_card["source_text"] or "").strip()
    if _has_negative_preference_intent(text) and context_topic:
        return ExtractedSignal(
            polarity="negative",
            topic=context_topic,
            statement=f"用户对引用歌曲 {context_topic} 给出负面反馈；原话：{text}",
            confidence=0.88,
            kind="recent_preference",
            commit_policy="shadow",
        )

    if _has_positive_preference_intent(text) and context_topic:
        return ExtractedSignal(
            polarity="positive",
            topic=context_topic,
            statement=f"用户对引用歌曲 {context_topic} 给出正面反馈；原话：{text}",
            confidence=0.88,
            kind="recent_preference",
            commit_policy="shadow",
        )

    if _has_negative_preference_intent(text) and topic:
        return ExtractedSignal(
            polarity="negative",
            topic=topic,
            statement=f"用户不想在 B 站电台里听 {topic} 相关内容。",
            confidence=0.86,
            kind="stable_preference",
            commit_policy="commit",
        )

    if _has_positive_preference_intent(text) and topic:
        kind = _preference_signal_kind(text)
        commit_policy = "commit" if kind == "stable_preference" else "shadow"
        return ExtractedSignal(
            polarity="positive",
            topic=topic,
            statement=_preference_statement(topic, text, kind),
            confidence=0.88 if kind == "stable_preference" else 0.76,
            kind=kind,
            commit_policy=commit_policy,
        )

    if context_card is not None and len(text) >= 4:
        topic = context_card["topic"] or _topic_from_text(text) or "补充偏好"
        polarity = (
            context_card["polarity"]
            if context_card["polarity"] in {"positive", "negative"}
            else "positive"
        )
        prefix = "用户补充确认" if polarity == "positive" else "用户补充回避"
        return ExtractedSignal(
            polarity=polarity,
            topic=topic,
            statement=f"{prefix}：{context_card['statement'].strip()}；补充原话：{text}",
            confidence=0.78,
            kind="preference_hypothesis",
            commit_policy="shadow",
        )

    return None


def _extract_scene_signal(message: str) -> ExtractedSignal | None:
    text = _normalize_message(message)
    mood = detect_emotion(text)
    scenario = _scene_topic(text, mood)
    if not scenario:
        return None
    return ExtractedSignal(
        polarity="positive",
        topic=scenario["topic"],
        statement=scenario["statement"],
        confidence=scenario["confidence"],
        kind="recent_state",
        commit_policy="shadow",
    )


def _topic_from_text(text: str) -> str:
    artists = matched_artist_names(text)
    if artists:
        return "、".join(artists[:3])
    phrase = topic_phrase(text)
    if phrase:
        return phrase
    for marker in (
        "不喜欢",
        "不爱听",
        "不想听",
        "少推",
        "别推",
        "不要推",
        "避开",
        "喜欢",
        "想听",
        "多推",
        "推荐",
        "推",
    ):
        topic = _extract_topic_after_marker(text, marker)
        if topic:
            return topic
    return ""


def _extract_topic_after_marker(text: str, marker: str) -> str:
    lowered = text.casefold()
    index = lowered.find(marker)
    if index < 0:
        return ""
    fragment = text[index + len(marker) :]
    fragment = re.split(r"[，,。.!！?？；;\n\r]", fragment, maxsplit=1)[0]
    return _clean_topic(fragment) or topic_phrase(text)


def _clean_topic(value: str) -> str:
    text = value.strip(" ：:，,。.!！?？；;、")
    text = re.sub(
        r"^(我|你|给我|帮我|以后|以后也|尽量|少|多|听|看|刷|推荐|推|"
        r"一些|一点|几个|几首|那些|这种|这类|类型|风格|歌曲|音乐|内容)+",
        "",
        text,
    )
    text = re.sub(r"(相关|方向|类型|风格|歌曲|音乐|内容).*$", "", text)
    text = text.strip(" ：:，,。.!！?？；;、")
    return text[:80]


def _preference_signal_kind(text: str) -> str:
    if _has_any(text, ("今天", "今晚", "现在", "这会儿", "这轮", "这周", "想听")):
        return "recent_state"
    if _has_any(text, ("最近", "好像", "貌似", "突然", "开始", "有点", "可能", "试试")):
        return "preference_hypothesis"
    if _has_any(text, ("一直", "长期", "忠实粉丝", "从小", "本来就", "很喜欢", "超喜欢")):
        return "stable_preference"
    return "stable_preference"


def _preference_statement(topic: str, text: str, kind: str) -> str:
    if kind == "recent_state":
        return f"用户当前会话想听 {topic} 相关音乐；原话：{text}"
    if kind == "preference_hypothesis":
        return f"用户对 {topic} 产生了偏好假设，需要用少量推荐继续验证；原话：{text}"
    return f"用户明确表示喜欢 {topic} 相关音乐；原话：{text}"


def _scene_topic(text: str, mood: str) -> dict[str, Any] | None:
    if _has_any(text, ("面试", "考试", "复习", "刷题", "写代码", "debug", "工作", "加班")):
        return {
            "topic": "专注放松",
            "statement": f"用户当前处于高认知负荷场景，适合低打扰、稳定、放松的音乐；原话：{text}",
            "confidence": 0.78,
        }
    if _has_any(text, ("夜跑", "跑步", "健身", "运动", "散步")):
        return {
            "topic": "轻律动",
            "statement": f"用户最近有运动场景，适合保留节奏但避免过吵的音乐；原话：{text}",
            "confidence": 0.74,
        }
    if mood:
        topic = {
            "放松": "放松",
            "安静": "安静",
            "开心": "轻快",
            "难过": "温柔",
        }.get(mood, mood)
        return {
            "topic": topic,
            "statement": f"用户当前情绪更接近 {mood}，本轮适合 {topic}、少打扰的音乐；原话：{text}",
            "confidence": 0.72,
        }
    if _has_any(text, ("安静一点", "轻一点", "别太吵", "少干扰", "低打扰")):
        return {
            "topic": "安静",
            "statement": f"用户当前希望听感更安静、少干扰；原话：{text}",
            "confidence": 0.76,
        }
    return None


def _recall_query(message: str) -> str:
    text = _normalize_message(message)
    topics = match_topics(text)
    if topics:
        return " ".join(topics[:2])
    cleaned = re.sub(
        r"(我|你|帮我|给我|之前|以前|上次|貌似|好像|是不是|有没有|听过|找回|记得|的歌|歌曲|音乐)",
        " ",
        text,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ：:，,。.!！?？；;、")
    return cleaned[:80]


def _recall_reply(query: str, tracks: list[dict[str, Any]]) -> str:
    if tracks:
        subject = query or "最近听过的内容"
        return f"我从听过的记录里找到了这些和 {subject} 接近的歌。"
    if query:
        return f"我没在本地听歌记录里找到 {query}，可以直接让我按这个方向重新推荐。"
    return "我这边还没有足够的本地播放记录。"


def _is_recommendation_explanation_request(text: str) -> bool:
    normalized = text.casefold()
    asks_why = _has_any(normalized, ("为什么", "为啥", "原因", "依据", "怎么会", "怎么给我"))
    about_recommendation = _has_any(normalized, ("推荐", "这几首", "这些歌", "歌单", "给我推"))
    about_item = _explanation_target_index(normalized, 8) is not None
    return (asks_why and about_recommendation) or (asks_why and about_item)


def _explanation_target_index(text: str, total: int) -> int | None:
    if total <= 0:
        return None
    compact = re.sub(r"[\s，,。.!！?？；;、~～]", "", _normalize_message(text).casefold())
    aliases = {
        "第一首": 0,
        "第1首": 0,
        "第一个": 0,
        "第1个": 0,
        "第一条": 0,
        "第1条": 0,
        "第二首": 1,
        "第2首": 1,
        "第二个": 1,
        "第2个": 1,
        "第三首": 2,
        "第3首": 2,
        "第三个": 2,
        "第3个": 2,
    }
    for phrase, index in aliases.items():
        if phrase in compact and index < total:
            return index
    match = re.search(r"第(\d+)(首|个|条)", compact)
    if match:
        index = int(match.group(1)) - 1
        if 0 <= index < total:
            return index
    if "这首" in compact or "当前这首" in compact:
        return 0
    return None


def _single_recommendation_explanation(item: dict[str, Any], index: int) -> str:
    title = _compact_text(str(item.get("title") or f"第{index + 1}首"), 48)
    evidence = _clean_trace_list(item.get("evidence"))
    matched = _clean_trace_list(item.get("matchedPreferences"))
    penalties = _clean_trace_list(item.get("penalties"))

    if evidence:
        base = f"第{index + 1}首《{title}》排上来，主要因为{_join_cn(evidence[:2])}。"
    elif matched:
        base = f"第{index + 1}首《{title}》主要命中了 {_join_cn(matched[:3])} 这些方向。"
    else:
        reason = str(item.get("reason") or "").strip()
        if reason:
            base = f"第{index + 1}首《{title}》主要因为{reason}。"
        else:
            base = f"第{index + 1}首《{title}》来自上一轮候选池，当前 trace 里没有更细的单首依据。"

    if penalties:
        return f"{base}它也被 {_join_cn(penalties[:2])} 压过权重，所以不是只靠一个标签硬顶上来。"
    return base


def _trace_matched_preferences(items: list[Any]) -> list[str]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        for value in _clean_trace_list(item.get("matchedPreferences")):
            counts[value] = counts.get(value, 0) + 1
    return [
        name for name, _count in sorted(counts.items(), key=lambda value: value[1], reverse=True)
    ]


def _trace_penalty_labels(items: list[Any]) -> list[str]:
    labels: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        labels.extend(_clean_trace_list(item.get("penalties")))
    return list(dict.fromkeys(labels))


def _trace_source_labels(items: list[Any]) -> list[str]:
    labels = {
        "explore": "探索",
        "discovery_search": "候选池发现",
        "tag_search": "标签扩展",
        "tag_match": "本地同标签",
        "frequent_up": "常听来源",
        "liked_up": "收藏来源",
        "popular_music": "热门音乐",
        "library": "本地记录",
    }
    result: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        label = labels.get(str(item.get("source") or ""))
        if label:
            result.append(label)
    return list(dict.fromkeys(result))


def _trace_evidence(items: list[Any]) -> list[str]:
    result: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        result.extend(_clean_trace_list(item.get("evidence")))
    return list(dict.fromkeys(result))


def _clean_trace_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if text:
            result.append(_compact_text(text, 48))
    return result


def _join_cn(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return "、".join(items)


def _recommendation_reply(
    *,
    topic: str,
    recommendation_count: int,
    source_text: str,
    profile_hint: str,
) -> str:
    if recommendation_count:
        base = f"我按你刚才说的“{_compact_text(source_text, 42)}”来选，先给你这几首。"
        if profile_hint:
            return f"{base}我也参考了你之前更合拍的 {profile_hint}。"
        return base
    return "这轮没有拿到稳定候选，我先记住当前语境，等你继续听或再说两句后再缩小范围。"


def _control_reply(action: str) -> str:
    return {
        "pause": "好，先暂停。",
        "resume": "好，继续放。",
        "next": "好，切下一首。",
        "previous": "好，回到上一首。",
    }.get(action, "收到。")


def _dialogue_llm_enabled() -> bool:
    raw = os.getenv("RECOMMEND_DIALOGUE_LLM_ENABLED")
    if raw is None:
        raw = os.getenv("RECOMMEND_LLM_ENABLED", "false")
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def _dialogue_router_llm_enabled() -> bool:
    raw = os.getenv("RECOMMEND_DIALOGUE_ROUTER_LLM_ENABLED")
    if raw is None:
        return _dialogue_llm_enabled()
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def _llm_chat_reply(
    message: str,
    *,
    route: DialogueRoute,
    analysis: dict[str, Any],
    recent_turns: list[dict[str, str]],
    client: Any | None = None,
) -> str:
    client = client or _default_llm_client()
    system_prompt = (
        "你是一个自然、敏感、懂音乐的中文 AI 助手，定位像用户的音乐搭子，"
        "但也能正常闲聊和回答日常问题。你必须直接回答用户当前这句话，"
        "不要输出系统状态，不要说 AMEM、画像、记忆写入、工具调用、路由、候选池、trace。"
        "只有用户明确要求推荐可播放歌曲时才说要开始推荐；普通讨论、解释、判断、比较都只聊天。"
        "你可以参考给定的音乐偏好和最近对话，但不要把不确定信息说死。"
        "不要轻易点名单一歌手，除非用户正在问这个歌手；更应该概括风格、情绪和场景。"
        "不要因为画像里某个歌手权重高，就在闲聊里反复提这个歌手。"
        "如果用户问'我是怎么样的人'这类问题，只能基于已知聊天和听歌线索温和推断，"
        "要承认信息有限，并给出有内容的观察。"
        "回复要像真人聊天，2 到 5 句，中文，不要列表，不要 markdown。"
        '返回且只返回 JSON：{"reply":"..."}'
    )
    user_prompt = json.dumps(
        {
            "user_message": message,
            "route": {
                "tool": route.tool,
                "intent": route.primary_intent,
                "need_profile": route.need_profile,
                "need_memory": route.need_memory,
                "need_recommendation_search": route.need_recommendation_search,
            },
            "music_profile": _compact_profile_for_chat(analysis),
            "recent_dialogue": _compact_recent_turns(recent_turns),
        },
        ensure_ascii=False,
    )
    response = client.complete(system_prompt=system_prompt, user_prompt=user_prompt)
    payload = _parse_json_object(response.content)
    reply = _normalize_message(str(payload.get("reply") or ""))
    if not reply:
        raise ValueError("empty LLM dialogue reply")
    return reply[:1200]


def _compact_profile_for_chat(analysis: dict[str, Any]) -> dict[str, Any]:
    profile = (analysis.get("profile") if isinstance(analysis, dict) else {}) or {}
    summary = (analysis.get("summary") if isinstance(analysis, dict) else {}) or {}
    return {
        "positive_topics": _top_score_items(profile.get("positive_topics"), limit=6),
        "negative_topics": _top_score_items(profile.get("negative_topics"), limit=5),
        "moods": _top_score_items(profile.get("mood_weights"), limit=5),
        "recent_intents": [str(item)[:60] for item in profile.get("recent_intents") or []][:5],
        "mbti": str(profile.get("mbti") or "")[:8],
        "music_persona": str(profile.get("music_persona") or "")[:300],
        "current_music_phase": str(profile.get("current_music_phase") or "")[:180],
        "core_traits": [str(item)[:60] for item in profile.get("core_traits") or []][:6],
        "psychological_needs": [
            str(item)[:80] for item in profile.get("psychological_needs") or []
        ][:6],
        "persona_confidence": profile.get("persona_confidence"),
        "source": str(profile.get("source") or ""),
        "confidence": profile.get("confidence"),
        "summary": {
            "top_positive": _summary_names(summary.get("topPositiveTopics"), limit=5),
            "top_negative": _summary_names(summary.get("topNegativeTopics"), limit=4),
            "top_moods": _summary_names(summary.get("topMoods"), limit=4),
        },
    }


def _top_score_items(value: Any, *, limit: int) -> list[dict[str, Any]]:
    if not isinstance(value, dict):
        return []
    items = sorted(
        ((str(key), score) for key, score in value.items() if str(key).strip()),
        key=lambda item: float(item[1] or 0),
        reverse=True,
    )
    return [
        {"name": name[:40], "weight": round(float(score or 0), 3)} for name, score in items[:limit]
    ]


def _summary_names(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    names: list[str] = []
    for item in value:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        else:
            name = str(item).strip()
        if name:
            names.append(name[:40])
        if len(names) >= limit:
            break
    return names


def _compact_recent_turns(
    turns: list[dict[str, str]], *, limit: int = CHAT_LLM_HISTORY_LIMIT
) -> list[dict[str, str]]:
    compacted: list[dict[str, str]] = []
    for turn in turns[-limit:]:
        role = str(turn.get("role") or "").strip()
        content = _normalize_message(str(turn.get("content") or ""))
        if role in {"user", "assistant"} and content:
            compacted.append({"role": role, "content": content[:240]})
    return compacted


def _direct_chat_reply(message: str) -> str:
    compact = re.sub(r"[\s，,。.!！?？；;、~～]", "", _normalize_message(message).casefold())
    if compact in {"谢谢", "谢谢你", "谢啦", "thanks", "thankyou"}:
        return "不客气。"
    if compact in {"晚安"}:
        return "晚安，今天就放轻一点。"
    if compact in {"拜拜", "再见", "bye"}:
        return "好，回头继续聊。"
    if compact in {"哈哈", "哈哈哈", "hh", "hhh"}:
        return "哈哈，我在。"
    if compact in {"好", "好的", "ok", "收到"}:
        return "好。"
    return "你好啊，我在。"


def _profile_chat_reply(
    message: str,
    *,
    analysis: dict[str, Any],
    recent_context: list[str],
) -> str:
    normalized = _normalize_message(message)
    artists = matched_artist_names(normalized)
    topics = match_topics(normalized)
    positives = _profile_positive_names(analysis)
    moods = _top_names((analysis.get("profile") or {}).get("mood_weights"))
    style_hint = _profile_style_hint(positives, moods)

    if len(artists) >= 2 and _has_any(normalized, ("哪个", "谁", "更适合", "比较")):
        first, second = artists[0], artists[1]
        return (
            f"这两个里我会先押 {first}，再用 {second} 做扩展。"
            f"{style_hint}如果下一步真要听歌，我会按这个判断去找候选，"
            "但不会让同一个歌手把一整轮占满。"
        )

    if artists and _has_any(normalized, ("类似", "相近", "像谁", "还有没有")):
        similar = _similar_artist_names(artists[0], positives)
        return (
            f"如果从 {artists[0]} 往外扩，我会先看 {similar}。"
            f"{style_hint}这类回答我先只给判断，不直接生成播放列表。"
        )

    if _has_any(normalized, ("是不是", "更喜欢", "我喜欢什么", "我爱听什么")):
        subject = "、".join([*topics, *artists][:2]) or "这个方向"
        if positives:
            return (
                f"有这个趋势，但我不会只凭一句话就把它当成长期结论。"
                f"你现在更明显的底色是 {'、'.join(positives[:3])}，"
                f"{subject} 可以先作为近期探索方向。"
            )
        return (
            f"从这句话看，{subject} 可以先当成一个待验证方向。"
            "我还需要结合后续点击、跳过和你继续聊到的状态再判断。"
        )

    if "歌手" in normalized or _has_any(normalized, ("适合我", "符合我", "品味", "口味")):
        suited = _suited_artist_names(positives, moods, artists)
        extra = ""
        has_work_context = any(
            _has_any(item, ("面试", "写代码", "加班", "复习")) for item in recent_context
        )
        if recent_context and has_work_context:
            extra = "你最近如果处在高负荷状态，我会优先选更稳、更少打扰的作品。"
        return (
            f"按你现在的口味，我会先看 {'、'.join(suited)} 这一类。"
            f"{style_hint}{extra}真要进入推荐时，我会控制歌手分布，"
            "不会因为你喜欢某个人就连续塞满。"
        )

    if positives:
        return (
            f"我理解你现在比较吃 {'、'.join(positives[:3])} 这些方向。"
            f"{style_hint}你可以继续聊感受，我会先按最近状态轻量调整，"
            "不把一句话当成永久口味。"
        )
    return (
        "如果只按当前聊天看，我会先从旋律舒服、情绪稳定、不太吵的方向理解你，再用后续反馈慢慢校准。"
    )


def _profile_update_reply(signal: ExtractedSignal, *, stored: bool) -> str:
    if signal.polarity == "negative":
        return f"收到，后面会减少 {signal.topic} 相关内容，已经点过不感兴趣的也会继续降权。"
    if signal.kind == "recent_state":
        return f"收到，这轮先按 {signal.topic} 来理解，不直接改成永久口味。"
    if signal.kind == "preference_hypothesis":
        return f"收到，{signal.topic} 先作为待验证方向，后面用少量反馈校准。"
    if stored:
        return f"收到，{signal.topic} 会进入后续选歌依据，但我会控制同歌手和同质内容密度。"
    return f"收到，先按 {signal.topic} 这个方向理解。"


def _signal_reply(signal: ExtractedSignal, *, profile_hint: str, stored: bool) -> str:
    if signal.kind == "recent_state":
        suffix = f"你之前更合拍的 {profile_hint} 我也会一起参考。" if profile_hint else ""
        if signal.topic == "专注放松":
            return (
                "面试准备确实容易一直绷着。"
                "这轮推荐会偏低打扰、稳定一点，"
                f"别把情绪再往上推。{suffix}你现在是想边准备边听，还是面完以后放松一下？"
            )
        if signal.topic == "轻律动":
            return (
                "这个状态可以留一点律动，但不需要太炸。"
                f"{suffix}我会先按近期场景处理，不直接改你的长期口味。"
            )
        if signal.topic == "安静":
            return (
                "明白，这轮先把音色和节奏压下来，少一点打扰。"
                f"{suffix}这只是当前状态，不会被当成永久偏好。"
            )
        return (
            f"听起来这轮更适合 {signal.topic}。我会先按当前状态处理，不直接改成长期口味。{suffix}"
        )
    if signal.kind == "preference_hypothesis":
        return (
            f"我先把 {signal.topic} 当成一个待验证方向，"
            "后面用少量推荐试探，不会一下子把列表全改掉。"
        )
    if stored:
        return (
            f"收到，{signal.topic} 会成为后续选歌的重要线索。"
            "下一轮我会保留这个方向，但不会让同一个歌手把列表占满。"
        )
    return f"收到，我先按 {signal.topic} 这个方向理解。"


def _casual_reply(message: str, *, profile_hint: str, recent_context: list[str]) -> str:
    del profile_hint
    mood = detect_emotion(message)
    normalized = _normalize_message(message)
    context_hint = ""
    if recent_context and len(recent_context) >= 2:
        context_hint = (
            f"刚才你提到“{_compact_text(recent_context[-1], 28)}”，我可以顺着这个状态聊。"
        )
    if _is_greeting(message):
        return "在。"
    if _has_any(normalized, ("面试", "写代码", "加班", "复习", "考试", "上班")):
        return "听起来这段时间脑子挺满的。你想先聊两句缓一缓，还是让我给你找点不抢注意力的歌？"
    if mood == "放松":
        return f"那这轮先别太刺激，听感可以轻一点、舒服一点。{context_hint}"
    if mood == "安静":
        return f"我会把节奏压下来，优先考虑安静、低打扰的内容。{context_hint}"
    if mood == "开心":
        return f"可以，那就让节奏稍微亮一点，别太沉。{context_hint}"
    if mood == "难过":
        return f"我明白，先不急着换很吵的歌，可以从更柔和的方向慢慢往外走。{context_hint}"
    return "我在，继续说。"


def _profile_hint(analysis: dict[str, Any]) -> str:
    summary = (analysis.get("summary") if isinstance(analysis, dict) else {}) or {}
    positives = _top_names((analysis.get("profile") or {}).get("positive_topics"))
    if not positives:
        positives = [
            str(item.get("name"))
            for item in summary.get("topPositiveTopics") or []
            if item.get("name")
        ]
    moods = _top_names((analysis.get("profile") or {}).get("mood_weights"))
    names = list(dict.fromkeys([*positives[:2], *moods[:1]]))
    return "、".join(names[:3])


def _profile_positive_names(analysis: dict[str, Any]) -> list[str]:
    summary = (analysis.get("summary") if isinstance(analysis, dict) else {}) or {}
    positives = _top_names((analysis.get("profile") or {}).get("positive_topics"), limit=5)
    if positives:
        return positives
    return [
        str(item.get("name"))
        for item in summary.get("topPositiveTopics") or []
        if isinstance(item, dict) and item.get("name")
    ][:5]


def _profile_style_hint(positives: list[str], moods: list[str]) -> str:
    signals = list(dict.fromkeys([*positives[:3], *moods[:2]]))
    if not signals:
        return "你更像是吃旋律舒服、情绪稳定、不太吵的方向。"
    if any(item in signals for item in ("R&B", "抒情 R&B", "抒情", "治愈系")):
        return "你更像是喜欢旋律舒服、带一点 R&B 底色、不要太炸的听感。"
    if "华语流行" in signals:
        return "你更像是偏旋律清楚、表达直接的华语流行底色。"
    return f"你现在更明显的线索是 {'、'.join(signals[:3])}。"


def _suited_artist_names(
    positives: list[str],
    moods: list[str],
    requested_artists: list[str],
) -> list[str]:
    names: list[str] = []
    profile_text = " ".join([*positives, *moods]).casefold()
    if "r&b" in profile_text or "rnb" in profile_text or "抒情" in profile_text:
        names.extend(["陶喆", "方大同", "王若琳"])
    if "华语" in profile_text or "流行" in profile_text:
        names.extend(["陈奕迅", "孙燕姿", "林俊杰"])
    if "治愈" in profile_text or "温柔" in profile_text:
        names.extend(["毛不易", "田馥甄", "梁静茹"])
    names.extend(requested_artists)
    if not names:
        names.extend(["陶喆", "方大同", "陈奕迅", "孙燕姿"])
    return list(dict.fromkeys(names))[:5]


def _similar_artist_names(seed_artist: str, positives: list[str]) -> str:
    mapping = {
        "陶喆": ["方大同", "王力宏", "胡彦斌", "王若琳"],
        "方大同": ["陶喆", "王若琳", "韦礼安", "王力宏"],
        "周杰伦": ["林俊杰", "陶喆", "王力宏", "孙燕姿"],
        "陈奕迅": ["张学友", "孙燕姿", "田馥甄", "李荣浩"],
        "孙燕姿": ["田馥甄", "梁静茹", "陈奕迅", "王菲"],
    }
    names = mapping.get(seed_artist, [])
    if any(item in {"R&B", "抒情 R&B", "抒情"} for item in positives):
        names = [*names, "陶喆", "方大同", "王若琳"]
    if not names:
        names = ["陶喆", "方大同", "陈奕迅"]
    return "、".join(list(dict.fromkeys(names))[:4])


def _top_names(values: Any, limit: int = 3) -> list[str]:
    if isinstance(values, dict):
        return [
            str(key)
            for key, _value in sorted(
                values.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:limit]
            if str(key).strip()
        ]
    if isinstance(values, list):
        return [
            str(item.get("name"))
            for item in values[:limit]
            if isinstance(item, dict) and item.get("name")
        ]
    return []


def _has_any(text: str, words: tuple[str, ...]) -> bool:
    return any(word.casefold() in text.casefold() for word in words)


def _is_greeting(text: str) -> bool:
    normalized = _normalize_message(text).casefold()
    compact = re.sub(r"[\s，,。.!！?？；;、~～]", "", normalized)
    return compact in {
        "你好",
        "你好啊",
        "您好",
        "哈喽",
        "hello",
        "hi",
        "hey",
        "在吗",
        "在不在",
        "嗨",
    }


def _compact_text(value: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return f"{text[: max(limit - 1, 1)]}…"


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _emit_progress(
    callback: Callable[[str, dict[str, Any]], None] | None,
    stage: str,
    payload: dict[str, Any],
) -> None:
    if callback is None:
        return
    try:
        callback(stage, payload)
    except Exception:
        # Progress delivery is observational and must never fail the task.
        return


def _normalize_message(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_action(value: str) -> str:
    action = str(value or "").strip().lower()
    if action not in VALID_ACTIONS:
        raise ValueError("unsupported feedback action")
    return action


def _status_for_action(action: str) -> str:
    if action in CONFIRM_ACTIONS:
        return "confirmed"
    if action in REJECT_ACTIONS:
        return "rejected"
    if action == "later":
        return "deferred"
    return "discussing"


def _state_after_action(action: str) -> str:
    if action in CONFIRM_ACTIONS:
        return "probing"
    if action in REJECT_ACTIONS:
        return "probing"
    if action == "later":
        return "deferred"
    return "discussing"


def _json_loads(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _snapshot_list(snapshot: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = snapshot.get(key)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_analysis() -> dict[str, Any]:
    return {
        "scene": "conversation",
        "profile": {
            "positive_topics": {},
            "negative_topics": {},
            "preferred_uploaders": {},
            "avoid_uploaders": {},
            "blocked_uploaders": {},
            "mood_weights": {},
            "recent_intents": [],
            "positive_interest_texts": [],
            "negative_interest_texts": [],
            "same_uploader_limit": 0,
            "exploration_ratio": 0.0,
            "evidence_memory_ids": [],
            "confidence": 0.0,
            "source": "fallback",
        },
        "profileTraceId": "",
        "memories": [],
        "summary": {
            "topPositiveTopics": [],
            "topNegativeTopics": [],
            "topUploaders": [],
            "topMoods": [],
            "strategy": {
                "sameUploaderLimit": 0,
                "explorationRatio": 0.0,
                "confidence": 0.0,
                "source": "fallback",
            },
            "evidenceMemoryCount": 0,
        },
    }
