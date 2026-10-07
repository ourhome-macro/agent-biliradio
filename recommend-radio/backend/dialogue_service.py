from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from amem_bridge import record_music_behavior
from conversation_memory import ConversationMemoryService
from database import DEFAULT_DB_PATH, LEGACY_OWNER_USER_ID, get_connection, init_db
from dialogue_repository import DialogueRepository
from dialogue_rules import (
    CONFIRM_ACTIONS,
    GENERIC_PROBES,
    MAX_MESSAGE_LENGTH,
    RECALL_RESULT_LIMIT,
    RECOMMENDATION_CARD_LIMIT,
    REJECT_ACTIONS,
    SESSION_TURN_LIMIT,
    DialogueRoute,
    ExtractedSignal,
    _canonical_route,
    _card_context,
    _casual_reply,
    _context_card_with_track,
    _control_reply,
    _dialogue_llm_enabled,
    _dialogue_router_llm_enabled,
    _direct_chat_reply,
    _emit_progress,
    _empty_analysis,
    _escape_like,
    _explanation_target_index,
    _has_negative_preference_intent,
    _has_positive_preference_intent,
    _is_high_confidence_route,
    _json_loads,
    _llm_chat_reply,
    _llm_route_message,
    _merge_recommendations,
    _normalize_action,
    _normalize_message,
    _profile_chat_reply,
    _profile_hint,
    _profile_update_reply,
    _recall_query,
    _recall_reply,
    _recommendation_reply,
    _route_message,
    _route_payload,
    _serialize_card,
    _serialize_session_summary,
    _serialize_turn,
    _signal_reply,
    _single_recommendation_explanation,
    _state_after_action,
    _status_for_action,
    _topic_from_text,
    _trace_evidence,
    _trace_matched_preferences,
    _trace_penalty_labels,
    _trace_source_labels,
    _utc_now,
)
from error_code import APIError
from full_trace import FullTrace, hash_text
from music_agent import music_operation
from profile_projector import _default_llm_client
from recommendation_service import RecommendationService
from request_spec import RequestInterpreter, RequestSpec


def _finish_session_analysis(function):
    from functools import wraps

    @wraps(function)
    def execute(self, *args, **kwargs):
        result = function(self, *args, **kwargs)
        if isinstance(result, dict) and result.pop("_deferred_analysis", False):
            result["analysis"] = self._safe_analysis()
        return result

    return execute


class MusicDialogueService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        user_id: str = LEGACY_OWNER_USER_ID,
        recommendation_service: RecommendationService | None = None,
        router_llm_client: Any | None = None,
    ) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        self.user_id = user_id
        self.repository = DialogueRepository(self.db_path, self.user_id)
        init_db(self.db_path)
        self.recommendation_service = recommendation_service or RecommendationService(
            db_path=self.db_path,
            user_id=self.user_id,
        )
        self.router_llm_client = router_llm_client
        self.conversation_memory = ConversationMemoryService(
            str(self.db_path), user_id=self.user_id
        )

    @_finish_session_analysis
    def get_session(self, session_id: str | None = None) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            session = self.repository._get_or_create_session(conn, session_id=session_id)
            return self._serialize_session(conn, session)

    def resolve_session_id(self, session_id: str | None = None) -> str:
        """Resolve or create a session without running profile analysis."""
        with get_connection(self.db_path) as conn:
            session = self.repository._get_or_create_session(conn, session_id=session_id)
            return str(session["session_id"])

    def list_sessions(self, limit: int = 30) -> dict[str, Any]:
        bounded_limit = min(max(int(limit or 30), 1), 80)
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT s.*,
                       (
                           SELECT content
                           FROM agent_dialogue_turns t
                           WHERE t.session_id = s.session_id AND t.role = 'user'
                           ORDER BY t.id ASC
                           LIMIT 1
                       ) AS first_user_message,
                       (
                           SELECT content
                           FROM agent_dialogue_turns t
                           WHERE t.session_id = s.session_id
                           ORDER BY t.id DESC
                           LIMIT 1
                       ) AS latest_message,
                       (
                           SELECT COUNT(*)
                           FROM agent_dialogue_turns t
                           WHERE t.session_id = s.session_id
                       ) AS message_count
                FROM agent_dialogue_sessions s
                WHERE s.user_id = ?
                ORDER BY s.updated_at DESC
                LIMIT ?
                """,
                (self.user_id, bounded_limit),
            ).fetchall()
        return {"items": [_serialize_session_summary(row) for row in rows]}

    @_finish_session_analysis
    def create_session(self) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            session = self.repository._create_session(conn)
            return self._serialize_session(conn, session)

    def delete_session(self, session_id: str) -> dict[str, Any]:
        normalized = str(session_id or "").strip()
        if not normalized:
            raise APIError.validation_error("sessionId is required")
        with get_connection(self.db_path) as conn:
            self.repository._lock_session(conn, normalized)
            session = self.repository._load_session(conn, normalized)
            if session is None:
                raise APIError.not_found("dialogue session not found")
            active = conn.execute(
                """SELECT 1 FROM durable_jobs
                   WHERE user_id = ? AND kind IN ('dialogue', 'discovery_watch')
                     AND status IN ('queued', 'running')
                     AND json_extract(payload_json, '$.session_id') = ? LIMIT 1""",
                (self.user_id, normalized),
            ).fetchone()
            if active is not None:
                raise APIError.conflict("对话任务仍在运行，请完成后再删除")
            conn.execute(
                "DELETE FROM agent_dialogue_sessions WHERE session_id = ? AND user_id = ?",
                (normalized, self.user_id),
            )
        return {"deleted": True, "sessionId": normalized}

    @_finish_session_analysis
    def undo_last_message(self, *, session_id: str | None = None) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            session = self.repository._get_or_create_session(conn, session_id=session_id)
            checkpoint = self.repository._latest_checkpoint(conn, session["session_id"])
            if checkpoint is None:
                result = self._serialize_session(conn, session)
                result["undone"] = False
                result["message"] = "没有可撤回的上一条消息。"
                return result

            self.repository._restore_checkpoint(conn, checkpoint)
            conn.execute(
                """
                DELETE FROM agent_dialogue_checkpoints
                WHERE session_id = ? AND user_id = ? AND created_at >= ?
                """,
                (session["session_id"], self.user_id, checkpoint["created_at"]),
            )
            restored_session = self.repository._load_session(conn, session["session_id"])
            result = self._serialize_session(conn, restored_session)
            result["undone"] = True
            result["message"] = "已撤回上一轮对话。"
            return result

    @music_operation("dialogue")
    @_finish_session_analysis
    def send_message(
        self,
        message: str,
        *,
        session_id: str | None = None,
        context_card_id: str | None = None,
        context_track_id: str | None = None,
        progress: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        trace = FullTrace(
            str(self.db_path),
            trace_type="dialogue",
            user_id=self.user_id,
            session_id=session_id,
            attributes={
                "messageHash": hash_text(message),
                "messageLength": len(message or ""),
                "hasContextCard": bool(context_card_id),
            },
        )
        with trace:
            result = self._send_message(
                message,
                session_id=session_id,
                context_card_id=context_card_id,
                context_track_id=context_track_id,
                progress=progress,
                full_trace=trace,
            )
            result["fullTraceId"] = trace.trace_id
            trace.event(
                "dialogue.completed",
                {
                    "sessionId": result.get("sessionId"),
                    "state": result.get("state"),
                },
            )
            return result

    def _send_message(
        self,
        message: str,
        *,
        session_id: str | None = None,
        context_card_id: str | None = None,
        context_track_id: str | None = None,
        progress: Callable[[str, dict[str, Any]], None] | None = None,
        full_trace: FullTrace,
    ) -> dict[str, Any]:
        normalized = _normalize_message(message)
        if not normalized:
            raise ValueError("message is required")
        if len(normalized) > MAX_MESSAGE_LENGTH:
            raise ValueError("message is too long")

        session_started = time.perf_counter()
        with get_connection(self.db_path) as conn:
            session = self.repository._get_or_create_session(conn, session_id=session_id)
            context_card = (
                self.repository._load_card(conn, context_card_id) if context_card_id else None
            )
            if context_card_id and context_card is None:
                raise KeyError(context_card_id)
            if context_card is not None and context_track_id:
                context_card = _context_card_with_track(context_card, context_track_id)

            payload = {}
            if context_card is not None:
                payload["quotedContext"] = _card_context(context_card)
            self.repository._save_checkpoint(
                conn, session["session_id"], reason="before_user_message"
            )
            self.repository._insert_turn(
                conn,
                session["session_id"],
                "user",
                normalized,
                card_id=context_card_id,
                payload=payload,
            )
            resolved_session_id = session["session_id"]
        full_trace.record_span(
            "dialogue.session.prepare",
            (time.perf_counter() - session_started) * 1000,
            kind="storage",
            outputs={"sessionId": resolved_session_id},
        )

        self.conversation_memory.append(
            session_id=resolved_session_id, role="user", content=normalized
        )
        route_started = time.perf_counter()
        route = self._route_message(normalized, context_card, session_id=resolved_session_id)
        full_trace.record_span(
            "dialogue.route",
            (time.perf_counter() - route_started) * 1000,
            kind="planning",
            outputs={
                "tool": route.tool,
                "source": route.route_source,
                "confidence": route.confidence,
                "hasRequestSpec": route.request_spec is not None,
                "hasSignal": route.signal is not None,
            },
        )
        _emit_progress(
            progress,
            "route",
            {
                "tool": route.tool,
                "source": route.route_source,
                "confidence": route.confidence,
            },
        )
        warm_topic = (
            route.signal.topic
            if route.signal
            else route.request_spec.primary_label
            if route.request_spec
            else route.emotion
        )
        warm_memory_type = (
            "preference_context"
            if route.signal is not None
            else "request_summary"
            if route.request_spec is not None
            else "emotion_state"
        )
        warm_started = time.perf_counter()
        self.conversation_memory.refresh_warm(
            session_id=resolved_session_id,
            topic=warm_topic,
            memory_type=warm_memory_type,
            scope_type="scene",
            scope_key="music_conversation",
        )
        full_trace.record_span(
            "dialogue.warm_memory.write",
            (time.perf_counter() - warm_started) * 1000,
            kind="memory",
            inputs={"memoryType": warm_memory_type, "scope": "music_conversation"},
        )
        if route.signal is not None:
            self._record_conversation_signal(resolved_session_id, normalized, route.signal)
            referenced_track_id = (
                str(context_card.get("track_id") or "") if isinstance(context_card, dict) else ""
            )
            if referenced_track_id and (
                _has_positive_preference_intent(normalized)
                or _has_negative_preference_intent(normalized)
            ):
                try:
                    self.recommendation_service.record_events(
                        [
                            {
                                "trackId": referenced_track_id,
                                "event": "liked"
                                if route.signal.polarity == "positive"
                                else "dislike",
                                "scene": "conversation",
                                "source": "quoted_recommendation_dialogue",
                                "reason": normalized,
                            }
                        ]
                    )
                except Exception:
                    pass

        if route.tool == "direct_chat":
            assistant_text = _direct_chat_reply(normalized)
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus="闲聊",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                return self._serialize_session(conn, session, include_analysis=False)

        if route.tool == "explain_recommendation":
            assistant_text = self._recommendation_explanation(normalized)
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus="推荐原因",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                return self._serialize_session(conn, session)

        if route.tool == "recall_memory":
            recall_query = _recall_query(normalized)
            tracks = self._recall_tracks(recall_query)
            assistant_text = _recall_reply(recall_query, tracks)
            with get_connection(self.db_path) as conn:
                card = self.repository._insert_card(
                    conn,
                    resolved_session_id,
                    kind="memory_recall",
                    title="听过的歌",
                    prompt=assistant_text,
                    statement="",
                    topic=recall_query or "最近播放",
                    polarity="neutral",
                    source_text=normalized,
                    payload={
                        "tracks": tracks,
                        "note": "来自本地播放记录和曲库",
                    },
                )
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    card_id=card["card_id"],
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="serving",
                    focus=recall_query or "最近播放",
                    pending_context=_card_context(card),
                )
                session = self.repository._load_session(conn, resolved_session_id)
                return self._serialize_session(conn, session)

        if route.tool == "control":
            assistant_text = _control_reply(route.control_action)
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus="播放控制",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                return self._serialize_session(conn, session)

        if route.tool == "profile_chat":
            write_result = self._submit_signal_safely(route.signal)
            analysis = self._safe_analysis()
            recent_context = self.repository._recent_user_context(resolved_session_id)
            fallback_text = _profile_chat_reply(
                normalized,
                analysis=analysis,
                recent_context=recent_context,
            )
            chat_reply = self._generate_chat_reply(
                normalized,
                route=route,
                session_id=resolved_session_id,
                analysis=analysis,
                fallback_text=fallback_text,
            )
            assistant_text = chat_reply["text"]
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route, reply_engine=chat_reply["engine"]),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus=route.signal.topic if route.signal else "口味聊天",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                result = self._serialize_session(conn, session)
            if write_result is not None:
                result["memoryIds"] = write_result.get("memoryIds") or []
                result["eventId"] = write_result.get("eventId")
            return result

        if route.tool == "chat_with_signal" and route.signal is not None:
            signal = route.signal
            write_result = self._submit_signal_safely(signal)
            analysis = self._safe_analysis()
            fallback_text = _signal_reply(
                signal,
                profile_hint=_profile_hint(analysis),
                stored=write_result is not None,
            )
            chat_reply = self._generate_chat_reply(
                normalized,
                route=route,
                session_id=resolved_session_id,
                analysis=analysis,
                fallback_text=fallback_text,
            )
            assistant_text = chat_reply["text"]
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route, reply_engine=chat_reply["engine"]),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus=signal.topic or "当前状态",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                result = self._serialize_session(conn, session)
            if write_result is not None:
                result["memoryIds"] = write_result.get("memoryIds") or []
                result["eventId"] = write_result.get("eventId")
            return result

        if route.tool == "recommend_music":
            signal = route.signal
            write_result = self._submit_signal_safely(signal) if signal is not None else None
            request_spec = route.request_spec or RequestInterpreter().interpret(normalized)
            scene_memory_id = self.recommendation_service.remember_request(
                scene="conversation",
                request_spec=request_spec,
            )
            _emit_progress(
                progress,
                "memory",
                {
                    "sceneMemoryId": scene_memory_id,
                    "requestSpec": request_spec.to_dict(),
                },
            )
            _emit_progress(progress, "recommendation", {"status": "ranking"})
            recommendations = self._safe_recommendations(request_spec)
            discovery_job_id = (
                self._schedule_discovery(request_spec)
                if len(recommendations) < RECOMMENDATION_CARD_LIMIT
                else None
            )
            discovery_result = {
                "status": "queued" if discovery_job_id else "not_needed",
                "jobId": discovery_job_id,
                "initialCount": len(recommendations),
            }
            _emit_progress(
                progress,
                "discovery",
                {
                    "status": discovery_result["status"],
                    "jobId": discovery_job_id,
                    "initialCount": len(recommendations),
                },
            )
            title_topic = (
                signal.topic
                if signal
                else request_spec.primary_label
                or route.emotion
                or _topic_from_text(normalized)
                or "这轮"
            )
            memory_ids = [] if write_result is None else write_result.get("memoryIds") or []
            analysis = self._safe_analysis()
            assistant_text = _recommendation_reply(
                topic=title_topic,
                recommendation_count=len(recommendations),
                source_text=normalized,
                profile_hint=_profile_hint(analysis),
            )
            with get_connection(self.db_path) as conn:
                card = self.repository._insert_card(
                    conn,
                    resolved_session_id,
                    kind="recommendation_carousel",
                    title=f"{title_topic} 推荐",
                    prompt=assistant_text,
                    statement=signal.statement if signal else "",
                    topic=title_topic,
                    polarity=signal.polarity if signal else "neutral",
                    source_text=normalized,
                    payload={
                        "recommendations": recommendations,
                        "memoryIds": memory_ids,
                        "eventId": None if write_result is None else write_result.get("eventId"),
                        "requestSpec": request_spec.to_dict(),
                        "sceneMemoryId": scene_memory_id,
                        "discoveryJobId": discovery_job_id,
                        "discoveryStatus": "queued" if discovery_job_id else "completed",
                        "discovery": discovery_result,
                        "note": "按本轮请求范围和长期听歌记录推荐",
                    },
                )
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    card_id=card["card_id"],
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="serving",
                    focus=title_topic,
                    pending_context=_card_context(card),
                )
                session = self.repository._load_session(conn, resolved_session_id)
                result = self._serialize_session(conn, session)
            if write_result is not None:
                result["memoryIds"] = write_result.get("memoryIds") or []
                result["eventId"] = write_result.get("eventId")
            return result

        if route.tool == "confirm_signal" and route.signal is not None:
            signal = route.signal
            with get_connection(self.db_path) as conn:
                card = self.repository._insert_card(
                    conn,
                    resolved_session_id,
                    kind="pending_confirmation",
                    title="等你确认",
                    prompt=f"我先理解成：{signal.statement} 这样准吗？",
                    statement=signal.statement,
                    topic=signal.topic,
                    polarity=signal.polarity,
                    source_text=normalized,
                    payload={
                        "confidence": signal.confidence,
                        "signalKind": signal.kind,
                        "contextCardId": context_card_id,
                    },
                )
                assistant_text = (
                    f"这会明显影响长期推荐，我先按你原话理解成：{signal.statement} 准吗？"
                )
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    card_id=card["card_id"],
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="awaiting_confirmation",
                    focus=signal.topic or signal.polarity,
                    pending_context=_card_context(card),
                )
                session = self.repository._load_session(conn, resolved_session_id)
                return self._serialize_session(conn, session)

        if route.tool == "profile_update" and route.signal is not None:
            signal = route.signal
            write_result = self._submit_signal_safely(signal)
            assistant_text = _profile_update_reply(signal, stored=write_result is not None)
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus=signal.topic or "口味调整",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                result = self._serialize_session(conn, session)
            if write_result is not None:
                result["memoryIds"] = write_result.get("memoryIds") or []
                result["eventId"] = write_result.get("eventId")
            return result

        if route.signal is not None:
            signal = route.signal
            write_result = self._submit_signal_safely(signal)
            assistant_text = _signal_reply(
                signal,
                profile_hint=_profile_hint(self._safe_analysis()),
                stored=write_result is not None,
            )
            with get_connection(self.db_path) as conn:
                self.repository._insert_turn(
                    conn,
                    resolved_session_id,
                    "assistant",
                    assistant_text,
                    payload=_route_payload(route),
                )
                self.repository._touch_session(
                    conn,
                    resolved_session_id,
                    state="chatting",
                    focus=signal.topic or "当前状态",
                    pending_context={},
                )
                session = self.repository._load_session(conn, resolved_session_id)
                result = self._serialize_session(conn, session)
            if write_result is not None:
                result["memoryIds"] = write_result.get("memoryIds") or []
                result["eventId"] = write_result.get("eventId")
            return result

        recent_context = self.repository._recent_user_context(resolved_session_id)
        analysis = self._safe_analysis()
        fallback_text = _casual_reply(
            normalized,
            profile_hint=_profile_hint(analysis),
            recent_context=recent_context,
        )
        chat_reply = self._generate_chat_reply(
            normalized,
            route=route,
            session_id=resolved_session_id,
            analysis=analysis,
            fallback_text=fallback_text,
        )
        assistant_text = chat_reply["text"]
        with get_connection(self.db_path) as conn:
            self.repository._insert_turn(
                conn,
                resolved_session_id,
                "assistant",
                assistant_text,
                payload=_route_payload(route, reply_engine=chat_reply["engine"]),
            )
            self.repository._touch_session(
                conn,
                resolved_session_id,
                state="chatting",
                focus="日常聊天",
                pending_context={},
            )
            session = self.repository._load_session(conn, resolved_session_id)
            return self._serialize_session(conn, session)

    @_finish_session_analysis
    def submit_feedback(
        self,
        card_id: str,
        action: str,
        *,
        reply: str | None = None,
    ) -> dict[str, Any]:
        normalized_action = _normalize_action(action)
        feedback_reply = _normalize_message(reply or "")
        card_snapshot: dict[str, Any]

        with get_connection(self.db_path) as conn:
            card = self.repository._load_card(conn, card_id, for_update=True)
            if card is None:
                raise KeyError(card_id)
            session = self.repository._load_session(conn, card["session_id"])
            if session is None or session["user_id"] != self.user_id:
                raise KeyError(card_id)

            next_status = _status_for_action(normalized_action)
            payload = _json_loads(card["payload_json"])
            payload["lastAction"] = normalized_action
            if feedback_reply:
                payload["feedbackReply"] = feedback_reply
            self.repository._update_card(
                conn,
                card_id,
                status=next_status,
                payload=payload,
            )
            self.repository._touch_session(
                conn,
                card["session_id"],
                state="writing_profile" if normalized_action in CONFIRM_ACTIONS else next_status,
                focus=card["topic"] or card["polarity"],
                pending_context=_card_context(card) if normalized_action == "discuss" else {},
            )
            card_snapshot = dict(card)
            card_snapshot["status"] = next_status
            card_snapshot["payload_json"] = json.dumps(payload, ensure_ascii=False)

        write_result: dict[str, Any] | None = None
        if normalized_action in CONFIRM_ACTIONS and card_snapshot["statement"].strip():
            try:
                write_result = self._submit_confirmed_statement(card_snapshot["statement"])
            except Exception as exc:
                with get_connection(self.db_path) as conn:
                    payload = _json_loads(card_snapshot["payload_json"])
                    payload["error"] = str(exc)
                    self.repository._update_card(conn, card_id, status="failed", payload=payload)
                    session = self.repository._load_session(conn, card_snapshot["session_id"])
                    return self._serialize_session(conn, session)

        with get_connection(self.db_path) as conn:
            card = self.repository._load_card(conn, card_id, for_update=True)
            if card is None:
                raise KeyError(card_id)
            payload = _json_loads(card["payload_json"])
            if write_result is not None:
                payload["memoryIds"] = write_result.get("memoryIds") or []
                payload["eventId"] = write_result.get("eventId")
                self.repository._update_card(conn, card_id, status="confirmed", payload=payload)
                card = self.repository._load_card(conn, card_id)

            assistant_text = self._feedback_reply(card, normalized_action)
            turn_payload = (
                {"confirmedContext": _card_context(card)}
                if normalized_action in CONFIRM_ACTIONS
                else {}
            )
            self.repository._insert_turn(
                conn,
                card["session_id"],
                "assistant",
                assistant_text,
                card_id=card_id,
                payload=turn_payload,
            )

            follow_up = None
            if normalized_action in CONFIRM_ACTIONS:
                follow_up = self._insert_follow_up_card(conn, card)
            elif normalized_action in REJECT_ACTIONS:
                follow_up = self._insert_probe_card(
                    conn,
                    card["session_id"],
                    source_text=card["source_text"],
                )
            elif normalized_action == "discuss" and feedback_reply:
                follow_up = self._insert_context_confirmation_card(conn, card, feedback_reply)

            self.repository._touch_session(
                conn,
                card["session_id"],
                state=_state_after_action(normalized_action),
                focus=(follow_up or card)["topic"] or card["polarity"],
                pending_context=_card_context(follow_up or card),
            )
            session = self.repository._load_session(conn, card["session_id"])
            result = self._serialize_session(conn, session)

        if write_result is not None:
            result["memoryIds"] = write_result.get("memoryIds") or []
            result["eventId"] = write_result.get("eventId")
            result["analysis"] = write_result.get("analysis")
        return result

    @_finish_session_analysis
    def refresh_recommendation_card(self, card_id: str) -> dict[str, Any]:
        with get_connection(self.db_path) as conn:
            card = self.repository._load_card(conn, card_id)
            if card is None or card["kind"] != "recommendation_carousel":
                raise KeyError(card_id)
            payload = _json_loads(card["payload_json"])
            job_id = str(payload.get("discoveryJobId") or "")
            if job_id:
                status = self.recommendation_service.discovery_status(job_id)
                job_status = str(status.get("status") or "")
                if status.get("available") and job_status not in {
                    "completed", "failed", "needs_reconciliation"
                }:
                    payload["discoveryStatus"] = job_status or "queued"
                    self.repository._update_card(
                        conn, card_id, status=card["status"], payload=payload
                    )
                    session = self.repository._load_session(conn, card["session_id"])
                    return self._serialize_session(conn, session)
                if not status.get("available") or job_status in {
                    "failed", "needs_reconciliation"
                }:
                    payload["discoveryStatus"] = "failed"
                    payload["error"] = status.get("error") or "discovery job unavailable"
                    self.repository._update_card(
                        conn, card_id, status=card["status"], payload=payload
                    )
                    session = self.repository._load_session(conn, card["session_id"])
                    return self._serialize_session(conn, session)
            spec = RequestSpec.from_dict(payload.get("requestSpec") or {})
            existing = (
                payload.get("recommendations")
                if isinstance(payload.get("recommendations"), list)
                else []
            )
            payload["recommendations"] = _merge_recommendations(
                existing,
                self._safe_recommendations(spec),
                limit=RECOMMENDATION_CARD_LIMIT,
            )
            payload["discoveryStatus"] = "completed"
            self.repository._update_card(conn, card_id, status=card["status"], payload=payload)
            session = self.repository._load_session(conn, card["session_id"])
            return self._serialize_session(conn, session)

    def _insert_seed_card(self, conn: Any, session_id: str) -> dict[str, Any]:
        analysis = self._safe_analysis()
        top_positive = (analysis.get("summary") or {}).get("topPositiveTopics") or []
        if top_positive:
            topic = str(top_positive[0].get("name") or "").strip()
            if topic:
                return self.repository._insert_card(
                    conn,
                    session_id,
                    kind="interest_probe",
                    title="延续这个口味？",
                    prompt=f"我看你之前和 {topic} 比较合拍，这轮还按这个方向走吗？",
                    statement=f"用户希望 B 站电台继续优先推荐 {topic} 相关音乐。",
                    topic=topic,
                    polarity="positive",
                    source_text=f"profile:{topic}",
                    payload={"seed": "profile"},
                )
        return self._insert_probe_card(conn, session_id, source_text="onboarding")

    def _insert_probe_card(self, conn: Any, session_id: str, *, source_text: str) -> dict[str, Any]:
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM agent_dialogue_cards WHERE session_id = ?",
                (session_id,),
            ).fetchone()[0]
        )
        probe = GENERIC_PROBES[count % len(GENERIC_PROBES)]
        return self.repository._insert_card(
            conn,
            session_id,
            kind=probe["kind"],
            title=probe["title"],
            prompt=probe["prompt"],
            statement=probe["statement"],
            topic=probe["topic"],
            polarity=probe["polarity"],
            source_text=source_text,
            payload={"seed": "generic"},
        )

    def _insert_recommendation_follow_up(
        self,
        conn: Any,
        session_id: str,
        topic: str,
    ) -> dict[str, Any]:
        return self.repository._insert_card(
            conn,
            session_id,
            kind="interest_probe",
            title="继续校准",
            prompt="这轮如果方向对，我下次就多保留这种气质；如果不对，直接告诉我哪里偏了。",
            statement=f"用户希望继续保留 {topic} 这一类音乐气质。",
            topic=topic,
            polarity="positive",
            source_text=f"recommendation:{topic}",
            payload={"seed": "recommendation_follow_up"},
        )

    def _insert_context_confirmation_card(
        self,
        conn: Any,
        source_card: Any,
        reply: str,
    ) -> dict[str, Any]:
        statement = f"{source_card['statement'].strip()} 补充：{reply}"
        return self.repository._insert_card(
            conn,
            source_card["session_id"],
            kind="pending_confirmation",
            title="待确认补充",
            prompt=f"我把补充合并成：{statement} 准吗？",
            statement=statement,
            topic=source_card["topic"],
            polarity=source_card["polarity"],
            source_text=source_card["statement"],
            payload={"parentCardId": source_card["card_id"]},
        )

    def _insert_follow_up_card(self, conn: Any, source_card: Any) -> dict[str, Any]:
        if source_card["polarity"] == "negative":
            return self.repository._insert_card(
                conn,
                source_card["session_id"],
                kind="interest_probe",
                title="替代方向",
                prompt="要不要把被你避开的方向替换成旋律更清晰、情绪更稳定的内容？",
                statement="用户希望用旋律更清晰、情绪更稳定的内容替代明确回避的音乐方向。",
                topic="替代方向",
                polarity="positive",
                source_text=source_card["statement"],
                payload={"parentCardId": source_card["card_id"]},
            )
        return self.repository._insert_card(
            conn,
            source_card["session_id"],
            kind="avoid_probe",
            title="边界控制",
            prompt="就算喜欢这个方向，也避免同一 UP 主或同质内容连续刷屏吗？",
            statement="用户不希望推荐列表里连续出现同一 UP 主或高度同质的音乐内容。",
            topic="推荐边界",
            polarity="negative",
            source_text=source_card["statement"],
            payload={"parentCardId": source_card["card_id"]},
        )

    def _record_conversation_signal(
        self,
        session_id: str,
        source_text: str,
        signal: ExtractedSignal,
    ) -> None:
        now = _utc_now()
        with get_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO agent_dialogue_signals (
                    session_id, user_id, kind, topic, statement,
                    confidence, status, source_text, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    self.user_id,
                    signal.kind,
                    signal.topic[:80],
                    signal.statement[:1000],
                    signal.confidence,
                    signal.commit_policy,
                    source_text[:1000],
                    now,
                ),
            )
            from durable_jobs import enqueue_behavior

            queued = enqueue_behavior(
                conn,
                user_id=self.user_id,
                event="conversation_signal",
                scene="conversation",
                payload={
                    "sessionId": session_id,
                    "kind": signal.kind,
                    "topic": signal.topic,
                    "statement": signal.statement,
                    "confidence": signal.confidence,
                    "status": signal.commit_policy,
                    "sourceText": source_text,
                },
            )
        if queued:
            return
        try:
            record_music_behavior(
                self.recommendation_service.amem_bridge,
                user_id=self.user_id,
                event="conversation_signal",
                scene="conversation",
                payload={
                    "sessionId": session_id,
                    "kind": signal.kind,
                    "topic": signal.topic,
                    "statement": signal.statement,
                    "confidence": signal.confidence,
                    "status": signal.commit_policy,
                    "sourceText": source_text,
                },
            )
        except Exception:
            return

    def _submit_signal_safely(self, signal: ExtractedSignal | None) -> dict[str, Any] | None:
        if signal is None or signal.commit_policy != "commit":
            return None
        if signal.kind == "preference_hypothesis" and signal.confidence < 0.82:
            return None
        try:
            return self._submit_confirmed_statement(signal.statement)
        except Exception:
            return None

    def _generate_chat_reply(
        self,
        message: str,
        *,
        route: DialogueRoute,
        session_id: str,
        analysis: dict[str, Any],
        fallback_text: str,
    ) -> dict[str, str]:
        if not _dialogue_llm_enabled():
            return {"text": fallback_text, "engine": "fallback"}
        try:
            text = _llm_chat_reply(
                message,
                route=route,
                analysis=analysis,
                recent_turns=self.repository._recent_turn_context(session_id),
                client=_default_llm_client(user_id=self.user_id, db_path=str(self.db_path)),
            )
            return {"text": text, "engine": "llm"}
        except Exception:
            return {"text": fallback_text, "engine": "fallback"}

    def _recommendation_explanation(self, message: str) -> str:
        trace = self._latest_recommendation_trace()
        if not trace.get("available"):
            return (
                "我还没有稳定的一轮推荐记录。你可以先说今天想听什么，"
                "我会按你的原话、最近播放和负反馈一起选。"
            )

        final_results = trace.get("finalResults") or []
        target_index = _explanation_target_index(message, len(final_results))
        if target_index is not None:
            return _single_recommendation_explanation(final_results[target_index], target_index)

        matched = _trace_matched_preferences(final_results)
        penalties = _trace_penalty_labels(final_results)
        sources = _trace_source_labels(final_results)
        evidence = _trace_evidence(final_results)

        parts = ["这批不是重新猜出来的，是按上一轮推荐留下的排序记录解释。"]
        if matched:
            parts.append(f"主方向贴着 {'、'.join(matched[:4])}。")
        elif evidence:
            parts.append(f"主要依据是{evidence[0]}。")
        if "探索" in sources:
            parts.append("里面留了少量探索项，用来试探相近但不完全重复的口味。")
        if penalties:
            parts.append(f"同时实际做过这些压低处理：{'、'.join(penalties[:3])}。")
        parts.append("如果你想看单首原因，可以直接问“第一首为什么”。")
        return "".join(parts)

    def _latest_recommendation_trace(self) -> dict[str, Any]:
        for scene in ("conversation", "home"):
            try:
                trace = self.recommendation_service.latest_debug_trace(scene=scene)
            except Exception:
                continue
            if trace.get("available"):
                return trace
        return {"available": False}

    def _safe_recommendations(
        self, request_spec: RequestSpec | None = None
    ) -> list[dict[str, Any]]:
        try:
            result = self.recommendation_service.list_recommendations(
                scene="conversation",
                limit=RECOMMENDATION_CARD_LIMIT,
                request_spec=request_spec,
            )
        except Exception:
            return []
        items = result.get("items") if isinstance(result, dict) else []
        return items if isinstance(items, list) else []

    def _schedule_discovery(self, request_spec: RequestSpec) -> str | None:
        try:
            return self.recommendation_service.enqueue_discovery(
                scene="conversation",
                limit=RECOMMENDATION_CARD_LIMIT,
                request_spec=request_spec,
            )
        except Exception:
            return None

    def _bootstrap_discovery(self, request_spec: RequestSpec) -> dict[str, Any]:
        try:
            return self.recommendation_service.bootstrap_discovery(
                scene="conversation",
                limit=RECOMMENDATION_CARD_LIMIT,
                request_spec=request_spec,
            )
        except Exception:
            return {}

    def _recall_tracks(self, query: str) -> list[dict[str, Any]]:
        pattern = f"%{_escape_like(query)}%" if query else "%"
        with get_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT t.*,
                       COALESCE(
                           r.last_played_at,
                           pr.last_played_at,
                           t.updated_at
                       ) AS last_played_at,
                       COALESCE(r.play_count, 0) AS recent_play_count,
                       COALESCE(r.position_ms, pr.position_ms, 0) AS position_ms,
                       COALESCE(r.listen_ms, pr.listen_ms, 0) AS listen_ms,
                       COALESCE(r.completed, pr.completed, 0) AS completed
                FROM tracks t
                LEFT JOIN recent r ON r.user_id = ? AND r.track_id = t.track_id
                LEFT JOIN playback_recent pr ON pr.user_id = ? AND pr.track_id = t.track_id
                WHERE (r.track_id IS NOT NULL OR pr.track_id IS NOT NULL)
                  AND (
                    ? = ''
                    OR t.title LIKE ? ESCAPE '\\'
                    OR t.owner LIKE ? ESCAPE '\\'
                    OR COALESCE(t.page_title, '') LIKE ? ESCAPE '\\'
                  )
                ORDER BY COALESCE(r.last_played_at, pr.last_played_at, t.updated_at) DESC
                LIMIT ?
                """,
                (
                    self.user_id,
                    self.user_id,
                    query,
                    pattern,
                    pattern,
                    pattern,
                    RECALL_RESULT_LIMIT,
                ),
            ).fetchall()
        return [self.recommendation_service.library._track_payload_with_meta(row) for row in rows]

    def _serialize_session(
        self,
        conn: Any,
        session: Any,
        *,
        include_analysis: bool = True,
    ) -> dict[str, Any]:
        if session is None:
            return {}
        turns = conn.execute(
            """
            SELECT *
            FROM agent_dialogue_turns
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (session["session_id"], SESSION_TURN_LIMIT),
        ).fetchall()
        card_rows = conn.execute(
            """
            SELECT *
            FROM agent_dialogue_cards
            WHERE session_id = ?
            ORDER BY updated_at DESC
            """,
            (session["session_id"],),
        ).fetchall()
        cards = [_serialize_card(row) for row in card_rows]
        cards_by_id = {card["cardId"]: card for card in cards}
        defer_analysis = include_analysis and conn.in_transaction
        return {
            "sessionId": session["session_id"],
            "state": session["state"],
            "focus": session["focus"],
            "createdAt": session["created_at"],
            "updatedAt": session["updated_at"],
            "pendingContext": _json_loads(session["pending_context_json"]),
            "messages": [_serialize_turn(row, cards_by_id) for row in reversed(turns)],
            "cards": self.repository._pending_cards(conn, session["session_id"]),
            "analysis": self._safe_analysis()
            if include_analysis and not defer_analysis
            else _empty_analysis(),
            **({"_deferred_analysis": True} if defer_analysis else {}),
        }

    def _feedback_reply(self, card: Any, action: str) -> str:
        statement = card["statement"].strip()
        if action in CONFIRM_ACTIONS:
            return f"收到，这条后续会影响选歌：{statement}"
        if action in REJECT_ACTIONS:
            return "好，这条判断不采用。下一轮我换个角度。"
        if action == "later":
            return "好，先搁置，不影响后续推荐。"
        return f"我们继续聊这条：{statement}"

    def _submit_confirmed_statement(self, statement: str) -> dict[str, Any]:
        return self.recommendation_service.submit_profile_statement(statement)

    def _safe_analysis(self) -> dict[str, Any]:
        try:
            return self.recommendation_service.music_profile_analysis(scene="conversation")
        except Exception:
            return _empty_analysis()

    def _route_message(
        self, message: str, context_card: Any | None, *, session_id: str
    ) -> DialogueRoute:
        rule_route = _route_message(message, context_card)
        if _is_high_confidence_route(rule_route, message):
            return _canonical_route(rule_route, message, source="rule", confidence=1.0)
        if not _dialogue_router_llm_enabled():
            return _canonical_route(rule_route, message, source="rule_fallback", confidence=0.0)
        try:
            route = _llm_route_message(
                message,
                context_card=context_card,
                recent_turns=self.conversation_memory.hot(session_id=session_id),
                client=self.router_llm_client or _default_llm_client(
                    user_id=self.user_id, db_path=str(self.db_path)
                ),
            )
            return _canonical_route(route, message, source="llm_tool", confidence=0.72)
        except Exception:
            return _canonical_route(rule_route, message, source="rule_fallback", confidence=0.0)
