from __future__ import annotations

from dataclasses import replace

from agent_memory_runtime.agent.context_window import compact_checkpoint
from agent_memory_runtime.agent.model_gateway import _provider_message
from agent_memory_runtime.agent.models import AgentCheckpoint, ModelMessage
from agent_memory_runtime.agent.policy import AgentPolicy
from agent_memory_runtime.config import RuntimeConfig
from agent_memory_runtime.context.builder import ContextBuilder
from agent_memory_runtime.domain.memory import MemoryRecord
from agent_memory_runtime.domain.query import MemoryQuery, RetrievalTrace
from agent_memory_runtime.memory.compression.budget import estimate_tokens
from agent_memory_runtime.memory.compression.selector import select_under_budget
from agent_memory_runtime.tokens import AdaptiveTokenEstimator


def _policy() -> AgentPolicy:
    return AgentPolicy(
        model_context_tokens=6000,
        reserved_output_tokens=500,
        context_compaction_ratio=0.1,
        context_keep_recent_messages=2,
    )


def _filler() -> tuple[ModelMessage, ...]:
    return tuple(
        ModelMessage(role="assistant", content="ordinary output " * 150)
        for _ in range(10)
    )


def test_new_user_direction_supersedes_old_pin_across_two_compactions():
    checkpoint = AgentCheckpoint(
        run_id="constraint-run",
        messages=(
            ModelMessage(role="system", content="system rules"),
            ModelMessage(role="user", content="Recommend music"),
            ModelMessage(role="user", content="不要 Rap"),
            *_filler(),
            ModelMessage(role="user", content="现在可以 Rap"),
            *_filler(),
        ),
    )
    compacted, report = compact_checkpoint(
        checkpoint, tools=(), estimator=AdaptiveTokenEstimator(), policy=_policy(), model=None
    )
    assert report is not None
    assert any(
        item.content == "不要 Rap" and item.status == "superseded"
        for item in compacted.constraint_revisions
    )
    assert any(
        item.content == "现在可以 Rap" and item.status == "resolution"
        for item in compacted.constraint_revisions
    )
    assert all(item.content != "不要 Rap" for item in compacted.pinned_messages)
    assert "current_user_intent: 现在可以 Rap" in "\n".join(
        item.content for item in compacted.messages
    )

    restored = AgentCheckpoint.from_dict(compacted.to_dict())
    restored = replace(
        restored,
        messages=(*restored.messages, ModelMessage(role="user", content="不要 Rap"), *_filler()),
    )
    revised, report = compact_checkpoint(
        restored, tools=(), estimator=AdaptiveTokenEstimator(), policy=_policy(), model=None
    )
    assert report is not None
    rap_revisions = [item for item in revised.constraint_revisions if item.content == "不要 Rap"]
    assert [item.status for item in rap_revisions] == ["superseded", "active"]
    assert any(
        item.content == "现在可以 Rap" and item.status == "superseded"
        for item in revised.constraint_revisions
    )
    assert rap_revisions[-1].version > rap_revisions[0].version
    assert any(item.content == "不要 Rap" for item in revised.pinned_messages)
    assert "current_user_intent: 不要 Rap" in "\n".join(item.content for item in revised.messages)


def test_unrelated_new_topic_does_not_revoke_constraint():
    checkpoint = AgentCheckpoint(
        run_id="unrelated-run",
        messages=(
            ModelMessage(role="system", content="system rules"),
            ModelMessage(role="user", content="Recommend music"),
            ModelMessage(role="user", content="不要 Rap"),
            *_filler(),
            ModelMessage(role="user", content="现在可以 Jazz"),
            *_filler(),
        ),
    )
    compacted, report = compact_checkpoint(
        checkpoint, tools=(), estimator=AdaptiveTokenEstimator(), policy=_policy(), model=None
    )
    assert report is not None
    assert any(item.content == "不要 Rap" and item.status == "active"
               for item in compacted.constraint_revisions)


def test_original_task_constraint_becomes_historical_after_explicit_correction():
    checkpoint = AgentCheckpoint(
        run_id="original-rule",
        messages=(
            ModelMessage(role="system", content="system rules"),
            ModelMessage(role="user", content="不要 Rap"),
            *_filler(),
            ModelMessage(role="user", content="现在可以 Rap"),
            *_filler(),
        ),
    )
    compacted, report = compact_checkpoint(
        checkpoint, tools=(), estimator=AdaptiveTokenEstimator(), policy=_policy(), model=None
    )
    assert report is not None
    assert any(message.role == "user" and message.content == "不要 Rap"
               for message in compacted.messages)
    assert not compacted.pinned_messages
    state = next(message.content for message in compacted.messages
                 if message.content.startswith("<task-state>"))
    assert "current_user_intent: 现在可以 Rap" in state
    assert "superseded_user_constraint_v1: 不要 Rap" in state


def test_internal_message_identity_is_not_sent_to_model_provider():
    message = ModelMessage(role="user", content="hello", message_id="context-message:123")
    assert ModelMessage.from_dict(message.to_dict()).message_id == message.message_id
    assert "message_id" not in _provider_message(message)


def test_explicit_global_revocation_is_versioned_without_truncating_old_task():
    checkpoint = AgentCheckpoint(
        run_id="global-revision",
        messages=(
            ModelMessage(role="system", content="system rules"),
            ModelMessage(role="user", content="Keep the original task"),
            ModelMessage(role="user", content="不要 Rap"),
            ModelMessage(role="user", content="必须保留租户隔离"),
            *_filler(),
            ModelMessage(role="user", content="取消之前所有约束"),
            *_filler(),
        ),
    )
    compacted, report = compact_checkpoint(
        checkpoint, tools=(), estimator=AdaptiveTokenEstimator(), policy=_policy(), model=None
    )
    assert report is not None
    assert {item.content for item in compacted.pinned_messages} == set()
    assert all(
        item.status == "superseded"
        for item in compacted.constraint_revisions
        if item.content != "取消之前所有约束"
    )
    assert any(
        item.content == "取消之前所有约束" and item.status == "resolution"
        for item in compacted.constraint_revisions
    )
    assert "Keep the original task" in "\n".join(item.content for item in compacted.messages)


def test_oversized_pinned_constraint_is_preserved_for_hard_budget_rejection():
    important = "必须保留用户约束 " + "critical-data " * 1100
    checkpoint = AgentCheckpoint(
        run_id="hard-budget",
        messages=(
            ModelMessage(role="system", content="system rules"),
            ModelMessage(role="user", content="Initial task"),
            ModelMessage(role="user", content=important),
            *_filler(),
        ),
    )
    policy = AgentPolicy(
        model_context_tokens=1800,
        reserved_output_tokens=200,
        context_compaction_ratio=0.1,
        context_keep_recent_messages=2,
    )
    compacted, report = compact_checkpoint(
        checkpoint, tools=(), estimator=AdaptiveTokenEstimator(), policy=policy, model=None
    )
    assert report is not None
    assert important in "\n".join(item.content for item in compacted.messages)
    assert report.after_tokens > policy.model_context_tokens - policy.reserved_output_tokens


def _record(identity: str, content: str, *, level: str, excerpt: str = "") -> MemoryRecord:
    return MemoryRecord(
        memory_id=identity,
        memory_type="belief",
        session_id="music-profile",
        subject_id=identity,
        content=content,
        source_event_ids=("event-1",),
        rule_id="test",
        level=level,
        priority=0.9 if level == "L3" else 0.5,
        metadata={"context_excerpt": excerpt} if excerpt else {},
    )


def test_long_profile_uses_extract_under_hard_budget_without_mutating_store_record():
    original = _record(
        "profile",
        "User prefers calm jazz piano. " + "Supporting detail. " * 150,
        level="L3",
        excerpt="User prefers calm jazz piano.",
    )
    recent = _record("recent", "User recently completed a jazz track.", level="L1")
    budget = 190
    estimator = AdaptiveTokenEstimator()
    assert estimate_tokens(original, estimator=estimator) > budget
    selected = select_under_budget([original, recent], token_budget=budget, estimator=estimator)
    assert [record.memory_id for record in selected] == ["profile", "recent"]
    assert "Extractive excerpt" in selected[0].content
    assert original.content.endswith("Supporting detail. ")
    assert sum(estimate_tokens(item, estimator=estimator) for item in selected) <= budget


def test_unsafe_extract_is_rejected_and_reported_as_oversized():
    misleading = _record(
        "misleading",
        "User likes rap. However, never play rap at night. " + "Details. " * 150,
        level="L3",
        excerpt="User likes rap.",
    )
    recent = _record("recent", "User recently completed a jazz track.", level="L1")
    trace = RetrievalTrace(
        query=MemoryQuery(agent_id="agent", text="music"),
        candidate_count=2,
        blocked_count=0,
        selected_memory_ids=("misleading", "recent"),
    )
    context = ContextBuilder(RuntimeConfig(context_token_budget=160)).build(
        agent_id="agent", records=[misleading, recent], trace=trace
    )
    assert context.selected_memory_ids == ("recent",)
    assert context.metadata["omitted_oversized_memory_ids"] == ("misleading",)
    assert context.metadata["estimated_memory_tokens"] <= 160


def test_chinese_negative_clause_cannot_be_omitted_from_extract():
    misleading = _record(
        "night-rule",
        "用户喜欢爵士。晚上不喜欢高音量爵士。" + "其他背景。" * 180,
        level="L3",
        excerpt="用户喜欢爵士。",
    )
    assert select_under_budget([misleading], token_budget=120) == []
