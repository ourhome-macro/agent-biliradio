from __future__ import annotations

from dataclasses import dataclass, field

from agent_memory_runtime.access.projection import project_record
from agent_memory_runtime.config import RuntimeConfig
from agent_memory_runtime.context.fence import sanitize_context
from agent_memory_runtime.context.personalization import build_personalization_profile
from agent_memory_runtime.domain.memory import MemoryRecord
from agent_memory_runtime.domain.query import RetrievalTrace
from agent_memory_runtime.memory.compression import select_under_budget
from agent_memory_runtime.memory.compression.budget import estimate_tokens
from agent_memory_runtime.tokens import AdaptiveTokenEstimator, TokenEstimator


@dataclass(frozen=True)
class AgentContext:
    agent_id: str
    selected_memory_ids: tuple[str, ...]
    blocked_memory_count: int
    projected_context: str
    memories: tuple[dict[str, object], ...]
    trace: RetrievalTrace
    metadata: dict[str, object] = field(default_factory=dict)
    personalization_context: str = ""
    personalization: dict[str, str] = field(default_factory=dict)


class ContextBuilder:
    def __init__(
        self,
        config: RuntimeConfig | None = None,
        *,
        token_estimator: TokenEstimator | None = None,
    ) -> None:
        self.config = config or RuntimeConfig()
        self.token_estimator = token_estimator or AdaptiveTokenEstimator()

    def build(
        self,
        *,
        agent_id: str,
        records: list[MemoryRecord],
        trace: RetrievalTrace,
        metadata: dict[str, object] | None = None,
    ) -> AgentContext:
        selected = select_under_budget(
            records,
            token_budget=self.config.context_token_budget,
            estimator=self.token_estimator,
            model=self.config.llm.model,
        )
        estimated_tokens = sum(
            estimate_tokens(
                record,
                estimator=self.token_estimator,
                model=self.config.llm.model,
            )
            for record in selected
        )
        selected_ids = {record.memory_id for record in selected}
        oversized_memory_ids = tuple(
            record.memory_id
            for record in records
            if record.memory_id not in selected_ids
            and estimate_tokens(
                record, estimator=self.token_estimator, model=self.config.llm.model
            ) > self.config.context_token_budget
        )
        abbreviated_memory_ids = tuple(
            record.memory_id
            for record in selected
            if record.metadata.get("_context_abbreviated") is True
        )
        personalization = build_personalization_profile(selected)
        # 结构化投影和文本投影都先移除伪造围栏，避免下游调用绕过第一层防护。
        projected = tuple(_sanitize_projection(project_record(record)) for record in selected)
        return AgentContext(
            agent_id=agent_id,
            selected_memory_ids=tuple(record.memory_id for record in selected),
            blocked_memory_count=trace.blocked_count,
            projected_context=format_context(selected),
            memories=projected,
            trace=trace,
            metadata={
                **dict(metadata or {}),
                "estimated_memory_tokens": estimated_tokens,
                "memory_token_budget": self.config.context_token_budget,
                "abbreviated_memory_ids": abbreviated_memory_ids,
                "omitted_oversized_memory_ids": oversized_memory_ids,
            },
            personalization_context=personalization.render(),
            personalization=dict(personalization.values),
        )


def format_context(records: list[MemoryRecord]) -> str:
    lines: list[str] = []
    for record in records:
        lines.append(
            f"[{record.memory_id}] type={record.memory_type} level={record.level} "
            f"visibility={record.visibility} status={record.status} "
            f"temperature={record.temperature} priority={record.priority:.2f} "
            f"confidence={record.confidence:.2f} "
            f":: {record.content}"
        )
    return sanitize_context("\n".join(lines))


def _sanitize_projection(value: object) -> object:
    if isinstance(value, str):
        return sanitize_context(value)
    if isinstance(value, list):
        return [_sanitize_projection(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_projection(item) for item in value)
    if isinstance(value, dict):
        return {str(key): _sanitize_projection(item) for key, item in value.items()}
    return value
