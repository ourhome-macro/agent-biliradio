from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from agent_memory_runtime.agent.errors import AgentPolicyError
from agent_memory_runtime.agent.models import AgentRequest, ToolRisk

_RISK_ORDER = {
    ToolRisk.LOW: 0,
    ToolRisk.MEDIUM: 1,
    ToolRisk.HIGH: 2,
    ToolRisk.CRITICAL: 3,
}


@dataclass(frozen=True)
class AgentPolicy:
    """Bounded execution policy applied to one agent run."""

    allowed_tools: frozenset[str] | None = None
    blocked_tools: frozenset[str] = frozenset()
    allow_side_effects: bool = True
    approval_risk_threshold: ToolRisk | None = ToolRisk.HIGH
    approval_required_tools: frozenset[str] = frozenset()
    max_steps: int = 8
    max_model_calls: int = 8
    max_tool_calls: int = 16
    max_input_tokens: int = 100_000
    max_output_tokens: int = 16_000
    max_total_tokens: int = 110_000
    run_timeout_seconds: float = 180.0
    model_timeout_seconds: float = 90.0
    tool_timeout_seconds: float = 30.0
    tool_max_attempts: int = 2
    retry_base_seconds: float = 0.25
    model_context_tokens: int = 128_000
    reserved_output_tokens: int = 4_096
    context_compaction_ratio: float = 0.8
    context_keep_recent_messages: int = 6
    context_summary_max_tokens: int = 2_048
    tool_output_max_tokens: int = 800
    tool_output_head_lines: int = 40
    tool_output_tail_lines: int = 20
    input_cost_per_million_usd: float | None = None
    output_cost_per_million_usd: float | None = None
    max_run_cost_usd: float | None = None
    max_repeated_tool_failures: int = 3

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "allowed_tools",
            None if self.allowed_tools is None else frozenset(self.allowed_tools),
        )
        object.__setattr__(self, "blocked_tools", frozenset(self.blocked_tools))
        object.__setattr__(
            self,
            "approval_required_tools",
            frozenset(self.approval_required_tools),
        )
        integer_limits = (
            self.max_repeated_tool_failures,
            self.max_steps,
            self.max_model_calls,
            self.max_tool_calls,
            self.max_input_tokens,
            self.max_output_tokens,
            self.max_total_tokens,
            self.tool_max_attempts,
            self.model_context_tokens,
            self.reserved_output_tokens,
            self.context_keep_recent_messages,
            self.context_summary_max_tokens,
            self.tool_output_max_tokens,
            self.tool_output_head_lines,
            self.tool_output_tail_lines,
        )
        if any(value <= 0 for value in integer_limits):
            raise ValueError("agent policy limits must be positive")
        if self.run_timeout_seconds <= 0 or self.model_timeout_seconds <= 0:
            raise ValueError("agent timeouts must be positive")
        if self.tool_timeout_seconds <= 0 or self.retry_base_seconds < 0:
            raise ValueError("tool timeout must be positive and retry delay non-negative")
        if self.reserved_output_tokens >= self.model_context_tokens:
            raise ValueError("reserved output tokens must be smaller than model context")
        if not 0 < self.context_compaction_ratio <= 1:
            raise ValueError("context_compaction_ratio must be in (0, 1]")
        prices = (self.input_cost_per_million_usd, self.output_cost_per_million_usd)
        if any(value is not None and value < 0 for value in prices):
            raise ValueError("model token prices cannot be negative")
        if self.max_run_cost_usd is not None and self.max_run_cost_usd <= 0:
            raise ValueError("max_run_cost_usd must be positive")

    def allows_tool(self, tool_name: str, *, side_effects: bool) -> bool:
        if tool_name in self.blocked_tools:
            return False
        if self.allowed_tools is not None and tool_name not in self.allowed_tools:
            return False
        if side_effects and not self.allow_side_effects:
            return False
        return True

    def authorize_tool(self, tool_name: str, *, side_effects: bool) -> None:
        if not self.allows_tool(tool_name, side_effects=side_effects):
            raise AgentPolicyError(f"tool {tool_name!r} is not allowed by the run policy")

    def requires_approval(
        self,
        tool_name: str,
        *,
        risk: ToolRisk,
        explicitly_required: bool = False,
    ) -> bool:
        if explicitly_required or tool_name in self.approval_required_tools:
            return True
        threshold = self.approval_risk_threshold
        if threshold is None:
            return False
        return _RISK_ORDER[risk] >= _RISK_ORDER[threshold]


class AgentPolicyResolver(Protocol):
    def resolve(self, request: AgentRequest) -> AgentPolicy:
        ...


@dataclass(frozen=True)
class StaticAgentPolicyResolver:
    policy: AgentPolicy = AgentPolicy()

    def resolve(self, request: AgentRequest) -> AgentPolicy:
        return self.policy
