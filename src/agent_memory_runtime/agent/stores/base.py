from __future__ import annotations

from typing import Protocol

from agent_memory_runtime.agent.models import (
    AgentCheckpoint,
    AgentRun,
    AgentTurn,
    ApprovalRecord,
    ApprovalStatus,
    ToolCallRecord,
)


class AgentStateStore(Protocol):
    def transaction(self): ...

    def create_run(self, run: AgentRun) -> AgentRun: ...

    def get_run(self, run_id: str) -> AgentRun | None: ...

    def get_run_by_request(self, tenant_id: str, request_id: str) -> AgentRun | None: ...

    def claim_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> AgentRun | None: ...

    def renew_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_token: str,
        lease_seconds: float,
    ) -> bool: ...

    def update_run(
        self,
        run: AgentRun,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> AgentRun: ...

    def cancel_run(self, run_id: str, *, tenant_id: str) -> AgentRun: ...

    def save_checkpoint(
        self,
        checkpoint: AgentCheckpoint,
        *,
        expected_version: int | None,
        lease_token: str | None = None,
    ) -> AgentCheckpoint: ...

    def get_checkpoint(self, run_id: str) -> AgentCheckpoint | None: ...

    def save_turn(self, turn: AgentTurn, *, lease_token: str | None = None) -> AgentTurn: ...

    def get_turn(self, turn_id: str) -> AgentTurn | None: ...

    def list_turns(self, run_id: str) -> list[AgentTurn]: ...

    def create_tool_call(
        self, record: ToolCallRecord, *, lease_token: str | None = None
    ) -> ToolCallRecord: ...

    def get_tool_call(self, call_id: str) -> ToolCallRecord | None: ...

    def update_tool_call(
        self,
        record: ToolCallRecord,
        *,
        expected_version: int,
        lease_token: str | None = None,
    ) -> ToolCallRecord: ...

    def list_tool_calls(self, run_id: str) -> list[ToolCallRecord]: ...

    def create_approval(
        self, approval: ApprovalRecord, *, lease_token: str | None = None
    ) -> ApprovalRecord: ...

    def get_approval(self, approval_id: str) -> ApprovalRecord | None: ...

    def get_approval_for_call(self, call_id: str) -> ApprovalRecord | None: ...

    def decide_approval(
        self,
        approval_id: str,
        *,
        tenant_id: str,
        decision: ApprovalStatus,
        reviewer_id: str,
        reason: str | None,
    ) -> ApprovalRecord: ...
