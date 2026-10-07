from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

_PROVIDER_SCHEMA_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TurnStatus(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


class ToolCallStatus(StrEnum):
    PENDING = "pending"
    WAITING_APPROVAL = "waiting_approval"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    BLOCKED = "blocked"
    FAILED = "failed"
    REJECTED = "rejected"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ToolRisk(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}
)


@dataclass(frozen=True)
class ModelToolCall:
    call_id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.call_id.strip():
            raise ValueError("model tool call_id cannot be empty")
        if not self.name.strip():
            raise ValueError("model tool name cannot be empty")
        _ensure_json_serializable(self.arguments, label="model tool arguments")

    def to_dict(self) -> dict[str, Any]:
        return {
            "call_id": self.call_id,
            "name": self.name,
            "arguments": dict(self.arguments),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ModelToolCall:
        return cls(
            call_id=str(value["call_id"]),
            name=str(value["name"]),
            arguments=_dict(value.get("arguments")),
        )


@dataclass(frozen=True)
class ModelMessage:
    role: str
    content: str = ""
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[ModelToolCall, ...] = ()
    message_id: str | None = None

    def __post_init__(self) -> None:
        if self.role not in {"system", "user", "assistant", "tool"}:
            raise ValueError(f"unsupported model message role: {self.role}")
        if self.role == "tool" and not self.tool_call_id:
            raise ValueError("tool messages require tool_call_id")

    def to_dict(self) -> dict[str, Any]:
        value = {
            "role": self.role,
            "content": self.content,
            "name": self.name,
            "tool_call_id": self.tool_call_id,
            "tool_calls": [call.to_dict() for call in self.tool_calls],
        }
        if self.message_id is not None:
            value["message_id"] = self.message_id
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ModelMessage:
        return cls(
            role=str(value["role"]),
            content=str(value.get("content") or ""),
            name=_optional_str(value.get("name")),
            tool_call_id=_optional_str(value.get("tool_call_id")),
            tool_calls=tuple(
                ModelToolCall.from_dict(_dict(item))
                for item in _list(value.get("tool_calls"))
            ),
            message_id=_optional_str(value.get("message_id")),
        )


@dataclass(frozen=True)
class ConstraintRevision:
    version: int
    source_message_id: str
    content: str
    subject_tokens: tuple[str, ...]
    polarity: str
    status: str = "active"
    superseded_by: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "source_message_id": self.source_message_id,
            "content": self.content,
            "subject_tokens": list(self.subject_tokens),
            "polarity": self.polarity,
            "status": self.status,
            "superseded_by": self.superseded_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ConstraintRevision:
        return cls(
            version=int(value["version"]),
            source_message_id=str(value["source_message_id"]),
            content=str(value["content"]),
            subject_tokens=tuple(str(item) for item in _list(value.get("subject_tokens"))),
            polarity=str(value.get("polarity") or "unknown"),
            status=str(value.get("status") or "active"),
            superseded_by=_optional_str(value.get("superseded_by")),
        )


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    input_schema: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": dict(self.input_schema),
        }


@dataclass(frozen=True)
class ModelResponse:
    content: str = ""
    tool_calls: tuple[ModelToolCall, ...] = ()
    model: str = ""
    response_id: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    finish_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.content and not self.tool_calls:
            raise ValueError("model response must contain content or tool calls")
        if self.input_tokens < 0 or self.output_tokens < 0:
            raise ValueError("model token usage cannot be negative")
        call_ids = [call.call_id for call in self.tool_calls]
        if len(call_ids) != len(set(call_ids)):
            raise ValueError("model response contains duplicate tool call ids")

    def to_dict(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "tool_calls": [call.to_dict() for call in self.tool_calls],
            "model": self.model,
            "response_id": self.response_id,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "finish_reason": self.finish_reason,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ModelResponse:
        return cls(
            content=str(value.get("content") or ""),
            tool_calls=tuple(
                ModelToolCall.from_dict(_dict(item))
                for item in _list(value.get("tool_calls"))
            ),
            model=str(value.get("model") or ""),
            response_id=_optional_str(value.get("response_id")),
            input_tokens=int(value.get("input_tokens") or 0),
            output_tokens=int(value.get("output_tokens") or 0),
            finish_reason=_optional_str(value.get("finish_reason")),
        )


@dataclass(frozen=True)
class ModelGatewayStreamEvent:
    type: str
    delta: str = ""
    response: ModelResponse | None = None

    def __post_init__(self) -> None:
        if self.type not in {"delta", "completed"}:
            raise ValueError(f"unsupported model stream event type: {self.type}")
        if self.type == "delta" and not self.delta:
            raise ValueError("model delta event cannot be empty")
        if self.type == "completed" and self.response is None:
            raise ValueError("model completed event requires a response")


@dataclass(frozen=True)
class OutputContract:
    name: str
    schema: dict[str, Any]
    strict: bool = True
    max_repair_attempts: int = 1
    provider_native: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("output contract name cannot be empty")
        if self.provider_native and not _PROVIDER_SCHEMA_NAME_RE.fullmatch(self.name):
            raise ValueError(
                "provider-native output contract name must contain only letters, "
                "numbers, underscores, or hyphens and be at most 64 characters"
            )
        if self.max_repair_attempts < 0:
            raise ValueError("output contract max_repair_attempts cannot be negative")
        if not isinstance(self.schema, dict):
            raise ValueError("output contract schema must be a JSON object")
        _ensure_json_serializable(self.schema, label="output contract schema")
        try:
            Draft202012Validator.check_schema(self.schema)
        except SchemaError as error:
            raise ValueError("output contract schema is not valid Draft 2020-12") from error
        object.__setattr__(self, "schema", deepcopy(self.schema))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "schema": deepcopy(self.schema),
            "strict": self.strict,
            "max_repair_attempts": self.max_repair_attempts,
            "provider_native": self.provider_native,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> OutputContract:
        return cls(
            name=str(value["name"]),
            schema=_dict(value.get("schema")),
            strict=bool(value.get("strict", True)),
            max_repair_attempts=int(value.get("max_repair_attempts", 1)),
            provider_native=bool(value.get("provider_native", False)),
        )


@dataclass(frozen=True)
class AgentRequest:
    agent_id: str
    message: str
    actor_id: str = "user"
    session_id: str = "default"
    tenant_id: str = "default"
    user_id: str | None = None
    request_id: str = field(default_factory=lambda: str(uuid4()))
    instructions: tuple[str, ...] = ()
    module_names: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    output_contract: OutputContract | None = None

    def __post_init__(self) -> None:
        if not self.agent_id.strip():
            raise ValueError("agent_id cannot be empty")
        if not self.message.strip():
            raise ValueError("agent message cannot be empty")
        if not self.actor_id.strip() or not self.tenant_id.strip():
            raise ValueError("actor_id and tenant_id cannot be empty")
        if not self.request_id.strip():
            raise ValueError("request_id cannot be empty")
        _ensure_json_serializable(self.metadata, label="agent request metadata")

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "message": self.message,
            "actor_id": self.actor_id,
            "session_id": self.session_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "request_id": self.request_id,
            "instructions": list(self.instructions),
            "module_names": list(self.module_names),
            "metadata": dict(self.metadata),
            "output_contract": (
                None if self.output_contract is None else self.output_contract.to_dict()
            ),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentRequest:
        raw_output_contract = value.get("output_contract")
        return cls(
            agent_id=str(value["agent_id"]),
            message=str(value["message"]),
            actor_id=str(value.get("actor_id") or "user"),
            session_id=str(value.get("session_id") or "default"),
            tenant_id=str(value.get("tenant_id") or "default"),
            user_id=_optional_str(value.get("user_id")),
            request_id=str(value["request_id"]),
            instructions=tuple(str(item) for item in _list(value.get("instructions"))),
            module_names=tuple(str(item) for item in _list(value.get("module_names"))),
            metadata=_dict(value.get("metadata")),
            output_contract=(
                OutputContract.from_dict(_dict(raw_output_contract))
                if isinstance(raw_output_contract, dict)
                else None
            ),
        )


@dataclass(frozen=True)
class AgentRun:
    run_id: str
    request: AgentRequest
    status: RunStatus = RunStatus.PENDING
    version: int = 0
    step: int = 0
    model_calls: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    event_sequence: int = 0
    final_output: str | None = None
    error_type: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: str | None = None
    cost_usd: float = 0.0

    @classmethod
    def new(cls, request: AgentRequest, *, run_id: str | None = None) -> AgentRun:
        return cls(run_id=run_id or str(uuid4()), request=request)

    @property
    def tenant_id(self) -> str:
        return self.request.tenant_id

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_RUN_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "request": self.request.to_dict(),
            "status": self.status.value,
            "version": self.version,
            "step": self.step,
            "model_calls": self.model_calls,
            "tool_calls": self.tool_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "event_sequence": self.event_sequence,
            "final_output": self.final_output,
            "error_type": self.error_type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "lease_owner": self.lease_owner,
            "lease_token": self.lease_token,
            "lease_expires_at": self.lease_expires_at,
            "cost_usd": self.cost_usd,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentRun:
        return cls(
            run_id=str(value["run_id"]),
            request=AgentRequest.from_dict(_dict(value["request"])),
            status=RunStatus(str(value.get("status") or RunStatus.PENDING.value)),
            version=int(value.get("version") or 0),
            step=int(value.get("step") or 0),
            model_calls=int(value.get("model_calls") or 0),
            tool_calls=int(value.get("tool_calls") or 0),
            input_tokens=int(value.get("input_tokens") or 0),
            output_tokens=int(value.get("output_tokens") or 0),
            event_sequence=int(value.get("event_sequence") or 0),
            final_output=_optional_str(value.get("final_output")),
            error_type=_optional_str(value.get("error_type")),
            created_at=str(value.get("created_at") or utc_now_iso()),
            updated_at=str(value.get("updated_at") or utc_now_iso()),
            lease_owner=_optional_str(value.get("lease_owner")),
            lease_token=_optional_str(value.get("lease_token")),
            lease_expires_at=_optional_str(value.get("lease_expires_at")),
            cost_usd=float(value.get("cost_usd") or 0.0),
        )


@dataclass(frozen=True)
class AgentCheckpoint:
    run_id: str
    messages: tuple[ModelMessage, ...]
    pending_tool_calls: tuple[ModelToolCall, ...] = ()
    final_output: str | None = None
    version: int = 0
    updated_at: str = field(default_factory=utc_now_iso)
    compaction_count: int = 0
    compacted_message_count: int = 0
    last_estimated_input_tokens: int = 0
    output_repair_attempts: int = 0
    compaction_summary: str = ""
    pinned_messages: tuple[ModelMessage, ...] = ()
    constraint_revisions: tuple[ConstraintRevision, ...] = ()
    constraint_processed_message_ids: tuple[str, ...] = ()
    current_user_intent: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "messages": [message.to_dict() for message in self.messages],
            "pending_tool_calls": [call.to_dict() for call in self.pending_tool_calls],
            "final_output": self.final_output,
            "version": self.version,
            "updated_at": self.updated_at,
            "compaction_count": self.compaction_count,
            "compacted_message_count": self.compacted_message_count,
            "last_estimated_input_tokens": self.last_estimated_input_tokens,
            "output_repair_attempts": self.output_repair_attempts,
            "compaction_summary": self.compaction_summary,
            "pinned_messages": [message.to_dict() for message in self.pinned_messages],
            "constraint_revisions": [item.to_dict() for item in self.constraint_revisions],
            "constraint_processed_message_ids": list(self.constraint_processed_message_ids),
            "current_user_intent": self.current_user_intent,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentCheckpoint:
        return cls(
            run_id=str(value["run_id"]),
            messages=tuple(
                ModelMessage.from_dict(_dict(item)) for item in _list(value.get("messages"))
            ),
            pending_tool_calls=tuple(
                ModelToolCall.from_dict(_dict(item))
                for item in _list(value.get("pending_tool_calls"))
            ),
            final_output=_optional_str(value.get("final_output")),
            version=int(value.get("version") or 0),
            updated_at=str(value.get("updated_at") or utc_now_iso()),
            compaction_count=int(value.get("compaction_count") or 0),
            compacted_message_count=int(value.get("compacted_message_count") or 0),
            last_estimated_input_tokens=int(
                value.get("last_estimated_input_tokens") or 0
            ),
            output_repair_attempts=int(value.get("output_repair_attempts") or 0),
            compaction_summary=str(value.get("compaction_summary") or ""),
            pinned_messages=tuple(
                ModelMessage.from_dict(_dict(item))
                for item in _list(value.get("pinned_messages"))
            ),
            constraint_revisions=tuple(
                ConstraintRevision.from_dict(_dict(item))
                for item in _list(value.get("constraint_revisions"))
            ),
            constraint_processed_message_ids=tuple(
                str(item) for item in _list(value.get("constraint_processed_message_ids"))
            ),
            current_user_intent=str(value.get("current_user_intent") or ""),
        )


@dataclass(frozen=True)
class AgentTurn:
    turn_id: str
    run_id: str
    sequence: int
    status: TurnStatus
    response: ModelResponse | None = None
    error_type: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    @classmethod
    def new(cls, *, run_id: str, sequence: int) -> AgentTurn:
        return cls(
            turn_id=f"{run_id}:{sequence}",
            run_id=run_id,
            sequence=sequence,
            status=TurnStatus.STARTED,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "turn_id": self.turn_id,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "status": self.status.value,
            "response": None if self.response is None else self.response.to_dict(),
            "error_type": self.error_type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentTurn:
        raw_response = value.get("response")
        return cls(
            turn_id=str(value["turn_id"]),
            run_id=str(value["run_id"]),
            sequence=int(value["sequence"]),
            status=TurnStatus(str(value["status"])),
            response=(
                ModelResponse.from_dict(_dict(raw_response))
                if isinstance(raw_response, dict)
                else None
            ),
            error_type=_optional_str(value.get("error_type")),
            created_at=str(value.get("created_at") or utc_now_iso()),
            updated_at=str(value.get("updated_at") or utc_now_iso()),
        )


@dataclass(frozen=True)
class ToolCallRecord:
    call_id: str
    run_id: str
    tenant_id: str
    tool_name: str
    arguments: dict[str, Any]
    status: ToolCallStatus = ToolCallStatus.PENDING
    version: int = 0
    side_effects: bool = False
    idempotent: bool = True
    attempts: int = 0
    output: dict[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error_hash: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "call_id": self.call_id,
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "tool_name": self.tool_name,
            "arguments": dict(self.arguments),
            "status": self.status.value,
            "version": self.version,
            "side_effects": self.side_effects,
            "idempotent": self.idempotent,
            "attempts": self.attempts,
            "output": dict(self.output),
            "error_type": self.error_type,
            "error_hash": self.error_hash,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ToolCallRecord:
        return cls(
            call_id=str(value["call_id"]),
            run_id=str(value["run_id"]),
            tenant_id=str(value.get("tenant_id") or "default"),
            tool_name=str(value["tool_name"]),
            arguments=_dict(value.get("arguments")),
            status=ToolCallStatus(str(value.get("status") or ToolCallStatus.PENDING.value)),
            version=int(value.get("version") or 0),
            side_effects=bool(value.get("side_effects", False)),
            idempotent=bool(value.get("idempotent", True)),
            attempts=int(value.get("attempts") or 0),
            output=_dict(value.get("output")),
            error_type=_optional_str(value.get("error_type")),
            error_hash=_optional_str(value.get("error_hash")),
            created_at=str(value.get("created_at") or utc_now_iso()),
            updated_at=str(value.get("updated_at") or utc_now_iso()),
        )


@dataclass(frozen=True)
class ApprovalRecord:
    approval_id: str
    run_id: str
    call_id: str
    tenant_id: str
    status: ApprovalStatus = ApprovalStatus.PENDING
    reviewer_id: str | None = None
    reason: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    decided_at: str | None = None

    @classmethod
    def new(cls, *, run_id: str, call_id: str, tenant_id: str) -> ApprovalRecord:
        return cls(
            approval_id=str(uuid4()),
            run_id=run_id,
            call_id=call_id,
            tenant_id=tenant_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "run_id": self.run_id,
            "call_id": self.call_id,
            "tenant_id": self.tenant_id,
            "status": self.status.value,
            "reviewer_id": self.reviewer_id,
            "reason": self.reason,
            "created_at": self.created_at,
            "decided_at": self.decided_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ApprovalRecord:
        return cls(
            approval_id=str(value["approval_id"]),
            run_id=str(value["run_id"]),
            call_id=str(value["call_id"]),
            tenant_id=str(value.get("tenant_id") or "default"),
            status=ApprovalStatus(str(value.get("status") or ApprovalStatus.PENDING.value)),
            reviewer_id=_optional_str(value.get("reviewer_id")),
            reason=_optional_str(value.get("reason")),
            created_at=str(value.get("created_at") or utc_now_iso()),
            decided_at=_optional_str(value.get("decided_at")),
        )


@dataclass(frozen=True)
class AgentRunEvent:
    type: str
    run_id: str
    sequence: int
    tenant_id: str
    agent_id: str
    session_id: str
    data: dict[str, Any] = field(default_factory=dict)
    execution_id: str = ""
    event_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "type": self.type,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "execution_id": self.execution_id,
            "occurred_at": self.occurred_at,
            "data": dict(self.data),
        }


def _dict(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _list(value: object) -> list[object]:
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _ensure_json_serializable(value: object, *, label: str) -> None:
    try:
        json.dumps(value, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be JSON serializable") from error
