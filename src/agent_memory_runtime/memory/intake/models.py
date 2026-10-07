from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agent_memory_runtime.domain.enums import (
    MemoryLevel,
    MemoryStatus,
    MemoryVisibility,
)
from agent_memory_runtime.domain.event import Event
from agent_memory_runtime.domain.memory import MemoryRecord


@dataclass(frozen=True)
class MemoryToolIdentity:
    actor_id: str
    agent_id: str
    session_id: str = "default"
    tenant_id: str = "default"
    user_id: str | None = None
    labels: tuple[str, ...] = ("private",)
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class MemoryToolResult:
    status: str
    action: str
    event: Event | None = None
    proposal_id: str | None = None
    audit_id: str | None = None
    memory_ids: tuple[str, ...] = ()
    tombstoned_memory_ids: tuple[str, ...] = ()
    archived_memory_ids: tuple[str, ...] = ()
    reason: str | None = None
    retryable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "action": self.action,
            "event_id": (
                self.proposal_id if self.event is None else self.event.event_id
            ),
            "event_kind": "memory.proposal" if self.event is None else self.event.kind,
            "proposal_id": self.proposal_id,
            "audit_id": self.audit_id,
            "memory_ids": list(self.memory_ids),
            "tombstoned_memory_ids": list(self.tombstoned_memory_ids),
            "archived_memory_ids": list(self.archived_memory_ids),
            "reason": self.reason,
            "retryable": self.retryable,
        }


@dataclass(frozen=True)
class DreamCheckpoint:
    last_processed_sequence: int = 0
    last_state_hash: str | None = None
    dream_version: str = "auto-dream-v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_processed_sequence": self.last_processed_sequence,
            "last_state_hash": self.last_state_hash,
            "dream_version": self.dream_version,
        }


@dataclass(frozen=True)
class DreamJob:
    job_id: str
    tenant_id: str
    user_id: str | None
    agent_id: str | None
    session_id: str | None
    status: str
    reason: str
    created_at: str
    updated_at: str
    available_at: str
    attempts: int = 0
    max_attempts: int = 3
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: str | None = None
    error_type: str | None = None
    error_hash: str | None = None
    mode: str = "deep"
    rerun_requested: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "status": self.status,
            "reason": self.reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "available_at": self.available_at,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "lease_owner": self.lease_owner,
            "lease_token": self.lease_token,
            "lease_expires_at": self.lease_expires_at,
            "error_type": self.error_type,
            "error_hash": self.error_hash,
            "mode": self.mode,
            "rerun_requested": self.rerun_requested,
        }


@dataclass(frozen=True)
class MemoryProposal:
    proposal_id: str
    source: str
    action: str
    target_memory_id: str | None
    subject_id: str
    key: str | None
    content: str
    memory_type: str
    visible_to: tuple[str, ...]
    confidence: float
    salience: float
    source_message_ids: tuple[str, ...] = ()
    source_memory_ids: tuple[str, ...] = ()
    evidence_text: str = ""
    reason: str = ""
    dream_run_id: str | None = None
    dream_version: str | None = None
    actor_id: str = "runtime"
    agent_id: str | None = None
    tenant_id: str = "default"
    user_id: str | None = None
    session_id: str = "default"
    labels: tuple[str, ...] = ("private",)
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    expected_version: int | None = None
    level: str = MemoryLevel.ATOM.value
    visibility: str = MemoryVisibility.PRIVATE.value
    priority: float = 0.5
    status: str = MemoryStatus.ACTIVE.value
    temperature: str | None = None
    decision_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "proposal_id": self.proposal_id,
            "source": self.source,
            "action": self.action,
            "target_memory_id": self.target_memory_id,
            "subject_id": self.subject_id,
            "key": self.key,
            "content": self.content,
            "memory_type": self.memory_type,
            "visible_to": list(self.visible_to),
            "confidence": self.confidence,
            "salience": self.salience,
            "source_message_ids": list(self.source_message_ids),
            "source_memory_ids": list(self.source_memory_ids),
            "evidence_text": self.evidence_text,
            "reason": self.reason,
            "dream_run_id": self.dream_run_id,
            "dream_version": self.dream_version,
            "actor_id": self.actor_id,
            "agent_id": self.agent_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "labels": list(self.labels),
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
            "expected_version": self.expected_version,
            "level": self.level,
            "visibility": self.visibility,
            "priority": self.priority,
            "status": self.status,
            "temperature": self.temperature,
            "decision_status": self.decision_status,
            # Compatibility keys for the pre-proposal Auto Dream API.
            "kind": self.memory_type,
            "evidence_event_ids": list(self.source_message_ids),
            "recommended_action": (
                "auto_apply"
                if self.action in {"create", "merge", "supersede"}
                else "review"
            ),
        }

    @property
    def kind(self) -> str:
        return self.memory_type

    @property
    def evidence_event_ids(self) -> tuple[str, ...]:
        return self.source_message_ids


DreamProposal = MemoryProposal


@dataclass(frozen=True)
class MemoryAuditLog:
    audit_id: str
    memory_id: str | None
    proposal_id: str
    action: str
    actor_id: str
    agent_id: str | None
    tenant_id: str
    user_id: str | None
    before_record: MemoryRecord | None
    after_record: MemoryRecord | None
    source_message_ids: tuple[str, ...]
    source_memory_ids: tuple[str, ...]
    evidence_text: str
    confidence: float
    reason: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "memory_id": self.memory_id,
            "proposal_id": self.proposal_id,
            "action": self.action,
            "actor_id": self.actor_id,
            "agent_id": self.agent_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "before_record": (
                None if self.before_record is None else self.before_record.to_dict()
            ),
            "after_record": (
                None if self.after_record is None else self.after_record.to_dict()
            ),
            "source_message_ids": list(self.source_message_ids),
            "source_memory_ids": list(self.source_memory_ids),
            "evidence_text": self.evidence_text,
            "confidence": self.confidence,
            "reason": self.reason,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class MemoryProposalResult:
    status: str
    action: str
    proposal: MemoryProposal
    memory: MemoryRecord | None = None
    before_record: MemoryRecord | None = None
    audit_log: MemoryAuditLog | None = None
    tombstoned_memory_ids: tuple[str, ...] = ()
    archived_memory_ids: tuple[str, ...] = ()
    reason: str | None = None
    retryable: bool = False

    @property
    def memory_ids(self) -> tuple[str, ...]:
        return () if self.memory is None else (self.memory.memory_id,)


@dataclass(frozen=True)
class AutoDreamReport:
    source_sequence_range: tuple[int, int] | None
    base_state_hash: str
    proposals: tuple[MemoryProposal, ...] = ()
    checkpoint: DreamCheckpoint = field(default_factory=DreamCheckpoint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_sequence_range": (
                None
                if self.source_sequence_range is None
                else list(self.source_sequence_range)
            ),
            "base_state_hash": self.base_state_hash,
            "proposals": [proposal.to_dict() for proposal in self.proposals],
            "checkpoint": self.checkpoint.to_dict(),
        }


@dataclass(frozen=True)
class AutoDreamRunReport:
    job: DreamJob | None
    analyzed: bool = False
    proposals: int = 0
    applied: int = 0
    review: int = 0
    rejected: int = 0
    conflicts: int = 0
    failed: int = 0
    checkpoint: DreamCheckpoint | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": None if self.job is None else self.job.to_dict(),
            "analyzed": self.analyzed,
            "proposals": self.proposals,
            "applied": self.applied,
            "review": self.review,
            "rejected": self.rejected,
            "conflicts": self.conflicts,
            "failed": self.failed,
            "checkpoint": None if self.checkpoint is None else self.checkpoint.to_dict(),
        }
