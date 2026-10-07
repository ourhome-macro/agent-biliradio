"""Music operations share the LangGraph agent, policy and durable tool journal."""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import json
from functools import wraps
from dataclasses import dataclass
from typing import Callable
from pathlib import Path
from uuid import uuid4

from agent_memory_runtime.agent.models import AgentRequest, ModelResponse, ModelToolCall
from agent_memory_runtime.agent.policy import AgentPolicy, StaticAgentPolicyResolver
from agent_memory_runtime.agent.runtime import BusinessAgentRuntime
from agent_memory_runtime.agent.tool_runtime import AgentFunctionTool
from agent_memory_runtime.memory.stores import SQLiteStoreBundle
from agent_memory_runtime.runtime import AgentMemoryRuntime
from agent_memory_runtime.telemetry import span
from agent_memory_runtime.tools.registry import ToolRegistry
from job_errors import JobPermanentFailure, JobReconciliationRequired

current_job_id = contextvars.ContextVar("music_job_id", default=None)
_depth = contextvars.ContextVar("music_agent_depth", default=0)


@dataclass(frozen=True)
class OperationStage:
    name: str
    action: Callable[[dict], dict]
    idempotent: bool = False
    side_effects: bool = True


def _value(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(k): _value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_value(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"Operation input is not serializable: {type(value).__name__}")


def music_operation(name: str):
    def decorate(function):
        signature = inspect.signature(function)

        @wraps(function)
        def execute(self, *args, **kwargs):
            bound = signature.bind(self, *args, **kwargs)
            bound.apply_defaults()
            payload = {
                k: _value(v)
                for k, v in bound.arguments.items()
                if k not in {"self", "progress", "full_trace", "parent_trace_id"}
            }
            return execute_operation(
                self, name, payload, action=lambda: function(self, *args, **kwargs)
            )

        return execute

    return decorate


def execute_operation(service, name, payload, *, action=None, stages=None):
    job_id = current_job_id.get() if _depth.get() == 0 else None
    request_id = f"{job_id}:{name}" if job_id else f"{name}:{uuid4().hex}"
    depth_token = _depth.set(_depth.get() + 1)
    try:
        with span("music." + name):
            return asyncio.run(
                _execute(service, name, _value(payload), request_id, job_id, action, stages=stages)
            )
    finally:
        _depth.reset(depth_token)


async def _execute(service, name, payload, request_id, job_id, action, *, stages=None):
    bundle = SQLiteStoreBundle(Path(service.db_path).with_name("agent-runs.sqlite3"))
    store = bundle.agent_state_store
    is_workflow = stages is not None
    stages = tuple(stages or (OperationStage(name, lambda _: action()),))
    if len({stage.name for stage in stages}) != len(stages):
        raise ValueError("Workflow stage names must be unique")
    call_ids = [
        hashlib.sha256(f"{service.user_id}:{request_id}:{stage.name}".encode()).hexdigest()
        for stage in stages
    ]
    tool_names = [f"radio_{name}_{stage.name}" for stage in stages]
    if not is_workflow:
        # Preserve the original single-operation journal identity across upgrades.
        call_ids = [hashlib.sha256(f"{service.user_id}:{request_id}".encode()).hexdigest()]
        tool_names = [f"radio_{name}"]

    def stage_input(index):
        if index == 0:
            return payload
        previous = store.get_tool_call(call_ids[index - 1])
        if previous is None or previous.status.value != "succeeded":
            raise RuntimeError("Previous workflow stage has not committed")
        return previous.output

    def stage_arguments(index):
        if not is_workflow:
            return payload
        # The model/control plane carries references, never entire candidate pools.
        # The complete result remains authoritative in the durable tool journal.
        digest = hashlib.sha256(
            json.dumps(stage_input(index), sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        return {"inputCallId": call_ids[index - 1] if index else "request", "inputHash": digest}

    class OperationGateway:
        execution_kind = "control"
        # A deterministic router, not a second LLM. Business model calls stay in
        # the operation and are traced independently with their actual provider.
        async def complete(self, *, messages, tools, metadata):
            for index, stage in enumerate(stages):
                record = store.get_tool_call(call_ids[index])
                if record is None or record.status.value != "succeeded":
                    return ModelResponse(
                        content="",
                        model="music-operation-router",
                        tool_calls=(
                            ModelToolCall(
                                call_ids[index], tool_names[index], stage_arguments(index)
                            ),
                        ),
                    )
            record = store.get_tool_call(call_ids[-1])
            return ModelResponse(
                content=json.dumps(record.output, ensure_ascii=False),
                model="music-operation-router",
            )

    def handler_for(index, stage):
        def handler(arguments, context):
            if arguments != stage_arguments(index):
                raise ValueError("Operation input changed across the execution boundary")
            from full_trace import _CURRENT_TRACE_ID

            trace_token = _CURRENT_TRACE_ID.set("agent-run:" + context.run_id)
            try:
                with span("music.stage." + stage.name):
                    return stage.action(stage_input(index))
            finally:
                _CURRENT_TRACE_ID.reset(trace_token)

        return handler

    registry = ToolRegistry()
    for index, stage in enumerate(stages):
        registry.register(
            AgentFunctionTool(
                tool_names[index],
                handler_for(index, stage),
                side_effects=stage.side_effects,
                idempotent=stage.idempotent,
                input_schema={"type": "object"},
            )
        )
    memory = AgentMemoryRuntime(
        event_store=bundle.event_store,
        memory_store=bundle.memory_store,
        snapshot_store=bundle.snapshot_store,
        audit_store=bundle.audit_store,
        tombstone_store=bundle.tombstone_store,
        transaction_manager=bundle,
    )
    from agent_trace_observer import PersistedAgentTraceObserver

    runtime = BusinessAgentRuntime(
        model_gateway=OperationGateway(),
        memory_runtime=memory,
        state_store=store,
        tool_registry=registry,
        observers=(PersistedAgentTraceObserver(str(service.db_path)),),
        policy_resolver=StaticAgentPolicyResolver(
            AgentPolicy(
                allowed_tools=frozenset(tool_names),
                approval_risk_threshold=None,
                max_steps=len(stages) + 1,
                max_model_calls=len(stages) + 1,
                max_tool_calls=len(stages),
                tool_max_attempts=3,
                run_timeout_seconds=220,
                tool_timeout_seconds=210,
            )
        ),
    )
    request = AgentRequest(
        agent_id="recommend-radio",
        tenant_id="recommend-radio",
        user_id=service.user_id,
        actor_id=service.user_id,
        session_id=str(payload.get("session_id") or "music"),
        request_id=request_id,
        message=json.dumps(payload, ensure_ascii=False),
        metadata={"operation": name},
    )
    result = None
    failure = None
    linked = False
    async for event in runtime.run(request):
        if job_id and not linked:
            from database import get_connection

            with get_connection(service.db_path) as conn:
                conn.execute(
                    "UPDATE durable_jobs SET agent_run_id=? WHERE job_id=? "
                    "AND agent_run_id IS NULL",
                    (event.run_id, job_id),
                )
            linked = True
        if event.type == "run.completed":
            result = json.loads(event.data["output"])
        if event.type == "tool.reconciliation_required":
            failure = JobReconciliationRequired("Agent stage requires outcome reconciliation")
        elif event.type in {"run.failed", "run.cancelled"}:
            failure = JobPermanentFailure(f"Agent operation stopped: {event.type}")
    if failure:
        raise failure
    if result is not None:
        return result
    raise RuntimeError("Agent operation is awaiting approval or reconciliation")
