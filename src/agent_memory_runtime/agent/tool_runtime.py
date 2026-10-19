from __future__ import annotations

import asyncio
import inspect
import json
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any

from agent_memory_runtime.agent.cancellation import CancellationToken
from agent_memory_runtime.agent.errors import (
    AgentLeaseLostError,
    AgentPolicyError,
    AgentReconciliationRequired,
)
from agent_memory_runtime.agent.models import (
    AgentRequest,
    ModelToolCall,
    ToolCallRecord,
    ToolCallStatus,
    ToolDefinition,
    ToolRisk,
)
from agent_memory_runtime.agent.policy import AgentPolicy
from agent_memory_runtime.agent.stores.base import AgentStateStore
from agent_memory_runtime.audit.hashing import secure_hash
from agent_memory_runtime.tools.base import Tool

_IN_FLIGHT_SYNC_CALLS: set[asyncio.Task] = set()


@dataclass(frozen=True)
class ToolExecutionContext:
    """Stable context passed to tools; call_id is the external idempotency key."""

    call_id: str
    run_id: str
    request: AgentRequest
    attempt: int
    cancellation_token: CancellationToken | None = None
    stop_requested: threading.Event = field(default_factory=threading.Event)


AgentToolHandler = Callable[
    [dict[str, Any], ToolExecutionContext],
    dict[str, Any] | Awaitable[dict[str, Any]],
]
AgentToolCompensator = Callable[
    [dict[str, Any], ToolExecutionContext],
    dict[str, Any] | Awaitable[dict[str, Any]],
]


@dataclass(frozen=True)
class AgentFunctionTool:
    name: str
    handler: AgentToolHandler
    description: str = ""
    input_schema: dict[str, Any] | None = None
    side_effects: bool = False
    idempotent: bool = True
    risk_level: ToolRisk | None = None
    requires_approval: bool = False
    compensator: AgentToolCompensator | None = None

    async def execute(
        self,
        arguments: dict[str, Any],
        context: ToolExecutionContext,
    ) -> dict[str, Any]:
        result = await _invoke_callable(self.handler, arguments, context)
        return _normalize_output(result)

    async def compensate(
        self,
        output: dict[str, Any],
        context: ToolExecutionContext,
    ) -> dict[str, Any]:
        if self.compensator is None:
            raise AgentPolicyError(f"tool {self.name!r} does not support compensation")
        result = await _invoke_callable(self.compensator, output, context)
        return _normalize_output(result)


class ReliableToolRuntime:
    def __init__(self, *, state_store: AgentStateStore) -> None:
        self.state_store = state_store

    async def execute(
        self,
        record: ToolCallRecord,
        *,
        tool: Tool,
        request: AgentRequest,
        policy: AgentPolicy,
        cancellation_token: CancellationToken | None = None,
    ) -> ToolCallRecord:
        if record.status is ToolCallStatus.SUCCEEDED:
            return record
        if record.status is ToolCallStatus.RECONCILIATION_REQUIRED:
            raise AgentReconciliationRequired('tool outcome must be reconciled before execution')
        policy.authorize_tool(record.tool_name, side_effects=record.side_effects)
        try:
            validate_tool_arguments(record.arguments, tool_input_schema(tool))
        except AgentPolicyError as error:
            return await self._store_error(record, error, status=ToolCallStatus.BLOCKED)

        if (
            record.status is ToolCallStatus.EXECUTING
            and record.side_effects
            and not record.idempotent
        ):
            return await self._update(
                replace(
                    record,
                    status=ToolCallStatus.RECONCILIATION_REQUIRED,
                    error_type="UnknownSideEffectOutcome",
                    error_hash=secure_hash(
                        {"type": "UnknownSideEffectOutcome", "call_id": record.call_id}
                    ),
                )
            )

        maximum_attempts = policy.tool_max_attempts if record.idempotent else 1
        current = record
        while current.attempts < maximum_attempts:
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            current = await self._update(
                replace(
                    current,
                    status=ToolCallStatus.EXECUTING,
                    attempts=current.attempts + 1,
                    error_type=None,
                    error_hash=None,
                )
            )
            context = ToolExecutionContext(
                call_id=current.call_id,
                run_id=current.run_id,
                request=request,
                attempt=current.attempts,
                cancellation_token=cancellation_token,
            )
            try:
                async with asyncio.timeout(policy.tool_timeout_seconds):
                    output = await _invoke_tool(tool, current.arguments, context)
                return await self._update(
                    replace(
                        current,
                        status=ToolCallStatus.SUCCEEDED,
                        output=output,
                        error_type=None,
                        error_hash=None,
                    )
                )
            except AgentLeaseLostError:
                raise
            except AgentReconciliationRequired:
                # A synchronous tool may still be running after timeout/cancel.
                # Even idempotent calls must not overlap an unobserved attempt.
                await asyncio.shield(self._mark_reconciliation(current))
                raise
            except asyncio.CancelledError:
                if current.side_effects and not current.idempotent:
                    await asyncio.shield(self._mark_reconciliation(current))
                    raise AgentReconciliationRequired(
                        "non-idempotent tool was interrupted with an unknown outcome"
                    ) from None
                raise
            except Exception as error:
                if current.side_effects and not current.idempotent:
                    return await self._mark_reconciliation(current, error=error)
                if current.attempts >= maximum_attempts:
                    return await self._store_error(
                        current,
                        error,
                        status=ToolCallStatus.FAILED,
                    )
                delay = policy.retry_base_seconds * (2 ** (current.attempts - 1))
                if delay > 0:
                    await asyncio.sleep(delay)
        return current

    async def _mark_reconciliation(
        self,
        record: ToolCallRecord,
        *,
        error: Exception | None = None,
    ) -> ToolCallRecord:
        error_type = type(error).__name__ if error is not None else "InterruptedSideEffect"
        return await self._update(
            replace(
                record,
                status=ToolCallStatus.RECONCILIATION_REQUIRED,
                error_type=error_type,
                error_hash=secure_hash(
                    {
                        "type": error_type,
                        "call_id": record.call_id,
                    }
                ),
            )
        )

    async def compensate(
        self,
        record: ToolCallRecord,
        *,
        tool: Tool,
        request: AgentRequest,
        timeout_seconds: float,
    ) -> ToolCallRecord:
        if record.status is not ToolCallStatus.SUCCEEDED:
            raise AgentPolicyError("only a succeeded tool call can be compensated")
        compensate = getattr(tool, "compensate", None)
        if not callable(compensate):
            raise AgentPolicyError(f"tool {record.tool_name!r} does not support compensation")
        context = ToolExecutionContext(
            call_id=record.call_id,
            run_id=record.run_id,
            request=request,
            attempt=record.attempts,
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                await _invoke_callable(compensate, dict(record.output), context)
            return await self._update(
                replace(record, status=ToolCallStatus.COMPENSATED)
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return await self._store_error(
                record,
                error,
                status=ToolCallStatus.COMPENSATION_FAILED,
            )

    async def _store_error(
        self,
        record: ToolCallRecord,
        error: Exception,
        *,
        status: ToolCallStatus,
    ) -> ToolCallRecord:
        return await self._update(
            replace(
                record,
                status=status,
                error_type=type(error).__name__,
                error_hash=secure_hash(
                    {"type": type(error).__name__, "message": str(error)}
                ),
            )
        )

    async def _update(self, record: ToolCallRecord) -> ToolCallRecord:
        return await asyncio.to_thread(
            self.state_store.update_tool_call,
            record,
            expected_version=record.version,
        )


def tool_definition(tool: Tool) -> ToolDefinition:
    return ToolDefinition(
        name=tool.name,
        description=str(getattr(tool, "description", "")),
        input_schema=tool_input_schema(tool),
    )


def tool_input_schema(tool: Tool) -> dict[str, Any]:
    value = getattr(tool, "input_schema", None)
    if value is None:
        return {"type": "object", "additionalProperties": True}
    if not isinstance(value, dict):
        raise AgentPolicyError(f"tool {tool.name!r} input_schema must be an object")
    return {str(key): item for key, item in value.items()}


def tool_side_effects(tool: Tool) -> bool:
    return bool(getattr(tool, "side_effects", False))


def tool_idempotent(tool: Tool) -> bool:
    default = not tool_side_effects(tool)
    value = getattr(tool, "idempotent", default)
    return default if value is None else bool(value)


def tool_risk(tool: Tool) -> ToolRisk:
    default = ToolRisk.HIGH if tool_side_effects(tool) else ToolRisk.LOW
    value = getattr(tool, "risk_level", None) or default
    try:
        return value if isinstance(value, ToolRisk) else ToolRisk(str(value))
    except ValueError as error:
        raise AgentPolicyError(f"tool {tool.name!r} has an invalid risk level") from error


def tool_requires_approval(tool: Tool) -> bool:
    return bool(getattr(tool, "requires_approval", False))


def new_tool_call_record(
    call: ModelToolCall,
    *,
    run_id: str,
    tenant_id: str,
    tool: Tool,
) -> ToolCallRecord:
    return ToolCallRecord(
        call_id=call.call_id,
        run_id=run_id,
        tenant_id=tenant_id,
        tool_name=call.name,
        arguments=dict(call.arguments),
        side_effects=tool_side_effects(tool),
        idempotent=tool_idempotent(tool),
    )


def validate_tool_arguments(arguments: dict[str, Any], schema: dict[str, Any]) -> None:
    """Validate the declared dialect completely; never retrieve remote schemas."""
    from jsonschema import Draft202012Validator, FormatChecker, validators
    from jsonschema.exceptions import SchemaError, ValidationError
    from referencing import Registry

    checker = FormatChecker()

    def strict_format(validator, name, value, subschema):
        if name not in checker.checkers:
            yield ValidationError(f'format checker {name!r} is unavailable')
        else:
            yield from Draft202012Validator.VALIDATORS['format'](
                validator, name, value, subschema
            )

    validator_type = validators.extend(Draft202012Validator, {'format': strict_format})
    try:
        if schema.get("$schema", "https://json-schema.org/draft/2020-12/schema") not in {
            "https://json-schema.org/draft/2020-12/schema",
            "https://json-schema.org/draft/2020-12/schema#",
        }:
            raise AgentPolicyError("tool schemas must use JSON Schema draft 2020-12")
        Draft202012Validator.check_schema(schema)
        validator_type(
            schema, registry=Registry(), format_checker=checker
        ).validate(arguments)
    except (SchemaError, ValidationError) as error:
        raise AgentPolicyError(f"invalid tool arguments or schema: {error.message}") from error
    except AgentPolicyError:
        raise
    except Exception as error:
        # An unresolved reference is a configuration failure, not permission to
        # skip validation. Registry() has no network retrieval callback.
        raise AgentPolicyError("tool schema contains an unresolvable reference") from error


async def _invoke_tool(
    tool: Tool,
    arguments: dict[str, Any],
    context: ToolExecutionContext,
) -> dict[str, Any]:
    execute = getattr(tool, "execute", None)
    if callable(execute):
        result = await _invoke_callable(execute, dict(arguments), context)
    else:
        result = await _invoke_callable(tool.run, dict(arguments))
    return _normalize_output(result)


async def _invoke_callable(callable_: Callable[..., object], *args: object) -> object:
    if inspect.iscoroutinefunction(callable_):
        return await callable_(*args)
    task = asyncio.create_task(asyncio.to_thread(callable_, *args))
    _IN_FLIGHT_SYNC_CALLS.add(task)
    task.add_done_callback(_finish_sync_call)
    try:
        result = await asyncio.shield(task)
    except asyncio.CancelledError:
        for argument in args:
            if isinstance(argument, ToolExecutionContext):
                argument.stop_requested.set()
        if not task.done():
            raise AgentReconciliationRequired(
                "synchronous tool is still running after cancellation or timeout"
            ) from None
        raise
    if inspect.isawaitable(result):
        return await result
    return result


def _finish_sync_call(task: asyncio.Task) -> None:
    _IN_FLIGHT_SYNC_CALLS.discard(task)
    if not task.cancelled():
        task.exception()  # Retrieve a late failure without permitting another attempt.


def _normalize_output(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AgentPolicyError("tool output must be a JSON object")
    output = {str(key): item for key, item in value.items()}
    try:
        json.dumps(output, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError) as error:
        raise AgentPolicyError("tool output must be JSON serializable") from error
    return output
