from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Any

from full_trace import FullTrace, current_trace_id


class PersistedAgentTraceObserver:
    """BusinessAgentRuntime observer that projects run events into full traces."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._traces: dict[str, FullTrace] = {}
        self._starts: dict[tuple[str, str, str], float] = {}

    async def on_event(self, event: Any) -> None:
        await asyncio.to_thread(self._record, event)

    def _record(self, event: Any) -> None:
        run_id = str(event.run_id)
        trace = self._traces.get(run_id)
        if trace is None:
            try:
                trace = FullTrace.resume(self.db_path, f"agent-run:{run_id}")
            except KeyError:
                trace = self._new_trace(event)
            self._traces[run_id] = trace

        self._record_event(trace, event, run_id)

    def _new_trace(self, event):
        run_id = str(event.run_id)
        return FullTrace(
            self.db_path,
            trace_type="agent_run",
            trace_id=f"agent-run:{run_id}",
            parent_trace_id=current_trace_id(),
            user_id="",
            session_id=str(event.session_id),
            request_id=str(getattr(event, "execution_id", "") or run_id),
            attributes={
                "tenantId": str(event.tenant_id),
                "agentId": str(event.agent_id),
                "runId": run_id,
            },
        )

    def _record_event(self, trace, event, run_id):
        with trace.batch():
            event_type = str(event.type)
            data = dict(getattr(event, "data", {}) or {})
            trace.event(
                event_type,
                {
                    "sourceEventId": str(getattr(event, "event_id", "")),
                    "sourceSequence": int(getattr(event, "sequence", 0)),
                    **data,
                },
            )

            if event_type in {"model.started", "tool.started"}:
                identifier = str(data.get("model_call_id") or data.get("call_id") or data.get("turn") or event.sequence)
                self._starts[(run_id, event_type.split(".", 1)[0], identifier)] = perf_counter()
            elif event_type in {"model.completed", "tool.completed", "sampling.failed"}:
                kind = "model" if event_type == "sampling.failed" else event_type.split(".", 1)[0]
                identifier = str(data.get("model_call_id") or data.get("call_id") or data.get("turn") or event.sequence)
                started = self._starts.pop((run_id, kind, identifier), None)
                duration_ms = 0.0 if started is None else (perf_counter() - started) * 1000
                trace.record_span(
                    event_type,
                    duration_ms,
                    kind=kind,
                    status=(
                        "failed"
                        if data.get("error_type") or data.get("status") == "failed"
                        else "completed"
                    ),
                    outputs=data,
                    metrics={
                        "inputTokens": int(data.get("input_tokens") or 0),
                        "outputTokens": int(data.get("output_tokens") or 0),
                        "attempts": int(data.get("attempts") or 0),
                        "executionKind": data.get("execution_kind"),
                        "usageKnown": data.get("usage_known"),
                        "costUsd": data.get("cost_usd"),
                    },
                    error_type=(None if not data.get("error_type") else str(data["error_type"])),
                )

            if event_type in {"run.completed", "run.failed", "run.cancelled"}:
                status = {
                    "run.completed": "completed",
                    "run.failed": "failed",
                    "run.cancelled": "cancelled",
                }[event_type]
                trace.finish(
                    status,
                    attributes={
                        "modelCalls": int(data.get("model_calls") or 0),
                        "toolCalls": int(data.get("tool_calls") or 0),
                        "inputTokens": int(data.get("input_tokens") or 0),
                        "outputTokens": int(data.get("output_tokens") or 0),
                        "costUsd": float(data.get("cost_usd") or 0.0),
                    },
                    error_type=(None if not data.get("error_type") else str(data["error_type"])),
                )
                self._traces.pop(run_id, None)
