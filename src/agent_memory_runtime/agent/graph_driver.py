"""LangGraph owns control flow; domain journals retain authority over tool effects and approvals."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from typing import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from opentelemetry.trace import Status, StatusCode

from agent_memory_runtime.telemetry import span


class ExecutionState(TypedDict):
    run_id: str
    route: str
    checkpoint_version: int


@asynccontextmanager
async def _saver(runtime):
    path = getattr(runtime.state_store, "path", None)
    if path is not None:
        # LangGraph's versioned persistence backend owns these tables. Keeping it
        # separate from the domain journal avoids table-name/migration collisions.
        async with AsyncSqliteSaver.from_conn_string(str(path) + ".langgraph") as saver:
            yield saver
    else:
        if not hasattr(runtime, "_graph_memory_saver"):
            runtime._graph_memory_saver = InMemorySaver()
        yield runtime._graph_memory_saver


async def drive_graph(runtime, run, *, factory, token, policy):
    from agent_memory_runtime.agent.runtime import _ToolProgress

    async def state():
        current = await runtime._load_run(run.run_id)
        checkpoint = await asyncio.to_thread(runtime.state_store.get_checkpoint, run.run_id)
        return _ToolProgress(run=current, checkpoint=checkpoint)

    def route(progress):
        checkpoint = progress.checkpoint
        target = (
            "finish"
            if checkpoint.final_output is not None
            else "tool"
            if checkpoint.pending_tool_calls
            else "model"
        )
        return {"route": target, "checkpoint_version": checkpoint.version}

    async def emit(generator):
        writer = get_stream_writer()
        async for event in generator:
            writer(event)

    async def prepare(_state):
        progress = await state()
        with span("agent.prepare"):
            await emit(
                runtime._prepare_graph(progress, factory=factory, token=token, policy=policy)
            )
        return route(progress)

    async def model(_state):
        token.raise_if_cancelled()
        progress = await state()
        # A domain checkpoint may have committed before a process died while
        # LangGraph was saving the node result. Never regenerate that model turn.
        if progress.checkpoint.pending_tool_calls or progress.checkpoint.final_output is not None:
            return route(progress)
        with span("agent.model"):
            await emit(runtime._model_step(progress, factory=factory, token=token, policy=policy))
        return route(progress)

    async def tool(_state):
        progress = await state()
        if not progress.checkpoint.pending_tool_calls:
            return route(progress)
        tools = runtime._resolve_tools(progress.run.request, policy)
        call = progress.checkpoint.pending_tool_calls[0]
        with span("agent.tool", attributes={"tool.name": call.name}) as current:
            async for event in runtime._process_tool_call(
                progress,
                call,
                tools=tools,
                policy=policy,
                factory=factory,
                token=token,
            ):
                if event.type in {"tool.blocked", "tool.rejected"} or (
                    event.type == "tool.completed" and event.data.get("status") != "succeeded"
                ):
                    current.set_status(Status(StatusCode.ERROR))
                    current.set_attribute("error.type", event.data.get("error_type") or "ToolFailure")
                get_stream_writer()(await runtime._publish(event))
        if progress.paused:
            interrupt({"run_id": run.run_id, "status": progress.run.status.value})
        return route(progress)

    async def finish(_state):
        progress = await state()
        with span("agent.finish"):
            await emit(
                runtime._complete(progress.run, progress.checkpoint.final_output, factory=factory)
            )
        return {"route": END}

    builder = StateGraph(ExecutionState)
    for name, function in (
        ("prepare", prepare),
        ("model", model),
        ("tool", tool),
        ("finish", finish),
    ):
        builder.add_node(name, function)
    builder.add_edge(START, "prepare")
    for name in ("prepare", "model", "tool"):
        builder.add_conditional_edges(
            name,
            lambda values: values["route"],
            {"model": "model", "tool": "tool", "finish": "finish"},
        )
    builder.add_edge("finish", END)
    identity = json.dumps([run.tenant_id, run.request.user_id, run.run_id])
    config = {
        "configurable": {"thread_id": hashlib.sha256(identity.encode()).hexdigest()},
        "recursion_limit": policy.max_model_calls + policy.max_tool_calls + 16,
    }
    async with _saver(runtime) as saver:
        graph = builder.compile(checkpointer=saver)
        snapshot = await graph.aget_state(config)
        if snapshot.next:
            inputs = Command(resume=True) if snapshot.interrupts else None
        else:
            inputs = {"run_id": run.run_id, "route": "prepare", "checkpoint_version": 0}
        async for event in graph.astream(inputs, config, stream_mode="custom", version="v1"):
            yield event
