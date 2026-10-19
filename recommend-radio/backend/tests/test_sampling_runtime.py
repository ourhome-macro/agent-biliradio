from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from agent_memory_runtime.agent.cancellation import CancellationToken
from agent_memory_runtime.agent.model_gateway import OpenAICompatibleModelGateway
from agent_memory_runtime.agent.models import AgentRequest, ModelResponse, ModelToolCall
from agent_memory_runtime.agent.observability import RuntimeMetrics
from agent_memory_runtime.agent.policy import AgentPolicy, StaticAgentPolicyResolver
from agent_memory_runtime.agent.runtime import BusinessAgentRuntime
from agent_memory_runtime.agent.sampling import SamplingExecutor, SamplingState
from agent_memory_runtime.config import LLMConfig


def test_control_steps_are_not_model_attempts():
    class Gateway:
        async def complete(self, **kwargs):
            return ModelResponse(content="done", model="router")

    metrics = RuntimeMetrics()

    async def execute():
        state = SamplingState("one", "control")
        async for _ in SamplingExecutor(metrics).execute(
            state,
            gateway=Gateway(),
            messages=(),
            tools=(),
            metadata={},
            token=CancellationToken(),
            timeout=1,
        ):
            pass
        assert state.response.content == "done"

    asyncio.run(execute())
    counters = metrics.snapshot()["counters"]
    assert counters["sampling.control.attempts"] == 1 and "sampling.model.attempts" not in counters


def test_repeated_invalid_tool_actions_stop_before_another_model_call():
    class Gateway:
        calls = 0

        async def complete(self, **kwargs):
            self.calls += 1
            # Reusing the exact call id must also be detected.
            return ModelResponse(
                content="", tool_calls=(ModelToolCall("same", "missing", {"x": 1}),)
            )

    gateway = Gateway()
    runtime = BusinessAgentRuntime(model_gateway=gateway)

    async def run():
        events = [
            event async for event in runtime.run(AgentRequest(agent_id="test", message="do it"))
        ]
        assert any(event.type == "run.no_progress" for event in events)
        assert events[-1].type == "run.failed"

    asyncio.run(run())
    assert gateway.calls == 3


def test_parser_failure_preserves_reported_tokens_and_cost(tmp_path, monkeypatch):
    from agent_trace_observer import PersistedAgentTraceObserver
    from database import init_db
    from full_trace import FullTrace, record_sampling
    from trace_metrics import summarize_traces

    from agent_memory_runtime.agent.sampling import register_sampling_observer

    db_path = tmp_path / "sampling.sqlite3"
    init_db(db_path)
    register_sampling_observer(record_sampling)
    monkeypatch.setenv("RECOMMEND_INPUT_COST_PER_MILLION_USD", "1")
    monkeypatch.setenv("RECOMMEND_OUTPUT_COST_PER_MILLION_USD", "2")
    completion = SimpleNamespace(
        id="paid",
        model="test-model",
        usage=SimpleNamespace(prompt_tokens=10, completion_tokens=7),
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content="",
                    tool_calls=[
                        SimpleNamespace(
                            id="call", function=SimpleNamespace(name="tool", arguments="not json")
                        )
                    ],
                ),
                finish_reason="tool_calls",
            )
        ],
    )

    class Client:
        chat = SimpleNamespace(completions=SimpleNamespace(create=lambda **_: completion))

    class CompleteGateway(OpenAICompatibleModelGateway):
        stream = None

    config = LLMConfig.for_provider("deepseek")
    runtime = BusinessAgentRuntime(
        observers=(PersistedAgentTraceObserver(str(db_path)),),
        model_gateway=CompleteGateway(config, client=Client()),
        policy_resolver=StaticAgentPolicyResolver(
            AgentPolicy(input_cost_per_million_usd=1, output_cost_per_million_usd=2)
        ),
    )

    async def run():
        events = [e async for e in runtime.run(AgentRequest(agent_id="test", message="test"))]
        failed = next(e for e in events if e.type == "sampling.failed")
        assert (
            failed.data["usage_known"]
            and failed.data["input_tokens"] == 10
            and failed.data["output_tokens"] == 7
        )
        run = await runtime.get_run(events[0].run_id, tenant_id="default")
        assert (run.input_tokens, run.output_tokens) == (10, 7)
        assert run.cost_usd == pytest.approx(0.000024)

    with FullTrace(str(db_path), trace_type="dialogue", user_id="legacy-owner"):
        asyncio.run(run())
    metrics = summarize_traces(str(db_path))["trace"]
    assert metrics["tokenUsage"]["input"] == 10 and metrics["tokenUsage"]["output"] == 7
    assert metrics["modelUsage"]["attempts"] == 1
    assert metrics["modelUsage"]["knownCostUsd"] == pytest.approx(0.000024)


def test_stream_protocol_errors_are_not_silently_accepted():
    class Gateway:
        async def stream(self, **kwargs):
            yield SimpleNamespace(type="completed", response=ModelResponse(content="done"))
            yield SimpleNamespace(type="delta", delta="late")

    async def execute():
        from agent_memory_runtime.agent.errors import ModelProtocolError

        with pytest.raises(ModelProtocolError):
            async for _ in SamplingExecutor(RuntimeMetrics()).execute(
                SamplingState("one"),
                gateway=Gateway(),
                messages=(),
                tools=(),
                metadata={},
                token=CancellationToken(),
                timeout=1,
            ):
                pass

    asyncio.run(execute())


def test_empty_paid_legacy_stream_keeps_usage_and_closes_provider(tmp_path, monkeypatch):
    from database import init_db
    from full_trace import FullTrace, record_sampling
    from trace_metrics import summarize_traces

    from agent_memory_runtime.agent.sampling import register_sampling_observer
    from agent_memory_runtime.exceptions import LLMResponseError
    from agent_memory_runtime.llm.openai_compatible import OpenAICompatibleChatClient

    db_path = tmp_path / "stream.sqlite3"
    init_db(db_path)
    register_sampling_observer(record_sampling)
    monkeypatch.setenv("RECOMMEND_INPUT_COST_PER_MILLION_USD", "1")
    monkeypatch.setenv("RECOMMEND_OUTPUT_COST_PER_MILLION_USD", "2")

    class Stream:
        closed = False

        def __iter__(self):
            yield SimpleNamespace(
                id="paid-empty",
                model="test",
                choices=[],
                usage=SimpleNamespace(prompt_tokens=10, completion_tokens=3),
            )

        def close(self):
            self.closed = True

    stream = Stream()
    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=lambda **_: stream))
    )
    gateway = OpenAICompatibleChatClient(LLMConfig.for_provider("deepseek"), client=client)
    with FullTrace(str(db_path), trace_type="dialogue", user_id="legacy-owner"):
        with pytest.raises(LLMResponseError):
            list(gateway.stream_complete(system_prompt="test", user_prompt="test"))
    metrics = summarize_traces(str(db_path))["trace"]
    assert stream.closed and metrics["tokenUsage"]["input"] == 10
    assert metrics["tokenUsage"]["output"] == 3 and metrics["modelUsage"]["attempts"] == 1
    assert metrics["modelUsage"]["knownCostUsd"] == pytest.approx(0.000016)
