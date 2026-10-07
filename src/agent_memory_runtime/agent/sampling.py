"""One logical generation. Task/turn control and side effects stay in the runtime."""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import suppress
from dataclasses import dataclass
from time import perf_counter
from uuid import uuid4

from agent_memory_runtime.agent.errors import ModelProtocolError
from agent_memory_runtime.telemetry import span


@dataclass
class SamplingState:
    call_id: str
    execution_kind: str = "model"
    response: object | None = None
    usage_known: bool = False
    provider_record: dict | None = None


_capture = contextvars.ContextVar("sampling_provider_capture", default=None)
_identity = contextvars.ContextVar("sampling_model_call_id", default=None)


def report_provider_usage(record):
    capture = _capture.get()
    if capture:
        capture(dict(record))


async def cancellable(awaitable, token):
    operation = asyncio.ensure_future(awaitable)
    cancelled = asyncio.create_task(token.wait())
    try:
        done, _ = await asyncio.wait({operation, cancelled}, return_when=asyncio.FIRST_COMPLETED)
        if cancelled in done:
            operation.cancel()
            with suppress(asyncio.CancelledError):
                await operation
            token.raise_if_cancelled()
        return await operation
    finally:
        if not operation.done():
            operation.cancel()
            with suppress(asyncio.CancelledError):
                await operation
        cancelled.cancel()
        with suppress(asyncio.CancelledError):
            await cancelled


class SamplingExecutor:
    def __init__(self, metrics):
        self.metrics = metrics

    async def execute(
        self, state: SamplingState, *, gateway, messages, tools, metadata, token, timeout
    ):
        started = perf_counter()
        kind = state.execution_kind
        capture_token = _capture.set(lambda value: setattr(state, "provider_record", value))
        identity_token = _identity.set(state.call_id)
        self.metrics.increment(f"sampling.{kind}.attempts")
        with span(
            "sampling.call",
            attributes={
                "sampling.call_id": state.call_id,
                "sampling.execution_kind": kind,
                "sampling.attempt": 1,
                "agent.run_id": str(metadata.get("run_id", "")),
            },
        ) as current:
            try:
                token.raise_if_cancelled()
                async with asyncio.timeout(timeout):
                    stream = getattr(gateway, "stream", None)
                    if callable(stream):
                        completed = False
                        iterator = stream(messages=messages, tools=tools, metadata=metadata)
                        try:
                            while True:
                                try:
                                    event = await cancellable(anext(iterator), token)
                                except StopAsyncIteration:
                                    break
                                if getattr(event, "type", None) == "delta":
                                    delta = getattr(event, "delta", "")
                                    if completed or not isinstance(delta, str) or not delta:
                                        raise ModelProtocolError("Invalid generation delta")
                                    yield delta
                                elif getattr(event, "type", None) == "completed":
                                    if completed or getattr(event, "response", None) is None:
                                        raise ModelProtocolError("Invalid generation completion")
                                    completed = True
                                    state.response = event.response
                                else:
                                    raise ModelProtocolError("Unsupported generation event")
                        finally:
                            close = getattr(iterator, "aclose", None)
                            if close:
                                await close()
                        if not completed:
                            raise ModelProtocolError("Generation omitted completed response")
                    else:
                        state.response = await cancellable(
                            gateway.complete(messages=messages, tools=tools, metadata=metadata),
                            token,
                        )
                response = state.response
                input_tokens = getattr(response, "input_tokens", 0) or 0
                output_tokens = getattr(response, "output_tokens", 0) or 0
                state.usage_known = kind == "control" or bool(
                    (state.provider_record or {}).get("usageKnown") or input_tokens or output_tokens
                )
                current.set_attribute("gen_ai.response.model", str(getattr(response, "model", "")))
                current.set_attribute("sampling.usage_reported", state.usage_known)
                if kind == "model":
                    current.set_attribute("gen_ai.usage.input_tokens", input_tokens)
                    current.set_attribute("gen_ai.usage.output_tokens", output_tokens)
                    self.metrics.increment("sampling.model.input_tokens", input_tokens)
                    self.metrics.increment("sampling.model.output_tokens", output_tokens)
                self.metrics.increment(f"sampling.{kind}.completed")
            except BaseException:
                self.metrics.increment(f"sampling.{kind}.failed")
                record = state.provider_record or {}
                state.usage_known = bool(record.get("usageKnown"))
                if kind == "model" and not state.usage_known:
                    self.metrics.increment("sampling.model.usage_unknown")
                if kind == "model" and state.usage_known:
                    self.metrics.increment(
                        "sampling.model.input_tokens", record.get("inputTokens") or 0
                    )
                    self.metrics.increment(
                        "sampling.model.output_tokens", record.get("outputTokens") or 0
                    )
                current.set_attribute("sampling.usage_reported", state.usage_known)
                raise
            finally:
                _capture.reset(capture_token)
                _identity.reset(identity_token)
                self.metrics.observe(
                    f"sampling.{kind}.duration_ms", (perf_counter() - started) * 1000
                )


_sync_observers = []


def register_sampling_observer(observer):
    if observer not in _sync_observers:
        _sync_observers.append(observer)


def begin_provider_sampling(*, model, provider):
    return {
        "callId": uuid4().hex,
        "modelCallId": _identity.get(),
        "model": model,
        "provider": provider,
        "attempt": 1,
        "status": "failed",
        "usageKnown": False,
        "started": perf_counter(),
    }


def finish_provider_sampling(record):
    if record.get("recorded"):
        return
    record["recorded"] = True
    record["durationMs"] = (perf_counter() - record.pop("started")) * 1000
    report_provider_usage(record)
    for observer in tuple(_sync_observers):
        try:
            observer(dict(record))
        except Exception:
            # Telemetry must not change an already-executed provider outcome.
            pass


def sample_sync(invoke, *, model, provider):
    """Capture provider usage before business JSON parsing can reject the response."""
    record = begin_provider_sampling(model=model, provider=provider)
    with span(
        "llm.sampling",
        attributes={
            "gen_ai.request.model": model,
            "sampling.call_id": record["callId"],
            "sampling.execution_kind": "model",
        },
    ) as current:
        try:
            response = invoke()
            usage = getattr(response, "usage", None)
            record.update(
                status="completed",
                usageKnown=usage is not None,
                inputTokens=getattr(usage, "prompt_tokens", None),
                outputTokens=getattr(usage, "completion_tokens", None),
                responseId=getattr(response, "id", None),
            )
            current.set_attribute("sampling.usage_reported", usage is not None)
            if usage is not None:
                current.set_attribute("gen_ai.usage.input_tokens", record["inputTokens"] or 0)
                current.set_attribute("gen_ai.usage.output_tokens", record["outputTokens"] or 0)
            return response
        except BaseException as error:
            record["errorType"] = type(error).__name__
            current.set_attribute("sampling.usage_reported", False)
            raise
        finally:
            finish_provider_sampling(record)
