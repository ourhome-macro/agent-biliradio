from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any, Protocol

from agent_memory_runtime.agent.errors import ModelProtocolError
from agent_memory_runtime.agent.models import (
    ModelGatewayStreamEvent,
    ModelMessage,
    ModelResponse,
    ModelToolCall,
    ToolDefinition,
)
from agent_memory_runtime.config import LLMConfig
from agent_memory_runtime.agent.sampling import sample_sync, begin_provider_sampling, finish_provider_sampling
from agent_memory_runtime.exceptions import (
    LLMConfigurationError,
    LLMRequestError,
    LLMResponseError,
)
from agent_memory_runtime.llm.models import ChatClient


class ModelGateway(Protocol):
    """Provider-neutral async model and tool-calling contract."""

    async def complete(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> ModelResponse: ...


class StreamingModelGateway(ModelGateway, Protocol):
    def stream(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[ModelGatewayStreamEvent]: ...


class LegacyChatModelGateway:
    """Adapts the v0.2 text-only ChatClient to the new async gateway contract."""

    def __init__(self, client: ChatClient) -> None:
        self.client = client

    async def complete(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> ModelResponse:
        if tools:
            raise ModelProtocolError(
                "the legacy ChatClient adapter cannot perform model-directed tool calls"
            )
        system_prompt = "\n".join(
            message.content for message in messages if message.role == "system"
        )
        conversation = "\n".join(
            f"{message.role}: {message.content}" for message in messages if message.role != "system"
        )
        response = await asyncio.to_thread(
            self.client.complete,
            system_prompt=system_prompt,
            user_prompt=conversation,
        )
        return ModelResponse(
            content=response.content,
            model=response.model,
            response_id=response.response_id,
            input_tokens=response.input_tokens or 0,
            output_tokens=response.output_tokens or 0,
            finish_reason="stop",
        )

    async def stream(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[ModelGatewayStreamEvent]:
        if tools:
            raise ModelProtocolError(
                "the legacy ChatClient adapter cannot perform model-directed tool calls"
            )
        stream_complete = getattr(self.client, "stream_complete", None)
        if not callable(stream_complete):
            response = await self.complete(messages=messages, tools=tools, metadata=metadata)
            yield ModelGatewayStreamEvent(type="delta", delta=response.content)
            yield ModelGatewayStreamEvent(type="completed", response=response)
            return
        system_prompt, conversation = _legacy_prompts(messages)
        iterator = iter(
            await asyncio.to_thread(
                stream_complete,
                system_prompt=system_prompt,
                user_prompt=conversation,
            )
        )
        content: list[str] = []
        model = ""
        response_id: str | None = None
        input_tokens = 0
        output_tokens = 0
        while True:
            present, event = await asyncio.to_thread(_next_item, iterator)
            if not present:
                break
            if event.type == "token" and event.delta:
                content.append(event.delta)
                yield ModelGatewayStreamEvent(type="delta", delta=event.delta)
            model = event.model or model
            response_id = event.response_id or response_id
            input_tokens = event.input_tokens or input_tokens
            output_tokens = event.output_tokens or output_tokens
        response = ModelResponse(
            content="".join(content),
            model=model,
            response_id=response_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            finish_reason="stop",
        )
        yield ModelGatewayStreamEvent(type="completed", response=response)


class OpenAICompatibleModelGateway:
    """OpenAI Chat Completions gateway with provider-neutral tool-call parsing."""

    def __init__(self, config: LLMConfig, *, client: Any | None = None) -> None:
        self.config = config
        self._client = client

    async def complete(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> ModelResponse:
        try:
            return await asyncio.to_thread(self._complete_sync, messages, tools, metadata)
        except (LLMConfigurationError, LLMRequestError, LLMResponseError, ModelProtocolError):
            raise
        except Exception as error:
            raise LLMRequestError(
                f"{self.config.provider} agent completion failed: {type(error).__name__}."
            ) from error

    async def stream(
        self,
        *,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[ModelGatewayStreamEvent]:
        sampling_record = begin_provider_sampling(model=self.config.model, provider=self.config.provider)
        try:
            chunks = await asyncio.to_thread(
                self._start_stream_sync,
                messages,
                tools,
                metadata,
            )
            iterator = iter(chunks)
            content_parts: list[str] = []
            tool_buffers: dict[int, dict[str, str]] = {}
            model = self.config.model
            response_id: str | None = None
            input_tokens = 0
            output_tokens = 0
            finish_reason: str | None = None
            while True:
                present, chunk = await asyncio.to_thread(_next_item, iterator)
                if not present:
                    break
                response_id = _optional_str(getattr(chunk, "id", None)) or response_id
                model = str(getattr(chunk, "model", None) or model)
                sampling_record.update(model=model, responseId=response_id)
                usage = getattr(chunk, "usage", None)
                if usage is not None:
                    input_tokens = _optional_int(getattr(usage, "prompt_tokens", None))
                    output_tokens = _optional_int(getattr(usage, "completion_tokens", None))
                    sampling_record.update(usageKnown=True, inputTokens=input_tokens, outputTokens=output_tokens)
                choices = getattr(chunk, "choices", None) or ()
                if not choices:
                    continue
                choice = choices[0]
                finish_reason = (
                    _optional_str(getattr(choice, "finish_reason", None)) or finish_reason
                )
                delta = getattr(choice, "delta", None)
                delta_content = getattr(delta, "content", None)
                if isinstance(delta_content, str) and delta_content:
                    content_parts.append(delta_content)
                    yield ModelGatewayStreamEvent(type="delta", delta=delta_content)
                _merge_stream_tool_calls(
                    tool_buffers,
                    getattr(delta, "tool_calls", None) or (),
                )
            sampling_record.update(status="completed", responseId=response_id, model=model)
            finish_provider_sampling(sampling_record)
            tool_calls = _stream_tool_calls(tool_buffers)
            content = "".join(content_parts)
            if not content and not tool_calls:
                raise LLMResponseError(f"{self.config.provider} returned an empty agent stream.")
            response = ModelResponse(
                content=content,
                tool_calls=tool_calls,
                model=model,
                response_id=response_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                finish_reason=finish_reason,
            )
            yield ModelGatewayStreamEvent(type="completed", response=response)
        except (LLMConfigurationError, LLMRequestError, LLMResponseError, ModelProtocolError):
            sampling_record["errorType"] = "ModelStreamError"
            raise
        except Exception as error:
            raise LLMRequestError(
                f"{self.config.provider} agent stream failed: {type(error).__name__}."
            ) from error
        finally:
            finish_provider_sampling(sampling_record)
            if "chunks" in locals() and callable(getattr(chunks, "close", None)):
                await asyncio.to_thread(chunks.close)

    def _complete_sync(
        self,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None,
    ) -> ModelResponse:
        request = self._request(messages, tools, stream=False, metadata=metadata)
        try:
            response = sample_sync(lambda:self._get_client().chat.completions.create(**request),
                                   model=self.config.model,provider=self.config.provider)
        except (LLMConfigurationError, ModelProtocolError):
            raise
        except Exception as error:
            raise LLMRequestError(
                f"{self.config.provider} agent completion request failed: {type(error).__name__}."
            ) from error
        choices = getattr(response, "choices", None) or ()
        if not choices:
            raise LLMResponseError(f"{self.config.provider} returned no completion choices.")
        choice = choices[0]
        message = getattr(choice, "message", None)
        if message is None:
            raise ModelProtocolError("provider completion choice did not include a message")
        content_value = getattr(message, "content", None)
        content = content_value.strip() if isinstance(content_value, str) else ""
        tool_calls = _parse_tool_calls(getattr(message, "tool_calls", None) or ())
        if not content and not tool_calls:
            raise LLMResponseError(
                f"{self.config.provider} returned neither assistant content nor tool calls."
            )
        usage = getattr(response, "usage", None)
        return ModelResponse(
            content=content,
            tool_calls=tool_calls,
            model=str(getattr(response, "model", None) or self.config.model),
            response_id=_optional_str(getattr(response, "id", None)),
            input_tokens=_optional_int(getattr(usage, "prompt_tokens", None)),
            output_tokens=_optional_int(getattr(usage, "completion_tokens", None)),
            finish_reason=_optional_str(getattr(choice, "finish_reason", None)),
        )

    def _start_stream_sync(
        self,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        metadata: dict[str, Any] | None,
    ) -> object:
        try:
            return self._get_client().chat.completions.create(
                **self._request(messages, tools, stream=True, metadata=metadata)
            )
        except LLMConfigurationError:
            raise
        except Exception as error:
            raise LLMRequestError(
                f"{self.config.provider} agent stream request failed: {type(error).__name__}."
            ) from error

    def _request(
        self,
        messages: tuple[ModelMessage, ...],
        tools: tuple[ToolDefinition, ...],
        *,
        stream: bool,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        request: dict[str, object] = {
            "model": self.config.model,
            "messages": [_provider_message(message) for message in messages],
            "stream": stream,
        }
        if tools:
            request["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                    },
                }
                for tool in tools
            ]
            request["tool_choice"] = "auto"
        if self.config.temperature is not None:
            request["temperature"] = self.config.temperature
        if self.config.max_tokens > 0:
            request["max_tokens"] = self.config.max_tokens
        if self.config.extra_body:
            request["extra_body"] = self.config.extra_body
        output_contract = (metadata or {}).get("output_contract")
        if isinstance(output_contract, dict) and output_contract.get("provider_native"):
            schema = output_contract.get("schema")
            name = output_contract.get("name")
            if isinstance(schema, dict) and isinstance(name, str) and name:
                request["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": name,
                        "strict": bool(output_contract.get("strict", True)),
                        "schema": schema,
                    },
                }
        return request

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        from agent_memory_runtime.llm.transport import get_openai_client

        return get_openai_client(
            base_url=self.config.base_url,
            api_key_env=self.config.api_key_env,
            timeout_seconds=self.config.timeout_seconds,
        )


def _provider_message(message: ModelMessage) -> dict[str, object]:
    value: dict[str, object] = {"role": message.role, "content": message.content}
    if message.name:
        value["name"] = message.name
    if message.tool_call_id:
        value["tool_call_id"] = message.tool_call_id
    if message.tool_calls:
        value["tool_calls"] = [
            {
                "id": call.call_id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": json.dumps(
                        call.arguments,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            }
            for call in message.tool_calls
        ]
    return value


def _legacy_prompts(messages: tuple[ModelMessage, ...]) -> tuple[str, str]:
    system_prompt = "\n".join(message.content for message in messages if message.role == "system")
    conversation = "\n".join(
        f"{message.role}: {message.content}" for message in messages if message.role != "system"
    )
    return system_prompt, conversation


def _merge_stream_tool_calls(
    buffers: dict[int, dict[str, str]],
    values: object,
) -> None:
    iterable = values if isinstance(values, (list, tuple)) else tuple(values)
    for fallback_index, value in enumerate(iterable):
        raw_index = getattr(value, "index", fallback_index)
        try:
            index = int(raw_index)
        except (TypeError, ValueError) as error:
            raise ModelProtocolError("provider returned an invalid tool call index") from error
        buffer = buffers.setdefault(index, {"id": "", "name": "", "arguments": ""})
        call_id = getattr(value, "id", None)
        if call_id:
            buffer["id"] = str(call_id)
        function = getattr(value, "function", None)
        name = getattr(function, "name", None)
        arguments = getattr(function, "arguments", None)
        if name:
            buffer["name"] += str(name)
        if arguments:
            buffer["arguments"] += str(arguments)


def _stream_tool_calls(buffers: dict[int, dict[str, str]]) -> tuple[ModelToolCall, ...]:
    calls: list[ModelToolCall] = []
    for index in sorted(buffers):
        buffer = buffers[index]
        call_id = buffer["id"].strip()
        name = buffer["name"].strip()
        if not call_id or not name:
            raise ModelProtocolError("provider streamed a tool call without id or name")
        raw_arguments = buffer["arguments"] or "{}"
        try:
            arguments = json.loads(raw_arguments)
        except json.JSONDecodeError as error:
            raise ModelProtocolError(
                f"provider streamed invalid JSON arguments for tool {name!r}"
            ) from error
        if not isinstance(arguments, dict):
            raise ModelProtocolError(f"tool {name!r} arguments must be a JSON object")
        calls.append(
            ModelToolCall(
                call_id=call_id,
                name=name,
                arguments={str(key): item for key, item in arguments.items()},
            )
        )
    return tuple(calls)


def _next_item(iterator: object) -> tuple[bool, Any | None]:
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


def _parse_tool_calls(values: object) -> tuple[ModelToolCall, ...]:
    calls: list[ModelToolCall] = []
    seen: set[str] = set()
    for value in values if isinstance(values, (list, tuple)) else tuple(values):
        call_id = str(getattr(value, "id", "") or "").strip()
        function = getattr(value, "function", None)
        name = str(getattr(function, "name", "") or "").strip()
        raw_arguments = getattr(function, "arguments", "{}")
        if not call_id or not name:
            raise ModelProtocolError("provider returned a tool call without id or name")
        if call_id in seen:
            raise ModelProtocolError(f"provider returned duplicate tool call id {call_id!r}")
        try:
            arguments = (
                json.loads(raw_arguments) if isinstance(raw_arguments, str) else raw_arguments
            )
        except json.JSONDecodeError as error:
            raise ModelProtocolError(
                f"provider returned invalid JSON arguments for tool {name!r}"
            ) from error
        if not isinstance(arguments, dict):
            raise ModelProtocolError(f"tool {name!r} arguments must be a JSON object")
        calls.append(
            ModelToolCall(
                call_id=call_id,
                name=name,
                arguments={str(key): item for key, item in arguments.items()},
            )
        )
        seen.add(call_id)
    return tuple(calls)


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: object) -> int:
    if value is None:
        return 0
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ModelProtocolError("provider returned invalid token usage") from error
    if result < 0:
        raise ModelProtocolError("provider returned negative token usage")
    return result


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError as error:
        raise LLMConfigurationError(
            "The .env loader is unavailable. Run: pip install -e ."
        ) from error
    load_dotenv(override=False)
