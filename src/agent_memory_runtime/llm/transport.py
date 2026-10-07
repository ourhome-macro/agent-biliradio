"""Process-owned OpenAI connection pools shared across business and agent adapters."""

from __future__ import annotations

import atexit
import os
import threading

from agent_memory_runtime.exceptions import LLMConfigurationError

_lock = threading.Lock()
_clients: dict[tuple, object] = {}


def _after_fork():
    global _lock, _clients
    _lock = threading.Lock()
    _clients = {}


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def get_openai_client(
    *, base_url: str, api_key_env: str, timeout_seconds: float, require_https: bool = True,
    api_key: str | None = None,
):
    if require_https and not base_url.startswith("https://"):
        raise LLMConfigurationError("Model base_url must use HTTPS.")
    api_key = api_key or os.getenv(api_key_env)
    if not api_key:
        from dotenv import load_dotenv

        load_dotenv(override=False)
        api_key = os.getenv(api_key_env)
    if not api_key:
        raise LLMConfigurationError(f"Missing {api_key_env}.")
    key = (os.getpid(), base_url.rstrip("/"), api_key, timeout_seconds)
    with _lock:
        client = _clients.get(key)
        if client is None:
            from openai import OpenAI

            # Every provider attempt is explicit and budgeted by its caller.
            client = OpenAI(base_url=base_url, api_key=api_key, timeout=timeout_seconds, max_retries=0)
            _clients[key] = client
        return client


def close_clients():
    with _lock:
        clients = list(_clients.values())
        _clients.clear()
    for client in clients:
        try:
            client.close()
        except Exception:
            pass


atexit.register(close_clients)
