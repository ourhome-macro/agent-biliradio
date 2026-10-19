"""Execution-scoped writes; validation happens under the store's write lock."""

from datetime import UTC, datetime
from functools import partial

from agent_memory_runtime.agent.errors import AgentLeaseLostError
from agent_memory_runtime.agent.models import RunStatus


def assert_execution_lease(run, lease_token):
    if lease_token is None:
        return
    expires = None
    if run is not None and run.lease_expires_at:
        try:
            expires = datetime.fromisoformat(run.lease_expires_at)
        except ValueError:
            raise AgentLeaseLostError("agent execution lease expiry is invalid") from None
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
    if (
        run is None
        or run.status is not RunStatus.RUNNING
        or run.lease_token != lease_token
        or expires is None
        or expires <= datetime.now(UTC)
    ):
        raise AgentLeaseLostError("agent execution no longer owns its run")


class LeaseBoundAgentStateStore:
    """One facade per execution, never a mutable token shared between runs."""

    _writes = frozenset(
        {
            "save_checkpoint",
            "save_turn",
            "create_tool_call",
            "update_tool_call",
            "create_approval",
        }
    )

    def __init__(self, store, lease_token):
        if not lease_token:
            raise ValueError("an execution store requires a lease token")
        self._store, self._lease_token = store, lease_token

    def __getattr__(self, name):
        method = getattr(self._store, name)
        return partial(method, lease_token=self._lease_token) if name in self._writes else method
