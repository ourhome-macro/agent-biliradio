from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from agent_memory_runtime.agent.errors import AgentLeaseLostError, AgentRunConflictError
from agent_memory_runtime.agent.models import (
    AgentCheckpoint,
    AgentRequest,
    AgentRun,
    AgentTurn,
    RunStatus,
    ToolCallRecord,
    ToolCallStatus,
)
from agent_memory_runtime.agent.recovery import reconcile_tool_record
from agent_memory_runtime.agent.stores.in_memory import InMemoryAgentStateStore
from agent_memory_runtime.agent.stores.leases import LeaseBoundAgentStateStore
from agent_memory_runtime.agent.stores.sqlite import SQLiteAgentStateStore


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    return (
        InMemoryAgentStateStore()
        if request.param == "memory"
        else SQLiteAgentStateStore(tmp_path / "agent.sqlite3")
    )


def claimed(store):
    run = store.create_run(AgentRun.new(AgentRequest("a", "work", tenant_id="t", user_id="u")))
    return store.claim_run(run.run_id, worker_id="old", lease_seconds=30)


def expire(store, run):
    expired = replace(run, lease_expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat())
    # Fault injection: the normal update API deliberately preserves lease fields.
    if isinstance(store, InMemoryAgentStateStore):
        store._runs[run.run_id] = expired
    else:
        with store._manager.connection() as conn:
            conn.execute(
                "UPDATE agent_runs SET payload=? WHERE run_id=?",
                (store._codec.encode(expired.to_dict()), run.run_id),
            )
    return expired


def test_all_execution_writes_are_fenced_after_takeover(store):
    run = claimed(store)
    old = LeaseBoundAgentStateStore(store, run.lease_token)
    checkpoint = old.save_checkpoint(AgentCheckpoint(run.run_id, ()), expected_version=None)
    call = old.create_tool_call(ToolCallRecord("c", run.run_id, "t", "tool", {}))
    expire(store, run)
    new = store.claim_run(run.run_id, worker_id="new", lease_seconds=30)
    writes = [
        lambda: old.save_checkpoint(checkpoint, expected_version=checkpoint.version),
        lambda: old.save_turn(AgentTurn.new(run_id=run.run_id, sequence=1)),
        lambda: old.create_tool_call(replace(call, call_id="new-call")),
        lambda: old.update_tool_call(
            replace(call, status=ToolCallStatus.SUCCEEDED), expected_version=call.version
        ),
        lambda: old.create_approval(SimpleNamespace(run_id=run.run_id)),
    ]
    for write in writes:
        with pytest.raises(AgentLeaseLostError):
            write()
    assert store.get_checkpoint(run.run_id).version == checkpoint.version
    assert store.get_tool_call("c").status is ToolCallStatus.PENDING
    assert store.get_run(run.run_id).lease_token == new.lease_token


def test_expired_owner_cannot_commit_without_a_replacement(store):
    run = claimed(store)
    expire(store, run)
    with pytest.raises(AgentLeaseLostError):
        LeaseBoundAgentStateStore(store, run.lease_token).save_turn(
            AgentTurn.new(run_id=run.run_id, sequence=1)
        )


def unknown(store):
    run = claimed(store)
    call = store.create_tool_call(
        ToolCallRecord(
            "c",
            run.run_id,
            "t",
            "write",
            {},
            status=ToolCallStatus.RECONCILIATION_REQUIRED,
            side_effects=True,
            idempotent=False,
            attempts=1,
        )
    )
    run = store.update_run(
        replace(run, status=RunStatus.NEEDS_RECONCILIATION), expected_version=run.version
    )
    return run, call


def test_reconciliation_atomically_updates_run_tool_and_persists_evidence(store, monkeypatch):
    run, call = unknown(store)
    update = store.update_run

    def fail(*args, **kwargs):
        raise RuntimeError("crash before run commit")

    monkeypatch.setattr(store, "update_run", fail)
    args = dict(
        tenant_id="t",
        reviewer_id="operator",
        succeeded=True,
        output={"receipt": "verified"},
        reason="checked external receipt",
        resolution_id="resolution",
    )
    with pytest.raises(RuntimeError):
        reconcile_tool_record(store, "c", **args)
    assert store.get_tool_call("c").status is ToolCallStatus.RECONCILIATION_REQUIRED
    monkeypatch.setattr(store, "update_run", update)
    saved = reconcile_tool_record(store, "c", **args)
    assert saved.status is ToolCallStatus.SUCCEEDED
    assert store.get_run(run.run_id).status is RunStatus.PENDING
    assert saved.reconciliation["reviewer_id"] == "operator"
    assert reconcile_tool_record(store, "c", **args).version == saved.version
    with pytest.raises(AgentRunConflictError):
        reconcile_tool_record(store, "c", **{**args, "output": {"receipt": "different"}})


def test_reconciliation_rejects_live_owner(store):
    run = claimed(store)
    store.create_tool_call(
        ToolCallRecord("c", run.run_id, "t", "write", {}, status=ToolCallStatus.EXECUTING)
    )
    with pytest.raises(AgentRunConflictError):
        reconcile_tool_record(store, "c", tenant_id="t", reviewer_id="op", succeeded=True)


def test_reconciling_cancelled_effect_never_restarts_cancelled_run(store):
    run = claimed(store)
    store.create_tool_call(
        ToolCallRecord(
            "c",
            run.run_id,
            "t",
            "write",
            {},
            status=ToolCallStatus.EXECUTING,
            side_effects=True,
            idempotent=False,
        )
    )
    store.cancel_run(run.run_id, tenant_id="t")
    call = reconcile_tool_record(
        store,
        "c",
        tenant_id="t",
        reviewer_id="op",
        succeeded=True,
        output={"receipt": "checked after cancellation"},
    )
    assert call.status is ToolCallStatus.SUCCEEDED
    assert store.get_run(run.run_id).status is RunStatus.CANCELLED


def test_unresolved_second_effect_keeps_run_blocked(store):
    run, first = unknown(store)
    store.create_tool_call(replace(first, call_id="second"))
    reconcile_tool_record(store, "c", tenant_id="t", reviewer_id="op", succeeded=True)
    assert store.get_run(run.run_id).status is RunStatus.NEEDS_RECONCILIATION


def test_verified_failure_can_fail_the_run_without_replaying_tool(store):
    run, _ = unknown(store)
    result = reconcile_tool_record(
        store, "c", tenant_id="t", reviewer_id="op", succeeded=False, fail_run_on_failure=True
    )
    assert result.status is ToolCallStatus.FAILED
    assert store.get_run(run.run_id).status is RunStatus.FAILED


def test_old_exception_handler_cannot_fail_new_owner(store):
    from agent_memory_runtime.agent.runtime import BusinessAgentRuntime, _EventFactory

    run = claimed(store)
    factory = _EventFactory(run)
    expire(store, run)
    latest = store.claim_run(run.run_id, worker_id="new", lease_seconds=30)
    runtime = BusinessAgentRuntime(model_gateway=object(), state_store=store)
    asyncio.run(runtime._mark_failed(run.run_id, factory, error_type="OldWorkerError"))
    assert store.get_run(run.run_id) == latest


def test_runtime_cancel_during_effect_keeps_unknown_receipt_for_reconciliation(store):
    from agent_memory_runtime.agent.models import ModelResponse, ModelToolCall
    from agent_memory_runtime.agent.policy import AgentPolicy, StaticAgentPolicyResolver
    from agent_memory_runtime.agent.runtime import BusinessAgentRuntime
    from agent_memory_runtime.agent.tool_runtime import AgentFunctionTool
    from agent_memory_runtime.tools.registry import ToolRegistry

    class Gateway:
        async def complete(self, **kwargs):
            return ModelResponse(content="", tool_calls=(ModelToolCall("c", "write", {}),))

    async def effect(arguments, context):
        store.cancel_run(context.run_id, tenant_id="t")
        return {"receipt": "committed during cancellation"}

    tools = ToolRegistry()
    tools.register(AgentFunctionTool("write", effect, side_effects=True, idempotent=False))
    runtime = BusinessAgentRuntime(
        model_gateway=Gateway(),
        state_store=store,
        tool_registry=tools,
        policy_resolver=StaticAgentPolicyResolver(AgentPolicy(approval_risk_threshold=None)),
    )

    async def run():
        return [
            event
            async for event in runtime.run(AgentRequest("a", "work", tenant_id="t", user_id="u"))
        ]

    events = asyncio.run(run())
    assert events[-1].type == "run.cancelled"
    assert store.get_tool_call("c").status is ToolCallStatus.EXECUTING
    saved = reconcile_tool_record(
        store, "c", tenant_id="t", reviewer_id="op", succeeded=True, output={"receipt": "verified"}
    )
    assert saved.status is ToolCallStatus.SUCCEEDED
    assert store.get_run(saved.run_id).status is RunStatus.CANCELLED


@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.parametrize("succeeded", [False, True])
def test_radio_reconciliation_resumes_next_stage_without_repeating_effect(
    tmp_path, monkeypatch, crash, succeeded
):
    import agent_job_recovery
    from database import get_connection, init_db
    from durable_jobs import claim, enqueue, finish
    from job_errors import JobReconciliationRequired
    from music_agent import OperationStage, current_job_id, execute_operation

    path = tmp_path / "radio.sqlite3"
    init_db(path)
    with get_connection(path) as conn:
        enqueue(conn, kind="discovery", user_id="u", payload={}, job_id="j", retry_safe=True)
    with get_connection(path) as conn:
        job = claim(conn, "j")
    effects = []

    def first(_state):
        effects.append("external-write")
        raise RuntimeError("external write committed, response lost")

    def second(state):
        effects.append("next-stage")
        return {"verified": state["receipt"]}

    stages = (OperationStage("first", first), OperationStage("second", second))
    service = SimpleNamespace(db_path=path, user_id="u")
    token = current_job_id.set("j")
    try:
        with pytest.raises(JobReconciliationRequired) as caught:
            execute_operation(service, "test", {}, stages=stages)
        with get_connection(path) as conn:
            finish(conn, job, error=caught.value)
            row = conn.execute("SELECT * FROM durable_jobs WHERE job_id='j'").fetchone()
        agent = SQLiteAgentStateStore(tmp_path / "agent-runs.sqlite3")
        call = agent.list_tool_calls(row["agent_run_id"])[0]
        with pytest.raises(ValueError, match="unknown side effects"):
            agent_job_recovery.resume_job(
                path, "j", resolution_id="unsafe", operator="op", reason="attempt"
            )
        params = dict(
            call_id=call.call_id,
            resolution_id="r",
            succeeded=succeeded,
            output={"receipt": "external-receipt"} if succeeded else {},
            operator="op",
            reason="receipt verified",
        )
        apply = agent_job_recovery.reconcile_tool_record
        if crash:

            def interrupted(*args, **kwargs):
                apply(*args, **kwargs)
                raise RuntimeError("crash after Agent commit")

            monkeypatch.setattr(agent_job_recovery, "reconcile_tool_record", interrupted)
            with pytest.raises(RuntimeError):
                agent_job_recovery.reconcile_job_tool(path, "j", **params)
            with get_connection(path) as conn:
                assert (
                    conn.execute("SELECT status FROM durable_jobs WHERE job_id='j'").fetchone()[0]
                    == "needs_reconciliation"
                )
            monkeypatch.setattr(agent_job_recovery, "reconcile_tool_record", apply)
        expected = "queued" if succeeded else "failed"
        assert agent_job_recovery.reconcile_job_tool(path, "j", **params)["status"] == expected
        assert agent_job_recovery.reconcile_job_tool(path, "j", **params)["status"] == expected
        with pytest.raises(ValueError, match="different evidence"):
            agent_job_recovery.reconcile_job_tool(path, "j", **{**params, "reason": "changed"})
        with get_connection(path) as conn:
            resumed = claim(conn, "j")
        if not succeeded:
            assert resumed is None
            assert agent.get_run(call.run_id).status is RunStatus.FAILED
            assert effects == ["external-write"]
            return
        result = execute_operation(service, "test", {}, stages=stages)
        with get_connection(path) as conn:
            finish(conn, resumed, result=result)
        assert result == {"verified": "external-receipt"}
        assert effects == ["external-write", "next-stage"]
    finally:
        current_job_id.reset(token)


def test_admin_inspects_calls_and_rejects_job_only_resolution(tmp_path, monkeypatch):
    import job_admin
    from click.testing import CliRunner
    from database import get_connection, init_db
    from durable_jobs import enqueue

    path = tmp_path / "radio.sqlite3"
    init_db(path)
    store = SQLiteAgentStateStore(tmp_path / "agent-runs.sqlite3")
    run, call = unknown(store)
    with get_connection(path) as conn:
        enqueue(conn, kind="dialogue", user_id="u", payload={}, job_id="j")
        conn.execute(
            "UPDATE durable_jobs SET status='needs_reconciliation',agent_run_id=? WHERE job_id='j'",
            (run.run_id,),
        )
    monkeypatch.setattr(job_admin, "DEFAULT_DB_PATH", path)
    monkeypatch.setattr(job_admin, "init_db", lambda: None)
    monkeypatch.setattr(job_admin, "get_connection", lambda: get_connection(path))
    runner = CliRunner()
    inspected = runner.invoke(job_admin.cli, ["inspect-job", "j"])
    assert inspected.exit_code == 0, inspected.output
    assert '"callId": "c"' in inspected.output
    refused = runner.invoke(
        job_admin.cli, ["resolve", "j", "--decision", "completed", "--reason", "verified"]
    )
    assert refused.exit_code == 1 and "reconcile-tool" in refused.output
    with get_connection(path) as conn:
        assert (
            conn.execute("SELECT status FROM durable_jobs WHERE job_id='j'").fetchone()[0]
            == "needs_reconciliation"
        )


def test_completed_agent_can_finish_interrupted_job_without_repeating_effect(tmp_path):
    import time

    from agent_job_recovery import resume_job
    from database import get_connection, init_db
    from durable_jobs import claim, enqueue, finish, recover_expired
    from music_agent import current_job_id, execute_operation

    path = tmp_path / "radio.sqlite3"
    init_db(path)
    with get_connection(path) as conn:
        enqueue(conn, kind="dialogue", user_id="u", payload={}, job_id="j")
    with get_connection(path) as conn:
        assert claim(conn, "j")
    effects = []
    service = SimpleNamespace(db_path=path, user_id="u")

    def action():
        effects.append("write")
        return {"receipt": "ok"}

    token = current_job_id.set("j")
    try:
        result = execute_operation(service, "test", {}, action=action)
        # Crash after the Agent commit, before durable_jobs.finish.
        with get_connection(path) as conn:
            recover_expired(conn, now=time.time() + 400)
        assert (
            resume_job(
                path,
                "j",
                resolution_id="r",
                operator="op",
                reason="worker stopped, journal verified",
            )["status"]
            == "queued"
        )
        with get_connection(path) as conn:
            resumed = claim(conn, "j")
        replayed = execute_operation(service, "test", {}, action=action)
        with get_connection(path) as conn:
            finish(conn, resumed, result=replayed)
        assert effects == ["write"] and replayed == result
        assert (
            resume_job(
                path,
                "j",
                resolution_id="r",
                operator="op",
                reason="worker stopped, journal verified",
            )["status"]
            == "completed"
        )
    finally:
        current_job_id.reset(token)
