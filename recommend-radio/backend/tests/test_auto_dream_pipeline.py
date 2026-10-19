from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from amem_bridge import AmemBridge

from agent_memory_runtime.domain.event import Event
from agent_memory_runtime.memory.intake.dream import AutoDreamAnalyzer, _has_information_gain
from agent_memory_runtime.memory.intake.models import DreamCheckpoint
from agent_memory_runtime.memory.intake.triggers import AutoDreamTriggers
from agent_memory_runtime.memory.intake.worker import AutoDreamWorker
from agent_memory_runtime.memory.stores.sqlite import SQLiteStoreBundle
from agent_memory_runtime.runtime import AgentMemoryRuntime


def _bundle(tmp_path):
    return SQLiteStoreBundle(tmp_path / "dream.sqlite3")


def _event(bundle, kind: str, *, index: int = 0):
    event = Event(
        event_id=f"event-{kind}-{index}",
        kind="observation.created",
        actor_id="user-1",
        tenant_id="radio",
        user_id="user-1",
        agent_id="radio-agent",
        session_id="track-session",
        tags=("music",),
        payload={
            "event": kind,
            "listenMs": 1000,
            "track": {"trackId": f"track-{index}", "title": "jazz"},
        },
    )
    return bundle.event_store.append(event)


def test_three_skips_schedule_micro_and_ten_events_schedule_batch(tmp_path):
    bundle = _bundle(tmp_path)
    triggers = AutoDreamTriggers(bundle.dream_store)
    for index in range(3):
        modes = triggers.record_event(_event(bundle, "skipped", index=index))
    assert modes == ("micro",)
    observed = [
        triggers.record_event(_event(bundle, "completed", index=index)) for index in range(10)
    ]
    assert any("batch" in modes for modes in observed)
    jobs = {job.mode: job for job in bundle.dream_store.list_jobs()}
    assert set(jobs) == {"micro", "batch"}
    assert jobs["micro"].status == "pending"


def test_long_skips_do_not_count_as_quick_skip_streak(tmp_path):
    bundle = _bundle(tmp_path)
    triggers = AutoDreamTriggers(bundle.dream_store)
    for index in range(3):
        event = bundle.event_store.append(Event(
            event_id=f"long-skip-{index}", kind="observation.created",
            actor_id="user-1", tenant_id="radio", user_id="user-1",
            agent_id="radio-agent", payload={
                "event": "skipped", "playedSeconds": 45,
                "track": {"trackId": f"track-{index}"},
            },
        ))
        assert triggers.record_event(event) == ()
    assert bundle.dream_store.list_jobs() == []


def test_expired_lease_can_be_reclaimed_and_old_worker_cannot_complete(tmp_path):
    bundle = _bundle(tmp_path)
    store = bundle.dream_store
    store.schedule(tenant_id="radio", user_id="user-1", mode="micro")
    old = store.claim_next(worker_id="old", lease_seconds=0)
    assert old is not None
    current = store.claim_next(worker_id="new", lease_seconds=30)
    assert current is not None and current.lease_token != old.lease_token
    with pytest.raises(RuntimeError, match="lease lost"):
        store.complete(old, DreamCheckpoint())
    store.schedule(tenant_id="radio", user_id="user-1", mode="micro")
    completed = store.complete(current, DreamCheckpoint(last_processed_sequence=7))
    assert completed.status == "pending"
    assert store.checkpoint_for(current).last_processed_sequence == 7


def test_new_trigger_does_not_reset_retry_backoff(tmp_path):
    bundle = _bundle(tmp_path)
    store = bundle.dream_store
    store.schedule(tenant_id="radio", user_id="user-1", mode="batch")
    running = store.claim_next(worker_id="worker")
    assert running is not None
    retry = store.fail(running, RuntimeError("temporary"))
    scheduled = store.schedule(tenant_id="radio", user_id="user-1", mode="batch")
    assert scheduled.attempts == 1
    assert scheduled.available_at == retry.available_at


def test_llm_output_requires_real_evidence_and_passes_through_worker(tmp_path):
    bundle = _bundle(tmp_path)
    event = _event(bundle, "liked", index=1)

    class Client:
        def complete(self, **_kwargs):
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "facts": [
                            {
                                "key": "jazz",
                                "content": "User recently likes jazz.",
                                "evidence_event_ids": [event.event_id],
                                "confidence": 0.8,
                                "salience": 0.8,
                            },
                            {
                                "key": "invented",
                                "content": "Unsupported claim.",
                                "evidence_event_ids": ["missing-event"],
                                "confidence": 1.0,
                                "salience": 1.0,
                            },
                            {
                                "key": "rock",
                                "polarity": "negative",
                                "evidence_event_ids": [event.event_id],
                                "confidence": 1.0,
                                "salience": 1.0,
                            },
                        ]
                    }
                )
            )

    runtime = AgentMemoryRuntime(
        event_store=bundle.event_store,
        memory_store=bundle.memory_store,
        snapshot_store=bundle.snapshot_store,
        audit_store=bundle.audit_store,
        tombstone_store=bundle.tombstone_store,
        transaction_manager=bundle,
        dream_store=bundle.dream_store,
    )
    bundle.dream_store.schedule(
        tenant_id="radio", user_id="user-1", agent_id="radio-agent", mode="micro"
    )
    worker = AutoDreamWorker(
        runtime=runtime,
        store=bundle.dream_store,
        analyzer=AutoDreamAnalyzer(llm_client_factory=lambda _user: Client()),
    )
    report = worker.run_once()
    assert report.applied == 1
    assert report.proposals == 1
    saved = bundle.memory_store.list_records_for_scope(tenant_id="radio", user_id="user-1")
    assert len(saved) == 1
    assert saved[0].session_id == "music-profile"
    assert saved[0].content == "User shows recent positive music preference for topic: jazz."
    assert saved[0].source_event_ids == (event.event_id,)


def test_idle_and_nightly_deep_trigger_only_once_per_activity(tmp_path):
    bundle = _bundle(tmp_path)
    triggers = AutoDreamTriggers(bundle.dream_store)
    triggers.record_event(_event(bundle, "completed", index=1))
    with bundle._manager.connection() as connection:
        row = connection.execute("SELECT scope_key, payload FROM dream_trigger_state").fetchone()
        state = json.loads(row[1])
        state["last_activity_at"] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
        connection.execute(
            "UPDATE dream_trigger_state SET payload = ? WHERE scope_key = ?",
            (json.dumps(state), row[0]),
        )
    assert triggers.schedule_due() >= 1
    assert triggers.schedule_due() == 0
    assert any(job.mode == "deep" for job in bundle.dream_store.list_jobs())
    triggers.record_event(_event(bundle, "completed", index=2))
    with bundle._manager.connection() as connection:
        row = connection.execute("SELECT scope_key, payload FROM dream_trigger_state").fetchone()
        state = json.loads(row[1])
        state["last_activity_at"] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
        connection.execute(
            "UPDATE dream_trigger_state SET payload = ? WHERE scope_key = ?",
            (json.dumps(state), row[0]),
        )
    assert triggers.schedule_due() == 0


def test_micro_cooldown_persists_flush_until_batch(tmp_path):
    bundle = _bundle(tmp_path)
    triggers = AutoDreamTriggers(bundle.dream_store)
    for index in range(3):
        triggers.record_event(_event(bundle, "skipped", index=index))
    assert triggers.record_event(_event(bundle, "liked", index=1)) == ()
    with bundle._manager.connection() as connection:
        row = connection.execute("SELECT scope_key, payload FROM dream_trigger_state").fetchone()
        state = json.loads(row[1])
        assert state["pending_flush"] is True
        state["last_micro_at"] = (datetime.now(UTC) - timedelta(minutes=4)).isoformat()
        connection.execute(
            "UPDATE dream_trigger_state SET payload = ? WHERE scope_key = ?",
            (json.dumps(state), row[0]),
        )
    assert triggers.schedule_due() >= 1
    assert any(job.mode == "batch" for job in bundle.dream_store.list_jobs())


def test_profile_matching_completions_skip_llm_gate_but_not_other_feedback():
    record = SimpleNamespace(
        status="active", level="L3", metadata={"topic": "Jazz", "signal": "positive_topic"}
    )
    completed = Event(
        kind="observation.created", actor_id="user-1",
        payload={"event": "completed", "track": {"tags": ["Jazz"]}},
    )
    liked = Event(kind="observation.created", actor_id="user-1", payload={"event": "liked"})
    assert _has_information_gain([completed], [record]) is False
    assert _has_information_gain([completed, liked], [record]) is True


def test_micro_prompt_keeps_signal_when_play_events_follow_it():
    seen: list[str] = []

    class Client:
        def complete(self, *, system_prompt, user_prompt):
            del system_prompt
            seen.extend(item["event_id"] for item in json.loads(user_prompt)["events"])
            return SimpleNamespace(content='{"facts": []}')

    signal = Event(
        event_id="liked", sequence=1, kind="observation.created",
        actor_id="user-1", user_id="user-1", agent_id="radio-agent",
        payload={"event": "liked"},
    )
    played = [
        Event(
            event_id=f"played-{index}", sequence=index + 2,
            kind="observation.created", actor_id="user-1",
            user_id="user-1", agent_id="radio-agent",
            payload={"event": "played"},
        )
        for index in range(12)
    ]
    AutoDreamAnalyzer(llm_client_factory=lambda _user: Client()).analyze(
        events=[signal, *played], records=[], mode="micro"
    )
    assert seen == ["liked"]


def test_deep_llm_profile_promotion_requires_cross_day_evidence():
    start = datetime.now(UTC) - timedelta(days=9)
    events = [
        Event(
            event_id=f"deep-{index}", sequence=index + 1,
            kind="observation.created", actor_id="user-1", tenant_id="radio",
            user_id="user-1", agent_id="radio-agent", tags=("music",),
            occurred_at=(start + timedelta(days=index * 1.5)).isoformat(),
            payload={"event": "liked", "track": {"title": "jazz"}},
        )
        for index in range(6)
    ]

    class Client:
        def complete(self, **_kwargs):
            return SimpleNamespace(content=json.dumps({"facts": [{
                "key": "jazz", "content": "User consistently likes jazz.",
                "evidence_event_ids": [event.event_id for event in events],
                "confidence": 0.9, "salience": 0.9,
            }]}))

    analyzer = AutoDreamAnalyzer(llm_client_factory=lambda _user: Client())
    report = analyzer.analyze(
        events=events, records=[], mode="deep", context_events=events,
    )
    assert len(report.proposals) == 1
    assert report.proposals[0].level == "L3"
    assert report.proposals[0].source_message_ids == tuple(event.event_id for event in events)


def test_dream_client_caps_model_output_and_timeout(monkeypatch):
    import profile_projector
    import settings_service

    client = SimpleNamespace(max_tokens=3000, timeout_seconds=60)
    monkeypatch.setenv("AUTO_DREAM_LLM_ENABLED", "true")
    monkeypatch.setenv("RECOMMEND_LLM_PROVIDER", "deepseek")
    monkeypatch.setattr(
        settings_service.SettingsService,
        "has_deepseek_api_key",
        lambda _self: True,
    )
    monkeypatch.setattr(profile_projector, "_default_llm_client", lambda **_kwargs: client)
    assert AmemBridge.__new__(AmemBridge)._dream_llm_client("user-1") is client
    assert client.max_tokens == 800
    assert client.timeout_seconds == 12


def test_dream_never_uses_service_key_without_personal_key(monkeypatch):
    import profile_projector
    import settings_service

    monkeypatch.setenv("AUTO_DREAM_LLM_ENABLED", "true")
    monkeypatch.setenv("RECOMMEND_LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "server-key")
    monkeypatch.setattr(
        settings_service.SettingsService,
        "has_deepseek_api_key",
        lambda _self: False,
    )
    monkeypatch.setattr(
        profile_projector, "_default_llm_client",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("service key used")),
    )
    assert AmemBridge.__new__(AmemBridge)._dream_llm_client("user-1") is None


def test_amem_semantic_timeout_can_match_cloud_latency(tmp_path, monkeypatch):
    monkeypatch.setenv("AMEM_EMBEDDING_MODEL", "")
    monkeypatch.setenv("AMEM_SEMANTIC_TIMEOUT_MS", "5000")
    monkeypatch.setenv("AUTO_DREAM_ENABLED", "false")
    bridge = AmemBridge(str(tmp_path / "amem.sqlite3"))
    try:
        assert bridge.handle.runtime.config.hybrid_retrieval.semantic_timeout_ms == 5000
    finally:
        bridge.close()
