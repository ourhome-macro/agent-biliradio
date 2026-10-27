from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
from amem_bridge import NoopAmemBridge
from content_embeddings import ContentEmbeddingService
from database import get_connection, init_db
from dialogue_delivery import DialogueProgress
from dialogue_service import MusicDialogueService
from embedding_jobs import enqueue_text, execute
from library_service import LibraryService
from models import Track
from profile_projector import ProfileProjector
from recommendation_service import RecommendationService
from request_spec import RequestSpec
from sse_event_client import SSEEventPublisher
from sse_event_store import list_events_after


@pytest.fixture
def sql_database(tmp_path):
    root_url = os.getenv("FIRST_RESULT_MYSQL_ADMIN_URL")
    if not root_url:
        yield tmp_path / "first-result.sqlite3"
        return
    import sqlalchemy as sa
    from mysql_fixture import SecretURL
    from mysql_storage.connection import engine_for

    root = sa.engine.make_url(root_url)
    assert root.host in {"127.0.0.1", "localhost"} and root.port not in {None, 3306, 3307}
    name = "first_result_" + uuid4().hex[:16]
    admin = engine_for(root_url)
    with admin.begin() as conn:
        conn.execute(sa.schema.CreateSchema(name))
    url = SecretURL(root.set(database=name).render_as_string(hide_password=False))
    try:
        yield url
    finally:
        engine_for(str(url)).dispose()
        with admin.begin() as conn:
            conn.execute(sa.schema.DropSchema(name))


@pytest.fixture
def serving(sql_database, monkeypatch):
    monkeypatch.setenv("RECOMMEND_LLM_ENABLED", "true")
    monkeypatch.setenv("AMEM_EMBEDDING_BASE_URL", "")
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    monkeypatch.setenv("SSE_GATEWAY_INTERNAL_URL", "")
    db = sql_database
    init_db(db)
    projector = ProfileProjector(NoopAmemBridge(), enabled=True)

    def forbidden(**kwargs):
        pytest.fail("Conversation serving must not synthesize a profile with an LLM")

    monkeypatch.setattr(projector, "project", forbidden)
    recommendations = RecommendationService(
        db, amem_bridge=NoopAmemBridge(), profile_projector=projector, auto_discovery=False
    )
    tracks = [
        Track(
            bvid=f"BV{i:010d}",
            cid=i + 1,
            title=f"Music live piano {i}",
            owner=f"Artist{i}",
            owner_mid=i + 1,
            type_name="音乐",
            duration=60,
        )
        for i in range(12)
    ]
    recommendations.candidate_pool.admit(
        tracks, source="discovery_search", request_spec=RequestSpec(), query="音乐"
    )
    service = MusicDialogueService(db, recommendation_service=recommendations)
    return db, recommendations, service


def test_conversation_ranking_uses_committed_profile(serving):
    _db, recommendations, _service = serving
    result = recommendations.list_recommendations(scene="conversation", request_spec=RequestSpec())
    assert len(result["items"]) == 8
    assert not result["timing"].get("profileLlmApiMs")


def test_conversation_supply_and_analysis_do_not_synthesize_profiles(serving):
    db, recommendations, service = serving
    assert recommendations.enqueue_discovery(scene="conversation")
    assert service._safe_analysis()["profileTraceId"].startswith("profile:committed:")
    with get_connection(db) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_jobs WHERE kind='discovery'").fetchone()[0]
            == 1
        )


def test_first_card_event_is_visible_before_optional_analysis(serving, monkeypatch):
    db, _recommendations, service = serving
    session_id = service.resolve_session_id()
    publisher = SSEEventPublisher(db_path=db, url="")
    progress = DialogueProgress(
        publisher, task_id="first", session_id=session_id, user_id="legacy-owner"
    )

    def analysis():
        events = list_events_after(db, user_id="legacy-owner", session_id=session_id, after_id=0)
        ready = [e for e in events if e["payload"].get("firstRecommendation")]
        assert len(ready) == 1 and ready[0]["status"] == "running"
        assert len(ready[0]["payload"]["session"]["messages"][-1]["card"]["recommendations"]) == 8
        assert not any(e["type"] == "done" for e in events)
        return {}

    monkeypatch.setattr(service, "_safe_analysis", analysis)
    result = service.send_message("推荐几首音乐", session_id=session_id, progress=progress)
    assert result["messages"][-1]["card"]["recommendations"]


@pytest.mark.parametrize("remote", [False, True])
def test_card_and_first_event_roll_back_together(serving, monkeypatch, remote):
    db, _recommendations, service = serving
    session_id = service.resolve_session_id()
    publisher = SSEEventPublisher(db_path=db, url="http://gateway.invalid" if remote else "")
    progress = DialogueProgress(
        publisher, task_id="rollback", session_id=session_id, user_id="legacy-owner"
    )
    original = progress.recommendation_ready

    def fail(conn, value):
        original(conn, value)
        raise RuntimeError("rollback after appending event")

    monkeypatch.setattr(progress, "recommendation_ready", fail)
    with pytest.raises(RuntimeError):
        service.send_message("推荐几首音乐", session_id=session_id, progress=progress)
    with get_connection(db) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM agent_dialogue_cards WHERE kind='recommendation_carousel'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM dialogue_events WHERE event_type='session'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE kind='sse' "
                "AND json_extract(payload_json,'$.event_type')='session'"
            ).fetchone()[0]
            == 0
        )
    publisher.close()


def embedding_case(db, monkeypatch):
    init_db(db)
    monkeypatch.setenv("AMEM_EMBEDDING_BASE_URL", "http://embedding.invalid")
    library = LibraryService(db)
    track = Track(bvid="BV1234567890", cid=1, title="Artist - Music", owner="Artist")
    library.upsert_track(track)
    track = library.get_track(track.track_id)
    service = ContentEmbeddingService(str(db), user_id="legacy-owner")
    return db, library, track, service


def test_embedding_enqueue_is_durable_deduplicated_and_not_a_model_call(sql_database, monkeypatch):
    db, _library, track, service = embedding_case(sql_database, monkeypatch)
    monkeypatch.setattr(service, "_embed_texts", lambda *a: pytest.fail("Must not run on enqueue"))
    first, replay = enqueue_text(service, [track]), enqueue_text(service, [track])
    assert first["jobId"] == replay["jobId"] and first["queued"] == 1
    with get_connection(db) as conn:
        job = dict(
            conn.execute("SELECT * FROM durable_jobs WHERE kind='content_embedding'").fetchone()
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE kind='content_embedding'"
            ).fetchone()[0]
            == 1
        )
    monkeypatch.setattr(ContentEmbeddingService, "_embed_texts", lambda *a: [[1.0, 0.0]])
    from task_app import dispatch

    assert dispatch(job, db_path=db)["embedded"] == 1
    assert execute(db, job)["embedded"] == 0
    assert service.text_vectors([track.track_id])[track.track_id] == [1.0, 0.0]


def test_embedding_failure_is_not_acknowledged_as_success(sql_database, monkeypatch):
    db, _library, track, service = embedding_case(sql_database, monkeypatch)
    enqueue_text(service, [track])
    with get_connection(db) as conn:
        job = dict(
            conn.execute("SELECT * FROM durable_jobs WHERE kind='content_embedding'").fetchone()
        )
    monkeypatch.setattr(
        ContentEmbeddingService, "_embed_texts", lambda *a: (_ for _ in ()).throw(TimeoutError())
    )
    with pytest.raises(RuntimeError):
        execute(db, job)


def test_slow_embedding_cannot_publish_superseded_metadata(sql_database, monkeypatch):
    _db, library, track, service = embedding_case(sql_database, monkeypatch)

    def change(_texts):
        track.title = "Artist - Revised Music"
        library.upsert_track(track)
        return [[1.0, 0.0]]

    monkeypatch.setattr(service, "_embed_texts", change)
    result = service.ensure_text_embeddings([track])
    assert result["embedded"] == 0 and result["superseded"] == 1
    assert service.text_vectors([track.track_id]) == {}


def test_embedding_transport_uses_media_budget():
    from kafka_transport import lane_for as kafka_lane
    from redis_stream_jobs import lane_for as redis_lane

    assert kafka_lane("content_embedding") == redis_lane("content_embedding") == "media"


def test_conversation_keeps_strict_request_when_inventory_is_small(serving):
    _db, recommendations, _service = serving
    spec = RequestSpec(required_genres=("rnb",))
    track = Track(bvid="BV9999999999", cid=1, title="R&B music", duration=60, type_name="音乐")
    recommendations.candidate_pool.admit(
        [track], source="discovery_search", request_spec=spec, query="R&B"
    )
    result = recommendations.list_recommendations(scene="conversation", request_spec=spec)
    assert [i["track"]["trackId"] for i in result["items"]] == [track.track_id]


def test_router_timeout_does_not_silently_turn_into_rule_recommendations(serving, monkeypatch):
    import dialogue_service as module
    from error_code import APIError

    _db, _recommendations, service = serving
    monkeypatch.setattr(module, "_dialogue_router_llm_enabled", lambda: True)
    monkeypatch.setattr(module, "_is_high_confidence_route", lambda *a: False)
    service.router_llm_client = SimpleNamespace(
        complete_tool=lambda **kw: (_ for _ in ()).throw(TimeoutError())
    )
    with pytest.raises(APIError) as error:
        service._route_message("我不是让你推荐音乐", None, session_id=service.resolve_session_id())
    assert error.value.status_code == 504


def test_router_model_has_its_own_bounded_budget(monkeypatch):
    from profile_projector import RecommendationOpenAIChatClient

    import agent_memory_runtime.llm.transport as transport

    observed = {}

    def create(**kwargs):
        observed.update(kwargs)
        call = SimpleNamespace(
            type="function",
            function=SimpleNamespace(name="route_dialogue", arguments='{"tool":"recommend_music"}'),
        )
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(tool_calls=[call]))]
        )

    def client(**kwargs):
        observed["timeout"] = kwargs["timeout_seconds"]
        return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))

    monkeypatch.setattr(transport, "get_openai_client", client)
    monkeypatch.setenv("RECOMMEND_DIALOGUE_ROUTER_TIMEOUT_SECONDS", "1.2")
    model = RecommendationOpenAIChatClient(
        base_url="https://fixture.invalid",
        api_key_env="UNUSED",
        model="fixture",
        timeout_seconds=8,
        temperature=0,
        top_p=1,
        max_tokens=3000,
    )
    monkeypatch.setattr(model, "_api_key", lambda: "dummy")
    model.complete_tool(system_prompt="route", user_prompt="message", tools=[])
    assert observed["timeout"] == 1.2 and observed["max_tokens"] == 400


def test_query_embedding_budget_does_not_change_offline_worker_budget(tmp_path, monkeypatch):
    observed = []
    model = ContentEmbeddingService(str(tmp_path / "query.sqlite3"), timeout_seconds=15)
    model.text_base_url = "http://embedding.invalid"

    def encode(self, texts):
        observed.append(self.timeout_seconds)
        return [[1.0, 0.0] for _ in texts]

    monkeypatch.setattr(ContentEmbeddingService, "_embed_texts", encode)
    assert model.embed_query("piano") == [1.0, 0.0]
    model._embed_texts(["offline candidate"])
    assert observed == [0.25, 15] and model.timeout_seconds == 15


def test_empty_card_does_not_count_as_a_first_recommendation(sql_database):
    init_db(sql_database)
    publisher = SSEEventPublisher(sql_database, url="")
    progress = DialogueProgress(
        publisher, task_id="empty", session_id="empty", user_id="legacy-owner"
    )
    session = {"messages": [{"card": {"recommendations": []}}]}
    with get_connection(sql_database) as conn:
        progress.recommendation_ready(conn, session)
    events = list_events_after(sql_database, user_id="legacy-owner", session_id="empty", after_id=0)
    assert events[0]["type"] == "session" and events[0]["status"] == "running"
    assert events[0]["payload"]["firstRecommendation"] is False


def test_late_sse_connection_recovers_committed_cards_without_model_or_writes(
    serving, monkeypatch, tmp_path
):
    from flask import Flask
    from sse_event_store import append_event
    from starlette.requests import Request

    db, _recommendations, service = serving
    session_id = service.resolve_session_id()
    service.send_message("推荐几首音乐", session_id=session_id)
    identity = SimpleNamespace(
        cookie_name="session", current_user=lambda *a, **k: {"id": "legacy-owner"}
    )
    monkeypatch.setitem(
        sys.modules, "app", SimpleNamespace(app=Flask("snapshot"), oidc_auth=identity)
    )
    sys.modules.pop("web_asgi", None)
    module = importlib.import_module("web_asgi")
    original = MusicDialogueService.session_snapshot

    def racing_snapshot(self, sid):
        result = original(self, sid)
        with get_connection(db) as conn:
            append_event(
                conn,
                task_id="race",
                session_id=sid,
                user_id="legacy-owner",
                event_type="progress",
                payload={"stage": "after_cursor"},
            )
        return result

    monkeypatch.setattr(MusicDialogueService, "session_snapshot", racing_snapshot)
    server = module.RadioASGI(db_path=db, frontend_root=tmp_path)

    def request(sid):
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/agent/events",
                "query_string": f"sessionId={sid}".encode(),
                "headers": [],
                "server": ("localhost", 3000),
                "client": ("127.0.0.1", 1),
                "scheme": "http",
                "http_version": "1.1",
            }
        )

    async def run():
        response = await server._stream(request(session_id))
        stream = response.body_iterator
        assert await anext(stream) == "retry: 3000\n\n"
        snapshot = await anext(stream)
        data = json.loads(snapshot.split("data: ", 1)[1])
        assert data["payload"]["snapshot"] is True and data["status"] == "running"
        assert len(data["payload"]["session"]["messages"][-1]["card"]["recommendations"]) == 8
        assert "after_cursor" in await anext(stream)
        await stream.aclose()
        denied = await server._stream(request("does-not-exist"))
        assert denied.status_code == 404

    try:
        with get_connection(db) as conn:
            before = conn.execute("SELECT COUNT(*) FROM agent_dialogue_sessions").fetchone()[0]
        asyncio.run(run())
        with get_connection(db) as conn:
            assert (
                conn.execute("SELECT COUNT(*) FROM agent_dialogue_sessions").fetchone()[0] == before
            )
    finally:
        sys.modules.pop("web_asgi", None)
