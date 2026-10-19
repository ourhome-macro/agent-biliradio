"""Integration gates exercise persisted retrieval, real ranking and feedback evidence."""

from __future__ import annotations

import json
import time
from dataclasses import replace

import fakeredis
import pytest
from amem_bridge import NoopAmemBridge
from database import get_connection
from error_code import APIError
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.media import MediaService
from feed_runtime.repository import FeedRepository
from models import Track
from recommendation_contracts import UserProfile
from recommendation_service import RecommendationService
from request_spec import RequestSpec


@pytest.fixture
def serving(tmp_path, monkeypatch):
    for key, value in {
        "RABBITMQ_ENABLED": "false",
        "RECOMMEND_LLM_ENABLED": "false",
        "RECOMMEND_TEXT_EMBEDDING_BASE_URL": "",
        "RECOMMEND_AUDIO_EMBEDDING_BASE_URL": "",
    }.items():
        monkeypatch.setenv(key, value)
    repo = FeedRepository(tmp_path / "quality.sqlite3")
    config = replace(FeedConfig(), workdir=tmp_path / "work", cancel_grace_seconds=0)
    recommendations = RecommendationService(
        db_path=repo.db_path, amem_bridge=NoopAmemBridge(), auto_discovery=False
    )
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    yield repo, config, cache, recommendations
    cache.close()


def test_personal_feed_uses_real_ranker_frozen_profile_and_revised_request(serving, monkeypatch):
    repo, config, cache, recommendations = serving
    stages = [
        Track(
            bvid=f"BV{i:010d}",
            cid=i + 1,
            title=f"音乐 唱跳舞台 现场 {i}",
            type_name="音乐",
            duration=60,
        )
        for i in range(24)
    ]
    other = Track(
        bvid="BV9999999999", cid=100, title="音乐 钢琴演奏 安静独奏", type_name="音乐", duration=60
    )
    recommendations.candidate_pool.admit(
        [*stages, other], source="discovery_search", request_spec=RequestSpec(), query="音乐"
    )
    service = FeedService(repo, config, cache, recommendations=recommendations)
    first = service.create("legacy-owner", request_key="stage", request_text="推荐唱跳舞台")
    assert len(first["items"]) == 10
    assert all("唱跳舞台" in item["track"]["title"] for item in first["items"])
    original = recommendations.profile_reader._load_user_profile

    def forbidden():
        raise AssertionError("pagination must use the committed profile snapshot")

    monkeypatch.setattr(recommendations.profile_reader, "_load_user_profile", forbidden)
    second = service.page("legacy-owner", first["sessionId"], cursor=first["nextCursor"])
    assert len(second["items"]) == 10
    assert not {i["contentId"] for i in first["items"]} & {i["contentId"] for i in second["items"]}
    monkeypatch.setattr(recommendations.profile_reader, "_load_user_profile", original)
    corrected = service.create(
        "legacy-owner", request_key="piano", request_text="不要 Rap，来点钢琴纯音乐"
    )
    assert corrected["revision"] != first["revision"]
    assert any(i["track"]["trackId"] == other.track_id for i in corrected["items"])
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM recommendation_impressions").fetchone()[0] == 0
        for row in conn.execute("SELECT snapshot_json FROM feed_items"):
            assert json.loads(row[0])["decision"], "each personal item must pass the ranker"


def test_shared_catalog_cannot_bypass_personal_hard_filters_or_music_gate(serving, monkeypatch):
    repo, config, cache, recommendations = serving
    blocked = Track(
        bvid="BV0000000001", cid=1, title="音乐现场 blocked", type_name="音乐", owner_mid=999
    )
    allowed = Track(
        bvid="BV0000000002", cid=2, title="音乐现场 allowed", type_name="音乐", owner_mid=100
    )
    noise = Track(bvid="BV0000000003", cid=3, title="数据库入门知识讲解", type_name="知识")
    for track in (blocked, allowed, noise):
        repo.admit(track, user_id="legacy-owner")
    monkeypatch.setattr(
        recommendations.profile_reader,
        "_load_user_profile",
        lambda: UserProfile(negative_owner_mids={999}),
    )
    service = FeedService(repo, config, cache, recommendations=recommendations)
    page = service.create("legacy-owner", request_key="filter")
    assert [i["track"]["trackId"] for i in page["items"]] == [allowed.track_id]
    assert len(repo.list_content("legacy-owner")) == 2


def test_playback_evidence_is_validated_and_logical_completion_is_idempotent(serving, monkeypatch):
    repo, config, cache, _ = serving
    content = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live", duration=12), user_id="legacy-owner"
    )
    service = FeedService(repo, config, cache)
    item = service.create("legacy-owner", request_key="one")["items"][0]
    media = MediaService(repo, config, None)
    playback = media.create("legacy-owner", content, "one")
    with get_connection(repo.db_path) as conn:
        conn.execute("UPDATE media_playbacks SET created_at=?", (time.time() - 15,))
        conn.execute("UPDATE media_import_jobs SET complete_source=1")
    event = {
        "eventId": "end-1",
        "itemId": item["itemId"],
        "type": "complete",
        "playbackId": playback["playbackId"],
        "watchMs": 12000,
    }
    monkeypatch.setenv("RABBITMQ_ENABLED", "true")
    service.events("legacy-owner", [event])
    service.events("legacy-owner", [{**event, "eventId": "end-2"}])
    for bad in (None, "12000", True, float("nan")):
        with pytest.raises(APIError):
            service.events("legacy-owner", [{**event, "eventId": "bad", "watchMs": bad}])
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM feed_events WHERE event_type='complete'").fetchone()[
                0
            ]
            == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE kind='behavior' "
                "AND payload_json LIKE '%completed%'"
            ).fetchone()[0]
            == 1
        )


def test_new_viewer_after_idle_check_prevents_cancellation(serving):
    repo, config, _, _ = serving
    content = repo.admit(
        Track(bvid="BV0000000001", cid=1, title="Music live", duration=12), user_id="legacy-owner"
    )
    media = MediaService(repo, config, None)
    first = media.create("legacy-owner", content, "first")
    with get_connection(repo.db_path) as conn:
        key = conn.execute("SELECT import_id FROM media_import_jobs").fetchone()[0]
    job = repo.claim_import(key, config)
    media.release(first["playbackId"], "legacy-owner")
    repo.demand(job, config)
    assert not repo.demand(job, config)
    second = media.create("legacy-owner", content, "second")
    assert not repo.cancel_if_idle(job, config)
    assert second["assetId"] == first["assetId"]


def test_full_sqlite_hybrid_retrieval_context_budget_and_private_scope(tmp_path):
    from agent_memory_runtime.config import HybridRetrievalConfig, RuntimeConfig
    from agent_memory_runtime.domain.memory import MemoryRecord
    from agent_memory_runtime.domain.query import MemoryQuery
    from agent_memory_runtime.memory.embeddings.base import CallableEmbeddingProvider
    from agent_memory_runtime.memory.embeddings.models import EmbeddingSpec
    from agent_memory_runtime.memory.embeddings.worker import EmbeddingWorker
    from agent_memory_runtime.memory.stores.sqlite import SQLiteStoreBundle
    from agent_memory_runtime.runtime import AgentMemoryRuntime

    def vector(text):
        return (
            [1.0, 0.0]
            if any(term in text.casefold() for term in ("piano", "keys", "calm"))
            else [0.0, 1.0]
        )

    provider = CallableEmbeddingProvider(
        EmbeddingSpec("fixture", "semantic-music", 2),
        query_embedder=vector,
        document_embedder=lambda texts: [vector(text) for text in texts],
    )
    stores = SQLiteStoreBundle(tmp_path / "memory.sqlite3")
    stores.embedding_generations.register(provider.spec, status="backfill")
    for identity, user, content in (
        ("wanted", "alice", "I like calm piano music."),
        ("other-user", "bob", "I like calm piano music."),
        ("noise", "alice", "My shipping address is overseas."),
    ):
        stores.memory_store.upsert(
            MemoryRecord(
                memory_id=identity,
                memory_type="preference",
                session_id="s",
                subject_id=user,
                content=content,
                source_event_ids=(),
                rule_id="fixture",
                tenant_id="radio",
                user_id=user,
                agent_id="music",
                owner_id="music",
                labels=("private",),
                visibility="private",
            )
        )
    worker = EmbeddingWorker(
        provider=provider,
        jobs=stores.embedding_jobs,
        vectors=stores.vector_index,
        memories=stores.memory_store,
    )
    while worker.run_once() is not None:
        pass
    stores.activate_embedding_generation(provider.spec.generation)
    stores.memory_store.embedding_provider = provider
    runtime = AgentMemoryRuntime(
        config=RuntimeConfig(
            context_token_budget=256,
            hybrid_retrieval=HybridRetrievalConfig(min_semantic_similarity=0.8),
        ),
        memory_store=stores.memory_store,
        event_store=stores.event_store,
        snapshot_store=stores.snapshot_store,
        audit_store=stores.audit_store,
        tombstone_store=stores.tombstone_store,
        transaction_manager=stores,
    )
    try:
        context = runtime.project(
            MemoryQuery(
                agent_id="music",
                text="calm keys for tonight",
                session_id="s",
                tenant_id="radio",
                user_id="alice",
            )
        )
        assert "wanted" in context.selected_memory_ids
        assert (
            "other-user" not in context.selected_memory_ids
            and "noise" not in context.selected_memory_ids
        )
        assert (
            context.trace.semantic_candidate_count > 0 and context.trace.lexical_candidate_count > 0
        )
        assert context.metadata["estimated_memory_tokens"] <= 256
    finally:
        runtime.close()
