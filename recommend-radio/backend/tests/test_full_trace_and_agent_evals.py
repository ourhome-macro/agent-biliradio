from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from agent_trace_observer import PersistedAgentTraceObserver
from amem_bridge import NoopAmemBridge
from database import get_connection, init_db
from discovery_planner import DiscoveryPlanner
from discovery_service import DiscoveryService
from full_trace import FullTrace, current_trace_id, load_trace_tree
from models import Track
from music_profile import MusicProfile
from recommendation_service import RecommendationService
from request_spec import RequestSpec
from trace_metrics import summarize_traces


def test_full_trace_persists_hierarchy_spans_and_redaction(tmp_path) -> None:
    db_path = tmp_path / "trace.sqlite3"
    init_db(db_path)
    with FullTrace(
        str(db_path),
        trace_type="dialogue",
        user_id="legacy-owner",
        attributes={"authorization": "Bearer secret", "safe": "visible"},
    ) as parent:
        assert current_trace_id() == parent.trace_id
        with FullTrace(
            str(db_path),
            trace_type="recommendation",
            user_id="legacy-owner",
            parent_trace_id=current_trace_id(),
        ) as child:
            child.record_span(
                "candidate_pool.read",
                1.25,
                kind="retrieval",
                outputs={"candidateCount": 3},
            )

    with get_connection(db_path) as conn:
        child_row = conn.execute(
            "SELECT * FROM evaluation_traces WHERE trace_id=?",
            (child.trace_id,),
        ).fetchone()
        parent_row = conn.execute(
            "SELECT * FROM evaluation_traces WHERE trace_id=?",
            (parent.trace_id,),
        ).fetchone()
    assert child_row["parent_trace_id"] == parent.trace_id
    assert child_row["root_trace_id"] == parent.trace_id
    assert child_row["status"] == "completed"
    assert parent_row["status"] == "completed"
    assert "secret" not in parent_row["attributes_json"]
    assert json.loads(parent_row["attributes_json"])["authorization"] == "[REDACTED]"
    tree = load_trace_tree(str(db_path), child.trace_id)
    assert tree["rootTraceId"] == parent.trace_id
    assert len(tree["traces"]) == 2
    assert len(tree["spans"]) == 1


def test_recommendation_writes_full_trace_and_metrics(tmp_path) -> None:
    db_path = tmp_path / "recommendation.sqlite3"
    service = RecommendationService(
        db_path=db_path,
        amem_bridge=NoopAmemBridge(),
        auto_discovery=False,
    )
    result = service.list_recommendations(scene="home", limit=8)
    assert result["fullTraceId"] == result["debugTraceId"]
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT status FROM evaluation_traces WHERE trace_id=?",
            (result["fullTraceId"],),
        ).fetchone()
        span_names = {
            item["name"]
            for item in conn.execute(
                "SELECT name FROM evaluation_trace_spans WHERE trace_id=?",
                (result["fullTraceId"],),
            ).fetchall()
        }
        feedback_trace = conn.execute(
            """
            SELECT parent_trace_id, root_trace_id, status
            FROM evaluation_traces WHERE trace_type='feedback'
            """
        ).fetchone()
    assert row["status"] == "completed"
    assert {"scene_memory.retrieve", "profile.project", "candidate_pool.read"} <= span_names
    assert feedback_trace["parent_trace_id"] == result["fullTraceId"]
    assert feedback_trace["root_trace_id"].startswith('agent-run:')
    assert feedback_trace["status"] == "completed"
    metrics = summarize_traces(str(db_path))
    assert metrics["trace"]["count"] == 3
    assert metrics["trace"]["statusCounts"] == {"completed": 3}
    assert metrics["process"]["candidatePoolHitRate"] == 0.0
    assert metrics["process"]["toolCallSuccessRate"] == 1.0
    assert metrics["result"]["propensityCoverage"] is None


def test_discovery_trace_records_tool_and_admission_spans(tmp_path) -> None:
    class Client:
        @staticmethod
        def search(query, **kwargs):
            return [
                Track(
                    bvid="BV5000000001",
                    title=f"Artist - {query}",
                    duration=180,
                    type_name="音乐",
                )
            ]

    db_path = tmp_path / "discovery.sqlite3"
    init_db(db_path)
    service = DiscoveryService(
        str(db_path),
        user_id="legacy-owner",
        bili_client=Client(),
        planner=DiscoveryPlanner(search_budget=1),
    )
    service.content_embeddings.text_base_url = ""
    result = service.discover_now(
        profile=MusicProfile.empty(),
        request_spec=RequestSpec(required_genres=("rnb",)),
        scene="home",
        limit=1,
    )
    with get_connection(db_path) as conn:
        spans = {
            row["name"]: row["status"]
            for row in conn.execute(
                "SELECT name, status FROM evaluation_trace_spans WHERE trace_id=?",
                (result["fullTraceId"],),
            ).fetchall()
        }
    assert spans["bilibili.search"] == "completed"
    assert spans["candidate.admit"] == "completed"
    assert spans["candidate.embedding.enqueue"] == "completed"
    assert "candidate.embedding.persist" not in spans
    metrics = summarize_traces(str(db_path))
    assert metrics["process"]["toolCallSuccessRate"] == 1.0
    assert metrics["process"]["invalidRecallRate"] == 0.0


def test_agent_runtime_observer_records_tool_and_terminal_events(tmp_path) -> None:
    db_path = tmp_path / "agent.sqlite3"
    init_db(db_path)
    observer = PersistedAgentTraceObserver(str(db_path))
    common = {
        "run_id": "run-1",
        "tenant_id": "tenant",
        "agent_id": "agent",
        "session_id": "session",
        "execution_id": "execution",
    }
    asyncio.run(
        observer.on_event(
            SimpleNamespace(
                type="tool.started",
                sequence=1,
                data={"call_id": "call-1"},
                event_id="event-1",
                **common,
            )
        )
    )
    asyncio.run(
        observer.on_event(
            SimpleNamespace(
                type="tool.completed",
                sequence=2,
                data={"call_id": "call-1", "status": "succeeded", "attempts": 1},
                event_id="event-2",
                **common,
            )
        )
    )
    asyncio.run(
        observer.on_event(
            SimpleNamespace(
                type="run.completed",
                sequence=3,
                data={"model_calls": 1, "tool_calls": 1},
                event_id="event-3",
                **common,
            )
        )
    )
    with get_connection(db_path) as conn:
        trace = conn.execute(
            "SELECT status FROM evaluation_traces WHERE trace_id='agent-run:run-1'"
        ).fetchone()
        span = conn.execute(
            "SELECT name, status FROM evaluation_trace_spans WHERE trace_id='agent-run:run-1'"
        ).fetchone()
    assert trace["status"] == "completed"
    assert (span["name"], span["status"]) == ("tool.completed", "completed")
