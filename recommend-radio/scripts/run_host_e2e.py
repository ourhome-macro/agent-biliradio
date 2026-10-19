"""Exercise host Web/SSE, SQLite outbox, RabbitMQ and embedded AMEM together."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import requests

BACKEND = Path(__file__).resolve().parents[1] / "backend"
sys.path.insert(0, str(BACKEND))

from amem_bridge import AmemBridge  # noqa: E402
from database import get_connection  # noqa: E402
from durable_jobs import enqueue_behavior  # noqa: E402
from models import Track  # noqa: E402
from sse_event_client import SSEEventPublisher  # noqa: E402

from agent_memory_runtime.domain.query import MemoryQuery  # noqa: E402
from agent_memory_runtime.memory.intake.models import MemoryProposal  # noqa: E402


def _read_event(url: str, session_id: str, *, after_id: int | None = None) -> dict:
    params: dict[str, str | int] = {"sessionId": session_id}
    if after_id is not None:
        params["lastEventId"] = after_id
    with requests.get(
        f"{url}/api/agent/events", params=params, stream=True, timeout=(5, 20)
    ) as response:
        response.raise_for_status()
        for line in response.iter_lines(chunk_size=1, decode_unicode=True):
            if line.startswith("data:"):
                return json.loads(line[5:].strip())
    raise AssertionError("SSE connection closed without an event")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:35002")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=40.0)
    args = parser.parse_args()
    db = args.data_dir / "bili_radio.sqlite3"
    amem = args.data_dir / "amem.sqlite3"
    nonce = uuid4().hex[:10]
    session_id = f"smoke-session-{nonce}"
    task_id = f"smoke-task-{nonce}"
    health = requests.get(f"{args.url}/health/ready", timeout=5)
    health.raise_for_status()
    page = requests.get(args.url, timeout=5)
    assert page.status_code == 200 and '<div id="app">' in page.text
    identity = requests.get(f"{args.url}/api/session/me", timeout=5)
    identity.raise_for_status()
    assert identity.json()["data"]["authenticated"]

    publisher = SSEEventPublisher(db_path=db, url="")
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(_read_event, args.url, session_id)
        time.sleep(0.4)
        first_id = int(publisher.publish(
            task_id=task_id, session_id=session_id, user_id="legacy-owner",
            event_type="progress", payload={"stage": "first"},
        ))
        first = pending.result(timeout=25)
    assert first["eventId"] == first_id and first["payload"]["stage"] == "first"
    second_id = int(publisher.publish(
        task_id=task_id, session_id=session_id, user_id="legacy-owner",
        event_type="done", payload={"stage": "second"}, status="completed",
    ))
    replay = _read_event(args.url, session_id, after_id=first_id)
    assert replay["eventId"] == second_id and replay["type"] == "done"

    track = Track(bvid="BVsmoke00001", title="Synthetic Jazz", tags=("Jazz",))
    with get_connection(db) as connection:
        job_id = enqueue_behavior(
            connection, user_id="legacy-owner", event="liked", scene="home",
            track=track, event_id=f"synthetic-{nonce}",
        )
    assert job_id
    deadline = time.monotonic() + args.timeout
    status = "queued"
    while time.monotonic() < deadline:
        with get_connection(db) as connection:
            row = connection.execute(
                "SELECT status FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        status = row[0]
        if status in {"completed", "failed", "needs_reconciliation"}:
            break
        time.sleep(0.4)
    assert status == "completed", f"behavior job ended as {status}"
    with sqlite3.connect(f"{amem.resolve().as_uri()}?mode=ro", uri=True) as connection:
        stored = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_id = ?", (job_id,)
        ).fetchone()[0]
    assert stored == 1
    bridge = AmemBridge(str(amem))
    try:
        proposal = MemoryProposal(
            proposal_id=f"smoke-memory-{nonce}", source="smoke", action="create",
            target_memory_id=None, subject_id="legacy-owner", key=f"jazz-{nonce}",
            content="The user enjoys gentle jazz piano on a quiet evening.",
            memory_type="belief", visible_to=("recommend-radio",),
            confidence=0.92, salience=0.85, agent_id="recommend-radio",
            tenant_id="recommend-radio", user_id="legacy-owner",
            session_id="music-profile", labels=("private",),
            tags=("music", "recommend-radio"), level="L1", visibility="private",
        )
        result = bridge.handle.runtime.apply_memory_proposal(proposal)
        assert result.status == "succeeded" and result.memory is not None
        deadline = time.monotonic() + args.timeout
        embedding_status = "pending"
        while time.monotonic() < deadline:
            with sqlite3.connect(f"{amem.resolve().as_uri()}?mode=ro", uri=True) as connection:
                row = connection.execute(
                    "SELECT status FROM embedding_jobs WHERE memory_id = ? "
                    "ORDER BY created_at DESC LIMIT 1",
                    (result.memory.memory_id,),
                ).fetchone()
            embedding_status = row[0] if row else "missing"
            if embedding_status in {"succeeded", "dead_letter"}:
                break
            time.sleep(0.4)
        assert embedding_status == "succeeded", f"embedding ended as {embedding_status}"
        query = MemoryQuery(
            agent_id="recommend-radio", tenant_id="recommend-radio", user_id="legacy-owner",
            session_id="music-profile", session_policy="exact", visibilities=("private",),
            text="安静夜晚的爵士钢琴音乐", limit=5,
        )
        found, trace = bridge.handle.runtime.retrieve(query)
        semantic_hit = any(item.memory_id == result.memory.memory_id for item in found)
        assert semantic_hit and "semantic" in trace.retrieval_legs
    finally:
        bridge.close()
    print(json.dumps({
        "passed": True,
        "web": "ready",
        "frontend": "ready",
        "sse_live": True,
        "sse_replay": True,
        "behavior_job": status,
        "amem_event_once": stored == 1,
        "cloud_embedding_job": embedding_status,
        "cross_language_semantic_hit": semantic_hit,
    }))


if __name__ == "__main__":
    main()
