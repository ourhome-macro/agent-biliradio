from __future__ import annotations

import asyncio
import sqlite3
import sys
from types import SimpleNamespace

from database import get_connection, init_db
from migrate_legacy_sse import import_legacy_sse_events
from sse_event_client import SSEEventPublisher
from sse_event_store import append_event, latest_event_id, list_events_after


def test_sse_event_is_transactional_and_duplicate_delivery_is_idempotent(tmp_path):
    path = tmp_path / "radio.sqlite3"
    init_db(path)
    publisher = SSEEventPublisher(path, url="")
    event = dict(
        task_id="task", session_id="session", user_id="alice",
        event_type="progress", payload={"stage": "search"}, status="running",
    )
    with get_connection(path) as connection:
        first = publisher.publish(connection=connection, **event)
        assert publisher.publish(connection=connection, **event) == first
    assert latest_event_id(path, user_id="alice", session_id="session") == int(first)
    assert len(list_events_after(path, user_id="alice", session_id="session", after_id=0)) == 1

    try:
        with get_connection(path) as connection:
            publisher.publish(connection=connection, task_id="rollback", **{
                key: value for key, value in event.items() if key != "task_id"
            })
            raise RuntimeError("rollback")
    except RuntimeError:
        pass
    assert latest_event_id(path, user_id="alice", session_id="session") == int(first)


def test_sse_replay_paginates_past_500_without_cross_user_events(tmp_path):
    path = tmp_path / "radio.sqlite3"
    init_db(path)
    with get_connection(path) as connection:
        for index in range(505):
            append_event(
                connection, task_id="task", session_id="session", user_id="alice",
                event_type="progress", payload={"index": index},
            )
        append_event(
            connection, task_id="other", session_id="session", user_id="bob",
            event_type="progress", payload={"index": "private"},
        )
    first = list_events_after(path, user_id="alice", session_id="session", after_id=1)
    second = list_events_after(
        path, user_id="alice", session_id="session", after_id=first[-1]["eventId"]
    )
    assert len(first) == 500 and len(second) == 4
    assert [item["payload"]["index"] for item in [*first, *second]] == list(range(1, 505))
    assert all(item["userId"] == "alice" for item in [*first, *second])


def test_asgi_sse_authentication_and_replay(tmp_path, monkeypatch):
    import importlib

    from flask import Flask
    from starlette.requests import Request

    path = tmp_path / "radio.sqlite3"
    init_db(path)
    with get_connection(path) as connection:
        for index in range(3):
            append_event(
                connection, task_id="task", session_id="session", user_id="alice",
                event_type="progress", payload={"index": index},
            )
    identity = SimpleNamespace(
        cookie_name="session", current_user=lambda token, touch=False: (
            {"id": "alice"} if token == "valid" else None
        )
    )
    monkeypatch.setitem(sys.modules, "app", SimpleNamespace(app=Flask("test"), oidc_auth=identity))
    sys.modules.pop("web_asgi", None)
    web_asgi = importlib.import_module("web_asgi")
    server = web_asgi.RadioASGI(db_path=path, frontend_root=tmp_path)

    def request(cookie):
        return Request({
            "type": "http", "method": "GET", "path": "/api/agent/events",
            "query_string": b"sessionId=session&lastEventId=1",
            "headers": [(b"cookie", f"session={cookie}".encode())],
            "server": ("localhost", 3000), "client": ("127.0.0.1", 1234),
            "scheme": "http", "http_version": "1.1",
        })

    async def run():
        denied = await server._stream(request("invalid"))
        assert denied.status_code == 401
        response = await server._stream(request("valid"))
        assert response.status_code == 200
        stream = response.body_iterator
        chunks = [await anext(stream) for _ in range(3)]
        await stream.aclose()
        assert chunks[0] == "retry: 3000\n\n"
        assert "id: 2" in chunks[1]
        assert "id: 3" in chunks[2]

    try:
        asyncio.run(run())
    finally:
        sys.modules.pop("web_asgi", None)


def test_legacy_sse_history_import_preserves_event_ids(tmp_path):
    source = tmp_path / "sse-events.sqlite3"
    target = tmp_path / "radio.sqlite3"
    init_db(target)
    with sqlite3.connect(source) as old:
        old.execute(
            """CREATE TABLE dialogue_events (
            event_id INTEGER PRIMARY KEY, task_id TEXT, session_id TEXT, user_id TEXT,
            event_type TEXT, status TEXT, payload_json BLOB, created_at TEXT)"""
        )
        old.execute(
            "INSERT INTO dialogue_events VALUES (42,'task','session','alice',"
            "'done','completed',?,'2026-09-26T00:00:00+00:00')",
            (b'{"ok":true}',),
        )
    assert import_legacy_sse_events(target, source) == 1
    assert import_legacy_sse_events(target, source) == 0
    rows = list_events_after(target, user_id="alice", session_id="session", after_id=41)
    assert rows[0]["eventId"] == 42
    assert rows[0]["payload"] == {"ok": True}


def test_asgi_routes_api_and_static_files_without_a_proxy_process(tmp_path, monkeypatch):
    import importlib

    import httpx
    from flask import Flask

    static = tmp_path / "dist"
    static.mkdir()
    (static / "index.html").write_text("radio-ui", encoding="utf-8")
    fake = Flask("test")
    fake.add_url_rule("/api/ping", "ping", lambda: "api-ok")
    monkeypatch.setitem(
        sys.modules, "app",
        SimpleNamespace(app=fake, oidc_auth=SimpleNamespace(cookie_name="session")),
    )
    sys.modules.pop("web_asgi", None)
    web_asgi = importlib.import_module("web_asgi")
    server = web_asgi.RadioASGI(db_path=tmp_path / "radio.sqlite3", frontend_root=static)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server), base_url="http://test"
        ) as client:
            assert (await client.get("/api/ping")).text == "api-ok"
            assert (await client.get("/")).text == "radio-ui"
            assert (await client.get("/music/history")).text == "radio-ui"

    try:
        asyncio.run(run())
    finally:
        sys.modules.pop("web_asgi", None)
