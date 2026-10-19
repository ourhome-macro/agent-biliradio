from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import task_app


def test_behavior_job_uses_embedded_memory_bridge(monkeypatch):
    seen = []
    monkeypatch.setenv("AMEM_TRANSPORT", "embedded")
    bridge = SimpleNamespace(record_behavior=lambda payload: seen.append(payload) or {"ok": True})
    monkeypatch.setattr(task_app, "_worker_memory", lambda: (bridge, object()))
    result = task_app.dispatch({"kind": "behavior", "payload_json": json.dumps({"event": "liked"})})
    assert result == {"ok": True}
    assert seen == [{"event": "liked"}]


def test_outbox_runs_inside_worker_lifecycle(monkeypatch):
    published = threading.Event()
    monkeypatch.setenv("AMEM_ENABLED", "false")
    monkeypatch.setattr(task_app, "init_db", lambda: None)
    monkeypatch.setattr(
        task_app, "publish_pending", lambda *_args: published.set()
    )
    task_app.start_runtime_services()
    try:
        assert published.wait(3)
    finally:
        task_app.stop_runtime_services()
    assert task_app._outbox_thread is None


def test_embedding_worker_stops_with_runtime_owner(monkeypatch):
    stopped = threading.Event()
    processed = threading.Event()
    monkeypatch.setattr(task_app, "_outbox_stop", stopped)
    bridge = SimpleNamespace(
        process_embedding_jobs=lambda **_kwargs: processed.set() or SimpleNamespace(processed=0)
    )
    thread = threading.Thread(target=task_app._embedding_loop, args=(bridge,))
    thread.start()
    assert processed.wait(2)
    stopped.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
