"""Real Vue/HTTP/SSE first-card comparison with an isolated controlled profile model."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "artifacts/perf/recommendation-first-result"


def serve(directory, port):
    # The fixture uses real application routes and Vue. No upstream requests.
    import requests
    import uvicorn
    from amem_bridge import NoopAmemBridge
    from database import get_connection
    from dialogue_service import MusicDialogueService
    from dialogue_task_service import DialogueTaskService
    from durable_jobs import claim, finish
    from models import Track
    from music_agent import current_job_id
    from profile_projector import ProfileProjector
    from recommendation_service import RecommendationService
    from request_spec import RequestSpec
    from settings_service import SettingsService
    from sse_event_client import SSEEventPublisher

    original = requests.Session.request

    def local_only(self, method, url, *args, **kwargs):
        from urllib.parse import urlsplit

        if urlsplit(url).hostname not in {"127.0.0.1", "localhost"}:
            raise RuntimeError("External network disabled in timing fixture")
        return original(self, method, url, *args, **kwargs)

    requests.Session.request = local_only
    import app as application
    from app import app
    from web_asgi import RadioASGI

    db = directory / "data" / "database.sqlite3"
    # Resolve the actual configured path instead of assuming a legacy filename.
    from database import DEFAULT_DB_PATH

    db = DEFAULT_DB_PATH
    SettingsService(db_path=db, user_id="legacy-owner").set_deepseek_api_key(
        "fixture-not-a-real-key"
    )
    calls = [0]

    class ControlledModel:
        def complete(self, **kwargs):
            calls[0] += 1
            time.sleep(3.2)
            return SimpleNamespace(content="{}", latency_ms=3200)

    projector = ProfileProjector(NoopAmemBridge(), llm_client=ControlledModel(), enabled=True)
    recommendations = RecommendationService(
        db, amem_bridge=NoopAmemBridge(), profile_projector=projector, auto_discovery=False
    )
    tracks = [
        Track(
            bvid=f"BV{i:010d}",
            cid=i + 1,
            title=f"Artist{i} - Music piano live {i}",
            owner=f"Artist{i}",
            owner_mid=i + 1,
            duration=60,
            type_name="音乐",
        )
        for i in range(256)
    ]
    recommendations.candidate_pool.admit(
        tracks, source="discovery_search", request_spec=RequestSpec(), query="音乐"
    )
    service = MusicDialogueService(db, recommendation_service=recommendations)
    bundle = SimpleNamespace(
        dialogue=service, recommendations=recommendations, library=recommendations.library
    )
    application._music_services_for_request = lambda: bundle
    tasks = DialogueTaskService(SSEEventPublisher(db_path=db, url=""))
    application.dialogue_task_service = tasks

    @app.get("/api/fixture/ready")
    def ready():
        return {"ready": True}

    @app.post("/api/fixture/reset")
    def reset():
        projector.clear_cache()
        return {"modelCalls": calls[0]}

    @app.get("/api/fixture/stats")
    def stats():
        with get_connection(db) as conn:
            states = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT status,COUNT(*) FROM durable_jobs WHERE kind='dialogue' GROUP BY status"
                )
            }
        return {"modelCalls": calls[0], "taskStates": states}

    def worker():
        while True:
            time.sleep(0.01)
            with get_connection(db) as conn:
                row = conn.execute(
                    "SELECT job_id FROM durable_jobs WHERE kind='dialogue' "
                    "AND status='queued' ORDER BY id LIMIT 1"
                ).fetchone()
                job = claim(conn, row[0]) if row else None
            if job:
                token = current_job_id.set(job["job_id"])
                result, error = None, None
                try:
                    payload = json.loads(job["payload_json"])
                    result = tasks._run(
                        service=service, user_id=job["user_id"], task_id=job["job_id"], **payload
                    )
                except Exception as caught:
                    error = caught
                finally:
                    current_job_id.reset(token)
                with get_connection(db) as conn:
                    finish(conn, job, result=result, error=error)

    threading.Thread(target=worker, daemon=True).start()
    uvicorn.run(
        RadioASGI(db_path=db), host="127.0.0.1", port=port, access_log=False, log_level="error"
    )


def measure(base, pairs):
    import requests
    from playwright.sync_api import sync_playwright

    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route("**/*", lambda r: r.continue_() if r.request.url.startswith(base) else r.abort())
        page.add_init_script("""
          const NativeSource = window.EventSource;
          window.EventSource = class extends NativeSource {
            constructor(...args) {
              super(...args);
              this.addEventListener('session', e => {
                const v=JSON.parse(e.data), s=v.payload?.session;
                if(!window.__started || window.__received || !s) return;
                const cards=(s.messages||[]).map(m=>m.card).filter(Boolean);
                if(cards.some(c=>c.kind==='recommendation_carousel' &&
                    !window.__old.includes(c.cardId) && c.recommendations?.length>=8))
                  window.__received=performance.now()-window.__started;
              });
            }
          };
        """)
        page.goto(base + "/#/agent")
        text = page.get_by_label("输入消息")
        text.wait_for(timeout=30000)
        for trial in range(pairs):
            for state in ("cold", "warm"):
                send = page.get_by_title("发送消息", exact=True)
                text.fill("推荐几首音乐")
                page.wait_for_function(
                    "!document.querySelector('button[title=\"发送消息\"]').disabled", timeout=30000
                )
                if state == "cold":
                    requests.post(base + "/api/fixture/reset", timeout=5).raise_for_status()
                page.evaluate("""() => {
                  window.__old=Array.from(document.querySelectorAll('.recommendation_carousel'))
                    .map(e=>e.dataset.cardId);
                  window.__received=0;window.__started=performance.now();
                }""")
                send.click()
                page.wait_for_function(
                    """() => Array.from(document.querySelectorAll('.recommendation_carousel'))
                  .some(e=>!window.__old.includes(e.dataset.cardId) &&
                    e.querySelectorAll('.song-glass-card').length>=8 &&
                    Array.from(e.querySelectorAll('.song-glass-card')).some(s=>{
                      const r=s.getBoundingClientRect();return r.bottom>0 && r.top<innerHeight;
                    }))""",
                    timeout=30000,
                )
                values = page.evaluate("""() => new Promise(resolve=>requestAnimationFrame(()=>
                  resolve({visibleMs:performance.now()-window.__started,
                           receivedMs:window.__received})))""")
                rows.append({"trial": trial, "cache": state, **values})
        deadline = time.monotonic() + 15
        while True:
            final = requests.get(base + "/api/fixture/stats", timeout=5).json()
            if final["taskStates"] == {"completed": pairs * 2}:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Timing fixture tasks did not finish cleanly")
            time.sleep(0.1)
        browser.close()
    return {"samples": rows, **final}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--mysql", action="store_true")
    parser.add_argument("--baseline", default="fcaf202c92e967671638ede54241e5be20b00951")
    args = parser.parse_args()
    if args.server:
        serve(args.server, args.port)
        return
    if not 1 <= args.pairs <= 10:
        parser.error("pairs must be between 1 and 10 for the bounded fixture inventory")
    import requests
    from quality import isolated_environment

    output = OUTPUT / "mysql" if args.mysql else OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    baseline = (
        subprocess.check_output(
            ["git", "rev-parse", "--verify", "--end-of-options", args.baseline + "^{commit}"]
        )
        .decode()
        .strip()
    )
    result = {
        "scope": "Real Vue + HTTP + replayable SSE + "
        + ("native MySQL" if args.mysql else "SQLite")
        + "; fixture identities; "
        "controlled 3200ms profile model, no external provider or Kafka; sequential requests",
        "baselineCommit": baseline,
        "sourceHashes": {
            str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (ROOT / "recommend-radio/backend").glob("*.py")
            if p.name
            in {
                "dialogue_service.py",
                "dialogue_task_service.py",
                "dialogue_delivery.py",
                "recommendation_service.py",
                "web_asgi.py",
                "content_embeddings.py",
                "profile_projector.py",
                "keyword_governance.py",
            }
        },
    }
    with tempfile.TemporaryDirectory(prefix="recommendation-first-", dir=OUTPUT.parent) as root:
        for arm in ("before", "after"):
            directory = Path(root) / arm
            directory.mkdir()
            overlay = directory / "overlay"
            overlay.mkdir()
            if arm == "before":
                for name in [
                    "dialogue_service.py",
                    "dialogue_task_service.py",
                    "recommendation_service.py",
                    "web_asgi.py",
                ]:
                    source = subprocess.check_output(
                        ["git", "show", baseline + ":recommend-radio/backend/" + name]
                    )
                    (overlay / name).write_bytes(source)
            env = isolated_environment(str(directory))
            env.update(
                APP_SECRET_KEY="isolated-recommendation-timing-fixture-" + "x" * 32,
                APP_DATA_DIR=str(directory / "data"),
                AUTH_MODE="disabled",
                ALLOW_INSECURE_LOCAL_AUTH="1",
                RECOMMEND_LLM_ENABLED="true",
                RECOMMEND_DIALOGUE_ROUTER_LLM_ENABLED="false",
                AMEM_ENABLED="false",
                AMEM_EMBEDDING_BASE_URL="",
                RECOMMEND_LLM_PROVIDER="deepseek",
                JOB_TRANSPORT="disabled",
                SSE_GATEWAY_INTERNAL_URL="",
                FRONTEND_DIST=str(ROOT / "recommend-radio/frontend/dist"),
            )
            env["PYTHONPATH"] = str(overlay) + os.pathsep + env["PYTHONPATH"]
            native = None
            if args.mysql:
                sys.path[:0] = [
                    str(ROOT / "recommend-radio/scripts"),
                    str(ROOT / "recommend-radio/backend"),
                ]
                from mysql_fixture import NativeMySQL

                os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
                native = NativeMySQL(directory / "mysql")
                env["DATABASE_URL"] = str(native.url)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            with (output / (arm + ".log")).open("w", encoding="utf-8") as log:
                process = subprocess.Popen(
                    [sys.executable, __file__, "--server", str(directory), "--port", str(port)],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
                base = f"http://127.0.0.1:{port}"
                try:
                    deadline = time.monotonic() + 120
                    while True:
                        if process.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError("Timing fixture did not start; inspect its log")
                        try:
                            if requests.get(base + "/api/fixture/ready", timeout=1).ok:
                                break
                        except requests.RequestException:
                            pass
                        time.sleep(0.1)
                    result[arm] = measure(base, args.pairs)
                    result[arm]["underThreeSeconds"] = all(
                        row["visibleMs"] < 3000 for row in result[arm]["samples"]
                    )
                    print(json.dumps({"arm": arm, **result[arm]}), flush=True)
                    (output / "summary.json").write_text(
                        json.dumps(result, indent=2), encoding="utf-8"
                    )
                    if arm == "after" and not result[arm]["underThreeSeconds"]:
                        raise RuntimeError("Controlled first-card latency exceeded three seconds")
                finally:
                    process.terminate()
                    process.wait(timeout=15)
                    if native:
                        native.close()


if __name__ == "__main__":
    main()
