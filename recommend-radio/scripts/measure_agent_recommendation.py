"""Measure one real dialogue recommendation from click through card completion."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "server-data" / "timing-review"
DEFAULT_PROMPT = "给我推荐一些唱跳舞台"


def backend_timings(task_id: str, session_id: str) -> dict:
    program = r"""
import json,sqlite3,sys
from datetime import datetime,timezone
task_id,session_id=sys.argv[1],sys.argv[2]
db=sqlite3.connect('/app/data/bili_radio.sqlite3')
db.row_factory=sqlite3.Row
def rows(sql,args=()): return [dict(row) for row in db.execute(sql,args).fetchall()]
cards=rows("SELECT card_id,json_extract(payload_json,'$.discoveryJobId') AS discovery_job_id,json_extract(payload_json,'$.discoveryStatus') AS discovery_status,json_array_length(json_extract(payload_json,'$.recommendations')) AS count FROM agent_dialogue_cards WHERE session_id=? AND kind='recommendation_carousel' ORDER BY updated_at DESC",(session_id,))
discovery_ids=[row['discovery_job_id'] for row in cards if row['discovery_job_id']]
job_ids=[task_id,'watch:'+task_id,*discovery_ids]
jobs=[]
for job_id in job_ids:
    jobs+=rows("SELECT job_id,kind,status,created_at,updated_at,error FROM durable_jobs WHERE job_id=?",(job_id,))
sse_jobs=rows("SELECT job_id,status,created_at,updated_at,json_extract(payload_json,'$.event_type') AS event_type,json_extract(payload_json,'$.payload.stage') AS stage FROM durable_jobs WHERE kind='sse' AND json_extract(payload_json,'$.task_id')=? ORDER BY id",(task_id,))
discoveries=[]
for job_id in discovery_ids:
    discoveries+=rows("SELECT job_id,status,created_at,updated_at,error FROM discovery_jobs WHERE job_id=?",(job_id,))
root=rows("SELECT root_trace_id FROM evaluation_traces WHERE trace_type='dialogue' AND session_id=? ORDER BY started_at DESC LIMIT 1",(session_id,))
traces=rows("SELECT trace_id,trace_type,parent_trace_id,started_at,duration_ms,status FROM evaluation_traces WHERE root_trace_id=? ORDER BY started_at",(root[0]['root_trace_id'],)) if root else []
spans=[]
for trace in traces:
    spans+=rows("SELECT trace_id,name,duration_ms,started_at FROM evaluation_trace_spans WHERE trace_id=? ORDER BY started_at",(trace['trace_id'],))
db.close()
sse=sqlite3.connect('/app/data/sse-events.sqlite3')
sse.row_factory=sqlite3.Row
events=[dict(row) for row in sse.execute("SELECT event_id,event_type,status,created_at,json_extract(payload_json,'$.stage') AS stage FROM dialogue_events WHERE task_id=? ORDER BY event_id",(task_id,)).fetchall()]
sse.close()
print(json.dumps({'cards':cards,'jobs':jobs,'sse_jobs':sse_jobs,'discoveries':discoveries,'traces':traces,'spans':spans,'sse_events':events},ensure_ascii=False))
"""
    result = subprocess.run(
        ["docker", "exec", "bilibili-radio-backend", "python", "-c", program,
         task_id, session_id],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--keep-session", action="store_true")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    report: dict = {"prompt": args.prompt, "startedAt": datetime.now(timezone.utc).isoformat()}

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.add_init_script("""
          window.__radioMeasure = {sse: [], click: null};
          const NativeEventSource = window.EventSource;
          window.EventSource = function(url, options) {
            const source = new NativeEventSource(url, options);
            for (const type of ['task','progress','session','discovery','done','error']) {
              source.addEventListener(type, event => {
                let payload = {};
                try { payload = JSON.parse(event.data); } catch (_) {}
                const cards = payload.payload?.session?.cards || [];
                const card = cards.find(item => item.kind === 'recommendation_carousel');
                window.__radioMeasure.sse.push({
                  timeMs: Date.now(), type, taskId: payload.taskId,
                  stage: payload.payload?.stage || null,
                  eventId: payload.eventId,
                  discoveryStatus: card?.discoveryStatus || null,
                  recommendationCount: card?.recommendations?.length ?? null,
                });
              });
            }
            return source;
          };
          window.EventSource.prototype = NativeEventSource.prototype;
        """)
        errors = []
        network = []
        page.on("pageerror", lambda error: errors.append(str(error)))

        def record_response(response) -> None:
            url = response.url
            if "/api/agent/dialogue/tasks" in url or "/api/recommendations/discovery/" in url or "/api/agent/dialogue/cards/" in url:
                network.append({
                    "timeMs": round(time.time() * 1000),
                    "method": response.request.method,
                    "path": url.split("/api/", 1)[-1].split("?", 1)[0],
                    "status": response.status,
                })

        page.on("response", record_response)
        session_id = ""
        task_id = ""
        try:
            page.goto(args.base_url.rstrip("/") + "/#/agent", wait_until="domcontentloaded")
            page.locator(".new-chat-btn").wait_for(timeout=30000)
            with page.expect_response(
                lambda response: response.request.method == "POST"
                and response.url.endswith("/api/agent/dialogue/sessions"), timeout=30000
            ) as created:
                page.locator(".new-chat-btn").click()
            session_id = created.value.json()["data"]["sessionId"]
            page.locator(".chat-surface").wait_for(timeout=30000)
            page.get_by_role("textbox", name="输入消息").fill(args.prompt)
            page.evaluate("""() => {
              document.querySelector('.send-btn').addEventListener('click',
                () => window.__radioMeasure.click = Date.now(), {once: true});
            }""")
            with page.expect_response(
                lambda response: response.request.method == "POST"
                and response.url.endswith("/api/agent/dialogue/tasks"), timeout=30000
            ) as accepted:
                page.get_by_role("button", name="发送", exact=True).click()
            task_id = accepted.value.json()["data"]["taskId"]
            report["taskId"] = task_id
            report["sessionId"] = session_id
            page.locator(".inline-card.recommendation_carousel").last.wait_for(
                timeout=args.timeout_seconds * 1000
            )
            report["initialCardTimeMs"] = round(time.time() * 1000)
            page.wait_for_function(
                """() => {
                  const cards = document.querySelectorAll('.inline-card.recommendation_carousel');
                  const card = cards[cards.length - 1];
                  return card && ['completed','failed'].includes(card.dataset.discoveryStatus);
                }""",
                timeout=args.timeout_seconds * 1000,
            )
            report["terminalCardTimeMs"] = round(time.time() * 1000)
            report["terminalStatus"] = page.locator(
                ".inline-card.recommendation_carousel"
            ).last.get_attribute("data-discovery-status")
            report["visibleTracks"] = page.locator(
                ".inline-card.recommendation_carousel"
            ).last.locator(".song-glass-card").count()
            report["passed"] = True
        except Exception as error:
            report["passed"] = False
            report["error"] = f"{type(error).__name__}: {error}"[:500]
        finally:
            browser_data = page.evaluate("window.__radioMeasure || {}")
            report["browser"] = browser_data
            report["network"] = network
            report["javascriptErrors"] = errors
            if task_id and session_id:
                try:
                    report["backend"] = backend_timings(task_id, session_id)
                except Exception as error:
                    report["backendError"] = f"{type(error).__name__}: {error}"[:300]
            if session_id and not args.keep_session:
                for _attempt in range(35):
                    try:
                        page.once("dialog", lambda dialog: dialog.accept())
                        with page.expect_response(
                            lambda response: response.request.method == "DELETE"
                            and "/api/agent/dialogue/sessions/" in response.url,
                            timeout=10000,
                        ) as deleted:
                            page.locator(
                                ".context-row:has(.context-item.active) .delete-session-btn"
                            ).click()
                        report["cleanupStatus"] = deleted.value.status
                        if deleted.value.status == 200:
                            break
                    except Exception as error:
                        report["cleanupError"] = f"{type(error).__name__}: {error}"[:300]
                        break
                    time.sleep(1)
            page.screenshot(path=str(run_dir / "agent-timing.png"), full_page=True)
            browser.close()

    output = run_dir / "agent-timing.json"
    click_ms = report.get("browser", {}).get("click")
    if click_ms:
        if report.get("initialCardTimeMs"):
            report["clickToInitialCardMs"] = report["initialCardTimeMs"] - click_ms
        if report.get("terminalCardTimeMs"):
            report["clickToTerminalCardMs"] = report["terminalCardTimeMs"] - click_ms
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "passed": report.get("passed"), "taskId": task_id,
        "initialCardTimeMs": report.get("initialCardTimeMs"),
        "terminalCardTimeMs": report.get("terminalCardTimeMs"),
        "terminalStatus": report.get("terminalStatus"),
        "clickToInitialCardMs": report.get("clickToInitialCardMs"),
        "clickToTerminalCardMs": report.get("clickToTerminalCardMs"),
        "visibleTracks": report.get("visibleTracks"),
        "cleanupStatus": report.get("cleanupStatus"),
        "report": str(output),
    }, ensure_ascii=False))
    raise SystemExit(0 if report.get("passed") else 1)


if __name__ == "__main__":
    main()
