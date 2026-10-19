"""Isolated UI contract acceptance against mocked APIs; no external services are changed."""
from __future__ import annotations

import json
import socket
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "recommend-radio" / "frontend"


def main():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen(
        ["node", "node_modules/vite/bin/vite.js", "--host", "127.0.0.1", "--port", str(port)],
        cwd=FRONTEND, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    origin = f"http://127.0.0.1:{port}"
    results = []
    try:
        for _ in range(100):
            try:
                with urlopen(origin, timeout=1):
                    break
            except OSError:
                time.sleep(.1)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            for role in ("admin", "user"):
                page = browser.new_page(viewport={"width": 1440, "height": 1000})
                commands, errors = [], []
                relation = {"state": "neutral", "version": 0}
                track = {"bvid": "BV0000000001", "cid": 1, "title": "Fixture live", "owner": "Fixture musician", "duration": 60, "cover": "", "trackId": "fixture-track"}
                def content():
                    return {"contentId": "content-one", "track": track, "assetStatus": "ready",
                            "reaction": relation["state"], "relationVersion": relation["version"],
                            "projectionPending": False, "sharePath": "/#/feed?content=content-one",
                            "counts": {"likes": int(relation["state"] == "like"), "dislikes": int(relation["state"] == "dislike"), "version": relation["version"]}}
                def feed():
                    return {"sessionId": "session", "revision": "revision", "mode": "new", "items": [{**content(), "itemId": "item-one", "rank": 0, "suppressed": False}], "nextCursor": "tail", "hasMore": False, "supplyState": "exhausted", "expiresAt": time.time() + 1000}
                page.on("pageerror", lambda error: errors.append(str(error)))
                inventory = {"contents": {"admitted": 1}, "assets": {"ready": 1},
                             "contentsList": [{"contentId": "content-one", "title": "Fixture live", "status": "admitted", "assetStatus": "ready"}],
                             "imports": [{"importId": "job-one", "assetId": "asset-one", "status": "failed", "stage": "upload", "attempt": 1, "errorType": "upload_error", "updatedAt": 1}],
                             "retainedBytes": 1000000, "storageBudgetBytes": 1000000000,
                             "scratchBytes": 0, "scratchBudgetBytes": 100000000}
                def respond(route):
                    path = urlparse(route.request.url).path
                    if not path.startswith('/api/'):
                        route.continue_()
                        return
                    data = {}
                    if path == "/api/session/me":
                        data = {"authenticated": True, "user": {"id": "fixture", "displayName": "Fixture", "role": role}, "csrfToken": "fixture-csrf", "oidcEnabled": False, "biliConnected": False}
                    elif path == "/api/settings":
                        data = {"feedEnabled": True, "audioQualityPreference": "auto", "playbackSpeed": 1}
                    elif path == "/api/auth/status":
                        data = {"qrLoginEnabled": False, "isLoggedIn": False, "user": None}
                    elif path == "/api/admin/feed/operations":
                        data = {"inventory": inventory, "metrics": {"startupMs": {"cold": {"p50": 1000, "p95": 2000, "count": 2}}, "bufferingRatio": .1, "playbackErrorRate": 0, "swipeCount": 3}, "alerts": []}
                    elif path.startswith("/api/admin/feed/"):
                        commands.append({"path": path, "body": route.request.post_data_json, "csrf": route.request.headers.get("x-csrf-token")})
                        if path.endswith("/status"):
                            inventory["contentsList"][0]["status"] = route.request.post_data_json["status"]
                    elif path == "/api/content/content-one/reaction":
                        body = route.request.post_data_json
                        assert body["expectedVersion"] == relation["version"]
                        assert route.request.headers.get("idempotency-key")
                        relation["state"] = body["state"]
                        relation["version"] += 1
                        data = {"state": relation["state"], "relationVersion": relation["version"], "counts": content()["counts"]}
                    elif path == "/api/content/content-one": data = content()
                    elif path == "/api/feed/sessions" or path == "/api/feed/content/content-one/open": data = feed()
                    elif path == "/api/feed/history": data = {"items": [{"contentId": "content-one", "track": track, "watchMs": 12000, "watchedAt": time.time(), "completed": False}], "nextCursor": None}
                    elif path == "/api/media/playbacks": data = {"playbackId": "playback", "assetId": "asset", "mode": "stored", "status": "failed", "manifestUrl": None, "expiresAt": 0, "sourceComplete": False, "generation": 0}
                    elif path == "/api/library/playlists": data = {"playlists": [{"id": "playlist-one", "name": "Fixture playlist", "tracks": [], "cover": None, "createdAt": 1}]}
                    elif path == "/api/library/playlists/playlist-one/items:batch":
                        commands.append({"path": path, "body": route.request.post_data_json})
                    elif path == "/api/player/queue": data = {"queue": [], "currentIndex": -1, "playMode": "sequence"}
                    elif path == "/api/recommendations": data = {"tracks": [], "recommendations": []}
                    elif path.startswith("/api/library/"): data = {"tracks": []}
                    route.fulfill(content_type="application/json", body=json.dumps({"success": True, "data": data}))
                page.route("**/api/**", respond)
                page.goto(origin + "/#/admin/feed")
                if role == "admin":
                    expect(page.get_by_role("heading", name="视频运营", exact=True)).to_be_visible()
                    expect(page.get_by_role("link", name="视频运营", exact=True)).to_be_visible()
                    page.get_by_role("button", name="下架", exact=True).click()
                    page.get_by_label("操作原因").fill("Fixture retirement acceptance")
                    page.get_by_role("button", name="确认操作", exact=True).click()
                    expect(page.get_by_role("button", name="重新上架", exact=True)).to_be_visible()
                    assert commands[0]["body"]["status"] == "retired"
                    assert commands[0]["csrf"] == "fixture-csrf"
                    page.get_by_role("button", name="重试", exact=True).click()
                    page.get_by_label("操作原因").fill("Fixture upload retry acceptance")
                    page.get_by_role("button", name="确认操作", exact=True).click()
                    expect(page.get_by_role("dialog")).not_to_be_visible()
                    assert commands[1]["path"].endswith("/imports/job-one/retry")
                    page.locator("main.operations").evaluate("element => element.scrollTop = 0")
                    page.screenshot(path=str(ROOT / "doc" / "feed-operations-ui-2026-10-07.png"), full_page=True)
                    results += ["admin dashboard", "status command with CSRF and reason", "failed import retry"]
                    page.goto(origin + "/#/feed")
                    page.get_by_role("button", name="喜欢 0", exact=True).click()
                    expect(page.get_by_role("button", name="喜欢 1", exact=True)).to_be_visible()
                    page.get_by_role("button", name="不感兴趣", exact=True).click()
                    expect(page.get_by_role("button", name="撤销", exact=True)).to_be_visible()
                    page.get_by_role("button", name="撤销", exact=True).click()
                    expect(page.get_by_role("button", name="喜欢 1", exact=True)).to_be_visible()
                    assert relation == {"state": "like", "version": 3}
                    page.get_by_role("button", name="加入歌单", exact=True).click()
                    page.get_by_role("dialog").get_by_role("button", name="Fixture playlist", exact=True).click()
                    expect(page.get_by_role("dialog")).not_to_be_visible()
                    assert commands[-1]["body"]["tracks"][0]["title"] == "Fixture live"
                    page.get_by_role("button", name="详情 / 分享", exact=True).click()
                    expect(page.get_by_role("dialog").get_by_role("heading", name="Fixture live")).to_be_visible()
                    page.get_by_role("dialog").get_by_role("button", name="关闭", exact=True).click()
                    page.get_by_role("button", name="观看历史", exact=True).click()
                    page.get_by_role("dialog").get_by_role("button", name="Fixture live").click()
                    expect(page.get_by_role("dialog")).not_to_be_visible()
                    expect(page.get_by_role("button", name="喜欢 1", exact=True)).to_be_visible()
                    results += ["reaction count refresh", "last-card dislike undo restores liked state", "add current track to existing playlist", "content details", "history explicit replay"]
                else:
                    expect(page).to_have_url(origin + "/#/")
                    expect(page.get_by_role("link", name="视频运营", exact=True)).to_have_count(0)
                    assert not commands
                    results.append("ordinary user route and nav isolation")
                assert not errors, errors
                page.close()
            browser.close()
    finally:
        process.terminate()
        process.wait(timeout=10)
    report = {"kind": "mocked-api-browser-contract", "passed": results}
    (ROOT / "doc" / "feed-product-ui-2026-10-07.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
