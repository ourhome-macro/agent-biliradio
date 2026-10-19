"""Real Chromium/HLS acceptance for cold transitions and expiring segment URLs."""

import json
import re
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def main():
    report = {}
    with tempfile.TemporaryDirectory(prefix="radio-cold-playback-") as folder:
        fixture = Path(folder)
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x180:rate=24",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440",
                "-t",
                "48",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                "-g",
                "48",
                "-c:a",
                "aac",
                "-f",
                "hls",
                "-hls_time",
                "2",
                "-hls_playlist_type",
                "event",
                "-hls_segment_type",
                "fmp4",
                "-hls_segment_filename",
                "seg_%06d.m4s",
                "index.m3u8",
            ],
            cwd=fixture,
            check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        playlist = (fixture / "index.m3u8").read_text()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with (fixture / "vite.log").open("w") as log:
            server = subprocess.Popen(
                [
                    "node",
                    "node_modules/vite/bin/vite.js",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
                cwd=ROOT / "recommend-radio/frontend",
                stdout=log,
                stderr=log,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                with sync_playwright() as runner:
                    browser = runner.chromium.launch()
                    page = browser.new_page(viewport={"width": 1100, "height": 850})
                    errors, keys, received, reads = [], [], [], []
                    state = dict(
                        stored=False, polls=0, beats=0, token=0, expired=0, generation="source-1"
                    )
                    token_expiry = {}
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    item = {
                        "itemId": "i1",
                        "contentId": "c1",
                        "rank": 0,
                        "suppressed": False,
                        "track": {
                            "id": "t1",
                            "source": "ugc",
                            "title": "Cold test",
                            "duration": 48,
                        },
                        "reaction": "neutral",
                        "relationVersion": 0,
                        "counts": {"likes": 0, "dislikes": 0, "version": 0},
                    }

                    def descriptor():
                        return {
                            "playbackId": "p1",
                            "assetId": "a1",
                            "mode": "stored" if state["stored"] else "cold",
                            "status": "ready"
                            if state["stored"]
                            else "preparing"
                            if state["polls"] < 2
                            else "streaming",
                            "manifestUrl": "/api/fixture/index.m3u8",
                            "generation": 1,
                            "generationKey": state["generation"],
                            "expiresAt": time.time() + 15,
                            "leaseExpiresAt": time.time() + 15,
                            "manifestExpiresAt": time.time() + 2 if state["stored"] else None,
                            "refreshAfterSeconds": 1,
                            "durationSeconds": 48,
                            "sourceComplete": state["stored"],
                            "seekable": state["stored"],
                        }

                    def respond(route, data=None, status=200):
                        route.fulfill(
                            status=status,
                            content_type="application/json",
                            body=json.dumps(
                                {
                                    "success": status < 400,
                                    "data": data,
                                    "error": {"code": "fixture", "message": "controlled failure"},
                                }
                            ),
                        )

                    def api(route):
                        parsed = urlparse(route.request.url)
                        path = parsed.path
                        if not path.startswith("/api/"):
                            route.continue_()
                            return
                        if path == "/api/fixture/index.m3u8":
                            text = playlist
                            if state["stored"]:
                                state["token"] += 1
                                token = str(state["token"])
                                token_expiry[token] = time.time() + 2
                                text = re.sub(
                                    r"(?:init\.mp4|seg_\d{6}\.m4s)",
                                    lambda match: (
                                        "/api/fixture/" + match[0] + "?signature=" + token
                                    ),
                                    text,
                                )
                            else:
                                text = text[: text.index("#EXTINF:", text.index("seg_000003.m4s"))]
                            route.fulfill(body=text, content_type="application/vnd.apple.mpegurl")
                        elif path.startswith("/api/fixture/"):
                            token = parse_qs(parsed.query).get("signature", [None])[0]
                            if token and token_expiry.get(token, 0) <= time.time():
                                state["expired"] += 1
                                respond(route, status=403)
                            else:
                                reads.append((path, token))
                                route.fulfill(
                                    body=(fixture / path.rsplit("/", 1)[1]).read_bytes(),
                                    content_type="video/mp4",
                                )
                        elif path == "/api/settings":
                            respond(route, {"feedEnabled": True})
                        elif path == "/api/feed/sessions":
                            respond(
                                route,
                                {
                                    "sessionId": "s1",
                                    "revision": "1",
                                    "mode": "personal",
                                    "items": [item],
                                    "nextCursor": "",
                                    "hasMore": False,
                                    "supplyState": "exhausted",
                                    "expiresAt": time.time() + 1800,
                                },
                            )
                        elif path == "/api/media/playbacks":
                            keys.append(route.request.headers["idempotency-key"])
                            respond(route, descriptor(), status=503 if len(keys) == 1 else 201)
                        elif path.endswith("/heartbeat"):
                            state["beats"] += 1
                            respond(route, descriptor(), status=503 if state["beats"] == 1 else 200)
                        elif path.startswith("/api/media/playbacks/"):
                            state["polls"] += 1
                            respond(route, descriptor())
                        elif path == "/api/feed/events":
                            received.extend(route.request.post_data_json["events"])
                            respond(route, {})
                        else:
                            respond(route, {"items": [], "tracks": [], "playlists": []})

                    page.route("**/api/**", api)
                    for _ in range(50):
                        try:
                            page.goto(
                                f"http://127.0.0.1:{port}/tests/feed-browser.html", timeout=1000
                            )
                            break
                        except Exception:
                            time.sleep(0.1)
                    try:
                        page.wait_for_function(
                            "document.querySelector('video')?.currentTime > 1", timeout=20000
                        )
                    except Exception:
                        print(
                            json.dumps(
                                {
                                    "url": page.url,
                                    "html": page.content()[:1500],
                                    "vite": (fixture / "vite.log").read_text(
                                        encoding="utf-8", errors="replace"
                                    ),
                                    "state": state,
                                    "errors": errors,
                                    "reads": reads,
                                    "keys": keys,
                                    "text": page.locator("body").inner_text(),
                                    "video": page.locator("video").evaluate_all(
                                    "vs => vs.map(v => ({src:v.currentSrc, paused:v.paused, "
                                    "ready:v.readyState, error:v.error?.message}))"
                                    ),
                                },
                                ensure_ascii=True,
                            )
                        )
                        raise
                    assert len(keys) == 2 and len(set(keys)) == 1
                    report["ambiguous_creation_retries_same_idempotency_key"] = True
                    source = page.locator("video").evaluate("v => v.currentSrc")
                    state["stored"] = True
                    page.wait_for_function(
                        "!document.querySelector('[aria-label=\"视频进度\"]')?.disabled",
                        timeout=15000,
                    )
                    assert page.locator("video").evaluate("v => v.currentSrc") == source
                    assert page.locator("video").evaluate("v => v.currentTime") > 3
                    report["cold_to_stored_keeps_media_source_and_position"] = True
                    assert state["beats"] >= 2
                    report["transient_heartbeat_keeps_playback"] = True
                    page.get_by_role("button", name="暂停", exact=True).click()
                    paused_at = page.locator("video").evaluate("v => v.currentTime")
                    prior_expired = state["expired"]
                    page.wait_for_timeout(3500)
                    assert page.locator("video").evaluate("v => v.paused")
                    assert (
                        abs(page.locator("video").evaluate("v => v.currentTime") - paused_at) < 0.3
                    )
                    assert page.locator("video").evaluate("v => v.currentSrc") == source
                    report["signature_refresh_preserves_pause_buffers_and_position"] = True
                    slider = page.get_by_role("slider", name="视频进度")
                    slider.fill("40")
                    slider.dispatch_event("change")
                    page.get_by_role("button", name="播放", exact=True).click()
                    page.wait_for_function(
                        "document.querySelector('video')?.currentTime > 40.5", timeout=10000
                    )
                    assert state["expired"] == prior_expired
                    assert any(path.endswith("seg_000020.m4s") and token for path, token in reads)
                    assert page.locator("video").evaluate("v => v.currentSrc") == source
                    report["seek_after_signature_expiry_uses_renewed_segment_urls"] = True
                    page.get_by_role("button", name="暂停", exact=True).click()
                    state["generation"] = "source-2"
                    page.wait_for_function(
                        "old => document.querySelector('video')?.currentSrc !== old",
                        arg=source,
                        timeout=10000,
                    )
                    page.wait_for_timeout(600)
                    assert page.locator("video").evaluate("v => v.paused && v.currentTime > 39")
                    report["real_source_replacement_restores_position_without_autoplay"] = True
                    page.goto("about:blank")
                    assert (
                        len([event for event in received if event["type"] == "play_started"]) == 1
                    )
                    assert not errors, errors
                    report["single_watch_identity_and_no_javascript_errors"] = True
                    browser.close()
            finally:
                server.terminate()
                server.wait(timeout=10)
        destination = ROOT / "doc/cold-playback-recovery-browser-2026-10-19.json"
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
