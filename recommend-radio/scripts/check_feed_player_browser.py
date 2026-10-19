"""Local browser acceptance using real HLS plus controlled API fault injection."""
from __future__ import annotations

import json
import argparse
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--following', action='store_true')
    parser.add_argument('--ugc', action='store_true')
    args = parser.parse_args()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    fixture = Path(tempfile.mkdtemp(prefix="feed-browser-"))
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=size=320x180:rate=24", "-f", "lavfi", "-i", "sine=frequency=440", "-t", "30", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "48", "-c:a", "aac", "-f", "hls", "-hls_time", "2", "-hls_list_size", "0", "-hls_segment_type", "fmp4", "index.m3u8"], cwd=fixture, check=True)
    log = (fixture / "vite.log").open("w")
    server = subprocess.Popen(["node", "node_modules/vite/bin/vite.js", "--host", "127.0.0.1", "--port", str(port)], cwd=ROOT / "recommend-radio/frontend", stdout=log, stderr=log, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    report = {}
    try:
        with sync_playwright() as runner:
            browser = runner.chromium.launch()
            page = browser.new_page(viewport={"width": 1100, "height": 850})
            errors, received, successful, requests = [], [], [], []
            page.on("pageerror", lambda error: errors.append(str(error)))
            state = {"starts": 0, "beats": 0, "manifests": 0, "failed_events": False}
            items = [{"itemId": f"i{n}", "contentId": f"c{n}", "rank": n, "suppressed": False, "track": {"id": f"t{n}", "bvid": "BVtest", "cid": n + 1, "title": f"Video {n}", "owner": "Fixture", "duration": 30, "cover": ""}, "reaction": "neutral", "relationVersion": 0, "counts": {"likes": 0, "dislikes": 0, "version": 0}} for n in range(5)]
            for n, item in enumerate(items): item['track']['ownerMid'] = 100 if n in (0, 2) else 100 + n
            creators = {str(mid): {'creatorId': f'creator-{mid}', 'provider': 'bilibili', 'externalId': str(mid), 'name': f'UP {mid}', 'following': False, 'confirmedFollowing': False, 'syncStatus': 'synced', 'relationVersion': 0, 'accountMid': 999, 'errorType': None, 'supported': True} for mid in (100, 101, 103, 104)}
            follow_commands, follow_keys, follow_attempts = {}, [], {}

            def descriptor(number):
                return {"playbackId": f"p{number}", "assetId": "a", "mode": "stored", "status": "ready", "manifestUrl": "/api/test-hls/index.m3u8", "expiresAt": time.time() + 15, "leaseExpiresAt": time.time() + 15, "manifestExpiresAt": time.time() + (1 if state['beats'] >= 2 and number == 1 else 3600), "sourceComplete": True, "generation": int(state["beats"] >= 2), "generationKey": str(int(state["beats"] >= 2)), "durationSeconds": 30}

            def respond(route, data=None, status=200):
                route.fulfill(status=status, content_type="application/json", body=json.dumps({"success": status < 400, "data": data, "error": {"code": "temporary", "message": "temporary"}}))

            def api(route):
                path = urlparse(route.request.url).path
                if not path.startswith('/api/'):
                    route.continue_(); return
                requests.append((route.request.method, path))
                if path.startswith("/api/test-hls/"):
                    name = path.rsplit("/", 1)[1]
                    if name == "index.m3u8": state["manifests"] += 1
                    route.fulfill(body=(fixture / name).read_bytes(), content_type="application/vnd.apple.mpegurl" if name.endswith("m3u8") else "video/mp4")
                elif path == "/api/settings": respond(route, {"feedEnabled": True})
                elif path == "/api/feed/sessions":
                    mode = route.request.post_data_json.get('mode', 'personal')
                    selected = [item for item in items if (creator := creators[str(item['track']['ownerMid'])])['following'] and creator['confirmedFollowing'] and creator['syncStatus'] == 'synced'] if mode == 'following' else items
                    respond(route, {"sessionId": "s", "revision": str(len(follow_commands)), "mode": mode, "items": selected, "nextCursor": "", "hasMore": False, "supplyState": "exhausted", "expiresAt": time.time() + 1800, 'followingState': {'reason': 'available' if selected else 'no_follows', 'snapshotVersion': len(follow_commands), 'hasUpdates': False, 'lastSyncedAt': time.time()} if mode == 'following' else None})
                elif path.startswith('/api/content/') and path.endswith('/creator'):
                    item = next(item for item in items if item['contentId'] == path.split('/')[3])
                    respond(route, creators[str(item['track']['ownerMid'])])
                elif path.startswith('/api/creators/') and path.endswith('/follow'):
                    mid = path.split('/')[3].replace('creator-', '')
                    creator = creators[mid]
                    if route.request.method == 'PUT':
                        key = route.request.headers['idempotency-key']; follow_keys.append(key)
                        if key not in follow_commands:
                            follow_attempts[mid] = follow_attempts.get(mid, 0) + 1
                            creator.update(following=route.request.post_data_json['following'], syncStatus='pending', relationVersion=creator['relationVersion'] + 1)
                            follow_commands[key] = dict(creator)
                            if len(follow_commands) == 1: respond(route, status=503); return
                        respond(route, follow_commands[key])
                    else:
                        if creator['syncStatus'] == 'pending':
                            if follow_attempts.get(mid) == 1: creator.update(syncStatus='failed', errorType='fixture_error')
                            else: creator.update(syncStatus='synced', confirmedFollowing=creator['following'], errorType=None)
                        respond(route, creator)
                elif path == '/api/following/sync':
                    creators['101'].update(following=True, confirmedFollowing=True, syncStatus='synced', relationVersion=1)
                    respond(route, {'syncRunId': 'sync-1', 'status': 'queued'})
                elif path == '/api/following': respond(route, {'items': list(creators.values()), 'accountMid': 999, 'relationRevision': 4, 'sync': {'status': 'succeeded', 'lastSyncedAt': time.time(), 'errorType': None}})
                elif path == "/api/feed/events":
                    batch = route.request.post_data_json["events"]
                    received.extend(batch)
                    if any(event["type"] == "watch_progress" for event in batch) and not state["failed_events"]:
                        state["failed_events"] = True; respond(route, status=503)
                    else: successful.extend(batch); respond(route, {})
                elif path == "/api/media/playbacks":
                    state["starts"] += 1; respond(route, descriptor(state["starts"]))
                elif path.endswith("/heartbeat"):
                    state["beats"] += 1
                    if state["beats"] == 1: respond(route, status=503)
                    else: respond(route, descriptor(state["starts"]))
                elif path.startswith("/api/media/playbacks/"): respond(route, descriptor(state["starts"]))
                elif path.startswith("/api/content/") and path.endswith("/reaction"): respond(route, {"state": "neutral", "relationVersion": 0, "counts": {"likes": 0, "dislikes": 0, "version": 0}})
                else: respond(route, {"items": [], "tracks": [], "playlists": []})

            page.route("**/api/**", api)
            for _ in range(50):
                try: page.goto(f"http://127.0.0.1:{port}/tests/feed-browser.html", timeout=1000); break
                except Exception: time.sleep(.1)
            try:
                page.wait_for_function("document.querySelector('video')?.currentTime > 1", timeout=20000)
            except Exception:
                print({"errors": errors, "requests": requests, "text": page.locator('body').inner_text(), "video": page.locator('video').evaluate_all('(vs) => vs.map(v => ({src:v.currentSrc,paused:v.paused,error:v.error?.message}))')})
                raise
            page.wait_for_timeout(11000)
            assert state["starts"] == 1 and state["beats"] >= 2
            assert state["manifests"] >= 3
            assert page.locator("video").evaluate("v => v.currentTime") > 5
            report["transient_heartbeat_preserves_session"] = True
            report["generation_change_reattaches_without_restarting_watch"] = True
            report["signed_manifest_refresh_preserves_position"] = True
            page.get_by_role('button', name='全屏', exact=True).click()
            page.wait_for_function('document.fullscreenElement !== null')
            page.get_by_role('button', name='全屏', exact=True).click()
            report['desktop_fullscreen'] = True
            page.get_by_role("button", name="下一个 ↓").click()
            page.wait_for_function("document.querySelector('video')?.currentTime > .4")
            page.wait_for_timeout(3500)
            old = [event for event in successful if event.get("playbackId") == "p1"]
            assert any(event["type"] == "swipe_next" for event in old)
            watch = [event["watchMs"] for event in old if event["type"] == "watch_progress"]
            assert watch and watch == sorted(watch) and watch[-1] >= 5000
            assert len(received) > len({event["eventId"] for event in received})
            report["final_watch_old_item_attribution_and_stable_retry_ids"] = True
            assert any(event["type"] == "impression" for event in successful)
            report["continuous_exposure"] = True
            slider = page.get_by_role("slider", name="视频进度")
            slider.fill("28.5"); slider.dispatch_event("change")
            page.wait_for_timeout(2500)
            assert state["starts"] >= 3
            assert not any(event["type"] == "complete" and event.get("playbackId") == "p2" for event in successful)
            report["stored_seek_and_natural_auto_next"] = True
            report["seek_to_end_does_not_fake_completion"] = True
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(500)
            assert page.locator("video").count() == 1
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            page.screenshot(path=str(ROOT / "doc/feed-player-mobile-2026-10-07.png"))
            report["mobile_layout_single_player"] = True
            if args.ugc:
                items[0]['track']['source']='ugc';items[0]['track']['trackId']='ugc-video-c0';items[0]['track'].pop('bvid',None)
                before_creators=sum(path.endswith('/creator') for _,path in requests)
                page.get_by_role('button',name='新发现',exact=True).click()
                page.wait_for_function("document.querySelector('video')?.currentTime > .2",timeout=10000)
                assert page.get_by_role('button',name='加入歌单',exact=True).count()==0
                assert page.get_by_role('link',name='详情 / 分享',exact=True).get_attribute('href').endswith('/content/c0')
                assert page.locator('[aria-label="作者关注"] button').count()==0
                assert sum(path.endswith('/creator') for _,path in requests)==before_creators
                report['ugc_video_avoids_bili_playlist_creator_and_detail_routes']=True
            if args.following:
                page.get_by_role('button', name='关注作者', exact=True).click()
                page.get_by_text('正在同步到 B 站…', exact=True).wait_for()
                page.get_by_role('button', name='重试同步', exact=True).wait_for(timeout=10000)
                assert follow_keys[0] == follow_keys[1]
                report['follow_command_network_retry_reuses_idempotency_key'] = True
                page.get_by_role('button', name='重试同步', exact=True).click()
                page.get_by_text('已同步 B 站关注', exact=True).wait_for(timeout=10000)
                report['follow_pending_failure_retry_confirmed'] = True
                page.get_by_role('button', name='关注', exact=True).click()
                page.get_by_role('button', name='取消关注', exact=True).wait_for(timeout=10000)
                assert page.locator('article').count() == 2
                report['following_mode_filters_confirmed_creators'] = True
                page.get_by_role('button', name='取消关注', exact=True).click()
                page.wait_for_function('!document.querySelector("video")')
                assert page.locator('article').count() == 0
                report['unfollow_immediately_removes_all_author_cards'] = True
                page.get_by_role('button', name='同步 B 站关注', exact=True).click()
                page.get_by_role('button', name='取消关注', exact=True).wait_for(timeout=10000)
                assert 'Video 1' in page.locator('.feed-caption').inner_text()
                report['sync_following_reloads_new_revision'] = True
            assert not errors, errors
            report["javascript_errors"] = errors
            browser.close()
    finally:
        server.terminate(); server.wait(timeout=10); log.close()
        name = 'ugc-player-browser-2026-10-08.json' if args.ugc else 'following-player-browser-2026-10-08.json' if args.following else 'feed-player-browser-2026-10-07.json'
        (ROOT / 'doc' / name).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
