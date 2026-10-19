from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import fakeredis
import pytest
from database import get_connection
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.following_feed import index_content, refresh_creator, schedule_supply
from feed_runtime.repository import FeedRepository
from following.service import FollowingService
from models import Track


@pytest.fixture
def timeline(tmp_path, monkeypatch):
    monkeypatch.setenv("JOB_TRANSPORT", "disabled")
    repo = FeedRepository(tmp_path / "timeline.sqlite3")
    config = replace(FeedConfig(), workdir=tmp_path / "media")
    cache = FeedCache("redis://unused", client=fakeredis.FakeRedis(decode_responses=True))
    service = FeedService(repo, config, cache)
    state = {
        "accountMid": "99",
        "bindingKey": "binding",
        "creatorIds": [],
        "version": "v1",
        "lastSyncedAt": None,
        "syncStatus": "synced",
        "reason": "available",
    }
    monkeypatch.setattr(FollowingService, "snapshot", lambda self, user: dict(state))
    with get_connection(repo.db_path) as conn:
        for mid in (1, 2, 3):
            creator = FollowingService.creator(conn, mid, f"UP{mid}")
            if mid < 3:
                state["creatorIds"].append(creator)
    return repo, service, state


def seed(repo, n, *, mid=1, published=None, work="same-song", scope="public"):
    creator_id = None
    with get_connection(repo.db_path) as conn:
        creator_id = FollowingService.creator(conn, mid)
    content = repo.admit(
        Track(
            bvid=f"BVfollow{mid:02}{n:04}",
            cid=n + mid * 10000,
            title=f"音乐 演奏 {n}",
            type_name="音乐",
            owner_mid=mid,
            work_id=work,
        ),
        user_id="legacy-owner",
        scope=scope,
    )
    with get_connection(repo.db_path) as conn:
        index_content(
            conn, creator_id, content, published if published is not None else time.time() - n - 10
        )
    return content


def test_fixed_pages_time_order_and_no_work_collapse(timeline):
    repo, service, state = timeline
    ids = [seed(repo, i) for i in range(15)]
    seed(repo, 0, mid=2)
    seed(repo, 0, mid=3)  # Not followed; must never leak through exploration.
    first = service.create("legacy-owner", mode="following", request_key="first")
    assert len(first["items"]) == 10
    assert all(item["track"]["ownerMid"] in (1, 2) for item in first["items"])
    again = service.page("legacy-owner", first["sessionId"])
    assert [i["contentId"] for i in again["items"]] == [i["contentId"] for i in first["items"]]
    # New publications above the session watermark appear only after refresh.
    new = seed(repo, 90, published=time.time() + 1)
    second = service.page("legacy-owner", first["sessionId"], cursor=first["nextCursor"])
    old_ids = {i["contentId"] for i in first["items"]}
    assert not old_ids & {i["contentId"] for i in second["items"]}
    assert new not in {i["contentId"] for i in second["items"]}
    assert len(first["items"] + second["items"]) == 16
    assert set(ids).issubset(old_ids | {i["contentId"] for i in second["items"]})


def test_unfollow_rechecks_cached_page_and_rebinding_invalidates(timeline):
    repo, service, state = timeline
    seed(repo, 1)
    seed(repo, 2, mid=2)
    first = service.create("legacy-owner", mode="following", request_key="first")
    removed = state["creatorIds"].pop(0)
    state["version"] = "v2"
    result = service.page("legacy-owner", first["sessionId"])
    assert sum(i["suppressed"] for i in result["items"]) == 1
    assert result["followingState"]["hasUpdates"]
    state["bindingKey"] = "rebound-same-account"
    state["creatorIds"].append(removed)
    result = service.page("legacy-owner", first["sessionId"])
    assert all(i["suppressed"] for i in result["items"])
    assert result["followingState"]["reason"] == "account_changed"


def test_shared_author_supply_only_one_reservation(timeline):
    repo, service, state = timeline
    creator = state["creatorIds"][0]
    with ThreadPoolExecutor(max_workers=8) as pool:
        counts = list(pool.map(lambda _: schedule_supply(repo.db_path, [creator]), range(8)))
    assert sum(counts) == 1
    with get_connection(repo.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM durable_jobs WHERE kind='creator_supply'"
            ).fetchone()[0]
            == 1
        )


def test_empty_fixed_page_then_supply_arrives(timeline):
    repo, service, state = timeline
    first = service.create("legacy-owner", mode="following", request_key="first")
    assert first["items"] == []
    assert first["supplyState"] == "replenishing"
    content = seed(repo, 3)
    second = service.page("legacy-owner", first["sessionId"], cursor=first["nextCursor"])
    assert [i["contentId"] for i in second["items"]] == [content]
    assert service.page("legacy-owner", first["sessionId"])["items"] == []


def test_dislike_scope_and_non_music_excluded(timeline):
    repo, service, state = timeline
    content = seed(repo, 1)
    private = seed(repo, 2, scope="user:legacy-owner")
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO content_reactions VALUES (?,?,?,?,?)",
            ("other", content, "dislike", 1, time.time()),
        )
    result = service.create("other", mode="following", request_key="first")
    assert result["items"] == []
    assert private not in {i["contentId"] for i in result["items"]}


def test_worker_source_public_dedup_and_stale_version(timeline):
    repo, service, state = timeline
    creator = state["creatorIds"][0]
    calls = []

    class Client:
        def __init__(self, *, cookie_provider):
            assert cookie_provider() is None

        def list_user_tracks(self, mid, **kwargs):
            calls.append(mid)
            return {
                "tracks": [
                    Track(
                        bvid="BVpublictest",
                        title="摇滚 音乐演奏",
                        owner_mid=mid,
                        type_name="音乐",
                        published_at="2026-01-01T00:00:00Z",
                    ).to_dict()
                ],
                "hasMore": False,
            }

        def close(self):
            pass

    schedule_supply(repo.db_path, [creator])
    assert (
        refresh_creator(repo.db_path, creator, version=0, client_factory=Client)["status"]
        == "superseded"
    )
    assert not calls
    assert refresh_creator(repo.db_path, creator, version=1, client_factory=Client)["indexed"] == 1
    assert (
        refresh_creator(repo.db_path, creator, version=1, client_factory=Client)["status"]
        == "ready"
    )
    assert len(calls) == 1
    constrained = service.create('legacy-owner', mode='following', request_text='摇滚', request_key='facets')
    assert len(constrained['items']) == 1


def test_late_backfill_above_cursor_is_issued_only_on_new_page(timeline):
    repo, service, state = timeline
    old = seed(repo, 10, published=time.time() - 100)
    first = service.create('legacy-owner', mode='following', request_key='late')
    assert [item['contentId'] for item in first['items']] == [old]
    late = seed(repo, 11, published=time.time() - 10)
    second = service.page('legacy-owner', first['sessionId'], cursor=first['nextCursor'])
    assert [item['contentId'] for item in second['items']] == [late]
    assert [item['contentId'] for item in service.page('legacy-owner', first['sessionId'])['items']] == [old]


def test_deep_author_scan_continues_and_checks_new_head_every_attempt(timeline, monkeypatch):
    repo, service, state = timeline
    creator = state['creatorIds'][0]
    monkeypatch.setenv('FOLLOWING_AUTHOR_MAX_PAGES', '2')
    calls = []
    class Client:
        def __init__(self, **kwargs): pass
        def close(self): pass
        def list_user_tracks(self, mid, *, page, **kwargs):
            calls.append(page)
            return {'tracks': [Track(bvid=f'BVdeep{page:08}', cid=page, owner_mid=mid,
                title='音乐演奏' if page == 4 else '日常闲聊', type_name='音乐' if page == 4 else '生活',
                published_at='2026-01-01T00:00:00Z').to_dict()], 'hasMore': page < 4}
    for version in range(1, 4):
        with get_connection(repo.db_path) as conn:
            conn.execute('UPDATE creator_supply SET next_check_at=0 WHERE creator_id=?', (creator,))
        assert schedule_supply(repo.db_path, [creator]) == 1
        result = refresh_creator(repo.db_path, creator, version=version, client_factory=Client)
        assert result['status'] == ('ready' if version == 3 else 'partial')
    assert calls == [1, 2, 1, 3, 1, 4]
    result = service.create('legacy-owner', mode='following', request_key='deep')
    assert len(result['items']) == 1
    assert result['items'][0]['track']['title'] == '音乐演奏'


def test_supply_constructor_failure_marks_failed_without_waiting_for_lease(timeline):
    repo, service, state = timeline
    creator = state['creatorIds'][0]
    schedule_supply(repo.db_path, [creator])
    def broken(**kwargs): raise RuntimeError('construction_failed')
    with pytest.raises(RuntimeError, match='construction_failed'):
        refresh_creator(repo.db_path, creator, version=1, client_factory=broken)
    with get_connection(repo.db_path) as conn:
        row = conn.execute('SELECT status,lease_until FROM creator_supply WHERE creator_id=?', (creator,)).fetchone()
    assert row['status'] == 'failed' and row['lease_until'] == 0


def test_expired_or_replaced_worker_cannot_publish_ready(timeline):
    repo, service, state = timeline
    creator = state['creatorIds'][0]
    schedule_supply(repo.db_path, [creator])
    class Client:
        def __init__(self, **kwargs): pass
        def close(self): pass
        def list_user_tracks(self, mid, **kwargs):
            with get_connection(repo.db_path) as conn:
                conn.execute("UPDATE creator_supply SET lease_token='replacement',version=2,lease_until=? WHERE creator_id=?", (time.time() + 240, creator))
            return {'tracks': [], 'hasMore': False}
    with pytest.raises(RuntimeError, match='LeaseLost'):
        refresh_creator(repo.db_path, creator, version=1, client_factory=Client)
    with get_connection(repo.db_path) as conn:
        row = conn.execute('SELECT * FROM creator_supply WHERE creator_id=?', (creator,)).fetchone()
    assert row['status'] == 'running' and row['lease_token'] == 'replacement' and row['version'] == 2


def test_partial_page_failure_resumes_after_last_complete_page(timeline, monkeypatch):
    repo, service, state = timeline
    creator = state['creatorIds'][0]
    monkeypatch.setenv('FOLLOWING_AUTHOR_MAX_PAGES', '2')
    calls = []
    class Client:
        fail = True
        def __init__(self, **kwargs): pass
        def close(self): pass
        def list_user_tracks(self, mid, *, page, **kwargs):
            calls.append(page)
            if page == 2 and self.fail: raise OSError('network')
            return {'tracks': [], 'hasMore': page < 2}
    schedule_supply(repo.db_path, [creator])
    with pytest.raises(OSError): refresh_creator(repo.db_path, creator, version=1, client_factory=Client)
    with get_connection(repo.db_path) as conn:
        assert conn.execute('SELECT next_page FROM creator_supply WHERE creator_id=?', (creator,)).fetchone()[0] == 2
        conn.execute('UPDATE creator_supply SET next_check_at=0 WHERE creator_id=?', (creator,))
    Client.fail = False
    schedule_supply(repo.db_path, [creator])
    assert refresh_creator(repo.db_path, creator, version=2, client_factory=Client)['status'] == 'ready'
    assert calls == [1, 2, 1, 2]


def test_large_nonmatching_prefix_advances_bounded_scan(timeline):
    import json
    repo, service, state = timeline
    source = seed(repo, 1)
    creator = state['creatorIds'][0]
    with get_connection(repo.db_path) as conn:
        base = dict(conn.execute('SELECT * FROM feed_content WHERE content_id=?', (source,)).fetchone())
        base_track = dict(conn.execute('SELECT * FROM tracks WHERE track_id=?', (base['track_id'],)).fetchone())
        conn.execute('DELETE FROM creator_content WHERE content_id=?', (source,))
        for n in range(1001):
            content_id = f'bulk-{n:05}'
            metadata = json.loads(base['metadata_json']); metadata.update(bvid=f'BVbulk{n:08}', cid=n, title='摇滚 音乐' if n == 1000 else '民谣 音乐')
            track_id = f"{metadata['bvid']}:{n}"
            track = dict(base_track, track_id=track_id, bvid=metadata['bvid'], cid=n, title=metadata['title'])
            conn.execute('INSERT INTO tracks (' + ','.join(track) + ') VALUES (' + ','.join('?' for _ in track) + ')', tuple(track.values()))
            conn.execute('INSERT INTO feed_content(content_id,track_id,scope,metadata_json,facets_json,evidence_json,admitted_at,status) VALUES (?,?,?,?,?,?,?,?)',
                (content_id, track_id, 'public', json.dumps(metadata, ensure_ascii=False), json.dumps({'genres': ['rock' if n == 1000 else 'folk']}), '[]', time.time(), 'admitted'))
            index_content(conn, creator, content_id, time.time() - 100 - n)
    first = service.create('legacy-owner', mode='following', request_text='摇滚', request_key='strict')
    assert first['items'] == [] and first['hasMore']
    second = service.page('legacy-owner', first['sessionId'], cursor=first['nextCursor'])
    assert [item['contentId'] for item in second['items']] == ['bulk-01000']
