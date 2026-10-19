"""Business fencing races on both SQLite and a private native MySQL instance."""
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest

from database import begin_write, get_connection
from durable_jobs import claim, enqueue, finish
from error_code import APIError
from feed_runtime.config import FeedConfig
from feed_runtime.media import MediaService
from feed_runtime.reactions import ReactionService
from feed_runtime.repository import FeedRepository
from models import Track


@pytest.fixture(scope='module')
def mysql(tmp_path_factory):
    from mysql_fixture import NativeMySQL
    server = NativeMySQL(tmp_path_factory.mktemp('native-fencing-mysql'))
    yield server.url
    from mysql_storage.connection import engine_for
    engine_for(server.url).dispose()
    server.close()


@pytest.fixture(params=['sqlite', 'mysql'])
def runtime(request, tmp_path, monkeypatch):
    monkeypatch.setenv('FEED_ENABLED', 'false')
    monkeypatch.setenv('JOB_TRANSPORT', 'disabled')
    target = request.getfixturevalue('mysql') if request.param == 'mysql' else tmp_path / 'fence.sqlite3'
    repo = FeedRepository(target)
    config = replace(FeedConfig(), workdir=tmp_path / 'work', cancel_grace_seconds=0)
    return repo, config


def track(*, cid=1, title='Music Live Concert'):
    return Track(bvid='BV' + uuid4().hex[:10], cid=cid, title=title, type_name='音乐', duration=30)


def admit(runtime, value=None, source=None):
    repo, config = runtime
    content = repo.admit(value or track(), user_id='legacy-owner')
    kwargs = {'source_factory': source} if source else {}
    media = MediaService(repo, config, None, **kwargs)
    return content, media


def claimed(runtime, media, content):
    repo, config = runtime
    playback = media.create('legacy-owner', content, uuid4().hex)
    with get_connection(repo.db_path) as conn:
        row = conn.execute('SELECT import_id FROM media_playbacks WHERE playback_id=?', (playback['playbackId'],)).fetchone()
    job = repo.claim_import(row[0], config)
    return playback, job


def test_source_resolution_merges_latest_metadata_and_bumps_version(runtime):
    repo, config = runtime
    entered, release = threading.Event(), threading.Event()
    class Source:
        def __init__(self, **kwargs): pass
        def close(self): pass
        def resolve_cid(self, value):
            entered.set(); assert release.wait(10); return 77
    original = track(cid=None, title='Music original')
    content, media = admit(runtime, original, Source)
    with ThreadPoolExecutor(max_workers=1) as pool:
        waiting = pool.submit(media.create, 'legacy-owner', content, uuid4().hex)
        assert entered.wait(5)
        try:
            replacement = replace(original, title='Music updated during resolution', description='fresh description')
            repo.admit(replacement, user_id='legacy-owner')
            with get_connection(repo.db_path) as conn:
                version = conn.execute('SELECT version FROM feed_content WHERE content_id=?', (content,)).fetchone()[0]
        finally: release.set()
        assert waiting.result(timeout=10)['status'] == 'preparing'
    with get_connection(repo.db_path) as conn:
        current = conn.execute('SELECT metadata_json,version FROM feed_content WHERE content_id=?', (content,)).fetchone()
    metadata = json.loads(current['metadata_json'])
    assert metadata['title'] == replacement.title and metadata['description'] == replacement.description
    assert metadata['cid'] == 77 and current['version'] > version


def test_expired_finish_is_rejected_without_recovery_scan(runtime):
    repo, _ = runtime
    job_id = 'fence:' + uuid4().hex
    with get_connection(repo.db_path) as conn: enqueue(conn, kind='behavior', user_id='legacy-owner', payload={}, job_id=job_id, retry_safe=True)
    with get_connection(repo.db_path) as conn: job = claim(conn, job_id, now=100, lease_seconds=10)
    with get_connection(repo.db_path) as conn: finish(conn, job, result='too late', now=111)
    with get_connection(repo.db_path) as conn: row = conn.execute('SELECT * FROM durable_jobs WHERE job_id=?', (job_id,)).fetchone()
    assert row['status'] == 'running' and row['lease_token'] == job['lease_token'] and row['result_json'] is None


def test_heartbeat_waits_for_cancellation_lock_then_rejects_cancelled_import(runtime, monkeypatch):
    import feed_runtime.media as module
    repo, config = runtime
    content, media = admit(runtime)
    playback, job = claimed(runtime, media, content)
    attempted = threading.Event()
    original = module.begin_write
    def observed(conn, **kwargs):
        if kwargs.get('namespace') == 'media-asset': attempted.set()
        return original(conn, **kwargs)
    monkeypatch.setattr(module, 'begin_write', observed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with get_connection(repo.db_path) as conn:
            begin_write(conn, namespace='media-asset', key=job['asset_id'])
            future = pool.submit(media.heartbeat, playback['playbackId'], 'legacy-owner')
            assert attempted.wait(5)
            assert not future.done()
            conn.execute("UPDATE media_import_jobs SET status='cancelled',lease_token=NULL,lease_until=0 WHERE import_id=?", (job['import_id'],))
        with pytest.raises(APIError): future.result(timeout=10)
    with get_connection(repo.db_path) as conn:
        assert conn.execute('SELECT expires_at FROM media_playbacks WHERE playback_id=?', (playback['playbackId'],)).fetchone()[0] == playback['expiresAt']


def test_heartbeat_winning_race_prevents_idle_cancellation(runtime):
    repo, config = runtime
    content, media = admit(runtime)
    playback, job = claimed(runtime, media, content)
    with get_connection(repo.db_path) as conn:
        conn.execute('UPDATE media_playbacks SET expires_at=? WHERE playback_id=?', (time.time() - 1, playback['playbackId']))
        conn.execute('UPDATE media_import_jobs SET idle_since=? WHERE import_id=?', (time.time() - 5, job['import_id']))
    media.heartbeat(playback['playbackId'], 'legacy-owner')
    assert repo.cancel_if_idle(job, config) is False


def test_alias_bootstrap_and_unlike_share_track_fence(runtime, monkeypatch):
    import feed_runtime.repository as repository_module
    import feed_runtime.reactions as reaction_module
    repo, _ = runtime
    value = track()
    public = repo.admit(value, user_id='legacy-owner')
    reactions = ReactionService(repo)
    reactions.set('legacy-owner', public, 'like', uuid4().hex)
    inserted, release, command_waiting = threading.Event(), threading.Event(), threading.Event()
    raw_connection, raw_lock = repository_module.get_connection, reaction_module.begin_write
    class Observed:
        def __init__(self, inner): self.inner = inner
        def __enter__(self): self.inner.__enter__(); return self
        def __exit__(self, *args): return self.inner.__exit__(*args)
        def __getattr__(self, name): return getattr(self.inner, name)
        def execute(self, sql, *args, **kwargs):
            result = self.inner.execute(sql, *args, **kwargs)
            if 'INSERT INTO content_reactions' in sql and 'SELECT user_id' in sql:
                inserted.set(); assert release.wait(10)
            return result
    monkeypatch.setattr(repository_module, 'get_connection', lambda path: Observed(raw_connection(path)))
    def observe_lock(conn, **kwargs):
        if kwargs.get('namespace') == 'reaction-track': command_waiting.set()
        return raw_lock(conn, **kwargs)
    monkeypatch.setattr(reaction_module, 'begin_write', observe_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        alias_task = pool.submit(repo.admit, value, user_id='legacy-owner', scope='user:legacy-owner')
        assert inserted.wait(5)
        try:
            unlike = pool.submit(reactions.set, 'legacy-owner', public, 'neutral', uuid4().hex)
            if repo.db_path.startswith('mysql'):
                assert command_waiting.wait(5)
            time.sleep(.1)
            assert not unlike.done()
        finally: release.set()
        alias = alias_task.result(timeout=10)
        unlike.result(timeout=10)
    with get_connection(repo.db_path) as conn:
        rows = conn.execute('SELECT state FROM content_reactions WHERE user_id=? AND content_id IN (?,?)', ('legacy-owner', public, alias)).fetchall()
        assert [row[0] for row in rows] == ['neutral', 'neutral']
        assert conn.execute('SELECT COUNT(*) FROM likes WHERE user_id=? AND track_id=?', ('legacy-owner', value.track_id)).fetchone()[0] == 0
        assert conn.execute('SELECT SUM(likes) FROM content_counters WHERE content_id IN (?,?)', (public, alias)).fetchone()[0] == 0


def test_old_attempt_cannot_mark_new_attempt_unsupported(runtime):
    repo, config = runtime
    content, media = admit(runtime)
    _, old = claimed(runtime, media, content)
    repo.stop_import(old, status='failed')
    _, current = claimed(runtime, media, content)
    with pytest.raises(RuntimeError, match='MediaLeaseLost'):
        repo.stop_import(old, status='failed', asset_status='unsupported')
    with get_connection(repo.db_path) as conn:
        assert conn.execute('SELECT status FROM media_assets WHERE asset_id=?', (current['asset_id'],)).fetchone()[0] != 'unsupported'
        assert conn.execute('SELECT lease_token FROM media_import_jobs WHERE import_id=?', (current['import_id'],)).fetchone()[0] == current['lease_token']


def test_retire_rechecks_asset_created_after_initial_unlinked_read(runtime, monkeypatch):
    from types import SimpleNamespace
    import feed_runtime.repository as module
    from feed_runtime.operations import FeedOperations
    repo, config = runtime
    content, media = admit(runtime)
    operations = FeedOperations(SimpleNamespace(repo=repo, config=config))
    entered, release = threading.Event(), threading.Event()
    initial = module.begin_write
    used = False
    def interleave(conn, **kwargs):
        nonlocal used
        if threading.current_thread().name.startswith('retire-race') and kwargs.get('namespace') == 'content' and not used:
            used = True; entered.set(); assert release.wait(10)
        return initial(conn, **kwargs)
    monkeypatch.setattr(module, 'begin_write', interleave)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='retire-race') as pool:
        retired = pool.submit(operations.set_status, content, 'retired', actor_id='legacy-owner', reason='race acceptance')
        assert entered.wait(5)
        try: playback = media.create('legacy-owner', content, uuid4().hex)
        finally: release.set()
        assert retired.result(timeout=10)['status'] == 'retired'
    with get_connection(repo.db_path) as conn:
        assert conn.execute('SELECT status FROM media_assets WHERE asset_id=?', (playback['assetId'],)).fetchone()[0] == 'retired'
        assert conn.execute('SELECT active FROM media_playbacks WHERE playback_id=?', (playback['playbackId'],)).fetchone()[0] == 0
        assert conn.execute('SELECT status FROM media_import_jobs WHERE asset_id=?', (playback['assetId'],)).fetchone()[0] == 'cancelled'
