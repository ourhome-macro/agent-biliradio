"""Local regression for aggregate locks while the business database is migrated."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from database import get_connection
from dialogue_repository import DialogueRepository
from library_service import LibraryService
from models import Track


@pytest.fixture
def library(tmp_path, monkeypatch):
    monkeypatch.setenv('RABBITMQ_ENABLED', 'false')
    monkeypatch.setenv('FEED_ENABLED', 'false')
    monkeypatch.setenv('JOB_TRANSPORT', 'redis_stream')
    return LibraryService(tmp_path / 'legacy.sqlite3')


def concurrent(*calls):
    barrier = Barrier(len(calls))
    def run(call):
        barrier.wait()
        return call()
    with ThreadPoolExecutor(max_workers=len(calls)) as pool:
        return list(pool.map(run, calls))


def test_parallel_playlist_append_assigns_unique_positions(library):
    playlist = library.create_playlist('concurrent')
    batches = [[Track(bvid=f'BV{i:010d}', cid=i + 1, title=f'Track {i}')] for i in range(12)]
    concurrent(*(lambda tracks=tracks: library.batch_add_playlist_items(playlist['id'], tracks) for tracks in batches))
    with get_connection(library.db_path) as conn:
        positions = [row[0] for row in conn.execute('SELECT position FROM playlist_items WHERE playlist_id=? ORDER BY position', (playlist['id'],))]
    assert positions == list(range(12))


def test_parallel_playlist_partial_updates_keep_both_fields(library):
    playlist = library.create_playlist('old')
    concurrent(lambda: library.update_playlist(playlist['id'], name='new'),
               lambda: library.update_playlist(playlist['id'], cover='cover-new'))
    value = library.get_playlist(playlist['id'])
    assert value['name'] == 'new'
    assert value['cover'] == 'cover-new'


def test_same_review_concurrency_has_one_changed_behavior(library):
    track = Track(bvid='BV0000000001', cid=1, title='Review track')
    concurrent(*(lambda: library.save_review(track, 4, '开心', 'same') for _ in range(6)))
    with get_connection(library.db_path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM track_reviews').fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM durable_jobs WHERE kind='behavior' AND json_extract(payload_json,'$.event')='track_reviewed'").fetchone()[0] == 1


def test_default_dialogue_session_creation_is_serialized(library):
    repo = DialogueRepository(library.db_path, library.user_id)
    def resolve():
        with get_connection(library.db_path) as conn:
            return repo._get_or_create_session(conn, session_id=None)['session_id']
    results = concurrent(*(resolve for _ in range(8)))
    assert len(set(results)) == 1
