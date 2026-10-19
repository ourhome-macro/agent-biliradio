from dataclasses import replace
from contextlib import closing
import sqlite3
import time

import fakeredis
from alembic import command

from database import get_connection
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.media import MediaService
from feed_runtime.repository import FeedRepository
from models import Track
from schema_migrations import config as alembic_config


def test_upgrade_backfills_monotonic_watch_and_exposure_from_existing_events(tmp_path,monkeypatch):
    monkeypatch.setenv("JOB_TRANSPORT","disabled")
    repo=FeedRepository(tmp_path/"legacy.sqlite3")
    configuration=replace(FeedConfig(),workdir=tmp_path/"work")
    cache=FeedCache(None,client=fakeredis.FakeRedis(decode_responses=True))
    try:
        content=repo.admit(Track(bvid="BV0000000001",cid=1,title="Music Live",duration=12),user_id="legacy-owner")
        item=FeedService(repo,configuration,cache).create("legacy-owner",request_key="one")["items"][0]
        playback=MediaService(repo,configuration,None).create("legacy-owner",content,"one")
        with get_connection(repo.db_path) as conn:
            conn.execute("UPDATE media_playbacks SET created_at=?",(time.time()-15,))
            conn.execute("UPDATE media_import_jobs SET complete_source=1")
        feed=FeedService(repo,configuration,cache)
        feed.events("legacy-owner",[{"eventId":"shown","itemId":item["itemId"],"type":"impression",
                                    "visibleRatio":1,"visibleMs":1000}])
        for index,duration in enumerate((11000,5000,12000)):
            feed.events("legacy-owner",[{"eventId":f"watch-{index}","itemId":item["itemId"],
                "type":"complete" if index==2 else "watch_progress",
                "playbackId":playback["playbackId"],"watchMs":duration,"bufferingMs":1000}])
        # Build an actual historical schema copy. Newer delivery/permission facts
        # deliberately refuse destructive downgrade in production.
        historical=tmp_path/"historical_004.sqlite3"
        command.upgrade(alembic_config(historical),"radio_004")
        with closing(sqlite3.connect(repo.db_path)) as source:
            with closing(sqlite3.connect(historical)) as target:
                names=[r[0] for r in target.execute("SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'alembic_version'")]
                for name in names:
                    fields=[r[1] for r in target.execute(f'PRAGMA table_info("{name}")')]
                    target.execute(f'DELETE FROM "{name}"')
                    rows=source.execute(f'SELECT {",".join(fields)} FROM "{name}"').fetchall()
                    if rows:
                        target.executemany(f'INSERT INTO "{name}" ({",".join(fields)}) '
                            f'VALUES ({",".join("?" for _ in fields)})',rows)
                target.commit()
        command.upgrade(alembic_config(historical),"head")
        with get_connection(historical) as conn:
            row=conn.execute("SELECT * FROM feed_user_content").fetchone()
            assert row["watch_ms"]==12000 and row["completed"]==1 and row["last_exposed_at"]>0
            progress=conn.execute("SELECT * FROM feed_playback_progress").fetchone()
            assert progress["watch_ms"]==12000 and progress["buffering_ms"]==1000
            assert conn.execute("SELECT content_id FROM media_playbacks").fetchone()[0]==content
            assert conn.execute("PRAGMA user_version").fetchone()[0]==33
    finally:
        cache.close()
