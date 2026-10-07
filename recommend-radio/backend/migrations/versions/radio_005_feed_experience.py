"""Grounded watch projections, operational audit and durable media measurements."""

from alembic import op

revision = "radio_005"
down_revision = "radio_004"


def upgrade():
    op.execute("ALTER TABLE media_playbacks ADD COLUMN playable_at REAL")
    op.execute(
        "ALTER TABLE media_playbacks ADD COLUMN content_id TEXT REFERENCES feed_content(content_id)"
    )
    op.execute("""UPDATE media_playbacks SET content_id=COALESCE(
        (SELECT i.content_id FROM feed_events e JOIN feed_items i ON i.item_id=e.item_id
         WHERE json_extract(e.payload_json,'$.playbackId')=media_playbacks.playback_id
         AND e.user_id=media_playbacks.user_id ORDER BY e.recorded_at LIMIT 1),
        (SELECT content_id FROM media_assets a WHERE a.asset_id=media_playbacks.asset_id))""")
    op.execute("""UPDATE media_playbacks SET playable_at=(SELECT MIN(e.recorded_at)
        FROM feed_events e WHERE e.event_type='play_started'
        AND json_extract(e.payload_json,'$.playbackId')=media_playbacks.playback_id
        AND e.user_id=media_playbacks.user_id)""")
    op.execute("""CREATE TABLE feed_user_content (
        user_id TEXT NOT NULL, content_id TEXT NOT NULL REFERENCES feed_content(content_id),
        last_exposed_at REAL NOT NULL DEFAULT 0, last_watched_at REAL NOT NULL DEFAULT 0,
        watch_ms REAL NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0,
        last_event_at REAL NOT NULL DEFAULT 0, PRIMARY KEY(user_id,content_id))""")
    op.execute(
        "CREATE INDEX idx_feed_user_content_seen ON feed_user_content(user_id,last_exposed_at)"
    )
    op.execute("""CREATE TABLE feed_playback_progress (
        playback_id TEXT PRIMARY KEY REFERENCES media_playbacks(playback_id),
        user_id TEXT NOT NULL, content_id TEXT NOT NULL,
        watch_ms REAL NOT NULL DEFAULT 0, buffering_ms REAL NOT NULL DEFAULT 0,
        startup_ms REAL, completed INTEGER NOT NULL DEFAULT 0,
        last_event_at REAL NOT NULL DEFAULT 0)""")
    op.execute("""CREATE TABLE feed_ops_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id TEXT NOT NULL,
        action TEXT NOT NULL, target_id TEXT NOT NULL, reason TEXT NOT NULL,
        details_json TEXT NOT NULL, created_at REAL NOT NULL)""")
    op.execute("""CREATE TABLE media_measurements (
        import_id TEXT PRIMARY KEY REFERENCES media_import_jobs(import_id),
        first_fragment_seconds REAL, import_seconds REAL,
        outcome TEXT, updated_at REAL NOT NULL)""")
    op.execute(
        "CREATE INDEX idx_feed_events_user_type "
        "ON feed_events(user_id,event_type,item_id,recorded_at)"
    )
    op.execute("CREATE INDEX idx_feed_events_time_type ON feed_events(recorded_at,event_type)")
    op.execute("CREATE INDEX idx_feed_progress_time ON feed_playback_progress(last_event_at)")
    op.execute("CREATE INDEX idx_media_measurement_time ON media_measurements(updated_at)")
    op.execute("""INSERT INTO feed_playback_progress
        (playback_id,user_id,content_id,watch_ms,buffering_ms,startup_ms,completed,last_event_at)
        SELECT p.playback_id,p.user_id,p.content_id,
        MAX(COALESCE(CAST(json_extract(e.payload_json,'$.watchMs') AS REAL),0)),
        MAX(COALESCE(CAST(json_extract(e.payload_json,'$.bufferingMs') AS REAL),0)),
        MIN(CAST(json_extract(e.payload_json,'$.startupMs') AS REAL)),
        MAX(e.event_type='complete'),MAX(e.recorded_at)
        FROM feed_events e JOIN feed_items i ON i.item_id=e.item_id JOIN media_playbacks p
        ON p.playback_id=json_extract(e.payload_json,'$.playbackId')
        AND p.user_id=e.user_id AND p.content_id=i.content_id
        WHERE e.event_type IN ('play_started','watch_progress','complete')
        GROUP BY p.playback_id,p.user_id,p.content_id""")
    op.execute("""INSERT INTO feed_user_content
        (user_id,content_id,last_exposed_at,last_event_at)
        SELECT e.user_id,i.content_id,MAX(e.recorded_at),MAX(e.recorded_at)
        FROM feed_events e JOIN feed_items i ON i.item_id=e.item_id WHERE e.event_type='impression'
        GROUP BY e.user_id,i.content_id""")
    op.execute("""INSERT INTO feed_user_content
        (user_id,content_id,last_watched_at,watch_ms,completed,last_event_at)
        SELECT user_id,content_id,MAX(CASE WHEN watch_ms>0 THEN last_event_at ELSE 0 END),
        SUM(watch_ms),MAX(completed),MAX(last_event_at) FROM feed_playback_progress
        WHERE content_id IS NOT NULL GROUP BY user_id,content_id
        ON CONFLICT(user_id,content_id) DO UPDATE SET last_watched_at=excluded.last_watched_at,
        watch_ms=excluded.watch_ms,completed=excluded.completed,
        last_event_at=MAX(feed_user_content.last_event_at,excluded.last_event_at)""")
    op.execute("PRAGMA user_version = 29")


def downgrade():
    op.drop_index("idx_feed_events_user_type", table_name="feed_events")
    op.drop_index("idx_feed_events_time_type", table_name="feed_events")
    for table in (
        "media_measurements",
        "feed_ops_audit",
        "feed_playback_progress",
        "feed_user_content",
    ):
        op.drop_table(table)
    op.drop_column("media_playbacks", "playable_at")
    op.drop_column("media_playbacks", "content_id")
    op.execute("PRAGMA user_version = 28")
