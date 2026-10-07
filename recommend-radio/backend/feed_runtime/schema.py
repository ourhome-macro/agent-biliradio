"""Schema is applied exclusively by Alembic, never lazily by HTTP handlers."""

STATEMENTS = (
    """CREATE TABLE feed_content (
        content_id TEXT PRIMARY KEY, track_id TEXT NOT NULL REFERENCES tracks(track_id),
        scope TEXT NOT NULL, owner_id TEXT, metadata_json TEXT NOT NULL,
        facets_json TEXT NOT NULL DEFAULT '{}', evidence_json TEXT NOT NULL DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'admitted', version INTEGER NOT NULL DEFAULT 1,
        admitted_at REAL NOT NULL, UNIQUE(track_id, scope))""",
    "CREATE INDEX idx_feed_content_scope ON feed_content(scope,status,admitted_at,content_id)",
    """CREATE TABLE media_assets (
        asset_id TEXT PRIMARY KEY, content_id TEXT NOT NULL REFERENCES feed_content(content_id),
        policy TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'empty',
        version INTEGER NOT NULL DEFAULT 0,
        manifest_json TEXT NOT NULL DEFAULT '{}', bytes INTEGER NOT NULL DEFAULT 0,
        error_type TEXT, updated_at REAL NOT NULL, UNIQUE(content_id,policy))""",
    """CREATE TABLE content_media_links (
        content_id TEXT PRIMARY KEY REFERENCES feed_content(content_id),
        asset_id TEXT NOT NULL REFERENCES media_assets(asset_id))""",
    """CREATE TABLE media_import_jobs (
        import_id TEXT PRIMARY KEY, asset_id TEXT NOT NULL REFERENCES media_assets(asset_id),
        job_id TEXT NOT NULL UNIQUE, attempt INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        stage TEXT NOT NULL DEFAULT 'queued', lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0,
        work_key TEXT NOT NULL, complete_source INTEGER NOT NULL DEFAULT 0,
        fetch_attempts INTEGER NOT NULL DEFAULT 0,
        idle_since REAL, error_type TEXT, bytes INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL, updated_at REAL NOT NULL, UNIQUE(asset_id,attempt))""",
    "CREATE UNIQUE INDEX idx_media_active_import ON media_import_jobs(asset_id) "
    "WHERE status IN ('queued','running')",
    """CREATE TABLE media_playbacks (
        playback_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
        asset_id TEXT NOT NULL REFERENCES media_assets(asset_id),
        import_id TEXT REFERENCES media_import_jobs(import_id), request_key TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1, expires_at REAL NOT NULL, created_at REAL NOT NULL,
        UNIQUE(user_id,request_key))""",
    "CREATE INDEX idx_media_playbacks_demand ON media_playbacks(import_id,active,expires_at)",
    """CREATE TABLE feed_sessions (
        session_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_key TEXT NOT NULL,
        mode TEXT NOT NULL, spec_json TEXT NOT NULL, profile_json TEXT NOT NULL,
        profile_version TEXT NOT NULL, policy_version TEXT NOT NULL,
        revision TEXT NOT NULL, expires_at REAL NOT NULL, created_at REAL NOT NULL,
        UNIQUE(user_id,request_key))""",
    """CREATE TABLE feed_pages (
        page_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES feed_sessions(session_id),
        page_number INTEGER NOT NULL, cursor TEXT NOT NULL UNIQUE, next_cursor TEXT NOT NULL UNIQUE,
        supply_state TEXT NOT NULL, created_at REAL NOT NULL, UNIQUE(session_id,page_number))""",
    """CREATE TABLE feed_items (
        item_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES feed_sessions(session_id),
        page_id TEXT NOT NULL REFERENCES feed_pages(page_id),
        content_id TEXT NOT NULL REFERENCES feed_content(content_id),
        rank INTEGER NOT NULL, snapshot_json TEXT NOT NULL, issued_at REAL NOT NULL,
        UNIQUE(session_id,content_id), UNIQUE(page_id,rank))""",
    """CREATE TABLE feed_events (
        event_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
        item_id TEXT NOT NULL REFERENCES feed_items(item_id),
        event_type TEXT NOT NULL, payload_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
        occurred_at REAL NOT NULL, recorded_at REAL NOT NULL)""",
    "CREATE INDEX idx_feed_events_item ON feed_events(item_id,event_type)",
    """CREATE TABLE content_reactions (
        user_id TEXT NOT NULL, content_id TEXT NOT NULL REFERENCES feed_content(content_id),
        state TEXT NOT NULL, version INTEGER NOT NULL, updated_at REAL NOT NULL,
        PRIMARY KEY(user_id,content_id))""",
    """CREATE TABLE reaction_commands (
        user_id TEXT NOT NULL, command_id TEXT NOT NULL, content_id TEXT NOT NULL,
        desired_state TEXT NOT NULL, result_json TEXT NOT NULL, PRIMARY KEY(user_id,command_id))""",
    """CREATE TABLE feed_inbox (
        consumer TEXT NOT NULL, event_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
        processed_at REAL NOT NULL, PRIMARY KEY(consumer,event_id))""",
    """CREATE TABLE reaction_projection (
        user_id TEXT NOT NULL, content_id TEXT NOT NULL, state TEXT NOT NULL,
        version INTEGER NOT NULL, PRIMARY KEY(user_id,content_id))""",
    """CREATE TABLE content_counters (
        content_id TEXT PRIMARY KEY REFERENCES feed_content(content_id),
        likes INTEGER NOT NULL DEFAULT 0 CHECK(likes>=0),
        dislikes INTEGER NOT NULL DEFAULT 0 CHECK(dislikes>=0),
        version INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)""",
    """CREATE TABLE media_storage_reservations (
        import_id TEXT PRIMARY KEY REFERENCES media_import_jobs(import_id),
        bytes INTEGER NOT NULL, updated_at REAL NOT NULL)""",
)

TABLES = (
    "media_storage_reservations",
    "content_counters",
    "reaction_projection",
    "feed_inbox",
    "reaction_commands",
    "content_reactions",
    "feed_events",
    "feed_items",
    "feed_pages",
    "feed_sessions",
    "media_playbacks",
    "media_import_jobs",
    "content_media_links",
    "media_assets",
    "feed_content",
)
