"""Bounded direct-upload commands; object bytes never enter SQL."""

STATEMENTS = (
    """CREATE TABLE ugc_audio_uploads (
        upload_id TEXT PRIMARY KEY, content_id TEXT NOT NULL REFERENCES ugc_posts(content_id),
        user_id TEXT NOT NULL, request_key TEXT NOT NULL, input_hash TEXT NOT NULL,
        object_key TEXT NOT NULL UNIQUE, expected_bytes INTEGER NOT NULL,
        expected_sha256 TEXT NOT NULL, content_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'awaiting', asset_id TEXT NOT NULL,
        job_id TEXT NOT NULL UNIQUE, expires_at REAL NOT NULL,
        error_type TEXT, staging_deleted INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL, updated_at REAL NOT NULL,
        UNIQUE(user_id,request_key))""",
    "CREATE INDEX idx_ugc_upload_expiry ON ugc_audio_uploads(status,expires_at,upload_id)",
    "CREATE INDEX idx_ugc_upload_owner ON ugc_audio_uploads(user_id,status,created_at)",
)
