STATEMENTS = (
    """CREATE TABLE ugc_posts (
        content_id TEXT PRIMARY KEY REFERENCES feed_content(content_id),
        author_id TEXT NOT NULL,kind TEXT NOT NULL,title TEXT NOT NULL,body TEXT NOT NULL,
        status TEXT NOT NULL,visibility TEXT NOT NULL,version INTEGER NOT NULL,
        permission_version INTEGER NOT NULL,media_source_content_id TEXT,
        media_asset_id TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL,published_at REAL)""",
    "CREATE INDEX idx_ugc_author_status ON ugc_posts(author_id,status,updated_at)",
    """CREATE TABLE ugc_commands (
        user_id TEXT NOT NULL,command_id TEXT NOT NULL,content_id TEXT NOT NULL,
        operation TEXT NOT NULL,payload_hash TEXT NOT NULL,result_json TEXT NOT NULL,
        created_at REAL NOT NULL,PRIMARY KEY(user_id,command_id))""",
)
