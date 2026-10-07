"""Trusted creators, account-scoped follow commands and shared creator supply."""

from alembic import op

revision = "radio_006"
down_revision = "radio_005"


def upgrade():
    statements = (
        """CREATE TABLE bili_binding_epochs (
        user_id TEXT PRIMARY KEY,account_mid TEXT NOT NULL,generation INTEGER NOT NULL DEFAULT 1,
        active INTEGER NOT NULL DEFAULT 1,updated_at REAL NOT NULL,
        binding_key TEXT NOT NULL DEFAULT '')""",
        """CREATE TABLE creators (
        creator_id TEXT PRIMARY KEY, provider TEXT NOT NULL, external_id TEXT NOT NULL,
        display_name TEXT NOT NULL DEFAULT '', avatar TEXT NOT NULL DEFAULT '',
        verified_at REAL NOT NULL, UNIQUE(provider,external_id))""",
        """CREATE TABLE creator_follow_relations (
        user_id TEXT NOT NULL, bili_account_mid TEXT NOT NULL,
        creator_id TEXT NOT NULL REFERENCES creators(creator_id), binding_key TEXT NOT NULL,
        desired_state INTEGER NOT NULL, confirmed_state INTEGER, sync_status TEXT NOT NULL,
        version INTEGER NOT NULL, last_verified_at REAL, error_type TEXT,
        lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0, updated_at REAL NOT NULL,
        PRIMARY KEY(user_id,bili_account_mid,creator_id))""",
        "CREATE INDEX idx_follow_active ON "
        "creator_follow_relations(user_id,bili_account_mid,desired_state,confirmed_state)",
        "CREATE INDEX idx_follow_supply ON "
        "creator_follow_relations(desired_state,confirmed_state,sync_status,creator_id)",
        """CREATE TABLE creator_follow_commands (
        user_id TEXT NOT NULL, command_id TEXT NOT NULL, creator_id TEXT NOT NULL,
        bili_account_mid TEXT NOT NULL, binding_key TEXT NOT NULL, desired_state INTEGER NOT NULL,
        version INTEGER NOT NULL, result_json TEXT NOT NULL, created_at REAL NOT NULL,
        PRIMARY KEY(user_id,command_id))""",
        """CREATE TABLE follow_sync_runs (
        sync_run_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, bili_account_mid TEXT NOT NULL,
        binding_key TEXT NOT NULL, command_id TEXT NOT NULL, status TEXT NOT NULL,
        page INTEGER NOT NULL DEFAULT 1, total INTEGER, seen_json TEXT NOT NULL DEFAULT '[]',
        reconcile_json TEXT NOT NULL DEFAULT '[]', complete INTEGER NOT NULL DEFAULT 0,
        error_type TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
        UNIQUE(user_id,command_id))""",
        "CREATE INDEX idx_follow_sync_user ON "
        "follow_sync_runs(user_id,bili_account_mid,created_at)",
        """CREATE TABLE creator_content (
        creator_id TEXT NOT NULL REFERENCES creators(creator_id),
        content_id TEXT NOT NULL REFERENCES feed_content(content_id),
        published_at REAL NOT NULL, indexed_at REAL NOT NULL,
        PRIMARY KEY(creator_id,content_id))""",
        "CREATE INDEX idx_creator_content_time ON "
        "creator_content(creator_id,published_at,content_id)",
        """CREATE TABLE creator_supply (
        creator_id TEXT PRIMARY KEY REFERENCES creators(creator_id),
        status TEXT NOT NULL DEFAULT 'idle',
        lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0,next_check_at REAL NOT NULL DEFAULT 0,
        last_success_at REAL,error TEXT,version INTEGER NOT NULL DEFAULT 0,
        next_page INTEGER NOT NULL DEFAULT 1,scan_complete INTEGER NOT NULL DEFAULT 0)""",
    )
    for statement in statements:
        op.execute(statement)
    op.execute("""INSERT INTO bili_binding_epochs(user_id,account_mid,generation,active,updated_at)
        SELECT user_id,CAST(user_mid AS TEXT),1,1,0 FROM bili_accounts
        WHERE user_mid IS NOT NULL AND cookie_encrypted IS NOT NULL""")
    import hashlib
    import json

    import sqlalchemy as sa

    connection = op.get_bind()
    for user, mid in connection.execute(
        sa.text("SELECT user_id,account_mid FROM bili_binding_epochs")
    ):
        key = hashlib.sha256(
            json.dumps(
                [user, mid, 1], ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        connection.execute(
            sa.text("UPDATE bili_binding_epochs SET binding_key=:key WHERE user_id=:user"),
            {"key": key, "user": user},
        )
    op.execute("PRAGMA user_version=30")


def downgrade():
    for name in (
        "creator_supply",
        "creator_content",
        "follow_sync_runs",
        "creator_follow_commands",
        "creator_follow_relations",
        "creators",
        "bili_binding_epochs",
    ):
        op.drop_table(name)
    op.execute("PRAGMA user_version=29")
