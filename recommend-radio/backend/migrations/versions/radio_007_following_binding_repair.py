"""Complete the following schema for databases created during the 006 rollout."""

import hashlib
import json

import sqlalchemy as sa
from alembic import op

revision = "radio_007"
down_revision = "radio_006"


def upgrade():
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    tables = set(inspector.get_table_names())
    if "bili_binding_epochs" not in tables:
        op.execute("""CREATE TABLE bili_binding_epochs (
            user_id TEXT PRIMARY KEY,account_mid TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 1,active INTEGER NOT NULL DEFAULT 1,
            updated_at REAL NOT NULL,binding_key TEXT NOT NULL DEFAULT '')""")
    elif "binding_key" not in {
        column["name"] for column in inspector.get_columns("bili_binding_epochs")
    }:
        op.execute(
            "ALTER TABLE bili_binding_epochs ADD COLUMN binding_key TEXT NOT NULL DEFAULT ''"
        )
    supply_columns = {column["name"] for column in inspector.get_columns("creator_supply")}
    if "next_page" not in supply_columns:
        op.execute("ALTER TABLE creator_supply ADD COLUMN next_page INTEGER NOT NULL DEFAULT 1")
    if "scan_complete" not in supply_columns:
        op.execute("ALTER TABLE creator_supply ADD COLUMN scan_complete INTEGER NOT NULL DEFAULT 0")
    indexes = {index["name"] for index in inspector.get_indexes("creator_follow_relations")}
    if "idx_follow_supply" not in indexes:
        op.execute(
            "CREATE INDEX idx_follow_supply ON creator_follow_relations"
            "(desired_state,confirmed_state,sync_status,creator_id)"
        )
    op.execute("""INSERT INTO bili_binding_epochs(user_id,account_mid,generation,active,updated_at)
        SELECT b.user_id,CAST(b.user_mid AS TEXT),1,1,0 FROM bili_accounts b
        WHERE b.user_mid IS NOT NULL AND b.cookie_encrypted IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM bili_binding_epochs e WHERE e.user_id=b.user_id)""")
    for user, mid, generation in connection.execute(
        sa.text(
            "SELECT user_id,account_mid,generation FROM bili_binding_epochs WHERE binding_key=''"
        )
    ):
        key = hashlib.sha256(
            json.dumps(
                [user, mid, generation], ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        connection.execute(
            sa.text("UPDATE bili_binding_epochs SET binding_key=:key WHERE user_id=:user"),
            {"key": key, "user": user},
        )
    op.execute("PRAGMA user_version=31")


def downgrade():
    # 006 already declares these columns for new installations; retaining them
    # preserves the 006 contract without deleting binding or pagination state.
    op.execute("PRAGMA user_version=30")
