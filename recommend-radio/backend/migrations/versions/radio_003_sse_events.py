"""Keep replayable SSE events in the business database."""

import sqlalchemy as sa
from alembic import op

revision = "radio_003"
down_revision = "radio_002"


def upgrade():
    op.create_table(
        "dialogue_events",
        sa.Column("event_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_key", sa.Text(), nullable=False, unique=True),
        sa.Column("task_id", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
    )
    op.create_index(
        "idx_dialogue_events_replay",
        "dialogue_events",
        ["user_id", "session_id", "event_id"],
    )
    op.create_index("idx_dialogue_events_created", "dialogue_events", ["created_at"])
    op.execute("PRAGMA user_version = 27")


def downgrade():
    op.drop_index("idx_dialogue_events_created", table_name="dialogue_events")
    op.drop_index("idx_dialogue_events_replay", table_name="dialogue_events")
    op.drop_table("dialogue_events")
    op.execute("PRAGMA user_version = 26")
