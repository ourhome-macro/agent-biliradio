"""Durable feed, platform reactions and shared media materialization."""

from alembic import op
from feed_runtime.schema import STATEMENTS, TABLES

revision = "radio_004"
down_revision = "radio_003"


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)
    op.execute("PRAGMA user_version = 28")


def downgrade():
    for table in TABLES:
        op.drop_table(table)
    op.execute("PRAGMA user_version = 27")
