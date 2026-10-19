"""Domain delivery journal, independently leased job publication, and UGC authority."""

import sqlalchemy as sa
from alembic import op
from domain_event_schema import JOB_PUBLISH_COLUMNS, STATEMENTS
from ugc.schema import STATEMENTS as UGC_STATEMENTS

revision = "radio_009"
down_revision = "radio_008"


def upgrade():
    connection = op.get_bind()
    if connection.dialect.name != "sqlite":
        raise RuntimeError("Use the explicit MySQL radio_008→radio_009 Core upgrade")
    with op.batch_alter_table("feed_content") as batch:
        batch.alter_column("track_id", existing_type=sa.Text(), nullable=True)
        batch.add_column(
            sa.Column("resource_kind", sa.Text(), nullable=False, server_default="video")
        )
        batch.add_column(
            sa.Column("permission_version", sa.Integer(), nullable=False, server_default="1")
        )
    for name, definition in JOB_PUBLISH_COLUMNS:
        op.execute(f"ALTER TABLE durable_jobs ADD COLUMN {name} {definition}")
    for statement in (*STATEMENTS, *UGC_STATEMENTS):
        op.execute(statement)
    op.execute("PRAGMA user_version=33")


def downgrade():
    raise RuntimeError("Delivery and permission facts cannot be destructively downgraded")
