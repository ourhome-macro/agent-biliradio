"""Indexed projection batches; durable event payloads and identities are retained."""

from alembic import op

revision = "radio_011"
down_revision = "radio_010"


def upgrade():
    if op.get_bind().dialect.name != "sqlite":
        raise RuntimeError("Use the explicit MySQL radio_010 to radio_011 upgrade")
    op.execute("ALTER TABLE durable_jobs ADD COLUMN projection_key TEXT NOT NULL DEFAULT ''")
    op.execute(
        "UPDATE durable_jobs SET projection_key=COALESCE(json_extract(payload_json,'$.content_id'),'') "
        "WHERE kind='feed_projection'"
    )
    op.execute(
        "CREATE INDEX idx_job_projection_batch ON durable_jobs "
        "(projection_key,status,available_at,id)"
    )
    op.execute("PRAGMA user_version=35")


def downgrade():
    raise RuntimeError("Projection journals cannot be destructively downgraded")
