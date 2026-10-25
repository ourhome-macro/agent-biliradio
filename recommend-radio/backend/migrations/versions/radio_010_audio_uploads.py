"""UGC audio direct-upload sessions and attachment identity."""

from alembic import op
from ugc.uploads_schema import STATEMENTS

revision = "radio_010"
down_revision = "radio_009"


def upgrade():
    if op.get_bind().dialect.name != "sqlite":
        raise RuntimeError("Use the explicit MySQL radio_009→radio_010 upgrade")
    op.execute("ALTER TABLE ugc_posts ADD COLUMN audio_upload_id TEXT")
    for statement in STATEMENTS:
        op.execute(statement)
    op.execute("PRAGMA user_version=34")


def downgrade():
    raise RuntimeError("Audio upload facts cannot be destructively downgraded")
