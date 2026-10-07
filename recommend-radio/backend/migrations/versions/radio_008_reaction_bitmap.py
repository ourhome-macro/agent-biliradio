"""Persistent dense user identities for sharded Redis reaction bitmaps."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import VARCHAR

revision = "radio_008"
down_revision = "radio_007"


def upgrade():
    connection = op.get_bind()
    mysql = connection.dialect.name == "mysql"
    inspector = sa.inspect(connection)
    if not inspector.has_table("reaction_bitmap_users"):
        op.create_table(
            "reaction_bitmap_users",
            sa.Column(
                "bitmap_id",
                sa.Integer().with_variant(sa.BigInteger(), "mysql"),
                primary_key=True,
                autoincrement=True,
            ),
            sa.Column(
                "user_id",
                sa.Text().with_variant(VARCHAR(191, collation="utf8mb4_0900_bin"), "mysql"),
                nullable=False,
            ),
            sqlite_autoincrement=True,
            mysql_engine="InnoDB",
            mysql_charset="utf8mb4",
            mysql_collate="utf8mb4_0900_bin",
        )
    indexes = sa.inspect(connection).get_indexes("reaction_bitmap_users")
    if "idx_reaction_bitmap_user" not in {i["name"] for i in indexes}:
        op.create_index(
            "idx_reaction_bitmap_user", "reaction_bitmap_users", ["user_id"], unique=True
        )
    # Deterministic initial allocation. Re-running after an interrupted MySQL
    # DDL completes only missing mappings and never renumbers existing IDs.
    op.execute("""INSERT INTO reaction_bitmap_users(user_id)
        SELECT u.user_id FROM (SELECT id AS user_id FROM app_users
        UNION SELECT user_id FROM content_reactions) AS u
        WHERE NOT EXISTS (SELECT 1 FROM reaction_bitmap_users b WHERE b.user_id=u.user_id)
        ORDER BY u.user_id""")
    if not mysql:
        op.execute("PRAGMA user_version=32")


def downgrade():
    # Keep immutable IDs on rollback. A later upgrade reuses the same mapping.
    if op.get_bind().dialect.name != "mysql":
        op.execute("PRAGMA user_version=31")
