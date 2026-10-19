"""One-shot schema migration, run before application workers start."""

import os

os.environ.pop("REQUIRE_DB_MIGRATION", None)

from database import DEFAULT_DB_PATH, init_db, mysql_target  # noqa: E402
from migrate_legacy_sse import import_legacy_sse_events  # noqa: E402

if __name__ == "__main__":
    target = mysql_target()
    if target:
        import sqlalchemy as sa
        from mysql_storage.connection import engine_for
        from mysql_storage.upgrade import upgrade_mysql

        if sa.inspect(engine_for(target)).has_table("alembic_version"):
            upgrade_mysql(target)
        else:
            init_db()
    else:
        init_db()
    import_legacy_sse_events(DEFAULT_DB_PATH, DEFAULT_DB_PATH.parent / "sse-events.sqlite3")
