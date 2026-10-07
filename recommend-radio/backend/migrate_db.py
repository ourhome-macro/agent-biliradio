"""One-shot schema migration, run before application workers start."""
import os

os.environ.pop('REQUIRE_DB_MIGRATION', None)

from database import DEFAULT_DB_PATH, init_db  # noqa: E402
from migrate_legacy_sse import import_legacy_sse_events  # noqa: E402

if __name__ == '__main__':
    init_db()
    import_legacy_sse_events(DEFAULT_DB_PATH, DEFAULT_DB_PATH.parent / 'sse-events.sqlite3')
