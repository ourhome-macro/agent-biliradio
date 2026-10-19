"""Explicit MySQL upgrades: apply a known revision, never stamp an old baseline."""

from __future__ import annotations

import sqlalchemy as sa

from .connection import _initialized, engine_for, tables_for


def upgrade_mysql(url):
    from schema_migrations import HEAD

    engine = engine_for(url)
    with engine.connect() as connection:
        if not sa.inspect(connection).has_table("alembic_version"):
            raise RuntimeError("Initialize or import a new MySQL database before upgrading")
        current = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one()
    from .upgrades import upgrade_mysql as apply_versions

    apply_versions(engine, HEAD)
    tables_for.cache_clear()
    _initialized.clear()
    return {"revision": HEAD, "changed": current != HEAD}


def main():
    import argparse
    import json
    import os

    from env_loader import load_recommend_radio_env

    load_recommend_radio_env()
    parser = argparse.ArgumentParser(description="Apply an explicit MySQL business schema upgrade")
    parser.add_argument(
        "--url-env",
        default="MYSQL_MIGRATION_URL",
        help="Environment variable containing an administrative database URL",
    )
    args = parser.parse_args()
    url = os.getenv(args.url_env)
    if not url:
        parser.error("The specified migration URL environment variable is empty")
    print(json.dumps(upgrade_mysql(url)))


if __name__ == "__main__":
    main()
