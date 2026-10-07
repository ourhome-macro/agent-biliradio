"""Explicit MySQL upgrades: apply a known revision, never stamp an old baseline."""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from .connection import engine_for, tables_for


def upgrade_mysql(url):
    from schema_migrations import HEAD

    engine = engine_for(url)
    with engine.connect() as connection:
        owner = connection.exec_driver_sql(
            "SELECT GET_LOCK('radio_business_schema_migration',60)"
        ).scalar()
        if owner != 1:
            raise TimeoutError("MySQL schema migration is already owned")
        try:
            if not sa.inspect(connection).has_table("alembic_version"):
                raise RuntimeError("Initialize or import a new MySQL database before upgrading")
            current = connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            ).scalar()
            if current == HEAD:
                return {"revision": HEAD, "changed": False}
            if current != "radio_007" or HEAD != "radio_008":
                raise RuntimeError("No explicit MySQL upgrade path exists for this revision")
            migration = importlib.import_module("migrations.versions.radio_008_reaction_bitmap")
            with Operations.context(MigrationContext.configure(connection)):
                migration.upgrade()
            table = sa.inspect(connection)
            fields = {c["name"] for c in table.get_columns("reaction_bitmap_users")}
            primary = table.get_pk_constraint("reaction_bitmap_users")["constrained_columns"]
            indexes = table.get_indexes("reaction_bitmap_users")
            if (
                fields != {"bitmap_id", "user_id"}
                or primary != ["bitmap_id"]
                or not any(i["column_names"] == ["user_id"] and i["unique"] for i in indexes)
            ):
                raise RuntimeError("Bitmap mapping table failed schema validation")
            connection.execute(
                sa.text(
                    "UPDATE alembic_version SET version_num=:revision WHERE version_num=:previous"
                ),
                {"revision": HEAD, "previous": current},
            )
            connection.commit()
            tables_for.cache_clear()
            return {"revision": HEAD, "changed": True}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.exec_driver_sql("SELECT RELEASE_LOCK('radio_business_schema_migration')")


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
