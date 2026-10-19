"""Explicit, resumable MySQL upgrades; never stamp an arbitrary existing schema."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from .schema import canonical_schema, metadata_from_catalog


def upgrade_mysql(engine, target):
    with engine.connect() as connection:
        if (
            connection.exec_driver_sql(
                "SELECT GET_LOCK('radio_business_schema_migration',60)"
            ).scalar()
            != 1
        ):
            raise TimeoutError("MySQL schema migration ownership unavailable")
        try:
            current = connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            if current == target:
                return
            if current == "radio_007" and target in {"radio_008", "radio_009"}:
                path = (
                    Path(__file__).resolve().parents[1]
                    / "migrations/versions/radio_008_reaction_bitmap.py"
                )
                spec = importlib.util.spec_from_file_location("mysql_revision_008", path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                with Operations.context(MigrationContext.configure(connection)):
                    module.upgrade()
                connection.execute(sa.text("UPDATE alembic_version SET version_num='radio_008'"))
                connection.commit()
                current = "radio_008"
            if current == "radio_008" and target == "radio_009":
                _upgrade_009(connection)
                connection.execute(sa.text("UPDATE alembic_version SET version_num='radio_009'"))
                connection.commit()
                current = "radio_009"
            if current != target:
                raise RuntimeError(f"No explicit MySQL migration path from {current} to {target}")
        finally:
            connection.exec_driver_sql("SELECT RELEASE_LOCK('radio_business_schema_migration')")


def _upgrade_009(connection):
    from domain_event_schema import JOB_PUBLISH_COLUMNS

    metadata = metadata_from_catalog(canonical_schema())
    operations = Operations(MigrationContext.configure(connection))
    inspector = sa.inspect(connection)
    feed = {column["name"]: column for column in inspector.get_columns("feed_content")}
    if not feed["track_id"]["nullable"]:
        operations.alter_column(
            "feed_content", "track_id", existing_type=feed["track_id"]["type"], nullable=True
        )
    for name in ("resource_kind", "permission_version"):
        if name not in feed:
            operations.add_column("feed_content", metadata.tables["feed_content"].c[name]._copy())
    jobs = {column["name"] for column in inspector.get_columns("durable_jobs")}
    for name, _definition in JOB_PUBLISH_COLUMNS:
        if name not in jobs:
            operations.add_column("durable_jobs", metadata.tables["durable_jobs"].c[name]._copy())
    created = {
        "domain_event_sequences",
        "domain_event_outbox",
        "domain_event_inbox",
        "domain_consumer_heads",
        "job_worker_leases",
        "kafka_delivery_rejections",
        "ugc_posts",
        "ugc_commands",
    }
    for table in metadata.sorted_tables:
        if table.name in created:
            table.create(connection, checkfirst=True)
            for index in table.indexes:
                index.create(connection, checkfirst=True)
    # DDL can commit implicitly. Verify all required columns before stamping.
    inspector = sa.inspect(connection)
    for name in created | {"feed_content", "durable_jobs"}:
        expected = {column.name for column in metadata.tables[name].columns}
        actual = {column["name"] for column in inspector.get_columns(name)}
        if not expected <= actual:
            raise RuntimeError(f"MySQL radio_009 schema verification failed for {name}")


def main():
    import os

    from database import mysql_target
    from schema_migrations import HEAD

    from .connection import _initialized, engine_for, tables_for

    target = mysql_target(os.getenv("DATABASE_URL") or os.getenv("BUSINESS_DATABASE_URL"))
    if not target:
        raise SystemExit("An administrator DATABASE_URL is required for the migration command")
    engine = engine_for(target)
    if sa.inspect(engine).has_table("alembic_version"):
        upgrade_mysql(engine, HEAD)
    else:
        from .connection import ensure_mysql

        ensure_mysql(target, migrate=True)
    tables_for.cache_clear()
    _initialized.clear()
    print(f"MySQL business migration verified at {HEAD}")


if __name__ == "__main__":
    main()
