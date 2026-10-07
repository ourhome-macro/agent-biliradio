"""Offline, verified business snapshots; no implicit production cutover."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import sqlalchemy as sa

from .connection import engine_for, ensure_mysql, tables_for
from .schema import canonical_schema


def _validate_table_set(url, catalog, *, allow_missing=False):
    inspector = sa.inspect(engine_for(url))
    actual = set(inspector.get_table_names()) | set(inspector.get_view_names())
    expected = set(catalog) | {"business_locks"}
    extra = actual - expected
    missing = expected - actual
    if extra or (missing and not allow_missing):
        raise RuntimeError(
            "MySQL schema has unmapped or missing business tables; explicit mapping required: "
            + ",".join(sorted(extra or missing))
        )


def _row_hash(rows):
    def value(item):
        return (
            {"bytes": bytes(item).hex()}
            if isinstance(item, (bytes, bytearray, memoryview))
            else item
        )

    serialized = [
        json.dumps(
            {key: value(item) for key, item in row.items()},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for row in rows
    ]
    return hashlib.sha256("\n".join(sorted(serialized)).encode()).hexdigest()


def snapshot_sqlite(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if destination.exists():
        raise FileExistsError("Snapshot destination must be new")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as original:
        with closing(sqlite3.connect(destination)) as copied:
            original.backup(copied, pages=128, sleep=0.01)
    from schema_migrations import ensure_database

    ensure_database(destination, migrate=True)  # Upgrade only the offline copy.
    return destination


def export_snapshot(url, destination):
    """Export one real MySQL MVCC snapshot into the portable SQLite backup format."""
    from schema_migrations import HEAD, ensure_database

    destination = Path(destination).resolve()
    if destination.exists():
        raise FileExistsError("Snapshot destination must be new")
    destination.parent.mkdir(parents=True, exist_ok=True)
    ensure_mysql(url, migrate=False)
    _validate_table_set(url, canonical_schema())
    ensure_database(destination, migrate=True)
    tables = tables_for(url)
    report = {
        "backend": "mysql",
        "format": "sqlite-business-snapshot",
        "revision": HEAD,
        "tables": {},
        "consistentSnapshot": True,
    }
    with engine_for(url).connect().execution_options(isolation_level="REPEATABLE READ") as source:
        source.exec_driver_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        with closing(sqlite3.connect(destination)) as target:
            target.execute("PRAGMA foreign_keys=OFF")
            target.execute("BEGIN IMMEDIATE")
            target_tables = {
                row[0]
                for row in target.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            for name in target_tables:
                target.execute(f'DELETE FROM "{name}"')
            for name in sorted(target_tables):
                table = tables[name]
                fields = [column.name for column in table.columns if column.computed is None]
                rows = [
                    dict(row)
                    for row in source.execute(
                        sa.select(*(table.c[field] for field in fields))
                    ).mappings()
                ]
                if rows:
                    target.executemany(
                        f'INSERT INTO "{name}" ({",".join(fields)}) '
                        f"VALUES ({','.join('?' for _ in fields)})",
                        [[row[field] for field in fields] for row in rows],
                    )
                report["tables"][name] = {"count": len(rows), "sha256": _row_hash(rows)}
            violations = target.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError(
                    f"MySQL snapshot contains {len(violations)} foreign-key violations"
                )
            target.commit()
        source.rollback()
    return report


def import_snapshot(source, url):
    """Copy a consistent SQLite snapshot into an empty MySQL business database.

    The supplied SQLite file is read-only and must already be at the canonical
    revision. Existing non-bootstrap data at the destination is a hard refusal.
    """
    from schema_migrations import HEAD

    catalog = canonical_schema()
    _validate_table_set(url, catalog, allow_missing=True)
    ensure_mysql(url, migrate=True)
    _validate_table_set(url, catalog)
    metadata_tables = tables_for(url)
    report = {"backend": "mysql", "revision": HEAD, "tables": {}, "preflight": []}
    with closing(
        sqlite3.connect(Path(source).resolve().as_uri() + "?mode=ro", uri=True)
    ) as original:
        original.row_factory = sqlite3.Row
        if original.execute("SELECT version_num FROM alembic_version").fetchone()[0] != HEAD:
            raise RuntimeError("Import source must be an upgraded consistent snapshot")
        if original.execute("PRAGMA foreign_key_check").fetchone():
            raise RuntimeError("Import source has foreign-key violations")
        source_tables = {
            row[0]
            for row in original.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if source_tables != set(catalog):
            raise RuntimeError(
                "Source table set differs from canonical business schema; explicit mapping required"
            )
        data = {}
        for name in sorted(source_tables):
            rows = [dict(row) for row in original.execute(f'SELECT * FROM "{name}"')]
            table = metadata_tables[name]
            for column in table.columns:
                if column.computed is not None:
                    continue
                limit = getattr(column.type, "length", None)
                if isinstance(column.type, sa.String) and limit:
                    observed = max(
                        (
                            len(str(row[column.name]))
                            for row in rows
                            if row[column.name] is not None
                        ),
                        default=0,
                    )
                    report["preflight"].append(
                        {
                            "table": name,
                            "column": column.name,
                            "limit": limit,
                            "observedMax": observed,
                        }
                    )
                    if observed > limit:
                        raise ValueError(
                            f"Explicit MySQL type widening required: {name}.{column.name} "
                            f"({observed}>{limit}); no rows copied"
                        )
            data[name] = rows
    with engine_for(url).begin() as target:
        # The destination can only contain canonical bootstrap records, never
        # live user content. Compare all fields except bootstrap timestamps.
        for name in data:
            existing = [
                dict(row) for row in target.execute(sa.select(metadata_tables[name])).mappings()
            ]
            if not existing or name == "alembic_version":
                continue

            def stable(row, table=metadata_tables[name]):
                return {
                    key: item
                    for key, item in row.items()
                    if key not in {"created_at", "updated_at"} and table.c[key].computed is None
                }

            if _row_hash([stable(row) for row in existing]) != _row_hash(
                [stable(row) for row in catalog[name]["rows"]]
            ):
                raise RuntimeError(
                    f"Destination is not empty/bootstrap-only: {name}; refusing overwrite"
                )
        ordered = [
            table
            for table in next(iter(metadata_tables.values())).metadata.sorted_tables
            if table.name in data
        ]
        for table in reversed(ordered):
            target.execute(sa.delete(table))
        for table in ordered:
            rows = data[table.name]
            for start in range(0, len(rows), 500):
                target.execute(sa.insert(table), rows[start : start + 500])
        for table in ordered:
            fields = [column for column in table.columns if column.computed is None]
            copied = [dict(row) for row in target.execute(sa.select(*fields)).mappings()]
            digest = _row_hash(copied)
            if digest != _row_hash(data[table.name]):
                raise RuntimeError(
                    f"Checksum verification failed: {table.name}; transaction rolled back"
                )
            report["tables"][table.name] = {"count": len(copied), "sha256": digest}
    report["verified"] = True
    return report


def main():
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Verified SQLite→empty MySQL migration rehearsal")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--apply", action="store_true", help="Copy into the configured empty target"
    )
    args = parser.parse_args()
    url = os.getenv("DATABASE_URL") or os.getenv("BUSINESS_DATABASE_URL")
    if not args.apply or not url:
        parser.error(
            "--apply and DATABASE_URL are required; source is only read via a consistent backup"
        )
    with tempfile.TemporaryDirectory(prefix="radio-business-migration-") as directory:
        snapshot = snapshot_sqlite(args.source, Path(directory) / "source.sqlite3")
        report = import_snapshot(snapshot, url)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Verified business migration report:", args.report)


if __name__ == "__main__":
    main()
