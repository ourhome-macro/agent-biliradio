from __future__ import annotations

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import sqlalchemy as sa
import sqlglot
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlglot import exp

KEY_LENGTHS = {
    ("conversation_warm_memories", "scope_type"): 32,
    ("conversation_warm_memories", "memory_type"): 64,
    ("content_embedding_jobs", "modality"): 16,
    ("content_embedding_jobs", "model_version"): 64,
    ("content_embedding_jobs", "content_hash"): 64,
}


def canonical_schema():
    """Build the canonical business schema in an isolated SQLite migration fixture."""
    from schema_migrations import ensure_database

    with tempfile.TemporaryDirectory(prefix="radio-schema-") as directory:
        path = Path(directory) / "schema.sqlite3"
        ensure_database(path, migrate=True)
        with closing(sqlite3.connect(path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            result = {}
            for row in rows:
                name = row["name"]
                columns = [dict(c) for c in connection.execute(f'PRAGMA table_info("{name}")')]
                indexes = []
                for index in connection.execute(f'PRAGMA index_list("{name}")'):
                    fields = [
                        i[2] for i in connection.execute(f'PRAGMA index_info("{index["name"]}")')
                    ]
                    indexes.append({**dict(index), "fields": fields})
                result[name] = {
                    "columns": columns,
                    "indexes": indexes,
                    "sql": row["sql"],
                    "foreign_keys": [
                        dict(f) for f in connection.execute(f'PRAGMA foreign_key_list("{name}")')
                    ],
                    "rows": [dict(r) for r in connection.execute(f'SELECT * FROM "{name}"')],
                }
            return result


def metadata_from_catalog(catalog):
    metadata = sa.MetaData()
    foreign_targets = {
        (f["table"], f["to"]) for info in catalog.values() for f in info["foreign_keys"]
    }
    for name, info in catalog.items():
        indexed = {field for index in info["indexes"] for field in index["fields"] if field}
        indexed |= {column["name"] for column in info["columns"] if column["pk"]}
        indexed |= {f["from"] for f in info["foreign_keys"]}
        indexed |= {field for table, field in foreign_targets if table == name}
        columns = []
        primary = [c for c in info["columns"] if c["pk"]]
        for column in info["columns"]:
            field, kind = column["name"], column["type"].upper()
            if "INT" in kind:
                datatype = sa.BigInteger()
            elif any(part in kind for part in ("REAL", "FLOAT", "DOUBLE")):
                datatype = sa.Double()
            elif "BLOB" in kind:
                datatype = sa.LargeBinary(length=16777215)
            else:
                datatype = (
                    sa.String(KEY_LENGTHS.get((name, field), 191), collation="utf8mb4_0900_bin")
                    if field in indexed
                    else LONGTEXT()
                )
            default = column["dflt_value"]
            options = {
                "primary_key": bool(column["pk"]),
                "nullable": not column["notnull"] and not column["pk"],
            }
            if default is not None:
                options["server_default"] = sa.text(f"({default})")
            if column["pk"] and len(primary) == 1 and "INT" in kind:
                options["autoincrement"] = True
            columns.append(sa.Column(field, datatype, **options))
        table = sa.Table(
            name,
            metadata,
            *columns,
            mysql_engine="InnoDB",
            mysql_charset="utf8mb4",
            mysql_collate="utf8mb4_0900_bin",
        )
        if name == "media_import_jobs":
            table.append_column(
                sa.Column(
                    "active_asset_key",
                    sa.String(191, collation="utf8mb4_0900_bin"),
                    sa.Computed(
                        "CASE WHEN status IN ('queued','running') THEN asset_id ELSE NULL END",
                        persisted=True,
                    ),
                )
            )
            sa.Index("idx_media_active_import", table.c.active_asset_key, unique=True)
        for index in info["indexes"]:
            if index["origin"] == "pk":
                continue
            if index["partial"]:
                if name == "media_import_jobs" and index["name"] == "idx_media_active_import":
                    continue
                raise ValueError(
                    f"MySQL schema needs an explicit partial-index mapping: {index['name']}"
                )
            if not all(index["fields"]):
                raise ValueError(f"Expression index needs explicit mapping: {index['name']}")
            label = index["name"]
            if label.startswith("sqlite_autoindex_"):
                label = "uq_" + name + "_" + str(index["seq"])
            fields = [table.c[field] for field in index["fields"]]
            strings = [field for field in fields if isinstance(field.type, sa.String)]
            options = {}
            if sum(field.type.length * 4 for field in strings) > 3072:
                if index["unique"]:
                    raise ValueError(
                        f"Explicit bounded type mapping required for UNIQUE index: {label}"
                    )
                options["mysql_length"] = {
                    field.name: min(field.type.length, 3000 // len(strings) // 4)
                    for field in strings
                }
            sa.Index(label[:64], *fields, unique=bool(index["unique"]), **options)
        from .compiler import mysql_expression

        definition = sqlglot.parse_one(info["sql"], read="sqlite")
        for number, check in enumerate(definition.find_all(exp.CheckColumnConstraint)):
            table.append_constraint(
                sa.CheckConstraint(mysql_expression(check.this), name=f"ck_{name}_{number}"[:64])
            )
    for name, info in catalog.items():
        groups = {}
        for foreign in info["foreign_keys"]:
            groups.setdefault(foreign["id"], []).append(foreign)
        for number, group in groups.items():
            group.sort(key=lambda f: f["seq"])
            item = group[0]
            metadata.tables[name].append_constraint(
                sa.ForeignKeyConstraint(
                    [f["from"] for f in group],
                    [f"{f['table']}.{f['to']}" for f in group],
                    name=f"fk_{name}_{number}"[:64],
                    ondelete=item["on_delete"],
                    onupdate=item["on_update"],
                )
            )
    sa.Table(
        "business_locks",
        metadata,
        sa.Column("namespace", sa.String(64, collation="utf8mb4_0900_bin"), primary_key=True),
        sa.Column("lock_key", sa.String(64, collation="utf8mb4_0900_bin"), primary_key=True),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_0900_bin",
    )
    return metadata


def install_schema(engine):
    catalog = canonical_schema()
    metadata = metadata_from_catalog(catalog)
    # MySQL DDL commits implicitly: serialize only this operator-driven migration.
    with engine.connect() as connection:
        acquired = connection.exec_driver_sql(
            "SELECT GET_LOCK('radio_business_schema_migration',60)"
        ).scalar()
        if acquired != 1:
            raise TimeoutError("Could not acquire schema migration ownership")
        try:
            metadata.create_all(connection)
            from alembic.migration import MigrationContext
            from alembic.operations import Operations

            operations = Operations(MigrationContext.configure(connection))
            inspector = sa.inspect(connection)
            for table in metadata.sorted_tables:
                existing = {column["name"] for column in inspector.get_columns(table.name)}
                for column in table.columns:
                    if column.name not in existing:
                        operations.add_column(table.name, column._copy())
                for index in table.indexes:
                    index.create(connection, checkfirst=True)
            from sqlalchemy.dialects.mysql import insert

            for table in metadata.sorted_tables:
                name = table.name
                if name not in catalog or name == "alembic_version":
                    continue
                info = catalog[name]
                for row in info["rows"]:
                    statement = insert(metadata.tables[name]).values(**row)
                    first = next(iter(metadata.tables[name].primary_key.columns), None)
                    if first is not None:
                        statement = statement.on_duplicate_key_update({first.name: first})
                    connection.execute(statement)
            connection.execute(sa.delete(metadata.tables["alembic_version"]))
            connection.execute(
                sa.insert(metadata.tables["alembic_version"]), catalog["alembic_version"]["rows"]
            )
            connection.commit()
        finally:
            connection.exec_driver_sql("SELECT RELEASE_LOCK('radio_business_schema_migration')")
    return metadata
