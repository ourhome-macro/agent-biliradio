from __future__ import annotations

import sqlite3
import threading
from decimal import Decimal
from functools import lru_cache

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import DataError, IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.dml import Insert
from sqlglot import exp

from .compiler import (
    UnsupportedBusinessSQL,
    bind_parameters,
    expression_value,
    mysql_expression,
    parse_query,
)


@lru_cache(maxsize=8)
def engine_for(url):
    parsed = sa.engine.make_url(url)
    if parsed.drivername == "mysql":
        parsed = parsed.set(drivername="mysql+pymysql")
    return sa.create_engine(
        parsed,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=10,
        pool_timeout=3,
        pool_recycle=1800,
        isolation_level="READ COMMITTED",
        hide_parameters=True,
        connect_args={
            "connect_timeout": 5,
            "read_timeout": 35,
            "write_timeout": 35,
            "charset": "utf8mb4",
            "init_command": (
                "SET time_zone='+00:00',sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,"
                "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
            ),
        },
    )


_init_lock = threading.Lock()
_initialized = set()


def ensure_mysql(url, *, migrate):
    from schema_migrations import HEAD

    with _init_lock:
        if (url, HEAD) in _initialized:
            return
        engine = engine_for(url)
        inspector = sa.inspect(engine)
        current = None
        if inspector.has_table("alembic_version"):
            with engine.connect() as connection:
                current = connection.execute(
                    sa.text("SELECT version_num FROM alembic_version")
                ).scalar()
        if current != HEAD:
            if current is not None:
                raise RuntimeError(
                    "Existing MySQL revisions require an explicit versioned upgrade; "
                    "baseline installation cannot stamp an older database"
                )
            if not migrate:
                raise RuntimeError("MySQL business schema is not current; run mysql migration")
            from .schema import install_schema

            install_schema(engine)
            tables_for.cache_clear()
        _initialized.add((url, HEAD))


@lru_cache(maxsize=8)
def tables_for(url):
    metadata = sa.MetaData()
    metadata.reflect(engine_for(url))
    for table in metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, sa.Float):
                column.type.asdecimal = False
    return metadata.tables


class BusinessRow:
    """sqlite.Row-compatible integer and name lookup, without leaking connection state."""

    def __init__(self, values, keys):
        self._values = tuple(
            int(value)
            if isinstance(value, Decimal) and value == value.to_integral_value()
            else float(value)
            if isinstance(value, Decimal)
            else value
            for value in values
        )
        self._keys = tuple(keys)
        self._map = {}
        for key, value in zip(self._keys, self._values, strict=True):
            self._map.setdefault(key, value)

    def __getitem__(self, key):
        return self._values[key] if isinstance(key, (int, slice)) else self._map[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def keys(self):
        return self._keys


class BusinessResult:
    def __init__(self, result=None, *, rowcount=0, rows=None, keys=()):
        self.rowcount = rowcount if result is None else result.rowcount
        self.lastrowid = getattr(result, "lastrowid", None)
        self._keys = (
            tuple(result.keys()) if result is not None and result.returns_rows else tuple(keys)
        )
        self._rows = (
            list(result.fetchall())
            if result is not None and result.returns_rows
            else list(rows or ())
        )
        self._position = 0

    def fetchone(self):
        if self._position >= len(self._rows):
            return None
        row = BusinessRow(self._rows[self._position], self._keys)
        self._position += 1
        return row

    def fetchall(self):
        return list(self)

    def __iter__(self):
        while (row := self.fetchone()) is not None:
            yield row


class ReplaceInsert(Insert):
    inherit_cache = False


@compiles(ReplaceInsert, "mysql")
def compile_replace(statement, compiler, **kwargs):
    # REPLACE has real delete+insert semantics on both engines. This is an
    # explicit Core DML compiler, not an SQL-text heuristic at query call sites.
    generated = compiler.visit_insert(statement, **kwargs)
    if not generated.startswith("INSERT "):
        raise UnsupportedBusinessSQL("Unexpected Core INSERT compilation")
    return "REPLACE " + generated[len("INSERT ") :]


class MySQLConnection:
    dialect = "mysql"

    def __init__(self, url):
        self.url = url
        self.connection = engine_for(url).connect()
        self.tables = tables_for(url)

    @property
    def in_transaction(self):
        return self.connection.in_transaction()

    def __enter__(self):
        return self

    def __exit__(self, kind, error, trace):
        try:
            self.connection.rollback() if error else self.connection.commit()
        finally:
            self.connection.close()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()

    def lock_key(self, namespace, key):
        if not isinstance(namespace, str) or not namespace or len(namespace) > 64:
            raise ValueError("A bounded business lock namespace is required")
        table = self.tables["business_locks"]
        statement = mysql_insert(table).values(namespace=namespace, lock_key=key)
        self.connection.execute(statement.on_duplicate_key_update(lock_key=table.c.lock_key))
        self.connection.execute(
            sa.select(table.c.lock_key)
            .where(table.c.namespace == namespace, table.c.lock_key == key)
            .with_for_update()
        ).first()

    def lock_row(self, table, where):
        target = self.tables[table]
        result = self.connection.execute(
            sa.select(target)
            .where(*(target.c[key] == value for key, value in where.items()))
            .with_for_update()
        )
        return BusinessResult(result).fetchone()

    def execute(self, sql, parameters=()):
        try:
            return self._execute(sql, parameters)
        except IntegrityError as error:
            raise sqlite3.IntegrityError(str(error.orig)) from error
        except OperationalError as error:
            if getattr(error.orig, "args", (None,))[0] == 3819:
                raise sqlite3.IntegrityError(str(error.orig)) from error
            raise sqlite3.OperationalError(str(error.orig)) from error
        except DataError as error:
            raise sqlite3.DataError(str(error.orig)) from error
        except ProgrammingError as error:
            raise sqlite3.ProgrammingError(str(error.orig)) from error

    def executemany(self, sql, values):
        count = 0
        for parameters in values:
            count += max(0, self.execute(sql, parameters).rowcount)
        return BusinessResult(rowcount=count)

    def _execute(self, sql, parameters):
        if not isinstance(sql, str):
            return BusinessResult(self.connection.execute(sql, parameters or {}))
        tree, count = parse_query(sql)
        bound = bind_parameters(count, parameters)
        if isinstance(tree, exp.Transaction):
            if not self.in_transaction:
                self.connection.begin()
            return BusinessResult()
        if isinstance(tree, exp.Commit):
            self.commit()
            return BusinessResult()
        if isinstance(tree, exp.Rollback):
            self.rollback()
            return BusinessResult()
        if isinstance(tree, exp.Pragma):
            raise UnsupportedBusinessSQL("SQLite PRAGMA is not a MySQL business operation")
        if isinstance(tree, exp.Create):
            name = tree.this.name
            if isinstance(tree.this, exp.Schema):
                name = tree.this.this.name
            if tree.args.get("exists") and name in self.tables:
                return BusinessResult()
            raise UnsupportedBusinessSQL(
                "Runtime DDL must be managed by business schema migrations"
            )
        if isinstance(tree, exp.Insert):
            return self._insert(tree, bound)
        return BusinessResult(self.connection.execute(sa.text(mysql_expression(tree)), bound))

    def _insert(self, tree, bound):
        target = tree.this
        names = (
            [column.name for column in target.expressions]
            if isinstance(target, exp.Schema)
            else None
        )
        table_name = target.this.name if isinstance(target, exp.Schema) else target.name
        table = self.tables[table_name]
        names = names or [column.name for column in table.columns if column.computed is None]
        source = tree.expression
        values = None
        if isinstance(source, exp.Values):
            if len(source.expressions) != 1:
                raise UnsupportedBusinessSQL("Use executemany for multirow portable INSERT")
            nodes = source.expressions[0].expressions
            if len(nodes) != len(names):
                raise ValueError("INSERT column/value counts differ")
            values = dict(zip(names, [expression_value(node) for node in nodes], strict=True))
        constructor = ReplaceInsert if tree.args.get("alternative") == "REPLACE" else sa.insert
        statement = constructor(table)
        if values is not None:
            statement = statement.values(**values)
        elif isinstance(source, (exp.Select, exp.Union)):
            statement = statement.from_select(
                names,
                sa.text(mysql_expression(source)).columns(*(sa.column(name) for name in names)),
            )
        else:
            raise UnsupportedBusinessSQL("Unsupported INSERT source")
        conflict = tree.args.get("conflict")
        ignoring = tree.args.get("alternative") == "IGNORE" or bool(
            conflict and str(conflict.args.get("action")).upper() == "DO NOTHING"
        )
        if not conflict and not ignoring:
            result = BusinessResult(self.connection.execute(statement, bound))
            if constructor is ReplaceInsert and result.rowcount == 2:
                result.rowcount = 1
            return result
        # IGNORE suppresses only a duplicate-key error, never truncation, invalid
        # values, FK violations or CHECK failures as MySQL INSERT IGNORE would.
        savepoint = self.connection.begin_nested()
        try:
            result = self.connection.execute(statement, bound)
            savepoint.commit()
            return BusinessResult(result)
        except IntegrityError as error:
            savepoint.rollback()
            if getattr(error.orig, "args", (None,))[0] != 1062:
                raise
            if ignoring:
                return BusinessResult(rowcount=0)
            if values is None:
                raise UnsupportedBusinessSQL(
                    "INSERT SELECT conflict updates need an explicit repository upsert"
                ) from error
            keys = [key.name for key in conflict.args.get("conflict_keys", [])]
            if not keys:
                raise UnsupportedBusinessSQL("An explicit conflict target is required") from error
            predicate = sa.and_(*(table.c[key] == values[key] for key in keys))
            old = self.connection.execute(
                sa.select(table).where(predicate).with_for_update(), bound
            ).first()
            if old is None:
                raise  # Conflict hit a different UNIQUE constraint; SQLite would reject it.
            assignments = {}
            for item in conflict.expressions:
                if not isinstance(item, exp.EQ):
                    raise UnsupportedBusinessSQL("Unsupported ON CONFLICT assignment") from error
                expression = item.expression.copy()
                # Core UPDATE binds incoming values directly instead of relying
                # on MySQL VALUES() outside its permitted ON DUPLICATE clause.
                if isinstance(expression, exp.Column) and expression.table.lower() == "excluded":
                    assignments[item.this.name] = values[expression.name]
                else:
                    for incoming in list(expression.find_all(exp.Column)):
                        if incoming.table.lower() == "excluded":
                            original = source.expressions[0].expressions[names.index(incoming.name)]
                            incoming.replace(original.copy())
                    assignments[item.this.name] = expression_value(expression)
            update = sa.update(table).where(predicate).values(**assignments)
            if conflict.args.get("where"):
                condition = conflict.args["where"].this.copy()
                for incoming in list(condition.find_all(exp.Column)):
                    if incoming.table.lower() == "excluded":
                        incoming.replace(
                            source.expressions[0].expressions[names.index(incoming.name)].copy()
                        )
                update = update.where(sa.text(mysql_expression(condition)))
            return BusinessResult(self.connection.execute(update, bound))
