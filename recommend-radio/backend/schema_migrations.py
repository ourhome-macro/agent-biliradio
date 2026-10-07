import sqlite3
from contextlib import closing
from pathlib import Path

from alembic import command
from alembic.config import Config
from filelock import FileLock

HEAD = "radio_007"
ROOT = Path(__file__).resolve().parent


def config(path: Path) -> Config:
    value = Config()
    value.set_main_option("script_location", str(ROOT / "migrations"))
    value.attributes["db_path"] = str(path)
    return value


def current_revision(path: Path) -> str | None:
    from database import mysql_target

    target = mysql_target(path)
    if target:
        import sqlalchemy as sa
        from mysql_storage.connection import engine_for

        engine = engine_for(target)
        if not sa.inspect(engine).has_table("alembic_version"):
            return None
        with engine.connect() as connection:
            return connection.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
    path = Path(path)
    if not path.exists():
        return None
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='alembic_version'").fetchone():
            return None
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        return row[0] if row else None


def ensure_database(path: Path, *, migrate: bool) -> None:
    from database import mysql_target

    target = mysql_target(path)
    if target:
        from mysql_storage.connection import ensure_mysql

        ensure_mysql(target, migrate=migrate)
        return
    path = Path(path)
    if current_revision(path) == HEAD:
        return
    if not migrate:
        raise RuntimeError("Database revision is not current; run the migrate service")
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(path) + ".migration.lock", timeout=60):
        command.upgrade(config(path), "head")
    if current_revision(path) != HEAD:
        raise RuntimeError("Database migration did not reach the expected head")
