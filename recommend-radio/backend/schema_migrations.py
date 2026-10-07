import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config
from filelock import FileLock

HEAD = "radio_004"
ROOT = Path(__file__).resolve().parent


def config(path: Path) -> Config:
    value = Config()
    value.set_main_option("script_location", str(ROOT / "migrations"))
    value.attributes["db_path"] = str(path)
    return value


def current_revision(path: Path) -> str | None:
    if not path.exists():
        return None
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='alembic_version'").fetchone():
            return None
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        return row[0] if row else None


def ensure_database(path: Path, *, migrate: bool) -> None:
    if current_revision(path) == HEAD:
        return
    if not migrate:
        raise RuntimeError("Database revision is not current; run the migrate service")
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(path) + ".migration.lock", timeout=60):
        command.upgrade(config(path), "head")
    if current_revision(path) != HEAD:
        raise RuntimeError("Database migration did not reach the expected head")
