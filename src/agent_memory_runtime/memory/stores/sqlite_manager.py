from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import sqlite_vec

from agent_memory_runtime.exceptions import StoreError


@dataclass(frozen=True)
class SQLiteBackupReport:
    path: Path
    integrity_check: str
    page_count: int
    schema_version: int


@dataclass(frozen=True)
class _Migration:
    version: int
    name: str
    statements: tuple[str, ...]

    @property
    def checksum(self) -> str:
        payload = "\n".join((str(self.version), self.name, *self.statements))
        return sha256(payload.encode("utf-8")).hexdigest()


_MIGRATIONS = (
    _Migration(
        version=1,
        name="core_event_memory_snapshot",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY,
                event_id TEXT UNIQUE NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS memories (
                memory_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS llm_call_traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT UNIQUE NOT NULL,
                payload TEXT NOT NULL
            )
            """,
        ),
    ),
    _Migration(
        version=2,
        name="audit_queue_and_governance",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS audit_envelopes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT UNIQUE NOT NULL,
                audit_type TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS derivation_jobs (
                job_id TEXT PRIMARY KEY,
                event_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_derivation_jobs_status
            ON derivation_jobs(status)
            """,
            """
            CREATE TABLE IF NOT EXISTS review_items (
                review_id TEXT PRIMARY KEY,
                dedupe_key TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_review_items_status
            ON review_items(status)
            """,
            """
            CREATE TABLE IF NOT EXISTS vault_records (
                token_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_vault_records_owner
            ON vault_records(tenant_id, owner_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_tombstones (
                memory_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
        ),
    ),
    _Migration(
        version=3,
        name="business_agent_runs",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
                run_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(tenant_id, request_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_runs_tenant_status
            ON agent_runs(tenant_id, status)
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_turns (
                turn_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(run_id, sequence),
                FOREIGN KEY(run_id) REFERENCES agent_runs(run_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_checkpoints (
                run_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES agent_runs(run_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_tool_calls (
                call_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES agent_runs(run_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_tool_calls_run_status
            ON agent_tool_calls(run_id, status)
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_approvals (
                approval_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                call_id TEXT UNIQUE NOT NULL,
                tenant_id TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES agent_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(call_id) REFERENCES agent_tool_calls(call_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_approvals_tenant_status
            ON agent_approvals(tenant_id, status)
            """,
        ),
    ),
    _Migration(
        version=4,
        name="controlled_agent_orchestrations",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS agent_orchestrations (
                orchestration_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(tenant_id, request_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_orchestrations_tenant_status
            ON agent_orchestrations(tenant_id, status)
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_delegations (
                orchestration_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(orchestration_id, task_id),
                FOREIGN KEY(orchestration_id)
                    REFERENCES agent_orchestrations(orchestration_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_delegations_run_status
            ON agent_delegations(orchestration_id, status)
            """,
        ),
    ),
    _Migration(
        version=5,
        name="indexed_memory_projection",
        statements=(
            "ALTER TABLE memories ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE memories ADD COLUMN user_id TEXT",
            "ALTER TABLE memories ADD COLUMN agent_id TEXT",
            "ALTER TABLE memories ADD COLUMN session_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE memories ADD COLUMN layer TEXT NOT NULL DEFAULT 'working'",
            "ALTER TABLE memories ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
            "ALTER TABLE memories ADD COLUMN memory_type TEXT NOT NULL DEFAULT 'episodic'",
            "ALTER TABLE memories ADD COLUMN scope TEXT NOT NULL DEFAULT 'private'",
            "ALTER TABLE memories ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE memories ADD COLUMN salience REAL NOT NULL DEFAULT 0.5",
            "ALTER TABLE memories ADD COLUMN search_indexed INTEGER NOT NULL DEFAULT 0",
            """
            UPDATE memories
            SET tenant_id = COALESCE(json_extract(payload, '$.tenant_id'), 'default'),
                user_id = json_extract(payload, '$.user_id'),
                agent_id = COALESCE(
                    json_extract(payload, '$.agent_id'),
                    json_extract(payload, '$.owner_id')
                ),
                session_id = COALESCE(json_extract(payload, '$.session_id'), 'default'),
                layer = COALESCE(json_extract(payload, '$.layer'), 'working'),
                status = COALESCE(json_extract(payload, '$.status'), 'active'),
                memory_type = COALESCE(json_extract(payload, '$.memory_type'), 'episodic'),
                scope = COALESCE(json_extract(payload, '$.scope'), 'private'),
                updated_at = COALESCE(json_extract(payload, '$.updated_at'), ''),
                salience = COALESCE(json_extract(payload, '$.salience'), 0.5)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_identity_layer_status
            ON memories(tenant_id, user_id, layer, status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_session_layer_status
            ON memories(tenant_id, session_id, layer, status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_type_scope
            ON memories(tenant_id, memory_type, scope)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_recency_salience
            ON memories(tenant_id, updated_at, salience)
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_terms (
                memory_id TEXT NOT NULL,
                term TEXT NOT NULL,
                PRIMARY KEY(memory_id, term),
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memory_terms_term
            ON memory_terms(term, memory_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_tags (
                memory_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY(memory_id, tag),
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memory_tags_tag
            ON memory_tags(tag, memory_id)
            """,
        ),
    ),
    _Migration(
        version=6,
        name="fts5_and_semantic_memory_projection",
        statements=(
            "ALTER TABLE memories ADD COLUMN retrieval_v6_indexed INTEGER NOT NULL DEFAULT 0",
            "DROP TABLE IF EXISTS memory_terms",
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
                memory_id UNINDEXED,
                terms,
                tokenize='unicode61 remove_diacritics 2'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_acl (
                memory_id TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                PRIMARY KEY(memory_id, principal_id),
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memory_acl_principal
            ON memory_acl(principal_id, memory_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS embedding_generations (
                generation TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_revision TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                distance_metric TEXT NOT NULL,
                status TEXT NOT NULL,
                spec_payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                activated_at TEXT
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_generations_active
            ON embedding_generations(status) WHERE status = 'active'
            """,
            """
            CREATE TABLE IF NOT EXISTS embedding_jobs (
                job_id TEXT PRIMARY KEY,
                memory_id TEXT NOT NULL,
                generation TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                source_sequence INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                available_at TEXT NOT NULL,
                lease_owner TEXT,
                lease_token TEXT,
                lease_expires_at TEXT,
                error_type TEXT,
                error_hash TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(memory_id, generation, content_hash),
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id) ON DELETE CASCADE,
                FOREIGN KEY(generation)
                    REFERENCES embedding_generations(generation) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_embedding_jobs_ready
            ON embedding_jobs(status, available_at, lease_expires_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_embedding_jobs_memory_generation
            ON embedding_jobs(memory_id, generation, status)
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_embeddings (
                vector_id INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_id TEXT NOT NULL,
                generation TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_revision TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                source_sequence INTEGER NOT NULL,
                embedding BLOB NOT NULL,
                status TEXT NOT NULL,
                embedded_at TEXT NOT NULL,
                UNIQUE(memory_id, generation),
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id) ON DELETE CASCADE,
                FOREIGN KEY(generation)
                    REFERENCES embedding_generations(generation) ON DELETE CASCADE,
                CHECK(typeof(embedding) = 'blob'),
                CHECK(length(embedding) = dimensions * 4)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memory_embeddings_generation_status
            ON memory_embeddings(generation, status, memory_id)
            """,
        ),
    ),
    _Migration(
        version=7,
        name="auto_dream_jobs_and_policy_reviews",
        statements=(
            "DROP TABLE IF EXISTS derivation_jobs",
            "DROP TABLE IF EXISTS review_items",
            """
            CREATE TABLE IF NOT EXISTS dream_jobs (
                job_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                user_id TEXT,
                agent_id TEXT,
                session_id TEXT,
                status TEXT NOT NULL,
                available_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_dream_jobs_status
            ON dream_jobs(status, available_at)
            """,
            """
            CREATE TABLE IF NOT EXISTS dream_checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                user_id TEXT,
                agent_id TEXT,
                session_id TEXT,
                updated_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS memory_proposal_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT UNIQUE NOT NULL,
                proposal_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT,
                agent_id TEXT,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """,
        ),
    ),
    _Migration(
        version=8,
        name="memory_level_visibility_priority",
        statements=(
            "ALTER TABLE memories ADD COLUMN level TEXT NOT NULL DEFAULT 'L1'",
            "ALTER TABLE memories ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private'",
            "ALTER TABLE memories ADD COLUMN priority REAL NOT NULL DEFAULT 0.5",
            """
            UPDATE memories
            SET level = COALESCE(
                    json_extract(payload, '$.level'),
                    CASE json_extract(payload, '$.layer')
                        WHEN 'core' THEN 'L3'
                        ELSE 'L1'
                    END
                ),
                visibility = COALESCE(
                    json_extract(payload, '$.visibility'),
                    CASE json_extract(payload, '$.scope')
                        WHEN 'global' THEN 'public'
                        WHEN 'shared' THEN 'shared'
                        ELSE 'private'
                    END
                ),
                priority = COALESCE(
                    json_extract(payload, '$.priority'),
                    json_extract(payload, '$.salience'),
                    0.5
                )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_level_status
            ON memories(tenant_id, level, status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_visibility
            ON memories(tenant_id, visibility)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_priority_recency
            ON memories(tenant_id, priority, updated_at)
            """,
        ),
    ),
    _Migration(
        version=9,
        name="drop_memory_layer_scope_projection",
        statements=(
            "DROP INDEX IF EXISTS idx_memories_identity_layer_status",
            "DROP INDEX IF EXISTS idx_memories_session_layer_status",
            "DROP INDEX IF EXISTS idx_memories_type_scope",
            "ALTER TABLE memories DROP COLUMN layer",
            "ALTER TABLE memories DROP COLUMN scope",
            """
            CREATE INDEX IF NOT EXISTS idx_memories_identity_status_level
            ON memories(tenant_id, user_id, status, level)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_session_status_level
            ON memories(tenant_id, session_id, status, level)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_type_visibility
            ON memories(tenant_id, memory_type, visibility)
            """,
        ),
    ),
    _Migration(
        version=10,
        name="memory_temperature_projection",
        statements=(
            "ALTER TABLE memories ADD COLUMN temperature TEXT NOT NULL DEFAULT 'warm'",
            """
            UPDATE memories
            SET temperature = CASE
                WHEN COALESCE(json_extract(payload, '$.status'), status)
                     IN ('archived', 'superseded', 'deleted')
                    THEN 'cold'
                WHEN json_extract(payload, '$.temperature') IN ('hot', 'warm', 'cold')
                    THEN json_extract(payload, '$.temperature')
                WHEN COALESCE(json_extract(payload, '$.level'), level) = 'L0'
                    THEN 'hot'
                ELSE 'warm'
            END
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_temperature_status
            ON memories(tenant_id, temperature, status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_memories_temperature_updated
            ON memories(tenant_id, temperature, updated_at)
            """,
        ),
    ),
)

LATEST_SCHEMA_VERSION = _MIGRATIONS[-1].version


class SQLiteTransactionManager:
    """SQLite connection policy, migrations, transactions, and online backup."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: float = 5.0,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = max(0.1, timeout_seconds)
        self.busy_timeout_ms = max(1, busy_timeout_ms)
        self._local = threading.local()
        self.migrate()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self._active_connection() is not None:
            try:
                yield
            except BaseException:
                self._local.rollback_only = True
                raise
            return

        connection = self._connect()
        self._local.connection = connection
        self._local.rollback_only = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield
            if self._local.rollback_only:
                raise StoreError("nested SQLite operation marked the transaction rollback-only")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            self._local.connection = None
            self._local.rollback_only = False
            connection.close()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        active = self._active_connection()
        if active is not None:
            yield active
            return
        with self.transaction():
            connection = self._active_connection()
            if connection is None:
                raise StoreError("SQLite transaction did not expose an active connection")
            yield connection

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        active = self._active_connection()
        if active is not None:
            yield active
            return
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.timeout_seconds,
            isolation_level=None,
        )
        _load_sqlite_vec(connection)
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _active_connection(self) -> sqlite3.Connection | None:
        return getattr(self._local, "connection", None)

    @property
    def schema_version(self) -> int:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).fetchone()
        return int(row[0])

    def migrate(self) -> int:
        from alembic import command
        from alembic.config import Config
        from filelock import FileLock

        with FileLock(str(self.path) + ".migration.lock", timeout=60):
            with closing(self._connect()) as check:
                exists = check.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='amem_alembic_version'"
                ).fetchone()
                if exists:
                    row = check.execute("SELECT version_num FROM amem_alembic_version").fetchone()
                    if row and row[0] == "amem_001":
                        return LATEST_SCHEMA_VERSION
            config = Config()
            config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
            config.attributes["manager"] = self
            command.upgrade(config, "head")
        return LATEST_SCHEMA_VERSION

    def _migrate_legacy(self) -> int:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for migration in _MIGRATIONS:
                self._apply_migration(connection, migration)
        finally:
            connection.close()
        return LATEST_SCHEMA_VERSION

    def _apply_migration(
        self,
        connection: sqlite3.Connection,
        migration: _Migration,
    ) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT name, checksum FROM schema_migrations WHERE version = ?",
                (migration.version,),
            ).fetchone()
            if row is not None:
                if row[0] != migration.name or row[1] != migration.checksum:
                    raise StoreError(f"SQLite migration {migration.version} checksum mismatch")
                connection.commit()
                return
            for statement in migration.statements:
                connection.execute(statement)
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, checksum, applied_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    migration.version,
                    migration.name,
                    migration.checksum,
                    datetime.now(UTC).isoformat(),
                ),
            )
            connection.execute(f"PRAGMA user_version={migration.version}")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def integrity_check(self) -> str:
        with self.read_connection() as connection:
            row = connection.execute("PRAGMA integrity_check").fetchone()
        return str(row[0])

    def backup(self, destination: str | Path) -> SQLiteBackupReport:
        target = Path(destination)
        if target.resolve() == self.path.resolve():
            raise ValueError("SQLite backup destination must differ from the live database")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        source_connection = self._connect()
        target_connection: sqlite3.Connection | None = None
        try:
            try:
                target_connection = sqlite3.connect(temporary, isolation_level=None)
                _load_sqlite_vec(target_connection)
                source_connection.backup(target_connection)
                integrity = str(target_connection.execute("PRAGMA integrity_check").fetchone()[0])
                if integrity != "ok":
                    raise StoreError(f"SQLite backup integrity check failed: {integrity}")
                page_count = int(target_connection.execute("PRAGMA page_count").fetchone()[0])
                schema_version = int(
                    target_connection.execute(
                        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
                    ).fetchone()[0]
                )
            finally:
                if target_connection is not None:
                    target_connection.close()
                source_connection.close()
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        try:
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return SQLiteBackupReport(
            path=target,
            integrity_check=integrity,
            page_count=page_count,
            schema_version=schema_version,
        )


def _load_sqlite_vec(connection: sqlite3.Connection) -> None:
    try:
        connection.enable_load_extension(True)
        sqlite_vec.load(connection)
    except (AttributeError, sqlite3.Error) as error:
        connection.close()
        raise StoreError("sqlite-vec 0.1.9 could not be loaded") from error
    finally:
        try:
            connection.enable_load_extension(False)
        except (AttributeError, sqlite3.Error):
            pass
