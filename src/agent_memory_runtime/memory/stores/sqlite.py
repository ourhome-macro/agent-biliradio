from __future__ import annotations

import json
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING

from agent_memory_runtime.audit.stores.sqlite import SQLiteAuditStore
from agent_memory_runtime.domain.enums import (
    MemoryLabel,
    MemoryLevel,
    MemoryStatus,
    MemoryTemperature,
    MemoryVisibility,
)
from agent_memory_runtime.domain.event import Event
from agent_memory_runtime.domain.memory import MemoryRecord, normalize_record_temperature
from agent_memory_runtime.domain.query import MemoryQuery
from agent_memory_runtime.domain.tombstone import MemoryTombstone
from agent_memory_runtime.exceptions import EventConflictError, StoreError
from agent_memory_runtime.memory.retrieval.candidates import CandidateHit
from agent_memory_runtime.memory.retrieval.lexical import (
    fts_document_text,
    fts_match_query,
)
from agent_memory_runtime.memory.stores.sqlite_filters import structured_memory_where
from agent_memory_runtime.memory.stores.sqlite_manager import (
    SQLiteBackupReport,
    SQLiteTransactionManager,
)

if TYPE_CHECKING:
    from agent_memory_runtime.agent.orchestration.stores import OrchestrationStateStore
    from agent_memory_runtime.agent.stores import StateCodec
    from agent_memory_runtime.memory.embeddings import (
        EmbeddingProvider,
        SQLiteEmbeddingScheduler,
        VectorIndex,
    )


class SQLiteStore:
    def __init__(self, path_or_manager: str | Path | SQLiteTransactionManager) -> None:
        self._manager = (
            path_or_manager
            if isinstance(path_or_manager, SQLiteTransactionManager)
            else SQLiteTransactionManager(path_or_manager)
        )
        self.path = self._manager.path


class SQLiteEventStore(SQLiteStore):
    def append(self, event: Event) -> Event:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if row is not None:
                existing = Event.from_dict(json.loads(row[0]))
                if not existing.is_retry_of(event):
                    raise EventConflictError(
                        f"event_id {event.event_id!r} is already bound to a different event"
                    )
                return existing
            current = connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM events").fetchone()
            sequence = event.sequence or int(current[0]) + 1
            stored = Event.from_dict({**event.to_dict(), "sequence": sequence})
            connection.execute(
                "INSERT INTO events(sequence, event_id, payload) VALUES (?, ?, ?)",
                (stored.sequence, stored.event_id, _serialize(stored.to_dict())),
            )
        return stored

    def get(self, event_id: str) -> Event | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return Event.from_dict(json.loads(row[0]))

    def list_events(self) -> list[Event]:
        with self._manager.read_connection() as connection:
            rows = connection.execute("SELECT payload FROM events ORDER BY sequence").fetchall()
        return [Event.from_dict(json.loads(row[0])) for row in rows]

    def list_events_since(
        self,
        sequence: int,
        *,
        tenant_id: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        limit: int = 500,
    ) -> list[Event]:
        clauses = ["sequence > ?", "json_extract(payload, '$.tenant_id') = ?"]
        params: list[object] = [sequence, tenant_id]
        for name, value in (
            ("user_id", user_id), ("agent_id", agent_id), ("session_id", session_id)
        ):
            if value is not None:
                clauses.append(f"json_extract(payload, '$.{name}') = ?")
                params.append(value)
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM events WHERE " + " AND ".join(clauses)
                + " ORDER BY sequence LIMIT ?",
                [*params, max(1, limit)],
            ).fetchall()
        return [Event.from_dict(json.loads(row[0])) for row in rows]

    def recent_events_for_scope(
        self,
        *,
        tenant_id: str,
        user_id: str,
        agent_id: str | None = None,
        since: str,
        max_events: int = 80,
        through_sequence: int | None = None,
    ) -> list[Event]:
        clauses = [
            "json_extract(payload, '$.tenant_id') = ?",
            "json_extract(payload, '$.user_id') = ?",
            "json_extract(payload, '$.occurred_at') >= ?",
            "(json_extract(payload, '$.kind') != 'observation.created' OR "
            "json_extract(payload, '$.payload.event') IN "
            "('skipped','dismissed','dislike','liked','collection_added',"
            "'completed','track_reviewed','profile_statement','search'))",
        ]
        params: list[object] = [tenant_id, user_id, since]
        if agent_id is not None:
            clauses.append("json_extract(payload, '$.agent_id') = ?")
            params.append(agent_id)
        if through_sequence is not None:
            clauses.append("sequence <= ?")
            params.append(through_sequence)
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM events WHERE " + " AND ".join(clauses)
                + " ORDER BY sequence DESC LIMIT 2000",
                params,
            ).fetchall()
        per_day: dict[str, int] = {}
        selected: list[Event] = []
        for row in rows:
            event = Event.from_dict(json.loads(row[0]))
            day = event.occurred_at[:10]
            if per_day.get(day, 0) >= 6:
                continue
            selected.append(event)
            per_day[day] = per_day.get(day, 0) + 1
            if len(selected) >= max_events:
                break
        return sorted(selected, key=lambda item: item.sequence)

    def clear(self) -> None:
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM events")


class SQLiteMemoryStore(SQLiteStore):
    def __init__(
        self,
        path_or_manager: str | Path | SQLiteTransactionManager,
        *,
        embedding_scheduler: SQLiteEmbeddingScheduler | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        vector_index: VectorIndex | None = None,
    ) -> None:
        super().__init__(path_or_manager)
        self.embedding_scheduler = embedding_scheduler
        self.embedding_provider = embedding_provider
        self.vector_index = vector_index
        self._backfill_search_index()

    def build_candidate_retriever(self, config: object) -> object:
        from agent_memory_runtime.config import (
            HybridRetrievalConfig,
            RuntimeConfig,
        )
        from agent_memory_runtime.memory.retrieval import (
            HybridCandidateRetriever,
            SemanticRetriever,
            StoreLexicalRetriever,
        )

        if isinstance(config, RuntimeConfig):
            config = config.hybrid_retrieval
        if not isinstance(config, HybridRetrievalConfig):
            raise TypeError("config must be RuntimeConfig or HybridRetrievalConfig")
        semantic = None
        if (
            config.enable_semantic
            and self.embedding_provider is not None
            and self.vector_index is not None
        ):
            semantic = SemanticRetriever(
                provider=self.embedding_provider,
                vector_index=self.vector_index,
                config=config,
            )
        if not config.enable_lexical and semantic is None:
            from agent_memory_runtime.exceptions import EmbeddingConfigurationError

            raise EmbeddingConfigurationError(
                "semantic-only retrieval requires an active embedding provider"
            )
        return HybridCandidateRetriever(
            lexical=(StoreLexicalRetriever(self) if config.enable_lexical else None),
            semantic=semantic,
            config=config,
        )

    def upsert(self, record: MemoryRecord) -> None:
        record = normalize_record_temperature(record)
        with self._manager.connection() as connection:
            connection.execute(
                """
                INSERT INTO memories(
                    memory_id, payload, tenant_id, user_id, agent_id, session_id,
                    status, memory_type, updated_at, salience,
                    level, visibility, priority, temperature,
                    search_indexed, retrieval_v6_indexed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                ON CONFLICT(memory_id) DO UPDATE SET
                    payload = excluded.payload,
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    agent_id = excluded.agent_id,
                    session_id = excluded.session_id,
                    status = excluded.status,
                    memory_type = excluded.memory_type,
                    updated_at = excluded.updated_at,
                    salience = excluded.salience,
                    level = excluded.level,
                    visibility = excluded.visibility,
                    priority = excluded.priority,
                    temperature = excluded.temperature,
                    search_indexed = 1,
                    retrieval_v6_indexed = 1
                """,
                _memory_row(record),
            )
            self._replace_search_index(connection, record)
            self._schedule_embedding(record)

    def get(self, memory_id: str) -> MemoryRecord | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM memories WHERE memory_id = ?",
                (memory_id,),
            ).fetchone()
        if row is None:
            return None
        return MemoryRecord.from_dict(json.loads(row[0]))

    def delete(self, memory_id: str) -> None:
        _retire_vector_memory(self.vector_index, memory_id)
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM memory_fts WHERE memory_id = ?", (memory_id,))
            connection.execute("DELETE FROM memory_tags WHERE memory_id = ?", (memory_id,))
            connection.execute("DELETE FROM memory_acl WHERE memory_id = ?", (memory_id,))
            connection.execute("DELETE FROM memories WHERE memory_id = ?", (memory_id,))

    def list_records(self) -> list[MemoryRecord]:
        with self._manager.read_connection() as connection:
            rows = connection.execute("SELECT payload FROM memories ORDER BY memory_id").fetchall()
        return [MemoryRecord.from_dict(json.loads(row[0])) for row in rows]

    def list_records_for_scope(
        self, *, tenant_id: str, user_id: str | None = None, agent_id: str | None = None
    ) -> list[MemoryRecord]:
        clauses = ["tenant_id = ?"]
        params: list[object] = [tenant_id]
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(user_id)
        if agent_id is not None:
            clauses.append("agent_id = ?")
            params.append(agent_id)
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM memories WHERE " + " AND ".join(clauses), params
            ).fetchall()
        return [MemoryRecord.from_dict(json.loads(row[0])) for row in rows]

    def get_many(self, memory_ids: list[str] | tuple[str, ...]) -> list[MemoryRecord]:
        ordered_ids = tuple(dict.fromkeys(memory_ids))
        if not ordered_ids:
            return []
        placeholders = ", ".join("?" for _ in ordered_ids)
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                f"SELECT memory_id, payload FROM memories WHERE memory_id IN ({placeholders})",
                ordered_ids,
            ).fetchall()
        by_id = {
            str(memory_id): MemoryRecord.from_dict(json.loads(payload))
            for memory_id, payload in rows
        }
        return [by_id[memory_id] for memory_id in ordered_ids if memory_id in by_id]

    def query_records(
        self,
        query: MemoryQuery,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[MemoryRecord]:
        if limit <= 0:
            return []
        match_query = fts_match_query(query.text)
        where, parameters = structured_memory_where(query)
        if match_query:
            sql = f"""
                SELECT memories.payload
                FROM memory_fts
                JOIN memories ON memories.memory_id = memory_fts.memory_id
                WHERE memory_fts MATCH ? AND {" AND ".join(where)}
                ORDER BY bm25(memory_fts) ASC,
                         {_temperature_order_sql('memories')} ASC,
                         memories.priority DESC,
                         memories.updated_at DESC,
                         memories.memory_id ASC
                LIMIT ? OFFSET ?
            """
            parameters.insert(0, match_query)
        else:
            sql = f"""
                SELECT memories.payload
                FROM memories
                WHERE {" AND ".join(where)}
                ORDER BY {_temperature_order_sql('memories')} ASC,
                         memories.priority DESC,
                         memories.updated_at DESC,
                         memories.memory_id ASC
                LIMIT ? OFFSET ?
            """
        parameters.extend([limit, max(0, offset)])
        with self._manager.read_connection() as connection:
            rows = connection.execute(sql, tuple(parameters)).fetchall()
        return [MemoryRecord.from_dict(json.loads(row[0])) for row in rows]

    def search_lexical(
        self,
        query: MemoryQuery,
        *,
        limit: int,
    ) -> list[CandidateHit]:
        if limit <= 0:
            return []
        match_query = fts_match_query(query.text)
        where, parameters = structured_memory_where(query)
        if match_query:
            sql = f"""
                SELECT memories.memory_id, bm25(memory_fts) AS lexical_score
                FROM memory_fts
                JOIN memories ON memories.memory_id = memory_fts.memory_id
                WHERE memory_fts MATCH ? AND {" AND ".join(where)}
                ORDER BY lexical_score ASC,
                         {_temperature_order_sql('memories')} ASC,
                         memories.priority DESC,
                         memories.updated_at DESC,
                         memories.memory_id ASC
                LIMIT ?
            """
            parameters.insert(0, match_query)
        else:
            sql = f"""
                SELECT memories.memory_id, NULL AS lexical_score
                FROM memories
                WHERE {" AND ".join(where)}
                ORDER BY {_temperature_order_sql('memories')} ASC,
                         memories.priority DESC,
                         memories.updated_at DESC,
                         memories.memory_id ASC
                LIMIT ?
            """
        parameters.append(limit)
        with self._manager.read_connection() as connection:
            rows = connection.execute(sql, tuple(parameters)).fetchall()
        return [
            CandidateHit(
                memory_id=str(memory_id),
                sources=("lexical",),
                lexical_rank=rank,
                lexical_raw_score=(None if lexical_score is None else float(lexical_score)),
            )
            for rank, (memory_id, lexical_score) in enumerate(rows, start=1)
        ]

    def replace_all(self, records: list[MemoryRecord]) -> None:
        records = [normalize_record_temperature(record) for record in records]
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM memory_fts")
            connection.execute("DELETE FROM memory_tags")
            connection.execute("DELETE FROM memory_acl")
            connection.execute("DELETE FROM memories")
            connection.executemany(
                """
                INSERT INTO memories(
                    memory_id, payload, tenant_id, user_id, agent_id, session_id,
                    status, memory_type, updated_at, salience,
                    level, visibility, priority, temperature,
                    search_indexed, retrieval_v6_indexed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                """,
                [_memory_row(record) for record in records],
            )
            self._insert_search_indexes(connection, records)
            if self.embedding_scheduler is not None:
                self.embedding_scheduler.schedule_many(
                    [
                        (record, _should_embed(record))
                        for record in records
                    ]
                )

    @staticmethod
    def _insert_search_indexes(
        connection: object,
        records: list[MemoryRecord],
    ) -> None:
        indexed_records = [
            (record, principals)
            for record in records
            if (principals := _acl_principals(record))
        ]
        fts_rows = [
            (record.memory_id, document)
            for record, _ in indexed_records
            if (document := fts_document_text(record))
        ]
        if fts_rows:
            connection.executemany(
                "INSERT INTO memory_fts(memory_id, terms) VALUES (?, ?)",
                fts_rows,
            )
        tag_rows = [
            (record.memory_id, tag)
            for record, _ in indexed_records
            for tag in sorted(set(record.tags))
        ]
        if tag_rows:
            connection.executemany(
                "INSERT INTO memory_tags(memory_id, tag) VALUES (?, ?)",
                tag_rows,
            )
        acl_rows = [
            (record.memory_id, principal)
            for record, principals in indexed_records
            for principal in principals
        ]
        if acl_rows:
            connection.executemany(
                "INSERT INTO memory_acl(memory_id, principal_id) VALUES (?, ?)",
                acl_rows,
            )

    def clear(self) -> None:
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM memory_fts")
            connection.execute("DELETE FROM memory_tags")
            connection.execute("DELETE FROM memory_acl")
            connection.execute("DELETE FROM memories")

    def _backfill_search_index(self) -> None:
        with self._manager.connection() as connection:
            rows = connection.execute(
                "SELECT memory_id, payload FROM memories WHERE retrieval_v6_indexed = 0"
            ).fetchall()
            records = [MemoryRecord.from_dict(json.loads(payload)) for _, payload in rows]
            self._insert_search_indexes(connection, records)
            connection.executemany(
                """
                UPDATE memories
                SET search_indexed = 1, retrieval_v6_indexed = 1
                WHERE memory_id = ?
                """,
                [(memory_id,) for memory_id, _ in rows],
            )

    @staticmethod
    def _replace_search_index(connection: object, record: MemoryRecord) -> None:
        connection.execute("DELETE FROM memory_fts WHERE memory_id = ?", (record.memory_id,))
        connection.execute("DELETE FROM memory_tags WHERE memory_id = ?", (record.memory_id,))
        connection.execute("DELETE FROM memory_acl WHERE memory_id = ?", (record.memory_id,))
        principals = _acl_principals(record)
        if not principals:
            return
        document = fts_document_text(record)
        if document:
            connection.execute(
                "INSERT INTO memory_fts(memory_id, terms) VALUES (?, ?)",
                (record.memory_id, document),
            )
        if record.tags:
            connection.executemany(
                "INSERT INTO memory_tags(memory_id, tag) VALUES (?, ?)",
                [(record.memory_id, tag) for tag in sorted(set(record.tags))],
            )
        connection.executemany(
            "INSERT INTO memory_acl(memory_id, principal_id) VALUES (?, ?)",
            [(record.memory_id, principal) for principal in principals],
        )

    def enqueue_embedding_backfill(self) -> int:
        if self.embedding_scheduler is None:
            return 0
        records = self.list_records()
        return len(
            self.embedding_scheduler.schedule_many(
                [
                    (record, _should_embed(record))
                    for record in records
                ]
            )
        )

    def _schedule_embedding(self, record: MemoryRecord) -> None:
        _retire_vector_memory(self.vector_index, record.memory_id)
        if self.embedding_scheduler is None:
            return
        self.embedding_scheduler.schedule(
            record,
            retrievable=_should_embed(record),
        )


class SQLiteSnapshotStore(SQLiteStore):
    def save(self, snapshot: dict[str, object]) -> None:
        with self._manager.connection() as connection:
            connection.execute("INSERT INTO snapshots(payload) VALUES (?)", (_serialize(snapshot),))

    def latest(self) -> dict[str, object] | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return dict(json.loads(row[0]))

    def clear(self) -> None:
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM snapshots")

    def prune(self, *, keep_last: int) -> int:
        keep = max(0, keep_last)
        with self._manager.connection() as connection:
            before = int(connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
            if keep:
                connection.execute(
                    """
                    DELETE FROM snapshots
                    WHERE id NOT IN (
                        SELECT id FROM snapshots ORDER BY id DESC LIMIT ?
                    )
                    """,
                    (keep,),
                )
            else:
                connection.execute("DELETE FROM snapshots")
            after = int(connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
        return before - after


class SQLiteTombstoneStore(SQLiteStore):
    def put(self, tombstone: MemoryTombstone) -> None:
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM memory_tombstones WHERE memory_id = ?",
                (tombstone.memory_id,),
            ).fetchone()
            if row is not None:
                current = MemoryTombstone.from_dict(json.loads(row[0]))
                if current.deleted_through_sequence > tombstone.deleted_through_sequence:
                    return
            connection.execute(
                """
                INSERT OR REPLACE INTO memory_tombstones(
                    memory_id, tenant_id, deleted_at, payload
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    tombstone.memory_id,
                    tombstone.tenant_id,
                    tombstone.deleted_at,
                    _serialize(tombstone.to_dict()),
                ),
            )

    def get(self, memory_id: str) -> MemoryTombstone | None:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM memory_tombstones WHERE memory_id = ?",
                (memory_id,),
            ).fetchone()
        return None if row is None else MemoryTombstone.from_dict(json.loads(row[0]))

    def list_tombstones(self) -> list[MemoryTombstone]:
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM memory_tombstones ORDER BY memory_id"
            ).fetchall()
        return [MemoryTombstone.from_dict(json.loads(row[0])) for row in rows]

    def clear(self) -> None:
        with self._manager.connection() as connection:
            connection.execute("DELETE FROM memory_tombstones")


class SQLiteStoreBundle:
    """Creates stores that share one transaction manager and database file."""

    def __init__(
        self,
        path: str | Path,
        *,
        agent_state_codec: StateCodec | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        vector_index: VectorIndex | None = None,
    ) -> None:
        from agent_memory_runtime.agent.orchestration.stores import (
            SQLiteOrchestrationStore,
        )
        from agent_memory_runtime.agent.stores import SQLiteAgentStateStore
        from agent_memory_runtime.memory.embeddings import (
            SQLiteEmbeddingGenerationStore,
            SQLiteEmbeddingJobStore,
            SQLiteEmbeddingScheduler,
            SQLiteVectorIndex,
        )
        from agent_memory_runtime.memory.intake.worker import SQLiteDreamStore

        self._manager = SQLiteTransactionManager(path)
        self.embedding_generations = SQLiteEmbeddingGenerationStore(self._manager)
        self.embedding_jobs = SQLiteEmbeddingJobStore(self._manager)
        self.vector_index = vector_index or SQLiteVectorIndex(self._manager)
        if embedding_provider is not None:
            self.embedding_generations.ensure_active(embedding_provider.spec)
        self.embedding_provider = embedding_provider
        embedding_scheduler = SQLiteEmbeddingScheduler(
            generations=self.embedding_generations,
            jobs=self.embedding_jobs,
            vectors=self.vector_index,
        )
        self.event_store = SQLiteEventStore(self._manager)
        self.memory_store = SQLiteMemoryStore(
            self._manager,
            embedding_scheduler=embedding_scheduler,
            embedding_provider=embedding_provider,
            vector_index=self.vector_index,
        )
        self.snapshot_store = SQLiteSnapshotStore(self._manager)
        self.tombstone_store = SQLiteTombstoneStore(self._manager)
        self.audit_store = SQLiteAuditStore(self._manager)
        self.dream_store = SQLiteDreamStore(self._manager)
        self.agent_state_store = SQLiteAgentStateStore(
            self._manager,
            codec=agent_state_codec,
        )
        self.orchestration_store: OrchestrationStateStore = SQLiteOrchestrationStore(
            self._manager,
            codec=agent_state_codec,
        )
        if embedding_provider is not None:
            self.memory_store.enqueue_embedding_backfill()

    def transaction(self) -> AbstractContextManager[None]:
        return self._manager.transaction()

    @property
    def schema_version(self) -> int:
        return self._manager.schema_version

    def integrity_check(self) -> str:
        return self._manager.integrity_check()

    def backup(self, destination: str | Path) -> SQLiteBackupReport:
        return self._manager.backup(destination)

    def enqueue_embedding_backfill(self) -> int:
        return self.memory_store.enqueue_embedding_backfill()

    def activate_embedding_generation(
        self,
        generation: str,
        *,
        minimum_coverage: float = 1.0,
        allow_pending_jobs: bool = False,
    ) -> object:
        if not 0.0 <= minimum_coverage <= 1.0:
            raise ValueError("minimum_coverage must be between 0 and 1")
        coverage = self.vector_index.coverage(generation=generation)
        pending = self.embedding_jobs.outstanding_count(generation=generation)
        if coverage < minimum_coverage:
            raise StoreError(
                f"embedding generation {generation!r} coverage {coverage:.4f} "
                f"is below the activation threshold {minimum_coverage:.4f}"
            )
        if pending and not allow_pending_jobs:
            raise StoreError(
                f"embedding generation {generation!r} still has {pending} pending jobs"
            )
        return self.embedding_generations.activate(generation)

    def semantic_status(self) -> dict[str, object]:
        active = self.embedding_generations.active()
        generation = None if active is None else active.generation
        generation_statuses = []
        for item in self.embedding_generations.list_generations():
            item_generation = str(item["generation"])
            item_jobs = self.embedding_jobs.list_jobs(generation=item_generation)
            item_counts: dict[str, int] = {}
            for job in item_jobs:
                item_counts[job.status] = item_counts.get(job.status, 0) + 1
            generation_statuses.append(
                {
                    **item,
                    "embedding_coverage": self.vector_index.coverage(
                        generation=item_generation
                    ),
                    "ready_vectors": _ready_count(self.vector_index, generation=item_generation),
                    "job_status_counts": item_counts,
                    "embedding_backlog_lag_seconds": (
                        self.embedding_jobs.backlog_lag_seconds(
                            generation=item_generation
                        )
                    ),
                }
            )
        jobs = self.embedding_jobs.list_jobs(generation=generation)
        status_counts: dict[str, int] = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        coverage = None if generation is None else self.vector_index.coverage(generation=generation)
        return {
            "semantic_available": self.embedding_provider is not None,
            "sqlite_vec_loaded": True,
            "active_generation": generation,
            "provider_generation": (
                None if self.embedding_provider is None else self.embedding_provider.spec.generation
            ),
            "embedding_coverage": coverage,
            "ready_vectors": (
                0 if generation is None else _ready_count(self.vector_index, generation=generation)
            ),
            "job_status_counts": status_counts,
            "embedding_backlog_lag_seconds": self.embedding_jobs.backlog_lag_seconds(
                generation=generation
            ),
            "generations": generation_statuses,
        }

    def delete_retired_embedding_generation(self, generation: str) -> None:
        self.embedding_generations.delete_retired(generation)

    def embedding_worker(
        self,
        provider: EmbeddingProvider | None = None,
        **kwargs: object,
    ) -> object:
        from agent_memory_runtime.memory.embeddings import EmbeddingWorker

        selected_provider = provider or self.embedding_provider
        if selected_provider is None:
            raise ValueError("an embedding provider is required to create a worker")
        return EmbeddingWorker(
            provider=selected_provider,
            jobs=self.embedding_jobs,
            vectors=self.vector_index,
            memories=self.memory_store,
            **kwargs,
        )

    def shadow_replay(
        self,
        *,
        config: object | None = None,
    ) -> object:
        from agent_memory_runtime.audit.replay import shadow_replay_events

        return shadow_replay_events(
            self.event_store.list_events(),
            self.snapshot_store.latest(),
            config=config,
        )


def _serialize(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _retire_vector_memory(vector_index: object | None, memory_id: str) -> None:
    if vector_index is None:
        return
    mark_retired = getattr(vector_index, "mark_retired_stale", None)
    try:
        if callable(mark_retired):
            mark_retired(memory_id)
            return
        delete_memory = getattr(vector_index, "delete_memory", None)
        if callable(delete_memory):
            delete_memory(memory_id)
    except Exception:
        return


def _ready_count(vector_index: object, *, generation: str) -> int:
    ready_count = getattr(vector_index, "ready_count", None)
    if not callable(ready_count):
        return 0
    return int(ready_count(generation=generation))


def _memory_row(record: MemoryRecord) -> tuple[object, ...]:
    return (
        record.memory_id,
        _serialize(record.to_dict()),
        record.tenant_id,
        record.user_id,
        record.agent_id or record.owner_id,
        record.session_id,
        record.status,
        record.memory_type,
        record.updated_at,
        record.salience,
        record.level,
        record.visibility,
        record.priority,
        record.temperature,
    )


def _acl_principals(record: MemoryRecord) -> tuple[str, ...]:
    labels = set(record.labels)
    if MemoryLabel.SENSITIVE.value in labels:
        return ()
    owner_agent_id = record.agent_id or record.owner_id
    principals = set(record.visible_to)
    if owner_agent_id is not None:
        principals.add(owner_agent_id)
    if MemoryLabel.PRIVATE.value not in labels:
        if record.visibility == MemoryVisibility.PUBLIC.value:
            principals.add("*")
        elif record.visibility == MemoryVisibility.SHARED.value and not record.visible_to:
            principals.add("*")
    return tuple(sorted(principals))


def _should_embed(record: MemoryRecord) -> bool:
    if record.status != MemoryStatus.ACTIVE.value:
        return False
    if record.temperature != MemoryTemperature.WARM.value:
        return False
    if not _acl_principals(record):
        return False
    if record.level == MemoryLevel.ATOM.value:
        return True
    if record.level == MemoryLevel.RAW.value:
        return bool(record.metadata.get("embedding_index"))
    return False


def _temperature_order_sql(alias: str) -> str:
    return f"CASE {alias}.temperature WHEN 'hot' THEN 0 WHEN 'warm' THEN 1 ELSE 2 END"
