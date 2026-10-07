from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING
from uuid import uuid4

from agent_memory_runtime.audit.hashing import secure_hash
from agent_memory_runtime.domain.event import Event
from agent_memory_runtime.domain.memory import MemoryRecord
from agent_memory_runtime.memory.intake.dream import AutoDreamAnalyzer
from agent_memory_runtime.memory.intake.models import (
    AutoDreamRunReport,
    DreamCheckpoint,
    DreamJob,
    MemoryProposal,
)

if TYPE_CHECKING:
    from agent_memory_runtime.runtime import AgentMemoryRuntime

LOGGER = logging.getLogger(__name__)


class SQLiteDreamStore:
    def __init__(self, path_or_manager: object) -> None:
        self._manager = _manager(path_or_manager)
        self.path = self._manager.path
        self._init_schema()

    def schedule(
        self,
        *,
        tenant_id: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        reason: str = "scheduled",
        mode: str = "deep",
        max_attempts: int = 3,
    ) -> DreamJob:
        if mode not in {"micro", "batch", "deep"}:
            raise ValueError("unsupported dream mode")
        now = _now()
        job_id = _job_id(
            tenant_id=tenant_id,
            user_id=user_id,
            agent_id=agent_id,
            session_id=session_id,
            mode=mode,
        )
        payload = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "reason": reason,
        }
        with self._manager.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM dream_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                job = DreamJob(
                    job_id=job_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    agent_id=agent_id,
                    session_id=session_id,
                    status="pending",
                    reason=reason,
                    created_at=now,
                    updated_at=now,
                    available_at=now,
                    max_attempts=max_attempts,
                    mode=mode,
                )
            else:
                existing = _job_from_dict(json.loads(row[0]))
                if existing.status == "pending":
                    return existing
                if existing.status == "running":
                    job = replace(existing, rerun_requested=True, updated_at=now)
                    connection.execute(
                        "UPDATE dream_jobs SET updated_at = ?, payload = ? WHERE job_id = ?",
                        (now, _serialize(job.to_dict()), job_id),
                    )
                    return job
                job = replace(
                    existing,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    agent_id=agent_id,
                    session_id=session_id,
                    status="pending",
                    reason=reason,
                    updated_at=now,
                    available_at=now,
                    attempts=0,
                    max_attempts=max_attempts,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                    error_type=None,
                    error_hash=None,
                    mode=mode,
                    rerun_requested=False,
                )
            connection.execute(
                """
                INSERT INTO dream_jobs(
                    job_id, tenant_id, user_id, agent_id, session_id, status,
                    available_at, updated_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    agent_id = excluded.agent_id,
                    session_id = excluded.session_id,
                    status = excluded.status,
                    available_at = excluded.available_at,
                    updated_at = excluded.updated_at,
                    payload = excluded.payload
                """,
                (
                    job.job_id,
                    job.tenant_id,
                    job.user_id,
                    job.agent_id,
                    job.session_id,
                    job.status,
                    job.available_at,
                    job.updated_at,
                    _serialize({**payload, **job.to_dict()}),
                ),
            )
        return job

    def claim_next(self, *, worker_id: str, lease_seconds: float = 30.0) -> DreamJob | None:
        now = _now()
        lease_until = _iso(datetime.now(UTC) + timedelta(seconds=lease_seconds))
        token = str(uuid4())
        with self._manager.connection() as connection:
            expired = connection.execute(
                "SELECT job_id, payload FROM dream_jobs WHERE status = 'running' "
                "AND json_extract(payload, '$.lease_expires_at') <= ?",
                (now,),
            ).fetchall()
            for expired_job_id, raw in expired:
                stale = _job_from_dict(json.loads(raw))
                if stale.attempts >= stale.max_attempts:
                    dead = replace(
                        stale, status="dead_letter", updated_at=now,
                        lease_owner=None, lease_token=None, lease_expires_at=None,
                        error_type="LeaseExpired",
                    )
                    connection.execute(
                        "UPDATE dream_jobs SET status = ?, updated_at = ?, payload = ? "
                        "WHERE job_id = ?",
                        (dead.status, now, _serialize(dead.to_dict()), expired_job_id),
                    )
            row = connection.execute(
                """
                SELECT job_id, payload
                FROM dream_jobs
                WHERE (status = 'pending' AND available_at <= ?)
                   OR (status = 'running' AND json_extract(payload, '$.lease_expires_at') <= ?)
                ORDER BY available_at, updated_at, job_id
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            job = _job_from_dict(json.loads(row[1]))
            claimed = replace(
                job,
                status="running",
                updated_at=now,
                attempts=job.attempts + 1,
                lease_owner=worker_id,
                lease_token=token,
                lease_expires_at=lease_until,
                rerun_requested=False,
            )
            connection.execute(
                """
                UPDATE dream_jobs
                SET status = ?, updated_at = ?, payload = ?
                WHERE job_id = ? AND (status = 'pending' OR
                    (status = 'running' AND json_extract(payload, '$.lease_expires_at') <= ?))
                """,
                (
                    claimed.status, claimed.updated_at,
                    _serialize(claimed.to_dict()), job.job_id, now,
                ),
            )
        return claimed

    def complete(self, job: DreamJob, checkpoint: DreamCheckpoint) -> DreamJob:
        now = _now()
        completed = replace(
            job,
            status="succeeded",
            updated_at=now,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            error_type=None,
            error_hash=None,
        )
        with self._manager.connection() as connection:
            current = self._owned_job(connection, job)
            if current.rerun_requested:
                completed = replace(completed, status="pending", available_at=now)
            connection.execute(
                """
                UPDATE dream_jobs
                SET status = ?, available_at = ?, updated_at = ?, payload = ?
                WHERE job_id = ?
                """,
                (
                    completed.status, completed.available_at, now,
                    _serialize(completed.to_dict()), job.job_id,
                ),
            )
            self._put_checkpoint(connection, job, checkpoint, updated_at=now)
        return completed

    def fail(self, job: DreamJob, error: Exception) -> DreamJob:
        now = _now()
        retry = job.attempts < job.max_attempts
        failed = replace(
            job,
            status="pending" if retry else "dead_letter",
            updated_at=now,
            available_at=_iso(datetime.now(UTC) + timedelta(seconds=min(300, 2**job.attempts))),
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            error_type=type(error).__name__,
            error_hash=secure_hash(str(error)),
        )
        with self._manager.connection() as connection:
            current = self._owned_job(connection, job)
            failed = replace(failed, rerun_requested=current.rerun_requested)
            connection.execute(
                """
                UPDATE dream_jobs
                SET status = ?, available_at = ?, updated_at = ?, payload = ?
                WHERE job_id = ?
                """,
                (
                    failed.status,
                    failed.available_at,
                    failed.updated_at,
                    _serialize(failed.to_dict()),
                    job.job_id,
                ),
            )
        return failed

    def _owned_job(self, connection: object, job: DreamJob) -> DreamJob:
        row = connection.execute(
            "SELECT payload FROM dream_jobs WHERE job_id = ?", (job.job_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("dream job disappeared")
        current = _job_from_dict(json.loads(row[0]))
        if current.status != "running" or current.lease_token != job.lease_token:
            raise RuntimeError("dream job lease lost")
        return current

    def renew(self, job: DreamJob, *, lease_seconds: float) -> None:
        until = _iso(datetime.now(UTC) + timedelta(seconds=lease_seconds))
        with self._manager.connection() as connection:
            current = self._owned_job(connection, job)
            renewed = replace(current, lease_expires_at=until)
            connection.execute(
                "UPDATE dream_jobs SET payload = ? WHERE job_id = ?",
                (_serialize(renewed.to_dict()), job.job_id),
            )

    def checkpoint_for(self, job: DreamJob) -> DreamCheckpoint:
        with self._manager.read_connection() as connection:
            row = connection.execute(
                "SELECT payload FROM dream_checkpoints WHERE checkpoint_key = ?",
                (_checkpoint_key(job),),
            ).fetchone()
        if row is None:
            return DreamCheckpoint()
        value = json.loads(row[0])
        return DreamCheckpoint(
            last_processed_sequence=int(value.get("last_processed_sequence", 0)),
            last_state_hash=(
                None
                if value.get("last_state_hash") is None
                else str(value.get("last_state_hash"))
            ),
            dream_version=str(value.get("dream_version") or "auto-dream-v1"),
        )

    def append_review(self, proposal: MemoryProposal, *, status: str, reason: str | None) -> None:
        now = _now()
        review_id = f"dream-review:{proposal.proposal_id}"
        payload = {
            "review_id": review_id,
            "proposal": proposal.to_dict(),
            "status": status,
            "reason": reason,
            "created_at": now,
        }
        with self._manager.connection() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO memory_proposal_reviews(
                    review_id, proposal_id, status, tenant_id, user_id,
                    agent_id, created_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    proposal.proposal_id,
                    status,
                    proposal.tenant_id,
                    proposal.user_id,
                    proposal.agent_id,
                    now,
                    _serialize(payload),
                ),
            )

    def list_reviews(self) -> list[dict[str, object]]:
        with self._manager.read_connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM memory_proposal_reviews ORDER BY id"
            ).fetchall()
        return [dict(json.loads(row[0])) for row in rows]

    def list_jobs(self) -> list[DreamJob]:
        with self._manager.read_connection() as connection:
            rows = connection.execute("SELECT payload FROM dream_jobs ORDER BY job_id").fetchall()
        return [_job_from_dict(json.loads(row[0])) for row in rows]

    def _put_checkpoint(
        self,
        connection: object,
        job: DreamJob,
        checkpoint: DreamCheckpoint,
        *,
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO dream_checkpoints(
                checkpoint_key, tenant_id, user_id, agent_id, session_id, updated_at, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(checkpoint_key) DO UPDATE SET
                updated_at = excluded.updated_at,
                payload = excluded.payload
            """,
            (
                _checkpoint_key(job),
                job.tenant_id,
                job.user_id,
                job.agent_id,
                job.session_id,
                updated_at,
                _serialize(checkpoint.to_dict()),
            ),
        )

    def _init_schema(self) -> None:
        with self._manager.connection() as connection:
            connection.execute(
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
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dream_jobs_status
                ON dream_jobs(status, available_at)
                """
            )
            connection.execute(
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
                """
            )
            connection.execute(
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
                """
            )


class AutoDreamWorker:
    def __init__(
        self,
        *,
        runtime: AgentMemoryRuntime,
        store: SQLiteDreamStore,
        analyzer: AutoDreamAnalyzer | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
        poll_interval_seconds: float = 1.0,
        before_poll: Callable[[], object] | None = None,
        on_applied: Callable[[str | None], object] | None = None,
    ) -> None:
        self.runtime = runtime
        self.store = store
        self.analyzer = analyzer or AutoDreamAnalyzer()
        self.worker_id = worker_id or f"auto-dream-{uuid4()}"
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.before_poll = before_poll
        self.on_applied = on_applied
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> AutoDreamRunReport:
        job = self.store.claim_next(
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )
        if job is None:
            return AutoDreamRunReport(job=None)
        try:
            checkpoint = self.store.checkpoint_for(job)
            list_events_since = getattr(self.runtime.event_store, "list_events_since", None)
            events = (
                list_events_since(
                    checkpoint.last_processed_sequence,
                    tenant_id=job.tenant_id,
                    user_id=job.user_id,
                    agent_id=job.agent_id,
                    session_id=job.session_id,
                    limit=500,
                )
                if callable(list_events_since)
                else _events_for_job(self.runtime.event_store.list_events(), job)
            )
            list_records_for_scope = getattr(
                self.runtime.memory_store, "list_records_for_scope", None
            )
            records = (
                list_records_for_scope(
                    tenant_id=job.tenant_id, user_id=job.user_id, agent_id=job.agent_id
                )
                if callable(list_records_for_scope)
                else _records_for_job(self.runtime.memory_store.list_records(), job)
            )
            context_events = None
            recent_for_scope = getattr(self.runtime.event_store, "recent_events_for_scope", None)
            if (
                events and job.mode == "deep" and job.user_id is not None
                and callable(recent_for_scope)
            ):
                context_events = recent_for_scope(
                    tenant_id=job.tenant_id,
                    user_id=job.user_id,
                    agent_id=job.agent_id,
                    since=_iso(datetime.now(UTC) - timedelta(days=14)),
                    through_sequence=events[-1].sequence,
                )
            report = self.analyzer.analyze(
                events=events,
                records=records,
                checkpoint=checkpoint,
                dream_run_id=job.job_id,
                mode=job.mode,
                context_events=context_events,
            )
            applied = review = rejected = conflicts = failed = 0
            for proposal in report.proposals:
                self.store.renew(job, lease_seconds=self.lease_seconds)
                if proposal.decision_status == "pending_review":
                    review += 1
                    self.store.append_review(
                        proposal,
                        status="pending_review",
                        reason=proposal.reason,
                    )
                    continue
                result = self.runtime.apply_memory_proposal(proposal)
                if result.status == "succeeded":
                    applied += 1
                    if self.on_applied is not None:
                        self.on_applied(job.user_id)
                elif result.status == "needs_review":
                    review += 1
                    self.store.append_review(proposal, status=result.status, reason=result.reason)
                elif result.status == "rejected":
                    rejected += 1
                    self.store.append_review(proposal, status=result.status, reason=result.reason)
                elif result.status == "conflict":
                    conflicts += 1
                    self.store.append_review(proposal, status=result.status, reason=result.reason)
                else:
                    failed += 1
                    self.store.append_review(proposal, status=result.status, reason=result.reason)
            if conflicts or failed:
                raise RuntimeError("Auto Dream proposals need retry before checkpoint")
            if len(events) >= 500:
                self.store.schedule(
                    tenant_id=job.tenant_id, user_id=job.user_id,
                    agent_id=job.agent_id, session_id=job.session_id,
                    mode=job.mode, reason="continue_paged_events",
                )
            completed = self.store.complete(job, report.checkpoint)
            return AutoDreamRunReport(
                job=completed,
                analyzed=True,
                proposals=len(report.proposals),
                applied=applied,
                review=review,
                rejected=rejected,
                conflicts=conflicts,
                failed=failed,
                checkpoint=report.checkpoint,
            )
        except Exception as error:
            LOGGER.warning(
                "Auto Dream job failed: job_id=%s error_type=%s",
                job.job_id, type(error).__name__,
            )
            try:
                failed_job = self.store.fail(job, error)
            except RuntimeError:
                failed_job = job
            return AutoDreamRunReport(job=failed_job, analyzed=False, failed=1)

    def run_forever(self, *, stop_after_jobs: int | None = None) -> AutoDreamRunReport:
        processed = 0
        totals = AutoDreamRunReport(job=None)
        while not self._stop.is_set() and (stop_after_jobs is None or processed < stop_after_jobs):
            if self.before_poll is not None:
                self.before_poll()
            report = self.run_once()
            if report.job is None:
                sleep(self.poll_interval_seconds)
                continue
            processed += 1
            totals = AutoDreamRunReport(
                job=report.job,
                analyzed=totals.analyzed or report.analyzed,
                proposals=totals.proposals + report.proposals,
                applied=totals.applied + report.applied,
                review=totals.review + report.review,
                rejected=totals.rejected + report.rejected,
                conflicts=totals.conflicts + report.conflicts,
                failed=totals.failed + report.failed,
                checkpoint=report.checkpoint or totals.checkpoint,
            )
        return totals

    def start_background(self) -> threading.Thread:
        if self._thread is not None and self._thread.is_alive():
            return self._thread
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run_forever,
            name=f"amem-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()
        return self._thread

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)


def _events_for_job(events: list[Event], job: DreamJob) -> list[Event]:
    return [
        event
        for event in events
        if event.tenant_id == job.tenant_id
        and (job.user_id is None or event.user_id == job.user_id)
        and (job.agent_id is None or event.agent_id == job.agent_id)
        and (job.session_id is None or event.session_id == job.session_id)
    ]


def _records_for_job(records: list[MemoryRecord], job: DreamJob) -> list[MemoryRecord]:
    return [
        record
        for record in records
        if record.tenant_id == job.tenant_id
        and (job.user_id is None or record.user_id == job.user_id)
        and (job.agent_id is None or record.agent_id == job.agent_id)
    ]


def _manager(path_or_manager: object) -> object:
    if hasattr(path_or_manager, "connection") and hasattr(path_or_manager, "path"):
        return path_or_manager
    from pathlib import Path

    from agent_memory_runtime.memory.stores.sqlite_manager import SQLiteTransactionManager

    return SQLiteTransactionManager(Path(str(path_or_manager)))


def _job_id(
    *,
    tenant_id: str,
    user_id: str | None,
    agent_id: str | None,
    session_id: str | None,
    mode: str = "deep",
) -> str:
    return "auto-dream:" + secure_hash(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "agent_id": agent_id,
            "session_id": session_id,
            **({"mode": mode} if mode != "deep" else {}),
        }
    )[:24]


def _checkpoint_key(job: DreamJob) -> str:
    return _job_id(
        tenant_id=job.tenant_id,
        user_id=job.user_id,
        agent_id=job.agent_id,
        session_id=job.session_id,
        mode=job.mode,
    )


def _job_from_dict(value: dict[str, object]) -> DreamJob:
    return DreamJob(
        job_id=str(value["job_id"]),
        tenant_id=str(value.get("tenant_id") or "default"),
        user_id=None if value.get("user_id") is None else str(value["user_id"]),
        agent_id=None if value.get("agent_id") is None else str(value["agent_id"]),
        session_id=None if value.get("session_id") is None else str(value["session_id"]),
        status=str(value.get("status") or "pending"),
        reason=str(value.get("reason") or "scheduled"),
        created_at=str(value.get("created_at") or ""),
        updated_at=str(value.get("updated_at") or ""),
        available_at=str(value.get("available_at") or ""),
        attempts=int(value.get("attempts") or 0),
        max_attempts=int(value.get("max_attempts") or 3),
        lease_owner=None if value.get("lease_owner") is None else str(value["lease_owner"]),
        lease_token=None if value.get("lease_token") is None else str(value["lease_token"]),
        lease_expires_at=(
            None if value.get("lease_expires_at") is None else str(value["lease_expires_at"])
        ),
        error_type=None if value.get("error_type") is None else str(value["error_type"]),
        error_hash=None if value.get("error_hash") is None else str(value["error_hash"]),
        mode=str(value.get("mode") or "deep"),
        rerun_requested=bool(value.get("rerun_requested", False)),
    )


def _serialize(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _now() -> str:
    return _iso(datetime.now(UTC))


def _iso(value: datetime) -> str:
    return value.isoformat()
