from __future__ import annotations

import json
import logging
import os
import threading
import time
import atexit
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from celery import Celery
from celery.signals import worker_ready, worker_shutdown
from database import DEFAULT_DB_PATH, get_connection, init_db
from durable_jobs import claim, finish, publish_pending
from kombu import Exchange, Queue
from rabbitmq_bus import RabbitMQSettings
from job_transport import job_transport

LOGGER = logging.getLogger("recommend-radio.tasks")
_outbox_stop = threading.Event()
_outbox_thread: threading.Thread | None = None
_embedding_thread: threading.Thread | None = None
_dream_owner = None
if job_transport() == "redis_stream":
    # Redis Stream workers reuse domain dispatch; they do not connect through Celery.
    settings = None
    broker = "memory://"
else:
    settings = RabbitMQSettings.from_env()
    settings.validate()
    broker = (
        f"amqp://{quote(settings.user, safe='')}:{quote(settings.password, safe='')}@"
        f"{settings.host}:{settings.port}/{quote(settings.virtual_host, safe='')}"
    )
app = Celery("recommend-radio", broker=broker)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    task_soft_time_limit=240,
    task_time_limit=270,
    broker_heartbeat=30,
    broker_connection_timeout=5,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"confirm_publish": True},
    # RabbitMQ 4.3 rejects transient non-exclusive queues by default. Control
    # replies and event listeners are connection-scoped, so make them exclusive.
    control_queue_exclusive=True,
    event_queue_exclusive=True,
    task_publish_retry=False,
    task_create_missing_queues=False,
    task_default_queue="radio.jobs.v2",
    task_default_queue_type="quorum",
    task_queues=tuple(
        Queue(
            name,
            exchange=Exchange(name, type="direct", durable=True),
            routing_key=name,
            durable=True,
            queue_arguments={"x-queue-type": "quorum"},
        )
        for name in ("radio.jobs.v2", "radio.events.v2", "radio.media.v1")
    ),
)


def publish_job(job_id: str, kind: str) -> None:
    from telemetry_setup import setup
    from agent_memory_runtime.telemetry import carrier, span
    setup('radio-outbox')
    queue = "radio.media.v1" if kind == "media_import" else "radio.events.v2" if kind in {"behavior", "sse", "feed_projection", "feed_asset_ready"} else "radio.jobs.v2"
    with get_connection() as conn:
        row = conn.execute('SELECT trace_context FROM durable_jobs WHERE job_id=?', (job_id,)).fetchone()
    parent = json.loads(row['trace_context']) if row else {}
    with span('messaging.publish', parent=parent, attributes={'job.kind':kind,'job.id':job_id}):
        task = execute_media_job if kind == "media_import" else execute_job
        task.apply_async(args=[job_id], task_id=job_id, queue=queue,
                         routing_key=queue, delivery_mode=2, headers=carrier())


def _outbox_loop() -> None:
    from job_transport import publish_job as publish_selected
    while not _outbox_stop.is_set():
        try:
            publish_pending(DEFAULT_DB_PATH, publish_selected)
        except Exception:
            LOGGER.exception("Outbox dispatch failed; persisted jobs will be retried")
        _outbox_stop.wait(1)


def _embedding_loop(bridge) -> None:
    while not _outbox_stop.is_set():
        try:
            report = bridge.process_embedding_jobs(max_jobs=64)
            processed = int(getattr(report, "processed", 0) or 0)
        except Exception:
            LOGGER.exception("AMEM embedding worker iteration failed")
            processed = 0
        _outbox_stop.wait(0.1 if processed else 1)


@worker_ready.connect
def start_runtime_services(**_kwargs) -> None:
    global _outbox_thread, _embedding_thread, _dream_owner
    if _outbox_thread is not None or _dream_owner is not None:
        return
    init_db()
    if os.getenv("RADIO_OUTBOX_EMBEDDED", "true").lower() not in {"0", "false", "no", "off"}:
        _outbox_stop.clear()
        _outbox_thread = threading.Thread(target=_outbox_loop, name="radio-outbox", daemon=True)
        _outbox_thread.start()
    if (
        os.getenv("AMEM_ENABLED", "true").lower() not in {"0", "false", "no", "off"}
        and os.getenv("AMEM_TRANSPORT", "embedded").lower() == "embedded"
    ):
        from amem_bridge import AmemBridge
        from profile_projector import ProfileProjector

        bridge = AmemBridge.from_env()
        bind = getattr(bridge, "bind_dream_cache_invalidator", None)
        if callable(bind):
            projector = ProfileProjector(bridge)
            bind(projector.clear_cache)
            _dream_owner = bridge
            _embedding_thread = threading.Thread(
                target=_embedding_loop, args=(bridge,), name="radio-embedding", daemon=True
            )
            _embedding_thread.start()


@worker_shutdown.connect
def stop_runtime_services(**_kwargs) -> None:
    global _outbox_thread, _embedding_thread, _dream_owner
    _outbox_stop.set()
    if _outbox_thread is not None:
        _outbox_thread.join(timeout=5)
        _outbox_thread = None
    if _embedding_thread is not None:
        _embedding_thread.join(timeout=5)
        _embedding_thread = None
    if _dream_owner is not None:
        _dream_owner.close()
        _dream_owner = None


@lru_cache(maxsize=1)
def _worker_memory():
    from amem_bridge import AmemBridge
    from profile_projector import ProfileProjector

    bridge = AmemBridge.from_env()
    atexit.register(getattr(bridge, "close", lambda: None))
    return bridge, ProfileProjector(bridge)


@app.task(bind=True, name="radio.execute_job")
def execute_job(self, job_id: str) -> None:
    return _execute_durable(self, job_id)


def _execute_durable(self, job_id: str) -> None:
    from telemetry_setup import setup
    from agent_memory_runtime.telemetry import span, flush
    from music_agent import current_job_id
    setup('radio-worker')
    init_db()
    with get_connection() as conn:
        job = claim(conn, job_id)
    if job is None:
        return
    parent = self.request.headers or json.loads(job['trace_context'])
    token = current_job_id.set(job_id)
    try:
        with span('task.execute', parent=parent, attributes={'job.id':job_id,'job.kind':job['kind']}):
            try:
                result = dispatch(job)
            except Exception as error:
                LOGGER.exception("Job failed: %s", job_id)
                with get_connection() as conn:
                    finish(conn, job, error=error)
                raise
            else:
                with get_connection() as conn:
                    finish(conn, job, result=result)
                if job["kind"] == "sse":
                    try:
                        publish_pending(DEFAULT_DB_PATH, publish_job)
                    except Exception:
                        LOGGER.exception("Could not immediately publish the next SSE event")
    finally:
        current_job_id.reset(token)
        flush()


@app.task(bind=True, name="radio.execute_media_job", soft_time_limit=3600, time_limit=3660)
def execute_media_job(self, job_id: str) -> None:
    # Same durable transaction protocol, separate queue and media-specific timeout.
    return _execute_durable(self, job_id)


def dispatch(job: dict):
    payload = json.loads(job["payload_json"])
    if job["kind"] in {"media_import", "feed_projection", "feed_asset_ready"}:
        from feed_runtime.config import FeedConfig
        from feed_runtime.services import FeedServices
        config = FeedConfig.from_env(data_dir=Path(DEFAULT_DB_PATH).parent)
        if job["kind"] == "media_import" and not config.enabled:
            from job_errors import JobPermanentFailure
            with get_connection() as conn:
                acquired = conn.execute("SELECT complete_source FROM media_import_jobs WHERE import_id=?",
                                        (payload["import_id"],)).fetchone()
                conn.execute("""UPDATE media_import_jobs SET status='cancelled',stage='cancelled',
                    error_type='FeedDisabled',updated_at=? WHERE import_id=? AND complete_source=0
                    AND (status='queued' OR (status='running' AND lease_until<=?))""",
                    (time.time(),payload["import_id"],time.time()))
            if not acquired or not acquired["complete_source"]:
                raise JobPermanentFailure("Feed is disabled; no new imports are executed")
        runtime = FeedServices(user_id=job["user_id"], config=config)
        if job["kind"] == "media_import":
            from feed_runtime.worker import MediaWorker
            return MediaWorker(runtime.repo, config, runtime.storage).run(payload["import_id"], transport_job=job)
        if job["kind"] == "feed_projection":
            return runtime.reactions.project(payload)
        # Source content, not private preference memory, gets a semantic projection.
        from content_embeddings import ContentEmbeddingService
        from models import Track
        with get_connection() as conn:
            asset = conn.execute("""SELECT c.metadata_json FROM media_assets a JOIN feed_content c
                ON c.content_id=a.content_id WHERE a.asset_id=? AND a.status='ready' AND c.status='admitted'""",(payload["asset_id"],)).fetchone()
        if asset:
            return ContentEmbeddingService(str(DEFAULT_DB_PATH),user_id=job["user_id"]).ensure_text_embeddings(
                [Track.from_dict(json.loads(asset["metadata_json"]))])
        return {"ready":False,"contentId":payload["content_id"]}
    if job["kind"] == "behavior":
        if os.getenv("AMEM_TRANSPORT", "embedded").lower() == "grpc":
            from amem_grpc_bridge import AmemGrpcBridge

            bridge = AmemGrpcBridge.from_env()
            try:
                return bridge.record_behavior(payload)
            finally:
                bridge.close()
        bridge, _projector = _worker_memory()
        return bridge.record_behavior(payload)
    if job["kind"] == "sse":
        from sse_event_client import SSEEventPublisher

        publisher = SSEEventPublisher()
        try:
            return publisher.publish_http(**payload) if publisher.url else publisher.publish(**payload)
        finally:
            publisher.close()

    # Never deserialize request cookies, Python service instances or caller-owned
    # paths from broker payloads. Identity and input come from the durable row.
    from service_factory import MusicServices

    user_id = job["user_id"]
    memory = (
        None if os.getenv("AMEM_TRANSPORT", "embedded").lower() == "grpc"
        else _worker_memory()
    )
    with MusicServices(user_id=user_id, amem_runtime=memory) as services:
        if job["kind"] in {"discovery", "evolution"}:
            from request_spec import RequestSpec

            service = services.discovery
            if job["kind"] == "evolution":
                if service.keyword_governance.evolution_due():
                    service._run_evolution(payload["blocked_topics"])
                return None
            service._run_job(
                job["job_id"],
                payload["queries"],
                payload["negative_queries"],
                payload["keyword_specs"],
                payload["negative_keyword_specs"],
                RequestSpec.from_dict(payload["request_spec"]),
                payload["limit"],
            )
            status = service.job_status(job["job_id"])
            if status["status"] == "failed":
                raise RuntimeError("DiscoveryFailed")
            return status
        if job["kind"] in {"dialogue", "discovery_watch"}:
            from dialogue_task_service import DialogueTaskService
            from sse_event_client import SSEEventPublisher

            service = services.dialogue
            publisher = SSEEventPublisher()
            tasks = DialogueTaskService(publisher)
            try:
                if job["kind"] == "dialogue":
                    return tasks._run(
                        service=service, user_id=user_id, task_id=job["job_id"], **payload
                    )
                return tasks._watch_discovery(
                    service, user_id, payload["task_id"], payload["session_id"], payload["initial"]
                )
            finally:
                tasks.close()
        raise ValueError("unknown durable job kind")
