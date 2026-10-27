"""Per-partition execution with explicit SQL-before-offset acknowledgement."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from uuid import uuid4

from confluent_kafka import TopicPartition
from database import DEFAULT_DB_PATH, get_connection, init_db
from durable_jobs import claim, finish
from kafka_transport import KafkaJobBus, lane_for

LOGGER = logging.getLogger("radio.kafka-worker")


def decode_message(message):
    raw = message.value()
    if not isinstance(raw, bytes) or len(raw) > 1024 * 1024:
        raise ValueError("Invalid Kafka message size")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Kafka envelope must be an object")
    return value


def reject_delivery(db_path, consumer_name, message, reason):
    with get_connection(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO kafka_delivery_rejections
            (consumer_name,topic,partition_id,offset_id,payload_hash,error_type,recorded_at)
            VALUES (?,?,?,?,?,?,?)""",
            (
                consumer_name,
                message.topic(),
                message.partition(),
                message.offset(),
                hashlib.sha256(message.value() or b"").hexdigest(),
                reason,
                time.time(),
            ),
        )
    return True


class JobProcessor:
    def __init__(self, bus, *, dispatcher=None, lease_seconds=120, consumer_name="jobs-v1"):
        self.bus, self.dispatcher = bus, dispatcher
        self.lease_seconds, self.consumer_name = lease_seconds, consumer_name

    def __call__(self, message, cancelled):
        if cancelled.is_set():
            return False
        try:
            envelope = decode_message(message)
            job_id = envelope["jobId"]
            if (
                envelope.get("schemaVersion") != "radio.job.v1"
                or not isinstance(job_id, str)
                or len(job_id) > 180
            ):
                raise ValueError("Invalid job envelope")
        except (ValueError, KeyError, TypeError, UnicodeError):
            return reject_delivery(self.bus.db_path, self.consumer_name, message, "InvalidEnvelope")
        with get_connection(self.bus.db_path) as conn:
            row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
        if (
            not row
            or row["input_hash"] != envelope.get("inputHash")
            or row["kind"] != envelope.get("kind")
            or message.topic() != self.bus.settings.topic(lane_for(row["kind"]))
        ):
            return reject_delivery(
                self.bus.db_path, self.consumer_name, message, "AuthorityMismatch"
            )
        if row['status'] in {'completed','failed','needs_reconciliation'}:
            return True  # A batch may have durably finished this job before delivery.
        with get_connection(self.bus.db_path) as conn:
            job = claim(conn, job_id, lease_seconds=self.lease_seconds)
        if job is None:
            with get_connection(self.bus.db_path) as conn:
                state = conn.execute(
                    "SELECT status FROM durable_jobs WHERE job_id=?", (job_id,)
                ).fetchone()[0]
            return state in {"completed", "failed", "needs_reconciliation"}
        stop = threading.Event()

        def renew():
            while not stop.wait(min(10, self.lease_seconds / 3)):
                if cancelled.is_set():
                    return
                try:
                    now = time.time()
                    with get_connection(self.bus.db_path) as conn:
                        changed = conn.execute(
                            """UPDATE durable_jobs SET lease_until=?,updated_at=?
                            WHERE job_id=? AND lease_token=?
                            AND status='running' AND lease_until>?""",
                            (now + self.lease_seconds, now, job_id, job["lease_token"], now),
                        ).rowcount
                    if not changed:
                        cancelled.set()
                        return
                except Exception:
                    # Without proven ownership the task cannot safely continue.
                    cancelled.set()
                    return

        pulse = threading.Thread(target=renew, daemon=True)
        pulse.start()
        from music_agent import current_job_id

        token = current_job_id.set(job_id)
        result, error = None, None
        from agent_memory_runtime.telemetry import flush, span

        try:
            with span(
                "task.kafka.execute",
                parent=json.loads(job["trace_context"]),
                attributes={
                    "job.id": job_id,
                    "job.kind": job["kind"],
                    "messaging.kafka.partition": message.partition(),
                    "messaging.kafka.offset": message.offset(),
                },
            ) as current:
                try:
                    if self.dispatcher:
                        result = self.dispatcher(job)
                    elif job['kind']=='feed_projection':
                        from feed_runtime.projection_batch import execute
                        result = execute(self.bus.db_path,job,cancelled=cancelled)
                    elif job["kind"] in {
                        "media_import",
                        "dialogue",
                        "discovery",
                        "evolution",
                        "discovery_watch",
                    }:
                        from isolated_job import dispatch_isolated

                        result = dispatch_isolated(
                            job, cancel_event=cancelled, db_path=self.bus.db_path
                        )
                    else:
                        from task_app import dispatch

                        result = dispatch(job, db_path=self.bus.db_path)
                except Exception as caught:
                    from opentelemetry.trace import Status, StatusCode

                    current.set_status(Status(StatusCode.ERROR))
                    current.set_attribute("error.type", type(caught).__name__)
                    error = caught
            if cancelled.is_set():
                # No offset and no old-token completion after assignment/lease loss.
                return False
            with get_connection(self.bus.db_path) as conn:
                finish(conn, job, result=result, error=error)
                state = conn.execute(
                    "SELECT status FROM durable_jobs WHERE job_id=?", (job_id,)
                ).fetchone()[0]
            return state in {"completed", "failed", "needs_reconciliation", "queued"}
        finally:
            current_job_id.reset(token)
            stop.set()
            pulse.join(timeout=2)
            flush()


@dataclass
class _Ticket:
    message: object
    epoch: int
    cancelled: threading.Event
    future: object
    retry_at: float = 0


class KafkaConsumerLoop:
    """Only the poll thread owns the Kafka Consumer and offset commits."""

    def __init__(self, bus, *, group, lanes, processor, concurrency=2, consumer=None):
        self.bus, self.group, self.lanes, self.processor = bus, group, tuple(lanes), processor
        self.consumer = consumer or bus.consumer(group)
        self.pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="kafka-domain")
        self.stop = threading.Event()
        self.epochs, self.inflight = {}, {}
        self.epoch = 0
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"
        self.last_heartbeat = 0
        self.consumer.subscribe(
            [bus.settings.topic(lane) for lane in self.lanes],
            on_assign=self._assign,
            on_revoke=self._revoke,
            on_lost=self._revoke,
        )

    def _assign(self, consumer, partitions):
        self.epoch += 1
        for partition in partitions:
            self.epochs[(partition.topic, partition.partition)] = self.epoch

    def _revoke(self, consumer, partitions):
        for partition in partitions:
            key = partition.topic, partition.partition
            self.epochs.pop(key, None)
            ticket = self.inflight.pop(key, None)
            if ticket:
                ticket.cancelled.set()
                ticket.future.cancel()

    def _heartbeat(self):
        now = time.time()
        if now - self.last_heartbeat < 5:
            return
        self.last_heartbeat = now
        with get_connection(self.bus.db_path) as conn:
            conn.execute(
                """INSERT INTO job_worker_leases
                (worker_id,transport,lanes_json,lease_until,updated_at) VALUES (?,'kafka',?,?,?)
                ON CONFLICT(worker_id) DO UPDATE SET lanes_json=excluded.lanes_json,
                lease_until=excluded.lease_until,updated_at=excluded.updated_at""",
                (self.worker_id, json.dumps(self.lanes), now + 15, now),
            )

    def poll_once(self, timeout=0.1):
        try:
            self._heartbeat()
        except Exception as error:
            LOGGER.warning("Kafka worker SQL heartbeat deferred: %s", type(error).__name__)
        completed = []
        for key, ticket in list(self.inflight.items()):
            if self.epochs.get(key) != ticket.epoch:
                continue
            if ticket.cancelled.is_set():
                if ticket.future.done():
                    ticket.cancelled = threading.Event()
                    ticket.future = self.pool.submit(
                        self.processor, ticket.message, ticket.cancelled
                    )
                    ticket.retry_at = time.monotonic() + 1
                continue
            if not ticket.future.done() or ticket.retry_at > time.monotonic():
                continue
            try:
                durable = ticket.future.result()
            except Exception as error:
                LOGGER.warning("Kafka processing deferred: %s", type(error).__name__)
                durable = False
            if durable:
                completed.append((key, ticket))
            else:
                ticket.retry_at = time.monotonic() + 1
                ticket.future = self.pool.submit(self.processor, ticket.message, ticket.cancelled)
        ready = [
            (key, ticket)
            for key, ticket in completed
            if self.inflight.get(key) is ticket
            and self.epochs.get(key) == ticket.epoch
            and not ticket.cancelled.is_set()
        ]
        if ready:
            try:
                acknowledgements = self.consumer.commit(
                    offsets=[
                        TopicPartition(key[0], key[1], ticket.message.offset() + 1)
                        for key, ticket in ready
                    ],
                    asynchronous=False,
                )
            except Exception as error:
                LOGGER.warning("Kafka offset batch commit deferred: %s", type(error).__name__)
                for _key, ticket in ready:
                    ticket.retry_at = time.monotonic() + 1
            else:
                failed = {
                    (part.topic, part.partition)
                    for part in (acknowledgements or [])
                    if part.error is not None
                }
                resume = []
                for key, ticket in ready:
                    if key in failed:
                        ticket.retry_at = time.monotonic() + 1
                        continue
                    if self.inflight.get(key) is ticket and self.epochs.get(key) == ticket.epoch:
                        self.inflight.pop(key, None)
                        resume.append(TopicPartition(*key))
                if resume:
                    self.consumer.resume(resume)
        # Paused partitions cannot wake poll when their SQL future completes.
        # Check promptly while busy; retain the idle poll budget and group callbacks.
        message = self.consumer.poll(min(timeout, 0.01) if self.inflight else timeout)
        if message is None or message.error():
            return
        key = message.topic(), message.partition()
        if key not in self.epochs:
            return
        if key in self.inflight:
            # pause() should drain no additional records; seek preserves a rare
            # already-queued delivery without ever advancing its offset.
            self.consumer.seek(TopicPartition(*key, message.offset()))
            return
        self.consumer.pause([TopicPartition(*key)])
        cancelled = threading.Event()
        self.inflight[key] = _Ticket(
            message,
            self.epochs[key],
            cancelled,
            self.pool.submit(self.processor, message, cancelled),
        )

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    self.poll_once()
                except Exception as error:
                    LOGGER.warning("Kafka consumer retry: %s", type(error).__name__)
                    self.stop.wait(0.5)
        finally:
            self.close()

    def close(self):
        self.stop.set()
        for ticket in self.inflight.values():
            ticket.cancelled.set()
            ticket.future.cancel()
        self.consumer.close()
        self.pool.shutdown(wait=True, cancel_futures=True)
        try:
            with get_connection(self.bus.db_path) as conn:
                conn.execute("DELETE FROM job_worker_leases WHERE worker_id=?", (self.worker_id,))
        except Exception:
            LOGGER.warning("Kafka worker heartbeat will expire after SQL outage")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Kafka jobs with SQL leases and manual offsets")
    parser.add_argument("--lanes", default="jobs,events")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--provision", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 16:
        parser.error("concurrency must be between1 and16")
    os.environ["JOB_TRANSPORT"] = "kafka"
    init_db()
    from telemetry_setup import setup

    setup("radio-kafka-worker")
    bus = KafkaJobBus(DEFAULT_DB_PATH)
    if args.provision:
        bus.provision()
        return
    lanes = tuple(args.lanes.split(","))
    processor = JobProcessor(bus)
    worker = KafkaConsumerLoop(
        bus,
        group=f"{bus.settings.topic_prefix}.worker.{args.lanes}",
        lanes=lanes,
        processor=processor,
        concurrency=args.concurrency,
    )
    from task_app import start_runtime_services, stop_runtime_services

    start_runtime_services()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: worker.stop.set())
    logging.basicConfig(level=logging.INFO)
    try:
        worker.run()
    finally:
        stop_runtime_services()


if __name__ == "__main__":
    main()
