"""At-least-once Streams delivery backed by the existing SQLite job journal."""

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
from uuid import uuid4

from database import DEFAULT_DB_PATH, database_identity, get_connection, init_db
from durable_jobs import claim, finish, publish_pending
from redis import Redis
from redis.exceptions import ResponseError

LOGGER = logging.getLogger("radio.redis-stream")
_PUBLISH = """
local old=redis.call('get',KEYS[2])
if old and #redis.call('xrange',KEYS[1],old,old)>0 then return old end
local id=redis.call('xadd',KEYS[1],'*','job_id',ARGV[1],'kind',ARGV[2],'input_hash',ARGV[3])
redis.call('set',KEYS[2],id)
return id
"""
_ACK = """
local pending=redis.call('xpending',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #pending==0 or pending[1][2]~=ARGV[3] then return 0 end
redis.call('xack',KEYS[1],ARGV[1],ARGV[2])
redis.call('xdel',KEYS[1],ARGV[2])
if redis.call('get',KEYS[2])==ARGV[2] then redis.call('del',KEYS[2]) end
return 1
"""
_TOUCH = """
local pending=redis.call('xpending',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #pending==0 or pending[1][2]~=ARGV[3] then return 0 end
redis.call('xclaim',KEYS[1],ARGV[1],ARGV[3],0,ARGV[2],'JUSTID');return 1
"""


def lane_for(kind):
    return (
        "media"
        if kind == "media_import"
        else "events"
        if kind in {"behavior", "sse", "feed_projection", "feed_asset_ready"}
        else "jobs"
    )


class RedisJobBus:
    group = "workers.v1"

    def __init__(self, db_path, url=None, *, client=None):
        self.db_path = str(db_path)
        identity = database_identity(db_path)[:12]
        self.prefix = f"radio:queue:{identity}"
        self.redis = client or Redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=1,
            max_connections=32,
        )

    def key(self, lane):
        if lane not in {"jobs", "events", "media"}:
            raise ValueError("Unknown stream lane")
        return f"{self.prefix}:{lane}"

    def delivery_key(self, job_id):
        return f"{self.prefix}:delivery:{hashlib.sha256(job_id.encode()).hexdigest()}"

    def ensure_groups(self, lanes):
        version = tuple(int(n) for n in self.redis.info("server")["redis_version"].split(".")[:2])
        if version < (6, 2):
            raise RuntimeError("Redis 6.2+ required for Streams pending recovery")
        for lane in lanes:
            try:
                self.redis.xgroup_create(self.key(lane), self.group, id="0-0", mkstream=True)
            except ResponseError as error:
                if "BUSYGROUP" not in str(error):
                    raise

    def publish(self, job_id, kind):
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT kind,input_hash FROM durable_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        if not row or row["kind"] != kind:
            raise ValueError("Job metadata does not match durable authority")
        from agent_memory_runtime.telemetry import span

        with span("messaging.redis.publish", attributes={"job.id": job_id, "job.kind": kind}):
            return self.redis.eval(
                _PUBLISH,
                2,
                self.key(lane_for(kind)),
                self.delivery_key(job_id),
                job_id,
                kind,
                row["input_hash"],
            )

    def acknowledge(self, lane, message_id, consumer, job_id):
        return bool(
            self.redis.eval(
                _ACK, 2, self.key(lane), self.delivery_key(job_id), self.group, message_id, consumer
            )
        )

    def touch(self, lane, message_id, consumer):
        return bool(self.redis.eval(_TOUCH, 1, self.key(lane), self.group, message_id, consumer))

    def recover(self, lane, consumer, *, idle_ms=30000, start="0-0"):
        return self.redis.xautoclaim(self.key(lane), self.group, consumer, idle_ms, start, count=1)

    def read(self, lanes, consumer, *, block_ms=1000):
        values = self.redis.xreadgroup(
            self.group, consumer, {self.key(lane): ">" for lane in lanes}, count=1, block=block_ms
        )
        return [
            (key.rsplit(":", 1)[1], message_id, fields)
            for key, items in values
            for message_id, fields in items
        ]


class RedisJobWorker:
    def __init__(self, bus, *, lanes=("jobs", "events"), dispatcher=None, lease_seconds=120):
        self.bus, self.lanes, self.lease_seconds = bus, tuple(lanes), lease_seconds
        if dispatcher is None:
            from isolated_job import dispatch_isolated
            from task_app import dispatch

            def dispatcher(job):
                return (
                    dispatch_isolated(job)
                    if job["kind"]
                    in {"media_import", "dialogue", "discovery", "evolution", "discovery_watch"}
                    else dispatch(job)
                )

        self.dispatch = dispatcher
        self.stop = threading.Event()

    def process(self, lane, message_id, fields, consumer):
        job_id = fields.get("job_id", "")
        with get_connection(self.bus.db_path) as conn:
            row = conn.execute("SELECT * FROM durable_jobs WHERE job_id=?", (job_id,)).fetchone()
        if (
            not row
            or row["input_hash"] != fields.get("input_hash")
            or lane_for(row["kind"]) != lane
        ):
            # Untrusted broker metadata is never executed or merged into SQL inputs.
            self.bus.acknowledge(lane, message_id, consumer, job_id)
            return "invalid"
        with get_connection(self.bus.db_path) as conn:
            job = claim(conn, job_id, lease_seconds=self.lease_seconds)
        if job is None:
            with get_connection(self.bus.db_path) as conn:
                status = conn.execute(
                    "SELECT status FROM durable_jobs WHERE job_id=?", (job_id,)
                ).fetchone()[0]
            if status in {"completed", "failed", "needs_reconciliation"}:
                self.bus.acknowledge(lane, message_id, consumer, job_id)
            return "deferred_or_terminal"
        pulse_stop = threading.Event()

        def renew():
            while not pulse_stop.wait(min(10, self.lease_seconds / 3)):
                try:
                    with get_connection(self.bus.db_path) as conn:
                        changed = conn.execute(
                            """UPDATE durable_jobs SET lease_until=?,updated_at=?
                            WHERE job_id=? AND lease_token=? AND status='running'""",
                            (
                                time.time() + self.lease_seconds,
                                time.time(),
                                job_id,
                                job["lease_token"],
                            ),
                        ).rowcount
                    if not changed:
                        return
                    self.bus.touch(lane, message_id, consumer)
                    self.bus.redis.set(
                        f"{self.bus.prefix}:worker:{consumer}",
                        json.dumps({"lanes": self.lanes, "pid": os.getpid()}),
                        ex=15,
                    )
                except Exception:
                    LOGGER.warning("Job heartbeat deferred", extra={"job_id": job_id})

        pulse = threading.Thread(target=renew, daemon=True)
        pulse.start()
        from music_agent import current_job_id

        from agent_memory_runtime.telemetry import flush, span

        token = current_job_id.set(job_id)
        try:
            error = None
            result = None
            with span(
                "task.redis.execute",
                parent=json.loads(job["trace_context"]),
                attributes={"job.id": job_id, "job.kind": job["kind"]},
            ) as current:
                try:
                    result = self.dispatch(job)
                except Exception as caught:
                    from opentelemetry.trace import Status, StatusCode

                    current.set_status(Status(StatusCode.ERROR))
                    current.set_attribute("error.type", type(caught).__name__)
                    error = caught
                    LOGGER.warning(
                        "Job execution failed: %s", type(caught).__name__, extra={"job_id": job_id}
                    )
            with get_connection(self.bus.db_path) as conn:
                finish(conn, job, result=result, error=error)
                status = conn.execute(
                    "SELECT status,lease_token FROM durable_jobs WHERE job_id=?", (job_id,)
                ).fetchone()
            # A retry is durably scheduled in SQL before acknowledging the old entry.
            if status["status"] != "running":
                self.bus.acknowledge(lane, message_id, consumer, job_id)
            return status["status"]
        finally:
            current_job_id.reset(token)
            pulse_stop.set()
            pulse.join(timeout=2)
            flush()

    def run(self):
        consumer = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"
        recovery = {lane: "0-0" for lane in self.lanes}
        while not self.stop.is_set():
            try:
                self.bus.ensure_groups(self.lanes)
                self.bus.redis.set(
                    f"{self.bus.prefix}:worker:{consumer}",
                    json.dumps({"lanes": self.lanes, "pid": os.getpid()}),
                    ex=15,
                )
                deliveries = []
                for lane in self.lanes:
                    recovered = self.bus.recover(lane, consumer, start=recovery[lane])
                    recovery[lane] = recovered[0]
                    deliveries.extend((lane, key, data) for key, data in recovered[1])
                if not deliveries:
                    deliveries = self.bus.read(self.lanes, consumer)
                for lane, key, fields in deliveries:
                    self.process(lane, key, fields, consumer)
            except ResponseError as error:
                LOGGER.warning("Stream service unavailable: %s", type(error).__name__)
                self.stop.wait(1)
            except Exception as error:
                LOGGER.warning("Stream worker retry: %s", type(error).__name__)
                self.stop.wait(1)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SQLite durable jobs over Redis Streams")
    parser.add_argument("--lanes", default="jobs,events")
    parser.add_argument("--concurrency", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 16:
        raise SystemExit("Concurrency must be between 1 and 16")
    os.environ["JOB_TRANSPORT"] = "redis_stream"
    init_db()
    from task_app import start_runtime_services, stop_runtime_services

    start_runtime_services()
    from telemetry_setup import setup

    setup("radio-redis-worker")
    bus = RedisJobBus(DEFAULT_DB_PATH, os.getenv("JOB_REDIS_URL", "redis://127.0.0.1:6379/1"))
    workers = [RedisJobWorker(bus, lanes=args.lanes.split(",")) for _ in range(args.concurrency)]
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())

    def publish():
        while not stop.is_set():
            try:
                from following.scheduler import periodic_tick
                periodic_tick(bus.db_path)
                publish_pending(bus.db_path, bus.publish)
            except Exception as error:
                LOGGER.warning("Outbox retry: %s", type(error).__name__)
            stop.wait(1)

    producer = threading.Thread(target=publish, daemon=True)
    producer.start()
    logging.basicConfig(level=logging.INFO)
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(worker.run) for worker in workers]
        try:
            while not stop.wait(1):
                for future in futures:
                    if future.done():
                        future.result()
                        raise RuntimeError("Stream consumer stopped unexpectedly")
        finally:
            for worker in workers:
                worker.stop.set()
            producer.join(timeout=3)
            stop_runtime_services()


if __name__ == "__main__":
    main()
