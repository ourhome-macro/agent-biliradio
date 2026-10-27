from __future__ import annotations

import threading
import time
from uuid import uuid4

from confluent_kafka import TopicPartition
from database import get_connection
from durable_jobs import enqueue
from kafka_transport import KafkaJobBus, KafkaSettings
from kafka_worker import JobProcessor, KafkaConsumerLoop

from .common import save
from .infra import Kafka


def run(directory, output, db, *, quick=False):
    server = Kafka()
    bus, worker, thread, observer = None, None, None, None
    try:
        bus = KafkaJobBus(db, KafkaSettings(server.bootstrap, "perf." + uuid4().hex[:12], 3, 1))
        bus.provision()
        rates, seconds = [2, 10, 40], 2 if quick else 4
        count = sum(rates) * seconds
        ids = []
        with get_connection(db) as conn:
            for index in range(count):
                ids.append(
                    enqueue(
                        conn,
                        kind="behavior",
                        user_id="legacy-owner",
                        job_id="perf-behavior-" + str(index),
                        payload={"eventId": str(index)},
                        retry_safe=True,
                    )
                )
        # Work is deliberately bounded and fixed, not a private AMEM/model call.
        processed = set()
        lock = threading.Lock()

        def dispatch(job, **kwargs):
            time.sleep(0.02)
            with lock:
                processed.add(job["job_id"])
            return {"ok": True}

        group = bus.settings.topic_prefix + ".load"
        worker = KafkaConsumerLoop(
            bus,
            group=group,
            lanes=("events",),
            processor=JobProcessor(bus, dispatcher=dispatch),
            concurrency=2,
        )
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        assigned_by = time.monotonic() + 15
        while not worker.epochs and time.monotonic() < assigned_by:
            time.sleep(0.1)
        with get_connection(db) as conn:
            warm = enqueue(
                conn,
                kind="behavior",
                user_id="legacy-owner",
                job_id="perf-warmup",
                payload={"eventId": "warmup"},
                retry_safe=True,
            )
        bus.publish(warm, "behavior")
        warm_deadline = time.monotonic() + 20
        while time.monotonic() < warm_deadline:
            with get_connection(db) as conn:
                status = conn.execute(
                    "SELECT status FROM durable_jobs WHERE job_id=?", (warm,)
                ).fetchone()[0]
            if status == "completed":
                break
            time.sleep(0.1)
        if status != "completed":
            raise RuntimeError("Kafka consumer warm-up did not complete")
        processed.clear()
        observer = bus.consumer(group)
        topic = bus.settings.topic("events")
        partitions = [TopicPartition(topic, i) for i in range(3)]

        def lag():
            committed = observer.committed(partitions, timeout=5)
            total = 0
            for part in committed:
                _low, high = observer.get_watermark_offsets(part, timeout=3, cached=False)
                total += max(0, high - max(0, part.offset))
            return total

        samples, stages = [], []
        sampler_stop = threading.Event()
        last_lag = [None]
        sample_errors = []

        def sample():
            while not sampler_stop.is_set():
                try:
                    remaining = lag()
                    last_lag[0] = remaining
                    samples.append(
                        {
                            "seconds": time.monotonic() - start,
                            "lag": remaining,
                            "processed": len(processed),
                        }
                    )
                except Exception as error:
                    sample_errors.append(type(error).__name__)
                sampler_stop.wait(0.25)

        offset = 0
        start = time.monotonic()
        sampling = threading.Thread(target=sample, daemon=True)
        sampling.start()
        for rate in rates:
            phase_start = time.monotonic()
            delivered = 0
            for index in range(rate * seconds):
                delay = phase_start + index / rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                bus.publish(ids[offset], "behavior")
                delivered += 1
                offset += 1
            elapsed = time.monotonic() - phase_start
            stages.append(
                {
                    "offeredRps": rate,
                    "produced": delivered,
                    "elapsedSeconds": elapsed,
                    "achievedProducerRps": delivered / elapsed,
                    "lagAtEnd": last_lag[0],
                }
            )
        stop_time = time.monotonic()
        while time.monotonic() - stop_time < 90:
            remaining = last_lag[0]
            if remaining == 0 and len(processed) == count:
                break
            time.sleep(0.25)
        sampler_stop.set()
        sampling.join(timeout=10)
        result = {
            "stages": stages,
            "peakLag": max((v["lag"] for v in samples), default=0),
            "recoverySeconds": time.monotonic() - stop_time,
            "remainingLag": last_lag[0],
            "lagSampleErrors": sample_errors,
            "uniqueProcessed": len(processed),
            "expected": count,
            "consumerThroughputPerSecond": len(processed) / (time.monotonic() - start),
            "scope": (
                "real Kafka + MySQL JobProcessor; controlled 20ms handler; excludes real AMEM cost"
            ),
            "group": "isolated per-run group",
            "partitions": 3,
            "workerConcurrency": 2,
        }
        save(output / "raw-kafka-lag.json", samples)
        return result
    finally:
        if worker:
            worker.stop.set()
        if thread:
            thread.join(timeout=15)
        if observer:
            observer.close()
        if bus:
            bus.producer.flush(5)
        server.close()
