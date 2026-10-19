"""Real Kafka/MySQL acceptance; private Docker credentials never appear in pytest repr."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from confluent_kafka import TopicPartition
from database import get_connection, init_db
from domain_consumer import DomainEventConsumer
from domain_outbox import claim_events, complete_publish, enqueue_event
from dotenv import dotenv_values
from durable_jobs import claim_publish_batch, complete_publication, enqueue, publish_pending
from kafka_transport import KafkaJobBus, KafkaSettings
from kafka_worker import JobProcessor, KafkaConsumerLoop
from mysql_storage.connection import engine_for


class SecretURL(str):
    def __repr__(self):
        return repr(sa.engine.make_url(self).render_as_string(hide_password=True))


@pytest.fixture
def case(tmp_path, monkeypatch, request):
    private = Path(__file__).resolve().parents[2] / "server-data/kafka-acceptance/private.env"
    if not private.is_file():
        pytest.skip("Isolated Docker Kafka/MySQL credentials are not configured")
    values = dotenv_values(private)
    root = sa.engine.make_url(values["KAFKA_TEST_ROOT_DATABASE_URL"])
    assert root.port not in {3306, 3307}, (
        "Never run transport acceptance against existing host MySQL"
    )
    database = "radio_kafka_" + uuid4().hex[:14]
    admin = engine_for(SecretURL(root.set(database=None).render_as_string(hide_password=False)))
    with admin.begin() as connection:
        connection.execute(sa.schema.CreateSchema(database))
    url = SecretURL(root.set(database=database).render_as_string(hide_password=False))
    for key, value in {
        "DATABASE_URL": url,
        "BUSINESS_DATABASE_URL": "",
        "APP_DATA_DIR": str(tmp_path),
        "AUTH_KEY_PATH": str(tmp_path / "auth.key"),
        "JOB_TRANSPORT": "kafka",
        "AMEM_ENABLED": "false",
        "KAFKA_BOOTSTRAP_SERVERS": values.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
    }.items():
        monkeypatch.setenv(key, str(value))
    if getattr(request, "param", None) == "radio_008":
        from mysql_storage.schema import install_schema

        install_schema(engine_for(url), revision="radio_008")
    else:
        init_db(url)
    prefix = "radio.test." + uuid4().hex[:10]
    bus = None
    if getattr(request, "param", None) not in {"sql_only", "radio_008"}:
        bus = KafkaJobBus(url, KafkaSettings(os.environ["KAFKA_BOOTSTRAP_SERVERS"], prefix, 3, 1))
        bus.provision()
    value = SimpleNamespace(url=url, bus=bus, prefix=prefix, admin=admin, database=database)
    try:
        yield value
    finally:
        if bus:
            bus.producer.flush(5)
        engine_for(url).dispose()
        with admin.begin() as connection:
            connection.execute(sa.schema.DropSchema(database))


def job(case, name, *, lane="", retry_safe=True):
    with get_connection(case.url) as connection:
        return enqueue(
            connection,
            kind="fixture",
            user_id="legacy-owner",
            payload={"name": name},
            job_id=name,
            lane=lane,
            retry_safe=retry_safe,
        )


def status(case, name):
    with get_connection(case.url) as connection:
        return dict(
            connection.execute("SELECT * FROM durable_jobs WHERE job_id=?", (name,)).fetchone()
        )


def pump(workers, predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for worker in workers:
            worker.poll_once(0.05)
        if predicate():
            return
    raise AssertionError("Real Kafka acceptance deadline exceeded")


def committed(worker, delivery):
    partition = worker.consumer.committed(
        [TopicPartition(delivery["topic"], delivery["partition"])], timeout=3
    )[0]
    return partition.offset >= delivery["offset"] + 1


@pytest.mark.parametrize("case", ["sql_only"], indirect=True)
def test_mysql_skip_locked_publication_and_old_token_fencing(case):
    for index in range(24):
        job(case, f"job-{index}")
    with ThreadPoolExecutor(max_workers=4) as executor:
        batches = list(executor.map(lambda _: claim_publish_batch(case.url, limit=6), range(4)))
    rows = [row for batch in batches for row in batch]
    assert len(rows) == len({row["job_id"] for row in rows}) == 24
    held_id = rows[0]["job_id"]
    with get_connection(case.url) as connection:
        connection.execute("UPDATE durable_jobs SET next_publish_at=0,publish_lease_until=0")
    with engine_for(case.url).begin() as held:
        held.execute(
            sa.text("SELECT job_id FROM durable_jobs WHERE job_id=:id FOR UPDATE"), {"id": held_id}
        )
        started = time.monotonic()
        another = claim_publish_batch(case.url, limit=24)
        assert time.monotonic() - started < 3
        assert held_id not in {row["job_id"] for row in another}
    assert not complete_publication(case.url, rows[1])


def test_broker_duplicate_delivery_has_one_sql_execution_and_commits_after_completion(case):
    job(case, "duplicate")
    deliveries = [case.bus.publish("duplicate", "fixture") for _ in range(2)]
    calls = []
    processor = JobProcessor(
        case.bus, dispatcher=lambda value: calls.append(value["job_id"]) or {"ok": True}
    )
    worker = KafkaConsumerLoop(
        case.bus, group=case.prefix + ".duplicates", lanes=("jobs",), processor=processor
    )
    try:
        pump([worker], lambda: committed(worker, deliveries[-1]))
        assert status(case, "duplicate")["status"] == "completed"
        assert calls == ["duplicate"]
    finally:
        worker.close()


def test_failed_attempt_is_durably_retryable_before_offset_and_lane_order_is_sql_authority(case):
    job(case, "first", lane="same-lane")
    job(case, "second", lane="same-lane")
    assert publish_pending(case.url, case.bus.publish) == 1
    calls = []

    def execute(value):
        calls.append(value["job_id"])
        if len(calls) == 1:
            raise RuntimeError("safe retry fixture")
        return {"ok": True}

    worker = KafkaConsumerLoop(
        case.bus,
        group=case.prefix + ".retry",
        lanes=("jobs",),
        processor=JobProcessor(case.bus, dispatcher=execute),
    )
    try:
        pump(
            [worker],
            lambda: (
                status(case, "first")["status"] == "queued"
                and status(case, "first")["attempts"] == 1
            ),
        )
        # No second-lane item may overtake a durably queued retry of first.
        assert status(case, "second")["attempts"] == 0
        pump([worker], lambda: not worker.inflight)
        with get_connection(case.url) as connection:
            connection.execute(
                "UPDATE durable_jobs SET available_at=0,next_publish_at=0 WHERE job_id='first'"
            )
        assert publish_pending(case.url, case.bus.publish) == 1
        pump([worker], lambda: status(case, "first")["status"] == "completed")
        assert publish_pending(case.url, case.bus.publish) == 1
        pump([worker], lambda: status(case, "second")["status"] == "completed")
        assert calls == ["first", "first", "second"]
    finally:
        worker.close()


def test_real_rebalance_never_commits_old_assignment_or_old_execution_token(case):
    job(case, "rebalanced")
    delivery = case.bus.publish("rebalanced", "fixture")
    entered, release, fresh_entered, fresh_release = [threading.Event() for _ in range(4)]

    def old_dispatch(_job):
        entered.set()
        assert release.wait(15)
        return {"old": True}

    def fresh_dispatch(_job):
        fresh_entered.set()
        assert fresh_release.wait(15)
        return {"new": True}

    group = case.prefix + ".rebalance"
    first = KafkaConsumerLoop(
        case.bus,
        group=group,
        lanes=("jobs",),
        processor=JobProcessor(case.bus, dispatcher=old_dispatch, lease_seconds=1),
    )
    second = None
    try:
        pump([first], entered.is_set)
        old_token = status(case, "rebalanced")["lease_token"]
        second = KafkaConsumerLoop(
            case.bus,
            group=group,
            lanes=("jobs",),
            processor=JobProcessor(case.bus, dispatcher=fresh_dispatch, lease_seconds=1),
        )
        first.consumer.unsubscribe()
        pump([first, second], lambda: not first.epochs)
        release.set()
        pump([first, second], fresh_entered.is_set)
        assert status(case, "rebalanced")["lease_token"] != old_token
        assert not committed(second, delivery)
        fresh_release.set()
        pump([second], lambda: committed(second, delivery))
        assert json.loads(status(case, "rebalanced")["result_json"]) == {"new": True}
    finally:
        release.set()
        fresh_release.set()
        first.close()
        if second:
            second.close()


def test_domain_inbox_sql_failure_does_not_commit_offset_and_external_prepare_can_repeat(case):
    with get_connection(case.url) as connection:
        event_id = enqueue_event(
            connection,
            aggregate_type="content",
            aggregate_id="test-content",
            event_type="content.updated",
            payload={"version": 1},
        )
        envelope = json.loads(
            connection.execute(
                "SELECT event_json FROM domain_event_outbox WHERE event_id=?", (event_id,)
            ).fetchone()[0]
        )
    delivery = case.bus.publish_domain(envelope)
    failed, prepared = threading.Event(), []

    def handler(_connection, _envelope):
        if not failed.is_set():
            raise RuntimeError("SQL commit path unavailable")

    consumer = DomainEventConsumer(
        case.bus,
        consumer_name="acceptance",
        handler=handler,
        prepare=lambda event: prepared.append(event["eventId"]),
    )
    worker = consumer.worker()
    try:
        pump([worker], lambda: len(prepared) >= 1)
        assert not committed(worker, delivery)
        with get_connection(case.url) as connection:
            assert connection.execute("SELECT COUNT(*) FROM domain_event_inbox").fetchone()[0] == 0
        failed.set()
        pump([worker], lambda: committed(worker, delivery))
        prior = len(prepared)
        duplicate = case.bus.publish_domain(envelope)
        pump([worker], lambda: committed(worker, duplicate))
        assert len(prepared) == prior
        with get_connection(case.url) as connection:
            assert connection.execute("SELECT COUNT(*) FROM domain_event_inbox").fetchone()[0] == 1
    finally:
        failed.set()
        worker.close()


def test_domain_broker_ack_before_sql_commit_replays_without_losing_event(case):
    with get_connection(case.url) as connection:
        enqueue_event(
            connection,
            aggregate_type="content",
            aggregate_id="crash",
            event_type="content.updated",
            payload={},
        )
    old = claim_events(case.url, now=time.time() - 31)[0]
    envelope = json.loads(old["event_json"])
    first = case.bus.publish_domain(envelope)
    fresh = claim_events(case.url)[0]
    duplicate = case.bus.publish_domain(envelope)
    assert not complete_publish(case.url, old, first)
    assert complete_publish(case.url, fresh, duplicate)
    effects = []
    worker = DomainEventConsumer(
        case.bus, consumer_name="crash-check", handler=lambda _c, e: effects.append(e["eventId"])
    ).worker()
    try:
        pump([worker], lambda: committed(worker, duplicate))
        assert effects == [envelope["eventId"]]
    finally:
        worker.close()


@pytest.mark.parametrize("case", ["radio_008"], indirect=True)
def test_mysql_008_to_009_requires_explicit_migration_and_preserves_facts(case):
    from mysql_storage.connection import ensure_mysql
    from mysql_storage.upgrades import upgrade_mysql
    from schema_migrations import HEAD

    engine = engine_for(case.url)
    with engine.begin() as connection:
        connection.execute(
            sa.text("""INSERT INTO settings(user_id,`key`,value,updated_at)
            VALUES ('legacy-owner','migration-proof','keep','now')""")
        )
    with pytest.raises(RuntimeError, match="administrator"):
        ensure_mysql(case.url, migrate=True)
    assert not sa.inspect(engine).has_table("domain_event_outbox")
    upgrade_mysql(engine, HEAD)
    upgrade_mysql(engine, HEAD)
    init_db(case.url)
    with get_connection(case.url) as connection:
        assert (
            connection.execute("SELECT value FROM settings WHERE key='migration-proof'").fetchone()[
                0
            ]
            == "keep"
        )
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone()[0] == HEAD
    fields = {column["name"]: column for column in sa.inspect(engine).get_columns("feed_content")}
    assert fields["track_id"]["nullable"] and "permission_version" in fields


def test_parallel_domain_relays_preserve_per_aggregate_order_on_real_broker(case):
    for index in range(8):
        for revision in (1, 2):
            with get_connection(case.url) as connection:
                enqueue_event(
                    connection,
                    aggregate_type="content",
                    aggregate_id=f"parallel-{index}",
                    event_type="content.updated",
                    payload={"version": revision},
                )
    from domain_outbox import DomainOutboxRelay

    relay = DomainOutboxRelay(case.url, case.bus)
    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sum(executor.map(lambda _: relay.run_once(limit=4), range(2))) == 8
        assert sum(executor.map(lambda _: relay.run_once(limit=4), range(2))) == 8
    applied = {}

    def apply(_connection, event):
        applied.setdefault(event["aggregateId"], []).append(event["sequence"])

    worker = DomainEventConsumer(case.bus, consumer_name="parallel-relay", handler=apply).worker()
    try:
        pump([worker], lambda: sum(map(len, applied.values())) == 16)
        assert all(sequence == [1, 2] for sequence in applied.values())
        with get_connection(case.url) as connection:
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM domain_event_outbox WHERE status='published'"
                ).fetchone()[0]
                == 16
            )
    finally:
        worker.close()
