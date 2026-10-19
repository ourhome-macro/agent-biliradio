import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from database import get_connection, init_db
from domain_consumer import DomainEventConsumer
from domain_outbox import claim_events, complete_publish, encode, enqueue_event


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "events.sqlite3"
    init_db(path)
    return path


def add(db, aggregate="one", event_id=None):
    with get_connection(db) as connection:
        return enqueue_event(
            connection,
            aggregate_type="content",
            aggregate_id=aggregate,
            event_type="content.updated",
            payload={"version": 1},
            event_id=event_id,
        )


def test_domain_fact_rollback_and_event_id_binding(db):
    with pytest.raises(RuntimeError):
        with get_connection(db) as connection:
            enqueue_event(
                connection,
                aggregate_type="content",
                aggregate_id="rollback",
                event_type="content.updated",
                payload={},
                event_id="rollback",
            )
            raise RuntimeError("rollback")
    with get_connection(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM domain_event_outbox").fetchone()[0] == 0
    assert add(db, event_id="stable") == add(db, event_id="stable")
    with pytest.raises(ValueError):
        add(db, aggregate="different", event_id="stable")
    with get_connection(db) as connection:
        assert connection.execute("SELECT sequence FROM domain_event_sequences").fetchone()[0] == 1


def test_publish_claims_are_disjoint_and_ordered_per_aggregate(db):
    for index in range(8):
        add(db, str(index))
        add(db, str(index))
    with ThreadPoolExecutor(max_workers=8) as executor:
        batches = list(executor.map(lambda _: claim_events(db, limit=2, now=100), range(8)))
    claimed = [item for batch in batches for item in batch]
    assert len(claimed) == len({item["event_id"] for item in claimed}) == 8
    assert all(item["sequence"] == 1 for item in claimed)
    assert not claim_events(db, now=101)
    for item in claimed:
        assert complete_publish(db, item, {"topic": "domain", "partition": 0, "offset": 1}, now=102)
    assert all(item["sequence"] == 2 for item in claim_events(db, now=103))


def test_expired_publisher_cannot_commit_a_reclaimed_lease(db):
    add(db)
    old = claim_events(db, now=100, lease_seconds=10)[0]
    fresh = claim_events(db, now=111, lease_seconds=10)[0]
    delivery = {"topic": "domain", "partition": 0, "offset": 1}
    assert not complete_publish(db, old, delivery, now=112)
    assert complete_publish(db, fresh, delivery, now=112)


def message(db, event_id, mutate=None):
    with get_connection(db) as connection:
        value = json.loads(
            connection.execute(
                "SELECT event_json FROM domain_event_outbox WHERE event_id=?", (event_id,)
            ).fetchone()[0]
        )
    if mutate:
        mutate(value)
    return SimpleNamespace(
        value=lambda: encode(value).encode(),
        topic=lambda: "test.domain",
        partition=lambda: 0,
        offset=lambda: 1,
    )


def test_external_prepare_is_outside_transaction_and_inbox_retries(db):
    event = message(db, add(db))
    bus = SimpleNamespace(db_path=db, settings=SimpleNamespace(topic=lambda _: "test.domain"))
    effects = []

    def prepare(_envelope):
        # A second writer must succeed before prepare returns; no Inbox lock/tx is held.
        with get_connection(db) as connection:
            connection.execute(
                "INSERT OR REPLACE INTO settings VALUES ('legacy-owner','prepared','yes','now')"
            )
        effects.append("external-effect")

    def failed(_connection, _envelope):
        raise RuntimeError("SQL effect failed after external preparation")

    consumer = DomainEventConsumer(bus, consumer_name="fixture", prepare=prepare)
    with pytest.raises(RuntimeError):
        consumer.process(event, failed)
    with get_connection(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM domain_event_inbox").fetchone()[0] == 0
    assert consumer.process(event, lambda *_: None)
    assert consumer.process(event, lambda *_: pytest.fail("duplicate handler"))
    assert effects == ["external-effect", "external-effect"]


def test_forged_event_is_rejected_before_external_preparation(db):
    event = message(db, add(db), lambda value: value.update(ownerId="forged-owner"))
    bus = SimpleNamespace(db_path=db, settings=SimpleNamespace(topic=lambda _: "test.domain"))
    consumer = DomainEventConsumer(
        bus,
        consumer_name="fixture",
        handler=lambda *_: pytest.fail("handler"),
        prepare=lambda *_: pytest.fail("external"),
    )
    assert consumer.process(event)
    with get_connection(db) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM kafka_delivery_rejections").fetchone()[0] == 1
        )
        assert connection.execute("SELECT COUNT(*) FROM domain_event_inbox").fetchone()[0] == 0


def test_revoke_after_prepare_does_not_commit_inbox(db):
    event = message(db, add(db))
    cancelled = threading.Event()
    bus = SimpleNamespace(db_path=db, settings=SimpleNamespace(topic=lambda _: "test.domain"))
    consumer = DomainEventConsumer(
        bus,
        consumer_name="fixture",
        handler=lambda *_: pytest.fail("handler"),
        prepare=lambda *_: cancelled.set(),
    )
    assert consumer.process(event, cancelled=cancelled) is False
    with get_connection(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM domain_event_inbox").fetchone()[0] == 0
