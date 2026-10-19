import fakeredis
from feed_runtime.cache import FeedCache
from redis.exceptions import ResponseError


def test_outage_opens_one_circuit_for_all_card_reads_and_health_recovers(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    cache = FeedCache(None, client=client)
    original = client.get
    calls = 0

    def unavailable(*args):
        nonlocal calls
        calls += 1
        raise ConnectionError("test endpoint unavailable")

    monkeypatch.setattr(client, "get", unavailable)
    try:
        for index in range(10):
            assert cache.counter(str(index), lambda: {"likes": 1, "version": 1})["likes"] == 1
        assert calls == 1, "one dead Redis endpoint must not be retried for every card"
        monkeypatch.setattr(client, "get", original)
        assert cache.health()
        cache.publish_counter("recovered", {"likes": 5, "version": 3})
        cache.publish_counter("recovered", {"likes": 1, "version": 1})
        assert cache.counter("recovered", lambda: {})["likes"] == 5
    finally:
        cache.close()


def test_redis_command_error_does_not_open_connection_circuit(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    cache = FeedCache(None, client=client)
    calls = 0

    def denied(*_args):
        nonlocal calls
        calls += 1
        raise ResponseError("NOPERM test")

    monkeypatch.setattr(client, "get", denied)
    try:
        for index in range(2):
            assert cache.counter(str(index), lambda: {"likes": 1, "version": 1})["likes"] == 1
        assert calls == 2
        assert cache._redis_retry_at == 0
    finally:
        cache.close()


def test_elapsed_circuit_allows_read_probe_without_a_health_request(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    cache = FeedCache(None, client=client)
    try:
        cache._redis_retry_at = 1.0  # An elapsed monotonic retry deadline.
        assert cache.counter("probe", lambda: {"likes": 2, "version": 1})["likes"] == 2
        assert cache._redis_retry_at == 0
        assert client.get(cache.prefix + ":counter:probe")
    finally:
        cache.close()
