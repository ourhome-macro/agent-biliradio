from prometheus_client import Counter, Gauge, Histogram

OPERATIONS = Counter(
    "radio_feed_operations_total", "Feed operations by outcome", ("operation", "outcome")
)
DURATION = Histogram("radio_feed_duration_seconds", "Feed and media stage duration", ("operation",))
BYTES = Counter("radio_media_bytes_total", "Media bytes by path", ("path",))
ACTIVE = Gauge("radio_media_active_imports", "Media import processes", multiprocess_mode="livesum")


def count(operation: str, outcome: str = "success") -> None:
    OPERATIONS.labels(operation, outcome).inc()
