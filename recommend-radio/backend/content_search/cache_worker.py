from __future__ import annotations

import logging
import os
import signal

from database import DEFAULT_DB_PATH, init_db
from domain_consumer import DomainEventConsumer
from feed_runtime.config import FeedConfig
from feed_runtime.services import resources
from kafka_transport import KafkaJobBus


def main():
    init_db()
    cache, _storage = resources(FeedConfig.from_env(), str(DEFAULT_DB_PATH))

    def invalidate(envelope):
        if envelope["aggregateType"] == "content":
            cache.invalidate_content(envelope["aggregateId"])

    bus = KafkaJobBus(DEFAULT_DB_PATH)
    worker = DomainEventConsumer(
        bus, consumer_name="cache-v1", handler=lambda *_: None, prepare=invalidate
    ).worker(concurrency=2)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: worker.stop.set())
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        worker.run()
    finally:
        cache.close()


if __name__ == "__main__":
    main()
