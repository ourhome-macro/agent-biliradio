from __future__ import annotations

import argparse
import logging
import signal

from database import DEFAULT_DB_PATH, init_db
from domain_consumer import DomainEventConsumer
from kafka_transport import KafkaJobBus

from .projector import SearchProjector


def main():
    parser = argparse.ArgumentParser(
        description="Versioned ES projection with independent Kafka offsets"
    )
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    init_db()
    projector = SearchProjector(DEFAULT_DB_PATH)
    if args.rebuild:
        try:
            print(projector.rebuild())
        finally:
            projector.client.close()
        return
    bus = KafkaJobBus(DEFAULT_DB_PATH)
    worker = DomainEventConsumer(
        bus, consumer_name="search-v1", handler=projector.handle, prepare=projector.prepare
    ).worker(concurrency=2)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: worker.stop.set())
    logging.basicConfig(level=logging.INFO)
    try:
        worker.run()
    finally:
        projector.client.close()


if __name__ == "__main__":
    main()
