from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path

from database import DEFAULT_DB_PATH, init_db
from durable_jobs import publish_pending
from job_transport import publish_job

HEARTBEAT = Path("/tmp/radio-outbox-heartbeat")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    init_db()
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopped.set())
    while not stopped.is_set():
        try:
            publish_pending(DEFAULT_DB_PATH, publish_job)
            HEARTBEAT.write_text(str(time.time()), encoding="ascii")
        except Exception:
            logging.exception("Outbox dispatch failed; persisted jobs will be retried")
        stopped.wait(1)


if __name__ == "__main__":
    main()
