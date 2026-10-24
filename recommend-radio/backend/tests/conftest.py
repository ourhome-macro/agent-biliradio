from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
for path in (BACKEND, SCRIPTS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def pytest_collection_modifyitems(items):
    """Select real-service contracts without maintaining a second test file list in CI."""
    real_service_files = {
        "test_following_mysql.py",
        "test_kafka_mysql_integration.py",
        "test_mysql_business.py",
        "test_reaction_bitmap_native.py",
        "test_task_transport.py",
        "test_ugc_mysql.py",
    }
    for item in items:
        needs_service = item.path.name in real_service_files
        if item.path.name == "test_media_mysql_fencing.py":
            callspec = getattr(item, "callspec", None)
            needs_service = callspec is not None and "mysql" in callspec.params.values()
        if needs_service:
            item.add_marker(pytest.mark.integration)
