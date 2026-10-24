"""Read explicitly selected acceptance configuration without exposing credentials."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values


def acceptance_settings() -> dict[str, str] | None:
    configured = os.getenv("RADIO_TEST_ENV_FILE")
    path = (
        Path(configured)
        if configured
        else Path(__file__).resolve().parents[1] / "server-data/kafka-acceptance/private.env"
    )
    if not path.is_file():
        if configured or os.getenv("RADIO_REQUIRE_INTEGRATION") == "1":
            raise RuntimeError("Explicit isolated integration configuration is missing")
        return None
    return {key: value for key, value in dotenv_values(path).items() if value is not None}
