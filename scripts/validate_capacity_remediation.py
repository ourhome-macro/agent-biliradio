"""Concurrency invariants against a private native MySQL instance."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--kafka", action="store_true")
    args = parser.parse_args()
    output = ROOT / "artifacts/quality"
    output.mkdir(parents=True, exist_ok=True)
    if args.child:
        sys.path[:0] = [
            str(ROOT / "recommend-radio/backend"),
            str(ROOT / "recommend-radio/scripts"),
        ]
        from mysql_fixture import NativeMySQL
        from mysql_storage.connection import engine_for

        os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
        instance = NativeMySQL(args.child / "mysql")
        kafka = None
        try:
            env = {**os.environ, "CAPACITY_TEST_DATABASE_URL": str(instance.url)}
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "--tb=short",
                    "recommend-radio/backend/tests/test_capacity_remediation.py",
                    f"--basetemp={args.child / 'pytest'}",
                    f"--junitxml={output / 'capacity-remediation-mysql.xml'}",
                ],
                check=True,
                cwd=ROOT,
                env=env,
            )
            if args.kafka:
                from perf_suite.infra import Kafka

                kafka = Kafka()
                settings = args.child / "private.env"
                settings.write_text(
                    "KAFKA_TEST_ROOT_DATABASE_URL=" + str(instance.url) + "\n"
                    "KAFKA_TEST_BOOTSTRAP_SERVERS=" + kafka.bootstrap + "\n",
                    encoding="utf-8",
                )
                if os.name != "nt":
                    settings.chmod(0o600)
                env["RADIO_TEST_ENV_FILE"] = str(settings)
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        "-q",
                        "--tb=short",
                        "recommend-radio/backend/tests/test_kafka_mysql_integration.py",
                        f"--basetemp={args.child / 'kafka-pytest'}",
                        f"--junitxml={output / 'capacity-remediation-kafka.xml'}",
                    ],
                    check=True,
                    cwd=ROOT,
                    env=env,
                )
        finally:
            if kafka:
                kafka.close()
            engine_for(instance.url).dispose()
            instance.close()
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(prefix="capacity-fix-", dir=output) as directory:
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--child",
                directory,
                *(["--kafka"] if args.kafka else []),
            ],
            check=True,
            cwd=ROOT,
            env=isolated_environment(directory),
        )


if __name__ == "__main__":
    main()
