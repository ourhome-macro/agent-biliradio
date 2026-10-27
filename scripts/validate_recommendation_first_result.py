"""Run first-result and embedding invariants against an owned native MySQL."""

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
    args = parser.parse_args()
    if args.child:
        sys.path[:0] = [
            str(ROOT / "recommend-radio/backend"),
            str(ROOT / "recommend-radio/scripts"),
        ]
        from mysql_fixture import NativeMySQL
        from mysql_storage.connection import engine_for

        os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
        server = NativeMySQL(args.child / "mysql")
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "--tb=short",
                    "recommend-radio/backend/tests/test_recommendation_first_result.py",
                    f"--basetemp={args.child / 'pytest'}",
                    "--junitxml=artifacts/quality/recommendation-first-result-mysql.xml",
                ],
                cwd=ROOT,
                env={**os.environ, "FIRST_RESULT_MYSQL_ADMIN_URL": str(server.url)},
                check=False,
            )
            if result.returncode:
                raise RuntimeError("Native MySQL first-result verification failed")
        finally:
            engine_for(str(server.url)).dispose()
            engine_for(server.url).dispose()
            server.close()
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(
        prefix="first-result-mysql-", dir=ROOT / "artifacts/quality"
    ) as d:
        subprocess.run(
            [sys.executable, __file__, "--child", d],
            cwd=ROOT,
            env=isolated_environment(d),
            check=True,
        )


if __name__ == "__main__":
    main()
