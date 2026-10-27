"""One exploratory HTTP A/B pair with two separate SQL consumer processes per arm."""

from __future__ import annotations

import argparse
import json
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
    output = ROOT / "artifacts/perf/projection-http-processes"
    output.mkdir(parents=True, exist_ok=True)
    if args.child:
        sys.path[:0] = [
            str(ROOT / "recommend-radio/backend"),
            str(ROOT / "recommend-radio/scripts"),
        ]
        from mysql_fixture import NativeMySQL
        from mysql_storage.connection import engine_for
        from perf_suite.cache_load import run_reactions, seed
        from redis_fixture import NativeRedis

        os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
        result = {}
        for arm in ("scalar", "batch"):
            directory = output / arm
            directory.mkdir(exist_ok=True)
            mysql = NativeMySQL(args.child / arm / "mysql")
            redis = None
            try:
                redis = NativeRedis(args.child / arm / "redis")
                content = seed(mysql.url)
                result[arm] = run_reactions(
                    directory, mysql.url, redis.url, content, processes=2, scalar=arm == "scalar"
                )
                (output / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
                assert result[arm]["stateCorrect"] and result[arm]["serverQuiescent"]
            finally:
                if redis:
                    redis.close()
                engine_for(str(mysql.url)).dispose()
                engine_for(mysql.url).dispose()
                mysql.close()
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(
        prefix="projection-processes-", dir=output.parent
    ) as directory:
        env = isolated_environment(directory)
        env.update(FEED_ENABLED="true", JOB_TRANSPORT="disabled", AMEM_ENABLED="false")
        env["PYTHONPATH"] = os.pathsep.join(
            [
                str(ROOT / "scripts"),
                str(ROOT / "recommend-radio/backend"),
                str(ROOT / "recommend-radio/scripts"),
            ]
        )
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", directory],
            cwd=ROOT,
            env=env,
            check=True,
        )


if __name__ == "__main__":
    main()
