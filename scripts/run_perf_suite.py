"""Own isolated infrastructure and run bounded load experiments; raw evidence is retained."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def child(directory, output, phases, quick):
    sys.path[:0] = [str(ROOT / "recommend-radio/backend"), str(ROOT / "recommend-radio/scripts")]
    from mysql_fixture import NativeMySQL
    from mysql_storage.connection import engine_for
    from perf_suite.cache_load import run_cache, run_reactions, seed
    from perf_suite.common import save
    from redis_fixture import NativeRedis

    os.environ.pop("RADIO_TEST_MYSQL_ADMIN_URL", None)
    mysql, redis = NativeMySQL(directory / "mysql"), None
    result = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpuLogical": os.cpu_count(),
            "mysqlBufferPoolMiB": 64,
            "http": "uvicorn + a2wsgi workers=16; fixture identity, no OIDC cost",
        },
        "definitions": {
            "latency": (
                "scheduled arrival through full HTTP response; "
                "includes client scheduling and queue delay"
            ),
            "status0": "timeout",
            "status-1": "HTTP transport error",
            "status-2": "bounded generator drop",
            "source": "isolated synthetic fixtures; no business credentials or traffic",
        },
        "results": {},
    }
    try:
        redis = NativeRedis(directory / "redis")
        content = seed(mysql.url)
        if "reactions" in phases:
            result["results"]["reactions"] = run_reactions(
                output, mysql.url, redis.url, content, quick=quick
            )
            save(output / "summary.json", result)
        if "cache" in phases:
            result["results"]["cache"] = run_cache(output, mysql.url, redis.url, quick=quick)
            save(output / "summary.json", result)
        if "feed" in phases:
            result["results"]["cache"] = run_cache(
                output, mysql.url, redis.url, quick=quick, feed_only=True
            )
            save(output / "summary.json", result)
        for phase, module, function in [
            ("stream", "perf_suite.stream_load", "run"),
            ("kafka", "perf_suite.kafka_load", "run"),
            ("uploads", "perf_suite.upload_load", "run"),
            ("amem", "perf_suite.memory_ab", "run"),
        ]:
            if phase not in phases:
                continue
            imported = __import__(module, fromlist=[function])
            result["results"][phase] = getattr(imported, function)(
                directory, output, mysql.url, quick=quick
            )
            save(output / "summary.json", result)
        save(output / "summary.json", result)
    finally:
        if redis:
            redis.close()
        engine_for(mysql.url).dispose()
        mysql.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/perf/latest")
    parser.add_argument("--phases", default="reactions,cache,stream,kafka,uploads,amem")
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    allowed = {"reactions", "cache", "feed", "stream", "kafka", "uploads", "amem"}
    if not args.phases or any(value not in allowed for value in args.phases.split(",")):
        parser.error("Unknown or empty performance phase")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    phases = args.phases.split(",")
    if args.child:
        child(args.child, output, phases, args.quick)
        return
    from quality import isolated_environment

    with tempfile.TemporaryDirectory(prefix="perf-infra-", dir=output.parent) as directory:
        env = isolated_environment(directory)
        env = {key: value for key, value in env.items() if not key.startswith("OTEL_")}
        env.update(
            PYTHONPATH=os.pathsep.join([str(ROOT / "scripts"), env["PYTHONPATH"]]),
            FEED_ENABLED="true",
            JOB_TRANSPORT="disabled",
            AMEM_ENABLED="false",
        )
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            directory,
            "--output",
            str(output),
            "--phases",
            args.phases,
        ]
        if args.quick:
            command.append("--quick")
        started = time.monotonic()
        subprocess.run(command, check=True, cwd=ROOT, env=env)
        print(
            json.dumps(
                {
                    "report": str(output / "summary.json"),
                    "elapsedSeconds": time.monotonic() - started,
                }
            )
        )


if __name__ == "__main__":
    main()
