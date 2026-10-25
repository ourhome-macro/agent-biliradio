"""One quality entry point for developer hooks and GitHub Actions."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote, urlsplit

from check_docs import ROOT, check, repository_documents

ARTIFACTS = ROOT / "artifacts" / "quality"
CRITICAL_RULES = "E9,F63,F7,F82,F821"
STRICT_ROOTS = ("src", "tests", "scripts")
LEGACY_ROOTS = ("recommend-radio/backend", "recommend-radio/scripts")


def run(command: list[str], *, env=None, cwd: Path = ROOT) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def python(*args: str, **kwargs) -> None:
    run([sys.executable, *args], **kwargs)


def docs(paths: list[Path] | None = None) -> None:
    if check(paths):
        raise SystemExit(1)
    selected = repository_documents() if paths is None else paths
    if not selected:
        return
    executable = ROOT / "node_modules/markdownlint-cli2/markdownlint-cli2-bin.mjs"
    if not executable.is_file():
        raise RuntimeError("Run npm ci at repository root before linting Markdown")
    # Bound argv on Windows without introducing a second document selection policy.
    for start in range(0, len(selected), 30):
        run(["node", str(executable), *[str(p) for p in selected[start : start + 30]]])


def lint_python(files: list[str] | None = None) -> None:
    if files is None:
        strict, legacy = list(STRICT_ROOTS), list(LEGACY_ROOTS)
    else:
        selected = [Path(p).as_posix() for p in files if p.endswith(".py") and Path(p).is_file()]
        strict = [p for p in selected if p.startswith(tuple(r + "/" for r in STRICT_ROOTS))]
        legacy = [p for p in selected if p not in strict]
    if strict:
        python("-m", "ruff", "check", *strict)
    if legacy:
        python("-m", "ruff", "check", "--select", CRITICAL_RULES, *legacy)


def configuration(files: list[str]) -> None:
    import yaml

    for name in files:
        path = ROOT / name
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            json.loads(text)
        elif path.suffix in {".yaml", ".yml"}:
            yaml.safe_load(text)
        elif path.suffix == ".toml":
            tomllib.loads(text)


def isolated_environment(directory: str) -> dict[str, str]:
    # Do not inherit local credentials or silently load the developer's .env.
    prefixes = (
        "AMEM_",
        "FEED_",
        "MINIO_",
        "UGC_",
        "SEARCH_ES_",
        "KAFKA_",
        "RABBITMQ_",
        "OPENAI_",
        "DEEPSEEK_",
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith(prefixes)}
    env.update(
        APP_DATA_DIR=directory,
        DATABASE_URL="",
        BUSINESS_DATABASE_URL="",
        BUSINESS_SQLITE_PATH="",
        JOB_TRANSPORT="",
        KAFKA_BOOTSTRAP_SERVERS="",
        RABBITMQ_ENABLED="false",
        RADIO_OUTBOX_EMBEDDED="true",
        AUTH_MODE="disabled",
        ALLOW_INSECURE_LOCAL_AUTH="1",
        REQUIRE_DB_MIGRATION="0",
        PYTHON_DOTENV_DISABLED="1",
        PYTHONPATH=os.pathsep.join([str(ROOT / "src"), str(ROOT / "recommend-radio/backend")]),
        PYTHONIOENCODING="utf-8",
    )
    redis = shutil.which("redis-server")
    if redis:
        env.setdefault("REDIS_SERVER_PATH", redis)
    return env


def verify_junit(path: Path, *, integration: bool) -> None:
    document = ET.parse(path).getroot()
    cases = document.findall(".//testcase")
    if not cases:
        raise RuntimeError("pytest collected no tests")
    unexpected = []
    for case in cases:
        skip = case.find("skipped")
        # Windows may forbid symlink creation. No other silent skips are accepted.
        if skip is not None and not (
            os.name == "nt" and "symlinks" in skip.attrib.get("message", "")
        ):
            unexpected.append(case.attrib.get("name", "unknown"))
    if unexpected:
        raise RuntimeError("Unexpected skipped tests: " + ", ".join(unexpected))
    if integration and not any(
        "test_kafka_mysql_integration" in c.get("classname", "") for c in cases
    ):
        raise RuntimeError("Full validation did not collect real Kafka/MySQL acceptance tests")


def tests(*, integration: bool = False) -> None:
    report = ARTIFACTS / ("main.xml" if integration else "pr.xml")
    with tempfile.TemporaryDirectory(prefix="radio-quality-", dir=ARTIFACTS) as directory:
        env = isolated_environment(directory)
        if integration:
            required = ("RADIO_TEST_MYSQL_ADMIN_URL", "RADIO_TEST_ENV_FILE")
            missing = [name for name in required if not env.get(name)]
            if missing:
                raise RuntimeError("Full validation needs isolated services: " + ", ".join(missing))
            env.update(RADIO_REQUIRE_INTEGRATION="1", RABBITMQ_INTEGRATION="1")
            broker = urlsplit(os.environ.get("RADIO_TEST_RABBITMQ_URL", ""))
            if not broker.hostname:
                raise RuntimeError("Full validation needs RADIO_TEST_RABBITMQ_URL")
            env.update(
                RABBITMQ_HOST=broker.hostname,
                RABBITMQ_PORT=str(broker.port or 5672),
                RABBITMQ_USER=unquote(broker.username or ""),
                RABBITMQ_PASSWORD=unquote(broker.password or ""),
                RABBITMQ_VHOST=unquote(broker.path[1:]) or "/",
            )
        args = [
            "-m",
            "pytest",
            "-q",
            "--tb=short",
            "--strict-markers",
            "tests",
            "recommend-radio/backend/tests",
            "recommend-radio/frontend/tests",
        ]
        if not integration:
            args += ["-m", "not integration"]
        python(*args, f"--basetemp={Path(directory) / 'pytest'}", f"--junitxml={report}", env=env)
        verify_junit(report, integration=integration)


def golden() -> None:
    with tempfile.TemporaryDirectory(prefix="radio-golden-", dir=ARTIFACTS) as directory:
        python(
            "recommend-radio/scripts/run_agent_evals.py",
            "--fail-on-gate",
            "--quiet",
            "--output",
            str(ARTIFACTS / "golden.json"),
            env=isolated_environment(directory),
        )
    report = json.loads((ARTIFACTS / "golden.json").read_text(encoding="utf-8"))
    manifest = json.loads(
        (ROOT / "recommend-radio/evals/manifest.v1.json").read_text(encoding="utf-8")
    )
    if (
        report["caseCount"] < manifest["minimumCaseCount"]
        or report["failedCount"]
        or not report["gates"]["passed"]
    ):
        raise RuntimeError("Golden set is incomplete or contains failures")


def frontend() -> None:
    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("Node.js and npm are required")
    run([npm, "test"], cwd=ROOT / "recommend-radio/frontend")
    run([npm, "run", "build"], cwd=ROOT / "recommend-radio/frontend")


def search_live() -> None:
    with tempfile.TemporaryDirectory(prefix="radio-es-quality-", dir=ARTIFACTS) as directory:
        env = isolated_environment(directory)
        private = env.get("RADIO_TEST_ENV_FILE")
        if not private:
            raise RuntimeError("Live ES acceptance needs RADIO_TEST_ENV_FILE")
        python(
            "recommend-radio/scripts/check_content_search_live.py",
            "--endpoint",
            os.environ.get("RADIO_TEST_ES_URL", "http://127.0.0.1:19200"),
            "--env-file",
            private,
            "--output",
            str(ARTIFACTS / "search-live.json"),
            env=env,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage",
        choices=("hook", "docs", "lint", "test", "golden", "frontend", "integration", "pr", "main"),
    )
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    os.chdir(ROOT)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    if args.stage == "hook":
        lint_python(args.files)
        configuration(args.files)
        return check([ROOT / p for p in args.files if p.endswith(".md") and (ROOT / p).is_file()])
    if args.stage in {"docs", "lint", "pr", "main"}:
        if args.stage != "docs":
            lint_python()
            configuration(
                ["pyproject.toml", ".pre-commit-config.yaml", ".github/workflows/quality.yml"]
            )
        docs()
    if args.stage in {"test", "pr"}:
        tests()
    if args.stage in {"integration", "main"}:
        tests(integration=True)
        search_live()
    if args.stage in {"golden", "pr", "main"}:
        golden()
    if args.stage in {"frontend", "pr", "main"}:
        frontend()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (subprocess.CalledProcessError, RuntimeError, ValueError, OSError) as error:
        print(f"Quality gate failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
