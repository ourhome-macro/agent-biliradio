"""Regression tests for false-green CI outcomes and documentation budgets."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from check_docs import MAX_CHARACTERS, inspect_document  # noqa: E402
from quality import isolated_environment, verify_junit  # noqa: E402


def test_document_budget_counts_unicode_and_code_without_crlf_penalty(tmp_path):
    path = tmp_path / "guide.md"
    text = "# 标题\n\n" + "中" * (MAX_CHARACTERS - len("# 标题\n\n"))
    path.write_bytes(text.replace("\n", "\r\n").encode("utf-8"))
    assert inspect_document(path, root=tmp_path) == []
    path.write_text(text + "a", encoding="utf-8")
    assert "8001 characters" in inspect_document(path, root=tmp_path)[0]


def test_document_links_check_files_but_ignore_code_examples_and_urls(tmp_path):
    guide = tmp_path / "guide.md"
    (tmp_path / "other file.md").write_text("# Other\n", encoding="utf-8")
    guide.write_text(
        "# Guide\n\n[ok](other%20file.md)\n[web](https://example.invalid/missing)\n"
        "```markdown\n[example](does-not-exist.md)\n```\n[broken](missing.md)\n",
        encoding="utf-8",
    )
    errors = inspect_document(guide, root=tmp_path)
    assert len(errors) == 1
    assert "missing.md" in errors[0]


def test_environment_drops_api_keys_and_local_database_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "local-secret")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "production.invalid:9092")
    monkeypatch.setenv("DATABASE_URL", "mysql://production.invalid/private")
    env = isolated_environment(str(tmp_path))
    assert "OPENAI_API_KEY" not in env
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == env["DATABASE_URL"] == ""
    assert env["PYTHON_DOTENV_DISABLED"] == "1"
    assert env["APP_DATA_DIR"] == str(tmp_path)


def test_empty_or_skipped_pytest_result_is_not_a_passing_gate(tmp_path):
    report = tmp_path / "results.xml"
    report.write_text("<testsuites><testsuite/></testsuites>", encoding="utf-8")
    with pytest.raises(RuntimeError, match="no tests"):
        verify_junit(report, integration=False)
    report.write_text(
        '<testsuites><testsuite><testcase name="real_kafka">'
        '<skipped message="missing credentials"/></testcase></testsuite></testsuites>',
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="Unexpected skipped"):
        verify_junit(report, integration=True)


def test_full_gate_requires_collected_kafka_contracts(tmp_path):
    report = tmp_path / "results.xml"
    report.write_text(
        '<testsuites><testsuite><testcase name="unit" classname="unit"/></testsuite></testsuites>',
        encoding="utf-8",
    )
    verify_junit(report, integration=False)
    with pytest.raises(RuntimeError, match="real Kafka/MySQL"):
        verify_junit(report, integration=True)


def test_explicit_integration_configuration_cannot_silently_skip(tmp_path, monkeypatch):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "recommend-radio/scripts"))
    from integration_settings import acceptance_settings

    monkeypatch.setenv("RADIO_TEST_ENV_FILE", str(tmp_path / "missing.env"))
    with pytest.raises(RuntimeError, match="configuration is missing"):
        acceptance_settings()
