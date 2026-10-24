from __future__ import annotations

import json

import pytest
from run_agent_evals import (
    DEFAULT_GATES,
    DEFAULT_GOLDEN_DIR,
    _load_cases,
    _ranking_case,
    evaluate_cases,
    evaluate_gates,
)


def test_agent_golden_set_passes_regression_gates() -> None:
    cases, golden_hash = _load_cases(DEFAULT_GOLDEN_DIR)
    report = evaluate_cases(cases, golden_hash=golden_hash)
    gates = json.loads(DEFAULT_GATES.read_text(encoding="utf-8"))
    gate_result = evaluate_gates(report, gates)
    baseline = json.loads(
        (DEFAULT_GOLDEN_DIR.parent / "baseline.v1.json").read_text(encoding="utf-8")
    )
    assert {case["id"] for case in baseline["cases"]} <= {case["id"] for case in cases}
    assert len(cases) >= baseline["caseCount"]
    assert report["failedCount"] == 0
    assert gate_result["passed"] is True


def test_expected_forbidden_labels_do_not_filter_serving_inputs():
    candidate = {
        "track": {"trackId": "must-be-selected", "bvid": "BV1234567890", "title": "Music live"},
        "score": 10,
        "source": "discovery_search",
        "facets": {"genres": ["pop"]},
    }
    result = _ranking_case(
        {"limit": 1, "candidates": [candidate]},
        {"forbiddenIds": ["must-be-selected"]},
    )
    assert result["actual"]["selectedIds"] == ["must-be-selected"]
    assert result["passed"] is False
    assert "forbiddenIds" in result["mismatches"]


def test_real_policy_regression_is_detected_by_golden_set(monkeypatch):
    from recommendation_policy import RecommendationPolicy

    cases, _ = _load_cases(DEFAULT_GOLDEN_DIR)
    case = next(case for case in cases if case["id"] == "biz-ranking-002")
    # Feedback is checked at admission and selection. Disable both defenses to
    # prove that the frozen expected labels catch a real behavioral regression.
    monkeypatch.setattr(RecommendationPolicy, "_is_hard_filtered", staticmethod(lambda *_: False))
    monkeypatch.setattr(RecommendationPolicy, "_is_skipped", staticmethod(lambda *_: False))
    result = _ranking_case(case["input"], case["expected"])
    assert result["passed"] is False
    assert result["metrics"]["forbiddenViolations"] > 0


@pytest.mark.parametrize("actual", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_metrics_cannot_pass_quality_gates(actual):
    result = evaluate_gates({"metrics": {"accuracy": actual}}, {"minimum": {"accuracy": 1.0}})
    assert result["passed"] is False


def test_manifest_prevents_accidental_loss_of_regression_cases(tmp_path):
    directory = tmp_path / "golden"
    directory.mkdir()
    for source in DEFAULT_GOLDEN_DIR.glob("*.jsonl"):
        (directory / source.name).write_bytes(source.read_bytes())
    manifest = DEFAULT_GOLDEN_DIR.parent / "manifest.v1.json"
    (tmp_path / manifest.name).write_bytes(manifest.read_bytes())
    regression = directory / "regression.v1.jsonl"
    regression.write_text(
        "\n".join(regression.read_text(encoding="utf-8").splitlines()[1:]), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="required cases are missing"):
        _load_cases(directory)


def test_golden_case_must_have_explicit_input_and_expected(tmp_path):
    (tmp_path / "invalid.jsonl").write_text(
        json.dumps({"id": "missing-answer", "layer": "capability", "task": "ranking"}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid golden case"):
        _load_cases(tmp_path)
