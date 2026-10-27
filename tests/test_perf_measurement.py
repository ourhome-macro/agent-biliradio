from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("perf_common", ROOT / "scripts/perf_suite/common.py")
common = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(common)


def test_quantiles_include_errors_and_empty_is_not_zero():
    rows = [
        {"status": 200, "latencyMs": 1, "correct": True},
        {"status": 503, "latencyMs": 250, "correct": False},
        {"status": 0, "latencyMs": 5000, "correct": False},
    ]
    result = common.summary(rows)
    assert result["p99Ms"] == 5000
    assert result["successP99Ms"] == 1
    assert result["successRate"] == 1 / 3
    assert common.summary([])["p99Ms"] is None


def test_incorrect_success_is_not_counted_as_correct_by_status_alone():
    result = common.summary([{"status": 200, "latencyMs": 1, "correct": False}])
    assert result["incorrectSuccesses"] == 1


def test_graded_ranking_reference_is_independent():
    # Ideal DCG for grades [3, 1] is 7 + 1/log2(3), rather than 3 + 1/log2(3).
    import math

    ideal = 7 + 1 / math.log2(3)
    reverse = 1 + 7 / math.log2(3)
    assert math.isclose(common.graded_ndcg(["b", "a"], {"a": 3, "b": 1}), reverse / ideal)
    assert common.graded_ndcg(["a"], {"a": 0}) is None
    assert common.percentile([1, 2, 3, 4], 0.5) == 2


def test_generator_drops_are_reported_and_never_become_zero_latency_success(monkeypatch):
    import httpx

    original = httpx.AsyncClient

    async def backend(request):
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={"id": 0})

    monkeypatch.setattr(
        common.httpx,
        "AsyncClient",
        lambda **kw: original(transport=httpx.MockTransport(backend), **kw),
    )

    async def request(client, _index):
        return await client.get("/bench/metadata/0")

    result, rows = asyncio.run(
        common.offered_load("http://fixture.invalid", 200, 0.05, request, limit=1)
    )
    assert len(rows) == 10 and result["clientDropped"] > 0
    assert result["p99Ms"] >= 45
    assert result["offeredSuccessRate"] == (10 - result["clientDropped"]) / 10
