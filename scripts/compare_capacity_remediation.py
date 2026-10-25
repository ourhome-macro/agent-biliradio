"""Recompute remediation evidence without treating admission rejections as capacity gains."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def verify(case, rows, field="latencyMs", metric="p99Ms"):
    attempts = [r for r in rows if r["status"] != -2]
    assert dict(Counter(str(r["status"]) for r in attempts)) == case["statusCounts"]
    values = sorted(r[field] for r in attempts)
    assert round(values[math.ceil(len(values) * 0.99) - 1], 3) == case[metric]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=ROOT / "artifacts/perf/remediation-final")
    parser.add_argument("--final", type=Path, default=ROOT / "artifacts/perf/remediation-final-v2")
    args = parser.parse_args()
    before = read(ROOT / "doc/performance-results-2026-10-25.json")
    after = read(args.base / "summary.json")
    updated = read(args.final / "summary.json")
    after["results"]["reactions"] = updated["results"]["reactions"]
    after["results"]["stream"] = updated["results"]["stream"]
    receipts, requests = [], 0
    for case in after["results"]["cache"]:
        path = args.base / f"raw-feed-{case['offeredRps']}.json"
        rows = read(path)
        verify(case, rows)
        requests += len(rows)
        receipts.append(path)
    for case in after["results"]["reactions"]["stages"]:
        path = args.final / f"raw-reaction-{case['offeredRps']}.json"
        rows = read(path)
        verify(case, rows)
        requests += len(rows)
        receipts.append(path)
    for case in after["results"]["stream"]:
        cached = case["arm"] == "cached_coalesced"
        path = args.final / f"raw-stream-{cached}-{case['seed']}-{case['concurrentViewers']}.json"
        rows = read(path)
        verify(case, rows, "ttfbMs", "ttfbP99Ms")
        requests += len(rows)
        receipts.append(path)
    lag = read(args.base / "raw-kafka-lag.json")
    assert max(r["lag"] for r in lag) == after["results"]["kafka"]["peakLag"]
    value = {
        "before": before["results"],
        "after": after["results"],
        "environment": after["environment"],
        "requestsRecomputed": requests,
        "notes": [
            "Same offered loads and fixture inventories; no database pool increase",
            "16 authorization slots change admission behavior: compare accepted counts too",
            "SQL profile records successful driver execution, not Python compile/pool wait",
        ],
        "receipts": [
            {
                "path": str(p.relative_to(ROOT)).replace("\\", "/"),
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for p in receipts
        ],
    }
    (ROOT / "doc/performance-remediation-2026-10-25.json").write_text(
        json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "verifiedRequests": requests,
                "afterConvergenceSeconds": value["after"]["reactions"][
                    "convergenceSecondsAfterProducerStop"
                ],
                "afterQuiescent": value["after"]["reactions"]["serverQuiescent"],
            }
        )
    )


if __name__ == "__main__":
    main()
