"""Independently verify raw latency/count evidence and build a compact engineering readout."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def p(values, fraction):
    return round(sorted(values)[math.ceil(len(values) * fraction) - 1], 3) if values else None


def verify(case, rows):
    attempted = [v for v in rows if v["status"] != -2]
    assert len(attempted) == case["requests"]
    assert dict(Counter(str(v["status"]) for v in attempted)) == case["statusCounts"]
    assert p([v["latencyMs"] for v in attempted], 0.99) == case["p99Ms"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=ROOT / "artifacts/perf/full")
    parser.add_argument("--main", type=Path, default=ROOT / "artifacts/perf/remaining")
    args = parser.parse_args()
    cache_source, source = read(args.cache / "summary.json"), read(args.main / "summary.json")
    result = source["results"]
    result["cache"] = cache_source["results"]["cache"]
    verified = 0
    aggregate = defaultdict(list)
    request_groups = defaultdict(list)
    receipts = []
    for case in result["cache"]:
        if case["experiment"] == "feed_http":
            path = args.cache / f"raw-feed-{case['offeredRps']}.json"
        else:
            path = args.cache / (
                f"raw-cache-{case['experiment']}-{case['arm']}-"
                f"{case['pattern']}-{case['seed']}.json"
            )
        rows = read(path)
        verify(case, rows)
        verified += len(rows)
        if case["experiment"] != "feed_http":
            key = (case["experiment"], case["pattern"], case["arm"])
            aggregate[key].append(case)
            request_groups[key].extend(rows)
        receipts.append(
            {
                "path": str(path.relative_to(ROOT)).replace("\\", "/"),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "rows": len(rows),
            }
        )
    for case in result["reactions"]["stages"]:
        rows = read(args.main / f"raw-reaction-{case['offeredRps']}.json")
        verify(case, rows)
        verified += len(rows)
        case["offeredSuccessRate"] = sum(200 <= r["status"] < 300 for r in rows) / len(rows)
    stream_verified = 0
    for case in result["stream"]:
        cached = case["arm"] == "cached_coalesced"
        rows = read(
            args.main / f"raw-stream-{cached}-{case['seed']}-{case['concurrentViewers']}.json"
        )
        assert len(rows) == case["requests"]
        assert dict(Counter(str(r["status"]) for r in rows)) == case["statusCounts"]
        assert p([r["ttfbMs"] for r in rows], 0.99) == case["ttfbP99Ms"]
        stream_verified += len(rows)
    lag_rows = read(args.main / "raw-kafka-lag.json")
    assert max(r["lag"] for r in lag_rows) == result["kafka"]["peakLag"]
    assert lag_rows[-1]["lag"] == result["kafka"]["remainingLag"]
    for case in result["uploads"]["stages"]:
        rows = read(args.main / f"raw-uploads-{case['concurrency']}.json")
        assert len(rows) == case["requests"]
        assert (
            sum(r["status"] in {200, 201, 204} for r in rows) / len(rows)
            == case["uploadSuccessRate"]
        )
        assert p([r["uploadMs"] for r in rows], 0.95) == case["uploadP95Ms"]
        assert (
            p([r["confirmMs"] for r in rows if r.get("confirmMs") is not None], 0.95)
            == case["confirmationP95Ms"]
        )
    known = {r["path"] for r in receipts}
    for folder in (args.cache, args.main):
        for path in sorted(folder.glob("raw-*.json")):
            relative = str(path.relative_to(ROOT)).replace("\\", "/")
            if relative not in known:
                receipts.append(
                    {
                        "path": relative,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "rows": len(read(path)),
                    }
                )
                known.add(relative)
    labels = read(args.main / "raw-memory-labels-and-ranks.json")
    for arm, field in [("control", "ndcgAt8Control"), ("amem_profile", "ndcgAt8AmemProfile")]:
        scores = []
        for row in labels:
            ideal = sum(
                (2**grade - 1) / math.log2(i + 2)
                for i, grade in enumerate(sorted(row["labels"].values(), reverse=True)[:8])
            )
            observed = sum(
                (2 ** row["labels"][identity] - 1) / math.log2(i + 2)
                for i, identity in enumerate(row[arm]["ids"][:8])
            )
            scores.append(observed / ideal)
        assert math.isclose(sum(scores) / len(scores), result["amem"][field], rel_tol=1e-12)
    condensed = []
    for key, cases in aggregate.items():
        rows = request_groups[key]
        counts = Counter()
        for case in cases:
            counts.update(case["cacheEvents"])
        condensed.append(
            {
                "experiment": key[0],
                "pattern": key[1],
                "arm": key[2],
                "requests": len(rows),
                "p95Ms": p([r["latencyMs"] for r in rows], 0.95),
                "p99Ms": p([r["latencyMs"] for r in rows], 0.99),
                "originLoads": sum(c["originLoads"] for c in cases),
                "foregroundCompleted": counts["cache:miss"],
                "proactiveCompleted": counts["hotkey:refreshed"],
                "redisClientCalls": sum(c["redisClientCalls"] for c in cases),
                "redisClientQps": sum(c["redisClientCalls"] for c in cases)
                / sum(c["elapsedSeconds"] for c in cases),
                "coalesced": counts["cache:coalesced"],
                "allSuccessful": all(r["status"] == 200 and r.get("correct", True) for r in rows),
            }
        )
    snapshot = {
        "environment": source["environment"],
        "results": result,
        "cacheAggregate": condensed,
        "verification": {
            "rawRequestsRecomputed": verified,
            "streamRequestsRecomputed": stream_verified,
            "memoryQueriesRecomputed": len(labels),
            "note": "Latency distributions and status counts recomputed directly from raw rows",
        },
        "receipts": receipts,
    }
    destination = ROOT / "doc/performance-results-2026-10-25.json"
    destination.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps({"verifiedRequests": verified, "cacheAggregate": condensed}, ensure_ascii=False)
    )


if __name__ == "__main__":
    main()
