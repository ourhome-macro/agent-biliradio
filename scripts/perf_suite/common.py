from __future__ import annotations

import asyncio
import ctypes
import json
import math
import os
import time
from collections import Counter
from pathlib import Path

import httpx


def percentile(values, fraction):
    return (
        round(sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)], 3) if values else None
    )


def graded_ndcg(ids, labels, k=8):
    def dcg(values):
        return sum((2**v - 1) / math.log2(i + 2) for i, v in enumerate(values[:k]))

    ideal = dcg(sorted(labels.values(), reverse=True))
    return dcg([labels[i] for i in ids]) / ideal if ideal else None


def summary(rows):
    durations = [r["latencyMs"] for r in rows]
    successful = [r["latencyMs"] for r in rows if 200 <= r["status"] < 300]
    return {
        "requests": len(rows),
        "statusCounts": dict(Counter(str(r["status"]) for r in rows)),
        "p50Ms": percentile(durations, 0.5),
        "p95Ms": percentile(durations, 0.95),
        "p99Ms": percentile(durations, 0.99),
        "successP99Ms": percentile(successful, 0.99),
        "successRate": len(successful) / len(rows) if rows else None,
        "incorrectSuccesses": sum(
            not r.get("correct", True) for r in rows if 200 <= r["status"] < 300
        ),
    }


def resources():
    handles = None
    if os.name == "nt":
        from ctypes import wintypes

        class Memory(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("faults", wintypes.DWORD),
                *[
                    (field, ctypes.c_size_t)
                    for field in ("peak", "rss", "qpp", "qpc", "qnpp", "qnpc", "page", "peakPage")
                ],
            ]

        memory = Memory()
        memory.cb = ctypes.sizeof(memory)
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            ctypes.c_void_p(-1), ctypes.byref(memory), memory.cb
        ):
            raise OSError("Process memory sampling failed")
        rss = memory.rss
        handle = wintypes.DWORD()
        if ctypes.windll.kernel32.GetProcessHandleCount(ctypes.c_void_p(-1), ctypes.byref(handle)):
            handles = handle.value
    else:
        rss = int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        handles = len(list(Path("/proc/self/fd").iterdir()))
    return {
        "pid": os.getpid(),
        "rssBytes": rss,
        "handlesOrFDs": handles,
        "cpuSeconds": time.process_time(),
    }


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def offered_load(base, rate, seconds, request, *, limit=256, timeout=5):
    """Open-loop arrivals. Bound outstanding work; count client drops explicitly."""
    rows, inflight = [], set()
    start = time.perf_counter()
    async with httpx.AsyncClient(
        base_url=base,
        timeout=timeout,
        limits=httpx.Limits(max_connections=limit, max_keepalive_connections=limit),
        trust_env=False,
    ) as client:
        start = time.perf_counter()  # Client setup is warm-up, never catch-up traffic.

        async def send(index, due):
            actual = time.perf_counter()
            try:
                response = await request(client, index)
                status, correct = response.status_code, True
                if response.status_code == 200 and "/reaction" in str(response.url):
                    data = response.json().get("data") or {}
                    expected = json.loads(response.request.content)["state"]
                    correct = data.get("state") == expected and isinstance(
                        data.get("relationVersion"), int
                    )
                elif response.status_code == 200 and "/bench/metadata/" in str(response.url):
                    correct = response.json().get("id") == int(response.url.path.rsplit("/", 1)[1])
            except httpx.TimeoutException:
                status, correct = 0, False
            except httpx.HTTPError:
                status, correct = -1, False
            except (ValueError, KeyError, TypeError):
                status, correct = response.status_code, False
            ended = time.perf_counter()
            rows.append(
                {
                    "index": index,
                    "status": status,
                    "correct": correct,
                    "latencyMs": (ended - start - due) * 1000,
                    "serviceMs": (ended - actual) * 1000,
                    "sendLagMs": (actual - start - due) * 1000,
                }
            )

        for index in range(round(rate * seconds)):
            due = index / rate
            delay = start + due - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            if len(inflight) >= limit:
                rows.append(
                    {
                        "index": index,
                        "status": -2,
                        "correct": False,
                        "latencyMs": 0,
                        "serviceMs": 0,
                        "sendLagMs": (time.perf_counter() - start - due) * 1000,
                    }
                )
                continue
            task = asyncio.create_task(send(index, due))
            inflight.add(task)
            task.add_done_callback(inflight.discard)
        if inflight:
            await asyncio.gather(*inflight)
    result = summary([r for r in rows if r["status"] != -2])
    result.update(
        offeredRps=rate,
        offeredSeconds=seconds,
        offeredRequests=len(rows),
        clientDropped=sum(r["status"] == -2 for r in rows),
        elapsedSeconds=time.perf_counter() - start,
        achievedSuccessRps=sum(200 <= r["status"] < 300 for r in rows)
        / (time.perf_counter() - start),
        offeredSuccessRate=sum(200 <= r["status"] < 300 for r in rows) / len(rows)
        if rows
        else None,
    )
    return result, rows
