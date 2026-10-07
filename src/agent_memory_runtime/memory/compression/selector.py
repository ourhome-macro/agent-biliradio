from __future__ import annotations

import re
from dataclasses import dataclass, replace

from agent_memory_runtime.domain.memory import MemoryRecord
from agent_memory_runtime.memory.compression.budget import estimate_tokens
from agent_memory_runtime.tokens import TokenEstimator

_CRITICAL_CUE_RE = re.compile(
    r"不|无|除非|但是|不过|并非|仅|只有|"
    r"\b(?:not|never|except|unless|however|but|avoid|only)\b",
    re.IGNORECASE,
)
_EXCERPT_MARKER = " [Extractive excerpt; remaining memory text omitted.]"


@dataclass(frozen=True)
class _Option:
    record: MemoryRecord
    cost: int
    utility: int


def select_under_budget(
    records: list[MemoryRecord],
    *,
    token_budget: int,
    estimator: TokenEstimator | None = None,
    model: str | None = None,
) -> list[MemoryRecord]:
    """Pack ranked facts while treating level as data rather than a hard quota.

    A vetted extractive excerpt may stand in for a long record. Every selected
    view remains under the hard memory budget and output keeps retrieval order.
    """
    if token_budget <= 0 or not records:
        return []

    first_options = _record_options(
        records[0], rank=0, budget=token_budget, estimator=estimator, model=model
    )
    states: dict[int, tuple[int, tuple[tuple[int, _Option], ...]]] = (
        {option.cost: (option.utility, ((0, option),)) for option in first_options}
        if first_options else {0: (0, ())}
    )
    for rank, record in enumerate(records[1:], start=1):
        options = _record_options(
            record, rank=rank, budget=token_budget, estimator=estimator, model=model
        )
        if not options:
            continue
        updated = dict(states)
        for used, (utility, chosen) in states.items():
            for option in options:
                total = used + option.cost
                if total > token_budget:
                    continue
                candidate = (utility + option.utility, (*chosen, (rank, option)))
                previous = updated.get(total)
                if previous is None or candidate[0] > previous[0]:
                    updated[total] = candidate
        states = _prune_dominated(updated)

    _, (_, chosen) = max(states.items(), key=lambda item: (item[1][0], -item[0]))
    return [option.record for _, option in sorted(chosen, key=lambda item: item[0])]


def _record_options(
    record: MemoryRecord,
    *,
    rank: int,
    budget: int,
    estimator: TokenEstimator | None,
    model: str | None,
) -> tuple[_Option, ...]:
    # Retrieval has already ranked by relevance and filtered ACL/status. Rank
    # dominates the small priority/confidence tie-breakers; L3 is not mandatory.
    value = int(1000 / (rank + 1) + 80 * record.priority + 40 * record.confidence)
    options: list[_Option] = []
    full_cost = estimate_tokens(record, estimator=estimator, model=model)
    if full_cost <= budget:
        options.append(_Option(record=record, cost=full_cost, utility=value))

    excerpt = record.metadata.get("context_excerpt")
    if not isinstance(excerpt, str) or not _valid_extract(record.content, excerpt):
        return tuple(options)
    view = replace(
        record,
        content=excerpt.strip() + _EXCERPT_MARKER,
        metadata={**record.metadata, "_context_abbreviated": True},
    )
    excerpt_cost = estimate_tokens(view, estimator=estimator, model=model)
    if excerpt_cost < full_cost and excerpt_cost <= budget:
        options.append(_Option(record=view, cost=excerpt_cost, utility=max(1, int(value * 0.7))))
    return tuple(options)


def _valid_extract(content: str, excerpt: str) -> bool:
    excerpt = excerpt.strip()
    if not excerpt or excerpt == content.strip() or excerpt not in content:
        return False
    # Do not silently remove a later negation or exception that reverses an
    # earlier sentence. Other omitted text remains explicitly marked partial.
    for sentence in re.split(r"(?<=[.!?。！？；;])\s*", content):
        if _CRITICAL_CUE_RE.search(sentence) and sentence.strip() not in excerpt:
            return False
    return True


def _prune_dominated(
    states: dict[int, tuple[int, tuple[tuple[int, _Option], ...]]],
) -> dict[int, tuple[int, tuple[tuple[int, _Option], ...]]]:
    # A more expensive state with no greater utility can never win later.
    best = -1
    result = {}
    for cost, state in sorted(states.items()):
        if state[0] <= best:
            continue
        result[cost] = state
        best = state[0]
    return result
