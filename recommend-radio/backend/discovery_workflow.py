from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context

from models import Track
from full_trace import measure_step
from music_agent import OperationStage
from request_spec import RequestSpec


class DiscoveryWorkflow:
    """Named stages checkpoint their full JSON output in the common tool journal.

    Governance/admission are deliberately non-idempotent: uncertain outcomes need
    reconciliation. Search is read-only; embedding writes use content-keyed upserts.
    """

    def __init__(self, service, full_trace=None):
        self.service = service
        self.full_trace = full_trace

    def stages(self):
        return (
            OperationStage("prepare", self.prepare),
            OperationStage("search", self.search, idempotent=True, side_effects=False),
            OperationStage("admit", self.admit),
            OperationStage("embed", self.embed, idempotent=True),
        )

    def prepare(self, state):
        queries = state.get("queries")
        negative_queries = state.get("negative_queries")
        keyword_specs = state.get("keyword_specs")
        negative_keyword_specs = state.get("negative_keyword_specs")
        spec = RequestSpec.from_dict(state["spec"])
        full_trace = self.full_trace
        started = time.time()
        keyword_specs = dict(keyword_specs or {})
        governance_started = time.perf_counter()
        governance_variant = self.service.experiments.keyword_governance_variant()
        apply_governance = governance_variant != "control"
        if not spec.constrained and apply_governance:
            for reusable in self.service.keyword_governance.reusable_keywords(limit=32):
                query = str(reusable.get("query") or "").strip()
                if not query:
                    continue
                if query not in queries:
                    queries.append(query)
                canonical = reusable.get("canonicalSpec")
                if isinstance(canonical, dict):
                    keyword_specs.setdefault(query, canonical)
        governed = self.service.keyword_governance.prepare(
            queries,
            source="request" if spec.constrained else "profile",
            preserve_order=spec.constrained,
            family_specs=keyword_specs,
            limit=self.service.planner.search_budget,
            apply_governance=apply_governance,
        )
        queries = [item["query"] for item in governed]
        keyword_ids = {item["query"]: item["keywordId"] for item in governed}
        query_pages = {
            item["query"]: int(item.get("searchCount") or 0) % 10 + 1 for item in governed
        }
        negative_governed = self.service.keyword_governance.prepare(
            negative_queries or [],
            source="negative_probe",
            preserve_order=True,
            family_specs=negative_keyword_specs,
            limit=1,
            apply_governance=apply_governance,
        )
        negative_queries = [item["query"] for item in negative_governed]
        negative_keyword_ids = {item["query"]: item["keywordId"] for item in negative_governed}
        negative_query_pages = {
            item["query"]: int(item.get("searchCount") or 0) % 10 + 1 for item in negative_governed
        }
        if full_trace is not None:
            full_trace.record_span(
                "keyword_governance.select",
                (time.perf_counter() - governance_started) * 1000,
                kind="governance",
                outputs={
                    "selectedQueries": queries,
                    "negativeQueries": negative_queries,
                    "variant": governance_variant,
                },
                metrics={"selectedQueryCount": len(queries)},
            )
        return {
            **state,
            "queries": queries,
            "negative_queries": negative_queries,
            "governed": governed,
            "negative_governed": negative_governed,
            "keyword_ids": keyword_ids,
            "negative_keyword_ids": negative_keyword_ids,
            "query_pages": query_pages,
            "negative_query_pages": negative_query_pages,
            "governance_variant": governance_variant,
            "started": started,
        }

    def search(self, state):
        queries = state["queries"]
        negative_queries = state["negative_queries"]
        limit = state["limit"]
        query_pages = state["query_pages"]
        negative_query_pages = state["negative_query_pages"]
        per_query = max(4, min(16, max(limit, 1) * 2))

        def search(kind: str, query: str) -> tuple[str, str, int, list[Track], float, str | None]:
            query_started = time.perf_counter()
            page_size = min(per_query, 8) if kind == "negative" else per_query
            page = negative_query_pages[query] if kind == "negative" else query_pages[query]
            tracks, error_type = self.service._safe_search_result(query, page_size, page=page)
            return (
                kind,
                query,
                page,
                tracks,
                (time.perf_counter() - query_started) * 1000,
                error_type,
            )

        search_tasks = [("positive", query) for query in queries] + [
            ("negative", query) for query in negative_queries
        ]
        search_results: list[tuple[str, str, int, list[Track], float, str | None]] = []
        with ThreadPoolExecutor(
            max_workers=min(4, len(search_tasks) or 1), thread_name_prefix="music-search"
        ) as executor:
            futures = [
                executor.submit(copy_context().run, search, kind, query)
                for kind, query in search_tasks
            ]
            for future in as_completed(futures):
                search_results.append(future.result())
        supply = self.service._supply_lane_tracks(
            limit=max(limit * 2, 8), full_trace=self.full_trace
        )
        return {
            **state,
            "search_results": [
                [kind, query, page, [track.to_dict() for track in tracks], ms, error]
                for kind, query, page, tracks, ms, error in search_results
            ],
            "supply": [
                [source, query, [track.to_dict() for track in tracks]]
                for source, query, tracks in supply
            ],
        }

    def admit(self, state):
        trace_id = state.get("trace_id")
        spec = RequestSpec.from_dict(state["spec"])
        full_trace = self.full_trace
        keyword_ids = state["keyword_ids"]
        negative_keyword_ids = state["negative_keyword_ids"]
        total = {"enqueued": 0, "admitted": 0}
        query_timings = []
        search_results = [
            (kind, query, page, [Track.from_dict(t) for t in tracks], ms, error)
            for kind, query, page, tracks, ms, error in state["search_results"]
        ]
        negative_sample_count = 0
        admitted_tracks: list[Track] = []
        for kind, query, page, tracks, search_ms, error_type in search_results:
            if full_trace is not None:
                full_trace.record_span(
                    "bilibili.search",
                    search_ms,
                    kind="tool",
                    status="failed" if error_type else "completed",
                    inputs={"query": query, "page": page, "queryKind": kind},
                    outputs={"resultCount": len(tracks)},
                    error_type=error_type,
                )
            admit_started = time.perf_counter()
            if kind == "negative":
                recorded = self.service.pool.record_negative_samples(tracks, query=query)
                self.service.keyword_governance.record_discovery(
                    negative_keyword_ids[query],
                    tracks=tracks,
                    admitted_count=0,
                    discovery_job_id=trace_id,
                    admitted_track_ids=[],
                )
                negative_sample_count += recorded
                query_timings.append(
                    {
                        "query": query,
                        "kind": kind,
                        "page": page,
                        "searchMs": round(search_ms, 2),
                        "admissionMs": round((time.perf_counter() - admit_started) * 1000, 2),
                        "resultCount": len(tracks),
                        "errorType": error_type,
                    }
                )
                continue
            result = self.service.pool.admit(
                tracks, source="discovery_search", request_spec=spec, query=query
            )
            admitted_ids = set(result.get("admittedTrackIds") or [])
            admitted_tracks.extend(track for track in tracks if track.track_id in admitted_ids)
            self.service.keyword_governance.record_discovery(
                keyword_ids[query],
                tracks=tracks,
                admitted_count=result["admitted"],
                discovery_job_id=trace_id,
                admitted_track_ids=result.get("admittedTrackIds") or [],
                scope_kind=str(result.get("scopeKind") or "default"),
                scope_key=str(result.get("scopeKey") or ""),
            )
            admit_ms = (time.perf_counter() - admit_started) * 1000
            total["enqueued"] += result["enqueued"]
            total["admitted"] += result["admitted"]
            query_timings.append(
                {
                    "query": query,
                    "kind": kind,
                    "page": page,
                    "searchMs": round(search_ms, 2),
                    "admissionMs": round(admit_ms, 2),
                    "resultCount": len(tracks),
                    "errorType": error_type,
                }
            )
            if full_trace is not None:
                full_trace.record_span(
                    "candidate.admit",
                    admit_ms,
                    kind="retrieval",
                    inputs={"source": "discovery_search", "query": query},
                    outputs={
                        "resultCount": len(tracks),
                        "admittedCount": result["admitted"],
                    },
                    metrics={"invalidRecallCount": max(len(tracks) - result["admitted"], 0)},
                )
        supply_lanes = []
        for source, query, tracks in [
            (source, query, [Track.from_dict(t) for t in tracks])
            for source, query, tracks in state["supply"]
        ]:
            admit_started = time.perf_counter()
            result = self.service.pool.admit(
                tracks,
                source=source,
                request_spec=spec,
                query=query,
            )
            admitted_ids = set(result.get("admittedTrackIds") or [])
            admitted_tracks.extend(track for track in tracks if track.track_id in admitted_ids)
            total["enqueued"] += result["enqueued"]
            total["admitted"] += result["admitted"]
            supply_lanes.append(
                {
                    "source": source,
                    "query": query,
                    "resultCount": len(tracks),
                    "admittedCount": result["admitted"],
                    "admissionMs": round((time.perf_counter() - admit_started) * 1000, 2),
                }
            )
            if full_trace is not None:
                full_trace.record_span(
                    "candidate.admit",
                    supply_lanes[-1]["admissionMs"],
                    kind="retrieval",
                    inputs={"source": source, "query": query},
                    outputs={
                        "resultCount": len(tracks),
                        "admittedCount": result["admitted"],
                    },
                    metrics={"invalidRecallCount": max(len(tracks) - result["admitted"], 0)},
                )
        return {
            **state,
            "total": total,
            "query_timings": query_timings,
            "negative_sample_count": negative_sample_count,
            "supply_lanes": supply_lanes,
            "admitted_tracks": [track.to_dict() for track in admitted_tracks],
        }

    def embed(self, state):
        queries = state.get("queries")
        trace_id = state.get("trace_id")
        negative_queries = state.get("negative_queries")
        spec = RequestSpec.from_dict(state["spec"])
        full_trace = self.full_trace
        total = state["total"]
        query_timings = state["query_timings"]
        negative_sample_count = state["negative_sample_count"]
        supply_lanes = state["supply_lanes"]
        governed = state["governed"]
        negative_governed = state["negative_governed"]
        governance_variant = state["governance_variant"]
        started = state["started"]
        admitted_tracks = [Track.from_dict(t) for t in state["admitted_tracks"]]
        from embedding_jobs import enqueue_text

        with measure_step(
            full_trace, "candidate.embedding.enqueue", kind="embedding"
        ) as measurement:
            embedding_result = enqueue_text(self.service.content_embeddings, admitted_tracks[:64])
            audio_queued = self.service.content_embeddings.enqueue_audio_embeddings(
                admitted_tracks[:64]
            )
            measurement.outputs.update({**embedding_result, "audioQueued": audio_queued})
        embedding_ms = measurement.duration_ms
        available = self.service.pool.availability(spec)
        if full_trace is not None:
            full_trace.event(
                "discovery.completed",
                {
                    "admitted": total["admitted"],
                    "enqueued": total["enqueued"],
                    "available": available,
                },
            )
        return {
            "traceId": trace_id,
            "queries": queries,
            "negativeQueries": negative_queries or [],
            "negativeSampleCount": negative_sample_count,
            "keywords": [*governed, *negative_governed],
            "keywordGovernanceVariant": governance_variant,
            "supplyLanes": supply_lanes,
            **total,
            "available": available,
            "embedding": {**embedding_result, "audioQueued": audio_queued},
            "timing": {
                "queries": query_timings,
                "embeddingEnqueueMs": embedding_ms,
                "totalMs": round((time.time() - started) * 1000, 2),
            },
        }
