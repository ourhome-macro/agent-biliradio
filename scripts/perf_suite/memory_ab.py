from __future__ import annotations

import random
from types import SimpleNamespace

from models import Track
from music_profile import MusicProfile, RelevantMemory
from profile_projector import ProfileProjector
from recommendation_contracts import CandidateDraft, UserProfile
from recommendation_engine import RecommendationEngine, RecommendationRequest
from request_spec import RequestSpec

from .common import graded_ndcg, save


def run(directory, output, db, *, quick=False):
    rows = []
    genres = ["jazz", "rock", "folk", "pop", "blues"]
    for seed in range(100):
        preferred = genres[seed % len(genres)]
        memory = RelevantMemory(
            f"fixture-{seed}",
            f"prefers {preferred}",
            "L3",
            "profile",
            salience=0.9,
            confidence=0.9,
            metadata={"signal": "profile_l3_positive_topic", "topic": preferred},
        )
        projector = ProfileProjector(
            SimpleNamespace(retrieve_memories=lambda *a, m=memory, **kw: [m]), enabled=False
        )
        profile = projector.project_committed(
            user_id=f"fixture-{seed}", scene="home", fallback_profile=MusicProfile.empty()
        ).profile
        tracks = [
            Track(
                bvid=f"BV{i:010d}",
                cid=i + 1,
                title=f"{genre} music live concert {i}",
                owner=f"artist-{i}",
                owner_mid=i + 1,
                duration=120,
            )
            for i, genre in enumerate(genres * 8)
        ]
        random.Random(seed).shuffle(tracks)
        labels = {t.track_id: 3 if preferred in t.title else 1 for t in tracks}
        result = {}
        for arm, active_profile in [("control", MusicProfile.empty()), ("amem_profile", profile)]:
            engine = RecommendationEngine(user_id=f"fixture-{seed}")
            candidates = [
                engine.score(
                    CandidateDraft(track=t, sources={"feed_catalog"}, tags=set(t.tags)),
                    UserProfile(),
                    active_profile,
                    "fixed",
                    RequestSpec(),
                )
                for t in tracks
            ]
            _ranked, selected, _diagnostics = engine.rank_and_select(
                candidates,
                request=RecommendationRequest(
                    scene="feed",
                    limit=8,
                    request_spec=RequestSpec(),
                    profile=active_profile,
                    exclude_track_ids=set(),
                    recent_context={},
                ),
                legacy_profile=UserProfile(),
            )
            ids = [v.track["trackId"] for v in selected]
            result[arm] = {"ndcgAt8": graded_ndcg(ids, labels), "ids": ids}
        rows.append({"query": seed, "preferred": preferred, "labels": labels, **result})
    save(output / "raw-memory-labels-and-ranks.json", rows)
    return {
        "queries": len(rows),
        "ndcgAt8Control": sum(r["control"]["ndcgAt8"] for r in rows) / len(rows),
        "ndcgAt8AmemProfile": sum(r["amem_profile"]["ndcgAt8"] for r in rows) / len(rows),
        "CTR": None,
        "completionRate": None,
        "scope": (
            "controlled synthetic L3 -> production projector and ranking; "
            "not an LLM/user outcome claim"
        ),
        "unmeasuredReason": (
            "No randomized user outcomes; AMEM memory extraction quality is not evaluated"
        ),
    }
