from __future__ import annotations

import json
import time
from dataclasses import replace
from types import SimpleNamespace

import fakeredis
import pytest
from amem_bridge import NoopAmemBridge
from database import get_connection
from feed_runtime.cache import FeedCache
from feed_runtime.config import FeedConfig
from feed_runtime.feed import FeedService
from feed_runtime.product import FeedProductService
from feed_runtime.repository import FeedRepository, encode
from library_service import LibraryService
from models import Track
from music_profile import MusicProfile
from recommendation_contracts import UserProfile
from recommendation_service import RecommendationService


@pytest.fixture
def serving(tmp_path, monkeypatch):
    for key, value in {
        "FEED_ENABLED": "false",
        "RABBITMQ_ENABLED": "false",
        "RECOMMEND_LLM_ENABLED": "false",
        "RECOMMEND_TEXT_EMBEDDING_BASE_URL": "",
        "RECOMMEND_AUDIO_EMBEDDING_BASE_URL": "",
    }.items():
        monkeypatch.setenv(key, value)
    repo = FeedRepository(tmp_path / "feed.sqlite3")
    config = replace(FeedConfig(), workdir=tmp_path / "work")
    cache = FeedCache(None, client=fakeredis.FakeRedis(decode_responses=True))
    yield repo, config, cache
    cache.close()


def seed(repo, size, facet_for=None):
    """Batch catalog fixture avoids thousands of unrelated legacy upsert transactions."""
    base_track = Track(bvid="BV0000000000", cid=1, title="Music live 0", duration=60)
    base = repo.admit(base_track, user_id="legacy-owner", facets=(facet_for or (lambda _: {}))(0))
    ids = [base]
    with get_connection(repo.db_path) as conn:
        template = dict(
            conn.execute("SELECT * FROM tracks WHERE track_id=?", (base_track.track_id,)).fetchone()
        )
        fields = list(template)
        for index in range(1, size):
            track = Track(
                bvid=f"BV{index:010d}", cid=index + 1, title=f"Music live {index}", duration=60
            )
            record = dict(template)
            for key, value in {
                "track_id": track.track_id,
                "bvid": track.bvid,
                "cid": track.cid,
                "title": track.title,
            }.items():
                if key in record:
                    record[key] = value
            conn.execute(
                f"INSERT INTO tracks ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                [record[field] for field in fields],
            )
            content_id = f"content-{index}"
            ids.append(content_id)
            conn.execute(
                """INSERT INTO feed_content(content_id,track_id,scope,metadata_json,
                facets_json,evidence_json,status,version,admitted_at)
                VALUES (?,?, 'public',?,?, '[]','admitted',1,?)""",
                (
                    content_id,
                    track.track_id,
                    encode(track.to_dict()),
                    encode((facet_for or (lambda _: {}))(index)),
                    1000 + index,
                ),
            )
        conn.execute("UPDATE feed_content SET admitted_at=1000 WHERE content_id=?", (base,))
    return ids


def test_more_than_500_already_issued_do_not_hide_older_supply(serving):
    repo, config, cache = serving
    ids = seed(repo, 530)
    service = FeedService(repo, config, cache)
    first = service.create("legacy-owner", mode="new", request_key="first")
    with get_connection(repo.db_path) as conn:
        page_id = conn.execute(
            "SELECT page_id FROM feed_pages WHERE session_id=?", (first["sessionId"],)
        ).fetchone()[0]
        issued = {item["contentId"] for item in first["items"]}
        for rank, content in enumerate(ids[10:], 100):
            if content not in issued:
                conn.execute(
                    "INSERT INTO feed_items VALUES (?,?,?,?,?,?,?)",
                    (
                        f"synthetic-{rank}",
                        first["sessionId"],
                        page_id,
                        content,
                        rank,
                        encode({"decisionTraceId": "long-session"}),
                        time.time(),
                    ),
                )
    following = service.page("legacy-owner", first["sessionId"], cursor=first["nextCursor"])
    assert {item["contentId"] for item in following["items"]} == set(ids[:10])


def test_more_than_500_cross_session_exposures_are_excluded_before_limit(serving):
    repo, config, cache = serving
    ids = seed(repo, 530)
    with get_connection(repo.db_path) as conn:
        conn.executemany(
            "INSERT INTO feed_user_content(user_id,content_id,last_exposed_at) "
            "VALUES ('legacy-owner',?,?)",
            [(content, time.time()) for content in ids[10:]],
        )
    page = FeedService(repo, config, cache).create("legacy-owner", mode="new", request_key="unseen")
    assert {item["contentId"] for item in page["items"]} == set(ids[:10])


def test_seen_work_aliases_do_not_exhaust_scan_before_unseen_older_works(serving):
    repo, config, cache = serving
    ids = seed(
        repo, 1010, lambda index: {"work_id": f"work-{index if index < 510 else index - 500}"}
    )
    with get_connection(repo.db_path) as conn:
        conn.executemany(
            "INSERT INTO feed_user_content(user_id,content_id,last_exposed_at) "
            "VALUES ('legacy-owner',?,?)",
            [(content, time.time()) for content in ids[10:510]],
        )
    page = FeedService(repo, config, cache).create(
        "legacy-owner", mode="new", request_key="alias-unseen"
    )
    assert {item["contentId"] for item in page["items"]} == set(ids[:10])


def test_exactly_ten_items_launch_replenishment_without_exposure(serving):
    repo, config, cache = serving
    seed(repo, 10)
    calls = []
    recommendations = SimpleNamespace(
        candidate_pool=SimpleNamespace(list_ready=lambda _: []),
        auto_discovery=True,
        feed_legacy_snapshot=lambda: {},
        feed_snapshot=lambda **_: MusicProfile.empty(),
        enqueue_discovery=lambda **kwargs: calls.append(kwargs),
    )
    page = FeedService(repo, config, cache, recommendations=recommendations).create(
        "legacy-owner", mode="new", request_key="exactly-ten"
    )
    assert len(page["items"]) == 10
    assert page["hasMore"] and page["supplyState"] == "replenishing"
    assert len(calls) == 1
    with get_connection(repo.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_user_content").fetchone()[0] == 0


def test_exposure_cooldown_expires_but_explicit_replay_is_always_available(serving):
    repo, config, cache = serving
    content = seed(repo, 1)[0]
    service = FeedService(repo, config, cache)
    first = service.create("legacy-owner", mode="new", request_key="first")
    service.events(
        "legacy-owner",
        [
            {
                "eventId": "visible",
                "itemId": first["items"][0]["itemId"],
                "type": "impression",
                "visibleMs": 1000,
                "visibleRatio": 0.5,
            }
        ],
    )
    assert not service.create("legacy-owner", mode="new", request_key="cooled")["items"]
    replay = FeedProductService(repo, config).open("legacy-owner", content)
    assert replay["items"][0]["contentId"] == content
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "UPDATE feed_user_content SET last_exposed_at=?",
            (time.time() - config.seen_cooldown_seconds - 1,),
        )
    assert (
        service.create("legacy-owner", mode="new", request_key="expired")["items"][0]["contentId"]
        == content
    )


def test_cooldown_covers_public_private_alias_of_the_same_recording(serving):
    repo, config, cache = serving
    track = Track(bvid="BV5555555555", cid=1, title="Music live", duration=60)
    public = repo.admit(track, user_id="legacy-owner")
    private = repo.admit(track, user_id="legacy-owner", scope="user:legacy-owner")
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO feed_user_content(user_id,content_id,last_exposed_at) VALUES (?,?,?)",
            ("legacy-owner", public, time.time()),
        )
    page = FeedService(repo, config, cache).create(
        "legacy-owner", mode="new", request_key="aliases"
    )
    assert not page["items"]
    assert (
        FeedProductService(repo, config).open("legacy-owner", private)["items"][0]["contentId"]
        == private
    )


def test_strict_conditions_and_duplicate_work_cannot_starve_older_unique_supply(serving):
    repo, config, cache = serving

    def facets(index):
        return {
            "languages": ["english" if index < 520 else "japanese"],
            "genres": ["rock"],
            "work_id": f"work-{index}" if index < 20 else "repeated-work",
        }

    seed(repo, 530, facets)
    page = FeedService(repo, config, cache).create(
        "legacy-owner", mode="new", request_key="strict", request_text="英语摇滚"
    )
    assert len(page["items"]) == 10
    with get_connection(repo.db_path) as conn:
        selected = [
            json.loads(
                conn.execute(
                    "SELECT facets_json FROM feed_content WHERE content_id=?", (item["contentId"],)
                ).fetchone()[0]
            )
            for item in page["items"]
        ]
    assert len({facets["work_id"] for facets in selected}) == 10
    assert all(facets["languages"] == ["english"] for facets in selected)


def test_real_personal_ranking_records_bounded_exploration_and_preserves_pages(serving):
    repo, config, cache = serving
    seed(repo, 25)
    recommendations = RecommendationService(
        db_path=repo.db_path, amem_bridge=NoopAmemBridge(), auto_discovery=False
    )
    service = FeedService(repo, config, cache, recommendations=recommendations)
    page = service.create("legacy-owner", request_key="personal")
    assert len(page["items"]) == 10
    with get_connection(repo.db_path) as conn:
        snapshots = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT snapshot_json FROM feed_items WHERE session_id=?", (page["sessionId"],)
            )
        ]
    assert all(snapshot["decision"] for snapshot in snapshots)
    assert sum(snapshot["feedPolicy"]["exploration"] for snapshot in snapshots) == 2
    assert service.page("legacy-owner", page["sessionId"]) == page


def test_work_dedup_retains_allowed_representation_after_personal_hard_filter(serving, monkeypatch):
    repo, config, cache = serving
    allowed = Track(bvid="BV7777777771", cid=1, title="Music live", owner_mid=100, duration=60)
    blocked = Track(bvid="BV7777777772", cid=2, title="Music live", owner_mid=999, duration=60)
    allowed_id = repo.admit(allowed, user_id="legacy-owner", facets={"work_id": "same-work"})
    repo.admit(blocked, user_id="legacy-owner", facets={"work_id": "same-work"})
    recommendations = RecommendationService(
        db_path=repo.db_path, amem_bridge=NoopAmemBridge(), auto_discovery=False
    )
    monkeypatch.setattr(
        recommendations.profile_reader,
        "_load_user_profile",
        lambda: UserProfile(negative_owner_mids={999}),
    )
    page = FeedService(repo, config, cache, recommendations=recommendations).create(
        "legacy-owner", request_key="blocked-duplicate"
    )
    assert [item["contentId"] for item in page["items"]] == [allowed_id]


def test_imported_legacy_like_keeps_its_age_for_short_term_policy(serving):
    repo, _, _ = serving
    from feed_runtime.recommendation import rank_catalog

    old_like = Track(bvid="BV7777777777", cid=1, title="Music live", duration=60)
    LibraryService(repo.db_path).upsert_track(old_like)
    with get_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO likes(user_id,track_id,created_at) VALUES (?,?,?)",
            ("legacy-owner", old_like.track_id, "2020-01-01T00:00:00Z"),
        )
    repo.admit(old_like, user_id="legacy-owner", facets={"genres": ["rock"]})
    target = repo.admit(
        Track(bvid="BV7777777778", cid=2, title="Music live", duration=60),
        user_id="legacy-owner",
        facets={"genres": ["rock"]},
    )
    rows = [row for row in repo.list_content("legacy-owner") if row["content_id"] == target]
    ranked = rank_catalog(rows, user_id="legacy-owner", session_id="future", db_path=repo.db_path)
    assert ranked[0]["_feed_policy"]["shortTermAffinity"] == 0
