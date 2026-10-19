from __future__ import annotations

import copy
import json

import pytest
from content_search.elastic import ElasticClient
from content_search.projector import SearchProjector
from content_search.service import ContentSearchService
from error_code import APIError
from test_ugc_lifecycle import make_studio, publish


class CandidateCache:
    def __init__(self):
        self.values = {}

    def get(self, key, loader, **_):
        if key not in self.values:
            self.values[key] = copy.deepcopy(loader())
        return copy.deepcopy(self.values[key])


class CandidateIndex:
    def __init__(self, ids):
        self.ids, self.closed, self.calls = list(ids), [], 0
        self.records = {}

    def open_pit(self):
        return "fixture-pit"

    def close_pit(self, pit_id):
        self.closed.append(pit_id)

    def candidates(self, _user, _query, *, pit_id, after=None, size=100, **_):
        self.calls += 1
        start = int(after[0]) + 1 if after else 0
        return {
            "pitId": pit_id,
            "hits": [
                {"contentId": content, "sort": [number]}
                for number, content in enumerate(self.ids)
                if start <= number < start + size
            ],
        }

    def index_document(self, content_id, version, value):
        previous = self.records.get(content_id)
        if previous and previous[0] > version:
            return False
        self.records[content_id] = (version, copy.deepcopy(value))
        return True


@pytest.fixture
def search_setup(tmp_path, monkeypatch):
    yield from make_studio(tmp_path, monkeypatch)


def post(studio, key, *, visibility="public", published=True):
    draft = studio.posts.command(
        "alice",
        "create",
        {
            "title": "Secret title " + key,
            "body": "Private body marker " + key,
            "visibility": visibility,
        },
        key,
    )
    return publish(studio, draft) if published else draft


def search_service(studio, ids):
    index, cache = CandidateIndex(ids), CandidateCache()
    return (
        ContentSearchService(studio.repo.db_path, index, cache, secret="test-search-secret"),
        index,
        cache,
    )


def test_suggest_cached_candidates_never_serve_private_text_after_revocation(search_setup):
    studio = search_setup
    resource = post(studio, "suggest")
    service, index, cache = search_service(studio, [resource["id"]])
    assert service.suggest("bob", "Secret")["items"][0]["body"] == resource["body"]
    studio.posts.command(
        "alice",
        "edit",
        {"expectedVersion": resource["version"], "visibility": "private"},
        "hide",
        resource["id"],
    )
    assert service.suggest("bob", "Secret")["items"] == []
    assert index.calls == 1
    assert "Private body marker" not in json.dumps(cache.values)
    assert "Secret title" not in json.dumps(cache.values)
    assert service.suggest("alice", "Secret")["items"][0]["contentId"] == resource["id"]


def test_old_pit_cursor_reauthorizes_and_binds_user_and_query(search_setup):
    studio = search_setup
    first, second = post(studio, "first"), post(studio, "second")
    service, index, _ = search_service(studio, [first["id"], second["id"]])
    page = service.search("bob", "Secret", limit=1)
    assert page["hasMore"] and page["nextCursor"]
    studio.posts.command(
        "alice",
        "edit",
        {"expectedVersion": second["version"], "visibility": "private"},
        "hide-second",
        second["id"],
    )
    assert service.search("bob", "Secret", cursor=page["nextCursor"], limit=1)["items"] == []
    for user, query in (("alice", "Secret"), ("bob", "Different")):
        with pytest.raises(APIError):
            service.search(user, query, cursor=page["nextCursor"])
    # The preceding cursor remains replayable during its bounded lifetime.
    assert service.search("bob", "Secret", cursor=page["nextCursor"], limit=1)["items"] == []


def test_draft_hidden_and_current_db_text_wins_over_stale_match(search_setup):
    studio = search_setup
    draft, public = post(studio, "draft", published=False), post(studio, "public")
    service, _, _ = search_service(studio, [draft["id"], public["id"]])
    studio.posts.command(
        "alice",
        "edit",
        {
            "expectedVersion": public["version"],
            "title": "Updated authoritative title",
            "body": "Current authoritative body",
        },
        "edit",
        public["id"],
    )
    result = service.search("alice", "Secret")
    assert len(result["items"]) == 1
    assert result["items"][0]["title"] == "Updated authoritative title"
    assert result["items"][0]["body"] == "Current authoritative body"


def test_primary_database_failure_does_not_fall_back_to_candidate_cache(search_setup, monkeypatch):
    import resource_policy

    resource = post(search_setup, "db-failure")
    service, _, cache = search_service(search_setup, [resource["id"]])
    assert service.suggest("bob", "Secret")["items"]

    def unavailable(_):
        raise ConnectionError("controlled authority outage")

    monkeypatch.setattr(resource_policy, "get_connection", unavailable)
    with pytest.raises(ConnectionError):
        service.suggest("bob", "Secret")
    assert "body" not in json.dumps(cache.values)


def test_search_authority_failure_closes_pit_and_suggestion_closes_latest_pit(
    search_setup, monkeypatch
):
    import resource_policy

    resource = post(search_setup, "pit-cleanup")
    service, index, _ = search_service(search_setup, [resource["id"]])
    original = index.candidates

    def renewed(*args, **kwargs):
        value = original(*args, **kwargs)
        value["pitId"] = "renewed-pit"
        return value

    monkeypatch.setattr(index, "candidates", renewed)
    service.suggest("bob", "Secret")
    assert index.closed[-1] == "renewed-pit"

    def unavailable(_):
        raise ConnectionError("authority unavailable")

    monkeypatch.setattr(resource_policy, "get_connection", unavailable)
    with pytest.raises(ConnectionError):
        service.search("bob", "Secret")
    assert index.closed[-1] == "renewed-pit"


def test_expired_or_modified_cursor_is_rejected_before_es_calls(search_setup):
    service, index, _ = search_service(search_setup, [])
    token = service.encode_cursor(
        {
            "pitId": "pit",
            "after": None,
            "query": service.digest("query"),
            "user": service.digest("bob"),
            "expiresAt": 0,
        }
    )
    with pytest.raises(APIError) as error:
        service.search("bob", "query", cursor=token)
    assert error.value.status_code == 410 and index.calls == 0
    with pytest.raises(APIError):
        service.search("bob", "query", cursor=token[:-1] + ("1" if token[-1] != "1" else "2"))
    assert index.calls == 0


def test_bounded_filtered_scan_retains_cursor_for_later_visible_hits(search_setup):
    resource = post(search_setup, "reachable")
    service, _, _ = search_service(
        search_setup, [f"missing-{n}" for n in range(500)] + [resource["id"]]
    )
    first = service.search("bob", "Secret")
    assert first["items"] == [] and first["hasMore"] and first["nextCursor"]
    second = service.search("bob", "Secret", cursor=first["nextCursor"])
    assert [item["contentId"] for item in second["items"]] == [resource["id"]]


def test_projector_uses_current_sql_and_keeps_versioned_tombstones(search_setup):
    studio = search_setup
    resource = post(studio, "projection")
    client = CandidateIndex([])
    projector = SearchProjector(studio.repo.db_path, client=client)
    envelope = {"aggregateType": "content", "aggregateId": resource["id"]}
    projector.prepare(envelope)
    old_version, old_doc = client.records[resource["id"]]
    studio.posts.command(
        "alice", "delete", {"expectedVersion": resource["version"]}, "delete", resource["id"]
    )
    projector.prepare(envelope)  # Even an old event reads the current authoritative tombstone.
    version, tombstone = client.records[resource["id"]]
    assert version > old_version and tombstone["tombstone"]
    assert tombstone["title"] == tombstone["body"] == tombstone["completion"] == ""
    assert not client.index_document(resource["id"], old_version, old_doc)
    assert client.records[resource["id"]][1]["tombstone"]


def test_elastic_candidate_boundary_drops_source_text_and_sets_external_version(monkeypatch):
    client = ElasticClient(endpoint="http://127.0.0.1:19200", index="fixture")
    calls = []

    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if path == "/_search":
            return 200, {
                "hits": {
                    "hits": [{"_id": "id", "sort": [1], "_source": {"body": "forbidden raw text"}}]
                }
            }
        return 409, {}

    monkeypatch.setattr(client, "request", request)
    result = client.candidates("bob", "term", pit_id="pit")
    assert result["hits"] == [{"contentId": "id", "sort": [1]}]
    assert calls[0][2]["payload"]["_source"] is False
    assert not client.index_document("id", 3, {"tombstone": True})
    assert calls[1][2]["params"] == {"version": 3, "version_type": "external_gte"}
    client.close()
