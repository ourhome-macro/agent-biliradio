from __future__ import annotations

import os
import re
from urllib.parse import urlsplit

import requests

INDEX_DEFINITION = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "contentId": {"type": "keyword"},
            "source": {"type": "keyword"},
            "ownerId": {"type": "keyword"},
            "scope": {"type": "keyword"},
            "status": {"type": "keyword"},
            "kind": {"type": "keyword"},
            "title": {"type": "text"},
            "body": {"type": "text"},
            "completion": {"type": "search_as_you_type"},
            "contentVersion": {"type": "long"},
            "permissionVersion": {"type": "long"},
            "updatedAt": {"type": "double"},
            "tombstone": {"type": "boolean"},
        },
    },
}


class SearchUnavailable(RuntimeError):
    def __init__(self, message, *, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class SearchSessionExpired(SearchUnavailable):
    pass


class ElasticClient:
    def __init__(self, *, endpoint=None, index=None, writer=False, admin=False):
        self.endpoint = (endpoint or os.getenv("SEARCH_ES_URL", "http://127.0.0.1:19200")).rstrip(
            "/"
        )
        parsed = urlsplit(self.endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username
            or parsed.path
        ):
            raise ValueError("Elasticsearch endpoint must be an HTTP(S) origin without credentials")
        self.index = index or os.getenv("SEARCH_ES_INDEX", "radio-content-v1")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,99}", self.index):
            raise ValueError("Invalid Elasticsearch index name")
        self.session = requests.Session()
        prefix = "SEARCH_ES_ADMIN" if admin else "SEARCH_ES_WRITE" if writer else "SEARCH_ES_READ"
        username = os.getenv(
            prefix + "_USER", "elastic" if admin else "radio_indexer" if writer else "radio_search"
        )
        password = os.getenv(prefix + "_PASSWORD", "")
        if password:
            self.session.auth = (username, password)

    def request(self, method, path, *, params=None, payload=None, acceptable=()):
        try:
            response = self.session.request(
                method, self.endpoint + path, params=params, json=payload, timeout=(3, 8)
            )
        except requests.RequestException:
            raise SearchUnavailable("Elasticsearch is unavailable") from None
        if response.status_code not in (*acceptable, 200, 201):
            raise SearchUnavailable(
                f"Elasticsearch operation failed ({response.status_code})",
                status_code=response.status_code,
            )
        try:
            return response.status_code, response.json()
        except ValueError:
            raise SearchUnavailable("Elasticsearch returned invalid JSON") from None

    def ensure_index(self):
        code, _ = self.request("GET", f"/{self.index}", acceptable=(404,))
        if code == 404:
            self.request("PUT", f"/{self.index}", payload=INDEX_DEFINITION)

    def index_document(self, content_id, version, document):
        code, _ = self.request(
            "PUT",
            f"/{self.index}/_doc/{content_id}",
            params={"version": version, "version_type": "external_gte"},
            payload=document,
            acceptable=(409,),
        )
        return code != 409

    def open_pit(self):
        _, result = self.request("POST", f"/{self.index}/_pit", params={"keep_alive": "2m"})
        return result["id"]

    def candidates(self, user_id, query, *, pit_id, after=None, size=100, suggest=False):
        match = {"multi_match": {"query": query, "fields": ["title^3", "body"], "operator": "and"}}
        if suggest:
            match = {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",
                    "fields": ["completion", "completion._2gram", "completion._3gram"],
                }
            }
        payload = {
            "size": size,
            "_source": False,
            "pit": {"id": pit_id, "keep_alive": "2m"},
            "sort": [{"_score": "desc"}, {"updatedAt": "desc"}, {"contentId": "asc"}],
            "query": {
                "bool": {
                    "must": [match],
                    "filter": [
                        {"term": {"status": "published"}},
                        {"term": {"tombstone": False}},
                        {
                            "bool": {
                                "should": [
                                    {"term": {"scope": "public"}},
                                    {"term": {"ownerId": user_id}},
                                ],
                                "minimum_should_match": 1,
                            }
                        },
                    ],
                }
            },
        }
        if after:
            payload["search_after"] = after
        try:
            _, response = self.request("POST", "/_search", payload=payload)
        except SearchUnavailable as error:
            if error.status_code == 404:
                raise SearchSessionExpired("Search snapshot expired") from None
            raise
        # Neither raw index text nor total hit counts cross the candidate boundary.
        return {
            "hits": [
                {"contentId": hit["_id"], "sort": hit["sort"]}
                for hit in response.get("hits", {}).get("hits", [])
            ],
            "pitId": response.get("pit_id", pit_id),
        }

    def close_pit(self, pit_id):
        try:
            self.request("DELETE", "/_pit", payload={"id": pit_id})
        except SearchUnavailable:
            pass

    def health(self):
        try:
            self.request("GET", f"/{self.index}/_count")
            return True
        except SearchUnavailable:
            return False

    def close(self):
        self.session.close()
