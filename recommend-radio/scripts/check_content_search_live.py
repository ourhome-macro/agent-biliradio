"""Real Elasticsearch role/PIT acceptance with temporary SQL and a dedicated test index."""

from __future__ import annotations

import argparse
import copy
import json
import os
import secrets
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "recommend-radio/backend"), str(ROOT / "src")]


class CandidateCache:
    def __init__(self):
        self.values = {}

    def get(self, key, loader, **_):
        if key not in self.values:
            self.values[key] = copy.deepcopy(loader())
        return copy.deepcopy(self.values[key])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:19200")
    parser.add_argument(
        "--env-file", type=Path, help="Read only test admin credentials from a private env file"
    )
    parser.add_argument(
        "--output", type=Path, default=ROOT / "doc/content-search-live-2026-10-08.json"
    )
    args = parser.parse_args()
    if args.env_file:
        from dotenv import dotenv_values

        settings = dotenv_values(args.env_file)
        for name in ("SEARCH_ES_ADMIN_USER", "SEARCH_ES_ADMIN_PASSWORD"):
            if settings.get(name):
                os.environ[name] = settings[name]
        if not os.getenv("SEARCH_ES_ADMIN_PASSWORD") and settings.get("ELASTIC_PASSWORD"):
            os.environ["SEARCH_ES_ADMIN_PASSWORD"] = settings["ELASTIC_PASSWORD"]
    from content_search.elastic import ElasticClient, SearchUnavailable
    from content_search.projector import SearchProjector
    from content_search.service import ContentSearchService
    from database import get_connection
    from feed_runtime.repository import FeedRepository
    from ugc.service import UGCService

    suffix = uuid4().hex[:12]
    index = "radio-content-acceptance-" + suffix
    admin = ElasticClient(endpoint=args.endpoint, index=index, admin=True)
    writer = reader = None
    roles, users, previous_env = [], [], {}
    checks = []
    created = False
    try:
        admin.ensure_index()
        created = True
        for mode, privileges in (("READ", ["read", "view_index_metadata"]), ("WRITE", ["index"])):
            role = f"radio_acceptance_{mode.lower()}_{suffix}"
            user = role + "_user"
            password = secrets.token_urlsafe(24)
            admin.request(
                "PUT",
                f"/_security/role/{role}",
                payload={"cluster": [], "indices": [{"names": [index], "privileges": privileges}]},
            )
            roles.append(role)
            admin.request(
                "PUT",
                f"/_security/user/{user}",
                payload={
                    "password": password,
                    "roles": [role],
                    "enabled": True,
                    "metadata": {"purpose": "isolated acceptance"},
                },
            )
            users.append(user)
            for field, value in (("USER", user), ("PASSWORD", password)):
                key = f"SEARCH_ES_{mode}_{field}"
                previous_env[key] = os.environ.get(key)
                os.environ[key] = value
        writer = ElasticClient(endpoint=args.endpoint, index=index, writer=True)
        reader = ElasticClient(endpoint=args.endpoint, index=index)
        with tempfile.TemporaryDirectory(prefix="search-authority-acceptance-") as directory:
            repo = FeedRepository(Path(directory) / "authority.sqlite3")
            with get_connection(repo.db_path) as conn:
                for user in ("fixture-alice", "fixture-bob"):
                    conn.execute(
                        "INSERT INTO app_users(id,display_name,created_at,updated_at) "
                        "VALUES (?,?,?,?)",
                        (user, user, "2026-10-08", "2026-10-08"),
                    )
            posts, documents = UGCService(repo.db_path), []
            for number in range(3):
                value = posts.command(
                    "fixture-alice",
                    "create",
                    {
                        "title": f"Acceptance music {number}",
                        "body": f"Sensitive author text {number}",
                        "visibility": "public",
                    },
                    f"create-{number}",
                )
                if number < 2:
                    value = posts.command(
                        "fixture-alice",
                        "publish",
                        {"expectedVersion": value["version"]},
                        f"publish-{number}",
                        value["id"],
                    )
                documents.append(value)
            projector = SearchProjector(repo.db_path, client=writer)
            for value in documents:
                projector.prepare({"aggregateType": "content", "aggregateId": value["id"]})
            admin.request("POST", f"/{index}/_refresh")
            service = ContentSearchService(
                repo.db_path, reader, CandidateCache(), secret="isolated-fixture-secret"
            )
            result = service.search("fixture-bob", "Acceptance music", limit=1)
            assert len(result["items"]) == 1 and result["nextCursor"]
            assert result["items"][0]["contentId"] != documents[2]["id"]
            suggestions = service.suggest("fixture-bob", "Accept")
            assert len(suggestions["items"]) == 2
            checks += ["real reader-role search and prefix suggestions", "draft tombstone hidden"]
            first_id = result["items"][0]["contentId"]
            hidden = next(value for value in documents[:2] if value["id"] != first_id)
            private = posts.command(
                "fixture-alice",
                "edit",
                {"expectedVersion": hidden["version"], "visibility": "private"},
                "privatize",
                hidden["id"],
            )
            # Deliberately keep the ES PIT and suggest cache stale: SQL still revokes the grant.
            assert (
                service.search(
                    "fixture-bob", "Acceptance music", cursor=result["nextCursor"], limit=1
                )["items"]
                == []
            )
            assert hidden["id"] not in {
                value["contentId"] for value in service.suggest("fixture-bob", "Accept")["items"]
            }
            checks += [
                "stale real PIT reauthorizes after privatization",
                "stale suggest IDs cannot expose private text",
            ]
            projector.prepare({"aggregateType": "content", "aggregateId": hidden["id"]})
            admin.request("POST", f"/{index}/_refresh")
            assert hidden["id"] in {
                value["contentId"] for value in service.suggest("fixture-alice", "Accept")["items"]
            }
            checks.append("author can retrieve own published private post")
            try:
                reader.index_document("forbidden-reader-write", 1, {"title": "must not be written"})
            except SearchUnavailable as error:
                assert "403" in str(error)
            else:
                raise AssertionError("Reader principal unexpectedly has index write permission")
            checks.append("reader role cannot index documents")
            deleted = posts.command(
                "fixture-alice",
                "delete",
                {"expectedVersion": private["version"]},
                "delete",
                hidden["id"],
            )
            projector.prepare({"aggregateType": "content", "aggregateId": hidden["id"]})
            code, tombstone = admin.request("GET", f"/{index}/_doc/{hidden['id']}")
            assert code == 200 and tombstone["_source"]["tombstone"]
            assert tombstone["_source"]["body"] == tombstone["_source"]["title"] == ""
            assert not writer.index_document(
                hidden["id"],
                hidden["version"],
                {
                    **tombstone["_source"],
                    "title": "stale public title",
                    "body": "stale public secret",
                    "status": "published",
                    "tombstone": False,
                },
            )
            assert deleted["version"] > hidden["version"]
            checks += [
                "real versioned tombstone erases indexed text",
                "older external version cannot resurrect document",
            ]
        report = {
            "backend": "real-elasticsearch-with-temporary-sqlite",
            "index": index,
            "passed": checks,
            "sourceWrites": 0,
            "productionDatabaseTouched": False,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"passed": len(checks), "report": str(args.output)}))
    finally:
        cleanup_errors = []
        for user in users:
            try:
                assert user.endswith(suffix + "_user")
                admin.request("DELETE", f"/_security/user/{user}")
            except Exception:
                cleanup_errors.append("user:" + user)
        for role in roles:
            try:
                assert role.endswith(suffix)
                admin.request("DELETE", f"/_security/role/{role}")
            except Exception:
                cleanup_errors.append("role:" + role)
        if created:
            try:
                assert index == "radio-content-acceptance-" + suffix
                admin.request("DELETE", f"/{index}")
            except Exception:
                cleanup_errors.append("index:" + index)
        for client in (reader, writer, admin):
            if client:
                client.close()
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if cleanup_errors:
            raise RuntimeError(
                "Isolated acceptance cleanup incomplete: " + ", ".join(cleanup_errors)
            )


if __name__ == "__main__":
    main()
