from __future__ import annotations

import os
import hashlib

from .elastic import ElasticClient


def main():
    client = ElasticClient(admin=True)
    try:
        client.ensure_index()
        for mode, user, privileges in (
            ("READ", "radio_search", ["read", "view_index_metadata"]),
            ("WRITE", "radio_indexer", ["read", "write", "create_index", "view_index_metadata"]),
        ):
            username = os.getenv(f"SEARCH_ES_{mode}_USER", user)
            password = os.getenv(f"SEARCH_ES_{mode}_PASSWORD", "")
            if not password:
                raise RuntimeError("Dedicated Elasticsearch account passwords are required")
            role = "radio_content_" + hashlib.sha256(client.index.encode()).hexdigest()[:12] + "_" + mode.lower()
            client.request(
                "PUT",
                f"/_security/role/{role}",
                payload={
                    "cluster": [],
                    "indices": [{"names": [client.index], "privileges": privileges}],
                },
            )
            client.request(
                "PUT",
                f"/_security/user/{username}",
                payload={"password": password, "roles": [role]},
            )
        print("Elasticsearch content index and scoped read/write accounts provisioned")
    finally:
        client.close()


if __name__ == "__main__":
    main()
