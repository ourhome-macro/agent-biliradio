from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

from error_code import APIError, ErrorCode
from resource_policy import ResourcePolicy


class ContentSearchService:
    def __init__(self, db_path, client, cache, *, secret):
        self.policy, self.client, self.cache = ResourcePolicy(db_path), client, cache
        self.secret = secret.encode() if isinstance(secret, str) else secret

    @staticmethod
    def validate(query, limit):
        if not isinstance(query, str) or not 1 <= len(query.strip()) <= 200:
            raise APIError.validation_error("请输入1至200字符的搜索词")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise APIError.validation_error("Search limit must be between 1 and 50")
        return query.strip()

    @staticmethod
    def digest(value):
        return hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()

    def encode_cursor(self, value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        signed = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
        signature = hmac.new(self.secret, signed.encode(), hashlib.sha256).hexdigest()
        return signed + "." + signature

    def decode_cursor(self, token, user_id, query):
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError()
            signed, signature = token.rsplit(".", 1)
            expected = hmac.new(self.secret, signed.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise ValueError()
            value = json.loads(base64.urlsafe_b64decode(signed + "=" * (-len(signed) % 4)))
            if value["query"] != self.digest(query) or value["user"] != self.digest(user_id):
                raise ValueError()
            if value["expiresAt"] < time.time():
                raise APIError(ErrorCode.CONFLICT, "搜索会话已过期，请重新搜索", 410)
            return value
        except APIError:
            raise
        except (ValueError, KeyError, TypeError):
            raise APIError.validation_error("Invalid search cursor") from None

    def search(self, user_id, query, *, cursor=None, limit=20):
        query = self.validate(query, limit)
        state = (
            self.decode_cursor(cursor, user_id, query)
            if cursor
            else {
                "pitId": self.client.open_pit(),
                "after": None,
                "query": self.digest(query),
                "user": self.digest(user_id),
                "expiresAt": time.time() + 110,
            }
        )
        try:
            return self._search(user_id, query, state, limit)
        except Exception:
            self.client.close_pit(state["pitId"])
            raise

    def _search(self, user_id, query, state, limit):
        items, seen, more = [], set(), False
        for _scan in range(5):
            key = "candidates:" + self.digest([user_id, query, state["pitId"], state["after"]])
            page = self.cache.get(
                key,
                lambda: self.client.candidates(
                    user_id, query, pit_id=state["pitId"], after=state["after"], size=100
                ),
                soft=10,
                hard=30,
                shared=False,
            )
            state["pitId"] = page["pitId"]
            hits = page["hits"]
            if not hits:
                more = False
                break
            authorized = self.policy.resolve_many(
                user_id, [h["contentId"] for h in hits], action="search"
            )
            for index, hit in enumerate(hits):
                state["after"] = hit["sort"]
                content_id = hit["contentId"]
                if content_id not in seen and content_id in authorized:
                    items.append(authorized[content_id])
                    seen.add(content_id)
                if len(items) >= limit:
                    more = index + 1 < len(hits) or len(hits) == 100
                    break
            if len(items) >= limit:
                break
            if len(hits) < 100:
                more = False
                break
            more = True
        return {
            "items": items,
            "query": query,
            "hasMore": more,
            "nextCursor": self.encode_cursor(state) if more else None,
        }

    def suggest(self, user_id, query, *, limit=8):
        query = self.validate(query, limit)

        def candidates():
            pit_id = self.client.open_pit()
            try:
                page = self.client.candidates(user_id, query, pit_id=pit_id, size=100, suggest=True)
                pit_id = page.get("pitId", pit_id)
                return {"ids": [h["contentId"] for h in page["hits"]]}
            finally:
                self.client.close_pit(pit_id)

        cached = self.cache.get(
            "suggest:" + self.digest([user_id, query]), candidates, soft=5, hard=15, shared=False
        )
        values = self.policy.resolve_many(user_id, cached["ids"], action="search")
        return {"query": query, "items": [values[c] for c in cached["ids"] if c in values][:limit]}
