from __future__ import annotations

import os
from functools import lru_cache

from database import DEFAULT_DB_PATH, database_identity
from error_code import APIError, ErrorCode
from feed_runtime.cache import FeedCache
from flask import Blueprint, g, request
from result import Result

from .elastic import ElasticClient, SearchSessionExpired, SearchUnavailable
from .service import ContentSearchService


def enabled(name):
    return os.getenv(name, "false").lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=4)
def resources(db_identity, redis_url):
    return ElasticClient(), FeedCache(redis_url, prefix="radio:search:" + db_identity[:12])


def register_content_search(app):
    blueprint = Blueprint("content_search", __name__)

    @blueprint.get("/api/content/capabilities")
    def capabilities():
        return Result.ok(
            {
                "ugcEnabled": enabled("UGC_ENABLED"),
                "contentSearchEnabled": enabled("CONTENT_SEARCH_ENABLED"),
            }
        ).json()

    def execute(operation):
        if not enabled("CONTENT_SEARCH_ENABLED"):
            raise APIError.not_found("Platform content search is disabled")
        client, cache = resources(
            database_identity(DEFAULT_DB_PATH),
            os.getenv("FEED_REDIS_URL", "redis://127.0.0.1:6379/0"),
        )
        if not cache.request_slots.acquire(timeout=0.25):
            raise APIError(ErrorCode.CONFLICT, "Search is busy; retry shortly", 503)
        try:
            service = ContentSearchService(DEFAULT_DB_PATH, client, cache, secret=app.secret_key)
            try:
                limit = int(request.args.get("limit", "8" if operation == "suggest" else "20"))
            except ValueError:
                raise APIError.validation_error("Invalid search limit") from None
            kwargs = {"limit": limit}
            if operation == "search":
                kwargs["cursor"] = request.args.get("cursor")
            result = getattr(service, operation)(
                str(g.current_user["id"]), request.args.get("q", ""), **kwargs
            )
            return Result.ok(result).json()
        except SearchSessionExpired:
            raise APIError(ErrorCode.CONFLICT, "搜索会话已过期，请重新搜索", 410) from None
        except SearchUnavailable:
            raise APIError(ErrorCode.CONFLICT, "搜索服务暂时不可用，请稍后重试", 503) from None
        finally:
            cache.request_slots.release()

    @blueprint.get("/api/content/search")
    def search():
        return execute("search")

    @blueprint.get("/api/content/suggest")
    def suggest():
        return execute("suggest")

    @blueprint.after_request
    def private(response):
        response.headers["Cache-Control"] = "private, no-store"
        return response

    app.register_blueprint(blueprint)
