from error_code import APIError, ErrorCode
from flask import Blueprint, g, request
from result import Result

from .product import FeedProductService


def register_feed_product(app, *, services_factory):
    blueprint = Blueprint("feed_product", __name__)

    def service():
        runtime = services_factory()
        return FeedProductService(runtime.repo, runtime.config), runtime.user_id

    @blueprint.before_request
    def acquire_capacity():
        slots = services_factory().cache.request_slots
        if not slots.acquire(timeout=0.25):
            raise APIError(ErrorCode.CONFLICT, "Feed request capacity exceeded; retry shortly", 503)
        g._feed_product_slots = slots

    @blueprint.teardown_request
    def release_capacity(_error):
        slots = g.pop("_feed_product_slots", None)
        if slots is not None:
            slots.release()

    @blueprint.after_request
    def private_response(response):
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @blueprint.get("/api/content/<content_id>")
    def detail(content_id):
        product, user = service()
        return Result.ok(product.detail(user, content_id)).json()

    @blueprint.post("/api/feed/content/<content_id>/open")
    def open_content(content_id):
        product, user = service()
        return Result.ok(product.open(user, content_id)).json_with_status(201)

    @blueprint.get("/api/feed/history")
    def history():
        product, user = service()
        try:
            limit = int(request.args.get("limit", "20"))
        except ValueError:
            raise APIError.validation_error("Invalid limit") from None
        return Result.ok(
            product.history(user, cursor=request.args.get("cursor"), limit=limit)
        ).json()

    app.register_blueprint(blueprint)
