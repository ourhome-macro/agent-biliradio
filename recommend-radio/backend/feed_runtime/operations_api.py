from error_code import APIError
from flask import Blueprint, g, request
from result import Result

from .operations import FeedOperations
from .services import FeedServices


def register_feed_operations(app):
    blueprint = Blueprint("feed_operations", __name__)

    @blueprint.before_request
    def administrator():
        if not getattr(g, "current_user", None) or g.current_user.get("role") != "admin":
            raise APIError.forbidden("Administrator access is required")

    def service():
        return FeedOperations(FeedServices(user_id=str(g.current_user["id"])))

    @blueprint.get("/api/admin/feed/operations")
    def summary():
        return Result.ok(service().summary()).json()

    @blueprint.put("/api/admin/feed/content/<content_id>/status")
    def status(content_id):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise APIError.validation_error("JSON object is required")
        return Result.ok(
            service().set_status(
                content_id,
                payload.get("status"),
                actor_id=str(g.current_user["id"]),
                reason=payload.get("reason"),
            )
        ).json()

    @blueprint.post("/api/admin/feed/imports/<import_id>/retry")
    def retry(import_id):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise APIError.validation_error("JSON object is required")
        return Result.ok(
            service().retry(
                import_id, actor_id=str(g.current_user["id"]), reason=payload.get("reason")
            )
        ).json()

    @blueprint.after_request
    def private_response(response):
        response.headers["Cache-Control"] = "private, no-store"
        return response

    app.register_blueprint(blueprint)
