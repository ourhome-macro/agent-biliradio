from database import DEFAULT_DB_PATH
from error_code import APIError
from flask import Blueprint, g, request
from result import Result

from .service import FollowingService
from .sync import enqueue_probe, start_sync


def register_following(app):
    blueprint = Blueprint("creator_following", __name__)

    def service():
        return FollowingService(DEFAULT_DB_PATH), str(g.current_user["id"])

    @blueprint.after_request
    def private_response(response):
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @blueprint.get("/api/content/<content_id>/creator")
    def content_creator(content_id):
        value, user = service()
        state = value.content_creator(user, content_id)
        if state.get("creatorId") and state.get("supported"):
            enqueue_probe(value, user, state["creatorId"])
        return Result.ok(state).json()

    @blueprint.get("/api/creators/<creator_id>/follow")
    def creator_follow(creator_id):
        value, user = service()
        state = value.state(user, creator_id)
        if state["supported"]:
            enqueue_probe(value, user, creator_id)
        return Result.ok(state).json()

    @blueprint.get("/api/bili/users/<int:mid>/creator")
    def author(mid):
        value, user = service()
        state = value.author_creator(user, mid)
        enqueue_probe(value, user, state["creatorId"])
        return Result.ok(state).json()

    @blueprint.put("/api/creators/<creator_id>/follow")
    def set_follow(creator_id):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise APIError.validation_error("JSON object is required")
        value, user = service()
        return Result.ok(
            value.set(
                user,
                creator_id,
                payload.get("following"),
                request.headers.get("Idempotency-Key"),
                expected_version=payload.get("expectedVersion"),
            )
        ).json_with_status(202)

    @blueprint.get("/api/following")
    def following():
        value, user = service()
        return Result.ok(value.list(user)).json()

    @blueprint.post("/api/following/sync")
    def sync():
        value, user = service()
        return Result.ok(
            start_sync(value, user, request.headers.get("Idempotency-Key"))
        ).json_with_status(202)

    app.register_blueprint(blueprint)
