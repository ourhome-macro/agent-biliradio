import os

from database import DEFAULT_DB_PATH
from error_code import APIError, ErrorCode
from flask import Blueprint, g, request
from result import Result

from .service import UGCService


def register_ugc(app):
    blueprint = Blueprint("ugc_posts", __name__)

    @blueprint.before_request
    def enabled():
        if os.getenv("UGC_ENABLED", "false").lower() not in {"1", "true", "yes", "on"}:
            raise APIError.not_found("UGC is disabled")

    @blueprint.after_request
    def private_response(response):
        response.headers["Cache-Control"] = "private, no-store"
        return response

    def current():
        return UGCService(DEFAULT_DB_PATH), str(g.current_user["id"])

    def uploads():
        from feed_runtime.config import FeedConfig
        from feed_runtime.services import resources
        from .uploads import AudioUploads
        config = FeedConfig.from_env()
        if not config.minio_access_key or not config.minio_secret_key:
            raise APIError(ErrorCode.CONFLICT, 'Audio storage is not configured', 503)
        _cache, storage = resources(config, DEFAULT_DB_PATH)
        return AudioUploads(DEFAULT_DB_PATH, storage), str(g.current_user['id'])

    @blueprint.post('/api/ugc/posts/<content_id>/uploads')
    def initiate_upload(content_id):
        service, user = uploads()
        return Result.ok(service.initiate(user, content_id, request.get_json(silent=True),
                                         request.headers.get('Idempotency-Key'))).json_with_status(201)

    @blueprint.post('/api/ugc/uploads/<upload_id>/complete')
    def complete_upload(upload_id):
        service, user = uploads()
        return Result.ok(service.complete(user, upload_id)).json_with_status(202)

    @blueprint.get('/api/ugc/uploads/<upload_id>')
    def upload_status(upload_id):
        service, user = uploads()
        return Result.ok(service.get(user, upload_id)).json()

    def command(operation, content_id=None):
        service, user = current()
        result = service.command(
            user,
            operation,
            request.get_json(silent=True),
            request.headers.get("Idempotency-Key"),
            content_id,
        )
        return Result.ok(result).json_with_status(201 if operation == "create" else 200)

    @blueprint.get("/api/ugc/posts")
    def listing():
        service, user = current()
        return Result.ok(service.own_posts(user)).json()

    @blueprint.post("/api/ugc/posts")
    def create():
        return command("create")

    @blueprint.get("/api/ugc/posts/<content_id>")
    def detail(content_id):
        service, user = current()
        return Result.ok(service.detail(user, content_id)).json()

    @blueprint.patch("/api/ugc/posts/<content_id>")
    def edit(content_id):
        return command("edit", content_id)

    @blueprint.post("/api/ugc/posts/<content_id>/publish")
    def publish(content_id):
        return command("publish", content_id)

    @blueprint.delete("/api/ugc/posts/<content_id>")
    def delete(content_id):
        return command("delete", content_id)

    @blueprint.get("/api/ugc/media-candidates")
    def media():
        service, user = current()
        return Result.ok(service.media_candidates(user)).json()

    app.register_blueprint(blueprint)
