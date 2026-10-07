from __future__ import annotations

from error_code import APIError, ErrorCode
from flask import Blueprint, g, redirect, request
from result import Result

from .reactions import feed_enabled
from .services import FeedServices


def register_feed(app, *, music_services_factory):
    blueprint = Blueprint("music_feed", __name__)

    def services():
        if not feed_enabled():
            raise APIError.not_found("Video feed is disabled")
        value = getattr(g, "_feed_services", None)
        if value is None:
            value = g._feed_services = FeedServices(
                user_id=str(g.current_user["id"]),
                recommendations=music_services_factory().recommendations,
            )
        return value

    def body():
        value = request.get_json(silent=True)
        if not isinstance(value, dict):
            raise APIError.validation_error("JSON object is required")
        return value

    @blueprint.before_request
    def acquire_capacity():
        if not feed_enabled():
            return None
        slots = services().cache.request_slots
        if not slots.acquire(blocking=False):
            raise APIError(ErrorCode.CONFLICT, "Feed request capacity exceeded; retry shortly", 503)
        g._feed_request_slots = slots

    @blueprint.teardown_request
    def release_capacity(_error):
        slots = g.pop("_feed_request_slots", None)
        if slots is not None:
            slots.release()

    def command_key():
        key = request.headers.get("Idempotency-Key")
        if not key:
            raise APIError.validation_error("Idempotency-Key is required")
        return key

    @blueprint.get("/api/feed/health")
    def health():
        if not feed_enabled():
            return Result.ok({"enabled": False}).json()
        status = services().health()
        return Result.ok(status).json_with_status(200 if all(status.values()) else 503)

    @blueprint.post("/api/feed/sessions")
    def create_feed():
        payload = body()
        runtime = services()
        return Result.ok(
            runtime.feed.create(
                runtime.user_id,
                mode=payload.get("mode", "personal"),
                request_text=payload.get("requestText", ""),
                request_key=command_key(),
            )
        ).json_with_status(201)

    @blueprint.get("/api/feed/sessions/<session_id>/items")
    def feed_page(session_id):
        runtime = services()
        return Result.ok(
            runtime.feed.page(runtime.user_id, session_id, cursor=request.args.get("cursor"))
        ).json()

    @blueprint.post("/api/media/playbacks")
    def create_playback():
        runtime = services()
        result = runtime.media.create(
            runtime.user_id, str(body().get("contentId") or ""), command_key()
        )
        return Result.ok(result).json_with_status(202 if result["status"] == "preparing" else 201)

    @blueprint.get("/api/media/playbacks/<playback_id>")
    def playback(playback_id):
        runtime = services()
        return Result.ok(runtime.media.descriptor(playback_id, runtime.user_id)).json()

    @blueprint.post("/api/media/playbacks/<playback_id>/heartbeat")
    def heartbeat(playback_id):
        runtime = services()
        return Result.ok(runtime.media.heartbeat(playback_id, runtime.user_id)).json()

    @blueprint.delete("/api/media/playbacks/<playback_id>")
    def release(playback_id):
        runtime = services()
        return Result.ok(runtime.media.release(playback_id, runtime.user_id)).json()

    @blueprint.put("/api/content/<content_id>/reaction")
    def reaction(content_id):
        runtime = services()
        payload = body()
        return Result.ok(
            runtime.reactions.set(
                runtime.user_id,
                content_id,
                payload.get("state"),
                command_key(),
                expected_version=payload.get("expectedVersion"),
            )
        ).json()

    @blueprint.post("/api/feed/events")
    def events():
        runtime = services()
        return Result.ok(
            runtime.feed.events(runtime.user_id, body().get("events"))
        ).json_with_status(202)

    @blueprint.get("/api/content/<content_id>/cover")
    def cover(content_id):
        import json

        from database import get_connection

        runtime = services()
        runtime.repo.content(content_id, runtime.user_id)
        with get_connection(runtime.repo.db_path) as conn:
            row = conn.execute(
                """SELECT a.manifest_json FROM media_assets a JOIN content_media_links l
                ON l.asset_id=a.asset_id WHERE l.content_id=? AND a.status='ready'""",
                (content_id,),
            ).fetchone()
        entry = json.loads(row[0]).get("files", {}).get("cover.jpg") if row else None
        if not entry:
            raise APIError.not_found("Cover not ready")
        response = redirect(
            runtime.storage.presign(entry["key"], version_id=entry.get("versionId")), code=307
        )
        response.headers["Cache-Control"] = "private, no-store"
        return response

    app.register_blueprint(blueprint)
