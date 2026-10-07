"""Serve Flask, replayable SSE and the built Vue frontend from one ASGI process."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress
from pathlib import Path

from a2wsgi import WSGIMiddleware
from app import app as flask_app
from app import oidc_auth
from database import DEFAULT_DB_PATH
from sse_event_store import PAGE_SIZE, latest_event_id, list_events_after, prune_events
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, StreamingResponse, RedirectResponse, Response

FRONTEND_ROOT = Path(
    os.getenv("FRONTEND_DIST", str(Path(__file__).resolve().parents[1] / "frontend" / "dist"))
).resolve()
LOGGER = logging.getLogger("recommend-radio.sse")


def _cursor(value: str | None) -> int:
    try:
        return max(0, int(value or "0"))
    except ValueError:
        return 0


class RadioASGI:
    def __init__(self, *, db_path: str | Path | None = None, frontend_root: Path | None = None):
        self.db_path = str(db_path or DEFAULT_DB_PATH)
        self.frontend_root = (frontend_root or FRONTEND_ROOT).resolve()
        self.flask_app = WSGIMiddleware(flask_app, workers=16)
        self._prune_task: asyncio.Task | None = None

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] != "http":
            await JSONResponse({"error": "unsupported request"}, status_code=404)(
                scope, receive, send
            )
            return
        path = scope.get("path", "")
        if path == "/api/agent/events":
            response = await self._stream(Request(scope, receive=receive))
        elif path.startswith("/api/media/streams/"):
            response = await self._media(Request(scope, receive=receive))
        elif path.startswith("/api/") or path.startswith("/health/") or path == "/metrics":
            await self.flask_app(scope, receive, send)
            return
        else:
            response = self._frontend(path)
        await response(scope, receive, send)

    async def _media(self, request: Request):
        from error_code import APIError
        from feed_runtime.reactions import feed_enabled
        from feed_runtime.services import FeedServices
        if not feed_enabled():
            return JSONResponse({"error":"video feed is disabled"},status_code=404)
        if request.method not in {"GET","HEAD"}:
            return JSONResponse({"error":"method not allowed"},status_code=405)
        trusted_hosts = flask_app.config.get("TRUSTED_HOSTS")
        if trusted_hosts and request.url.hostname not in trusted_hosts:
            return JSONResponse({"error":"untrusted host"},status_code=400)
        user = await asyncio.to_thread(oidc_auth.current_user,
            request.cookies.get(oidc_auth.cookie_name),touch=False)
        if user is None:
            return JSONResponse({"error":"authentication required"},status_code=401)
        parts=request.url.path.split("/")
        if len(parts)!=6:
            return JSONResponse({"error":"media file not found"},status_code=404)
        try:
            runtime=await asyncio.to_thread(FeedServices,user_id=str(user["id"]),db_path=self.db_path)
            result=await asyncio.to_thread(runtime.media.stream_file,parts[4],str(user["id"]),parts[5])
        except APIError as error:
            return JSONResponse({"error":error.message},status_code=error.status_code)
        except Exception as error:
            LOGGER.warning("Media response failed: %s",type(error).__name__)
            return JSONResponse({"error":"media temporarily unavailable"},status_code=503)
        headers={"Cache-Control":"private, no-store","X-Content-Type-Options":"nosniff"}
        if "redirect" in result:
            return RedirectResponse(result["redirect"],status_code=307,headers=headers)
        if "text" in result:
            return Response(result["text"],media_type=result["mime"],headers=headers)
        return FileResponse(result["path"],media_type=result["mime"],headers=headers)

    async def _stream(self, request: Request):
        trusted_hosts = flask_app.config.get("TRUSTED_HOSTS")
        host = request.url.hostname
        if trusted_hosts and host not in trusted_hosts:
            return JSONResponse({"error": "untrusted host"}, status_code=400)
        session_id = request.query_params.get("sessionId", "").strip()
        if not session_id:
            return JSONResponse({"error": "sessionId is required"}, status_code=400)
        user = await asyncio.to_thread(
            oidc_auth.current_user,
            request.cookies.get(oidc_auth.cookie_name),
            touch=False,
        )
        if user is None:
            return JSONResponse({"error": "authentication required"}, status_code=401)
        user_id = str(user["id"])
        after = max(
            _cursor(request.headers.get("last-event-id")),
            _cursor(request.query_params.get("lastEventId")),
        )
        if after == 0:
            after = await asyncio.to_thread(
                latest_event_id, self.db_path, user_id=user_id, session_id=session_id
            )

        async def stream():
            nonlocal after
            yield "retry: 3000\n\n"
            elapsed = 0.0
            while True:
                batch = await asyncio.to_thread(
                    list_events_after,
                    self.db_path,
                    user_id=user_id,
                    session_id=session_id,
                    after_id=after,
                )
                for event in batch:
                    after = event["eventId"]
                    yield (
                        f"id: {after}\nevent: {event['type']}\n"
                        f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    )
                if len(batch) == PAGE_SIZE:
                    continue
                await asyncio.sleep(0.5)
                elapsed += 0.5
                if elapsed >= 15:
                    yield ": heartbeat\n\n"
                    elapsed = 0.0

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    def _frontend(self, path: str):
        target = (self.frontend_root / path.lstrip("/")).resolve()
        if target.is_relative_to(self.frontend_root) and target.is_file():
            return FileResponse(target)
        index = self.frontend_root / "index.html"
        if index.is_file() and not path.startswith("/api/"):
            return FileResponse(index)
        return JSONResponse({"error": "frontend build is missing"}, status_code=404)

    async def _lifespan(self, receive, send):
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                self._prune_task = asyncio.create_task(self._prune_loop())
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                if self._prune_task is not None:
                    self._prune_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await self._prune_task
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _prune_loop(self):
        while True:
            await asyncio.sleep(3600)
            try:
                await asyncio.to_thread(
                    prune_events,
                    self.db_path,
                    retention_days=int(os.getenv("SSE_EVENT_RETENTION_DAYS", "7")),
                )
            except Exception:
                LOGGER.exception("SSE event pruning failed")


app = RadioASGI()
