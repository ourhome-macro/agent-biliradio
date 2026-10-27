"""Bound work before identity/resource SQL; streamed bodies use a separate budget."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

from error_code import APIError, ErrorCode

from .metrics import count

AUTHORIZATION_LIMIT = 16
_SLOTS = threading.BoundedSemaphore(AUTHORIZATION_LIMIT)
_DEPTH = ContextVar("media_authorization_depth", default=0)


@contextmanager
def media_admission():
    if _DEPTH.get():
        yield
        return
    if not _SLOTS.acquire(blocking=False):
        count("media_admission", "rejected")
        raise APIError(
            ErrorCode.CONFLICT, "Media authorization capacity exceeded; retry shortly", 503
        )
    token = _DEPTH.set(1)
    try:
        yield
    finally:
        _DEPTH.reset(token)
        _SLOTS.release()


def admitted(function):
    @wraps(function)
    def execute(*args, **kwargs):
        with media_admission():
            return function(*args, **kwargs)

    return execute
