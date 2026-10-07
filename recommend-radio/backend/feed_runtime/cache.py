from __future__ import annotations

import json
import random
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from uuid import uuid4

from error_code import APIError, ErrorCode
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from .bitmap import BITMAP_TTL, PUBLISH, READ, bitmap_keys, validate_reaction
from .metrics import count

_UNLOCK = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0"
_PUBLISH = """
local current=redis.call('get',KEYS[1])
if current then
  local ok,v=pcall(cjson.decode,current)
  if ok and tonumber(v.version)>tonumber(ARGV[1]) then return 0 end
end
redis.call('set',KEYS[1],ARGV[2],'EX',ARGV[3]); return 1
"""


@dataclass
class _Flight:
    event: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None
    error: Exception | None = None


class FeedCache:
    """Versioned read projections. Leases/cancellation never depend on this cache."""

    def __init__(self, url: str | None, *, prefix: str = "radio:feed", client=None):
        self.prefix = prefix
        if client is not None:
            self.redis = client
        elif url:
            from redis import Redis
            from redis.backoff import NoBackoff
            from redis.retry import Retry

            self.redis = Redis.from_url(
                url,
                decode_responses=True,
                socket_timeout=0.15,
                socket_connect_timeout=0.15,
                max_connections=16,
                retry=Retry(NoBackoff(), 0),
                retry_on_timeout=False,
            )
        else:
            self.redis = None
        self._local: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._lock = threading.Lock()
        self._flights: dict[str, _Flight] = {}
        self._fallback = threading.BoundedSemaphore(8)
        self.request_slots = threading.BoundedSemaphore(8)
        self._refresh = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feed-cache")
        self._redis_state_lock = threading.Lock()
        self._redis_retry_at = 0.0
        self._redis_probe_running = False

    def _redis_call(self, command, *args, probe=False, **kwargs):
        if self.redis is None:
            return None
        with self._redis_state_lock:
            recovering = self._redis_retry_at > 0
            if recovering and (
                self._redis_probe_running or (not probe and time.monotonic() < self._redis_retry_at)
            ):
                raise RedisConnectionError("Feed cache Redis circuit is temporarily open")
            if recovering:
                self._redis_probe_running = True
        try:
            value = getattr(self.redis, command)(*args, **kwargs)
        except (RedisConnectionError, RedisTimeoutError, OSError):
            with self._redis_state_lock:
                self._redis_retry_at = time.monotonic() + 2.0
            raise
        else:
            with self._redis_state_lock:
                self._redis_retry_at = 0.0
            return value
        finally:
            if recovering:
                with self._redis_state_lock:
                    self._redis_probe_running = False

    def health(self) -> bool:
        try:
            return self.redis is not None and bool(self._redis_call("ping", probe=True))
        except Exception:
            return False

    def get(
        self,
        key: str,
        loader,
        *,
        version: int = 1,
        soft: float = 60,
        hard: float = 300,
        shared: bool = False,
    ) -> dict:
        cache_key = f"{self.prefix}:{key}:v{version}"
        now = time.time()
        if shared:
            with self._lock:
                cached = self._local.get(cache_key)
                if cached and cached[0] > now:
                    count("cache", "local_hit")
                    return dict(cached[1])
        value = None
        try:
            raw = self._redis_call("get", cache_key)
            value = json.loads(raw) if raw else None
        except Exception:
            count("cache", "redis_error")
        if value and value["hard_at"] > now:
            result = value["data"]
            count("cache", "redis_hit")
            if shared:
                self._remember(cache_key, result)
            if value["soft_at"] <= now:
                with self._lock:
                    if cache_key not in self._flights:
                        self._flights[cache_key] = _Flight()
                        self._refresh.submit(
                            self._load, cache_key, loader, version, soft, hard, shared
                        )
            return dict(result)
        with self._lock:
            flight = self._flights.get(cache_key)
            leader = flight is None
            if leader:
                if len(self._flights) >= 2048:
                    raise APIError(ErrorCode.CONFLICT, "Feed query capacity exceeded", 503)
                flight = self._flights[cache_key] = _Flight()
        if not leader:
            count("cache", "coalesced")
            if not flight.event.wait(1.0):
                raise APIError(ErrorCode.CONFLICT, "Feed is busy; retry shortly", 503)
            if flight.error:
                raise flight.error
            if flight.result is not None:
                return dict(flight.result)
        return self._load(cache_key, loader, version, soft, hard, shared)

    def _load(self, key, loader, version, soft, hard, shared):
        lock_key = key + ":refresh"
        owner = uuid4().hex
        acquired = False
        result = None
        failure = None
        try:
            if self.redis:
                try:
                    acquired = bool(self._redis_call("set", lock_key, owner, nx=True, ex=5))
                    if not acquired:
                        for _ in range(20):
                            time.sleep(0.025)
                            raw = self._redis_call("get", key)
                            if raw:
                                value = json.loads(raw)
                                if value["hard_at"] > time.time():
                                    result = dict(value["data"])
                                    return result
                        raise APIError(
                            ErrorCode.CONFLICT, "Feed refresh is busy; retry shortly", 503
                        )
                except APIError:
                    raise
                except Exception:
                    count("cache", "redis_error")
            if not self._fallback.acquire(blocking=False):
                raise APIError(ErrorCode.CONFLICT, "Feed query capacity exceeded", 503)
            try:
                result = dict(loader())
            finally:
                self._fallback.release()
            now = time.time()
            # Jitter never moves a snapshot beyond its caller-supplied hard deadline.
            life = max(0.1, hard * random.uniform(0.8, 1.0))
            envelope = {
                "version": version,
                "soft_at": now + min(soft, life),
                "hard_at": now + life,
                "data": result,
            }
            try:
                if self.redis:
                    self._redis_call(
                        "eval", _PUBLISH, 1, key, version, json.dumps(envelope), max(1, int(life))
                    )
            except Exception:
                count("cache", "redis_error")
            if shared:
                self._remember(key, result)
            count("cache", "miss")
            return result
        except Exception as error:
            failure = error
            raise
        finally:
            if acquired:
                try:
                    self._redis_call("eval", _UNLOCK, 1, lock_key, owner)
                except Exception:
                    count("cache", "redis_error")
            with self._lock:
                flight = self._flights.pop(key, None)
                if flight:
                    flight.result, flight.error = result, failure
                    flight.event.set()

    def _remember(self, key, result):
        with self._lock:
            self._local[key] = (time.time() + 2, dict(result))
            self._local.move_to_end(key)
            while len(self._local) > 2048:
                self._local.popitem(last=False)

    def publish_counter(self, content_id: str, value: dict) -> None:
        try:
            if self.redis:
                self._redis_call(
                    "eval",
                    _PUBLISH,
                    1,
                    f"{self.prefix}:counter:{content_id}",
                    value["version"],
                    json.dumps(value),
                    300,
                )
        except Exception:
            count("projection", "redis_error")
            raise

    def publish_reaction(
        self, user_id: str, content_id: str, state: str, version: int, *, bitmap_id: int
    ):
        validate_reaction(state, version)
        keys, offset = bitmap_keys(self.prefix, content_id, bitmap_id)
        try:
            if self.redis:
                return bool(
                    self._redis_call(
                        "eval",
                        PUBLISH,
                        4,
                        *keys,
                        offset,
                        str(version),
                        state,
                        BITMAP_TTL,
                    )
                )
            return False
        except Exception:
            count("projection", "redis_error")
            raise

    def reaction(
        self, user_id: str, content_id: str, loader, *, bitmap_id: int | None, expected_version: int
    ):
        if bitmap_id is not None:
            keys, offset = bitmap_keys(self.prefix, content_id, bitmap_id)
            try:
                value = self._redis_call("eval", READ, 4, *keys, offset)
                # Bind the projection to the SQL relationship snapshot. A stale
                # or even newer cached state cannot alter that authorized result.
                if value and int(value[0]) == expected_version:
                    state = value[1].decode("ascii") if isinstance(value[1], bytes) else value[1]
                    validate_reaction(state, int(value[0]))
                    count("reaction", "bitmap_hit")
                    return {"version": int(value[0]), "state": state}
            except Exception:
                count("reaction", "redis_error")
        if not self._fallback.acquire(blocking=False):
            raise APIError(ErrorCode.CONFLICT, "Reaction query capacity exceeded", 503)
        try:
            result = dict(loader())
            validate_reaction(result["state"], result["version"])
        finally:
            self._fallback.release()
        if bitmap_id is not None:
            try:
                self.publish_reaction(
                    user_id,
                    content_id,
                    result["state"],
                    result["version"],
                    bitmap_id=bitmap_id,
                )
            except Exception:
                pass
        count("reaction", "bitmap_miss")
        return result

    def counter(self, content_id: str, loader, *, min_version=0):
        try:
            raw = self._redis_call("get", f"{self.prefix}:counter:{content_id}")
            if raw and int(json.loads(raw).get("version", 0)) >= min_version:
                count("counter", "redis_hit")
                return json.loads(raw)
        except Exception:
            count("counter", "redis_error")
        if not self._fallback.acquire(blocking=False):
            raise APIError(ErrorCode.CONFLICT, "Counter query capacity exceeded", 503)
        try:
            value = dict(loader())
        finally:
            self._fallback.release()
        try:
            self.publish_counter(content_id, value)
        except Exception:
            pass
        return value

    def close(self):
        self._refresh.shutdown(wait=False, cancel_futures=True)
        if self.redis:
            self.redis.close()
