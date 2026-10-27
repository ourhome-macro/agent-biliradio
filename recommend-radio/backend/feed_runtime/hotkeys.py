"""Bounded, demand-driven hot content detection and leased proactive refresh."""

from __future__ import annotations

import json
import math
import os
import random
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from uuid import uuid4

from .metrics import count

# All writes are bounded by the configured observation batch / registry capacity.
# Redis server time keeps rolling buckets and due times consistent across API hosts.
_COMMON = """
local tm=redis.call('TIME')
local now=tonumber(ARGV[1]) or (tonumber(tm[1])+tonumber(tm[2])/1000000)
local window=tonumber(ARGV[2]); local step=window/6
local bucket=math.floor(now/step)
local function total(key)
  local n=0
  for i=0,5 do
    local t=tonumber(redis.call('HGET',key,'t'..i) or '-1')
    if t>bucket-6 and t<=bucket then
      n=n+tonumber(redis.call('HGET',key,'b'..i) or '0')
    end
  end
  return n
end
"""
_OBSERVE = (
    _COMMON
    + """
local entries=cjson.decode(ARGV[3]); local prefix=ARGV[4]
local promote=tonumber(ARGV[5]); local capacity=tonumber(ARGV[6])
local tracked=tonumber(ARGV[7]); local ahead=tonumber(ARGV[8]); local promoted=0; local dropped=0
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-window*4)
for id,n in pairs(entries) do
  if redis.call('ZSCORE',KEYS[1],id) or redis.call('ZCARD',KEYS[1])<tracked then
    local key=prefix..':hot:data:'..id
    redis.call('ZADD',KEYS[1],now,id)
    local slot=bucket%6
    if tonumber(redis.call('HGET',key,'t'..slot) or '-1')~=bucket then
      redis.call('HSET',key,'t'..slot,bucket,'b'..slot,0)
    end
    redis.call('HINCRBY',key,'b'..slot,n)
    redis.call('EXPIRE',key,math.ceil(window*4))
    if total(key)>=promote and not redis.call('ZSCORE',KEYS[2],id)
       and redis.call('ZCARD',KEYS[2])<capacity then
      redis.call('HSET',key,'low',0,'evaluated',math.floor(now/window))
      redis.call('ZADD',KEYS[2],now+ahead,id)
      promoted=promoted+1
    end
  else dropped=dropped+1 end
end
redis.call('EXPIRE',KEYS[1],math.ceil(window*4))
return {promoted,dropped}
"""
)
_CLAIM = (
    _COMMON
    + """
local id=ARGV[3]; local score=redis.call('ZSCORE',KEYS[1],id)
if not score or tonumber(score)>now then return 0 end
if redis.call('EXISTS',KEYS[2])==0 then
  redis.call('ZREM',KEYS[1],id); return -1
end
local epoch=math.floor(now/window)
local prior=tonumber(redis.call('HGET',KEYS[2],'evaluated') or epoch)
if epoch>prior then
  local low=tonumber(redis.call('HGET',KEYS[2],'low') or '0')
  if total(KEYS[2])<tonumber(ARGV[4]) then low=low+1 else low=0 end
  redis.call('HSET',KEYS[2],'low',low,'evaluated',epoch)
  if low>=tonumber(ARGV[5]) then
    redis.call('ZREM',KEYS[1],id); return -1
  end
end
redis.call('ZREMRANGEBYSCORE',KEYS[4],'-inf',now)
if redis.call('ZCARD',KEYS[4])>=tonumber(ARGV[8]) then return 0 end
if not redis.call('SET',KEYS[3],ARGV[6],'NX','PX',ARGV[7]) then return 0 end
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[7])/1000,id)
redis.call('ZADD',KEYS[4],now+tonumber(ARGV[7])/1000,ARGV[6])
return 1
"""
)
_RENEW = """
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
local tm=redis.call('TIME')
local now=tonumber(ARGV[3]) or (tonumber(tm[1])+tonumber(tm[2])/1000000)
redis.call('PEXPIRE',KEYS[1],ARGV[2])
redis.call('ZADD',KEYS[2],now+tonumber(ARGV[2])/1000,ARGV[1]); return 1
"""
_FINISH = (
    _COMMON
    + """
if redis.call('GET',KEYS[2])~=ARGV[3] then return 0 end
redis.call('ZREM',KEYS[5],ARGV[3])
if not redis.call('ZSCORE',KEYS[1],ARGV[4]) then
  redis.call('DEL',KEYS[2]); return 0
end
if ARGV[5]=='retire' then
  redis.call('ZREM',KEYS[1],ARGV[4])
elseif ARGV[5]=='publish' then
  local data=cjson.decode(ARGV[6]); local life=tonumber(ARGV[7])
  local envelope={data=data,version=tonumber(ARGV[8]),
                  soft_at=now+tonumber(ARGV[9]),hard_at=now+life}
  redis.call('SET',KEYS[3],cjson.encode(envelope),'PX',math.floor(life*1000))
  redis.call('HSET',KEYS[4],'failures',0)
  redis.call('ZADD',KEYS[1],now+tonumber(ARGV[10]),ARGV[4])
else
  local failures=redis.call('HINCRBY',KEYS[4],'failures',1)
  local delay=math.min(tonumber(ARGV[11]),tonumber(ARGV[10])*2^math.min(failures,6))
  redis.call('ZADD',KEYS[1],now+delay,ARGV[4])
end
redis.call('DEL',KEYS[2]); return 1
"""
)
_INVALIDATE = """
local owner=redis.call('GET',KEYS[4])
if owner then redis.call('ZREM',KEYS[5],owner) end
redis.call('ZREM',KEYS[1],ARGV[1]); redis.call('ZREM',KEYS[2],ARGV[1])
redis.call('DEL',KEYS[3],KEYS[4]); return 1
"""


@dataclass(frozen=True)
class HotKeyConfig:
    enabled: bool = False
    window: float = 60
    promote: int = 600
    demote: int = 120
    cold_windows: int = 3
    capacity: int = 256
    tracked: int = 2048
    batch: int = 256
    tick: float = 1
    refresh: float = 45
    soft: float = 60
    hard: float = 300
    lease: float = 10
    timeout: float = 5
    workers: int = 2

    def __post_init__(self):
        values = (
            self.window,
            self.tick,
            self.refresh,
            self.soft,
            self.hard,
            self.lease,
            self.timeout,
        )
        if not all(math.isfinite(v) and v > 0 for v in values):
            raise ValueError("Hot key timers must be finite and positive")
        if not (
            0 < self.demote < self.promote
            and self.cold_windows > 0
            and 0 < self.capacity <= min(self.tracked, 4096)
            and 0 < self.batch <= min(self.tracked, 512)
            and self.tracked <= 65536
            and 0 < self.workers <= 16
            and self.refresh < min(self.soft, self.hard * 0.8)
            and self.soft <= self.hard
            and self.tick < self.refresh
            and self.timeout < self.lease
            and self.refresh < self.window
        ):
            raise ValueError("Invalid hot key capacity, thresholds or deadlines")

    @classmethod
    def from_env(cls):
        return cls(
            enabled=os.getenv("FEED_HOTKEY_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            promote=int(os.getenv("FEED_HOTKEY_PROMOTE", "600")),
            demote=int(os.getenv("FEED_HOTKEY_DEMOTE", "120")),
            capacity=int(os.getenv("FEED_HOTKEY_CAPACITY", "256")),
        )


class HotKeys:
    def __init__(self, cache, loader, config, *, clock=None, background=True):
        self.cache, self.loader, self.config = weakref.proxy(cache), loader, config
        self.prefix = cache.prefix
        self.clock = clock  # Deterministic tests only; production uses Redis TIME.
        self.pending = {}
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.pool = ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="feed-hot")
        self.slots = threading.BoundedSemaphore(config.workers)
        if background:
            self.thread = threading.Thread(target=self._run, daemon=True, name="feed-hotkeys")
            self.thread.start()

    def _time(self):
        return "" if self.clock is None else self.clock()

    def _eval(self, script, keys, *args):
        return self.cache._redis_call("eval", script, len(keys), *keys, *args)

    @property
    def due(self):
        return self.prefix + ":hot:due"

    @property
    def tracked(self):
        return self.prefix + ":hot:tracked"

    @property
    def active(self):
        return self.prefix + ":hot:active"

    def _data(self, identity):
        return self.prefix + ":hot:data:" + identity

    def _lease(self, identity):
        return self.prefix + ":hot:lease:" + identity

    def observe(self, identity):
        if (
            not isinstance(identity, str)
            or not identity
            or len(identity) > 180
            or any(c in identity for c in "*?[]:")
        ):
            return
        with self.lock:
            if self.stop.is_set():
                return
            if identity in self.pending or len(self.pending) < self.config.tracked:
                self.pending[identity] = min(self.pending.get(identity, 0) + 1, 2**31 - 1)
            else:
                count("hotkey", "observation_dropped")

    def flush(self):
        # Bounded best-effort telemetry: never replay outage traffic as current demand.
        with self.lock:
            pending, self.pending = self.pending, {}
        entries = list(pending.items())
        c = self.config
        for offset in range(0, len(entries), c.batch):
            promoted, dropped = self._eval(
                _OBSERVE,
                [self.tracked, self.due],
                self._time(),
                c.window,
                json.dumps(dict(entries[offset : offset + c.batch])),
                self.prefix,
                c.promote,
                c.capacity,
                c.tracked,
                c.refresh * random.uniform(0.9, 1.0),
            )
            if promoted:
                count("hotkey", "promoted", promoted)
            if dropped:
                count("hotkey", "registry_full", dropped)

    def claim(self, identity):
        c, token = self.config, uuid4().hex
        result = self._eval(
            _CLAIM,
            [self.due, self._data(identity), self._lease(identity), self.active],
            self._time(),
            c.window,
            identity,
            c.demote,
            c.cold_windows,
            token,
            int(c.lease * 1000),
            c.workers,
        )
        if result == -1:
            count("hotkey", "demoted")
        return token if result == 1 else None

    def refresh_one(self, identity, token):
        c = self.config
        started = time.monotonic()
        done = threading.Event()
        lost = threading.Event()

        def renew():
            while not done.wait(c.lease / 3):
                try:
                    if (
                        self.stop.is_set()
                        or time.monotonic() - started >= c.timeout
                        or not self._eval(
                            _RENEW,
                            [self._lease(identity), self.active],
                            token,
                            int(c.lease * 1000),
                            self._time(),
                        )
                    ):
                        lost.set()
                        return
                except Exception:
                    lost.set()
                    return

        heartbeat = threading.Thread(target=renew, daemon=True, name="feed-hot-lease")
        heartbeat.start()
        try:
            value = self.loader(identity)
            if self.stop.is_set() or lost.is_set() or time.monotonic() - started >= c.timeout:
                count("hotkey", "refresh_deadline")
                return
            if value is None:
                mode, key, data, version = "retire", self._data(identity), {}, 0
            else:
                key, data, version = value
                mode = "publish"
            result = self._eval(
                _FINISH,
                [self.due, self._lease(identity), key, self._data(identity), self.active],
                self._time(),
                c.window,
                token,
                identity,
                mode,
                json.dumps(data),
                c.hard * random.uniform(0.8, 1.0),
                version,
                min(c.soft, c.hard * 0.8),
                c.refresh * random.uniform(0.9, 1.0),
                c.window,
            )
            count("hotkey", "refreshed" if result and value else "retired" if result else "fenced")
        except Exception:
            count("hotkey", "refresh_error")
            try:
                self._eval(
                    _FINISH,
                    [
                        self.due,
                        self._lease(identity),
                        self._data(identity),
                        self._data(identity),
                        self.active,
                    ],
                    self._time(),
                    c.window,
                    token,
                    identity,
                    "failure",
                    "{}",
                    c.hard,
                    0,
                    c.soft,
                    c.refresh,
                    c.window,
                )
            except Exception:
                pass  # Due score remains at lease expiry; another process can recover it.
        finally:
            done.set()
            heartbeat.join(timeout=1)

    def tick_once(self):
        self.flush()
        if self.clock:
            now = self.clock()
        else:
            seconds, microseconds = self.cache._redis_call("time")
            now = float(seconds) + float(microseconds) / 1_000_000
        identities = (
            self.cache._redis_call(
                "zrangebyscore", self.due, "-inf", now, start=0, num=self.config.batch
            )
            or []
        )
        for identity in identities:
            if not self.slots.acquire(blocking=False):
                break
            try:
                token = self.claim(identity)
                if token:
                    self.pool.submit(self._task, identity, token)
                else:
                    self.slots.release()
            except Exception:
                self.slots.release()
                raise

    def _task(self, identity, token):
        try:
            self.refresh_one(identity, token)
        finally:
            self.slots.release()

    def _run(self):
        while not self.stop.wait(self.config.tick):
            try:
                self.tick_once()
            except Exception:
                count("hotkey", "tick_error")

    def invalidate(self, identity):
        with self.lock:
            self.pending.pop(identity, None)
        self._eval(
            _INVALIDATE,
            [self.due, self.tracked, self._data(identity), self._lease(identity), self.active],
            identity,
        )

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=2)
        self.pool.shutdown(wait=True, cancel_futures=True)
