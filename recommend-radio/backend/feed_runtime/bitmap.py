"""Rebuildable, versioned reaction bitmaps. SQL remains the relationship authority."""

from __future__ import annotations

import hashlib

from database import begin_write, get_connection

SHARD_BITS = 4096
BITMAP_TTL = 300
STATES = {"neutral": (0, 0), "like": (1, 0), "dislike": (0, 1)}
MAX_INTEGER = 2**63 - 1

# All keys share a Redis Cluster hash tag. Version comparison uses decimal
# strings rather than Lua doubles, preserving the whole SQL BIGINT range.
PUBLISH = """
local offset=tonumber(ARGV[1])
local version=ARGV[2]
local state=ARGV[3]
local ttl=tonumber(ARGV[4])
local function validkeys()
  if redis.call('exists',unpack(KEYS))~=4 then return false end
  for i=1,3 do
    if redis.call('type',KEYS[i]).ok~='string' then return false end
  end
  return redis.call('type',KEYS[4]).ok=='hash'
end
if not validkeys() then redis.call('del',unpack(KEYS)) end
local old=redis.call('hget',KEYS[4],ARGV[1])
if old then
  local previous,previousstate=string.match(old,'^(%d+):(%a+)$')
  if previous then
    if #previous>#version or (#previous==#version and previous>version) then return 0 end
    if previous==version and previousstate~=state then
      return redis.error_reply('ReactionVersionConflict')
    end
  end
end
-- Metadata includes the expected state. A partial write/OOM cannot make an
-- inconsistent bitmap pass READ, even though Redis scripts do not roll back.
redis.call('hset',KEYS[4],ARGV[1],version..':'..state)
redis.call('setbit',KEYS[1],offset,state=='like' and 1 or 0)
redis.call('setbit',KEYS[2],offset,state=='dislike' and 1 or 0)
redis.call('setbit',KEYS[3],offset,1)
for i=1,4 do redis.call('expire',KEYS[i],ttl) end
return 1
"""

READ = """
if redis.call('exists',unpack(KEYS))~=4 then return {} end
for i=1,3 do
  if redis.call('type',KEYS[i]).ok~='string' then return {} end
end
if redis.call('type',KEYS[4]).ok~='hash' then return {} end
local offset=tonumber(ARGV[1])
if redis.call('getbit',KEYS[3],offset)~=1 then return {} end
local metadata=redis.call('hget',KEYS[4],ARGV[1])
if not metadata then return {} end
local version,state=string.match(metadata,'^(%d+):(%a+)$')
if not version then return {} end
local liked=redis.call('getbit',KEYS[1],offset)
local disliked=redis.call('getbit',KEYS[2],offset)
if state=='like' and liked==1 and disliked==0 then return {version,state} end
if state=='dislike' and liked==0 and disliked==1 then return {version,state} end
if state=='neutral' and liked==0 and disliked==0 then return {version,state} end
return {}
"""


def bitmap_keys(prefix: str, content_id: str, bitmap_id: int):
    if type(bitmap_id) is not int or not 1 <= bitmap_id <= MAX_INTEGER:
        raise ValueError("A persisted positive bitmap user ID is required")
    shard, offset = divmod(bitmap_id - 1, SHARD_BITS)
    namespace = hashlib.sha256(prefix.encode()).hexdigest()[:16]
    content = hashlib.sha256(content_id.encode()).hexdigest()
    base = f"{prefix}:bitmap:v1:{{{namespace}:{content}:{shard}}}"
    return tuple(f"{base}:{kind}" for kind in ("like", "dislike", "known", "versions")), offset


def validate_reaction(state, version):
    if state not in STATES or type(version) is not int or not 0 <= version <= MAX_INTEGER:
        raise ValueError("A known reaction state and a nonnegative BIGINT version are required")
    if version == 0 and state != "neutral":
        raise ValueError("Only an absent/neutral relationship can have version zero")


def user_bitmap_id(db_path, user_id: str, *, create=False):
    """Immutable mapping, allocated independently of domain locks and transactions."""
    if not isinstance(user_id, str) or not user_id:
        raise ValueError("A platform user ID is required")
    with get_connection(db_path) as conn:
        if create:
            begin_write(conn, namespace="bitmap-user", key=user_id)
        row = conn.execute(
            "SELECT bitmap_id FROM reaction_bitmap_users WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if row:
            return int(row[0])
        if not create:
            return None
        conn.execute("INSERT INTO reaction_bitmap_users(user_id) VALUES (?)", (user_id,))
        return int(
            conn.execute(
                "SELECT bitmap_id FROM reaction_bitmap_users WHERE user_id=?",
                (user_id,),
            ).fetchone()[0]
        )
