"""On-demand HLS byte-range views of upstream DASH; no media files or MinIO writes."""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from urllib.parse import quote, urljoin
from uuid import uuid4

import requests
from database import begin_write, get_connection
from error_code import APIError, ErrorCode
from models import Track
from resource_policy import ResourcePolicy

from .media import MediaService
from .repository import encode, identity
from .source import validate_media_url

POLICY = "bili-range-proxy-v1"
_SLOTS = threading.BoundedSemaphore(32)
_LOCK = threading.Lock()
_CACHE = OrderedDict()
_FLIGHTS = {}


@dataclass
class _Resolution:
    event: threading.Event = field(default_factory=threading.Event)
    value: object = None
    error: Exception | None = None


def byte_range(value):
    match = re.fullmatch(r"(\d+)-(\d+)", str(value))
    if not match or int(match[2]) < int(match[1]):
        raise ValueError("InvalidUpstreamIndexRange")
    return int(match[1]), int(match[2])


def sidx_segments(data, start):
    """Read bounded ISO BMFF sidx references, preserving upstream media bytes."""
    offset = 0
    while offset + 8 <= len(data):
        size, kind = struct.unpack_from(">I4s", data, offset)
        if size < 8 or offset + size > len(data):
            raise ValueError("InvalidSIDXBox")
        if kind == b"sidx":
            end, cursor = offset + size, offset + 8
            version = data[cursor]
            if version not in {0, 1}:
                raise ValueError("UnsupportedSIDXVersion")
            cursor += 8  # full-box flags and reference_ID
            scale = struct.unpack_from(">I", data, cursor)[0]
            cursor += 4
            fmt = ">II" if version == 0 else ">QQ"
            _earliest, first = struct.unpack_from(fmt, data, cursor)
            cursor += 8 if version == 0 else 16
            _reserved, count = struct.unpack_from(">HH", data, cursor)
            cursor += 4
            if not scale or not 0 < count <= 4096 or cursor + count * 12 > end:
                raise ValueError("InvalidSIDXReferences")
            position, result = start + end + first, []
            for _ in range(count):
                length, duration, _sap = struct.unpack_from(">III", data, cursor)
                cursor += 12
                if length & 0x80000000 or not length or not duration:
                    raise ValueError("UnsupportedSIDXReference")
                result.append((position, length, duration / scale))
                position += length
            return result
        offset += size
    raise ValueError("MissingSIDX")


def open_upstream(url, headers, *, method="GET"):
    session = requests.Session()
    session.trust_env = False
    try:
        for _ in range(4):
            validate_media_url(url)
            response = session.request(
                method,
                url,
                headers={**headers, "Accept-Encoding": "identity"},
                stream=True,
                allow_redirects=False,
                timeout=(3, 15),
            )
            if response.status_code not in {301, 302, 303, 307, 308}:
                return response, session
            target = urljoin(url, response.headers.get("Location", ""))
            response.close()
            url = validate_media_url(target)
        raise ValueError("TooManyMediaRedirects")
    except Exception:
        session.close()
        raise


class ProxyMediaService(MediaService):
    def _open(self, row, user_id, source, kind, version, *, range_header=None, method="GET"):
        headers = {**source.headers, **({"Range": range_header} if range_header else {})}
        response, session = open_upstream(getattr(source, kind + "_url"), headers, method=method)
        if response.status_code in {401, 403, 410}:
            response.close()
            session.close()
            source = self._bundle(row, user_id, force=True)
            if self._version(row, source) != version:
                raise APIError(ErrorCode.CONFLICT, "Upstream media representation changed", 409)
            response, session = open_upstream(
                getattr(source, kind + "_url"),
                {**source.headers, **({"Range": range_header} if range_header else {})},
                method=method,
            )
        return response, session

    def _source_content(self, user_id, content_id):
        value = ResourcePolicy(self.repo.db_path).require(user_id, content_id, action="play")
        source_id = value.get("mediaSourceContentId") or content_id
        return self.repo.content(source_id, user_id)

    def create(self, user_id, content_id, request_key):
        if not isinstance(request_key, str) or not request_key or len(request_key) > 180:
            raise APIError.validation_error("Idempotency-Key is required")
        content = self._source_content(user_id, content_id)
        track = json.loads(content["metadata_json"])
        if track.get("source") != "bili":
            return super().create(user_id, content_id, request_key)
        if not track.get("cid"):
            source = self.source_factory(
                user_id=user_id, scope=content["scope"], db_path=self.repo.db_path
            )
            try:
                track["cid"] = source.resolve_cid(Track.from_dict(track))
            finally:
                source.close()
        if type(track["cid"]) is not int or track["cid"] <= 0:
            raise APIError.validation_error("Invalid media CID")
        asset_id = identity("asset", track["bvid"], track["cid"], content["scope"], POLICY)
        now, playback_id = time.time(), uuid4().hex
        with get_connection(self.repo.db_path) as conn:
            begin_write(conn, namespace="playback-command", key=encode([user_id, request_key]))
            begin_write(conn, namespace="media-asset", key=asset_id)
            for identifier in sorted({content_id, content["content_id"]}):
                begin_write(conn, namespace="content", key=identifier)
            ResourcePolicy(self.repo.db_path).require(user_id, content_id, action="play", conn=conn)
            current = conn.execute(
                "SELECT * FROM feed_content WHERE content_id=?", (content["content_id"],)
            ).fetchone()
            current_metadata = json.loads(current["metadata_json"])
            if (
                current["scope"] != content["scope"]
                or current_metadata.get("bvid") != track["bvid"]
                or current_metadata.get("cid") not in {None, track["cid"]}
            ):
                raise APIError.conflict("Media identity changed; retry playback")
            old = conn.execute(
                "SELECT * FROM media_playbacks WHERE user_id=? AND request_key=?",
                (user_id, request_key),
            ).fetchone()
            if old:
                if old["content_id"] != content_id:
                    raise APIError.conflict("Playback key belongs to different content")
                playback_id = old["playback_id"]
            else:
                manifest = {
                    "format": POLICY,
                    "bvid": track["bvid"],
                    "cid": track["cid"],
                    "scope": content["scope"],
                    "duration": track.get("duration", 0),
                }
                conn.execute(
                    "INSERT OR IGNORE INTO media_assets(asset_id,content_id,policy,status,"
                    "version,manifest_json,updated_at) VALUES (?,?,?,'ready',1,?,?)",
                    (asset_id, content["content_id"], POLICY, encode(manifest), now),
                )
                for identifier in {content_id, content["content_id"]}:
                    conn.execute(
                        "INSERT INTO content_media_links(content_id,asset_id) VALUES (?,?) "
                        "ON CONFLICT(content_id) DO UPDATE SET asset_id=excluded.asset_id",
                        (identifier, asset_id),
                    )
                conn.execute(
                    "INSERT INTO media_playbacks(playback_id,user_id,asset_id,request_key,"
                    "expires_at,created_at,content_id) VALUES (?,?,?,?,?,?,?)",
                    (
                        playback_id,
                        user_id,
                        asset_id,
                        request_key,
                        now + self.config.playback_seconds,
                        now,
                        content_id,
                    ),
                )
        return self.descriptor(playback_id, user_id)

    def _bundle(self, row, user_id, *, force=False):
        manifest = json.loads(row["manifest_json"])
        key = (self.repo.db_path, row["asset_id"], user_id if manifest["scope"] != "public" else "")
        with _LOCK:
            cached = _CACHE.get(key)
            if cached and cached[0] > time.monotonic() and not force:
                _CACHE.move_to_end(key)
                return cached[1]
            flight = _FLIGHTS.get(key)
            if flight is None:
                if len(_FLIGHTS) >= 32:
                    raise APIError(ErrorCode.CONFLICT, "Media resolver is busy", 503)
                flight = _FLIGHTS[key] = _Resolution()
                leader = True
            else:
                leader = False
        if not leader:
            if not flight.event.wait(10):
                raise APIError(ErrorCode.CONFLICT, "Media resolution timed out", 503)
            if flight.error:
                raise flight.error
            return flight.value
        try:
            source = self.source_factory(
                user_id=user_id, scope=manifest["scope"], db_path=self.repo.db_path
            )
            try:
                value = source.resolve(Track.from_dict({**manifest, "source": "bili"}))
            finally:
                source.close()
            if not math.isfinite(value.duration) or value.duration <= 0:
                raise ValueError("InvalidUpstreamDuration")
            with _LOCK:
                _CACHE[key] = (time.monotonic() + 60, value)
                while len(_CACHE) > 256:
                    _CACHE.popitem(last=False)
            flight.value = value
            return value
        except Exception as error:
            flight.error = error
            raise
        finally:
            with _LOCK:
                _FLIGHTS.pop(key, None)
                flight.event.set()

    @staticmethod
    def _version(row, source):
        fields = asdict(source)
        for key in ("video_url", "audio_url", "headers", "cover_url"):
            fields.pop(key)
        return row["asset_id"] + ":" + hashlib.sha256(encode(fields).encode()).hexdigest()[:20]

    def descriptor(self, playback_id, user_id):
        row = self._playback(playback_id, user_id)
        manifest = json.loads(row["manifest_json"])
        if manifest.get("format") == "ugc-audio-v1":
            value = super().descriptor(playback_id, user_id)
            value.update(manifestUrl=f"/api/media/streams/{playback_id}/audio.m4a", format="audio")
            return value
        if manifest.get("format") != POLICY:
            return super().descriptor(playback_id, user_id)
        source = self._bundle(row, user_id)
        generation = self._version(row, source)
        with get_connection(self.repo.db_path) as conn:
            conn.execute(
                "UPDATE media_playbacks SET playable_at=? "
                "WHERE playback_id=? AND playable_at IS NULL",
                (time.time(), playback_id),
            )
        return {
            "playbackId": playback_id,
            "assetId": row["asset_id"],
            "mode": "proxy",
            "status": "ready",
            "manifestUrl": f"/api/media/streams/{playback_id}/index.m3u8?v={generation}",
            "expiresAt": row["expires_at"],
            "leaseExpiresAt": row["expires_at"],
            "manifestExpiresAt": None,
            "refreshAfterSeconds": 45,
            "generationKey": generation,
            "generation": row["asset_version"],
            "durationSeconds": source.duration,
            "seekable": True,
            "sourceComplete": True,
            "persistenceStatus": "not_required",
            "format": "hls",
        }

    def stream_file(
        self,
        playback_id,
        user_id,
        filename,
        *,
        expected_generation=None,
        range_header=None,
        method="GET",
    ):
        row = self._playback(playback_id, user_id)
        if json.loads(row["manifest_json"]).get("format") != POLICY:
            return super().stream_file(
                playback_id, user_id, filename, expected_generation=expected_generation
            )
        if filename not in {"index.m3u8", "video.m3u8", "audio.m3u8", "video.mp4", "audio.mp4"}:
            raise APIError.not_found("Media file not found")
        source = self._bundle(row, user_id)
        version = self._version(row, source)
        if expected_generation and version != expected_generation:
            raise APIError(ErrorCode.CONFLICT, "Upstream media representation changed", 409)
        suffix = "?v=" + quote(version, safe="")
        if filename == "index.m3u8":
            text = (
                "#EXTM3U\n#EXT-X-VERSION:7\n"
                '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Audio",DEFAULT=YES,AUTOSELECT=YES,'
                f'URI="audio.m3u8{suffix}"\n'
                f'#EXT-X-STREAM-INF:BANDWIDTH=4000000,CODECS="{source.video_codec},{source.audio_codec}",'
                f'AUDIO="audio"\nvideo.m3u8{suffix}\n'
            )
            return {"text": text, "mime": "application/vnd.apple.mpegurl"}
        kind = filename.split(".")[0]
        if filename.endswith(".m3u8"):
            first, last = byte_range(getattr(source, kind + "_index"))
            if last - first > 256 * 1024:
                raise ValueError("UpstreamIndexTooLarge")
            if not _SLOTS.acquire(blocking=False):
                raise APIError(ErrorCode.CONFLICT, "Media proxy capacity exceeded", 503)
            response = session = None
            try:
                response, session = self._open(
                    row, user_id, source, kind, version, range_header=f"bytes={first}-{last}"
                )
                if response.status_code != 206:
                    raise ValueError("UpstreamIndexRequiresRange")
                data = response.raw.read(last - first + 2)
                if len(data) != last - first + 1:
                    raise ValueError("InvalidUpstreamIndexLength")
            finally:
                if response is not None:
                    response.close()
                if session is not None:
                    session.close()
                _SLOTS.release()
            segments = sidx_segments(data, first)
            a, b = byte_range(getattr(source, kind + "_init"))
            target = f"{kind}.mp4{suffix}"
            text = (
                "#EXTM3U\n#EXT-X-VERSION:7\n"
                f"#EXT-X-TARGETDURATION:{math.ceil(max(s[2] for s in segments))}\n"
                "#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n"
                f'#EXT-X-MAP:URI="{target}",BYTERANGE="{b - a + 1}@{a}"\n'
            )
            for offset, length, duration in segments:
                text += f"#EXTINF:{duration:.6f},\n#EXT-X-BYTERANGE:{length}@{offset}\n{target}\n"
            return {"text": text + "#EXT-X-ENDLIST\n", "mime": "application/vnd.apple.mpegurl"}
        if range_header and not re.fullmatch(r"bytes=\d+-\d*", range_header):
            raise APIError(ErrorCode.VALIDATION_ERROR, "A single byte range is required", 416)
        if range_header and range_header.split("-")[1]:
            try:
                byte_range(range_header.removeprefix("bytes="))
            except ValueError as error:
                raise APIError(ErrorCode.VALIDATION_ERROR, "Invalid byte range", 416) from error
        if not _SLOTS.acquire(blocking=False):
            raise APIError(ErrorCode.CONFLICT, "Media proxy capacity exceeded", 503)
        try:
            response, session = self._open(
                row, user_id, source, kind, version, range_header=range_header, method=method
            )
            if response.status_code not in {200, 206, 416}:
                response.close()
                session.close()
                raise APIError(ErrorCode.CONFLICT, "Upstream media unavailable", 502)
        except Exception:
            _SLOTS.release()
            raise
        closed, close_lock = False, threading.Lock()

        def close():
            nonlocal closed
            with close_lock:
                if not closed:
                    closed = True
                    response.close()
                    session.close()
                    _SLOTS.release()

        return {
            "upstream": response,
            "close": close,
            "mime": "video/mp4" if kind == "video" else "audio/mp4",
        }
