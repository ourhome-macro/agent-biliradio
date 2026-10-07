from __future__ import annotations

import copy
import hashlib
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future
from http.cookiejar import DefaultCookiePolicy
from typing import Any, Callable, Optional
from urllib.parse import urlencode, urlparse

import requests
from constant import BilibiliAPI as APIConst
from constant import HttpHeader
from error_code import APIError
from models import AudioStreamInfo, FavoriteFolder, Track, VideoDetail, VideoInfo, normalize_bvid
from monitoring import record_bilibili_request
from track_service import (
    cover_info_from_video_data,
    normalize_cover,
    normalize_favorite_folder,
    normalize_favorite_media_item,
    normalize_player_chapters,
    normalize_player_subtitles,
    normalize_related_item,
    normalize_reply_comments,
    normalize_search_item,
    normalize_space_archive_item,
    normalize_space_profile,
    normalize_subtitle_lines,
    normalize_user_profile,
    normalize_video_detail,
    normalize_video_intro,
)

AUDIO_QUALITY_STREAM_IDS = {
    "64k": 30216,
    "132k": 30232,
    "192k": 30280,
    "dolby": 30250,
    "hires": 30251,
}
QUALITY_ORDER = {
    "auto": [],
    "64k": [30216, 30232, 30280, 30250, 30251],
    "132k": [30232, 30216, 30280, 30250, 30251],
    "192k": [30280, 30232, 30216, 30250, 30251],
    "dolby": [30250, 30280, 30232, 30216, 30251],
    "hires": [30251, 30280, 30232, 30216, 30250],
    # Compatibility for existing saved settings.
    "standard": [30232, 30216, 30280, 30250, 30251],
    "high": [30280, 30232, 30216, 30250, 30251],
}
QUALITY_ALIASES = {
    "standard": "132k",
    "high": "192k",
}

WBI_MIXIN_KEY_ENC_TAB = (
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    48,
    7,
    16,
    24,
    55,
    40,
    61,
    26,
    17,
    0,
    1,
    60,
    51,
    30,
    4,
    22,
    25,
    54,
    21,
    56,
    59,
    6,
    63,
    57,
    62,
    11,
    36,
    20,
    34,
    44,
    52,
)
WBI_FORBIDDEN_VALUE_CHARS = re.compile(r"[!'()*]")
WBI_SIGNATURE_REJECT_CODES = {-403}
WBI_KEY_PATTERN = re.compile(r"^[0-9a-fA-F]{32}$")
SUBTITLE_DURATION_MIN_MARGIN_SECONDS = 10.0
SUBTITLE_DURATION_MARGIN_RATIO = 0.05


class _RejectCookiesPolicy(DefaultCookiePolicy):
    def set_ok(self, cookie, request):
        return False


class BiliClient:
    SEARCH_URL = "https://api.bilibili.com/x/web-interface/search/type"
    HOME_URL = "https://www.bilibili.com/"

    def __init__(
        self,
        timeout: int | float | tuple[float, float] = 10,
        cookie_provider: Optional[Callable[[], Optional[str]]] = None,
        detail_cache_ttl_seconds: float = 5 * 60,
        player_cache_ttl_seconds: float = 60,
        wbi_key_cache_ttl_seconds: float = 10 * 60,
        metadata_cache_max_entries: int = 256,
    ):
        if isinstance(timeout, tuple):
            self.timeout = timeout
        else:
            timeout_value = max(float(timeout), 0.1)
            self.timeout = (min(3.05, timeout_value), timeout_value)
        self.cookie_provider = cookie_provider
        self.session = requests.Session()
        self._guest_session = self.session
        self.auth_session = requests.Session()
        self.session.headers.update(HttpHeader.default_headers())
        self.auth_session.headers.update(HttpHeader.default_headers())
        self.auth_session.cookies.set_policy(_RejectCookiesPolicy())
        for current_session in (self.session, self.auth_session):
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=8,
                pool_maxsize=32,
                max_retries=0,
                pool_block=True,
            )
            current_session.mount("https://", adapter)
            current_session.mount("http://", adapter)
        self._guest_cookie_ready = False
        self._guest_cookie_lock = threading.Lock()
        self._metadata_cache_lock = threading.RLock()
        self._detail_cache_ttl_seconds = max(float(detail_cache_ttl_seconds), 0)
        self._player_cache_ttl_seconds = max(float(player_cache_ttl_seconds), 0)
        self._wbi_key_cache_ttl_seconds = max(float(wbi_key_cache_ttl_seconds), 0)
        self._metadata_cache_max_entries = max(int(metadata_cache_max_entries), 1)
        self._detail_cache: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
        self._player_cache: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
        self._wbi_key_cache: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
        self._metadata_inflight: dict[str, Future[dict[str, Any]]] = {}

    @staticmethod
    def is_valid_bvid(bvid: str) -> bool:
        return bool(APIConst.BV_PATTERN.match((bvid or "").strip()))

    @staticmethod
    def extract_bvid(url: str) -> Optional[str]:
        match = APIConst.URL_PATTERN.search((url or "").strip())
        return normalize_bvid(match.group(3)) if match else None

    @staticmethod
    def parse_input(input_str: str) -> Optional[str]:
        value = (input_str or "").strip()
        if BiliClient.is_valid_bvid(value):
            return normalize_bvid(value)
        return BiliClient.extract_bvid(value)

    def search(self, keyword: str, page: int = 1, page_size: int = 20) -> list[Track]:
        keyword = (keyword or "").strip()
        if not keyword:
            raise APIError.validation_error("keyword is required")

        params = {
            "search_type": "video",
            "keyword": keyword,
            "page": max(page, 1),
            "page_size": min(max(page_size, 1), 50),
        }
        response = self._request_search(params, keyword)
        payload = self._json_payload(response, "search")
        if payload.get("code") == -412:
            self._ensure_guest_cookies(force=True)
            response = self._request_search(params, keyword)
            payload = self._json_payload(response, "search")

        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili search failed")

        result_items = payload.get("data", {}).get("result") or []
        return [normalize_search_item(item) for item in result_items if item.get("bvid")]

    def get_video_detail(self, bvid: str) -> VideoDetail:
        return normalize_video_detail(self._get_video_detail_payload(bvid))

    def list_related_tracks(self, bvid: str, *, limit: int = 20) -> list[Track]:
        normalized = normalize_bvid(bvid)
        if not self.is_valid_bvid(normalized):
            raise APIError.invalid_bvid(bvid)
        response = self._observed_get(
            "related_videos",
            self.session,
            APIConst.RELATED_VIDEOS_URL,
            params={"bvid": normalized},
            headers=HttpHeader.video_headers(normalized),
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = self._json_payload(response, "related videos")
        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili related videos failed")
        tracks = [normalize_related_item(item) for item in payload.get("data") or []]
        return [track for track in tracks if track is not None][: max(1, min(limit, 50))]

    def cache_scope(self) -> str:
        cookie = self.cookie_provider() if self.cookie_provider else None
        if not cookie:
            return "guest"
        return hashlib.sha256(cookie.encode("utf-8")).hexdigest()[:16]

    def get_cover_info(self, bvid: str, cid: Optional[int] = None) -> dict[str, Any]:
        if not self.is_valid_bvid(bvid):
            raise APIError.invalid_bvid(bvid)
        payload = self._get_video_detail_payload(bvid)
        return cover_info_from_video_data(payload, cid=cid)

    def get_video_intro(self, bvid: str, cid: Optional[int] = None) -> dict[str, Any]:
        payload = self._get_video_detail_payload(bvid)
        return normalize_video_intro(payload, cid=cid)

    def get_track_subtitles(self, bvid: str, cid: Optional[int] = None) -> dict[str, Any]:
        resolved_bvid, resolved_cid = self._resolve_bvid_cid(bvid, cid)
        detail = self._get_video_detail_payload(resolved_bvid)
        source_aid = int(detail.get("aid") or 0)
        duration = self._duration_for_cid(detail, resolved_cid)
        player_data = self._get_player_info_payload(resolved_bvid, resolved_cid)
        manifest = normalize_player_subtitles(
            player_data,
            resolved_bvid,
            resolved_cid,
            source_aid=source_aid,
        )
        subtitles = manifest.get("subtitles") or []
        bound_subtitles = [
            subtitle
            for subtitle in subtitles
            if self._subtitle_source_is_bound(
                player_data,
                subtitle.get("url") or "",
                source_aid,
                resolved_bvid,
                resolved_cid,
            )
        ]
        if not bound_subtitles:
            if subtitles:
                return self._discard_subtitle_manifest(manifest)
            return manifest

        manifest = {**manifest, "subtitles": bound_subtitles}
        selected = bound_subtitles[0]
        subtitle_url = selected.get("url") or ""
        lines: list[dict[str, Any]] = []
        if subtitle_url:
            try:
                response = self._observed_get(
                    "subtitle",
                    self._authenticated_http_session(),
                    subtitle_url,
                    headers=HttpHeader.video_headers(resolved_bvid),
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except requests.Timeout:
                raise APIError.request_timeout("subtitle")
            except requests.HTTPError as exc:
                raise self._http_error(exc, "subtitle")
            except requests.RequestException as exc:
                raise APIError.network_error(str(exc))
            lines = normalize_subtitle_lines(self._json_payload(response, "subtitle"))

        if not self._subtitle_duration_is_valid(lines, duration):
            return self._discard_subtitle_manifest(manifest)

        return {
            **manifest,
            "activeSubtitleId": selected.get("id"),
            "lines": lines,
        }

    def get_track_chapters(self, bvid: str, cid: Optional[int] = None) -> dict[str, Any]:
        resolved_bvid, resolved_cid = self._resolve_bvid_cid(bvid, cid)
        return normalize_player_chapters(
            self._get_player_info_payload(resolved_bvid, resolved_cid),
            resolved_bvid,
            resolved_cid,
        )

    def get_track_comments(
        self,
        bvid: str,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        detail = self._get_video_detail_payload(bvid)
        aid = int(detail.get("aid") or 0)
        if aid <= 0:
            raise APIError.video_not_found(bvid)
        page = max(int(page or 1), 1)
        page_size = min(max(int(page_size or 20), 1), 50)
        try:
            response = self._observed_get(
                "comments",
                self._authenticated_http_session(),
                APIConst.REPLY_MAIN_URL,
                params={
                    "type": 1,
                    "oid": aid,
                    "mode": 3,
                    "next": page - 1,
                    "ps": page_size,
                },
                headers=self._with_auth_cookie(HttpHeader.video_headers(normalize_bvid(bvid))),
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.Timeout:
            raise APIError.request_timeout("comments")
        except requests.HTTPError as exc:
            raise self._http_error(exc, "comments")
        except requests.RequestException as exc:
            raise APIError.network_error(str(exc))

        payload = self._json_payload(response, "comments")
        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili comments failed")
        return normalize_reply_comments(payload, normalize_bvid(bvid), aid, page, page_size)

    def _get_video_detail_payload(self, bvid: str) -> dict[str, Any]:
        if not self.is_valid_bvid(bvid):
            raise APIError.invalid_bvid(bvid)

        resolved_bvid = normalize_bvid(bvid)

        def load() -> dict[str, Any]:
            try:
                response = self._observed_get(
                    "video_detail",
                    self._authenticated_http_session(),
                    APIConst.VIDEO_INFO_URL,
                    params={"bvid": resolved_bvid},
                    headers=self._with_auth_cookie(HttpHeader.video_headers(resolved_bvid)),
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except requests.Timeout:
                raise APIError.request_timeout(resolved_bvid)
            except requests.HTTPError as exc:
                raise self._http_error(exc, "video detail")
            except requests.RequestException as exc:
                raise APIError.network_error(str(exc))

            payload = self._json_payload(response, "video detail")
            if payload.get("code") != 0:
                if payload.get("code") == -400:
                    raise APIError.video_not_found(resolved_bvid)
                raise APIError.api_error(payload.get("message") or "Bilibili detail failed")
            return payload.get("data") or {}

        return self._cached_metadata_payload(
            namespace="detail",
            cache=self._detail_cache,
            key=f"{self.cache_scope()}:{resolved_bvid}",
            ttl_seconds=self._detail_cache_ttl_seconds,
            loader=load,
        )

    def _resolve_bvid_cid(self, bvid: str, cid: Optional[int] = None) -> tuple[str, int]:
        if not self.is_valid_bvid(bvid):
            raise APIError.invalid_bvid(bvid)
        resolved_bvid = normalize_bvid(bvid)
        detail = self._get_video_detail_payload(resolved_bvid)
        if cid:
            resolved_cid = int(cid)
            page_cids = {
                int(page.get("cid") or 0)
                for page in detail.get("pages") or []
                if int(page.get("cid") or 0) > 0
            }
            if page_cids and resolved_cid not in page_cids:
                raise APIError.validation_error("cid does not belong to bvid")
            if not page_cids and int(detail.get("cid") or 0) != resolved_cid:
                raise APIError.validation_error("cid does not belong to bvid")
            return resolved_bvid, resolved_cid
        resolved_cid = int(detail.get("cid") or 0)
        if resolved_cid <= 0:
            raise APIError.validation_error("cid is required")
        return resolved_bvid, resolved_cid

    def _get_player_info_payload(self, bvid: str, cid: int) -> dict[str, Any]:
        if not self.is_valid_bvid(bvid):
            raise APIError.invalid_bvid(bvid)
        if not cid:
            raise APIError.validation_error("cid is required")

        resolved_bvid = normalize_bvid(bvid)
        resolved_cid = int(cid)
        detail = self._get_video_detail_payload(resolved_bvid)
        resolved_aid = int(detail.get("aid") or 0)
        if resolved_aid <= 0:
            raise APIError.video_not_found(resolved_bvid)

        def load() -> dict[str, Any]:
            wbi_keys = self._get_wbi_keys()
            for attempt in range(2):
                params = self._sign_wbi_params(
                    {
                        "aid": resolved_aid,
                        "bvid": resolved_bvid,
                        "cid": resolved_cid,
                    },
                    wbi_keys["img_key"],
                    wbi_keys["sub_key"],
                )
                try:
                    response = self._observed_get(
                        "player_info",
                        self._authenticated_http_session(),
                        APIConst.PLAYER_INFO_URL,
                        params=params,
                        headers=self._with_auth_cookie(HttpHeader.video_headers(resolved_bvid)),
                        timeout=self.timeout,
                    )
                    if response.status_code == 403:
                        if attempt == 0:
                            stale_keys = wbi_keys
                            wbi_keys = self._get_wbi_keys(
                                force_refresh=True,
                                stale_keys=stale_keys,
                            )
                            continue
                        response.raise_for_status()
                    payload = self._json_payload(response, "player info")
                    if self._is_wbi_signature_rejection(response, payload) and attempt == 0:
                        stale_keys = wbi_keys
                        wbi_keys = self._get_wbi_keys(force_refresh=True, stale_keys=stale_keys)
                        continue
                    response.raise_for_status()
                except requests.Timeout:
                    raise APIError.request_timeout(resolved_bvid)
                except requests.HTTPError as exc:
                    raise self._http_error(exc, "player info")
                except requests.RequestException as exc:
                    raise APIError.network_error(str(exc))

                if payload.get("code") != 0:
                    raise APIError.api_error(
                        payload.get("message") or "Bilibili signed player info failed"
                    )
                return self._validate_player_identity(
                    payload.get("data") or {},
                    resolved_aid,
                    resolved_bvid,
                    resolved_cid,
                )
            raise APIError.api_error("Bilibili WBI signature was rejected after key refresh")

        return self._cached_metadata_payload(
            namespace="player",
            cache=self._player_cache,
            key=f"{self.cache_scope()}:{resolved_aid}:{resolved_bvid}:{resolved_cid}",
            ttl_seconds=self._player_cache_ttl_seconds,
            loader=load,
        )

    def get_video_info(self, bvid: str) -> VideoInfo:
        return self.get_video_detail(bvid).info

    def get_audio_stream(
        self,
        bvid: str,
        cid: int,
        quality: str = "auto",
    ) -> AudioStreamInfo:
        if not self.is_valid_bvid(bvid):
            raise APIError.invalid_bvid(bvid)
        if not cid:
            raise APIError.validation_error("cid is required")

        params = {
            "bvid": normalize_bvid(bvid),
            "cid": int(cid),
            "qn": 16,
            "fnval": 16,
            "fnver": 0,
            "fourk": 0,
        }
        try:
            response = self._observed_get(
                "audio_info",
                self._authenticated_http_session(),
                APIConst.PLAY_URL,
                params=params,
                headers=self._with_auth_cookie(HttpHeader.video_headers(normalize_bvid(bvid))),
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.Timeout:
            raise APIError.request_timeout(bvid)
        except requests.HTTPError as exc:
            raise self._http_error(exc, "playurl")
        except requests.RequestException as exc:
            raise APIError.network_error(str(exc))

        payload = self._json_payload(response, "playurl")
        if payload.get("code") != 0:
            raise APIError.api_error(payload.get("message") or "Bilibili playurl failed")

        play_data = payload.get("data") or {}
        dash_data = play_data.get("dash") or {}
        audio_streams = dash_data.get("audio") or []
        if not dash_data:
            raise APIError.no_dash_stream()
        if not audio_streams:
            raise APIError.no_audio_stream()

        requested_quality = self._normalize_audio_quality(quality)
        selected = self._select_audio_stream(audio_streams, requested_quality)
        stream_id = selected.get("id")
        bitrate = int(selected.get("bandwidth") or 0)
        actual_quality = self._quality_label(stream_id, bitrate)
        codec = self._codec_label(selected.get("codecs"))
        fallback = requested_quality != "auto" and requested_quality != actual_quality
        available_qualities = self._available_audio_qualities(audio_streams)

        return AudioStreamInfo(
            url=selected.get("baseUrl") or selected.get("base_url") or "",
            backup_urls=selected.get("backupUrl") or selected.get("backup_url") or [],
            duration=int(play_data.get("timelength") or 0) // 1000,
            bitrate=bitrate,
            sample_rate=int(selected.get("sampleRate") or 44100),
            channels=int(selected.get("channel") or 2),
            init_range=(selected.get("segmentBase") or {}).get("initialization", ""),
            index_range=(selected.get("segmentBase") or {}).get("indexRange", ""),
            quality=requested_quality,
            actual_quality=actual_quality,
            codec=codec,
            fallback=fallback,
            stream_id=int(stream_id) if stream_id is not None else None,
            available_qualities=available_qualities,
        )

    def get_authenticated_user(self) -> dict[str, Any]:
        response = self._authenticated_get(APIConst.NAV_URL, "Bilibili nav")
        payload = self._json_payload(response, "Bilibili nav")
        data = payload.get("data") or {}
        if payload.get("code") != 0 or not data.get("isLogin"):
            raise APIError.auth_required(payload.get("message") or "Bilibili login is required")
        return normalize_user_profile(data).to_dict()

    def list_favorite_folders(self, up_mid: Optional[int] = None) -> list[FavoriteFolder]:
        if not up_mid:
            user = self.get_authenticated_user()
            up_mid = int(user["mid"])

        response = self._authenticated_get(
            APIConst.FAVORITE_FOLDERS_URL,
            "favorite folders",
            params={"up_mid": int(up_mid)},
        )
        payload = self._json_payload(response, "favorite folders")
        if payload.get("code") != 0:
            if payload.get("code") == -101:
                raise APIError.auth_required("Bilibili login is required")
            raise APIError.api_error(payload.get("message") or "Bilibili favorite folders failed")

        folders = (payload.get("data") or {}).get("list") or []
        return [normalize_favorite_folder(item) for item in folders if item.get("id")]

    def list_favorite_tracks(
        self,
        media_id: int,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        media_id = int(media_id or 0)
        if media_id <= 0:
            raise APIError.validation_error("mediaId is required")
        page = max(int(page or 1), 1)
        page_size = min(max(int(page_size or 20), 1), 20)

        response = self._authenticated_get(
            APIConst.FAVORITE_RESOURCE_URL,
            "favorite resources",
            params={
                "media_id": media_id,
                "pn": page,
                "ps": page_size,
                "order": "mtime",
                "type": 0,
                "tid": 0,
                "platform": "web",
            },
        )
        payload = self._json_payload(response, "favorite resources")
        if payload.get("code") != 0:
            if payload.get("code") == -101:
                raise APIError.auth_required("Bilibili login is required")
            raise APIError.api_error(payload.get("message") or "Bilibili favorite resources failed")

        data = payload.get("data") or {}
        folder = normalize_favorite_folder(data.get("info") or {"id": media_id})
        medias = data.get("medias") or []
        tracks = []
        unavailable = 0
        for item in medias:
            track = normalize_favorite_media_item(item)
            if track:
                tracks.append(track)
            else:
                unavailable += 1

        return {
            "mediaId": media_id,
            "page": page,
            "pageSize": page_size,
            "hasMore": bool(data.get("has_more")),
            "total": folder.media_count,
            "unavailable": unavailable,
            "folder": folder.to_dict(),
            "tracks": [track.to_dict() for track in tracks],
        }

    def list_all_favorite_tracks(
        self,
        media_id: int,
        max_pages: int = 10,
        page_size: int = 20,
    ) -> dict[str, Any]:
        max_pages = min(max(int(max_pages or 1), 1), 50)
        page_size = min(max(int(page_size or 20), 1), 20)
        pages = []
        all_tracks = []
        unavailable = 0
        has_more = False
        folder = None

        for page in range(1, max_pages + 1):
            current = self.list_favorite_tracks(media_id, page=page, page_size=page_size)
            pages.append(page)
            folder = current["folder"]
            all_tracks.extend(Track.from_dict(track) for track in current["tracks"])
            unavailable += int(current.get("unavailable") or 0)
            has_more = bool(current.get("hasMore"))
            if not has_more:
                break

        return {
            "mediaId": int(media_id),
            "pagesFetched": pages,
            "pageSize": page_size,
            "maxPages": max_pages,
            "hasMore": has_more,
            "total": folder.get("mediaCount", 0) if folder else 0,
            "unavailable": unavailable,
            "folder": folder or {"mediaId": int(media_id), "title": ""},
            "tracks": all_tracks,
        }

    def get_user_profile(self, mid: int) -> dict[str, Any]:
        mid = int(mid or 0)
        if mid <= 0:
            raise APIError.validation_error("mid is required")

        def load() -> dict[str, Any]:
            payload = self._space_wbi_get(
                APIConst.SPACE_INFO_URL,
                "space profile",
                {"mid": mid},
            )
            return normalize_space_profile(payload.get("data") or {})

        return self._cached_metadata_payload(
            namespace="space_profile",
            cache=self._detail_cache,
            key=f"{self.cache_scope()}:space:{mid}",
            ttl_seconds=self._detail_cache_ttl_seconds,
            loader=load,
        )

    def list_user_tracks(
        self,
        mid: int,
        page: int = 1,
        page_size: int = 20,
        order: str = "pubdate",
    ) -> dict[str, Any]:
        mid = int(mid or 0)
        if mid <= 0:
            raise APIError.validation_error("mid is required")
        page = max(int(page or 1), 1)
        page_size = min(max(int(page_size or 20), 1), 50)
        resolved_order = "click" if order == "click" else "pubdate"
        payload = self._space_wbi_get(
            APIConst.SPACE_ARCHIVE_URL,
            "space archives",
            {
                "mid": mid,
                "pn": page,
                "ps": page_size,
                "order": resolved_order,
            },
        )
        data = payload.get("data") or {}
        archive = (data.get("list") or {}).get("vlist") or []
        profile_data = (data.get("list") or {}).get("tlist") or {}
        profile = self.get_user_profile(mid)
        tracks = []
        for item in archive:
            track = normalize_space_archive_item(item, profile)
            if track:
                tracks.append(track)
        total = int((data.get("page") or {}).get("count") or len(tracks))
        return {
            "mid": mid,
            "page": page,
            "pageSize": page_size,
            "order": resolved_order,
            "total": total,
            "hasMore": page * page_size < total and len(tracks) > 0,
            "profile": profile,
            "tracks": [track.to_dict() for track in tracks],
            "rawCategories": profile_data,
        }

    def get_video_with_audio(self, input_str: str) -> tuple[VideoInfo, AudioStreamInfo]:
        bvid = self.parse_input(input_str)
        if not bvid:
            raise APIError.validation_error("Cannot parse BVID from input")
        video_info = self.get_video_info(bvid)
        return video_info, self.get_audio_stream(bvid, video_info.cid)

    def close(self) -> None:
        self.clear_metadata_cache()
        self.session.close()
        self.auth_session.close()

    def clear_metadata_cache(self) -> None:
        with self._metadata_cache_lock:
            self._detail_cache.clear()
            self._player_cache.clear()
            self._wbi_key_cache.clear()

    def _get_wbi_keys(
        self,
        force_refresh: bool = False,
        stale_keys: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        scope = self.cache_scope()
        if force_refresh:
            with self._metadata_cache_lock:
                cached = self._wbi_key_cache.get(scope)
                if cached:
                    _, cached_keys = cached
                    stale_matches = stale_keys is None or all(
                        cached_keys.get(name) == stale_keys.get(name)
                        for name in ("img_key", "sub_key")
                    )
                    if stale_matches:
                        self._wbi_key_cache.pop(scope, None)

        def load() -> dict[str, Any]:
            try:
                response = self._observed_get(
                    "wbi_nav",
                    self._authenticated_http_session(),
                    APIConst.NAV_URL,
                    headers=self._with_auth_cookie(HttpHeader.default_headers()),
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except requests.Timeout:
                raise APIError.request_timeout("WBI keys")
            except requests.HTTPError as exc:
                raise self._http_error(exc, "WBI keys")
            except requests.RequestException as exc:
                raise APIError.network_error(str(exc))

            payload = self._json_payload(response, "WBI keys")
            wbi_img = (payload.get("data") or {}).get("wbi_img") or {}
            try:
                return {
                    "img_key": self._extract_wbi_key(wbi_img.get("img_url")),
                    "sub_key": self._extract_wbi_key(wbi_img.get("sub_url")),
                }
            except APIError:
                if payload.get("code") != 0:
                    raise APIError.api_error(
                        payload.get("message") or "Bilibili WBI key lookup failed"
                    )
                raise

        return self._cached_metadata_payload(
            namespace="wbi_keys",
            cache=self._wbi_key_cache,
            key=scope,
            ttl_seconds=self._wbi_key_cache_ttl_seconds,
            loader=load,
        )

    @staticmethod
    def _extract_wbi_key(url: Any) -> str:
        filename = urlparse(str(url or "")).path.rsplit("/", 1)[-1]
        key = filename.rsplit(".", 1)[0]
        if not WBI_KEY_PATTERN.fullmatch(key):
            raise APIError.api_error("Bilibili nav returned an invalid WBI key")
        return key.lower()

    @staticmethod
    def _sign_wbi_params(
        params: dict[str, Any],
        img_key: str,
        sub_key: str,
        timestamp: Optional[int] = None,
    ) -> dict[str, str]:
        raw_mixin_key = f"{img_key}{sub_key}"
        if len(raw_mixin_key) < len(WBI_MIXIN_KEY_ENC_TAB):
            raise APIError.api_error("Bilibili WBI keys are incomplete")
        mixin_key = "".join(raw_mixin_key[index] for index in WBI_MIXIN_KEY_ENC_TAB)[:32]
        signed_params = {
            str(key): WBI_FORBIDDEN_VALUE_CHARS.sub("", str(value))
            for key, value in params.items()
            if value is not None and key not in {"w_rid", "wts"}
        }
        signed_params["wts"] = str(int(time.time() if timestamp is None else timestamp))
        signed_params = dict(sorted(signed_params.items()))
        query = urlencode(signed_params)
        signed_params["w_rid"] = hashlib.md5(f"{query}{mixin_key}".encode("utf-8")).hexdigest()
        return signed_params

    @staticmethod
    def _is_wbi_signature_rejection(response: requests.Response, payload: dict[str, Any]) -> bool:
        return response.status_code == 403 or payload.get("code") in WBI_SIGNATURE_REJECT_CODES

    def _validate_player_identity(
        self,
        player_data: dict[str, Any],
        expected_aid: int,
        expected_bvid: str,
        expected_cid: int,
    ) -> dict[str, Any]:
        try:
            actual_aid = int(player_data.get("aid") or 0)
            actual_cid = int(player_data.get("cid") or 0)
        except (TypeError, ValueError):
            actual_aid = 0
            actual_cid = 0
        actual_bvid = normalize_bvid(str(player_data.get("bvid") or ""))
        if actual_aid != expected_aid or actual_cid != expected_cid or actual_bvid != expected_bvid:
            raise APIError.api_error("Bilibili signed player info identity mismatch")

        verified = copy.deepcopy(player_data)
        verified["_verified_source"] = {
            "scope": self.cache_scope(),
            "aid": expected_aid,
            "bvid": expected_bvid,
            "cid": expected_cid,
        }
        return verified

    def _subtitle_source_is_bound(
        self,
        player_data: dict[str, Any],
        subtitle_url: str,
        expected_aid: int,
        expected_bvid: str,
        expected_cid: int,
    ) -> bool:
        source = player_data.get("_verified_source") or {}
        if (
            source.get("scope") != self.cache_scope()
            or int(source.get("aid") or 0) != expected_aid
            or normalize_bvid(str(source.get("bvid") or "")) != expected_bvid
            or int(source.get("cid") or 0) != expected_cid
        ):
            return False
        raw_subtitles = (player_data.get("subtitle") or {}).get("subtitles") or []
        appears_in_verified_manifest = any(
            normalize_cover(item.get("subtitle_url") or item.get("subtitleUrl")) == subtitle_url
            for item in raw_subtitles
            if isinstance(item, dict)
        )
        if not appears_in_verified_manifest:
            return False
        path = urlparse(subtitle_url).path.lower()
        ai_prefix = "/bfs/ai_subtitle/prod/"
        if "/bfs/ai_subtitle/" in path:
            if not path.startswith(ai_prefix):
                return False
            source_identifier = path[len(ai_prefix) :].split("/", 1)[0]
            return source_identifier.startswith(f"{expected_aid}{expected_cid}")
        return True

    @staticmethod
    def _duration_for_cid(detail: dict[str, Any], cid: int) -> float:
        for page in detail.get("pages") or []:
            if int(page.get("cid") or 0) == cid:
                return max(float(page.get("duration") or 0), 0.0)
        if int(detail.get("cid") or 0) == cid:
            return max(float(detail.get("duration") or 0), 0.0)
        return 0.0

    @staticmethod
    def _subtitle_duration_is_valid(lines: list[dict[str, Any]], duration: float) -> bool:
        if not lines or duration <= 0:
            return True
        allowed_margin = max(
            SUBTITLE_DURATION_MIN_MARGIN_SECONDS,
            duration * SUBTITLE_DURATION_MARGIN_RATIO,
        )
        return max(float(line.get("to") or 0) for line in lines) <= duration + allowed_margin

    @staticmethod
    def _discard_subtitle_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
        return {
            **manifest,
            "subtitles": [],
            "activeSubtitleId": None,
            "lines": [],
        }

    def _authenticated_http_session(self):
        # Tests and embedding callers historically replace ``session`` with a
        # deterministic transport. Preserve that injection point while the
        # default runtime keeps authenticated cookies out of the guest jar.
        if self.session is not self._guest_session:
            return self.session
        return self.auth_session

    def _cached_metadata_payload(
        self,
        namespace: str,
        cache: OrderedDict[str, tuple[float, dict[str, Any]]],
        key: str,
        ttl_seconds: float,
        loader: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        now = time.monotonic()
        inflight_key = f"{namespace}:{key}"
        cached_payload: Optional[dict[str, Any]] = None

        with self._metadata_cache_lock:
            cached = cache.get(key)
            if cached:
                expires_at, payload = cached
                if expires_at > now:
                    cache.move_to_end(key)
                    cached_payload = payload
                else:
                    cache.pop(key, None)
            if cached_payload is not None:
                future = None
                is_loader = False
            else:
                future = self._metadata_inflight.get(inflight_key)
                is_loader = future is None
                if future is None:
                    future = Future()
                    self._metadata_inflight[inflight_key] = future

        if cached_payload is not None:
            return copy.deepcopy(cached_payload)
        if not is_loader:
            return copy.deepcopy(future.result())

        try:
            payload = loader()
            if ttl_seconds > 0:
                with self._metadata_cache_lock:
                    cache[key] = (time.monotonic() + ttl_seconds, copy.deepcopy(payload))
                    cache.move_to_end(key)
                    while len(cache) > self._metadata_cache_max_entries:
                        cache.popitem(last=False)
            future.set_result(copy.deepcopy(payload))
            return copy.deepcopy(payload)
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._metadata_cache_lock:
                if self._metadata_inflight.get(inflight_key) is future:
                    self._metadata_inflight.pop(inflight_key, None)

    def _space_wbi_get(
        self,
        url: str,
        context: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if not self._has_auth_cookie():
            self._ensure_guest_cookies()
        wbi_keys = self._get_wbi_keys()
        for attempt in range(2):
            signed_params = self._sign_wbi_params(
                params,
                wbi_keys["img_key"],
                wbi_keys["sub_key"],
            )
            try:
                response = self._observed_get(
                    "space",
                    self._space_http_session(),
                    url,
                    params=signed_params,
                    headers=self._with_auth_cookie(HttpHeader.default_headers()),
                    timeout=self.timeout,
                )
                payload = self._json_payload(response, context)
                if self._is_wbi_signature_rejection(response, payload) and attempt == 0:
                    stale_keys = wbi_keys
                    wbi_keys = self._get_wbi_keys(force_refresh=True, stale_keys=stale_keys)
                    continue
                if self._is_space_risk_rejection(response, payload) and attempt == 0:
                    self._ensure_guest_cookies(force=True)
                    continue
                response.raise_for_status()
            except requests.Timeout:
                raise APIError.request_timeout(context)
            except requests.HTTPError as exc:
                raise self._http_error(exc, context)
            except requests.RequestException as exc:
                raise APIError.network_error(str(exc))

            if payload.get("code") != 0:
                raise APIError.api_error(payload.get("message") or f"Bilibili {context} failed")
            return payload
        raise APIError.api_error("Bilibili WBI signature was rejected after key refresh")

    def _space_http_session(self):
        if self.session is not self._guest_session:
            return self.session
        if self._has_auth_cookie():
            return self.auth_session
        return self.session

    @staticmethod
    def _is_space_risk_rejection(response: requests.Response, payload: dict[str, Any]) -> bool:
        message = str(payload.get("message") or "")
        return (
            response.status_code == 412 or payload.get("code") in {-412, -352} or "风控" in message
        )

    @classmethod
    def _normalize_audio_quality(cls, quality: str) -> str:
        normalized = (quality or "auto").strip().lower()
        normalized = QUALITY_ALIASES.get(normalized, normalized)
        return normalized if normalized in QUALITY_ORDER else "auto"

    @classmethod
    def _select_audio_stream(
        cls, audio_streams: list[dict[str, Any]], quality: str
    ) -> dict[str, Any]:
        normalized = cls._normalize_audio_quality(quality)
        if normalized == "auto":
            return max(audio_streams, key=lambda item: int(item.get("bandwidth") or 0))

        by_id = {int(item.get("id") or 0): item for item in audio_streams}
        for stream_id in QUALITY_ORDER[normalized]:
            if stream_id in by_id:
                return by_id[stream_id]
        return max(audio_streams, key=lambda item: int(item.get("bandwidth") or 0))

    @staticmethod
    def _quality_label(stream_id: Any, bitrate: int) -> str:
        try:
            sid = int(stream_id)
        except (TypeError, ValueError):
            sid = 0
        for label, candidate_id in AUDIO_QUALITY_STREAM_IDS.items():
            if sid == candidate_id:
                return label
        if bitrate >= 160000:
            return "192k"
        if bitrate >= 96000:
            return "132k"
        return "64k"

    @staticmethod
    def _available_audio_qualities(audio_streams: list[dict[str, Any]]) -> list[str]:
        available = {"auto"}
        stream_ids = {int(item.get("id") or 0) for item in audio_streams}
        for label, stream_id in AUDIO_QUALITY_STREAM_IDS.items():
            if stream_id in stream_ids:
                available.add(label)
        order = ["auto", "64k", "132k", "192k", "dolby", "hires"]
        return [label for label in order if label in available]

    @staticmethod
    def _codec_label(codecs: Any) -> str:
        value = str(codecs or "").lower()
        if "mp4a" in value:
            return "aac"
        if value:
            return value
        return "aac"

    @staticmethod
    def _json_payload(response: requests.Response, context: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            content_type = response.headers.get("content-type", "unknown")
            raise APIError.api_error(
                f"Bilibili {context} returned non-JSON response: "
                f"status={response.status_code}, content_type={content_type}"
            )
        if not isinstance(payload, dict):
            raise APIError.api_error(f"Bilibili {context} returned invalid JSON payload")
        return payload

    def _request_search(self, params: dict[str, Any], keyword: str) -> requests.Response:
        self._ensure_guest_cookies()
        try:
            response = self._observed_get(
                "search",
                self.session,
                self.SEARCH_URL,
                params=params,
                headers=HttpHeader.search_headers(),
                timeout=self.timeout,
            )
            if response.status_code == 412:
                self._ensure_guest_cookies(force=True)
                response = self._observed_get(
                    "search",
                    self.session,
                    self.SEARCH_URL,
                    params=params,
                    headers=HttpHeader.search_headers(),
                    timeout=self.timeout,
                )
            response.raise_for_status()
            return response
        except requests.Timeout:
            raise APIError.request_timeout(keyword)
        except requests.HTTPError as exc:
            raise self._http_error(exc, "search")
        except requests.RequestException as exc:
            raise APIError.network_error(str(exc))

    def _authenticated_get(
        self,
        url: str,
        context: str,
        params: Optional[dict[str, Any]] = None,
    ) -> requests.Response:
        cookie = self.cookie_provider() if self.cookie_provider else None
        if not cookie:
            raise APIError.auth_required("Bilibili login is required")
        try:
            operation = "favorite" if "favorite" in context.lower() else "auth"
            response = self._observed_get(
                operation,
                self._authenticated_http_session(),
                url,
                params=params,
                headers={**HttpHeader.default_headers(), "Cookie": cookie},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response
        except requests.Timeout:
            raise APIError.request_timeout(context)
        except requests.HTTPError as exc:
            raise self._http_error(exc, context)
        except requests.RequestException as exc:
            raise APIError.network_error(str(exc))

    @staticmethod
    def _relation_payload(response, context):
        payload = BiliClient._json_payload(response, context)
        code = payload.get("code")
        if code == -101:
            raise APIError.auth_required("Bilibili login expired")
        if code != 0:
            # Never include cookies, request bodies or untrusted remote text in errors.
            raise APIError.api_error(f"Bilibili relation rejected ({code})")
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    def get_follow_relation(self, mid: int) -> bool:
        if type(mid) is not int or mid <= 0:
            raise APIError.validation_error("Invalid creator MID")
        response = self._authenticated_get(
            "https://api.bilibili.com/x/relation", "follow relationship", {"fid": mid}
        )
        data = self._relation_payload(response, "follow relationship")
        attribute = data.get("attribute")
        if type(attribute) is not int or attribute not in {0, 1, 2, 6, 128}:
            raise APIError.api_error("Unrecognized Bilibili relation response")
        return attribute in {1, 2, 6}

    def list_followings(self, account_mid: int, *, page=1, page_size=50):
        if type(account_mid) is not int or account_mid <= 0 or type(page) is not int or page < 1:
            raise APIError.validation_error("Invalid following list parameters")
        response = self._authenticated_get(
            "https://api.bilibili.com/x/relation/followings", "following list",
            {"vmid": account_mid, "pn": page, "ps": min(50, max(1, page_size)), "order": "desc"},
        )
        data = self._relation_payload(response, "following list")
        if not isinstance(data.get("list"), list) or type(data.get("total")) is not int:
            raise APIError.api_error("Invalid Bilibili following list")
        return {"items": data["list"], "total": data["total"]}

    def set_follow_relation(self, mid: int, following: bool):
        from http.cookies import SimpleCookie
        if type(mid) is not int or mid <= 0 or type(following) is not bool:
            raise APIError.validation_error("Invalid follow command")
        cookie = self.cookie_provider() if self.cookie_provider else None
        if not cookie:
            raise APIError.auth_required("Bilibili login is required")
        parsed = SimpleCookie()
        parsed.load(cookie)
        csrf = parsed.get("bili_jct")
        if not csrf or not csrf.value:
            raise APIError.auth_required("Bilibili CSRF token is missing")
        started, outcome = time.perf_counter(), "success"
        try:
            response = self._authenticated_http_session().post(
                "https://api.bilibili.com/x/relation/modify",
                data={"fid": mid, "act": 1 if following else 2, "csrf": csrf.value, "re_src": 11},
                headers={**HttpHeader.default_headers(), "Cookie": cookie},
                timeout=self.timeout, allow_redirects=False,
            )
            response.raise_for_status()
            self._relation_payload(response, "follow modification")
        except requests.Timeout:
            outcome = "timeout"
            raise APIError.request_timeout("follow modification outcome is unknown") from None
        except requests.RequestException:
            outcome = "upstream_error"
            raise APIError.network_error("Bilibili follow request failed") from None
        except APIError:
            outcome = "upstream_error"
            raise
        finally:
            record_bilibili_request("relation_modify", outcome, time.perf_counter() - started)

    def _ensure_guest_cookies(self, force: bool = False) -> None:
        with self._guest_cookie_lock:
            if self._guest_cookie_ready and not force:
                return
            try:
                response = self._observed_get(
                    "guest_cookie",
                    self.session,
                    self.HOME_URL,
                    headers=HttpHeader.default_headers(),
                    timeout=self.timeout,
                )
                response.raise_for_status()
                self._guest_cookie_ready = True
            except requests.Timeout:
                raise APIError.request_timeout("bilibili guest cookie")
            except requests.RequestException as exc:
                raise APIError.network_error(f"Failed to warm Bilibili guest cookies: {exc}")

    def _with_auth_cookie(self, headers: dict[str, str]) -> dict[str, str]:
        cookie = self.cookie_provider() if self.cookie_provider else None
        if not cookie:
            return headers
        return {**headers, "Cookie": cookie}

    def _has_auth_cookie(self) -> bool:
        return bool((self.cookie_provider() if self.cookie_provider else None) or "")

    @staticmethod
    def _observed_get(
        operation: str,
        http_session: requests.Session,
        url: str,
        **kwargs,
    ) -> requests.Response:
        started_at = time.perf_counter()
        outcome = "success"
        try:
            response = http_session.get(url, **kwargs)
            if response.status_code in {412, 429}:
                outcome = "rate_limited"
            elif response.status_code in {401, 403}:
                outcome = "auth_error"
            elif response.status_code >= 400:
                outcome = "upstream_error"
            return response
        except requests.Timeout:
            outcome = "timeout"
            raise
        except requests.RequestException:
            outcome = "upstream_error"
            raise
        finally:
            record_bilibili_request(operation, outcome, time.perf_counter() - started_at)

    @staticmethod
    def _http_error(exc: requests.HTTPError, context: str) -> APIError:
        response = exc.response
        if response is None:
            return APIError.api_error(f"Bilibili {context} HTTP error: {exc}")
        return APIError.api_error(
            f"Bilibili {context} HTTP {response.status_code}: {response.reason}"
        )


BilibiliAPI = BiliClient
