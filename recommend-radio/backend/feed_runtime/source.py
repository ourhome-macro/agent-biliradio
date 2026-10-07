from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from constant import BilibiliAPI, HttpHeader
from models import Track


@dataclass(frozen=True)
class MediaSource:
    video_url: str
    audio_url: str
    duration: float
    headers: dict[str, str]
    video_codec: str = "h264"
    audio_codec: str = "aac"
    cover_url: str = ""


class MediaUnavailable(ValueError):
    """The upstream explicitly reports that this asset no longer exists."""


def validate_media_url(value: str, *, allow_local: bool = False) -> str:
    parsed = urlsplit(value)
    hostname = (parsed.hostname or "").lower()
    allowed = any(
        hostname == suffix or hostname.endswith("." + suffix)
        for suffix in ("bilivideo.com", "bilivideo.cn", "bilibili.com", "hdslb.com")
    )
    if allow_local and hostname in {"127.0.0.1", "localhost"}:
        allowed = True
    if parsed.scheme not in {"https", "http"} or not allowed or parsed.username or parsed.password:
        raise ValueError("Unsupported upstream media origin")
    return value


class BiliMediaSource:
    def __init__(self, *, user_id: str, scope: str, db_path):
        from bili_client import BiliClient

        self.auth = None
        if scope == "public":
            self.client = BiliClient()
        elif scope == f"user:{user_id}":
            from auth_service import AuthService

            self.auth = AuthService(db_path=db_path, user_id=user_id)
            self.client = BiliClient(cookie_provider=self.auth.get_cookie_header)
        else:
            raise ValueError("Invalid acquisition scope")

    def resolve_cid(self, track: Track) -> int:
        return int(track.cid or self.client.get_video_info(track.bvid).cid)

    def resolve(self, track: Track) -> MediaSource:
        cid = self.resolve_cid(track)
        response = self.client._observed_get(
            "audio_info",
            self.client._authenticated_http_session(),
            BilibiliAPI.PLAY_URL,
            params={"bvid": track.bvid, "cid": cid, "qn": 64, "fnval": 16, "fnver": 0, "fourk": 0},
            headers=self.client._with_auth_cookie(HttpHeader.video_headers(track.bvid)),
            timeout=self.client.timeout,
        )
        response.raise_for_status()
        payload = self.client._json_payload(response, "feed playurl")
        if payload.get("code") == -404:
            raise MediaUnavailable("UpstreamMediaRemoved")
        if payload.get("code") != 0:
            raise RuntimeError("UpstreamMediaUnavailable")
        data = payload.get("data") or {}
        dash = data.get("dash") or {}
        videos = [
            v
            for v in dash.get("video") or []
            if str(v.get("codecs") or "").startswith("avc1") and int(v.get("height") or 0) <= 720
        ]
        audios = [
            v for v in dash.get("audio") or [] if str(v.get("codecs") or "").startswith("mp4a")
        ]
        if not videos or not audios:
            raise ValueError("UnsupportedMediaCodec")
        video = max(videos, key=lambda v: (int(v.get("height") or 0), int(v.get("bandwidth") or 0)))
        audio = max(audios, key=lambda v: int(v.get("bandwidth") or 0))
        return MediaSource(
            video_url=validate_media_url(video.get("baseUrl") or video.get("base_url") or ""),
            audio_url=validate_media_url(audio.get("baseUrl") or audio.get("base_url") or ""),
            duration=float(data.get("timelength") or 0) / 1000,
            # CDN URLs are signed; account cookies never enter a process command line.
            headers=HttpHeader.stream_headers(track.bvid),
            cover_url=track.cover,
        )

    def close(self):
        self.client.close()
        if self.auth:
            self.auth.session.close()
