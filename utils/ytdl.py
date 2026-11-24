"""Blocking yt-dlp / ffmpeg helpers. Call these through asyncio.to_thread."""

import functools
import logging
import os
import shutil
import sysconfig
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from utils.music import (
    ParsedQuery,
    QueryKind,
    Track,
    is_valid_video_id,
    video_url,
)

log = logging.getLogger("debt-collector.music")

MAX_PLAYLIST_ITEMS: int = int(os.getenv("MUSIC_MAX_PLAYLIST", "50"))

_UNAVAILABLE_TITLES = ("[private video]", "[deleted video]")


class ExtractionError(Exception):
    """A problem the user can be told about verbatim."""


@dataclass
class StreamInfo:
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    duration: Optional[int] = None
    is_live: bool = False
    thumbnail: Optional[str] = None
    title: Optional[str] = None


@dataclass
class ResolvedQuery:
    tracks: List[Track]
    playlist_title: Optional[str] = None


@functools.lru_cache(maxsize=1)
def ffmpeg_executable() -> Optional[str]:
    """FFMPEG_PATH, then ffmpeg on PATH, then the static binary from imageio-ffmpeg."""
    override: Optional[str] = os.getenv("FFMPEG_PATH")
    if override:
        found = shutil.which(override)
        if found:
            return found
    on_path: Optional[str] = shutil.which("ffmpeg")
    if on_path:
        return on_path
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


def friendly_error(message: str) -> str:
    """Turn yt-dlp's raw error text into something a Discord user can act on."""
    text: str = (message or "").lower()
    if "confirm your age" in text or "age-restricted" in text or "age restricted" in text:
        return "That video is age-restricted and can't be played without a logged-in account."
    if "not a bot" in text or "sign in to confirm" in text:
        configured: str = os.getenv("YTDL_COOKIES_FILE", "").strip()
        if configured and _cookie_file() is None:
            return (
                "YouTube is blocking this server's IP with a bot check, and the cookies file "
                f"in `YTDL_COOKIES_FILE` (`{configured}`) doesn't exist. Check the path "
                "(on Render it's `/etc/secrets/<file name>`, starting with `/`)."
            )
        if configured:
            return (
                "YouTube is blocking this server's IP with a bot check and didn't accept "
                "the cookies. Export fresh cookies, or route YouTube through a proxy (`YTDL_PROXY`)."
            )
        return (
            "YouTube is blocking this server's IP with a bot check. "
            "The bot owner can route YouTube through a proxy (`YTDL_PROXY`) "
            "or supply cookies (`YTDL_COOKIES_FILE`)."
        )
    if "private video" in text:
        return "That video is private."
    if "members-only" in text or "members only" in text or "join this channel" in text:
        return "That video is for channel members only."
    if "not available in your country" in text or "blocked it in your country" in text:
        return "That video isn't available in this server's region."
    if "copyright" in text:
        return "That video was blocked for copyright reasons."
    if "video unavailable" in text or "this video is not available" in text:
        return "That video is unavailable."
    if "live event will begin" in text or "premieres in" in text:
        return "That live stream or premiere hasn't started yet."
    if "requested format is not available" in text or "only images are available" in text:
        return (
            "YouTube gave me no playable audio for that. If this happens for every video, "
            "the host is missing a JavaScript runtime (deno) or yt-dlp needs updating."
        )
    if "http error 403" in text:
        return "YouTube refused the request (HTTP 403). Try again in a moment."
    if "unable to download" in text or "timed out" in text or "connection" in text:
        return "I couldn't reach YouTube just now. Try again in a moment."
    return "I couldn't load that from YouTube."


class _YtdlLogger:
    """Keep yt-dlp out of stderr; real failures surface as ExtractionError instead."""

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        log.debug("yt-dlp: %s", message)

    def error(self, message: str) -> None:
        log.debug("yt-dlp: %s", message)


@functools.lru_cache(maxsize=1)
def _js_runtimes() -> Dict[str, Dict[str, Any]]:
    """YouTube needs a JavaScript runtime; yt-dlp only auto-enables deno, so enable any we find.

    deno is always listed: yt-dlp also finds the copy pip installs (yt-dlp[deno]) next to
    Python, which isn't always on PATH.
    """
    runtimes: Dict[str, Dict[str, Any]] = {"deno": {}}
    runtimes.update({name: {} for name in ("node", "bun") if shutil.which(name)})
    found: List[str] = [name for name in ("deno", "node", "bun") if _runtime_installed(name)]
    try:
        from yt_dlp.version import __version__ as ytdlp_version
    except Exception:
        ytdlp_version = "unknown"
    log.info("yt-dlp version %s", ytdlp_version)
    if found:
        log.info("JavaScript runtimes available to yt-dlp: %s", ", ".join(found))
    else:
        log.warning(
            "No JavaScript runtime (deno, node or bun) found; YouTube playback will fail. "
            "Install requirements.txt again (it includes deno)."
        )
    return runtimes


def _runtime_installed(name: str) -> bool:
    if shutil.which(name):
        return True
    scripts: str = sysconfig.get_path("scripts") or ""
    return bool(scripts) and shutil.which(name, path=scripts) is not None


def _base_options() -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "logger": _YtdlLogger(),
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 15,
        "retries": 3,
        "cachedir": False,
        "source_address": "0.0.0.0",  # prefer IPv4; YouTube blocks many IPv6 ranges
    }
    options["js_runtimes"] = {name: dict(config) for name, config in _js_runtimes().items()}
    cookies: Optional[str] = _cookie_file()
    if cookies:
        options["cookiefile"] = _writable_cookie_file(cookies)
    proxy: str = os.getenv("YTDL_PROXY", "").strip()
    if proxy:
        # The proxy has its own address, so don't pin the outgoing socket to IPv4.
        options["proxy"] = proxy
        options.pop("source_address", None)
    clients: List[str] = _player_clients()
    if clients:
        options["extractor_args"] = {"youtube": {"player_client": clients}}
    return options


def _cookie_file() -> Optional[str]:
    """The cookies file from YTDL_COOKIES_FILE, or None (with a warning) if it isn't there."""
    configured: str = os.getenv("YTDL_COOKIES_FILE", "").strip()
    return _find_cookie_file(configured) if configured else None


@functools.lru_cache(maxsize=4)
def _find_cookie_file(configured: str) -> Optional[str]:
    if os.path.isfile(configured):
        return configured
    # A common slip: "etc/secrets/cookies.txt" instead of "/etc/secrets/cookies.txt".
    rooted: str = os.path.join(os.sep, configured)
    if not os.path.isabs(configured) and os.path.isfile(rooted):
        log.warning("YTDL_COOKIES_FILE=%s not found; using %s instead", configured, rooted)
        return rooted
    log.warning("YTDL_COOKIES_FILE=%s doesn't exist; YouTube will be used without cookies", configured)
    return None


@functools.lru_cache(maxsize=4)
def _writable_cookie_file(path: str) -> str:
    """yt-dlp rewrites the cookies file, so copy a read-only one (e.g. a Render Secret File)."""
    if os.access(path, os.W_OK):
        return path
    target: str = os.path.join(tempfile.gettempdir(), "ytdl-cookies.txt")
    try:
        shutil.copyfile(path, target)
        os.chmod(target, 0o600)
    except OSError as exc:
        log.warning("Couldn't copy read-only cookies file %s: %s", path, exc)
        return path
    log.info("Cookies file %s is read-only; using a writable copy at %s", path, target)
    return target


def _player_clients() -> List[str]:
    """YTDL_PLAYER_CLIENTS, e.g. "tv_simply,web_embedded": which YouTube clients yt-dlp asks."""
    raw: str = os.getenv("YTDL_PLAYER_CLIENTS", "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def _entry_to_track(
    entry: Dict[str, Any], requester_id: int, requester_name: str
) -> Optional[Track]:
    video_id: Optional[str] = entry.get("id")
    if not video_id or not is_valid_video_id(str(video_id)):
        return None
    title: str = str(entry.get("title") or "Unknown title")
    if title.lower() in _UNAVAILABLE_TITLES:
        return None
    duration = entry.get("duration")
    is_live: bool = bool(entry.get("is_live")) or entry.get("live_status") == "is_live"
    thumbnail: Optional[str] = entry.get("thumbnail")
    if not thumbnail and entry.get("thumbnails"):
        thumbnail = entry["thumbnails"][-1].get("url")
    return Track(
        title=title,
        url=video_url(str(video_id)),
        duration=None if is_live or duration is None else int(duration),
        requester_id=requester_id,
        requester_name=requester_name,
        thumbnail=thumbnail,
        uploader=entry.get("uploader") or entry.get("channel"),
        is_live=is_live,
    )


def resolve_tracks(
    query: ParsedQuery, requester_id: int, requester_name: str
) -> ResolvedQuery:
    """Look up metadata for a link or search. Stream URLs are fetched later."""
    import yt_dlp

    options: Dict[str, Any] = _base_options()
    target: str = query.value

    if query.kind is QueryKind.PLAYLIST:
        options.update(
            {"noplaylist": False, "extract_flat": "in_playlist", "playlistend": MAX_PLAYLIST_ITEMS}
        )
    elif query.kind is QueryKind.SEARCH:
        options.update({"extract_flat": "in_playlist"})
        target = f"ytsearch1:{query.value}"

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info: Optional[Dict[str, Any]] = ydl.extract_info(
                target, download=False, process=query.kind is not QueryKind.VIDEO
            )
    except yt_dlp.utils.DownloadError as exc:
        log.warning("yt-dlp failed for %s: %s", target, exc)
        raise ExtractionError(friendly_error(str(exc))) from exc

    if not info:
        raise ExtractionError("I couldn't find anything for that.")

    if query.kind is QueryKind.VIDEO:
        track = _entry_to_track(info, requester_id, requester_name)
        if track is None:
            raise ExtractionError("That video is unavailable.")
        return ResolvedQuery([track])

    entries: List[Dict[str, Any]] = [e for e in (info.get("entries") or []) if e]
    tracks: List[Track] = []
    for entry in entries[: MAX_PLAYLIST_ITEMS if query.kind is QueryKind.PLAYLIST else 1]:
        track = _entry_to_track(entry, requester_id, requester_name)
        if track is not None:
            tracks.append(track)

    if not tracks:
        raise ExtractionError(
            "I couldn't find anything for that."
            if query.kind is QueryKind.SEARCH
            else "That playlist is empty or entirely unavailable."
        )
    title: Optional[str] = info.get("title") if query.kind is QueryKind.PLAYLIST else None
    return ResolvedQuery(tracks, playlist_title=title)


def resolve_stream(track: Track) -> StreamInfo:
    """Fetch a fresh stream URL (they expire, so this is done right before playing)."""
    import yt_dlp

    options: Dict[str, Any] = _base_options()
    options["format"] = "bestaudio/best"
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info: Optional[Dict[str, Any]] = ydl.extract_info(track.url, download=False)
    except yt_dlp.utils.DownloadError as exc:
        log.warning("yt-dlp failed for %s: %s", track.url, exc)
        raise ExtractionError(friendly_error(str(exc))) from exc

    if not info:
        raise ExtractionError("That video is unavailable.")
    url: Optional[str] = info.get("url")
    if not url:
        formats = [f for f in (info.get("requested_formats") or []) if f.get("url")]
        url = formats[0]["url"] if formats else None
    if not url:
        raise ExtractionError("YouTube gave me no playable audio for that video.")

    duration = info.get("duration")
    return StreamInfo(
        url=url,
        headers=dict(info.get("http_headers") or {}),
        duration=int(duration) if duration is not None else None,
        is_live=bool(info.get("is_live")),
        thumbnail=info.get("thumbnail"),
        title=info.get("title"),
    )
