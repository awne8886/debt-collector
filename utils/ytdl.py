"""Blocking yt-dlp / ffmpeg helpers. Call these through asyncio.to_thread."""

import functools
import logging
import os
import shutil
import sysconfig
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils import warp
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
            "YouTube gave me no playable audio for that from any of the clients I tried. "
            "If this happens for every video, the bot owner should check the logs."
        )
    if "http error 403" in text:
        return "YouTube refused the request (HTTP 403). Try again in a moment."
    if "unable to download" in text or "timed out" in text or "connection" in text:
        return "I couldn't reach YouTube just now. Try again in a moment."
    return "I couldn't load that from YouTube."


class _YtdlLogger:
    """Keep yt-dlp out of stderr; real failures surface as ExtractionError instead.

    Warnings are kept so a failed attempt can log why (skipped clients, missing PO
    tokens, failed JS challenges) without flooding the logs when things work.
    """

    def __init__(self) -> None:
        self.warnings: List[str] = []

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        log.debug("yt-dlp: %s", message)
        if len(self.warnings) < 20:
            self.warnings.append(message)

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
        "extractor_retries": 1,  # other clients are tried instead of retrying one
        "cachedir": _cache_dir(),
        "source_address": "0.0.0.0",  # prefer IPv4; YouTube blocks many IPv6 ranges
    }
    options["js_runtimes"] = {name: dict(config) for name, config in _js_runtimes().items()}
    cookies: Optional[str] = _cookie_file()
    if cookies:
        options["cookiefile"] = _writable_cookie_file(cookies)
    proxy: Optional[str] = _route_proxy()
    if proxy:
        # The proxy has its own address, so don't pin the outgoing socket to IPv4.
        options["proxy"] = proxy
        options.pop("source_address", None)
    clients: List[str] = _player_clients()
    if clients:
        options["extractor_args"] = {"youtube": {"player_client": clients}}
    return options


def _cache_dir() -> str:
    """yt-dlp caches YouTube's player JS and solved challenges here.

    Without it every play downloads and re-solves the player script, which takes tens
    of seconds on a small host (Render's free instances get a tenth of a CPU).
    """
    return os.getenv("YTDL_CACHE_DIR", "").strip() or os.path.join(
        tempfile.gettempdir(), "yt-dlp-cache"
    )


def _route_proxy() -> Optional[str]:
    """Where YouTube traffic goes: YTDL_PROXY, else WARP when it's running, else direct."""
    return os.getenv("YTDL_PROXY", "").strip() or warp.proxy_url()


def ffmpeg_input_options() -> List[str]:
    """ffmpeg options so the audio goes the same way as yt-dlp's requests.

    YouTube ties each stream URL to the IP address that asked for it, so behind a
    proxy (YTDL_PROXY or WARP), ffmpeg must fetch through the same proxy or it gets
    HTTP 403. ffmpeg only speaks HTTP proxies, not SOCKS.
    """
    proxy: Optional[str] = _route_proxy()
    if not proxy:
        return []
    if proxy.lower().startswith(("http://", "https://")):
        return ["-http_proxy", proxy]
    _warn_socks_once(proxy.split("://", 1)[0])
    return []


@functools.lru_cache(maxsize=1)
def _warn_socks_once(scheme: str) -> None:
    log.warning(
        "YTDL_PROXY uses %s, which ffmpeg can't use, so audio is fetched without the proxy and "
        "YouTube may refuse it (HTTP 403). Use an http:// proxy instead (wireproxy can "
        "serve one with an [http] section).",
        scheme,
    )


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


# --------------------------------------------------------------------------- #
# Client rotation
#
# YouTube treats its "clients" (web, TV, iOS, Vision Pro, Android VR, ...) differently,
# and which ones work changes with the host's IP, whether cookies are sent and
# YouTube's experiments of the week. Music bots built on Lavalink's youtube-source
# plugin handle this by trying clients in turn until one returns audio; this does the
# same with yt-dlp, and remembers which group worked so the next play starts there.
# --------------------------------------------------------------------------- #

# Clients that accept cookies and still return some audio without a PO token (the TV
# clients all of it; the web ones format 18 or HLS).
_COOKIE_CLIENTS: Tuple[str, ...] = ("tv_samsung", "tv", "web_embedded", "mweb", "web_music")
# Clients that need no JavaScript (fast on a slow CPU) but can't take cookies.
_JSLESS_CLIENTS: Tuple[str, ...] = ("visionos", "ios_producer", "android_vr")
# Cookie-less clients that do need the JavaScript player.
_JS_CLIENTS: Tuple[str, ...] = ("tv_samsung", "web_embedded", "mweb", "tv_simply", "web_music")

# Failures no other client can fix, so the rotation stops early.
_FINAL_ERRORS: Tuple[str, ...] = (
    "private video",
    "members-only",
    "members only",
    "join this channel",
    "copyright",
    "has been removed",
    "account associated with this video has been terminated",
    "live event will begin",
    "premieres in",
    "not available in your country",
    "blocked it in your country",
    "is not a valid url",
    "unsupported url",
)

try:
    ATTEMPT_BUDGET_SECONDS: float = max(float(os.getenv("YTDL_ATTEMPT_BUDGET", "75")), 5.0)
except ValueError:
    ATTEMPT_BUDGET_SECONDS = 75.0
_STREAM_TTL_SECONDS: float = 20 * 60
_PROBE_VIDEO: str = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # "Me at the zoo"


@dataclass(frozen=True)
class Strategy:
    clients: Tuple[str, ...]  # empty: let yt-dlp choose
    cookies: bool

    @property
    def label(self) -> str:
        names: str = ",".join(self.clients) or "yt-dlp default"
        return f"{names} ({'with' if self.cookies else 'no'} cookies)"


_state_lock = threading.Lock()
_preferred: Optional[Strategy] = None
_demoted: Optional[Strategy] = None  # its last stream died at once; tried last
_stream_cache: Dict[str, Tuple[float, "StreamInfo"]] = {}


def _rotation_enabled() -> bool:
    return os.getenv("YTDL_CLIENT_ROTATION", "1").strip().lower() not in ("0", "false", "off", "no")


def strategies() -> List[Strategy]:
    """Every client group to try, in order. The first is what the owner configured."""
    configured: Tuple[str, ...] = tuple(_player_clients())
    has_cookies: bool = _cookie_file() is not None
    order: List[Strategy] = [Strategy(configured, has_cookies)]
    if not _rotation_enabled():
        return order
    if has_cookies:
        order.append(Strategy(_COOKIE_CLIENTS, True))
    order += [
        Strategy(_JSLESS_CLIENTS, False),
        Strategy(_JS_CLIENTS, False),
    ]
    if configured and not has_cookies:
        order.insert(1, Strategy((), False))
    unique: List[Strategy] = []
    for strategy in order:
        if strategy not in unique:
            unique.append(strategy)
    return unique


def _ordered_strategies() -> List[Strategy]:
    """strategies(), with the group that last worked moved to the front."""
    order: List[Strategy] = strategies()
    with _state_lock:
        preferred: Optional[Strategy] = _preferred
        demoted: Optional[Strategy] = _demoted
    if demoted is not None and demoted in order and len(order) > 1:
        order.remove(demoted)
        order.append(demoted)
    if preferred is not None and preferred in order:
        order.remove(preferred)
        order.insert(0, preferred)
    return order


def stream_failed() -> None:
    """The last stream died as soon as it started (YouTube refused it): try other clients first.

    Stream URLs can resolve fine and still be refused when fetched, e.g. when a client
    needs a PO token for the download itself. Lavalink's YouTube source reacts to that
    by switching clients; so does this.
    """
    global _preferred, _demoted
    with _state_lock:
        if _preferred is None:
            return
        _demoted, _preferred = _preferred, None
        label: str = _demoted.label
    log.warning("A stream from %s died at once; trying other YouTube clients first", label)


def _remember(strategy: Strategy) -> None:
    global _preferred
    with _state_lock:
        changed: bool = _preferred != strategy
        _preferred = strategy
    if changed:
        log.info("YouTube is answering through %s; trying that first from now on", strategy.label)


def _options_for(strategy: Strategy) -> Dict[str, Any]:
    options: Dict[str, Any] = _base_options()
    if not strategy.cookies:
        options.pop("cookiefile", None)
    options.pop("extractor_args", None)
    if strategy.clients:
        options["extractor_args"] = {"youtube": {"player_client": list(strategy.clients)}}
    return options


def _is_final(message: str) -> bool:
    text: str = (message or "").lower()
    return any(marker in text for marker in _FINAL_ERRORS)


def _run_with_rotation(
    target: str,
    configure: Callable[[Dict[str, Any]], None],
    *,
    process: bool = True,
    check: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Run yt-dlp on `target`, moving through the client groups until one succeeds.

    `check` may raise yt_dlp's DownloadError to reject a result (e.g. no audio URL),
    which moves on to the next group like any other failure.
    """
    if warp.mode() == "on" and warp.available() and warp.proxy_url() is None:
        _switch_to_warp()
    try:
        return _rotate(target, configure, process=process, check=check)
    except _RouteChanged:
        return _rotate(target, configure, process=process, check=check)


# What YouTube says when it refuses the host's IP address outright.
_IP_BLOCK_SIGNS: Tuple[str, ...] = (
    "sign in to confirm you",
    "unable to download api page: http error 403",
)


class _RouteChanged(Exception):
    """YouTube traffic now goes another way (WARP); start the rotation again."""


def _looks_ip_blocked(message: str, warnings: List[str]) -> bool:
    text: str = " ".join([message, *warnings]).lower()
    return any(sign in text for sign in _IP_BLOCK_SIGNS)


def _switch_to_warp() -> bool:
    """Start WARP and route YouTube through it. False if it can't be used or started."""
    global _preferred, _demoted
    if not warp.available():
        return False
    if warp.proxy_url() is not None:
        return False  # already on WARP; a retry through it wouldn't change anything
    if warp.ensure_started() is None:
        return False
    with _state_lock:
        _preferred = None  # which clients work depends on the IP, so start over
        _demoted = None
        _stream_cache.clear()
    return True


def _rotate(
    target: str,
    configure: Callable[[Dict[str, Any]], None],
    *,
    process: bool,
    check: Optional[Callable[[Dict[str, Any]], None]],
) -> Dict[str, Any]:
    import yt_dlp

    started: float = time.monotonic()
    first_error: Optional[str] = None
    tried: List[str] = []
    for strategy in _ordered_strategies():
        if tried and time.monotonic() - started > ATTEMPT_BUDGET_SECONDS:
            log.warning("Gave up on %s after %.0fs (tried %s)", target, time.monotonic() - started, "; ".join(tried))
            break
        options: Dict[str, Any] = _options_for(strategy)
        configure(options)
        logger: _YtdlLogger = options["logger"]
        attempt_started: float = time.monotonic()
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info: Optional[Dict[str, Any]] = ydl.extract_info(target, download=False, process=process)
            if not info:
                raise yt_dlp.utils.DownloadError("ERROR: Video unavailable")
            if check is not None:
                check(info)
        except yt_dlp.utils.DownloadError as exc:
            message: str = str(exc)
            tried.append(strategy.label)
            log.warning(
                "yt-dlp failed for %s via %s in %.1fs: %s",
                target, strategy.label, time.monotonic() - attempt_started, message,
            )
            for warning in logger.warnings[-5:]:
                log.info("  yt-dlp said: %s", warning)
            # The first error is usually the most telling (the owner's own setup).
            if first_error is None or (_is_final(message) and not _is_final(first_error)):
                first_error = message
            if _is_final(message):
                break
            # YouTube refusing this IP: no other client will do better from here, so
            # hand over to WARP (auto mode) instead of waiting through the rest.
            if (
                warp.mode() == "auto"
                and warp.available()
                and warp.proxy_url() is None
                and _looks_ip_blocked(message, logger.warnings)
            ):
                log.warning("YouTube is refusing this server's IP; trying Cloudflare WARP")
                if _switch_to_warp():
                    raise _RouteChanged()
            continue
        if tried:
            log.info("Recovered %s via %s after: %s", target, strategy.label, "; ".join(tried))
        _remember(strategy)
        return info
    raise ExtractionError(friendly_error(first_error or "")) from None


def resolve_tracks(
    query: ParsedQuery, requester_id: int, requester_name: str
) -> ResolvedQuery:
    """Look up metadata for a link or search.

    A single video is fully resolved here, stream URL included, so playing it right
    away doesn't ask YouTube a second time.
    """
    if query.kind is QueryKind.VIDEO:
        info: Dict[str, Any] = _resolve_video(query.value)
        track = _entry_to_track(info, requester_id, requester_name)
        if track is None:
            raise ExtractionError("That video is unavailable.")
        return ResolvedQuery([track])

    target: str = query.value

    def configure(options: Dict[str, Any]) -> None:
        if query.kind is QueryKind.PLAYLIST:
            options.update(
                {"noplaylist": False, "extract_flat": "in_playlist", "playlistend": MAX_PLAYLIST_ITEMS}
            )
        else:
            options.update({"extract_flat": "in_playlist"})

    if query.kind is QueryKind.SEARCH:
        target = f"ytsearch1:{query.value}"

    info = _run_with_rotation(target, configure)

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


def _stream_url(info: Dict[str, Any]) -> Optional[str]:
    url: Optional[str] = info.get("url")
    if url:
        return url
    formats = [f for f in (info.get("requested_formats") or []) if f.get("url")]
    return formats[0]["url"] if formats else None


def _require_audio(info: Dict[str, Any]) -> None:
    if not _stream_url(info):
        import yt_dlp

        raise yt_dlp.utils.DownloadError("ERROR: Requested format is not available")


def _configure_stream(options: Dict[str, Any]) -> None:
    # Audio-only first; failing that, the smallest file that has audio (format 18 and
    # HLS streams are what PO-token-less clients often still return).
    options["format"] = "bestaudio/best[acodec!=none]/best"


def _resolve_video(url: str) -> Dict[str, Any]:
    info: Dict[str, Any] = _run_with_rotation(url, _configure_stream, check=_require_audio)
    _cache_stream(url, info)
    return info


def _cache_stream(url: str, info: Dict[str, Any]) -> None:
    stream: StreamInfo = _to_stream(info)
    now: float = time.monotonic()
    with _state_lock:
        for key in [k for k, (expires, _) in _stream_cache.items() if expires <= now]:
            del _stream_cache[key]
        if len(_stream_cache) >= 64:
            _stream_cache.pop(next(iter(_stream_cache)))
        _stream_cache[url] = (now + _STREAM_TTL_SECONDS, stream)


def _cached_stream(url: str) -> Optional["StreamInfo"]:
    with _state_lock:
        entry = _stream_cache.pop(url, None)
    if entry is None or entry[0] <= time.monotonic():
        return None
    return entry[1]


def _to_stream(info: Dict[str, Any]) -> "StreamInfo":
    url: Optional[str] = _stream_url(info)
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


def resolve_stream(track: Track) -> StreamInfo:
    """A stream URL for `track`: the one found while queueing it if still fresh, else a new one.

    Cached URLs are used once: a seek or a volume restart asks again, so a URL that
    YouTube has since revoked can't get stuck.
    """
    cached: Optional[StreamInfo] = _cached_stream(track.url)
    if cached is not None:
        return cached
    return _to_stream(_run_with_rotation(track.url, _configure_stream, check=_require_audio))


def warm_up() -> Optional[Strategy]:
    """Resolve a known video once so the working client group and player JS are ready."""
    try:
        _run_with_rotation(_PROBE_VIDEO, _configure_stream, check=_require_audio)
    except ExtractionError as exc:
        log.warning("YouTube warm-up failed with every client group: %s", exc)
        return None
    with _state_lock:
        return _preferred


def _check_playable(info: Dict[str, Any], seconds: int = 3) -> Optional[str]:
    """Decode the first seconds of a resolved stream the way playback does. None means it played."""
    import subprocess

    executable: Optional[str] = ffmpeg_executable()
    if executable is None:
        return "ffmpeg not found"
    command: List[str] = [
        executable, "-v", "error", "-nostdin",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
    ]
    agent: Optional[str] = (info.get("http_headers") or {}).get("User-Agent")
    if agent:
        command += ["-user_agent", agent]
    command += ffmpeg_input_options()
    command += ["-i", _stream_url(info) or "", "-t", str(seconds), "-vn",
                "-f", "s16le", "-ar", "48000", "-ac", "2", "-"]
    try:
        done = subprocess.run(command, capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        return "timed out"
    needed: int = 48000 * 2 * 2 * seconds // 2  # at least half of the requested audio
    if len(done.stdout) >= needed:
        return None
    error: str = done.stderr.decode("utf-8", "replace").strip().splitlines()[-1:] or ["no audio"]
    return error[0][-160:]


def probe(url: str = _PROBE_VIDEO) -> List[Tuple[str, bool, float, str]]:
    """Try every client group (not stopping at the first success) and log each result.

    Diagnostics for the bot owner: shows which groups work from this host right now.
    """
    import yt_dlp

    results: List[Tuple[str, bool, float, str]] = []
    groups: List[Strategy] = strategies()
    singles: List[Strategy] = []
    for group in groups:
        for client in group.clients:
            single = Strategy((client,), group.cookies)
            if single not in groups and single not in singles:
                singles.append(single)
    for strategy in groups + singles:
        if strategy in singles and not any(ok for _, ok, _, _ in results):
            # A group works when any of its clients does, so none of these will.
            log.info("probe: every client group failed, skipping single clients")
            break
        options: Dict[str, Any] = _options_for(strategy)
        _configure_stream(options)
        started: float = time.monotonic()
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=False)
            _require_audio(info or {})
            fetched: Optional[str] = _check_playable(info)
            detail: str = f"format {info.get('format_id')} ({info.get('acodec')})"
            ok: bool = fetched is None
            if fetched is not None:
                detail += f" resolved, but ffmpeg couldn't fetch it: {fetched}"
        except yt_dlp.utils.DownloadError as exc:
            detail = str(exc).replace("ERROR: ", "")[:160]
            ok = False
        elapsed: float = time.monotonic() - started
        results.append((strategy.label, ok, elapsed, detail))
        log.info("probe %-4s %5.1fs %s: %s", "OK" if ok else "FAIL", elapsed, strategy.label, detail)
        if not ok:
            for warning in options["logger"].warnings[-6:]:
                log.info("  yt-dlp said: %s", warning[:300])
    return results
