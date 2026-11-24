"""Blocking yt-dlp / ffmpeg helpers. Call these through asyncio.to_thread."""

import functools
import logging
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from utils.music import (
    YOUTUBE_HOSTS,
    ParsedQuery,
    QueryKind,
    Track,
    is_valid_video_id,
    video_url,
)

log = logging.getLogger("debt-collector.music")

# The guild a lookup is running for. asyncio.to_thread copies the context into its worker
# thread, so log lines from yt-dlp/resolver code get tagged with it (and are only shown in
# that server's `/errors music`). Unset means process-wide diagnostics.
LOG_GUILD: ContextVar[Optional[int]] = ContextVar("music_log_guild", default=None)

MAX_PLAYLIST_ITEMS: int = int(os.getenv("MUSIC_MAX_PLAYLIST", "50"))

_UNAVAILABLE_TITLES = ("[private video]", "[deleted video]")

# Render's build step installs Deno under /opt/render/project/.deno/bin, but that folder is
# not on the PATH of the running service. yt-dlp looks runtimes up by bare name, so without
# this it silently runs with no JavaScript runtime, can't solve YouTube's challenges, and the
# stream URLs it hands back can be refused or throttled (the bot "plays" but nothing is heard).
_RUNTIME_DIRS: Tuple[str, ...] = (
    "/opt/render/project/.deno/bin",
    "~/.deno/bin",
    "~/.bun/bin",
)


def _extend_path() -> List[str]:
    """Put the usual Deno/Bun install folders on PATH. Returns the folders it added."""
    candidates: List[str] = []
    install: Optional[str] = os.getenv("DENO_INSTALL")
    if install:
        candidates.append(os.path.join(install, "bin"))
    candidates.extend(os.path.expanduser(p) for p in _RUNTIME_DIRS)

    entries: List[str] = [e for e in os.environ.get("PATH", "").split(os.pathsep) if e]
    added: List[str] = []
    for directory in candidates:
        if os.path.isdir(directory) and directory not in entries and directory not in added:
            added.append(directory)
    if added:
        os.environ["PATH"] = os.pathsep.join(added + entries)
    return added


PATH_ADDITIONS: List[str] = _extend_path()


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
    host: Optional[str] = None  # for logs only; the full URL carries signed tokens


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


def _redact(text: Any) -> str:
    """Log-safe yt-dlp text. A malformed cookies file makes CPython/yt-dlp echo the offending
    row (a live session cookie) in its error, and /errors music is readable by server admins,
    so anything cookie-related or tab-separated is replaced by a generic note."""
    message: str = str(text)
    lowered: str = message.lower()
    if "cookie" in lowered or "\t" in message or "set-cookie" in lowered:
        return "cookie-related problem (details hidden; check YTDL_COOKIES_FILE is a valid Netscape cookies.txt)"
    return message[:500]


class _YtdlLogger:
    """Keep yt-dlp out of stderr. Its warnings are the best clue when playback is silent
    (missing JS runtime, unsolved challenge, skipped formats), so they go to the music log
    instead of being dropped; real failures also surface as ExtractionError."""

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        log.warning("yt-dlp: %s", _redact(message))

    def error(self, message: str) -> None:
        log.warning("yt-dlp: %s", _redact(message))


def _js_runtimes() -> Dict[str, Dict[str, Any]]:
    """YouTube needs a JavaScript runtime; yt-dlp only auto-enables a bare `deno`, so every
    runtime found is passed by full path (Render's Deno isn't on the service PATH, though
    `_extend_path` fixes that). deno is always listed: yt-dlp also finds the copy that pip
    installs (yt-dlp[deno]) next to Python, which isn't always on PATH.
    """
    runtimes: Dict[str, Dict[str, Any]] = {"deno": {}}
    found: List[str] = []
    scripts: str = sysconfig.get_path("scripts") or ""
    for name in ("deno", "node", "bun"):
        path: Optional[str] = shutil.which(name) or (shutil.which(name, path=scripts) if scripts else None)
        if path:
            runtimes[name] = {"path": path}
            found.append(name)
    if found:
        log.info("JavaScript runtimes available to yt-dlp: %s", ", ".join(found))
    else:
        log.warning(
            "No JavaScript runtime (deno, node or bun) found; YouTube playback will fail. "
            "Install requirements.txt again (it includes deno)."
        )
    return runtimes


def _player_clients() -> List[str]:
    """YTDL_PLAYER_CLIENTS, e.g. "tv_simply,web_embedded": which YouTube clients yt-dlp asks."""
    raw: str = os.getenv("YTDL_PLAYER_CLIENTS", "")
    return [c.strip() for c in raw.split(",") if c.strip()]


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
def _copy_cookies(path: str, signature: Tuple[int, int, int]) -> str:
    """Copy a read-only cookies file. `signature` is the *source's* mtime/size/inode, so a
    replaced source is copied again (the cache is keyed on it, never on the copy that yt-dlp
    keeps rewriting)."""
    target: str = os.path.join(tempfile.gettempdir(), "ytdl-cookies.txt")
    try:
        shutil.copyfile(path, target)
        os.chmod(target, 0o600)
    except OSError as exc:
        log.warning("Couldn't copy read-only cookies file %s: %s", path, exc)
        return path
    log.info("Cookies file %s is read-only; using a writable copy at %s", path, target)
    return target


def _writable_cookie_file(path: str) -> str:
    """yt-dlp rewrites the cookies file, so copy a read-only one (e.g. a Render Secret File)."""
    if os.access(path, os.W_OK):
        return path
    try:
        stat = os.stat(path)
    except OSError:
        return path
    return _copy_cookies(path, (stat.st_mtime_ns, stat.st_size, stat.st_ino))


_writable_cookie_file.cache_clear = _copy_cookies.cache_clear  # type: ignore[attr-defined]


def _base_options() -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "logger": _YtdlLogger(),
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 15,
        "retries": 3,
        "cachedir": False,
        "source_address": "0.0.0.0",  # prefer IPv4; YouTube blocks many IPv6 ranges
    }
    runtimes: Dict[str, Dict[str, Any]] = _js_runtimes()
    if runtimes:
        options["js_runtimes"] = runtimes
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


# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #


def memory_snapshot() -> str:
    """This process' RSS plus the container's usage/limit (Render caps small plans at 512 MB)."""
    parts: List[str] = []
    try:
        with open("/proc/self/status", encoding="ascii", errors="ignore") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts.append(f"rss={int(line.split()[1]) // 1024}MB")
                    break
    except OSError:
        pass
    for used_path, limit_path in (
        ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"),
        ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        try:
            with open(used_path, encoding="ascii") as handle:
                used = int(handle.read().strip()) // (1024 * 1024)
            with open(limit_path, encoding="ascii") as handle:
                raw = handle.read().strip()
            limit = f"{int(raw) // (1024 * 1024)}MB" if raw.isdigit() and int(raw) < 1 << 50 else "unlimited"
            parts.append(f"container={used}MB/{limit}")
            break
        except (OSError, ValueError):
            continue
    return " ".join(parts) or "n/a"


def _run_text(command: List[str], timeout: float = 10.0) -> str:
    try:
        done = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, errors="ignore"
        )
        return (done.stdout or "") + (done.stderr or "")
    except (OSError, subprocess.SubprocessError):
        return ""


def environment_report() -> List[Tuple[int, str]]:
    """Blocking. (log level, line) facts that decide whether music can work on this host."""
    report: List[Tuple[int, str]] = []
    try:
        import yt_dlp

        report.append((logging.INFO, f"yt-dlp {yt_dlp.version.__version__}"))
    except Exception as exc:
        report.append((logging.ERROR, f"yt-dlp is not importable: {exc}"))

    if PATH_ADDITIONS:
        report.append((logging.INFO, f"Added to PATH: {', '.join(PATH_ADDITIONS)}"))

    ffmpeg: Optional[str] = ffmpeg_executable()
    if ffmpeg is None:
        report.append((logging.ERROR, "ffmpeg: NOT FOUND (set FFMPEG_PATH or install imageio-ffmpeg)"))
    else:
        first = (_run_text([ffmpeg, "-hide_banner", "-version"]).splitlines() or ["?"])[0]
        encoders = _run_text([ffmpeg, "-hide_banner", "-encoders"])
        protocols = _run_text([ffmpeg, "-hide_banner", "-protocols"])
        has_opus = "libopus" in encoders
        has_https = bool(re.search(r"^\s+https\s*$", protocols, re.MULTILINE))
        report.append((logging.INFO, f"ffmpeg: {ffmpeg} | {first[:80]}"))
        if not has_opus:
            report.append((logging.WARNING, "ffmpeg has no libopus encoder: Opus fallback playback will fail"))
        if not has_https:
            report.append((logging.ERROR, "ffmpeg has no https support: it can't open YouTube streams"))

    runtimes: Dict[str, Dict[str, Any]] = {n: c for n, c in _js_runtimes().items() if c.get("path")}
    if not runtimes:
        report.append(
            (
                logging.ERROR,
                "No JavaScript runtime (deno/node/bun) found on PATH: yt-dlp can't solve YouTube's "
                "challenges, so stream URLs may be refused or throttled",
            )
        )
    for name, config in runtimes.items():
        version = (_run_text([config["path"], "--version"]).splitlines() or ["?"])[0].strip()
        report.append((logging.INFO, f"JS runtime {name}: {config['path']} ({version[:40]})"))
        match = re.search(r"deno (\d+)\.(\d+)\.(\d+)", version) if name == "deno" else None
        if match and tuple(int(g) for g in match.groups()) < (2, 3, 0):
            report.append((logging.WARNING, f"Deno {version} is older than 2.3.0, which yt-dlp requires"))

    cookies_env: Optional[str] = os.getenv("YTDL_COOKIES_FILE")
    if not cookies_env:
        report.append((logging.INFO, "Cookies: YTDL_COOKIES_FILE not set"))
    elif not os.path.isfile(cookies_env):
        report.append((logging.WARNING, f"Cookies: {cookies_env} does not exist"))
    else:
        report.append((logging.INFO, f"Cookies: {cookies_env} ({os.path.getsize(cookies_env)} bytes)"))

    # Never log the proxy URL itself: it can carry credentials.
    report.append((logging.INFO, f"Proxy: {'configured (YTDL_PROXY)' if os.getenv('YTDL_PROXY', '').strip() else 'none'}"))
    clients_cfg: List[str] = _player_clients()
    report.append((logging.INFO, f"YouTube player clients: {', '.join(clients_cfg) or 'yt-dlp default'}"))

    report.append((logging.INFO, f"Memory: {memory_snapshot()}"))
    report.append((logging.INFO, f"Python {sys.version.split()[0]}"))
    return report


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

    started: float = time.monotonic()
    log.info("resolve_tracks: kind=%s target=%.80s", query.kind.value, target)
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info: Optional[Dict[str, Any]] = ydl.extract_info(
                target, download=False, process=query.kind is not QueryKind.VIDEO
            )
    except yt_dlp.utils.DownloadError as exc:
        log.error("resolve_tracks: yt-dlp failed for %.80s: %s", target, _redact(exc))
        raise ExtractionError(friendly_error(str(exc))) from exc
    log.info("resolve_tracks: done in %.1fs", time.monotonic() - started)

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
    started: float = time.monotonic()
    log.info(
        "resolve_stream: start %s | js_runtimes=%s cookies=%s %s",
        track.url,
        ",".join(options.get("js_runtimes", {})) or "NONE",
        "yes" if options.get("cookiefile") else "no",
        memory_snapshot(),
    )
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info: Optional[Dict[str, Any]] = ydl.extract_info(track.url, download=False)
    except yt_dlp.utils.DownloadError as exc:
        log.error("resolve_stream: yt-dlp failed for %s: %s", track.url, _redact(exc))
        raise ExtractionError(friendly_error(str(exc))) from exc

    if not info:
        log.error("resolve_stream: yt-dlp returned nothing for %s", track.url)
        raise ExtractionError("That video is unavailable.")
    url: Optional[str] = info.get("url")
    if not url:
        formats = [f for f in (info.get("requested_formats") or []) if f.get("url")]
        url = formats[0]["url"] if formats else None

    # The signed URL is never logged (it carries tokens); its host and emptiness are enough.
    host: Optional[str] = (urlparse(url).hostname or "").lower() if url else None
    log.info(
        "resolve_stream: done in %.1fs | url_empty=%s host=%s format=%s ext=%s protocol=%s "
        "acodec=%s abr=%s live=%s",
        time.monotonic() - started,
        not url,
        host,
        info.get("format_id"),
        info.get("ext"),
        info.get("protocol"),
        info.get("acodec"),
        info.get("abr"),
        info.get("is_live"),
    )
    if not url:
        log.error("resolve_stream: no direct audio URL for %s (formats missing)", track.url)
        raise ExtractionError("YouTube gave me no playable audio for that video.")
    if not url.startswith(("http://", "https://")) or host in YOUTUBE_HOSTS:
        # ffmpeg needs the googlevideo stream URL, never the watch page.
        log.error("resolve_stream: got a page URL (host=%s) instead of an audio stream", host)
        raise ExtractionError("YouTube gave me a web page instead of audio for that video.")

    duration = info.get("duration")
    return StreamInfo(
        url=url,
        headers=dict(info.get("http_headers") or {}),
        duration=int(duration) if duration is not None else None,
        is_live=bool(info.get("is_live")),
        thumbnail=info.get("thumbnail"),
        title=info.get("title"),
        host=host,
    )
