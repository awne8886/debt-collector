"""Pure music helpers: URL parsing, the queue, timing and formatting.

Nothing in here touches Discord or the network, so all of it is unit-testable.
"""

import math
import random
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional
from urllib.parse import parse_qs, urlparse

MAX_QUEUE_SIZE: int = 100
MAX_QUERY_LENGTH: int = 200

YOUTUBE_HOSTS: frozenset = frozenset(
    {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",
        "www.youtu.be",
        "youtube-nocookie.com",
        "www.youtube-nocookie.com",
    }
)

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_PLAYLIST_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,64}$")
_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
_BARE_YOUTUBE_RE = re.compile(
    r"^(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com)/",
    re.IGNORECASE,
)
_PATH_VIDEO_PREFIXES = ("shorts", "embed", "live", "v")


class InvalidQuery(ValueError):
    """The text is neither a usable YouTube link nor a usable search."""


class QueueFull(Exception):
    """The guild queue is at capacity."""


class QueryKind(Enum):
    VIDEO = "video"
    PLAYLIST = "playlist"
    SEARCH = "search"


@dataclass(frozen=True)
class ParsedQuery:
    kind: QueryKind
    value: str  # canonical URL for VIDEO/PLAYLIST, the search text for SEARCH


def is_valid_video_id(value: str) -> bool:
    return bool(_VIDEO_ID_RE.match(value or ""))


def video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def parse_query(text: str) -> ParsedQuery:
    """Classify user input and canonicalise YouTube / YouTube Music links.

    Only YouTube hosts are accepted as links, so the bot can never be pointed at
    arbitrary sites (or local files) through the downloader.
    """
    raw: str = (text or "").strip().strip("<>").strip()
    if not raw:
        raise InvalidQuery("Give me a YouTube link or something to search for.")

    if _BARE_YOUTUBE_RE.match(raw):
        raw = "https://" + raw

    if _SCHEME_RE.match(raw):
        return _parse_url(raw)

    if len(raw) > MAX_QUERY_LENGTH:
        raise InvalidQuery(f"That search is too long (max {MAX_QUERY_LENGTH} characters).")
    return ParsedQuery(QueryKind.SEARCH, raw)


def _parse_url(raw: str) -> ParsedQuery:
    try:
        parsed = urlparse(raw)
        host: str = (parsed.hostname or "").lower()
    except ValueError:
        raise InvalidQuery("That doesn't look like a valid link.")

    if parsed.scheme not in ("http", "https") or host not in YOUTUBE_HOSTS:
        raise InvalidQuery("I can only play YouTube and YouTube Music links.")

    query = parse_qs(parsed.query)
    segments: List[str] = [s for s in parsed.path.split("/") if s]
    video_id: Optional[str] = None

    if host.endswith("youtu.be"):
        video_id = segments[0] if segments else None
    elif segments and segments[0] == "watch":
        video_id = (query.get("v") or [None])[0]
    elif len(segments) >= 2 and segments[0] in _PATH_VIDEO_PREFIXES:
        video_id = segments[1]

    if video_id is not None and is_valid_video_id(video_id):
        return ParsedQuery(QueryKind.VIDEO, video_url(video_id))

    if segments and segments[0] == "playlist":
        playlist_id = (query.get("list") or [""])[0]
        if _PLAYLIST_ID_RE.match(playlist_id):
            return ParsedQuery(
                QueryKind.PLAYLIST,
                f"https://www.youtube.com/playlist?list={playlist_id}",
            )

    raise InvalidQuery("That YouTube link isn't a video or a playlist I can play.")


# --------------------------------------------------------------------------- #
# Formatting / parsing of times
# --------------------------------------------------------------------------- #


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "LIVE"
    total: int = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


_UNIT_RE = re.compile(r"(\d+)\s*([hms])", re.IGNORECASE)


def parse_timestamp(text: str) -> int:
    """'90', '1:30', '1:02:03', '1m30s' and '1h2m3s' -> seconds."""
    raw: str = (text or "").strip().lower()
    if not raw:
        raise ValueError("Give me a time like `1:30`, `90` or `1m30s`.")

    if raw.isdigit():
        return int(raw)

    if ":" in raw:
        parts: List[str] = raw.split(":")
        if len(parts) > 3 or not all(p.isdigit() for p in parts):
            raise ValueError("Give me a time like `1:30`, `90` or `1m30s`.")
        numbers: List[int] = [int(p) for p in parts]
        total: int = 0
        for value in numbers:
            total = total * 60 + value
        return total

    matches = _UNIT_RE.findall(raw)
    if matches and "".join(f"{n}{u}" for n, u in matches) == re.sub(r"\s+", "", raw):
        multipliers = {"h": 3600, "m": 60, "s": 1}
        return sum(int(n) * multipliers[u.lower()] for n, u in matches)

    raise ValueError("Give me a time like `1:30`, `90` or `1m30s`.")


def progress_bar(position: float, duration: Optional[float], width: int = 18) -> str:
    if duration is None or duration <= 0:
        return "🔴 LIVE"
    ratio: float = min(max(position / duration, 0.0), 1.0)
    marker: int = min(int(ratio * width), width - 1)
    return "▬" * marker + "🔘" + "▬" * (width - marker - 1)


def required_votes(listeners: int) -> int:
    """Votes needed to skip: half of the listeners, rounded up, at least one."""
    return max(1, math.ceil(max(listeners, 0) / 2))


# --------------------------------------------------------------------------- #
# Tracks, queue and clock
# --------------------------------------------------------------------------- #


@dataclass
class Track:
    title: str
    url: str
    duration: Optional[int]
    requester_id: int
    requester_name: str
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    is_live: bool = False


class LoopMode(Enum):
    OFF = "off"
    TRACK = "track"
    QUEUE = "queue"

    def next(self) -> "LoopMode":
        order = [LoopMode.OFF, LoopMode.TRACK, LoopMode.QUEUE]
        return order[(order.index(self) + 1) % len(order)]


class MusicQueue:
    """The current track plus the tracks waiting behind it.

    User-facing positions are 1-based and refer to the waiting tracks only.
    """

    def __init__(self, max_size: int = MAX_QUEUE_SIZE) -> None:
        self.max_size: int = max_size
        self.current: Optional[Track] = None
        self.loop: LoopMode = LoopMode.OFF
        self._upcoming: List[Track] = []

    def __len__(self) -> int:
        return len(self._upcoming)

    @property
    def upcoming(self) -> List[Track]:
        return list(self._upcoming)

    @property
    def free_slots(self) -> int:
        return max(0, self.max_size - len(self._upcoming))

    def add(self, track: Track, *, front: bool = False) -> int:
        """Queue one track and return its 1-based position."""
        if len(self._upcoming) >= self.max_size:
            raise QueueFull()
        if front:
            self._upcoming.insert(0, track)
            return 1
        self._upcoming.append(track)
        return len(self._upcoming)

    def add_many(self, tracks: List[Track], *, front: bool = False) -> int:
        """Queue as many tracks as fit, keeping their order. Returns how many."""
        accepted: List[Track] = tracks[: self.free_slots]
        if front:
            self._upcoming[0:0] = accepted
        else:
            self._upcoming.extend(accepted)
        return len(accepted)

    def advance(self, *, skipped: bool = False) -> Optional[Track]:
        """Move to the next track and return it (None when nothing is left).

        A natural end repeats the track in TRACK mode; a skip never does.
        In QUEUE mode the finished track goes to the back of the line.
        """
        finished: Optional[Track] = self.current
        if finished is not None and self.loop is LoopMode.TRACK and not skipped:
            return finished
        if finished is not None and self.loop is LoopMode.QUEUE:
            self._upcoming.append(finished)
        self.current = self._upcoming.pop(0) if self._upcoming else None
        return self.current

    def _check_position(self, position: int) -> int:
        if not 1 <= position <= len(self._upcoming):
            raise IndexError(
                f"There is no track at position {position} "
                f"({len(self._upcoming)} waiting)."
            )
        return position - 1

    def peek(self, position: int) -> Track:
        return self._upcoming[self._check_position(position)]

    def remove(self, position: int) -> Track:
        return self._upcoming.pop(self._check_position(position))

    def move(self, source: int, destination: int) -> Track:
        src: int = self._check_position(source)
        dst: int = self._check_position(destination)
        track: Track = self._upcoming.pop(src)
        self._upcoming.insert(dst, track)
        return track

    def jump(self, position: int) -> Track:
        """Make `position` the next track; skipped tracks wrap around in QUEUE mode."""
        index: int = self._check_position(position)
        passed: List[Track] = self._upcoming[:index]
        del self._upcoming[:index]
        if self.loop is LoopMode.QUEUE:
            self._upcoming.extend(passed)
        return self._upcoming[0]

    def shuffle(self, rng: Optional[random.Random] = None) -> None:
        (rng or random).shuffle(self._upcoming)

    def clear(self) -> int:
        removed: int = len(self._upcoming)
        self._upcoming.clear()
        return removed

    def stop(self) -> None:
        self._upcoming.clear()
        self.current = None

    def remaining_seconds(self) -> int:
        return sum(t.duration or 0 for t in self._upcoming)


class PlaybackClock:
    """Tracks how far into the current track we are, across pauses and seeks."""

    def __init__(self, time_fn: Callable[[], float] = time.monotonic) -> None:
        self._now: Callable[[], float] = time_fn
        self._base: float = 0.0
        self._started: Optional[float] = None
        self._paused_at: Optional[float] = None

    def start(self, position: float = 0.0) -> None:
        self._base = float(position)
        self._started = self._now()
        self._paused_at = None

    def pause(self) -> None:
        if self._started is not None and self._paused_at is None:
            self._paused_at = self._now()

    def resume(self) -> None:
        if self._started is not None and self._paused_at is not None:
            self._started += self._now() - self._paused_at
            self._paused_at = None

    def reset(self) -> None:
        self._base = 0.0
        self._started = None
        self._paused_at = None

    @property
    def paused(self) -> bool:
        return self._paused_at is not None

    def position(self) -> float:
        if self._started is None:
            return 0.0
        reference: float = self._paused_at if self._paused_at is not None else self._now()
        return self._base + max(0.0, reference - self._started)
