"""YouTube / YouTube Music voice playback.

Slash and prefix commands share one implementation (hybrid commands). Playback
runs per guild through a `GuildPlayer`; blocking yt-dlp work is pushed to threads.
"""

import asyncio
import contextlib
import logging
import os
import shlex
import sys
import time
from collections import deque
from typing import Any, AsyncIterator, Deque, Dict, List, Literal, Optional, Set, Tuple, Union

import discord
from discord import app_commands
from discord.ext import commands

from utils.music import (
    InvalidQuery,
    LoopMode,
    MusicQueue,
    ParsedQuery,
    PlaybackClock,
    QueueFull,
    Track,
    format_duration,
    parse_query,
    parse_timestamp,
    progress_bar,
    required_votes,
)
from utils.pagination import build_pages, send_pages
from utils.ytdl import (
    ExtractionError,
    StreamInfo,
    LOG_GUILD,
    environment_report,
    ffmpeg_executable,
    memory_snapshot,
    resolve_stream,
    resolve_tracks,
)

log = logging.getLogger("debt-collector.music")

IDLE_TIMEOUT: float = 180.0  # leave this long after the queue runs dry
ALONE_TIMEOUT: float = 120.0  # leave this long after everyone else has left
RESOLVE_TIMEOUT: float = 60.0
STALL_CHECK_SECONDS: float = 6.0  # no audio from ffmpeg this long after play() = stalled stream
DAVE_READY_TIMEOUT: float = 5.0  # wait this long for voice end-to-end encryption to be ready
INSTANT_FAILURE_SECONDS: float = 3.0  # a track that ends sooner than this "died"
MIN_TRACK_FOR_FAILURE_CHECK: int = 10  # ...but only judge tracks at least this long
MAX_QUICK_FAILURES: int = 3
CONNECT_TIMEOUT: float = 20.0
MAX_VOLUME: int = 150
MUSIC_COLOR: int = 0x1DB954

try:
    DEFAULT_VOLUME: int = min(max(int(os.getenv("MUSIC_DEFAULT_VOLUME", "50")), 0), MAX_VOLUME)
except ValueError:
    DEFAULT_VOLUME = 50
try:
    MAX_TRACK_SECONDS: int = int(os.getenv("MUSIC_MAX_DURATION", str(3 * 3600)))
except ValueError:
    MAX_TRACK_SECONDS = 3 * 3600

VoiceChannel = Union[discord.VoiceChannel, discord.StageChannel]


class MusicError(Exception):
    """A problem to show the user (never a bug)."""


def opus_available() -> bool:
    """True when libopus can be loaded (needed for live volume control)."""
    import discord.opus as opus

    if opus.is_loaded():
        return True
    with contextlib.suppress(Exception):
        opus._load_default()
    if opus.is_loaded():
        return True
    for name in ("libopus.so.0", "libopus.so", "libopus.0.dylib", "opus"):
        with contextlib.suppress(Exception):
            opus.load_opus(name)
            if opus.is_loaded():
                return True
    return False


def _clean_title(title: str, limit: int = 60) -> str:
    cleaned: str = title.replace("[", "(").replace("]", ")").replace("\n", " ")
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 1].rstrip() + "…"
    return discord.utils.escape_markdown(cleaned)


def _track_link(track: Track, limit: int = 60) -> str:
    return f"[{_clean_title(track.title, limit)}]({track.url})"


def _is_superuser(user: discord.abc.User) -> bool:
    """The bot's superuser check lives in main.py, which is `__main__` when run as a script."""
    for module_name in ("__main__", "main"):
        checker = getattr(sys.modules.get(module_name), "is_superuser", None)
        if callable(checker):
            return bool(checker(user))
    return False


def _humans(channel: Optional[VoiceChannel]) -> List[discord.Member]:
    if channel is None:
        return []
    return [m for m in channel.members if not m.bot]


class _GuildLog(logging.LoggerAdapter):
    """Tags every line with the guild id (shown in /errors music, filtered per server)."""

    def process(self, msg: Any, kwargs: Any) -> Tuple[Any, Any]:
        extra: Dict[str, Any] = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return f"[g:{extra.get('guild_id')}] {msg}", kwargs


class MusicTrace(logging.Handler):
    """Keeps the recent music log in memory for `/errors music`. Warnings are also mirrored into
    the regular /errors list (errors already get there through the bot-wide log handler)."""

    def __init__(self, cog: "MusicCog") -> None:
        super().__init__(level=logging.INFO)
        self.cog: "MusicCog" = cog
        self.lines: Deque[Tuple[float, str, Optional[int], str]] = deque(maxlen=250)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message: str = record.getMessage()
            guild_id: Optional[int] = getattr(record, "guild_id", None)
            if guild_id is None:
                guild_id = LOG_GUILD.get()  # resolver/yt-dlp lines: the guild the lookup ran for
            self.lines.append((record.created, record.levelname, guild_id, message))
            if record.levelno == logging.WARNING and not getattr(record, "no_feed", False):
                self.cog.record_threadsafe("music:warning", message, guild_id)
        except Exception:  # a logging handler must never raise
            pass

    def tail(self, limit: int, guild_id: Optional[int] = None, everything: bool = False) -> List[str]:
        """Newest-last lines. Without `everything`, other servers' lines are left out."""
        # emit() runs on other threads (ffmpeg, yt-dlp); iterating a deque that is being appended
        # to raises RuntimeError, so take a snapshot under the handler lock first.
        self.acquire()
        try:
            snapshot = list(self.lines)
        finally:
            self.release()
        picked: List[str] = []
        for created, level, line_guild, message in snapshot:
            if not everything and line_guild is not None and line_guild != guild_id:
                continue
            stamp: str = time.strftime("%H:%M:%S", time.gmtime(created))
            picked.append(f"{stamp} {level[:4]} {message}")
        return picked[-limit:]


class _FfmpegStderr:
    """Sink for ffmpeg's stderr. It has no fileno(), so discord.py pipes ffmpeg's stderr into
    write(); every line then goes to the real stderr (Render logs), the music log and /errors."""

    MAX_RECORDED: int = 8  # per track, so a noisy ffmpeg can't flood /errors

    def __init__(self, player: "GuildPlayer", label: str) -> None:
        self._player: "GuildPlayer" = player
        self._label: str = label
        self._partial: str = ""
        self._recorded: int = 0

    def write(self, data: bytes) -> int:
        if not data:
            time.sleep(0.05)  # discord.py's reader keeps calling this at EOF; don't spin the CPU
            return 0
        text: str = self._partial + data.decode("utf-8", errors="replace")
        chunks: List[str] = text.replace("\r", "\n").split("\n")
        self._partial = chunks.pop()
        for line in chunks:
            self._emit(line.strip())
        return len(data)

    def _emit(self, line: str) -> None:
        if not line:
            return
        with contextlib.suppress(Exception):
            sys.stderr.write(f"[ffmpeg {self._label}] {line}\n")
            sys.stderr.flush()
        self._player.log.warning("ffmpeg: %s", line[:300], extra={"no_feed": True})
        if self._recorded < self.MAX_RECORDED:
            self._recorded += 1
            self._player.cog.record_threadsafe("music:ffmpeg", line[:300], self._player.guild.id)

    def flush(self) -> None:
        pass


class ExtractionBusy(asyncio.TimeoutError):
    """The extraction slot never freed up, so nothing was attempted (the track isn't broken)."""


class _ProbedSource(discord.AudioSource):
    """Wraps an audio source and counts what ffmpeg really delivers: frames, time to the first
    frame and (PCM only) peak amplitude. That tells "ffmpeg is silent" from "stalled stream"
    from "audio is fine but nobody can hear it"."""

    PEAK_FRAMES: int = 250  # measure the first ~5 s of 20 ms frames

    def __init__(self, inner: discord.AudioSource, *, pcm: bool) -> None:
        self.inner: discord.AudioSource = inner
        self.pcm: bool = pcm
        self.created: float = time.monotonic()
        self.first_frame_at: Optional[float] = None
        self.frames: int = 0
        self.peak: int = 0

    def read(self) -> bytes:
        data: bytes = self.inner.read()
        if not data:
            # discord.py's player looks for the ffmpeg failure on the source it was given
            error = getattr(self.inner, "_current_error", None)
            if error is not None:
                self._current_error = error
        if data:
            self.frames += 1
            if self.first_frame_at is None:
                self.first_frame_at = time.monotonic()
            if self.pcm and self.frames <= self.PEAK_FRAMES:
                samples = memoryview(data[: len(data) // 2 * 2]).cast("h")
                if len(samples):
                    self.peak = max(self.peak, max(samples), -min(samples))
        return data

    def is_opus(self) -> bool:
        return self.inner.is_opus()

    def cleanup(self) -> None:
        self.inner.cleanup()

    def summary(self) -> str:
        first = "never" if self.first_frame_at is None else f"{self.first_frame_at - self.created:.1f}s"
        peak = f" peak={self.peak}" if self.pcm else ""
        return f"frames={self.frames} first_frame={first}{peak}"


class GuildPlayer:
    """Queue, voice state and the playback loop for one guild."""

    def __init__(self, cog: "MusicCog", guild: discord.Guild) -> None:
        self.cog: "MusicCog" = cog
        self.guild: discord.Guild = guild
        self.log: _GuildLog = _GuildLog(log, {"guild_id": getattr(guild, "id", None)})
        self._probe: Optional[_ProbedSource] = None
        self._busy: bool = False  # set when _start gave up only because the extractor was busy
        self._watchers: Set["asyncio.Task[None]"] = set()
        self.queue: MusicQueue = MusicQueue()
        self.clock: PlaybackClock = PlaybackClock()
        self.volume: int = DEFAULT_VOLUME
        self.text_channel: Optional[discord.abc.Messageable] = None
        self.skip_votes: Set[int] = set()
        self._gen: int = 0  # bumps on every (re)start/stop so stale callbacks are ignored
        self._quick_failures: int = 0  # consecutive tracks that died almost instantly
        self._lock: asyncio.Lock = asyncio.Lock()
        self._timer: Optional[asyncio.Task] = None

    # -- state ------------------------------------------------------------ #

    @property
    def voice(self) -> Optional[discord.VoiceClient]:
        vc = self.guild.voice_client
        return vc if isinstance(vc, discord.VoiceClient) and vc.is_connected() else None

    @property
    def active(self) -> bool:
        vc = self.voice
        return bool(vc and (vc.is_playing() or vc.is_paused()))

    def listeners(self) -> List[discord.Member]:
        vc = self.voice
        return _humans(vc.channel) if vc else []

    # -- announcements ---------------------------------------------------- #

    async def notify(self, text: Optional[str] = None, embed: Optional[discord.Embed] = None) -> None:
        channel = self.text_channel
        if channel is None:
            return
        try:
            await channel.send(text, embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except discord.HTTPException as exc:
            log.debug("Couldn't announce in %s: %s", getattr(channel, "id", "?"), exc)

    # -- playback --------------------------------------------------------- #

    def _build_source(self, stream: StreamInfo, position: float) -> discord.AudioSource:
        executable: Optional[str] = ffmpeg_executable()
        if executable is None:
            raise MusicError("ffmpeg isn't installed on the host, so I can't play audio.")
        before: List[str] = [
            "-reconnect 1",
            "-reconnect_streamed 1",
            "-reconnect_delay_max 5",
        ]
        agent: Optional[str] = stream.headers.get("User-Agent")
        if agent:
            before.append(f"-user_agent {shlex.quote(agent)}")
        if position >= 1:
            before.append(f"-ss {int(position)}")
        before_options: str = " ".join(before)
        mode: str = "pcm" if self.cog.pcm else "opus"
        stderr = _FfmpegStderr(self, mode)  # no fileno(): discord.py pipes it through write()

        if self.cog.pcm:
            raw: discord.AudioSource = discord.FFmpegPCMAudio(
                stream.url,
                executable=executable,
                stderr=stderr,
                before_options=before_options,
                options="-vn",
            )
            probe = _ProbedSource(raw, pcm=True)
            source: discord.AudioSource = discord.PCMVolumeTransformer(probe, volume=self.volume / 100)
            volume_note: str = f"PCMVolumeTransformer={self.volume / 100:.2f}"
        else:
            # No libopus on this host: hand ffmpeg's Opus output straight to Discord and
            # apply volume as an ffmpeg filter (takes effect on the next (re)start).
            raw = discord.FFmpegOpusAudio(
                stream.url,
                executable=executable,
                stderr=stderr,
                before_options=before_options,
                options=f"-vn -af volume={self.volume / 100:.2f}",
            )
            probe = _ProbedSource(raw, pcm=False)
            source = probe
            volume_note = f"ffmpeg -af volume={self.volume / 100:.2f}"
        self._probe = probe
        self.log.info(
            "ffmpeg spawned: exe=%s mode=%s volume=%s(%d%%) host=%s position=%ss before_options=%r",
            executable,
            mode,
            volume_note,
            self.volume,
            stream.host,
            int(position),
            before_options if not agent else before_options.replace(shlex.quote(agent), "<ua>"),
        )
        return source

    async def _start(self, track: Track, position: float = 0.0, *, announce: bool = True) -> bool:
        """Begin playing `track`. Caller holds the lock. False means it couldn't play."""
        vc = self.voice
        if vc is None:
            self.log.warning("play: voice client isn't connected (still reconnecting?); can't start %s", track.url)
            return False
        self.log.info("play: preparing %s at %ss | %s", track.url, int(position), memory_snapshot())
        try:
            # Resolved right now (never from the queue): YouTube stream URLs expire. One
            # extraction at a time, because yt-dlp's JS runtime is the memory spike on small hosts.
            stream: StreamInfo = await self.cog.extract(self.guild.id, resolve_stream, track)
            await self._check_voice_ready(vc)
            source = self._build_source(stream, position)
        except MusicError as exc:
            self.log.error("play: %s", exc)
            await self.notify(f"❌ {exc}")
            return False
        except ExtractionError as exc:
            self.log.warning("play: skipping %s: %s", track.url, exc)
            await self.notify(f"⚠️ Skipping **{_clean_title(track.title)}** — {exc}")
            return False
        except ExtractionBusy:
            # never ran: another server's extraction held the slot. Keep the track queued.
            self.log.warning("play: extractor busy, keeping %s queued", track.url)
            self._busy = True
            await self.notify("⏳ Another song is still loading — try again in a moment.")
            return False
        except asyncio.TimeoutError:
            self.log.error("play: resolving %s timed out after %.0fs", track.url, RESOLVE_TIMEOUT)
            await self.notify(f"⚠️ Skipping **{_clean_title(track.title)}** — YouTube took too long to respond.")
            return False
        except Exception as exc:
            log.exception("Unexpected failure preparing %s", track.url)
            self.cog.record_error("music:prepare", exc, self.guild)
            await self.notify(f"⚠️ Skipping **{_clean_title(track.title)}** — something went wrong loading it.")
            return False

        if stream.duration and not track.duration and not stream.is_live:
            track.duration = stream.duration
        if stream.thumbnail and not track.thumbnail:
            track.thumbnail = stream.thumbnail

        vc = self.voice  # may have dropped while we were resolving
        if vc is None:
            self.log.warning("play: voice connection dropped while preparing %s", track.url)
            source.cleanup()
            return False

        self._gen += 1
        generation: int = self._gen
        if vc.is_playing() or vc.is_paused():
            self.log.info("play: stopping the previous source first")
            vc.stop()
        vc.play(source, after=lambda error, g=generation, p=self._probe: self._after(g, error, p))
        self.log.info("play: vc.play() called, is_playing=%s | %s", vc.is_playing(), memory_snapshot())
        self.clock.start(position)
        self.refresh_timer()
        self._watch_audio(generation)
        if announce:
            await self.notify(embed=self.cog.now_playing_embed(self, title="▶️ Now playing"))
        return True

    # -- voice health ----------------------------------------------------- #

    def _voice_problems(self, vc: discord.VoiceClient) -> List[str]:
        """Reasons the bot could be connected yet inaudible. Only explicit True/False count."""
        problems: List[str] = []
        me = self.guild.me
        if me is None:
            return problems
        with contextlib.suppress(Exception):
            perms = vc.channel.permissions_for(me)
            if perms.connect is False:
                problems.append("missing the Connect permission")
            if perms.speak is False:
                problems.append("missing the Speak permission")
            state = me.voice
            if state is not None:
                if state.mute is True:
                    problems.append("server-muted")
                if state.suppress is True:
                    problems.append("suppressed (Stage channel: an admin must invite it to speak)")
        return problems

    async def _check_voice_ready(self, vc: discord.VoiceClient) -> None:
        """Log (and tell the channel about) anything that would make playback inaudible, and give
        voice end-to-end encryption (DAVE) a moment to finish its handshake: audio sent before
        it's ready goes out unencrypted and other clients drop it."""
        problems: List[str] = self._voice_problems(vc)
        if problems:
            self.log.warning("voice: I may be inaudible: %s", ", ".join(problems))
            await self.notify(f"🔇 I might not be heard: I'm {', '.join(problems)} in this channel.")

        conn = getattr(vc, "_connection", None)
        version = getattr(conn, "dave_protocol_version", None)
        if not isinstance(version, int):
            return
        if version == 0:
            self.log.info("voice: DAVE end-to-end encryption not active (protocol 0)")
            return
        loop = asyncio.get_running_loop()
        deadline: float = loop.time() + DAVE_READY_TIMEOUT
        while getattr(conn, "can_encrypt", True) is False and loop.time() < deadline:
            await asyncio.sleep(0.25)
        if getattr(conn, "can_encrypt", True) is False:
            self.log.warning(
                "voice: DAVE session still not ready after %.0fs; audio may be dropped by listeners",
                DAVE_READY_TIMEOUT,
            )
        else:
            self.log.info("voice: DAVE ready (protocol %s)", version)

    def _watch_audio(self, generation: int) -> None:
        task: "asyncio.Task[None]" = asyncio.get_running_loop().create_task(self._audio_watchdog(generation))
        self._watchers.add(task)
        task.add_done_callback(self._watchers.discard)

    async def _audio_watchdog(self, generation: int) -> None:
        """A few seconds after play(), report whether ffmpeg is really delivering audio."""
        probe: Optional[_ProbedSource] = self._probe
        if probe is None:
            return
        await asyncio.sleep(STALL_CHECK_SECONDS)
        if generation != self._gen or probe is not self._probe:
            return
        vc = self.voice
        if vc is None or vc.is_paused():
            return
        if probe.frames == 0:
            self.log.error(
                "audio: ffmpeg delivered NO data %.0fs after play() (blocked, throttled or stalled "
                "stream); %s",
                STALL_CHECK_SECONDS,
                probe.summary(),
            )
            await self.notify(
                "⚠️ YouTube isn't sending any audio right now, so you'll hear nothing. "
                "An admin can check `/errors` (and `/errors music`) for why."
            )
        elif probe.pcm and probe.frames >= 50 and probe.peak == 0:
            self.log.error("audio: ffmpeg is delivering pure silence; %s", probe.summary())
        else:
            self.log.info("audio: flowing; %s | %s", probe.summary(), memory_snapshot())

    def _after(self, generation: int, error: Optional[Exception], probe: Optional[_ProbedSource] = None) -> None:
        """Runs on discord.py's player thread when a source finishes or is stopped.
        `probe` is the finishing source's own stats (self._probe may already be the next one)."""
        stats: str = probe.summary() if probe is not None else "no stats"
        if error is not None:
            # Printed straight to stderr as well, so it shows up in the Render logs whatever
            # the logging configuration is.
            print(f"[music] playback error in guild {self.guild.id}: {error!r} ({stats})", file=sys.stderr, flush=True)
            self.log.error("after(): playback error: %r (%s)", error, stats)
        else:
            self.log.info("after(): source ended without error (gen=%s, current=%s, %s)", generation, self._gen, stats)
        future = asyncio.run_coroutine_threadsafe(self._on_end(generation, error), self.cog.bot.loop)
        future.add_done_callback(self._log_future)

    @staticmethod
    def _log_future(future: Any) -> None:
        if not future.cancelled() and future.exception() is not None:
            log.error("Music end-of-track handler failed: %r", future.exception())

    async def _on_end(self, generation: int, error: Optional[Exception]) -> None:
        if generation != self._gen:
            return  # we stopped or replaced this source ourselves
        if error is not None:
            log.warning("Playback error in guild %s: %s", self.guild.id, error)
            self.cog.record_error("music:playback", error, self.guild)
        async with self._lock:
            if generation != self._gen:
                return
            if await self._died_instantly():
                return
            await self._advance(skipped=False, announce=True)

    async def _died_instantly(self) -> bool:
        """Detect a broken ffmpeg/stream: a long track that ends within seconds.

        Without this, a host problem would silently chew through the whole queue.
        Returns True when the queue was halted. Caller holds the lock.
        """
        track: Optional[Track] = self.queue.current
        if (
            track is None
            or track.is_live
            or not track.duration
            or track.duration < MIN_TRACK_FOR_FAILURE_CHECK
            or self.clock.position() >= INSTANT_FAILURE_SECONDS
        ):
            self._quick_failures = 0
            return False
        self._quick_failures += 1
        await self.notify(
            f"⚠️ **{_clean_title(track.title)}** stopped right after it started "
            "(a stream or ffmpeg problem on my end)."
        )
        if self._quick_failures < MAX_QUICK_FAILURES:
            return False
        self._quick_failures = 0
        self.queue.stop()
        self._halt()
        await self.notify("⏹️ Three tracks in a row failed to play, so I stopped the queue.")
        return True

    async def _advance(self, *, skipped: bool, announce: bool) -> bool:
        """Move to the next playable track. Caller holds the lock."""
        while True:
            track: Optional[Track] = self.queue.advance(skipped=skipped)
            skipped = True  # after a failure never loop the broken track
            if track is None:
                self._halt()
                if announce:
                    await self.notify("⏹️ Queue finished.")
                return False
            self.skip_votes.clear()
            if await self._start(track, announce=announce):
                return True
            if self._busy:  # don't burn through the queue while the extractor is occupied
                self._busy = False
                self._halt()
                return False

    def _halt(self) -> None:
        self._gen += 1
        vc = self.voice
        if vc is not None and (vc.is_playing() or vc.is_paused()):
            vc.stop()
        self.clock.reset()
        self.refresh_timer()

    async def ensure_playing(self, *, announce: bool = False) -> bool:
        """Start the queue if nothing is playing. True when this call started it."""
        async with self._lock:
            if self.active:
                return False
            return await self._advance(skipped=False, announce=announce)

    async def skip(self) -> None:
        async with self._lock:
            await self._advance(skipped=True, announce=True)

    async def skip_to(self, position: int) -> Track:
        async with self._lock:
            track: Track = self.queue.jump(position)
            await self._advance(skipped=True, announce=True)
            return track

    async def stop(self) -> None:
        async with self._lock:
            self.queue.stop()
            self._halt()

    async def seek(self, seconds: float) -> bool:
        async with self._lock:
            current: Optional[Track] = self.queue.current
            if current is None:
                return False
            was_paused: bool = self.clock.paused
            started: bool = await self._start(current, seconds, announce=False)
            if started and was_paused and self.voice is not None:
                self.voice.pause()
                self.clock.pause()
            return started

    def pause(self) -> None:
        vc = self.voice
        if vc is not None:
            vc.pause()
            self.clock.pause()

    def resume(self) -> None:
        vc = self.voice
        if vc is not None:
            vc.resume()
            self.clock.resume()

    async def set_volume(self, percent: int) -> None:
        self.volume = percent
        vc = self.voice
        if vc is None or vc.source is None:
            return
        if isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = percent / 100
        elif self.queue.current is not None:
            await self.seek(self.clock.position())  # re-apply the ffmpeg filter

    # -- idle / alone timers ---------------------------------------------- #

    def refresh_timer(self) -> None:
        """Arm or cancel the auto-leave timer based on what's happening right now."""
        self._cancel_timer()
        vc = self.voice
        if vc is None:
            return
        if not _humans(vc.channel):
            self._arm(ALONE_TIMEOUT, "alone")
        elif not self.active:
            self._arm(IDLE_TIMEOUT, "idle")

    def _arm(self, delay: float, reason: str) -> None:
        self._timer = asyncio.get_running_loop().create_task(self._leave_after(delay, reason))

    def _cancel_timer(self) -> None:
        timer, self._timer = self._timer, None
        if timer is None:
            return
        try:
            running = asyncio.current_task()
        except RuntimeError:  # no running loop: nothing can be waiting on it
            running = None
        if timer is not running:
            timer.cancel()

    async def _leave_after(self, delay: float, reason: str) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        vc = self.voice
        if vc is None:
            return
        still_alone: bool = not _humans(vc.channel)
        still_idle: bool = not self.active
        if (reason == "alone" and still_alone) or (reason == "idle" and still_idle):
            message = (
                "👋 Everyone left, so I did too."
                if reason == "alone"
                else "👋 Nothing left to play — leaving the voice channel."
            )
            await self.notify(message)
            await self.cog.destroy_player(self.guild, disconnect=True)

    def shutdown(self) -> None:
        """Stop everything synchronously (used when the voice connection is gone)."""
        self._cancel_timer()
        self._gen += 1
        self.queue.stop()
        self.clock.reset()
        vc = self.guild.voice_client
        if vc is not None and (vc.is_playing() or vc.is_paused()):  # type: ignore[union-attr]
            vc.stop()  # type: ignore[union-attr]


class MusicCog(commands.Cog, name="Music"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.players: Dict[int, GuildPlayer] = {}
        # One yt-dlp extraction at a time: each spins up a JS runtime (Deno), which together with
        # ffmpeg is what could push a 512 MB Render instance over its memory limit.
        self.extract_lock: asyncio.Lock = asyncio.Lock()
        self.trace: MusicTrace = MusicTrace(self)
        log.addHandler(self.trace)  # feeds `/errors music`
        if log.level == logging.NOTSET or log.level > logging.INFO:
            log.setLevel(logging.INFO)  # the play path logs at INFO; make sure it isn't filtered out
        self.pcm: bool = opus_available()
        if not self.pcm:
            log.warning(
                "libopus not found - using ffmpeg's Opus output; volume changes will "
                "restart the current track."
            )
        if ffmpeg_executable() is None:
            log.warning("ffmpeg not found - music commands will report that they can't play.")

    async def cog_load(self) -> None:
        """Log the facts that decide whether audio can work (shows in Render logs and /errors music)."""
        log.info("music: libopus loaded=%s -> %s path", self.pcm, "PCM" if self.pcm else "Opus passthrough")
        for level, line in await asyncio.to_thread(environment_report):
            log.log(level, "music env: %s", line)

    async def cog_unload(self) -> None:
        log.removeHandler(self.trace)
        for guild_id in list(self.players):
            guild = self.bot.get_guild(guild_id)
            if guild is not None:
                await self.destroy_player(guild, disconnect=True)

    # -- plumbing --------------------------------------------------------- #

    def record_error(self, where: str, error: Any, guild: Optional[discord.Guild]) -> None:
        recorder = getattr(self.bot, "log_error", None)
        if recorder is None:
            return
        with contextlib.suppress(Exception):
            recorder(where, error, guild=guild)

    async def extract(self, guild_id: Optional[int], func: Any, *args: Any) -> Any:
        """Run a blocking yt-dlp call in a worker thread, one at a time.

        The lock is released when the *worker* finishes, not when the caller gives up: a timeout
        abandons the wait (asyncio.TimeoutError) but the thread keeps running, and letting the
        next extraction start meanwhile would break the one-at-a-time memory guarantee.

        One RESOLVE_TIMEOUT deadline covers waiting for the lock *and* the extraction itself, so a
        stuck worker can't leave other servers (or this server's skip/stop) waiting forever."""
        LOG_GUILD.set(guild_id)  # tags the worker's log lines with this server
        loop = asyncio.get_running_loop()
        deadline: float = loop.time() + RESOLVE_TIMEOUT
        if self.extract_lock.locked():
            log.info("extract: waiting for another extraction to finish")
        try:
            await asyncio.wait_for(self.extract_lock.acquire(), RESOLVE_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning("extract: gave up waiting %.0fs for another extraction to finish", RESOLVE_TIMEOUT)
            raise ExtractionBusy() from None
        remaining: float = deadline - loop.time()
        if remaining <= 0:  # the wait used the whole budget; don't start work nobody is waiting for
            self.extract_lock.release()
            raise ExtractionBusy()
        try:
            worker: "asyncio.Future[Any]" = asyncio.ensure_future(asyncio.to_thread(func, *args))
        except BaseException:
            self.extract_lock.release()
            raise

        def _finished(done: "asyncio.Future[Any]") -> None:
            self.extract_lock.release()
            if not done.cancelled():
                done.exception()  # mark retrieved: a caller that timed out never awaits it

        worker.add_done_callback(_finished)
        return await asyncio.wait_for(asyncio.shield(worker), remaining)

    def record_threadsafe(self, where: str, message: str, guild_id: Optional[int]) -> None:
        """Feed /errors from any thread (ffmpeg's stderr reader, yt-dlp worker, logging)."""

        def _record() -> None:
            guild = self.bot.get_guild(guild_id) if guild_id else None
            self.record_error(where, message, guild)

        try:
            self.bot.loop.call_soon_threadsafe(_record)
        except Exception:  # loop closed or not running (shutdown): nothing useful left to do
            pass

    def get_player(self, guild: discord.Guild) -> GuildPlayer:
        player = self.players.get(guild.id)
        if player is None:
            player = GuildPlayer(self, guild)
            self.players[guild.id] = player
        return player

    async def destroy_player(self, guild: discord.Guild, *, disconnect: bool) -> None:
        player = self.players.pop(guild.id, None)
        if player is not None:
            player.shutdown()
        vc = guild.voice_client
        if disconnect and vc is not None:
            with contextlib.suppress(Exception):
                await vc.disconnect(force=True)

    @contextlib.asynccontextmanager
    async def _guard(self, ctx: commands.Context) -> AsyncIterator[None]:
        """Show MusicError messages privately instead of treating them as crashes."""
        try:
            yield
        except MusicError as exc:
            await ctx.send(f"❌ {exc}", ephemeral=True)

    @staticmethod
    def _can_force(member: discord.Member, player: GuildPlayer) -> bool:
        """Bypass votes: staff, or the only person listening."""
        if _is_superuser(member):
            return True
        perms = member.guild_permissions
        if perms.administrator or perms.manage_channels or perms.move_members:
            return True
        humans = player.listeners()
        return len(humans) <= 1 and member in humans

    # -- voice requirements ----------------------------------------------- #

    @staticmethod
    def _user_channel(ctx: commands.Context) -> VoiceChannel:
        voice = getattr(ctx.author, "voice", None)
        if voice is None or voice.channel is None:
            raise MusicError("Join a voice channel first.")
        return voice.channel

    async def _connect(self, ctx: commands.Context) -> GuildPlayer:
        """Make sure the bot is in the caller's voice channel and return its player."""
        if not discord.voice_client.has_nacl:
            raise MusicError("Voice support (PyNaCl) isn't installed on the host.")
        if ffmpeg_executable() is None:
            raise MusicError("ffmpeg isn't installed on the host, so I can't play audio.")

        channel: VoiceChannel = self._user_channel(ctx)
        guild: discord.Guild = ctx.guild
        player: GuildPlayer = self.get_player(guild)
        vc = player.voice

        if vc is not None:
            if vc.channel.id != channel.id:
                if player.active and _humans(vc.channel):
                    raise MusicError(
                        f"I'm already playing in {vc.channel.mention}. Join it to queue songs."
                    )
                await vc.move_to(channel)
        else:
            perms = channel.permissions_for(guild.me)
            if not perms.connect or not perms.speak:
                raise MusicError(f"I need **Connect** and **Speak** in {channel.mention}.")
            limit = getattr(channel, "user_limit", 0)
            if limit and len(channel.members) >= limit and not perms.move_members:
                raise MusicError(f"{channel.mention} is full.")
            stale = guild.voice_client
            if stale is not None:  # half-dead connection left behind
                with contextlib.suppress(Exception):
                    await stale.disconnect(force=True)
            try:
                await channel.connect(self_deaf=True, timeout=CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                raise MusicError("I couldn't connect to that voice channel in time.")
            except discord.ClientException as exc:
                raise MusicError(f"I couldn't join voice: {exc}")
            if isinstance(channel, discord.StageChannel):
                with contextlib.suppress(discord.HTTPException):
                    await guild.me.edit(suppress=False)

        player.text_channel = ctx.channel
        player.refresh_timer()
        return player

    def _control(self, ctx: commands.Context, *, need_track: bool = False) -> GuildPlayer:
        """The player for a user who must be in the bot's voice channel."""
        player = self.players.get(ctx.guild.id)
        vc = player.voice if player else None
        if player is None or vc is None:
            raise MusicError("I'm not in a voice channel — start with `/play`.")
        channel: VoiceChannel = self._user_channel(ctx)
        if channel.id != vc.channel.id:
            raise MusicError(f"Join {vc.channel.mention} to control the music.")
        if need_track and player.queue.current is None:
            raise MusicError("Nothing is playing right now.")
        player.text_channel = ctx.channel
        return player

    def _require_force(self, ctx: commands.Context, player: GuildPlayer, what: str) -> None:
        if not self._can_force(ctx.author, player):
            raise MusicError(
                f"Only staff (Manage Channels) or the only listener can {what}. "
                "Try `/skip` to vote instead."
            )

    # -- embeds ----------------------------------------------------------- #

    def now_playing_embed(self, player: GuildPlayer, *, title: str = "🎶 Now playing") -> discord.Embed:
        track: Optional[Track] = player.queue.current
        embed = discord.Embed(title=title, color=MUSIC_COLOR)
        if track is None:
            embed.description = "Nothing is playing."
            return embed
        position: float = player.clock.position()
        if track.duration:
            position = min(position, track.duration)
        embed.description = (
            f"**{_track_link(track, 80)}**\n"
            f"{progress_bar(position, track.duration)}\n"
            f"`{format_duration(position)} / {format_duration(track.duration)}`"
            if not track.is_live
            else f"**{_track_link(track, 80)}**\n🔴 LIVE"
        )
        embed.add_field(name="Requested by", value=discord.utils.escape_markdown(track.requester_name))
        embed.add_field(name="Volume", value=f"{player.volume}%")
        embed.add_field(name="Loop", value=player.queue.loop.value.title())
        if track.thumbnail:
            embed.set_thumbnail(url=track.thumbnail)
        waiting: int = len(player.queue)
        if waiting:
            embed.set_footer(text=f"{waiting} more in the queue")
        return embed

    # -- play / join / leave ---------------------------------------------- #

    async def _play(self, ctx: commands.Context, query: str, *, front: bool) -> None:
        try:
            parsed: ParsedQuery = parse_query(query)
        except InvalidQuery as exc:
            raise MusicError(str(exc))
        player: GuildPlayer = await self._connect(ctx)
        if len(player.queue) >= player.queue.max_size and not front:
            raise MusicError(f"The queue is full ({player.queue.max_size} tracks).")

        player.log.info("command: play query=%.80r front=%s user=%s", query, front, ctx.author.id)
        async with ctx.typing():
            try:
                resolved = await self.extract(
                    ctx.guild.id, resolve_tracks, parsed, ctx.author.id, ctx.author.display_name
                )
            except ExtractionError as exc:
                player.log.warning("command: lookup failed: %s", exc)
                raise MusicError(str(exc))
            except asyncio.TimeoutError:
                player.log.error("command: lookup timed out after %.0fs", RESOLVE_TIMEOUT)
                raise MusicError("YouTube took too long to respond. Try again in a moment.")
            except Exception as exc:
                log.exception("resolve_tracks failed for %r", query)
                self.record_error("music:resolve", exc, ctx.guild)
                raise MusicError("Something went wrong looking that up.")

        tracks: List[Track] = [
            t for t in resolved.tracks if t.duration is None or t.duration <= MAX_TRACK_SECONDS
        ]
        too_long: int = len(resolved.tracks) - len(tracks)
        if not tracks:
            raise MusicError(
                f"That's longer than the {format_duration(MAX_TRACK_SECONDS)} limit."
            )

        before: int = len(player.queue)
        try:
            added: int = player.queue.add_many(tracks, front=front)
        except QueueFull:
            added = 0
        if added == 0:
            raise MusicError(f"The queue is full ({player.queue.max_size} tracks).")

        started: bool = await player.ensure_playing(announce=False)
        notes: List[str] = []
        if too_long:
            notes.append(f"{too_long} skipped for length")
        if added < len(tracks):
            notes.append(f"{len(tracks) - added} didn't fit in the queue")
        footer: str = " · ".join(notes)

        if started:
            embed = self.now_playing_embed(player, title="▶️ Now playing")
            if added > 1:
                embed.add_field(
                    name="Playlist",
                    value=f"Queued **{added - 1}** more from {_clean_title(resolved.playlist_title or 'the playlist', 80)}.",
                    inline=False,
                )
        elif player.queue.current is None and not player.active:
            raise MusicError("I couldn't play that — see the message above for why.")
        else:
            first: Track = tracks[0]
            position: int = 1 if front else before + 1
            if added == 1:
                embed = discord.Embed(
                    title="➕ Added to the queue",
                    description=f"**{_track_link(first, 80)}**",
                    color=MUSIC_COLOR,
                )
                embed.add_field(name="Position", value=f"#{position}")
                embed.add_field(name="Length", value=format_duration(first.duration))
                if first.thumbnail:
                    embed.set_thumbnail(url=first.thumbnail)
            else:
                embed = discord.Embed(
                    title="➕ Playlist added",
                    description=(
                        f"**{added}** tracks from "
                        f"**{_clean_title(resolved.playlist_title or 'the playlist', 80)}**"
                    ),
                    color=MUSIC_COLOR,
                )
        if footer:
            embed.set_footer(text=footer)
        await ctx.send(embed=embed)

    @commands.hybrid_command(
        name="play",
        aliases=["p"],
        description="Play a YouTube or YouTube Music link (or a search) in your voice channel",
    )
    @app_commands.describe(query="YouTube / YouTube Music link, or words to search for")
    @commands.guild_only()
    async def play(self, ctx: commands.Context, *, query: str) -> None:
        async with self._guard(ctx):
            await self._play(ctx, query, front=False)

    @commands.hybrid_command(
        name="playnext",
        aliases=["pn"],
        description="Queue a YouTube link (or search) to play right after the current track",
    )
    @app_commands.describe(query="YouTube / YouTube Music link, or words to search for")
    @commands.guild_only()
    async def playnext(self, ctx: commands.Context, *, query: str) -> None:
        async with self._guard(ctx):
            await self._play(ctx, query, front=True)

    @commands.hybrid_command(
        name="leave",
        aliases=["disconnect", "dc"],
        description="Make the bot leave the voice channel and clear the queue",
    )
    @commands.guild_only()
    async def leave(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            self._require_force(ctx, player, "make me leave")
            await self.destroy_player(ctx.guild, disconnect=True)
            await ctx.send("👋 Left the voice channel.")

    # -- transport -------------------------------------------------------- #

    @commands.hybrid_command(name="pause", description="Pause the current track")
    @commands.guild_only()
    async def pause(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx, need_track=True)
            if player.voice.is_paused():
                raise MusicError("Already paused — use `/resume`.")
            if not player.active:
                raise MusicError("Nothing is playing right now.")
            player.pause()
            await ctx.send("⏸️ Paused.")

    @commands.hybrid_command(name="resume", aliases=["unpause"], description="Resume the paused track")
    @commands.guild_only()
    async def resume(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx, need_track=True)
            if not player.voice.is_paused():
                raise MusicError("Nothing is paused.")
            player.resume()
            await ctx.send("▶️ Resumed.")

    @commands.hybrid_command(
        name="skip", aliases=["s", "next"], description="Skip the current track (vote if others are listening)"
    )
    @commands.guild_only()
    async def skip(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx, need_track=True)
            track: Track = player.queue.current  # type: ignore[assignment]
            listeners = player.listeners()
            instant: bool = ctx.author.id == track.requester_id or self._can_force(ctx.author, player)
            if not instant:
                player.skip_votes.add(ctx.author.id)
                present: Set[int] = {m.id for m in listeners}
                player.skip_votes &= present
                needed: int = required_votes(len(listeners))
                if len(player.skip_votes) < needed:
                    await ctx.send(f"🗳️ Skip vote: **{len(player.skip_votes)}/{needed}**")
                    return
            await ctx.send(f"⏭️ Skipped **{_clean_title(track.title)}**.")
            await player.skip()

    @commands.hybrid_command(name="stop", description="Stop playing and clear the queue (I stay in the channel)")
    @commands.guild_only()
    async def stop(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            self._require_force(ctx, player, "stop the music")
            await player.stop()
            await ctx.send("⏹️ Stopped and cleared the queue.")

    @commands.hybrid_command(
        name="nowplaying", aliases=["np", "current"], description="Show what's playing right now"
    )
    @commands.guild_only()
    async def nowplaying(self, ctx: commands.Context) -> None:
        player = self.players.get(ctx.guild.id)
        if player is None or player.queue.current is None:
            await ctx.send("❌ Nothing is playing right now.", ephemeral=True)
            return
        await ctx.send(embed=self.now_playing_embed(player))

    @commands.hybrid_command(name="volume", aliases=["vol"], description=f"Show or set the volume (0-{MAX_VOLUME}%)")
    @app_commands.describe(level=f"New volume, 0-{MAX_VOLUME} (leave empty to see the current one)")
    @commands.guild_only()
    async def volume(self, ctx: commands.Context, level: Optional[int] = None) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            if level is None:
                await ctx.send(f"🔊 Volume is **{player.volume}%**.")
                return
            if not 0 <= level <= MAX_VOLUME:
                raise MusicError(f"Volume must be between 0 and {MAX_VOLUME}.")
            await player.set_volume(level)
            note = "" if self.pcm else " (applies from the current position onward)"
            await ctx.send(f"🔊 Volume set to **{level}%**{note}.")

    @commands.hybrid_command(name="loop", aliases=["repeat"], description="Loop the track or the whole queue")
    @app_commands.describe(mode="off, track or queue (leave empty to cycle)")
    @commands.guild_only()
    async def loop(self, ctx: commands.Context, mode: Optional[Literal["off", "track", "queue"]] = None) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            chosen: LoopMode = LoopMode(mode) if mode else player.queue.loop.next()
            player.queue.loop = chosen
            labels = {
                LoopMode.OFF: "➡️ Loop is **off**.",
                LoopMode.TRACK: "🔂 Looping the **current track**.",
                LoopMode.QUEUE: "🔁 Looping the **whole queue**.",
            }
            await ctx.send(labels[chosen])

    @commands.hybrid_command(name="seek", description="Jump to a time in the current track (e.g. 1:30 or 90)")
    @app_commands.describe(position="Where to jump to, like 1:30, 90 or 1m30s")
    @commands.guild_only()
    async def seek(self, ctx: commands.Context, position: str) -> None:
        async with self._guard(ctx):
            player = self._control(ctx, need_track=True)
            track: Track = player.queue.current  # type: ignore[assignment]
            if track.is_live or track.duration is None:
                raise MusicError("You can't seek in a live stream.")
            try:
                seconds: int = parse_timestamp(position)
            except ValueError as exc:
                raise MusicError(str(exc))
            if seconds >= track.duration:
                raise MusicError(f"That track is only {format_duration(track.duration)} long.")
            async with ctx.typing():
                ok: bool = await player.seek(seconds)
            if not ok:
                raise MusicError("I couldn't seek there.")
            await ctx.send(f"⏩ Jumped to **{format_duration(seconds)}**.")

    # -- queue ------------------------------------------------------------ #

    @commands.hybrid_group(
        name="queue",
        aliases=["q"],
        fallback="show",
        invoke_without_command=True,
        description="Show the queue, or manage it with shuffle / remove / move / clear / skipto",
    )
    @commands.guild_only()
    async def queue(self, ctx: commands.Context) -> None:
        player = self.players.get(ctx.guild.id)
        if player is None or (player.queue.current is None and len(player.queue) == 0):
            await ctx.send("📭 The queue is empty — add something with `/play`.", ephemeral=True)
            return

        lines: List[str] = []
        current: Optional[Track] = player.queue.current
        if current is not None:
            state: str = "⏸️" if player.clock.paused else "▶️"
            lines.append(
                f"{state} **Now:** {_track_link(current, 55)} `{format_duration(current.duration)}`"
            )
        for index, track in enumerate(player.queue.upcoming, start=1):
            lines.append(
                f"`{index}.` {_track_link(track, 50)} `{format_duration(track.duration)}` · "
                f"{discord.utils.escape_markdown(track.requester_name)}"
            )
        if len(lines) == 1:
            lines.append("*Nothing queued after this — add more with `/play`.*")

        remaining: str = format_duration(player.queue.remaining_seconds())
        footer: str = (
            f"{len(player.queue)} waiting · {remaining} up next · "
            f"Loop: {player.queue.loop.value} · Volume {player.volume}%"
        )
        pages = build_pages("🎵 Queue", lines, MUSIC_COLOR, per_page=10, footer=footer)
        await send_pages(ctx, pages)

    @queue.command(name="shuffle", description="Shuffle the waiting tracks")
    async def queue_shuffle(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            if len(player.queue) < 2:
                raise MusicError("Need at least two waiting tracks to shuffle.")
            player.queue.shuffle()
            await ctx.send(f"🔀 Shuffled **{len(player.queue)}** tracks.")

    @queue.command(name="remove", description="Remove a track from the queue by its position")
    @app_commands.describe(position="Position shown in /queue")
    async def queue_remove(self, ctx: commands.Context, position: int) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            try:
                target: Track = player.queue.peek(position)
            except IndexError as exc:
                raise MusicError(str(exc))
            if target.requester_id != ctx.author.id:
                self._require_force(ctx, player, "remove other people's tracks")
            player.queue.remove(position)
            await ctx.send(f"🗑️ Removed **{_clean_title(target.title)}**.")

    @queue.command(name="move", description="Move a queued track to a new position")
    @app_commands.describe(source="Current position", destination="New position")
    async def queue_move(self, ctx: commands.Context, source: int, destination: int) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            try:
                track: Track = player.queue.move(source, destination)
            except IndexError as exc:
                raise MusicError(str(exc))
            await ctx.send(f"↕️ Moved **{_clean_title(track.title)}** to #{destination}.")

    @queue.command(name="clear", description="Remove every waiting track (keeps the current one playing)")
    async def queue_clear(self, ctx: commands.Context) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            self._require_force(ctx, player, "clear the queue")
            removed: int = player.queue.clear()
            await ctx.send(f"🧹 Cleared **{removed}** tracks.")

    @queue.command(name="skipto", description="Skip ahead to a position in the queue")
    @app_commands.describe(position="Position shown in /queue")
    async def queue_skipto(self, ctx: commands.Context, position: int) -> None:
        async with self._guard(ctx):
            player = self._control(ctx)
            self._require_force(ctx, player, "skip ahead")
            try:
                player.queue.peek(position)
            except IndexError as exc:
                raise MusicError(str(exc))
            async with ctx.typing():
                target: Track = await player.skip_to(position)
            await ctx.send(f"⏭️ Skipped to **{_clean_title(target.title)}**.")

    # -- listeners -------------------------------------------------------- #

    @commands.Cog.listener()
    async def on_voice_state_update(
        self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
    ) -> None:
        player = self.players.get(member.guild.id)
        if player is None:
            return
        if self.bot.user is not None and member.id == self.bot.user.id:
            if after.channel is None:  # kicked, disconnected, or channel deleted
                self.players.pop(member.guild.id, None)
                player.shutdown()
            else:
                player.refresh_timer()
            return
        vc = player.voice
        if vc is not None and vc.channel in (before.channel, after.channel):
            player.refresh_timer()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MusicCog(bot))
