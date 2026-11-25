"""YouTube / YouTube Music voice playback.

Slash and prefix commands share one implementation (hybrid commands). Playback
runs per guild through a `GuildPlayer`; blocking yt-dlp work is pushed to threads.
"""

import asyncio
import contextlib
import logging
import os
import shlex
import time
from typing import Any, AsyncIterator, Dict, List, Literal, Optional, Set, Union

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import is_superuser
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
    ATTEMPT_BUDGET_SECONDS,
    ExtractionError,
    StreamInfo,
    ffmpeg_executable,
    ffmpeg_input_options,
    resolve_stream,
    stream_failed,
    resolve_tracks,
    set_route_hooks,
    start_warp_early,
    warm_up,
)
from utils import warp

log = logging.getLogger("debt-collector.music")

IDLE_TIMEOUT: float = 180.0  # leave this long after the queue runs dry
ALONE_TIMEOUT: float = 120.0  # leave this long after everyone else has left
# The client rotation stops starting new attempts after its budget; leave room for the last one.
RESOLVE_TIMEOUT: float = ATTEMPT_BUDGET_SECONDS + 45.0
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

# Bot-meta keys (MultiTenantSettingsManager.get_meta / set_meta).
WARP_HINT_KEY: str = "music:warp_needed"  # {"since": unix ts}: YouTube refused this host
WARP_ACCOUNT_KEY: str = "music:warp_account"  # the WARP registration, private key included
WARP_HINT_MAX_AGE: float = 7 * 86400.0


def warp_hint_is_fresh(record: Any, now: float, max_age: float = WARP_HINT_MAX_AGE) -> bool:
    """Whether a music:warp_needed record says this host needed WARP recently."""
    if not isinstance(record, dict):
        return False
    since = record.get("since")
    if isinstance(since, bool) or not isinstance(since, (int, float)):
        return False
    return 0 <= now - since < max_age


class MusicError(Exception):
    """A problem to show the user (never a bug)."""


def _check_davey() -> None:
    """Warn when davey is too new for this discord.py: voice then plays but nobody hears it."""
    try:
        from importlib.metadata import version

        davey_version: str = version("davey")
    except Exception:
        return
    parts = davey_version.split(".")
    too_new: bool = len(parts) >= 2 and parts[0] == "0" and parts[1].isdigit() and int(parts[1]) >= 2
    if too_new and discord.__version__ in ("2.7.0", "2.7.1"):
        log.warning(
            "davey %s with discord.py %s: listeners won't hear the bot (DAVE voice encryption "
            "mismatch). Install davey<0.2 (requirements.txt pins it).",
            davey_version, discord.__version__,
        )
    else:
        log.info("Voice encryption: davey %s, discord.py %s", davey_version, discord.__version__)


def _voice_encryption_state(vc: Any) -> str:
    """DAVE (Discord's end-to-end voice encryption) status, for the logs.

    When a channel uses DAVE and the bot's session isn't ready, Discord shows the bot
    speaking but listeners hear silence, so this is the first thing to check.
    """
    connection = getattr(vc, "_connection", None)
    version = getattr(connection, "dave_protocol_version", None)
    if version is None:
        return "DAVE status unknown"
    if not version:
        return "DAVE not in use"
    return f"DAVE v{version}, ready={bool(getattr(connection, 'can_encrypt', False))}"


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
    """The bot's superuser check (core.permissions)."""
    return is_superuser(user)


def _humans(channel: Optional[VoiceChannel]) -> List[discord.Member]:
    if channel is None:
        return []
    return [m for m in channel.members if not m.bot]


class GuildPlayer:
    """Queue, voice state and the playback loop for one guild."""

    def __init__(self, cog: "MusicCog", guild: discord.Guild) -> None:
        self.cog: "MusicCog" = cog
        self.guild: discord.Guild = guild
        self.queue: MusicQueue = MusicQueue()
        self.clock: PlaybackClock = PlaybackClock()
        self.volume: int = DEFAULT_VOLUME
        self.text_channel: Optional[discord.abc.Messageable] = None
        self.skip_votes: Set[int] = set()
        self._gen: int = 0  # bumps on every (re)start/stop so stale callbacks are ignored
        self._quick_failures: int = 0  # consecutive tracks that died almost instantly
        self._lock: asyncio.Lock = asyncio.Lock()
        self._timer: Optional[asyncio.Task] = None
        self.last_used: float = time.monotonic()  # for the bot's idle-state janitor

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
        before.extend(shlex.quote(part) for part in ffmpeg_input_options())
        before_options: str = " ".join(before)

        if self.cog.pcm:
            raw = discord.FFmpegPCMAudio(
                stream.url, executable=executable, before_options=before_options, options="-vn"
            )
            return discord.PCMVolumeTransformer(raw, volume=self.volume / 100)
        # No libopus on this host: hand ffmpeg's Opus output straight to Discord and
        # apply volume as an ffmpeg filter (takes effect on the next (re)start).
        return discord.FFmpegOpusAudio(
            stream.url,
            executable=executable,
            before_options=before_options,
            options=f"-vn -af volume={self.volume / 100:.2f}",
        )

    async def _start(self, track: Track, position: float = 0.0, *, announce: bool = True) -> bool:
        """Begin playing `track`. Caller holds the lock. False means it couldn't play."""
        vc = self.voice
        if vc is None:
            return False
        try:
            stream: StreamInfo = await asyncio.wait_for(
                asyncio.to_thread(resolve_stream, track), RESOLVE_TIMEOUT
            )
            source = self._build_source(stream, position)
        except MusicError as exc:
            await self.notify(f"❌ {exc}")
            return False
        except ExtractionError as exc:
            await self.notify(f"⚠️ Skipping **{_clean_title(track.title)}** — {exc}")
            return False
        except asyncio.TimeoutError:
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
            return False

        self._gen += 1
        generation: int = self._gen
        if vc.is_playing() or vc.is_paused():
            vc.stop()
        vc.play(source, after=lambda error, g=generation: self._after(g, error))
        self.clock.start(position)
        log.info(
            "Playing %s (%s) in guild %s from %ss; %s",
            track.url, format_duration(track.duration), self.guild.id, int(position),
            _voice_encryption_state(vc),
        )
        self.refresh_timer()
        if announce:
            await self.notify(embed=self.cog.now_playing_embed(self, title="▶️ Now playing"))
        return True

    def _after(self, generation: int, error: Optional[Exception]) -> None:
        """Runs on discord.py's player thread when a source finishes or is stopped."""
        future = asyncio.run_coroutine_threadsafe(self._on_end(generation, error), self.cog.bot.loop)
        future.add_done_callback(self._log_future)

    @staticmethod
    def _log_future(future: Any) -> None:
        if not future.cancelled() and future.exception() is not None:
            log.error("Music end-of-track handler failed: %r", future.exception())

    async def _on_end(self, generation: int, error: Optional[Exception]) -> None:
        if generation != self._gen:
            return  # we stopped or replaced this source ourselves
        track: Optional[Track] = self.queue.current
        if track is not None:
            played: float = self.clock.position()
            log.info(
                "Stream ended for %s after %s of %s%s",
                track.url, format_duration(played), format_duration(track.duration),
                f" (error: {error})" if error else "",
            )
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
        The first time a track dies like this, it's restarted once through other YouTube
        clients, since YouTube sometimes refuses a stream it has just handed out.
        Returns True when it dealt with the end itself (restarted the track or halted
        the queue). Caller holds the lock.
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
        if track.stream_retries < 1:
            track.stream_retries += 1
            stream_failed()
            log.warning("Retrying %s after its stream died at once", track.url)
            if await self._start(track, announce=False):
                return True
            self._quick_failures += 1  # _start has already said why it couldn't play
        else:
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
        self.pcm: bool = opus_available()
        if not self.pcm:
            log.warning(
                "libopus not found - using ffmpeg's Opus output; volume changes will "
                "restart the current track."
            )
        if ffmpeg_executable() is None:
            log.warning("ffmpeg not found - music commands will report that they can't play.")
        _check_davey()
        self._warm_up_task: Optional["asyncio.Task[Any]"] = None
        self._warp_hint_saved: bool = False
        # The bot's settings manager, for remembering WARP across restarts. utils/
        # never imports main, so it gets plain callbacks instead.
        meta: Any = getattr(bot, "settings", None)
        self._meta: Any = (
            meta if callable(getattr(meta, "get_meta", None)) and callable(getattr(meta, "set_meta", None)) else None
        )
        if self._meta is not None:
            warp.set_account_store(self._load_warp_account, self._save_warp_account)
            set_route_hooks(
                warp_needed=self._remember_warp_needed,
                direct_success=self._forget_warp_needed,
            )

    # -- WARP memory (all called from worker threads) ----------------------- #

    def _load_warp_account(self) -> Optional[Dict[str, Any]]:
        return self._meta.get_meta(WARP_ACCOUNT_KEY)

    def _save_warp_account(self, account: Optional[Dict[str, Any]]) -> None:
        self._meta.set_meta(WARP_ACCOUNT_KEY, account)

    def _remember_warp_needed(self) -> None:
        self._meta.set_meta(WARP_HINT_KEY, {"since": int(time.time())})
        self._warp_hint_saved = True

    def _forget_warp_needed(self) -> None:
        if not self._warp_hint_saved:
            return
        self._meta.set_meta(WARP_HINT_KEY, None)
        self._warp_hint_saved = False
        log.info("YouTube answered this host directly; forgot that it needed WARP")

    def boot_youtube(self, warm: bool) -> None:
        """Before the first YouTube attempt: start WARP if this host needed it in the
        last week (skips ~30 s of refused direct attempts), then warm up."""
        if self._meta is not None and warp.mode() == "auto" and warp.available():
            record: Any = self._meta.get_meta(WARP_HINT_KEY)
            if record:
                self._warp_hint_saved = True  # a direct success will clear it
            if warp_hint_is_fresh(record, time.time()):
                log.info("YouTube refused this host in the last 7 days; starting Cloudflare WARP first")
                if not start_warp_early():
                    log.warning("WARP didn't start early; trying YouTube directly as usual")
        if warm:
            warm_up()

    async def cog_load(self) -> None:
        # Find a YouTube client group that works from this host before anyone asks,
        # so the first /play doesn't wait through the failing ones.
        warm: bool = os.getenv("MUSIC_WARMUP", "1").strip().lower() not in ("0", "false", "off", "no")
        if warm or self._meta is not None:
            self._warm_up_task = asyncio.create_task(asyncio.to_thread(self.boot_youtube, warm))

    async def cog_unload(self) -> None:
        if self._warm_up_task is not None:
            self._warm_up_task.cancel()
        if self._meta is not None:
            set_route_hooks()
            warp.set_account_store(None, None)
        for guild_id in list(self.players):
            guild = self.bot.get_guild(guild_id)
            if guild is not None:
                await self.destroy_player(guild, disconnect=True)

    # -- plumbing --------------------------------------------------------- #

    def record_error(self, where: str, error: BaseException, guild: Optional[discord.Guild]) -> None:
        recorder = getattr(self.bot, "log_error", None)
        if recorder is None:
            return
        with contextlib.suppress(Exception):
            recorder(where, error, guild=guild)

    def get_player(self, guild: discord.Guild) -> GuildPlayer:
        player = self.players.get(guild.id)
        if player is None:
            player = GuildPlayer(self, guild)
            self.players[guild.id] = player
        player.last_used = time.monotonic()
        return player

    async def prune_idle_players(self, idle_seconds: float) -> int:
        """Drop players with no voice connection that nobody has used for a while.

        A player normally goes away when the bot leaves voice; this catches ones left
        behind by a connect that failed half-way. Players whose lock is held are kept.
        """
        now = time.monotonic()
        stale = [
            guild_id
            for guild_id, player in self.players.items()
            if player.voice is None
            and not player._lock.locked()
            and now - player.last_used > idle_seconds
        ]
        for guild_id in stale:
            player = self.players.pop(guild_id, None)
            if player is not None:
                player.shutdown()
        return len(stale)

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
                raise MusicError("I couldn't connect to that voice channel in time.") from None
            except discord.ClientException as exc:
                raise MusicError(f"I couldn't join voice: {exc}") from None
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
            raise MusicError(str(exc)) from None
        player: GuildPlayer = await self._connect(ctx)
        if len(player.queue) >= player.queue.max_size and not front:
            raise MusicError(f"The queue is full ({player.queue.max_size} tracks).")

        async with ctx.typing():
            try:
                resolved = await asyncio.wait_for(
                    asyncio.to_thread(resolve_tracks, parsed, ctx.author.id, ctx.author.display_name),
                    RESOLVE_TIMEOUT,
                )
            except ExtractionError as exc:
                raise MusicError(str(exc)) from None
            except asyncio.TimeoutError:
                raise MusicError("YouTube took too long to respond. Try again in a moment.") from None
            except Exception as exc:
                log.exception("resolve_tracks failed for %r", query)
                self.record_error("music:resolve", exc, ctx.guild)
                raise MusicError("Something went wrong looking that up.") from None

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
                raise MusicError(str(exc)) from None
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
                raise MusicError(str(exc)) from None
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
                raise MusicError(str(exc)) from None
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
                raise MusicError(str(exc)) from None
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
