"""The DebtCollectorBot class and the process-wide `settings_manager` / `bot` singletons."""

import asyncio
import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from utils.attachment_cache import AttachmentCache
from utils.runtime_stats import LoopLagMonitor
from core.config import AfkRecord, COMMAND_PREFIX, SnipedMessage
from core.errors import ErrorRecorder, ErrorRecorderHandler
from core.settings import MultiTenantSettingsManager
from core import registry

log: logging.Logger = logging.getLogger("debt-collector")


async def get_prefix(bot: "DebtCollectorBot", message: discord.Message) -> str:
    """Async prefix resolver - never blocks the event loop on a cache miss."""
    if message.guild is None or not hasattr(bot, "settings"):
        return COMMAND_PREFIX
    settings: Dict[str, Any] = await bot.settings.fetch_settings(message.guild.id)
    prefix: Any = settings.get("prefix", COMMAND_PREFIX)
    return prefix if isinstance(prefix, str) and prefix else COMMAND_PREFIX


# --------------------------------------------------------------------------- #
# Bot setup
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# AI Flush Loop
# --------------------------------------------------------------------------- #


async def ai_flush() -> None:
    """Append only the new AI messages, capped server-side, with a TTL stamp."""
    if not bot.ai_history_dirty:
        return

    dirty_channels = list(bot.ai_history_dirty)
    bot.ai_history_dirty.clear()

    operations = []
    drained: Dict[int, List[Dict[str, Any]]] = {}
    for channel_id in dirty_channels:
        pending = bot.ai_pending.pop(channel_id, [])
        if not pending:
            continue
        drained[channel_id] = pending
        operations.append(
            UpdateOne(
                {"channel_id": str(channel_id)},
                {
                    "$push": {"history": {"$each": pending, "$slice": -50}},
                    "$set": {"updated_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )
        )

    if not operations:
        return

    try:
        await asyncio.to_thread(
            bot.settings.ai_history.bulk_write, operations, ordered=False
        )
    except PyMongoError as exc:
        bot.log_error("ai:flush", exc)
        # Put the unsaved messages back so the next tick retries them.
        for channel_id, pending in drained.items():
            bot.ai_pending.setdefault(channel_id, [])[:0] = pending
            bot.ai_history_dirty.add(channel_id)


def command_signature(tree: app_commands.CommandTree) -> str:
    """Digest of the slash-command surface; a change means Discord needs a sync."""
    parts: List[str] = []
    for command in tree.walk_commands():
        params = ",".join(
            f"{p.name}:{getattr(p.type, 'name', p.type)}:{int(p.required)}:{p.description}"
            f":{'|'.join(str(c.value) for c in (p.choices or []))}"
            for p in (getattr(command, "parameters", None) or [])
        )
        parts.append(
            f"{command.qualified_name}|{getattr(command, 'description', '')}|{params}"
            f"|{getattr(command, 'default_permissions', None)}"
            f"|{getattr(command, 'guild_only', False)}"
        )
    return hashlib.sha256("::".join(sorted(parts)).encode("utf-8")).hexdigest()


class DebtCollectorBot(commands.Bot):
    def __init__(self, settings_manager: MultiTenantSettingsManager) -> None:
        intents: discord.Intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.typing = False
        intents.presences = False
        super().__init__(
            command_prefix=get_prefix,
            intents=intents,
            help_command=None,
            allowed_mentions=discord.AllowedMentions(roles=True),
            member_cache_flags=discord.MemberCacheFlags.from_intents(intents),
            chunk_guilds_at_startup=False,
        )
        self.settings: MultiTenantSettingsManager = settings_manager
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.start_time: float = time.time()
        self.afk_state: Dict[int, "AfkRecord"] = {}
        self.snipes: Dict[int, SnipedMessage] = {}
        self.next_fire: Dict[str, float] = {}
        self.starboard_webhooks: Dict[int, Any] = {}
        self.sticky_locks: Dict[int, asyncio.Lock] = {}
        self.sticky_last: Dict[int, float] = {}
        self.ai_history_buffer: Dict[int, List[Dict[str, Any]]] = (
            {}
        )  # channel_id -> messages
        self.ai_pending: Dict[int, List[Dict[str, Any]]] = (
            {}
        )  # channel_id -> unsaved messages
        self.ai_history_dirty: set = set()  # channel_ids that need flushing
        self.ai_active_conversations: Dict[int, float] = {}  # channel_id -> timestamp
        self.ai_next_fire: Dict[int, float] = {}  # channel_id -> cooldown expiry
        self.ai_locks: Dict[int, asyncio.Lock] = {}  # channel_id -> generation lock
        self.ai_lru: Dict[int, float] = {}  # channel_id -> last touched
        self.ai_repeat: Dict[int, Dict[str, Any]] = {}  # channel_id -> repeat tracker
        self.ai_user_recent: Dict[Tuple[int, int], List[float]] = {}  # burst tracker
        self.ai_stats: Dict[str, Dict[str, Any]] = {}  # provider -> counters
        self.ai_daily: Dict[str, int] = {}  # "guild:date" -> replies used
        self.automod_recent: Dict[Any, List[float]] = {}
        self.automod_strikes: Dict[Any, List[float]] = {}
        self.errors: ErrorRecorder = ErrorRecorder()
        self._background_tasks: set = set()
        # Attachments of recent messages, so the message log can re-upload them
        # after a delete. Byte-budgeted LRU (ATTACHMENT_CACHE_MB / _TTL_S).
        self.attachment_cache: AttachmentCache = AttachmentCache()
        self.attachment_semaphore: asyncio.Semaphore = asyncio.Semaphore(2)
        self.loop_lag: LoopLagMonitor = LoopLagMonitor()
        self._closing_started: bool = False

    def spawn(self, coro: Any, *, name: Optional[str] = None) -> Any:
        """Fire-and-forget with a hard reference and a terminal error sink."""
        task = asyncio.get_running_loop().create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._reap_task)
        return task

    def _reap_task(self, task: Any) -> None:
        if task.cancelled():
            return
        exc: Optional[BaseException] = task.exception()
        if exc is not None:
            self.errors.record(f"task:{task.get_name()}", exc)

    def log_error(
        self,
        where: str,
        err: Any,
        *,
        guild: Optional[discord.Guild] = None,
        channel: Optional[Any] = None,
        user: Optional[discord.abc.User] = None,
        command: Optional[str] = None,
    ) -> None:
        self.errors.record(
            where, err, guild=guild, channel=channel, user=user, command=command
        )

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession(
            headers={"User-Agent": "DebtCollectorBot"},
            timeout=aiohttp.ClientTimeout(total=30.0, connect=10.0, sock_read=25.0),
            connector=aiohttp.TCPConnector(
                limit=50, limit_per_host=10, ttl_dns_cache=300
            ),
        )
        try:
            await asyncio.to_thread(self.settings.ensure_indexes)
        except Exception as exc:
            log.warning("Index setup failed: %s", exc)
        for view_factory in registry.PERSISTENT_VIEWS:
            self.add_view(view_factory())
        self.add_dynamic_items(*registry.DYNAMIC_ITEMS)
        try:
            await self.load_extension("cogs.music")
        except Exception as exc:
            log.error("Music module failed to load - continuing without it: %s", exc)
        await self._sync_commands_if_changed()
        for loop_task in registry.STARTUP_LOOPS:
            if not loop_task.is_running():
                loop_task.start()
        self.loop_lag.start()

    async def _sync_commands_if_changed(self) -> None:
        """Only hit Discord's sync endpoint when the command surface actually changed."""
        digest = command_signature(self.tree)
        stored = await asyncio.to_thread(self.settings.get_meta, "command_hash")
        if os.getenv("SYNC_COMMANDS") == "1" or stored != digest:
            await self.tree.sync()
            await asyncio.to_thread(self.settings.set_meta, "command_hash", digest)
            log.info("Slash commands synced (signature %s).", digest[:8])
        else:
            log.info("Command signature unchanged — skipping sync.")

    async def _leave_voice(self) -> None:
        # Unloading the music cog runs its cog_unload, which stops every player and
        # disconnects it the same way /stop does.
        if "cogs.music" in self.extensions:
            try:
                await self.unload_extension("cogs.music")
            except Exception as exc:
                log.warning("Unloading the music cog failed: %s", exc)
        for voice_client in list(self.voice_clients):
            try:
                await voice_client.disconnect(force=True)
            except Exception as exc:
                log.debug("Voice disconnect failed: %s", exc)

    async def _close_http_session(self) -> None:
        session, self.http_session = self.http_session, None
        if session is not None and not session.closed:
            await session.close()

    @staticmethod
    def _stop_warp() -> None:
        from utils import warp

        warp.stop()

    async def close(self) -> None:
        """Flush AI history, leave voice, close HTTP + Mongo, stop WARP, then the gateway.

        Each step has its own time budget so one slow step can't eat Render's grace
        period; the signal path also wraps the whole thing in SHUTDOWN_TIMEOUT_S.
        """
        if self._closing_started:
            await super().close()
            return
        self._closing_started = True
        started: float = time.monotonic()

        async def step(name: str, action: Callable[[], Any], budget: float) -> None:
            try:
                await asyncio.wait_for(action(), budget)
            except asyncio.TimeoutError:
                log.warning("Shutdown step '%s' timed out after %.0fs.", name, budget)
            except Exception as exc:
                log.warning("Shutdown step '%s' failed: %s", name, exc)

        await step("flush AI history", ai_flush, 6.0)
        await step("disconnect voice", self._leave_voice, 4.0)
        await step("close HTTP session", self._close_http_session, 2.0)
        await step("close MongoDB", lambda: asyncio.to_thread(self.settings.close), 3.0)
        await step("stop WARP", lambda: asyncio.to_thread(self._stop_warp), 3.0)
        self.loop_lag.stop()
        await super().close()
        log.info("Shutdown finished in %.1fs.", time.monotonic() - started)


settings_manager: MultiTenantSettingsManager = MultiTenantSettingsManager()
bot: DebtCollectorBot = DebtCollectorBot(settings_manager)
bot.errors.bind(bot)

logging.getLogger().addHandler(ErrorRecorderHandler(bot.errors))
