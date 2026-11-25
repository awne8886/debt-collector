import asyncio
import logging
import math
import os
import sys as _sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from pymongo.errors import PyMongoError

from utils.pagination import build_pages, send_pages
from utils.runtime_stats import LoopLagMonitor
import core.bot  # noqa: F401
import core.checks  # noqa: F401
import core.modlog  # noqa: F401
import features.reactions  # noqa: F401
import features.help  # noqa: F401
import features.errors  # noqa: F401
import features.counting  # noqa: F401
import features.roles  # noqa: F401
import features.utility  # noqa: F401
import features.purge  # noqa: F401
import features.autopurge  # noqa: F401
import features.server_settings  # noqa: F401
import features.reminders  # noqa: F401
import features.automations  # noqa: F401
import features.ai  # noqa: F401
import features.ai_commands  # noqa: F401
import features.sticky  # noqa: F401
import features.moderation  # noqa: F401
import features.raid  # noqa: F401
import features.quarantine  # noqa: F401
import features.automod  # noqa: F401
import features.screening  # noqa: F401
import features.starboard  # noqa: F401
import features.messagelog  # noqa: F401
import features.modmail  # noqa: F401
import features.greeter  # noqa: F401
import features.server_clone  # noqa: F401
from core.config import SnipedMessage
from core.permissions import custom_has_permissions, is_superuser, member_has_perms
from core.registry import STARTUP_LOOPS
from core.bot import bot
from core.checks import _COOLDOWN_STATE
from core.helpers import _extract_message_text, humanize_seconds, parse_duration
from core.stats import _format_memory_line, collect_runtime_stats
from core.web import _deploy_version
from core.runner import run
from features.help import _uncategorised_roots
from features.counting import handle_counting, handle_counting_retraction
from features.utility import _handle_afk_mentions, _handle_afk_return
from features.reminders import reminder_loop
from features.automations import _apply_guild_automations
from features.ai import _handle_ai, ai_flush_loop
from features.sticky import _handle_sticky
from features.moderation import tempban_loop
from features.automod import _handle_automod
from features.messagelog import _attachments_to_cache, _cache_single_attachment
from features.modmail import _MODMAIL_LAST_OPEN

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# State janitor - drops per-channel / per-user state nobody has touched lately
# --------------------------------------------------------------------------- #

STATE_IDLE_SECONDS: float = 3600.0


def _latest_stamp(value: Any) -> Optional[float]:
    """The most recent timestamp held by a tracker value, if it has one."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (list, tuple)):
        stamps = [v for v in value if isinstance(v, (int, float))]
        return max(stamps) if stamps else None
    if isinstance(value, dict):
        at = value.get("at")
        return float(at) if isinstance(at, (int, float)) else None
    return None


def _prune_stamped(
    mapping: Dict[Any, Any], now: float, idle: float = STATE_IDLE_SECONDS
) -> int:
    """Remove entries whose newest timestamp is older than ``idle`` (or missing)."""
    stale = [
        key
        for key, value in mapping.items()
        if (stamp := _latest_stamp(value)) is None or now - stamp > idle
    ]
    for key in stale:
        mapping.pop(key, None)
    return len(stale)


def prune_idle_state(
    target: Any,
    *,
    now: float,
    today: str,
    idle: float = STATE_IDLE_SECONDS,
    extra_stamped: Optional[Dict[str, Dict[Any, Any]]] = None,
    monotonic_now: Optional[float] = None,
    monotonic_stamped: Optional[Dict[str, Dict[Any, Any]]] = None,
) -> Dict[str, int]:
    """Drop idle per-channel / per-user state from ``target`` (the bot). Pure: no I/O.

    ``now`` is wall-clock (time.time()) and ``today`` is the UTC date
    (YYYY-MM-DD). Never removes a lock that is held. Returns removals per store.
    """
    removed: Dict[str, int] = {}

    for name in ("automod_recent", "automod_strikes", "ai_user_recent", "ai_repeat"):
        mapping = getattr(target, name, None)
        if isinstance(mapping, dict):
            removed[name] = _prune_stamped(mapping, now, idle)

    sticky_last: Dict[int, float] = getattr(target, "sticky_last", {})
    removed["sticky_last"] = _prune_stamped(sticky_last, now, idle)
    sticky_locks: Dict[int, asyncio.Lock] = getattr(target, "sticky_locks", {})
    idle_locks = [
        key
        for key, lock in sticky_locks.items()
        if key not in sticky_last and not lock.locked()
    ]
    for key in idle_locks:
        sticky_locks.pop(key, None)
    removed["sticky_locks"] = len(idle_locks)

    ai_daily: Dict[str, int] = getattr(target, "ai_daily", {})
    old_days = [key for key in ai_daily if not key.endswith(f":{today}")]
    for key in old_days:
        ai_daily.pop(key, None)
    removed["ai_daily"] = len(old_days)

    snipes: Dict[int, Any] = getattr(target, "snipes", {})
    old_snipes = []
    for key, sniped in snipes.items():
        deleted_at = getattr(sniped, "deleted_at", None)
        if deleted_at is None or now - deleted_at.timestamp() > idle:
            old_snipes.append(key)
    for key in old_snipes:
        snipes.pop(key, None)
    removed["snipes"] = len(old_snipes)

    # next_fire holds the *next* due time; one overdue by more than `idle` is no
    # longer serviced by the reminder loop (reminder disabled or guild left).
    next_fire: Dict[str, float] = getattr(target, "next_fire", {})
    overdue = [key for key, due in next_fire.items() if now - float(due) > idle]
    for key in overdue:
        next_fire.pop(key, None)
    removed["next_fire"] = len(overdue)

    # Cached starboard webhooks for channels the bot can no longer see.
    webhooks: Dict[int, Any] = getattr(target, "starboard_webhooks", {})
    is_ready = getattr(target, "is_ready", None)
    if callable(is_ready) and is_ready():
        gone = [cid for cid in webhooks if target.get_channel(cid) is None]
        for cid in gone:
            webhooks.pop(cid, None)
        removed["starboard_webhooks"] = len(gone)

    for name, mapping in (extra_stamped or {}).items():
        removed[name] = _prune_stamped(mapping, now, idle)
    if monotonic_now is not None:
        for name, mapping in (monotonic_stamped or {}).items():
            removed[name] = _prune_stamped(mapping, monotonic_now, idle)
    return removed


@tasks.loop(minutes=10)
async def state_janitor_loop() -> None:
    removed = prune_idle_state(
        bot,
        now=time.time(),
        today=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        extra_stamped={"modmail_last_open": _MODMAIL_LAST_OPEN},
        monotonic_now=time.monotonic(),
        monotonic_stamped={"command_cooldowns": _COOLDOWN_STATE},
    )
    music = bot.get_cog("Music")
    prune_players = getattr(music, "prune_idle_players", None)
    if callable(prune_players):
        removed["music_players"] = await prune_players(STATE_IDLE_SECONDS)
    total = sum(removed.values())
    if total:
        log.info(
            "State janitor removed %d idle entries (%s).",
            total,
            ", ".join(f"{k}={v}" for k, v in sorted(removed.items()) if v),
        )


@state_janitor_loop.before_loop
async def before_state_janitor_loop() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=30)
async def memory_report_loop() -> None:
    log.info("Memory: %s", _format_memory_line(collect_runtime_stats(bot)))


@memory_report_loop.before_loop
async def before_memory_report_loop() -> None:
    await bot.wait_until_ready()


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)
    stray: List[str] = _uncategorised_roots()
    if stray:
        log.warning(
            "%d command(s) missing from COMMAND_CATEGORY and shown as uncategorised: %s",
            len(stray),
            ", ".join(stray),
        )


@bot.event
async def on_guild_remove(guild: discord.Guild) -> None:
    bot.settings._cache.pop(guild.id, None)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message) -> None:
    if before.guild is None or before.author.bot:
        return
    if before.content == after.content:
        return
    try:
        await handle_counting_retraction(before, after, "edited")
    except Exception as exc:
        log.error("on_message_edit counting error: %s", exc)


@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if message.guild is None or message.author.bot:
        return
    try:
        bot.snipes[message.channel.id] = SnipedMessage(
            author=str(message.author),
            author_avatar=(
                message.author.display_avatar.url
                if message.author.display_avatar
                else None
            ),
            content=message.content or "",
            created_at=message.created_at,
            deleted_at=datetime.now(timezone.utc),
        )
    except discord.DiscordException as exc:
        log.error("on_message_delete error: %s", exc)

    try:
        await handle_counting_retraction(message, message, "deleted")
    except Exception as exc:
        log.error("on_message_delete counting error: %s", exc)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild is None:
        return

    try:
        if not message.author.bot:
            await _handle_afk_return(message)
            await _handle_afk_mentions(message)

        for attachment in _attachments_to_cache(message):
            bot.spawn(
                _cache_single_attachment(message.id, attachment),
                name=f"attachment-cache:{message.id}",
            )

        settings: Dict[str, Any] = bot.settings.peek_settings(message.guild.id)
        autopurge: Dict[str, Any] = settings.get("autopurge") or {
            "channels": {},
            "exempt_roles": [],
            "users": {},
        }
        entry: Optional[Dict[str, Any]] = (autopurge.get("channels") or {}).get(
            str(message.channel.id)
        )

        if entry and message.author.id != bot.user.id:
            until: Optional[float] = entry.get("until")
            if until and time.time() > until:
                channels: Dict[str, Any] = dict(autopurge.get("channels") or {})
                channels.pop(str(message.channel.id), None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.channels": channels}
                )
            elif not any(
                role.id in (autopurge.get("exempt_roles") or [])
                for role in getattr(message.author, "roles", [])
            ):
                try:
                    await message.delete()
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge", exc, guild=message.guild, user=message.author
                    )
                return

        msg_entries = autopurge.get("messages") or {}
        for key, m_entry in list(msg_entries.items()):
            if message.author.id == bot.user.id:
                break

            until: Optional[float] = m_entry.get("until")
            if until and time.time() > until:
                msgs_dict = dict(autopurge.get("messages") or {})
                msgs_dict.pop(key, None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.messages": msgs_dict}
                )
                continue

            txt = m_entry.get("text", "")
            mtype = m_entry.get("match_type", "contains")
            content_lower = _extract_message_text(message)
            needle = txt.casefold()

            is_match = False
            if mtype == "starts_with" and content_lower.startswith(needle):
                is_match = True
            elif mtype == "ends_with" and content_lower.endswith(needle):
                is_match = True
            elif mtype == "contains" and needle in content_lower:
                is_match = True

            if is_match:
                try:
                    await message.delete()
                    return  # Stop processing this message since it's deleted
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge_msg", exc, guild=message.guild, user=message.author
                    )

        user_entry = (autopurge.get("users") or {}).get(str(message.author.id))
        if user_entry and message.author.id != bot.user.id:
            until: Optional[float] = user_entry.get("until")
            if until and time.time() > until:
                users_dict = dict(autopurge.get("users") or {})
                users_dict.pop(str(message.author.id), None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.users": users_dict}
                )
            elif str(message.channel.id) not in (
                user_entry.get("exempt_channels") or []
            ):
                try:
                    await message.delete()
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge_user", exc, guild=message.guild, user=message.author
                    )
                return

        if message.author.bot:
            return

        if await _handle_automod(message):
            return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        bot.log_error("on_message:gate", exc, guild=message.guild, user=message.author)

    # Commands dispatch before any long-running enrichment (AI calls take seconds).
    await bot.process_commands(message)

    async def _enrich() -> None:
        try:
            await _apply_guild_automations(message)
            await _handle_sticky(message)
            await _handle_ai(message)
            await handle_counting(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.log_error(
                "on_message:enrich", exc, guild=message.guild, user=message.author
            )

    bot.spawn(_enrich(), name=f"enrich:{message.id}")


@bot.hybrid_command(
    name="ping", description="Check that the bot is alive and see its latency"
)
async def ping_cmd(ctx: commands.Context):
    await ctx.send(f"🏓 Pong! Latency: {round(bot.latency * 1000)}ms")


@bot.hybrid_command(name="uptime", description="How long the bot has been running")
async def uptime_cmd(ctx: commands.Context):
    s = datetime.now(timezone.utc).timestamp() - bot.start_time
    await ctx.send(f"⏱️ Uptime: {humanize_seconds(s)}")


@bot.hybrid_command(
    name="userinfo",
    description="Account age, join date, roles and key permissions for a member",
)
async def userinfo_cmd(ctx: commands.Context, user: Optional[discord.Member] = None):
    user = user or ctx.author
    embed = discord.Embed(title=str(user), color=user.color)
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="ID", value=user.id, inline=False)
    embed.add_field(name="Created", value=f"<t:{int(user.created_at.timestamp())}:R>")
    embed.add_field(name="Joined", value=f"<t:{int(user.joined_at.timestamp())}:R>")
    await ctx.send(embed=embed)


@bot.hybrid_command(
    name="serverinfo",
    description="Member counts, channels, roles, boosts and creation date",
)
@commands.guild_only()
async def serverinfo_cmd(ctx: commands.Context):
    g = ctx.guild
    embed = discord.Embed(title=g.name, color=discord.Color.blurple())
    if g.icon:
        embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name="ID", value=g.id, inline=False)
    embed.add_field(name="Owner", value=f"<@{g.owner_id}>")
    embed.add_field(name="Members", value=str(g.member_count))
    embed.add_field(name="Created", value=f"<t:{int(g.created_at.timestamp())}:R>")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="avatar", description="Show a user's avatar")
async def avatar_cmd(ctx: commands.Context, user: Optional[discord.User] = None):
    user = user or ctx.author
    embed = discord.Embed(
        title=f"{user.display_name}'s avatar", color=discord.Color.blurple()
    )
    embed.set_image(url=user.display_avatar.url)
    await ctx.send(embed=embed)


@bot.hybrid_command(name="banner", description="Show a user's profile banner")
async def banner_cmd(ctx: commands.Context, user: Optional[discord.User] = None):
    user = user or ctx.author
    fetched = await bot.fetch_user(user.id)
    if not fetched.banner:
        return await ctx.send(f"**{user.display_name}** has no banner.", ephemeral=True)
    embed = discord.Embed(
        title=f"{user.display_name}'s banner", color=discord.Color.blurple()
    )
    embed.set_image(url=fetched.banner.url)
    await ctx.send(embed=embed)


@bot.hybrid_command(
    name="roleinfo",
    description="Colour, position, member count and permissions for a role",
)
@commands.guild_only()
async def roleinfo_cmd(ctx: commands.Context, role: discord.Role):
    embed = discord.Embed(title=f"@{role.name}", color=role.color)
    embed.add_field(name="ID", value=role.id, inline=False)
    embed.add_field(name="Members", value=str(len(role.members)))
    embed.add_field(name="Color", value=str(role.color))
    embed.add_field(name="Created", value=f"<t:{int(role.created_at.timestamp())}:R>")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="membercount", description="How many members this server has")
@commands.guild_only()
async def membercount_cmd(ctx: commands.Context):
    await ctx.send(f"👥 **{ctx.guild.member_count}** members.")


# --------------------------------------------------------------------------- #
# Personal reminders
# --------------------------------------------------------------------------- #


@bot.hybrid_group(
    name="remindme",
    description="Personal reminders delivered by DM",
    fallback="add",
)
@commands.guild_only()
@app_commands.describe(
    when="How long from now - 10m, 2h30m, 3d, 1w",
    text="What you want to be reminded about",
)
async def remindme_group(ctx: commands.Context, when: str, *, text: str) -> None:
    seconds: Optional[int] = parse_duration(when)
    if seconds is None:
        return await ctx.send(
            "\u274c I couldn't read that duration. Try `10m`, `2h30m`, `3d` or `1w`.",
            ephemeral=True,
        )
    if seconds < 30:
        return await ctx.send(
            "\u274c Minimum reminder delay is 30 seconds.", ephemeral=True
        )

    clean: str = discord.utils.escape_mentions(text.strip())[:500]
    if not clean:
        return await ctx.send(
            "\u274c Give me something to remind you about.", ephemeral=True
        )

    try:
        existing: int = await asyncio.to_thread(
            bot.settings.reminders.count_documents,
            {"user_id": str(ctx.author.id), "delivered": False},
        )
    except PyMongoError as exc:
        bot.log_error("remindme:count", exc, guild=ctx.guild, user=ctx.author)
        return await ctx.send(
            "\u26a0\ufe0f The reminder store is unreachable right now.", ephemeral=True
        )

    if existing >= 25 and not is_superuser(ctx.author):
        return await ctx.send(
            "\u274c You already have 25 pending reminders. Clear some with `/remindme list`.",
            ephemeral=True,
        )

    due_at: float = time.time() + seconds
    document: Dict[str, Any] = {
        "user_id": str(ctx.author.id),
        "guild_id": str(ctx.guild.id),
        "channel_id": str(ctx.channel.id),
        "text": clean,
        "due_at": due_at,
        "created_at": time.time(),
        "jump_url": ctx.message.jump_url if ctx.message is not None else None,
        "delivered": False,
    }
    try:
        await asyncio.to_thread(bot.settings.reminders.insert_one, document)
    except PyMongoError as exc:
        bot.log_error("remindme:insert", exc, guild=ctx.guild, user=ctx.author)
        return await ctx.send(
            "\u26a0\ufe0f I couldn't save that reminder.", ephemeral=True
        )

    log.info("Reminder scheduled for %s in %ds.", ctx.author, seconds)
    await ctx.send(
        f"\u23f0 I'll remind you <t:{int(due_at)}:R> - {clean[:120]}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@remindme_group.command(name="list", description="Show your pending reminders")
@commands.guild_only()
async def remindme_list(ctx: commands.Context) -> None:
    try:
        docs: List[Dict[str, Any]] = await asyncio.to_thread(
            lambda: list(
                bot.settings.reminders.find(
                    {"user_id": str(ctx.author.id), "delivered": False}
                )
                .sort("due_at", 1)
                .limit(25)
            )
        )
    except PyMongoError as exc:
        bot.log_error("remindme:list", exc, guild=ctx.guild, user=ctx.author)
        return await ctx.send(
            "\u26a0\ufe0f The reminder store is unreachable right now.", ephemeral=True
        )

    if not docs:
        return await ctx.send("\u2705 You have no pending reminders.", ephemeral=True)

    lines: List[str] = [
        f"`{str(doc['_id'])[-6:]}` <t:{int(doc['due_at'])}:R> - {str(doc['text'])[:120]}"
        for doc in docs
    ]
    pages = build_pages("Your reminders", lines, discord.Color.blurple(), per_page=8)
    await send_pages(ctx, pages, ephemeral=True)


@remindme_group.command(name="cancel", description="Cancel a pending reminder")
@commands.guild_only()
@app_commands.describe(reminder_id="The short id shown by /remindme list")
async def remindme_cancel(ctx: commands.Context, reminder_id: str) -> None:
    needle: str = reminder_id.strip().lower()
    if not needle:
        return await ctx.send("\u274c Give me a reminder id.", ephemeral=True)
    try:
        docs: List[Dict[str, Any]] = await asyncio.to_thread(
            lambda: list(
                bot.settings.reminders.find(
                    {"user_id": str(ctx.author.id), "delivered": False}
                )
            )
        )
        match: Optional[Dict[str, Any]] = next(
            (doc for doc in docs if str(doc["_id"]).lower().endswith(needle)), None
        )
        if match is None:
            return await ctx.send(
                f"\u274c No pending reminder `{needle[:12]}`.", ephemeral=True
            )
        await asyncio.to_thread(
            bot.settings.reminders.delete_one, {"_id": match["_id"]}
        )
    except PyMongoError as exc:
        bot.log_error("remindme:cancel", exc, guild=ctx.guild, user=ctx.author)
        return await ctx.send(
            "\u26a0\ufe0f I couldn't cancel that reminder.", ephemeral=True
        )

    await ctx.send(
        f"\U0001f5d1\ufe0f Cancelled - {str(match['text'])[:120]}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@tasks.loop(seconds=30.0)
async def personal_reminder_loop() -> None:
    now: float = time.time()
    try:
        due: List[Dict[str, Any]] = await asyncio.to_thread(
            lambda: list(
                bot.settings.reminders.find(
                    {"delivered": False, "due_at": {"$lte": now}}
                ).limit(50)
            )
        )
    except PyMongoError as exc:
        bot.log_error("remindme:sweep", exc)
        return

    for doc in due:
        user: Optional[discord.abc.User] = bot.get_user(int(doc["user_id"]))
        if user is None:
            try:
                user = await bot.fetch_user(int(doc["user_id"]))
            except discord.DiscordException:
                user = None

        embed: discord.Embed = discord.Embed(
            title="\u23f0 Reminder",
            description=str(doc["text"])[:2000],
            color=discord.Color.blurple(),
            timestamp=datetime.fromtimestamp(float(doc["created_at"]), tz=timezone.utc),
        )
        if doc.get("jump_url"):
            embed.add_field(
                name="Context", value=f"[original message]({doc['jump_url']})"
            )

        delivered: bool = False
        if user is not None:
            try:
                await user.send(embed=embed)
                delivered = True
            except (discord.Forbidden, discord.HTTPException):
                delivered = False

        if not delivered and doc.get("channel_id"):
            channel = bot.get_channel(int(doc["channel_id"]))
            if isinstance(channel, discord.abc.Messageable):
                try:
                    await channel.send(f"<@{doc['user_id']}>", embed=embed)
                    delivered = True
                except discord.DiscordException as exc:
                    bot.log_error("remindme:deliver", exc)

        try:
            await asyncio.to_thread(
                bot.settings.reminders.update_one,
                {"_id": doc["_id"]},
                {"$set": {"delivered": True, "delivered_at": time.time()}},
            )
        except PyMongoError as exc:
            bot.log_error("remindme:ack", exc)


@personal_reminder_loop.before_loop
async def before_personal_reminder_loop() -> None:
    await bot.wait_until_ready()


# --------------------------------------------------------------------------- #
# Operational self-check
# --------------------------------------------------------------------------- #

REQUIRED_GUILD_PERMISSIONS: Tuple[str, ...] = (
    "manage_roles",
    "manage_messages",
    "kick_members",
    "ban_members",
    "moderate_members",
    "manage_channels",
    "embed_links",
    "attach_files",
    "read_message_history",
    "add_reactions",
)


@bot.hybrid_command(
    name="diagnose", description="Run a health check on the bot (admin)"
)
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
@custom_has_permissions(administrator=True)
async def diagnose_cmd(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send(
            "\u274c You need Administrator permission.", ephemeral=True
        )
    await ctx.defer(ephemeral=True)

    started: float = time.perf_counter()
    db_ok: bool = await bot.settings.ping()
    db_ms: float = (time.perf_counter() - started) * 1000
    guild_settings: Dict[str, Any] = await bot.settings.fetch_settings(ctx.guild.id)

    embed: discord.Embed = build_diagnose_embed(
        bot, ctx.guild, db_ok=db_ok, db_ms=db_ms, guild_settings=guild_settings
    )
    await ctx.send(embed=embed, ephemeral=True)


DIAGNOSE_LOOPS: Dict[str, Any] = {}  # filled below once every loop exists


def _memory_limit_mb() -> Optional[float]:
    """The instance's memory limit, for "% used": MEMORY_LIMIT_MB, else 512 on Render."""
    raw: str = os.getenv("MEMORY_LIMIT_MB", "").strip()
    if raw:
        try:
            return float(raw) if float(raw) > 0 else None
        except ValueError:
            return None
    return 512.0 if os.getenv("RENDER") else None


def _fmt_ms(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value:.0f} ms"


def _diagnose_music_lines(target: Any) -> List[str]:
    music: Any = target.get_cog("Music") if hasattr(target, "get_cog") else None
    lines: List[str] = [
        f"Voice connections **{len(getattr(target, 'voice_clients', None) or [])}**"
    ]
    if music is None:
        lines.append("\u26a0\ufe0f Music module not loaded")
        return lines
    lines.append(f"Players **{len(getattr(music, 'players', {}) or {})}**")
    try:
        from utils import warp, ytdl

        route: Dict[str, Any] = ytdl.route_status()
        warp_state: Dict[str, Any] = warp.status()
        lines.append(f"YouTube route **{route['route']}**")
        if route.get("preferred"):
            lines.append(f"Clients `{route['preferred'][:60]}`")
        warp_text: str = "running" if warp_state["running"] else (
            "available" if warp_state["available"] else "unavailable"
        )
        lines.append(f"WARP **{warp_state['mode']}** \u00b7 {warp_text}")
    except Exception as exc:  # diagnostics must never fail the command
        lines.append(f"Route unknown ({type(exc).__name__})")
    return lines


def _diagnose_feature_lines(settings: Dict[str, Any]) -> List[str]:
    def on(flag: Any) -> str:
        return "\U0001f7e2" if flag else "\u26aa"

    counting: Dict[str, Any] = settings.get("counting") or {}
    ai: Dict[str, Any] = settings.get("ai") or {}
    messagelog: Dict[str, Any] = settings.get("messagelog") or {}
    modlog: Dict[str, Any] = settings.get("modlog") or {}
    automod: Dict[str, Any] = settings.get("automod") or {}
    automod_on: List[str] = [
        key for key in ("invites", "links", "spam", "caps", "mentions") if automod.get(key)
    ]
    return [
        f"{on(counting.get('channel_id'))} Counting"
        + (f" in <#{counting['channel_id']}>" if counting.get("channel_id") else ""),
        f"{on(ai.get('enabled'))} AI chat ({len(ai.get('channels') or [])} channels)",
        f"{on(messagelog.get('enabled') and messagelog.get('channel_id'))} Message log"
        + (" + attachment cache" if messagelog.get("cache_attachments") else ""),
        f"{on(modlog.get('enabled') and modlog.get('channel_id'))} Mod log",
        f"{on(automod_on)} Automod" + (f" ({', '.join(automod_on)})" if automod_on else ""),
        f"{on((settings.get('raid') or {}).get('enabled'))} Raid protection",
        f"{on((settings.get('modmail') or {}).get('enabled'))} Tickets",
    ]


def build_diagnose_embed(
    target: Any,
    guild: discord.Guild,
    *,
    db_ok: bool,
    db_ms: float,
    guild_settings: Optional[Dict[str, Any]] = None,
) -> discord.Embed:
    """The /diagnose report. Pure apart from reading in-memory state."""
    stats: Dict[str, Any] = collect_runtime_stats(target)
    lag: Dict[str, Any] = stats.get("loop_lag") or {}
    cache: Dict[str, int] = target.settings.cache_stats()
    lookups: int = cache["hits"] + cache["misses"]
    hit_rate: float = (cache["hits"] / lookups * 100) if lookups else 100.0
    errors: Dict[str, Any] = target.errors.stats()
    uptime: float = time.time() - target.start_time
    latency: float = target.latency
    latency_text: str = (
        f"{latency * 1000:.0f} ms" if math.isfinite(latency) else "not connected"
    )

    me: Any = guild.me
    missing: List[str] = [
        permission.replace("_", " ")
        for permission in REQUIRED_GUILD_PERMISSIONS
        if not getattr(me.guild_permissions, permission, False)
    ]

    loop_lines: List[str] = []
    for name, loop_obj in DIAGNOSE_LOOPS.items():
        if loop_obj.is_running():
            failed: bool = bool(loop_obj.failed())
            mark: str = "\u26a0\ufe0f" if failed else "\u2705"
            loop_lines.append(f"{mark} {name}{' (failed)' if failed else ''}")
        else:
            loop_lines.append(f"\u274c {name} (stopped)")
    lag_monitor: Any = getattr(target, "loop_lag", None)
    if isinstance(lag_monitor, LoopLagMonitor):
        loop_lines.append(
            ("\u2705" if lag_monitor.running else "\u274c") + " loop-lag monitor"
        )

    rss: Optional[float] = stats.get("rss_mb")
    limit: Optional[float] = _memory_limit_mb()
    memory_share: Optional[float] = (rss / limit * 100) if (rss and limit) else None
    attachment: Dict[str, Any] = stats.get("attachment_cache") or {}
    lag_max: Optional[float] = lag.get("max_ms")

    warnings: List[str] = []
    if not db_ok:
        warnings.append("database unreachable")
    if missing:
        warnings.append("missing permissions")
    if memory_share is not None and memory_share >= 85:
        warnings.append(f"memory at {memory_share:.0f}% of the limit")
    if lag_max is not None and lag_max >= 1000:
        warnings.append("event loop was blocked for over a second")
    if any("\u274c" in line or "failed" in line for line in loop_lines):
        warnings.append("a background loop is down")

    embed: discord.Embed = discord.Embed(
        title="\U0001fa7a Bot diagnostics",
        description=(
            "\u2705 Everything looks healthy."
            if not warnings
            else "\u26a0\ufe0f " + "; ".join(warnings).capitalize() + "."
        ),
        color=(
            discord.Color.red()
            if not db_ok
            else discord.Color.orange() if warnings else discord.Color.green()
        ),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Gateway",
        value=(
            f"Latency **{latency_text}**\n"
            f"Uptime **{humanize_seconds(uptime)}**\n"
            f"Guilds **{stats['guilds']}** \u00b7 Shards **{target.shard_count or 1}**\n"
            f"Cached users **{len(getattr(target, 'users', None) or [])}**"
        ),
        inline=True,
    )
    db_state: str = "\u2705 reachable" if db_ok else "\u274c unreachable"
    embed.add_field(
        name="Database",
        value=(
            f"{db_state}\n"
            f"Ping **{db_ms:.0f} ms**\n"
            f"Cache **{cache['entries']}** guilds - **{hit_rate:.1f}%** hits"
        ),
        inline=True,
    )
    embed.add_field(
        name="Errors",
        value=(
            f"Unique **{errors['unique']}**\n"
            f"Total **{errors['total']}**\n"
            + (
                f"Top `{errors['by_type'][0][0]}`"
                if errors["by_type"]
                else "None recorded"
            )
        ),
        inline=True,
    )
    memory_lines: List[str] = [
        f"RSS **{rss if rss is not None else 'n/a'} MB**"
        + (f" ({memory_share:.0f}% of {limit:.0f} MB)" if memory_share is not None else "")
        + f" \u00b7 peak {stats.get('peak_rss_mb') or 'n/a'} MB",
        f"Attachment cache **{attachment.get('bytes', 0) / 1048576:.1f}"
        f"/{attachment.get('max_bytes', 0) / 1048576:.0f} MB** "
        f"({attachment.get('entries', 0)} msgs, {attachment.get('evictions', 0)} evicted)",
        f"Settings cache **{stats['settings_cache_entries']}** guilds",
        f"AI buffers **{stats['ai_buffer_messages']}** msgs in "
        f"{stats['ai_buffer_channels']} channels \u00b7 {stats['ai_pending_messages']} unsaved",
        f"Snipes **{stats['snipes']}**",
    ]
    embed.add_field(name="Memory", value="\n".join(memory_lines), inline=False)
    runtime_lines: List[str] = [
        f"Python **{_sys.version.split()[0]}** \u00b7 discord.py **{discord.__version__}**"
        f" \u00b7 aiohttp **{aiohttp.__version__}**",
        f"Event-loop lag **{_fmt_ms(lag.get('last_ms'))}** now \u00b7 "
        f"p50 {_fmt_ms(lag.get('p50_ms'))} \u00b7 max {_fmt_ms(lag_max)} (5 min)",
        f"Threads **{stats['threads']}** \u00b7 asyncio tasks **{stats['asyncio_tasks']}**"
        f" \u00b7 background **{stats['background_tasks']}**",
    ]
    version: Optional[str] = _deploy_version()
    if version:
        runtime_lines.append(f"Deployed commit `{version}`")
    embed.add_field(name="Runtime", value="\n".join(runtime_lines), inline=False)
    embed.add_field(
        name="Music", value="\n".join(_diagnose_music_lines(target))[:1024], inline=True
    )
    embed.add_field(
        name="Background loops", value="\n".join(loop_lines)[:1024] or "none", inline=True
    )
    if guild_settings is not None:
        embed.add_field(
            name="This server",
            value="\n".join(_diagnose_feature_lines(guild_settings))[:1024],
            inline=False,
        )
    embed.add_field(
        name="Permissions in this server",
        value=(
            "\u2705 All required permissions granted"
            if not missing
            else "\u274c Missing: " + ", ".join(f"`{p}`" for p in missing)
        ),
        inline=False,
    )
    embed.set_footer(text="Run /errors detail <id> for tracebacks \u00b7 GET /health for JSON")
    return embed


DIAGNOSE_LOOPS.update(
    {
        "reminders": reminder_loop,
        "ai flush": ai_flush_loop,
        "tempbans": tempban_loop,
        "personal reminders": personal_reminder_loop,
        "state janitor": state_janitor_loop,
        "memory report": memory_report_loop,
    }
)


# --------------------------------------------------------------------------- #
# Effective permission resolution
# --------------------------------------------------------------------------- #

PERMCHECK_KEYS: Tuple[str, ...] = (
    "view_channel",
    "send_messages",
    "send_messages_in_threads",
    "embed_links",
    "attach_files",
    "add_reactions",
    "read_message_history",
    "manage_messages",
    "mention_everyone",
    "use_application_commands",
    "connect",
    "speak",
)


@bot.hybrid_command(
    name="permcheck",
    description="Show a member's effective permissions in a channel",
)
@commands.guild_only()
@app_commands.describe(
    member="Whose permissions to resolve (defaults to you)",
    channel="Which channel to resolve against (defaults to here)",
)
async def permcheck_cmd(
    ctx: commands.Context,
    member: Optional[discord.Member] = None,
    channel: Optional[Union[discord.TextChannel, discord.VoiceChannel]] = None,
) -> None:
    target: discord.Member = member or ctx.author
    scope: Any = channel or ctx.channel

    if target.id != ctx.author.id and not member_has_perms(
        ctx.author, manage_roles=True
    ):
        return await ctx.send(
            "\u274c You need **Manage Roles** to inspect another member.",
            ephemeral=True,
        )
    if not isinstance(
        scope, (discord.TextChannel, discord.VoiceChannel, discord.Thread)
    ):
        return await ctx.send("\u274c Pick a text or voice channel.", ephemeral=True)

    resolved: discord.Permissions = scope.permissions_for(target)
    granted: List[str] = []
    denied: List[str] = []
    for key in PERMCHECK_KEYS:
        (granted if getattr(resolved, key, False) else denied).append(
            key.replace("_", " ")
        )

    overwrite_notes: List[str] = []
    overwrites: Dict[Any, discord.PermissionOverwrite] = (
        getattr(scope, "overwrites", {}) or {}
    )
    for holder, overwrite in overwrites.items():
        applies: bool = holder == target or (
            isinstance(holder, discord.Role) and holder in target.roles
        )
        if not applies:
            continue
        allow, deny = overwrite.pair()
        pieces: List[str] = []
        if allow.value:
            pieces.append("+" + ", ".join(name for name, value in allow if value)[:120])
        if deny.value:
            pieces.append("-" + ", ".join(name for name, value in deny if value)[:120])
        if pieces:
            label: str = (
                holder.mention
                if isinstance(holder, discord.Role)
                else "member override"
            )
            overwrite_notes.append(f"{label} {' '.join(pieces)}")

    embed: discord.Embed = discord.Embed(
        title=f"Permissions - {target.display_name}",
        description=f"Resolved in {scope.mention}",
        color=discord.Color.green() if resolved.send_messages else discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(
        name="\u2705 Granted",
        value=", ".join(f"`{name}`" for name in granted) or "none",
        inline=False,
    )
    embed.add_field(
        name="\u274c Denied",
        value=", ".join(f"`{name}`" for name in denied) or "none",
        inline=False,
    )
    embed.add_field(
        name="Top role",
        value=f"{target.top_role.mention} (position {target.top_role.position})",
        inline=True,
    )
    embed.add_field(
        name="Administrator",
        value="yes - overrides everything" if resolved.administrator else "no",
        inline=True,
    )
    if overwrite_notes:
        embed.add_field(
            name="Applicable overwrites",
            value="\n".join(overwrite_notes[:6])[:1024],
            inline=False,
        )
    await ctx.send(
        embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
    )


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
STARTUP_LOOPS.append(personal_reminder_loop)
STARTUP_LOOPS.append(state_janitor_loop)
STARTUP_LOOPS.append(memory_report_loop)


if __name__ == "__main__":
    run()
