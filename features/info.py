"""Informational commands: ping, uptime, user/server/role info, diagnose, permcheck."""

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
from discord.ext import commands

from utils.runtime_stats import LoopLagMonitor
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot
from core.helpers import humanize_seconds
from core.stats import collect_runtime_stats
from core.web import _deploy_version
from features.reminders import reminder_loop
from features.ai import ai_flush_loop
from features.moderation import tempban_loop
from features.remindme import personal_reminder_loop
from features.maintenance import memory_report_loop, state_janitor_loop

log: logging.Logger = logging.getLogger("debt-collector")


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
