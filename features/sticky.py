"""Sticky messages."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.permissions import member_has_perms
from core.bot import bot
from core.helpers import sanitize_mass_pings

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# AI Commands
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# AI subsystem — configuration, providers, memory, commands
# --------------------------------------------------------------------------- #

STICKY_MIN_INTERVAL: float = 6.0


async def _handle_sticky(message: discord.Message) -> None:
    """Re-post the channel's sticky message underneath new chatter."""
    assert message.guild is not None
    settings = await bot.settings.fetch_settings(message.guild.id)
    sticky: Dict[str, Any] = settings.get("sticky") or {}
    entry: Optional[Dict[str, Any]] = sticky.get(str(message.channel.id))
    if not entry or not entry.get("content"):
        return

    channel_id: int = message.channel.id
    now: float = time.time()
    if now - bot.sticky_last.get(channel_id, 0.0) < STICKY_MIN_INTERVAL:
        return

    lock: asyncio.Lock = bot.sticky_locks.setdefault(channel_id, asyncio.Lock())
    if lock.locked():
        return

    async with lock:
        bot.sticky_last[channel_id] = now
        previous_id = entry.get("last_id")
        if previous_id:
            try:
                previous = await message.channel.fetch_message(int(previous_id))
                await previous.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        embed = discord.Embed(description=str(entry["content"])[:4000], color=0x5865F2)
        embed.set_author(name="📌 Sticky")
        try:
            sent = await message.channel.send(
                embed=embed, allowed_mentions=discord.AllowedMentions.none()
            )
        except discord.HTTPException as exc:
            bot.log_error("sticky", exc)
            return

        record: Dict[str, Any] = dict(entry)
        record["last_id"] = sent.id
        updated: Dict[str, Any] = dict(settings.get("sticky") or {})
        updated[str(channel_id)] = record
        await bot.settings.push_settings(message.guild.id, {"sticky": updated})


@bot.hybrid_group(
    name="sticky",
    description="Keep a message pinned to the bottom of a channel",
    fallback="set",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(message="Text to keep at the bottom of this channel")
async def sticky_group(ctx: commands.Context, *, message: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    content: str = message.strip()
    if not content:
        return await ctx.send("❌ Give me some text to stick.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    sticky: Dict[str, Any] = dict(settings.get("sticky") or {})
    existing: Dict[str, Any] = sticky.get(str(ctx.channel.id)) or {}
    sticky[str(ctx.channel.id)] = {
        "content": sanitize_mass_pings(content)[:4000],
        "last_id": existing.get("last_id"),
        "by": ctx.author.id,
        "at": int(time.time()),
    }
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"sticky": sticky})
    bot.sticky_last.pop(ctx.channel.id, None)

    await ctx.send(
        f"{'✅' if saved else '⚠️'} Sticky set for {ctx.channel.mention} — it re-posts under new "
        f"messages, at most once every {STICKY_MIN_INTERVAL:.0f}s."
        + ("" if saved else " (database write failed)"),
        ephemeral=True,
    )


@sticky_group.command(name="off", description="Stop the sticky message in a channel")
@app_commands.describe(channel="Channel (default: this one)")
async def sticky_off(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    target = channel or ctx.channel
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    sticky: Dict[str, Any] = dict(settings.get("sticky") or {})
    entry: Optional[Dict[str, Any]] = sticky.pop(str(target.id), None)
    if entry is None:
        return await ctx.send(
            f"ℹ️ No sticky message is set in {target.mention}.", ephemeral=True
        )

    last_id = entry.get("last_id")
    if last_id:
        try:
            previous = await target.fetch_message(int(last_id))
            await previous.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    bot.sticky_last.pop(target.id, None)
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"sticky": sticky})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Sticky removed from {target.mention}."
        + ("" if saved else " (database write failed)"),
        ephemeral=True,
    )


@sticky_group.command(
    name="list", description="Show every channel with a sticky message"
)
async def sticky_list(ctx: commands.Context):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    sticky: Dict[str, Any] = settings.get("sticky") or {}
    if not sticky:
        return await ctx.send(
            "No sticky messages are set. Add one with `/sticky set <message>`.",
            ephemeral=True,
        )

    lines: List[str] = []
    for channel_id, entry in sticky.items():
        channel = (
            ctx.guild.get_channel(int(channel_id)) if channel_id.isdigit() else None
        )
        where: str = (
            channel.mention
            if channel is not None
            else f"*deleted channel* (`{channel_id}`)"
        )
        preview: str = str(entry.get("content", ""))[:120].replace("\n", " ")
        author_id = entry.get("by")
        byline: str = f" · by <@{author_id}>" if author_id else ""
        lines.append(f"{where}{byline}\n└ {preview}")

    pages = build_pages(
        "Sticky messages",
        lines,
        0x5865F2,
        per_page=8,
        footer=f"{len(sticky)} channel(s)",
    )
    await send_pages(ctx, pages, ephemeral=True)
