"""Personal reminders: `remindme` group and personal_reminder_loop."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from pymongo.errors import PyMongoError

from utils.pagination import build_pages, send_pages
from core.permissions import is_superuser
from core.registry import STARTUP_LOOPS
from core.bot import bot
from core.helpers import parse_duration

log: logging.Logger = logging.getLogger("debt-collector")


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


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
STARTUP_LOOPS.append(personal_reminder_loop)
