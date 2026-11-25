"""Mod-log posting, case records and the after-invoke mod-log hook."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import commands
from pymongo.errors import PyMongoError

from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


async def send_modlog(
    guild: discord.Guild,
    title: str,
    description: str,
    color: Optional[discord.Color] = None,
    fields: Optional[List[Tuple[str, str]]] = None,
) -> None:
    settings = bot.settings.peek_settings(guild.id)
    channel_id = (settings.get("modlog") or {}).get("channel_id")
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if channel is None:
        return
    embed = discord.Embed(
        title=title,
        description=description[:4000],
        color=color if color is not None else discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    for name, value in fields or []:
        embed.add_field(name=name, value=value[:1024], inline=True)
    try:
        await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except discord.DiscordException as exc:
        bot.log_error("modlog", exc)


async def record_case(
    guild: discord.Guild,
    action: str,
    actor: discord.abc.User,
    target: Optional[discord.abc.User],
    reason: str,
) -> Optional[int]:
    """Append a numbered entry to the guild's case book."""
    try:
        counter = await asyncio.to_thread(
            bot.settings.meta.find_one_and_update,
            {"key": f"cases:{guild.id}"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=True,
        )
        case_id = int((counter or {}).get("value", 1))
        await asyncio.to_thread(
            bot.settings.cases.insert_one,
            {
                "guildid": str(guild.id),
                "case_id": case_id,
                "action": action,
                "actor_id": str(actor.id),
                "actor": str(actor),
                "target_id": str(target.id) if target else None,
                "target": str(target) if target else None,
                "reason": reason[:500],
                "at": datetime.now(timezone.utc),
            },
        )
        return case_id
    except PyMongoError as exc:
        bot.log_error("cases", exc)
        return None


MODLOG_ACTIONS: Dict[str, Tuple[str, discord.Color]] = {
    "ban": ("🔨 Ban", discord.Color.red()),
    "tempban": ("⏳ Temporary ban", discord.Color.red()),
    "unban": ("♻️ Unban", discord.Color.green()),
    "kick": ("👢 Kick", discord.Color.orange()),
    "timeout": ("🔇 Timeout", discord.Color.orange()),
    "untimeout": ("🔊 Timeout removed", discord.Color.green()),
    "warn": ("⚠️ Warn", discord.Color.gold()),
    "clearwarns": ("🧽 Warnings cleared", discord.Color.green()),
    "purge": ("🧹 Purge", discord.Color.blurple()),
    "purge user": ("🧹 Purge (user)", discord.Color.blurple()),
    "purge contains": ("🧹 Purge (text)", discord.Color.blurple()),
    "purge bots": ("🧹 Purge (bots)", discord.Color.blurple()),
    "purge links": ("🧹 Purge (links)", discord.Color.blurple()),
    "lock": ("🔒 Channel locked", discord.Color.dark_orange()),
    "unlock": ("🔓 Channel unlocked", discord.Color.green()),
    "slowmode": ("🐌 Slowmode", discord.Color.blurple()),
    "nickname": ("✏️ Nickname changed", discord.Color.blurple()),
    "role": ("🎭 Role changed", discord.Color.blurple()),
    "roleall": ("🎭 Mass role", discord.Color.blurple()),
}


async def _modlog_after_invoke(ctx: commands.Context) -> None:
    """One funnel: every successful moderation command lands in the mod log."""
    if ctx.guild is None or ctx.command is None or ctx.command_failed:
        return
    name = ctx.command.qualified_name
    entry = MODLOG_ACTIONS.get(name)
    if entry is None:
        return
    title, color = entry

    target = ctx.kwargs.get("user") or ctx.kwargs.get("member")
    reason = str(ctx.kwargs.get("reason") or "No reason given")
    details: List[Tuple[str, str]] = [
        ("Moderator", f"{ctx.author.mention} (`{ctx.author}`)")
    ]
    if target is not None:
        details.append(("Target", f"{getattr(target, 'mention', target)} (`{target}`)"))
    for key in (
        "amount",
        "minutes",
        "seconds",
        "role",
        "name",
        "text",
        "duration",
        "action",
    ):
        if key in ctx.kwargs and ctx.kwargs[key] is not None:
            details.append((key.capitalize(), str(ctx.kwargs[key])[:200]))
    details.append(
        ("Channel", ctx.channel.mention if hasattr(ctx.channel, "mention") else "—")
    )

    case_id = None
    if name in ("ban", "tempban", "unban", "kick", "timeout", "untimeout", "warn"):
        case_id = await record_case(ctx.guild, name, ctx.author, target, reason)

    await send_modlog(
        ctx.guild,
        f"{title}{f' · case #{case_id}' if case_id else ''}",
        f"**Reason:** {discord.utils.escape_mentions(reason)[:500]}",
        color,
        details[:6],
    )


bot.after_invoke(_modlog_after_invoke)


async def post_modlog(guild: discord.Guild, embed: discord.Embed) -> None:
    settings = bot.settings.peek_settings(guild.id)
    modlog = settings.get("modlog") or {}

    if not modlog.get("enabled"):
        return

    channel_id_str = modlog.get("channel_id")
    if not channel_id_str:
        return

    channel = guild.get_channel(int(channel_id_str))
    if not isinstance(channel, discord.TextChannel):
        return

    try:
        await channel.send(embed=embed)
    except discord.Forbidden:
        pass
    except discord.HTTPException as exc:
        bot.log_error("modlog:send", exc, guild=guild)
