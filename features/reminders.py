"""Recurring server reminders: `remind` group and reminder_loop."""

import logging
import time
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from core.permissions import member_has_perms
from core.registry import STARTUP_LOOPS
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Render keepalive (only when Render injects PORT, i.e. Web Service mode)
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Reminder Loop
# --------------------------------------------------------------------------- #


@tasks.loop(seconds=20)
async def reminder_loop():
    now = time.time()
    for guild in list(bot.guilds):
        cfg = bot.settings.peek_settings(guild.id).get("remind") or {}
        if not cfg.get("enabled") or not cfg.get("channel_id"):
            continue
        key = f"remind_{guild.id}"
        interval = max(60.0, float(cfg.get("interval", 181)) * 60.0)
        if key not in bot.next_fire:
            bot.next_fire[key] = now + interval
            continue
        if now < bot.next_fire[key]:
            continue
        bot.next_fire[key] = now + interval
        channel = guild.get_channel(int(cfg["channel_id"]))
        if channel is None:
            continue
        text = str(cfg.get("message", "Reminder!"))[:1800]
        role_id = cfg.get("role_id")
        try:
            await channel.send(
                f"<@&{role_id}> {text}" if role_id else text,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
        except Exception as e:
            bot.log_error("reminder", e)


@reminder_loop.before_loop
async def before_reminder_loop():
    await bot.wait_until_ready()


# --------------------------------------------------------------------------- #
# Recurring reminders (drives the existing remind settings block)
# --------------------------------------------------------------------------- #


@bot.hybrid_group(
    name="remind", description="Recurring server reminder", fallback="status"
)
@commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def remind_group(ctx: commands.Context):
    if ctx.invoked_subcommand is not None:
        return
    config = (await bot.settings.fetch_settings(ctx.guild.id)).get("remind") or {}
    if not config.get("enabled"):
        return await ctx.send(
            "🔕 No recurring reminder is set. Use `/remind set`.", ephemeral=True
        )
    next_at = bot.next_fire.get(f"remind_{ctx.guild.id}")
    await ctx.send(
        f"🔔 Every **{config.get('interval', 181)} min** in <#{config.get('channel_id')}>"
        + (f" for <@&{config.get('role_id')}>" if config.get("role_id") else "")
        + f"\n> {discord.utils.escape_mentions(str(config.get('message'))[:500])}"
        + (f"\nNext: <t:{int(next_at)}:R>" if next_at else ""),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@remind_group.command(name="set", description="Create or update the recurring reminder")
@app_commands.describe(
    interval="Minutes between reminders (1-10080)",
    message="What to say",
    channel="Where to post (default: this channel)",
    role="Optional role to ping",
)
async def remind_set(
    ctx: commands.Context,
    interval: app_commands.Range[int, 1, 10080],
    message: str,
    channel: Optional[discord.TextChannel] = None,
    role: Optional[discord.Role] = None,
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )
    target = channel or ctx.channel
    saved = await bot.settings.push_fields(
        ctx.guild.id,
        {
            "remind.enabled": True,
            "remind.interval": int(interval),
            "remind.channel_id": target.id,
            "remind.role_id": role.id if role else None,
            "remind.message": message[:1500],
        },
    )
    bot.next_fire[f"remind_{ctx.guild.id}"] = time.time() + interval * 60
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Reminder set for {target.mention} every **{interval} min**.",
        ephemeral=True,
    )


@remind_group.command(name="off", description="Stop the recurring reminder")
async def remind_off(ctx: commands.Context):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )
    saved = await bot.settings.push_fields(ctx.guild.id, {"remind.enabled": False})
    bot.next_fire.pop(f"remind_{ctx.guild.id}", None)
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Recurring reminder stopped.", ephemeral=True
    )


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
STARTUP_LOOPS.append(reminder_loop)
