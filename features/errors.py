"""Error reporting: event/command/app-command error handlers and the `errors` group."""

import io
import logging
import random
import sys as _sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands
from pymongo.errors import PyMongoError

from utils.pagination import build_pages, send_pages
from core.config import COMMAND_PREFIX
from core.errors import ErrorRecord
from core.permissions import (
    NotStaff,
    custom_has_permissions,
    is_superuser,
    member_has_perms,
)
from core.bot import bot
from features.help import PEASANT_TAUNTS

log: logging.Logger = logging.getLogger("debt-collector")


@bot.event
async def on_error(event_method: str, *args: Any, **kwargs: Any) -> None:
    """Catches listener failures that would otherwise only reach stderr."""
    exc_value: Optional[BaseException] = _sys.exc_info()[1]
    guild: Optional[discord.Guild] = None
    for arg in args:
        candidate = getattr(arg, "guild", None)
        if isinstance(candidate, discord.Guild):
            guild = candidate
            break
    if exc_value is not None:
        bot.errors.record(f"event:{event_method}", exc_value, guild=guild)
    log.exception("Unhandled exception in event %s", event_method)


@bot.hybrid_group(
    name="errors",
    description="Inspect the bot's recent runtime errors",
    fallback="recent",
)
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
@custom_has_permissions(administrator=True)
@app_commands.describe(
    source="Filter by where the error came from, e.g. 'ai' or 'modlog'"
)
async def errors_group(ctx: commands.Context, source: Optional[str] = None) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    records: List[ErrorRecord] = bot.errors.recent(
        where=source, guild_id=None if is_superuser(ctx.author) else ctx.guild.id
    )
    if not records:
        scope: str = f" matching `{source}`" if source else ""
        return await ctx.send(f"✅ No errors recorded{scope}.", ephemeral=True)

    stats: Dict[str, Any] = bot.errors.stats()
    pages = build_pages(
        "Recent errors (newest first)",
        [record.summary() for record in records],
        0xE74C3C,
        per_page=5,
        footer=(
            f"{stats['unique']} unique - {stats['total']} total - "
            "/errors detail <id> for a traceback"
        ),
    )
    await send_pages(ctx, pages, ephemeral=True)


@errors_group.command(
    name="detail", description="Show the full traceback for one error id"
)
@custom_has_permissions(administrator=True)
@app_commands.describe(error_id="The short id shown by /errors, e.g. 3f9a1c2b")
async def errors_detail(ctx: commands.Context, error_id: str) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    record: Optional[ErrorRecord] = bot.errors.get(error_id)
    if record is None:
        return await ctx.send(f"❌ No error with id `{error_id[:12]}`.", ephemeral=True)
    if record.guild_id not in (None, ctx.guild.id) and not is_superuser(ctx.author):
        return await ctx.send(
            "❌ That error belongs to another server.", ephemeral=True
        )

    embed: discord.Embed = discord.Embed(
        title=f"Error {record.short_id()} - {record.exc_type}",
        description=f"```{discord.utils.escape_markdown(record.message)[:1000]}```",
        color=0xE74C3C,
        timestamp=datetime.fromtimestamp(record.last_seen, tz=timezone.utc),
    )
    embed.add_field(name="Source", value=f"`{record.where}`", inline=True)
    embed.add_field(name="Occurrences", value=str(record.count), inline=True)
    embed.add_field(
        name="First seen", value=f"<t:{int(record.first_seen)}:R>", inline=True
    )
    if record.command:
        embed.add_field(name="Command", value=f"`{record.command}`", inline=True)
    if record.user_name:
        embed.add_field(name="Invoker", value=f"`{record.user_name}`", inline=True)
    if record.channel_id:
        embed.add_field(name="Channel", value=f"<#{record.channel_id}>", inline=True)

    if record.stack:
        payload: bytes = record.stack.encode("utf-8")
        return await ctx.send(
            embed=embed,
            file=discord.File(
                io.BytesIO(payload), filename=f"trace-{record.short_id()}.txt"
            ),
            ephemeral=True,
        )
    await ctx.send(embed=embed, ephemeral=True)


@errors_group.command(
    name="stats", description="Error totals grouped by exception type"
)
@custom_has_permissions(administrator=True)
async def errors_stats(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    stats: Dict[str, Any] = bot.errors.stats()
    if not stats["by_type"]:
        return await ctx.send(
            "✅ No errors recorded since the last restart.", ephemeral=True
        )
    body: str = "\n".join(
        f"`{name}` - **{count}**" for name, count in stats["by_type"][:15]
    )
    embed: discord.Embed = discord.Embed(
        title="Error statistics", description=body, color=0xE74C3C
    )
    embed.set_footer(
        text=(
            f"{stats['unique']} unique - {stats['total']} total - "
            f"window {stats['uptime'] / 3600:.1f}h"
        )
    )
    await ctx.send(embed=embed, ephemeral=True)


@errors_group.command(name="clear", description="Empty the in-memory error buffer")
@custom_has_permissions(administrator=True)
async def errors_clear(ctx: commands.Context) -> None:
    if not is_superuser(ctx.author):
        return await ctx.send(
            "❌ Only a bot superuser can clear the buffer.", ephemeral=True
        )
    removed: int = bot.errors.clear()
    log.info("Error buffer cleared by %s (%d records).", ctx.author, removed)
    await ctx.send(f"U0001f9f9 Cleared **{removed}** error records.", ephemeral=True)


# Operational error trapping (prefix + slash, one funnel)
# --------------------------------------------------------------------------- #


async def _report_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send("❌ This command only works inside a server.", ephemeral=True)
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(
            f"❌ Missing argument: `{error.param.name}`. "
            f"Check `{COMMAND_PREFIX}help {ctx.invoked_with}` for usage.",
            ephemeral=True,
        )
        return
    if isinstance(
        error,
        (
            commands.MemberNotFound,
            commands.ChannelNotFound,
            commands.RoleNotFound,
            commands.BadLiteralArgument,
            commands.BadArgument,
        ),
    ):
        await ctx.send(f"❌ {error}", ephemeral=True)
        return
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(
            f"⏳ Slow down — try again in {error.retry_after:.1f}s.", ephemeral=True
        )
        return
    if isinstance(error, (NotStaff, commands.MissingPermissions)):
        if ctx.interaction is None:
            # Guessed a staff command from chat. No details, no hints.
            await ctx.send(
                random.choice(PEASANT_TAUNTS),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return
        required: List[str] = list(getattr(error, "required", None) or [])
        detail: str = (
            " You need " + ", ".join(f"**{name}**" for name in required) + "."
            if required
            else ""
        )
        await ctx.send(
            f"❌ You don't have permission to use this command.{detail}",
            ephemeral=True,
        )
        return
    if isinstance(error, commands.CheckFailure):
        await ctx.send(
            "❌ You don't have permission to use this command.", ephemeral=True
        )
        return

    original: BaseException = getattr(error, "original", error)
    if isinstance(original, PyMongoError):
        log.error("Database error in command '%s': %s", ctx.command, original)
        await ctx.send(
            "⚠️ The database is temporarily unreachable — please try again shortly.",
            ephemeral=True,
        )
        return
    if isinstance(original, discord.Forbidden):
        await ctx.send(
            "❌ I'm missing the Discord permissions to do that.", ephemeral=True
        )
        return

    log.exception("Unhandled error in command '%s'", ctx.command, exc_info=error)
    bot.log_error(
        f"command:{ctx.command}",
        original,
        guild=ctx.guild,
        channel=ctx.channel,
        user=ctx.author,
        command=str(ctx.command),
    )
    try:
        await ctx.send(
            "⚠️ Something went wrong while running that command.", ephemeral=True
        )
    except discord.DiscordException:
        pass


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
    try:
        await _report_error(ctx, error)
    except discord.DiscordException as exc:
        log.error("Failed to report command error: %s", exc)


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction, error: app_commands.AppCommandError
) -> None:
    # Hybrid commands funnel through on_command_error; this catches the rest.
    log.error("App command error: %s", error)
    bot.errors.record(
        f"app_command:{getattr(interaction.command, 'qualified_name', 'unknown')}",
        error,
        guild=interaction.guild,
        channel=interaction.channel,
        user=interaction.user,
        command=getattr(interaction.command, "qualified_name", None),
    )
    message: str = "⚠️ Something went wrong while running that command."
    if isinstance(error, (NotStaff, app_commands.MissingPermissions)):
        message = "❌ You don't have permission to use this command."
    elif isinstance(error, app_commands.CheckFailure):
        message = "❌ You don't have permission to use this command."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.DiscordException:
        pass
