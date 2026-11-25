"""The `set` group, export and import of server settings."""

import copy
import io
import json
import logging
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

from core.config import DEFAULT_SETTINGS
from core.settings import _deep_merge
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# ================= /set =================
@bot.hybrid_group(
    name="set",
    description="Server configuration: prefix, mod log and message logging",
)
@commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def set_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send(
            f"Invalid subcommand. Try `{ctx.clean_prefix}help set`.", ephemeral=True
        )


@set_group.command(
    name="prefix", description="Set the chat command prefix for this server (default !)"
)
@app_commands.describe(prefix="New prefix, e.g. ! or x (max 5 characters)")
async def set_prefix_cmd(ctx: commands.Context, prefix: str):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)
    prefix = prefix.strip()
    if not prefix or len(prefix) > 5:
        return await ctx.send("❌ Prefix must be 1-5 characters.", ephemeral=True)

    await bot.settings.push_fields(ctx.guild.id, {"prefix": prefix})
    await ctx.send(
        f"✅ Prefix set to `{prefix}` — try `{prefix}ping` or `{prefix}hug @someone`.",
        ephemeral=True,
    )


# --------------------------------------------------------------------------- #
# Mod log / welcome / goodbye configuration
# --------------------------------------------------------------------------- #


@set_group.command(
    name="modlog", description="Send a log of every moderation action to a channel"
)
@app_commands.describe(channel="Log channel (leave empty to turn logging off)")
async def set_modlog_cmd(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)
    saved = await bot.settings.push_fields(
        ctx.guild.id,
        {
            "modlog.channel_id": str(channel.id) if channel else None,
            "modlog.enabled": channel is not None,
        },
    )
    text = (
        f"✅ Moderation actions will be logged to {channel.mention}."
        if channel
        else "✅ Moderation logging turned off."
    )
    await ctx.send(text if saved else text + " (database write failed)", ephemeral=True)


@set_group.command(
    name="messagelog", description="Log edited and deleted messages to a channel"
)
@app_commands.describe(channel="Log channel (leave empty to turn logging off)")
async def set_messagelog_cmd(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)
    if channel is not None:
        permissions = channel.permissions_for(ctx.guild.me)
        if not (permissions.send_messages and permissions.embed_links):
            return await ctx.send(
                f"❌ I need **Send Messages** and **Embed Links** in {channel.mention}.",
                ephemeral=True,
            )
    saved = await bot.settings.push_fields(
        ctx.guild.id,
        {
            "messagelog.channel_id": str(channel.id) if channel else None,
            "messagelog.enabled": channel is not None,
        },
    )
    text = (
        f"✅ Edits and deletions will be logged to {channel.mention}."
        if channel
        else "✅ Message logging turned off."
    )
    await ctx.send(text if saved else text + " (database write failed)", ephemeral=True)


# --------------------------------------------------------------------------- #
# Settings backup
# --------------------------------------------------------------------------- #


@bot.hybrid_command(
    name="export", description="Download this server's bot settings as JSON"
)
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
async def export_cmd(ctx: commands.Context):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)
    settings = copy.deepcopy(await bot.settings.fetch_settings(ctx.guild.id))
    settings.pop("_id", None)
    payload = json.dumps(settings, indent=2, default=str).encode("utf-8")
    if len(payload) > 7_000_000:
        return await ctx.send(
            "❌ The settings document is too large to export.", ephemeral=True
        )
    await ctx.send(
        "📦 Settings backup — keep it somewhere safe.",
        file=discord.File(
            io.BytesIO(payload), filename=f"settings-{ctx.guild.id}.json"
        ),
        ephemeral=True,
    )


# Settings restore

# --------------------------------------------------------------------------- #

IMPORTABLE_KEYS: frozenset = frozenset(DEFAULT_SETTINGS.keys())


@bot.hybrid_command(
    name="import", description="Restore settings from a /export backup file"
)
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
@custom_has_permissions(administrator=True)
@app_commands.describe(
    backup="The settings-<id>.json file produced by /export",
    merge="Merge into current settings instead of replacing them",
)
async def import_cmd(
    ctx: commands.Context,
    backup: discord.Attachment,
    merge: bool = True,
) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send(
            "\u274c You need Administrator permission.", ephemeral=True
        )
    if backup.size > 2_000_000:
        return await ctx.send(
            "\u274c That file is too large (2 MB limit).", ephemeral=True
        )
    if not backup.filename.lower().endswith(".json"):
        return await ctx.send(
            "\u274c I need the `.json` file from `/export`.", ephemeral=True
        )

    try:
        raw: bytes = await backup.read()
        payload: Any = json.loads(raw.decode("utf-8"))
    except (discord.HTTPException, UnicodeDecodeError, json.JSONDecodeError) as exc:
        bot.log_error("import:parse", exc, guild=ctx.guild, user=ctx.author)
        return await ctx.send(
            "\u274c I couldn't read that file as JSON.", ephemeral=True
        )

    if not isinstance(payload, dict):
        return await ctx.send(
            "\u274c That backup isn't a settings object.", ephemeral=True
        )

    accepted: Dict[str, Any] = {
        key: value for key, value in payload.items() if key in IMPORTABLE_KEYS
    }
    rejected: List[str] = sorted(set(payload) - IMPORTABLE_KEYS - {"_id", "guildid"})
    if not accepted:
        return await ctx.send(
            "\u274c No recognisable settings keys in that file.", ephemeral=True
        )

    if merge:
        current: Dict[str, Any] = copy.deepcopy(
            await bot.settings.fetch_settings(ctx.guild.id)
        )
        merged: Dict[str, Any] = _deep_merge(current, accepted)
        merged.pop("_id", None)
        final: Dict[str, Any] = {
            key: merged[key] for key in IMPORTABLE_KEYS if key in merged
        }
    else:
        base: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        final = _deep_merge(base, accepted)

    saved: bool = await bot.settings.push_settings(ctx.guild.id, final)
    if not saved:
        return await ctx.send(
            "\u26a0\ufe0f The database rejected the write - nothing changed.",
            ephemeral=True,
        )

    bot.settings.evict_cache(ctx.guild.id)
    log.info(
        "Settings imported into guild %s by %s (%d keys, merge=%s).",
        ctx.guild.id,
        ctx.author,
        len(accepted),
        merge,
    )
    note: str = (
        f"\n\u26a0\ufe0f Ignored unknown keys: `{', '.join(rejected[:10])}`"
        if rejected
        else ""
    )
    await ctx.send(
        f"\u2705 Restored **{len(accepted)}** settings sections "
        f"({'merged' if merge else 'replaced'}).{note}",
        ephemeral=True,
    )
