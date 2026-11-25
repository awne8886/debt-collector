"""Message log (edits/deletes) and the attachment cache that feeds it."""

import asyncio
import copy
import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from utils.attachment_cache import AttachmentCache
from core.config import DEFAULT_SETTINGS
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


def _is_cacheable_media(attachment: Any) -> bool:
    content_type: str = getattr(attachment, "content_type", None) or ""
    return content_type.startswith("image/") or content_type.startswith("video/")


def _attachments_to_cache(message: discord.Message) -> List[Any]:
    """Attachments of ``message`` the message-log cache should download, if any."""
    if message.guild is None or not message.attachments:
        return []
    if message.author.bot and not getattr(message, "webhook_id", None):
        return []  # the message log never reports deletes of bot messages
    config: Dict[str, Any] = _messagelog_config(message.guild.id)
    if not config.get("cache_attachments"):
        return []
    if not config.get("enabled") or not config.get("channel_id"):
        return []
    if _messagelog_skip(message.guild.id, message.channel.id):
        return []
    cache: AttachmentCache = bot.attachment_cache
    return [
        a
        for a in message.attachments
        if _is_cacheable_media(a) and cache.accepts(getattr(a, "size", None))
    ]


async def _cache_single_attachment(
    message_id: int, attachment: discord.Attachment
) -> None:
    cache: AttachmentCache = bot.attachment_cache
    if not cache.accepts(getattr(attachment, "size", None)):
        return
    try:
        async with bot.attachment_semaphore:
            data = await attachment.read()
    except (discord.HTTPException, aiohttp.ClientError, asyncio.TimeoutError) as exc:
        log.debug("Failed to cache attachment: %s", exc)
        return
    cache.put(message_id, attachment.filename, attachment.content_type, data)


# --------------------------------------------------------------------------- #
# Message archive - edits and deletions logged as embeds
# --------------------------------------------------------------------------- #


def _messagelog_config(guild_id: int) -> Dict[str, Any]:
    stored: Dict[str, Any] = (
        bot.settings.peek_settings(guild_id).get("messagelog") or {}
    )
    config: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS["messagelog"])
    for key, value in stored.items():
        if key in config:
            config[key] = value
    return config


def _messagelog_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    """The configured log channel, or None when logging is off or unusable."""
    config: Dict[str, Any] = _messagelog_config(guild.id)
    if not config.get("enabled") or not config.get("channel_id"):
        return None
    channel: Any = guild.get_channel(int(config["channel_id"]))
    if not isinstance(channel, discord.TextChannel):
        return None
    permissions: discord.Permissions = channel.permissions_for(guild.me)
    if not (permissions.send_messages and permissions.embed_links):
        return None
    return channel


def _messagelog_skip(guild_id: int, channel_id: int) -> bool:
    config: Dict[str, Any] = _messagelog_config(guild_id)
    if str(channel_id) == str(config.get("channel_id")):
        return True  # never log the log channel
    return str(channel_id) in [str(c) for c in (config.get("ignored_channels") or [])]


def _describe_attachments(items: List[Any]) -> str:
    if not items:
        return ""
    names: List[str] = []
    for item in items[:5]:
        name: Any = (
            item.get("filename")
            if isinstance(item, dict)
            else getattr(item, "filename", None)
        )
        if name:
            names.append(str(name))
    return ", ".join(names)


async def _post_messagelog(
    guild: discord.Guild, embed: discord.Embed, files: List[discord.File] = None
) -> None:
    channel: Optional[discord.TextChannel] = _messagelog_channel(guild)
    if channel is None:
        return
    try:
        await channel.send(
            embed=embed,
            files=files or [],
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.DiscordException as exc:
        bot.log_error("messagelog:send", exc, guild=guild)


@bot.listen("on_raw_message_edit")
async def messagelog_edit(payload: discord.RawMessageUpdateEvent) -> None:
    if payload.guild_id is None:
        return
    guild: Optional[discord.Guild] = bot.get_guild(payload.guild_id)
    if guild is None or _messagelog_skip(guild.id, payload.channel_id):
        return

    data: Dict[str, Any] = payload.data or {}
    author: Dict[str, Any] = data.get("author") or {}
    if author.get("bot") and "webhook_id" not in data:
        return

    after: str = str(data.get("content") or "")
    cached: Optional[discord.Message] = payload.cached_message
    before: str = cached.content if cached is not None else ""
    if cached is not None and cached.author.bot:
        return
    if before == after:
        return  # embed hydration and pin changes also fire this event

    author_name: str = (
        str(cached.author)
        if cached is not None
        else str(author.get("username") or "unknown")
    )
    author_id: Any = cached.author.id if cached is not None else author.get("id")
    jump: str = (
        f"https://discord.com/channels/{guild.id}/{payload.channel_id}/{payload.message_id}"
    )

    embed: discord.Embed = discord.Embed(
        title="\u270f\ufe0f Message edited",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    if cached is not None:
        embed.set_author(name=author_name, icon_url=cached.author.display_avatar.url)
    else:
        embed.set_author(name=author_name)
    embed.add_field(
        name="Before",
        value=(
            discord.utils.escape_markdown(before)[:1000] if before else "*not cached*"
        ),
        inline=False,
    )
    embed.add_field(
        name="After",
        value=(discord.utils.escape_markdown(after)[:1000] if after else "*empty*"),
        inline=False,
    )
    embed.add_field(name="Channel", value=f"<#{payload.channel_id}>", inline=True)
    embed.add_field(name="Jump", value=f"[go to message]({jump})", inline=True)
    if author_id:
        embed.set_footer(
            text=f"Author ID {author_id} \u00b7 Message ID {payload.message_id}"
        )
    await _post_messagelog(guild, embed)


@bot.listen("on_raw_message_delete")
async def messagelog_delete(payload: discord.RawMessageDeleteEvent) -> None:
    if payload.guild_id is None:
        return
    # Always free the cached bytes: the message is gone either way.
    cached_attachments: List[Dict[str, Any]] = bot.attachment_cache.pop(
        payload.message_id
    )
    guild: Optional[discord.Guild] = bot.get_guild(payload.guild_id)
    if guild is None or _messagelog_skip(guild.id, payload.channel_id):
        return

    cached: Optional[discord.Message] = payload.cached_message
    if (
        cached is not None
        and cached.author.bot
        and not getattr(cached, "webhook_id", None)
    ):
        return

    files: List[discord.File] = []
    embed: discord.Embed = discord.Embed(
        title="\U0001f5d1\ufe0f Message deleted",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    if cached is not None:
        embed.set_author(
            name=str(cached.author), icon_url=cached.author.display_avatar.url
        )
        embed.description = (
            discord.utils.escape_markdown(cached.content)[:2000]
            if cached.content
            else "*no text content*"
        )
        attachments: str = _describe_attachments(list(cached.attachments))
        for i, a in enumerate(cached_attachments):
            if a["data"]:
                try:
                    f = discord.File(io.BytesIO(a["data"]), filename=a["filename"])
                    files.append(f)
                    # We can optionally set the first image as the embed image
                    if (
                        i == 0
                        and a["content_type"]
                        and a["content_type"].startswith("image/")
                    ):
                        embed.set_image(url=f"attachment://{a['filename']}")
                except Exception:
                    pass

        if attachments:
            embed.add_field(name="Attachments", value=attachments[:1024], inline=False)
        if cached.stickers:
            embed.add_field(
                name="Stickers",
                value=", ".join(sticker.name for sticker in cached.stickers)[:1024],
                inline=False,
            )
        embed.add_field(
            name="Sent",
            value=f"<t:{int(cached.created_at.timestamp())}:R>",
            inline=True,
        )
        embed.set_footer(
            text=f"Author ID {cached.author.id} \u00b7 Message ID {payload.message_id}"
        )
    else:
        embed.description = "*Message was not cached, so its content is unavailable.*"
        embed.set_footer(text=f"Message ID {payload.message_id}")

    embed.add_field(name="Channel", value=f"<#{payload.channel_id}>", inline=True)
    await _post_messagelog(guild, embed, files=files or None)


@bot.listen("on_raw_bulk_message_delete")
async def messagelog_bulk_delete(payload: discord.RawBulkMessageDeleteEvent) -> None:
    if payload.guild_id is None:
        return
    for message_id in payload.message_ids:
        bot.attachment_cache.pop(message_id)
    guild: Optional[discord.Guild] = bot.get_guild(payload.guild_id)
    if guild is None or _messagelog_skip(guild.id, payload.channel_id):
        return

    embed: discord.Embed = discord.Embed(
        title="\U0001f9f9 Bulk delete",
        description=(
            f"**{len(payload.message_ids)}** messages were removed from "
            f"<#{payload.channel_id}>."
        ),
        color=discord.Color.dark_red(),
        timestamp=datetime.now(timezone.utc),
    )
    cached_count: int = len(payload.cached_messages)
    if cached_count:
        authors: Dict[str, int] = {}
        for message in payload.cached_messages:
            authors[str(message.author)] = authors.get(str(message.author), 0) + 1
        top: List[str] = [
            f"`{name}` \u00d7{count}"
            for name, count in sorted(
                authors.items(), key=lambda kv: kv[1], reverse=True
            )[:8]
        ]
        embed.add_field(
            name=f"Authors ({cached_count} cached)",
            value="\n".join(top)[:1024],
            inline=False,
        )
    await _post_messagelog(guild, embed)


@bot.hybrid_group(
    name="messagelog",
    description="Log edited and deleted messages to a channel",
    fallback="status",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@custom_has_permissions(manage_guild=True)
async def messagelog_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _messagelog_config(ctx.guild.id)
    ignored: List[str] = [str(c) for c in (config.get("ignored_channels") or [])]
    channel: Optional[discord.TextChannel] = _messagelog_channel(ctx.guild)

    embed: discord.Embed = discord.Embed(
        title="\U0001f4dc Message log",
        color=discord.Color.green() if channel is not None else discord.Color.greyple(),
    )
    if not config.get("enabled") or not config.get("channel_id"):
        state: str = "\u26aa off"
    elif channel is None:
        state = "\u26a0\ufe0f configured, but I can't post there"
    else:
        state = f"\U0001f7e2 logging to {channel.mention}"
    embed.add_field(name="State", value=state, inline=False)
    embed.add_field(
        name=f"Ignored channels ({len(ignored)})",
        value=", ".join(f"<#{c}>" for c in ignored[:15]) or "*none*",
        inline=False,
    )
    embed.set_footer(
        text=f"{ctx.clean_prefix}set messagelog #channel \u00b7 "
        f"{ctx.clean_prefix}messagelog ignore #channel"
    )
    await ctx.send(embed=embed, ephemeral=True)


@messagelog_group.command(
    name="ignore", description="Stop or resume logging one channel"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(channel="Which channel to toggle")
async def messagelog_ignore(
    ctx: commands.Context, channel: discord.TextChannel
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    config: Dict[str, Any] = _messagelog_config(ctx.guild.id)
    ignored: List[str] = [str(c) for c in (config.get("ignored_channels") or [])]
    if str(channel.id) in ignored:
        ignored.remove(str(channel.id))
        outcome: str = f"\u2705 Resumed logging in {channel.mention}."
    else:
        ignored.append(str(channel.id))
        outcome = f"\U0001f507 {channel.mention} will no longer be logged."

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"messagelog.ignored_channels": ignored}
    )
    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@messagelog_group.command(
    name="cache",
    description="Toggle holding deleted attachments in memory to prevent broken images",
)
@custom_has_permissions(manage_guild=True)
async def messagelog_cache(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    config: Dict[str, Any] = _messagelog_config(ctx.guild.id)
    cache_enabled = not config.get("cache_attachments", False)

    saved = await bot.settings.push_fields(
        ctx.guild.id, {"messagelog.cache_attachments": cache_enabled}
    )

    if cache_enabled:
        outcome = "\u2705 I will now cache small image and video attachments to ensure they are visible if deleted."
    else:
        outcome = "\U0001f507 Attachment caching disabled."
    stats: Dict[str, int] = bot.attachment_cache.stats()
    outcome += (
        f"\n-# Cache (all servers): {stats['entries']} messages, "
        f"{stats['bytes'] / 1048576:.1f}/{stats['max_bytes'] / 1048576:.0f} MB, "
        f"{stats['hits']} hits, {stats['evictions']} evictions."
    )

    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )
