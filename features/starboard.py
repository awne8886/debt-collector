"""The starboard."""

import copy
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core.config import DEFAULT_SETTINGS
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot
from features.automations import _parse_emoji_tokens

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Starboard
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Starboard - any emoji, relayed through a webhook as the original author
# --------------------------------------------------------------------------- #

STARBOARD_MAX_POSTED: int = 500
STARBOARD_CONTENT_BUDGET: int = 1800
_STARBOARD_DISCORD_RE: re.Pattern = re.compile(r"discord", re.IGNORECASE)
_STARBOARD_INFLIGHT: set = set()


def _starboard_config(guild_id: int) -> Dict[str, Any]:
    stored: Dict[str, Any] = bot.settings.peek_settings(guild_id).get("starboard") or {}
    config: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS["starboard"])
    for key, value in stored.items():
        if key in config:
            config[key] = value
    return config


def _sanitize_webhook_name(raw: str) -> str:
    """Discord rejects webhook usernames containing 'discord' or over 80 chars."""
    cleaned: str = _STARBOARD_DISCORD_RE.sub("d\u200bscord", raw or "").strip()
    cleaned = cleaned[:80].strip()
    return cleaned or "Member"


async def _starboard_webhook(channel: discord.TextChannel) -> Optional[discord.Webhook]:
    """Reusable webhook for the board channel, cached in memory and in settings."""
    cached: Optional[discord.Webhook] = bot.starboard_webhooks.get(channel.id)
    if cached is not None:
        return cached

    config: Dict[str, Any] = _starboard_config(channel.guild.id)
    stored_id: Any = config.get("webhook_id")
    stored_token: Any = config.get("webhook_token")
    if stored_id and stored_token and bot.http_session is not None:
        hook: discord.Webhook = discord.Webhook.partial(
            int(stored_id), str(stored_token), session=bot.http_session
        )
        bot.starboard_webhooks[channel.id] = hook
        return hook

    permissions: discord.Permissions = channel.permissions_for(channel.guild.me)
    if not permissions.manage_webhooks:
        log.warning(
            "Starboard needs Manage Webhooks in channel %s (guild %s).",
            channel.id,
            channel.guild.id,
        )
        return None

    try:
        existing: List[discord.Webhook] = await channel.webhooks()
        hook = discord.utils.find(
            lambda w: w.token is not None
            and w.user is not None
            and bot.user is not None
            and w.user.id == bot.user.id,
            existing,
        )
        if hook is None:
            hook = await channel.create_webhook(
                name="Starboard", reason="Starboard message relay"
            )
    except discord.DiscordException as exc:
        bot.log_error("starboard:webhook", exc, guild=channel.guild)
        return None

    bot.starboard_webhooks[channel.id] = hook
    await bot.settings.push_fields(
        channel.guild.id,
        {
            "starboard.webhook_id": str(hook.id),
            "starboard.webhook_token": str(hook.token),
        },
    )
    return hook


async def _starboard_best_reaction(
    message: discord.Message, config: Dict[str, Any]
) -> Tuple[Optional[str], int]:
    """Highest-counting eligible reaction on a message, as (emoji, count)."""
    blocked: List[str] = [str(e) for e in (config.get("blocked_emojis") or [])]
    allow_self: bool = bool(config.get("self_star"))

    best_emoji: Optional[str] = None
    best_count: int = 0

    sorted_reactions = sorted(
        message.reactions, key=lambda r: int(r.count or 0), reverse=True
    )
    for reaction in sorted_reactions:
        token: str = str(reaction.emoji)
        if token in blocked:
            continue
        count: int = int(reaction.count or 0)

        if count <= best_count:
            break

        if not allow_self and count:
            try:
                async for user in reaction.users(limit=100):
                    if user.id == message.author.id:
                        count -= 1
                        break
            except discord.DiscordException:
                pass

        if count > best_count:
            best_emoji, best_count = token, count

    return best_emoji, best_count


def _starboard_body(message: discord.Message) -> str:
    """Original text plus attachment links, with the jump link appended last."""
    parts: List[str] = []
    content: str = (message.content or "").strip()
    if content:
        parts.append(content[:STARBOARD_CONTENT_BUDGET])

    for attachment in message.attachments[:4]:
        parts.append(attachment.url)
    if message.stickers:
        parts.append(
            "*(sticker: " + ", ".join(s.name for s in message.stickers[:3])[:80] + ")*"
        )
    if not parts:
        parts.append("*(no text content)*")

    parts.append(f"\n[\u21aa jump to the original message]({message.jump_url})")
    return "\n".join(parts)[:2000]


@bot.listen("on_raw_reaction_add")
async def _starboard_listener(payload: discord.RawReactionActionEvent) -> None:
    if payload.guild_id is None:
        return
    guild: Optional[discord.Guild] = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    config: Dict[str, Any] = _starboard_config(guild.id)
    if not config.get("enabled") or not config.get("channel_id"):
        return
    if str(payload.emoji) in [str(e) for e in (config.get("blocked_emojis") or [])]:
        return
    if str(payload.channel_id) in [
        str(c) for c in (config.get("ignored_channels") or [])
    ]:
        return

    posted: Dict[str, Any] = dict(config.get("posted") or {})
    for k, v in list(posted.items()):
        if isinstance(v, str):
            posted[k] = {"relayed": v, "last": 0, "count_msg": None}

    if payload.message_id in _STARBOARD_INFLIGHT:
        return

    already_posted = str(payload.message_id) in posted
    if already_posted:
        posted_data = posted[str(payload.message_id)]
        last_update = posted_data.get("last", 0)
        if time.time() - last_update < 7200:
            return
        if not posted_data.get("count_msg"):
            return

    source: Any = guild.get_channel(payload.channel_id)
    board: Any = guild.get_channel(int(config["channel_id"]))
    if not isinstance(source, discord.TextChannel) or not isinstance(
        board, discord.TextChannel
    ):
        return
    if source.id == board.id:
        return
    if getattr(source, "is_nsfw", lambda: False)() and not board.is_nsfw():
        return  # never relay age-restricted content into a normal channel

    try:
        message: discord.Message = await source.fetch_message(payload.message_id)
    except discord.DiscordException:
        return
    if message.author.bot and not config.get("allow_bots"):
        return

    emoji, count = await _starboard_best_reaction(message, config)
    if emoji is None or count < int(config.get("threshold") or 3):
        return

    _STARBOARD_INFLIGHT.add(payload.message_id)
    try:
        if already_posted:
            posted_data = posted[str(payload.message_id)]
            count_msg_id = posted_data.get("count_msg")
            if count_msg_id:
                try:
                    count_msg = await board.fetch_message(int(count_msg_id))
                    await count_msg.edit(
                        content=f"{emoji} **{count}** · from {source.mention}"
                    )
                except discord.DiscordException:
                    pass
            posted_data["last"] = int(time.time())
            await bot.settings.push_fields(guild.id, {"starboard.posted": posted})
            return

        hook: Optional[discord.Webhook] = await _starboard_webhook(board)
        if hook is None:
            return

        author: Any = message.author
        try:
            relayed = await hook.send(
                content=_starboard_body(message),
                username=_sanitize_webhook_name(
                    getattr(author, "display_name", str(author))
                ),
                avatar_url=author.display_avatar.url,
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
            )
        except discord.NotFound:
            # Webhook was deleted out from under us - drop it and retry next time.
            bot.starboard_webhooks.pop(board.id, None)
            await bot.settings.push_fields(
                guild.id,
                {"starboard.webhook_id": None, "starboard.webhook_token": None},
            )
            return
        except discord.DiscordException as exc:
            bot.log_error("starboard:send", exc, guild=guild)
            return

        count_msg_id = None
        if config.get("show_count") and relayed is not None:
            try:
                count_msg = await board.send(
                    f"{emoji} **{count}** · from {source.mention}",
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                count_msg_id = str(count_msg.id)
            except discord.DiscordException:
                pass

        posted[str(payload.message_id)] = {
            "relayed": str(getattr(relayed, "id", "1")),
            "last": int(time.time()),
            "count_msg": count_msg_id,
        }
        if len(posted) > STARBOARD_MAX_POSTED:
            for stale in list(posted)[: len(posted) - STARBOARD_MAX_POSTED]:
                posted.pop(stale, None)
        await bot.settings.push_fields(guild.id, {"starboard.posted": posted})
        log.info(
            "Starboard relayed message %s (%s x%d) in guild %s.",
            payload.message_id,
            emoji,
            count,
            guild.id,
        )
    finally:
        _STARBOARD_INFLIGHT.discard(payload.message_id)


@bot.hybrid_group(
    name="starboard",
    description="Relay popular messages to a highlights channel",
    fallback="status",
)
@commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@custom_has_permissions(manage_guild=True)
async def starboard_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    config: Dict[str, Any] = _starboard_config(ctx.guild.id)
    channel: Any = (
        ctx.guild.get_channel(int(config["channel_id"]))
        if config.get("channel_id")
        else None
    )
    blocked: List[str] = [str(e) for e in (config.get("blocked_emojis") or [])]
    ignored: List[str] = [str(c) for c in (config.get("ignored_channels") or [])]

    embed: discord.Embed = discord.Embed(
        title="\u2b50 Starboard",
        colour=(
            discord.Colour.gold() if config.get("enabled") else discord.Colour.greyple()
        ),
    )
    embed.add_field(
        name="State",
        value=(
            f"\U0001f7e2 relaying to {channel.mention}"
            if config.get("enabled") and channel is not None
            else "\u26aa off"
        ),
        inline=False,
    )
    embed.add_field(
        name="Trigger",
        value=(
            f"**any** emoji reaching **{config.get('threshold', 3)}** reactions\n"
            f"author's own reaction counts: "
            f"{'yes' if config.get('self_star') else 'no'}\n"
            f"relay bot messages: {'yes' if config.get('allow_bots') else 'no'}"
        ),
        inline=False,
    )
    embed.add_field(
        name=f"Blocked emojis ({len(blocked)})",
        value=" ".join(blocked[:20]) or "*none*",
        inline=False,
    )
    embed.add_field(
        name=f"Ignored channels ({len(ignored)})",
        value=", ".join(f"<#{c}>" for c in ignored[:15]) or "*none*",
        inline=False,
    )
    embed.set_footer(
        text="Messages are reposted through a webhook using the author's name and avatar"
    )
    await ctx.send(embed=embed, ephemeral=True)


@starboard_group.command(
    name="set", description="Choose the highlights channel and threshold"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    channel="Where highlighted messages are relayed",
    threshold="How many of any one emoji are needed",
)
async def starboard_set(
    ctx: commands.Context,
    channel: discord.TextChannel,
    threshold: app_commands.Range[int, 1, 100] = 3,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    permissions: discord.Permissions = channel.permissions_for(ctx.guild.me)
    missing: List[str] = [
        name
        for name, ok in (
            ("Send Messages", permissions.send_messages),
            ("Manage Webhooks", permissions.manage_webhooks),
        )
        if not ok
    ]
    if missing:
        return await ctx.send(
            f"\u274c I need {', '.join(f'**{m}**' for m in missing)} in {channel.mention}. "
            "Manage Webhooks is what lets me repost as the original author.",
            ephemeral=True,
        )

    bot.starboard_webhooks.pop(channel.id, None)
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id,
        {
            "starboard.enabled": True,
            "starboard.channel_id": str(channel.id),
            "starboard.threshold": int(threshold),
            "starboard.webhook_id": None,
            "starboard.webhook_token": None,
        },
    )
    log.info("Starboard set in guild %s by %s.", ctx.guild.id, ctx.author)
    await ctx.send(
        f"\u2705 Starboard on \u2014 any emoji reaching **{threshold}** reactions is "
        f"reposted to {channel.mention} as the original author."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@starboard_group.command(name="threshold", description="How many reactions are needed")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(count="Reactions of any single emoji required")
async def starboard_threshold(
    ctx: commands.Context, count: app_commands.Range[int, 1, 100]
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"starboard.threshold": int(count)}
    )
    await ctx.send(
        f"\u2705 Threshold set to **{int(count)}** reactions."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@starboard_group.command(
    name="block", description="Stop one emoji from ever triggering"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(emoji="Emoji to block, or block again to unblock it")
async def starboard_block(ctx: commands.Context, emoji: str) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    tokens: List[str] = _parse_emoji_tokens(emoji)
    if not tokens:
        return await ctx.send(
            "\u274c I couldn't read an emoji in that.", ephemeral=True
        )

    config: Dict[str, Any] = _starboard_config(ctx.guild.id)
    blocked: List[str] = [str(e) for e in (config.get("blocked_emojis") or [])]
    added: List[str] = []
    removed: List[str] = []
    for token in tokens[:10]:
        if token in blocked:
            blocked.remove(token)
            removed.append(token)
        elif len(blocked) < 50:
            blocked.append(token)
            added.append(token)

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"starboard.blocked_emojis": blocked}
    )
    lines: List[str] = []
    if added:
        lines.append("\U0001f6ab Blocked " + " ".join(added))
    if removed:
        lines.append("\u2705 Unblocked " + " ".join(removed))
    await ctx.send(
        "\n".join(lines)
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@starboard_group.command(
    name="ignore", description="Stop watching reactions in a channel"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(channel="Channel to ignore, or run again to watch it")
async def starboard_ignore(ctx: commands.Context, channel: discord.TextChannel) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    config: Dict[str, Any] = _starboard_config(ctx.guild.id)
    ignored: List[str] = [str(c) for c in (config.get("ignored_channels") or [])]
    if str(channel.id) in ignored:
        ignored.remove(str(channel.id))
        outcome: str = f"\u2705 Watching {channel.mention} again."
    else:
        ignored.append(str(channel.id))
        outcome = f"\U0001f507 Reactions in {channel.mention} will be ignored."

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"starboard.ignored_channels": ignored}
    )
    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@starboard_group.command(
    name="options", description="Self-reactions, bot messages, count line"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    self_star="Count the author's own reaction toward the threshold",
    allow_bots="Also relay messages posted by bots",
    show_count="Post a small line naming the emoji and count under each relay",
)
async def starboard_options(
    ctx: commands.Context,
    self_star: Optional[bool] = None,
    allow_bots: Optional[bool] = None,
    show_count: Optional[bool] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    fields: Dict[str, Any] = {}
    if self_star is not None:
        fields["starboard.self_star"] = bool(self_star)
    if allow_bots is not None:
        fields["starboard.allow_bots"] = bool(allow_bots)
    if show_count is not None:
        fields["starboard.show_count"] = bool(show_count)
    if not fields:
        return await ctx.send(
            "\u274c Give me at least one option to change.", ephemeral=True
        )

    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    await ctx.send(
        f"\u2705 Updated **{len(fields)}** option(s)."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@starboard_group.command(name="off", description="Turn the starboard off")
@custom_has_permissions(manage_guild=True)
async def starboard_off(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"starboard.enabled": False}
    )
    await ctx.send(
        "\u2705 Starboard turned off. Your channel and settings are kept."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )
