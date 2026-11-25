"""Welcome and leave messages (`welcomer` / `leaver`)."""

import copy
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core.config import DEFAULT_SETTINGS
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Welcomer / leaver greetings
# --------------------------------------------------------------------------- #

GREETER_KEYS: Dict[str, Dict[str, str]] = {
    "welcome": {
        "label": "Welcomer",
        "icon": "\U0001f44b",
        "event": "joins",
        "command": "welcomer",
        "default": "\U0001f44b Welcome {mention} to **{server}** \u2014 you're our {ordinal} member!",
    },
    "goodbye": {
        "label": "Leaver",
        "icon": "\U0001f6aa",
        "event": "leaves",
        "command": "leaver",
        "default": "\U0001f6aa **{user}** left **{server}**. {count} members remain.",
    },
}

GREETER_PLACEHOLDERS: str = (
    "`{mention}` `{user}` `{tag}` `{id}` `{server}` `{count}` `{ordinal}` "
    "`{created}` `{joined}` `{avatar}`"
)


def _ordinal(number: int) -> str:
    """1 -> 1st, 2 -> 2nd, 13 -> 13th."""
    if 10 <= (number % 100) <= 20:
        suffix: str = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")
    return f"{number}{suffix}"


def _format_member_message(template: str, member: discord.Member) -> str:
    """Substitute greeter placeholders against a member."""
    count: int = int(member.guild.member_count or 0)
    joined: Any = getattr(member, "joined_at", None) or discord.utils.utcnow()
    replacements: Dict[str, str] = {
        "{mention}": member.mention,
        "{user}": member.display_name,
        "{tag}": str(member),
        "{id}": str(member.id),
        "{server}": member.guild.name,
        "{count}": str(count),
        "{ordinal}": _ordinal(count),
        "{created}": f"<t:{int(member.created_at.timestamp())}:R>",
        "{joined}": f"<t:{int(joined.timestamp())}:R>",
        "{avatar}": member.display_avatar.url,
    }
    text: str = template or ""
    for token, value in replacements.items():
        text = text.replace(token, value)
    return text[:2000]


def _greeter_config(guild_id: int, key: str) -> Dict[str, Any]:
    stored: Dict[str, Any] = bot.settings.peek_settings(guild_id).get(key) or {}
    config: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS[key])
    for name, value in stored.items():
        if name in config:
            config[name] = value
    return config


def _greeter_colour(raw: Any) -> discord.Colour:
    try:
        return discord.Colour(int(str(raw).lstrip("#"), 16))
    except (TypeError, ValueError):
        return discord.Colour.blurple()


def _render_greeter(
    config: Dict[str, Any], member: discord.Member, key: str
) -> Tuple[str, Optional[discord.Embed]]:
    """Build the outgoing content and embed for one greeting."""
    template: str = str(config.get("message") or GREETER_KEYS[key]["default"])
    body: str = _format_member_message(template, member)

    if not config.get("embed"):
        return body, None

    embed: discord.Embed = discord.Embed(
        description=body,
        colour=_greeter_colour(config.get("color")),
        timestamp=datetime.now(timezone.utc),
    )
    title: str = str(config.get("title") or "")
    if title:
        embed.title = _format_member_message(title, member)[:256]
    if config.get("thumbnail"):
        embed.set_thumbnail(url=member.display_avatar.url)
    image: str = str(config.get("image") or "")
    if image.startswith("http"):
        embed.set_image(url=image)
    embed.set_footer(text=f"{member} \u00b7 {member.id}")

    content: str = member.mention if config.get("ping") and key == "welcome" else ""
    return content, embed


async def _fire_greeter(member: discord.Member, key: str) -> None:
    config: Dict[str, Any] = _greeter_config(member.guild.id, key)
    if not config.get("enabled") or not config.get("channel_id"):
        return
    channel: Any = member.guild.get_channel(int(config["channel_id"]))
    if not isinstance(channel, discord.TextChannel):
        return
    permissions: discord.Permissions = channel.permissions_for(member.guild.me)
    if not permissions.send_messages:
        return
    if config.get("embed") and not permissions.embed_links:
        config = dict(config)
        config["embed"] = False

    content, embed = _render_greeter(config, member, key)
    try:
        await channel.send(
            content or None,
            embed=embed,
            allowed_mentions=discord.AllowedMentions(users=True),
        )
    except discord.DiscordException as exc:
        bot.log_error(key, exc, guild=member.guild, user=member)

    dm_text: str = str(config.get("dm_message") or "")
    if key == "welcome" and dm_text:
        try:
            await member.send(_format_member_message(dm_text, member))
        except (discord.Forbidden, discord.HTTPException):
            pass


@bot.listen("on_member_join")
async def _welcome_listener(member: discord.Member) -> None:
    if member.bot:
        return
    await _fire_greeter(member, "welcome")


@bot.listen("on_member_remove")
async def _goodbye_listener(member: discord.Member) -> None:
    if member.bot:
        return
    await _fire_greeter(member, "goodbye")


async def _greeter_status(ctx: commands.Context, key: str) -> None:
    meta: Dict[str, str] = GREETER_KEYS[key]
    config: Dict[str, Any] = _greeter_config(ctx.guild.id, key)
    channel: Any = (
        ctx.guild.get_channel(int(config["channel_id"]))
        if config.get("channel_id")
        else None
    )

    embed: discord.Embed = discord.Embed(
        title=f"{meta['icon']} {meta['label']}",
        colour=(
            discord.Colour.green()
            if config.get("enabled")
            else discord.Colour.greyple()
        ),
    )
    embed.add_field(
        name="State",
        value=(
            f"\U0001f7e2 posting in {channel.mention}"
            if config.get("enabled") and channel is not None
            else "\u26aa off"
        ),
        inline=False,
    )
    embed.add_field(
        name="Style",
        value=(
            f"embed: {'on' if config.get('embed') else 'off'}\n"
            f"thumbnail: {'on' if config.get('thumbnail') else 'off'}\n"
            + (
                f"ping on join: {'on' if config.get('ping') else 'off'}"
                if key == "welcome"
                else ""
            )
        ).strip(),
        inline=True,
    )
    embed.add_field(
        name="Message",
        value=f"```{str(config.get('message') or meta['default'])[:400]}```",
        inline=False,
    )
    if key == "welcome" and config.get("dm_message"):
        embed.add_field(
            name="Also DMs them",
            value=f"```{str(config['dm_message'])[:300]}```",
            inline=False,
        )
    embed.set_footer(
        text=f"{ctx.clean_prefix}{meta['command']} test to preview it on yourself"
    )
    await ctx.send(embed=embed, ephemeral=True)


async def _greeter_set(
    ctx: commands.Context,
    key: str,
    channel: discord.TextChannel,
    message: Optional[str],
) -> None:
    meta: Dict[str, str] = GREETER_KEYS[key]
    permissions: discord.Permissions = channel.permissions_for(ctx.guild.me)
    if not permissions.send_messages:
        return await ctx.send(
            f"\u274c I can't send messages in {channel.mention}.", ephemeral=True
        )

    fields: Dict[str, Any] = {
        f"{key}.channel_id": str(channel.id),
        f"{key}.enabled": True,
    }
    if message:
        fields[f"{key}.message"] = message[:1000]
    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    log.info(
        "%s channel set in guild %s by %s.", meta["label"], ctx.guild.id, ctx.author
    )

    await ctx.send(
        f"\u2705 {meta['label']} is on \u2014 messages post in {channel.mention} "
        f"when someone {meta['event']}."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


async def _greeter_message(
    ctx: commands.Context, key: str, message: Optional[str]
) -> None:
    meta: Dict[str, str] = GREETER_KEYS[key]
    text: str = (message or meta["default"])[:1000]
    saved: bool = await bot.settings.push_fields(ctx.guild.id, {f"{key}.message": text})
    preview: str = _format_member_message(text, ctx.author)
    await ctx.send(
        f"\u2705 {meta['label']} message updated.\n**Preview:** {preview[:500]}"
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def _greeter_style(
    ctx: commands.Context,
    key: str,
    embed_mode: Optional[bool],
    title: Optional[str],
    colour: Optional[str],
    image: Optional[str],
    thumbnail: Optional[bool],
    ping: Optional[bool],
) -> None:
    fields: Dict[str, Any] = {}
    if embed_mode is not None:
        fields[f"{key}.embed"] = bool(embed_mode)
    if title is not None:
        fields[f"{key}.title"] = title[:200]
    if thumbnail is not None:
        fields[f"{key}.thumbnail"] = bool(thumbnail)
    if ping is not None and key == "welcome":
        fields[f"{key}.ping"] = bool(ping)
    if image is not None:
        if image and not image.startswith("http"):
            return await ctx.send(
                "\u274c The image must be a direct URL.", ephemeral=True
            )
        fields[f"{key}.image"] = image[:400]
    if colour is not None:
        cleaned: str = colour.strip().lstrip("#")
        try:
            int(cleaned, 16)
        except ValueError:
            return await ctx.send(
                "\u274c Give me a hex colour like `5865F2`.", ephemeral=True
            )
        fields[f"{key}.color"] = cleaned[:6]

    if not fields:
        return await ctx.send(
            "\u274c Give me at least one thing to change.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    await ctx.send(
        f"\u2705 Updated **{len(fields)}** style setting(s)."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


async def _greeter_test(ctx: commands.Context, key: str) -> None:
    meta: Dict[str, str] = GREETER_KEYS[key]
    config: Dict[str, Any] = _greeter_config(ctx.guild.id, key)
    content, embed = _render_greeter(config, ctx.author, key)
    await ctx.send(
        content=f"**Preview \u2014 {meta['label']}**\n{content}".strip(),
        embed=embed,
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def _greeter_off(ctx: commands.Context, key: str) -> None:
    meta: Dict[str, str] = GREETER_KEYS[key]
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {f"{key}.enabled": False}
    )
    await ctx.send(
        f"\u2705 {meta['label']} turned off. Your message and styling are kept."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@bot.hybrid_group(
    name="welcomer",
    description="Greet members when they join",
    fallback="status",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@custom_has_permissions(manage_guild=True)
async def welcomer_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_status(ctx, "welcome")


@welcomer_group.command(
    name="set", description="Choose the channel joins are announced in"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    channel="Where to post the greeting",
    message=f"Optional greeting text. Placeholders: {GREETER_PLACEHOLDERS}",
)
async def welcomer_set(
    ctx: commands.Context,
    channel: discord.TextChannel,
    *,
    message: Optional[str] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_set(ctx, "welcome", channel, message)


@welcomer_group.command(name="message", description="Set the join greeting text")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    message=f"Leave empty to restore the default. {GREETER_PLACEHOLDERS}"
)
async def welcomer_message(
    ctx: commands.Context, *, message: Optional[str] = None
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_message(ctx, "welcome", message)


@welcomer_group.command(
    name="style", description="Embed, colour, image and ping options"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    embed="Send as an embed instead of plain text",
    title="Embed title",
    colour="Hex colour, e.g. 5865F2",
    image="Direct image URL for the embed banner",
    thumbnail="Show the member's avatar as the thumbnail",
    ping="Mention the member so they get a notification",
)
async def welcomer_style(
    ctx: commands.Context,
    embed: Optional[bool] = None,
    title: Optional[str] = None,
    colour: Optional[str] = None,
    image: Optional[str] = None,
    thumbnail: Optional[bool] = None,
    ping: Optional[bool] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_style(ctx, "welcome", embed, title, colour, image, thumbnail, ping)


@welcomer_group.command(name="dm", description="Also DM new members a private message")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(message="Leave empty to stop DMing new members")
async def welcomer_dm(ctx: commands.Context, *, message: Optional[str] = None) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"welcome.dm_message": (message or "")[:1000]}
    )
    body: str = (
        f"\u2705 New members will also be DMed:\n{_format_member_message(message, ctx.author)[:500]}"
        if message
        else "\u2705 Welcome DMs turned off."
    )
    await ctx.send(
        body + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@welcomer_group.command(name="test", description="Preview the greeting on yourself")
@custom_has_permissions(manage_guild=True)
async def welcomer_test(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_test(ctx, "welcome")


@welcomer_group.command(name="off", description="Stop announcing joins")
@custom_has_permissions(manage_guild=True)
async def welcomer_off(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_off(ctx, "welcome")


@bot.hybrid_group(
    name="leaver",
    description="Announce members when they leave",
    fallback="status",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@custom_has_permissions(manage_guild=True)
async def leaver_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_status(ctx, "goodbye")


@leaver_group.command(
    name="set", description="Choose the channel leaves are announced in"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    channel="Where to post the farewell",
    message=f"Optional farewell text. Placeholders: {GREETER_PLACEHOLDERS}",
)
async def leaver_set(
    ctx: commands.Context,
    channel: discord.TextChannel,
    *,
    message: Optional[str] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_set(ctx, "goodbye", channel, message)


@leaver_group.command(name="message", description="Set the leave announcement text")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    message=f"Leave empty to restore the default. {GREETER_PLACEHOLDERS}"
)
async def leaver_message(
    ctx: commands.Context, *, message: Optional[str] = None
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_message(ctx, "goodbye", message)


@leaver_group.command(name="style", description="Embed, colour and image options")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    embed="Send as an embed instead of plain text",
    title="Embed title",
    colour="Hex colour, e.g. ED4245",
    image="Direct image URL for the embed banner",
    thumbnail="Show the member's avatar as the thumbnail",
)
async def leaver_style(
    ctx: commands.Context,
    embed: Optional[bool] = None,
    title: Optional[str] = None,
    colour: Optional[str] = None,
    image: Optional[str] = None,
    thumbnail: Optional[bool] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_style(ctx, "goodbye", embed, title, colour, image, thumbnail, None)


@leaver_group.command(name="test", description="Preview the farewell on yourself")
@custom_has_permissions(manage_guild=True)
async def leaver_test(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_test(ctx, "goodbye")


@leaver_group.command(name="off", description="Stop announcing leaves")
@custom_has_permissions(manage_guild=True)
async def leaver_off(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await _greeter_off(ctx, "goodbye")
