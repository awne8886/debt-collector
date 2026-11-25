"""Utility commands: snipe, echo, AFK, emojis/steal, tags and polls."""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.config import AFK_GRACE_SECONDS, AfkPing, AfkRecord
from core.permissions import is_superuser, member_has_perms
from core.bot import bot
from core.helpers import humanize_seconds, sanitize_mass_pings

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


@bot.hybrid_command(
    name="snipe", description="Show the last deleted message in this channel."
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
async def snipe(ctx: commands.Context) -> None:
    sniped = bot.snipes.get(ctx.channel.id)
    if sniped is None:
        await ctx.send("❌ Nothing to snipe here.", ephemeral=True)
        return
    embed = discord.Embed(
        description=sniped.content,
        color=discord.Color.orange(),
        timestamp=sniped.created_at,
    )
    embed.set_author(name=sniped.author, icon_url=sniped.author_avatar)
    deleted_ago: str = humanize_seconds(
        (datetime.now(timezone.utc) - sniped.created_at).total_seconds()
    )
    embed.set_footer(text=f"Sent {deleted_ago} ago")
    await ctx.send(embed=embed)


# --------------------------------------------------------------------------- #
@bot.hybrid_command(
    name="echoset", description="Enable or disable the echo feature guild-wide."
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@app_commands.describe(state="Turn echo on or off for this server.")
async def echoset(ctx: commands.Context, state: Literal["on", "off"]) -> None:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send(
            "❌ You need the **Manage Server** permission to change echo settings.",
            ephemeral=True,
        )
        return
    enabled: bool = state == "on"
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"echoset": enabled})  # type: ignore[union-attr]
    if saved:
        await ctx.send(
            f"📢 Echo is now **{'enabled' if enabled else 'disabled'}** for this server."
        )
    else:
        await ctx.send(
            "⚠️ Echo setting could not be persisted to the database — please try again.",
            ephemeral=True,
        )


@bot.hybrid_command(
    name="echo",
    description="Make the bot say something, optionally in another channel.",
)
@commands.guild_only()
@app_commands.describe(
    channel="Target channel (defaults to the current channel).",
    message="What the bot should say.",
)
async def echo(
    ctx: commands.Context,
    channel: Optional[discord.TextChannel] = None,
    *,
    message: Optional[str] = None,
) -> None:
    guild: Optional[discord.Guild] = ctx.guild
    if guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("❌ This command only works inside a server.", ephemeral=True)
        return
    if not message or not message.strip():
        await ctx.send("❌ You must provide a message to echo.", ephemeral=True)
        return

    settings: Dict[str, Any] = await bot.settings.fetch_settings(guild.id)
    if not settings.get("echoset", False):
        await ctx.send(
            "❌ Echo is disabled on this server. An admin can enable it with `/echoset on`.",
            ephemeral=True,
        )
        return

    actor: discord.Member = ctx.author
    target: discord.TextChannel = channel or ctx.channel  # type: ignore[assignment]
    if not isinstance(target, discord.TextChannel):
        await ctx.send("❌ Echo only works in text channels.", ephemeral=True)
        return

    if not is_superuser(actor) and not target.permissions_for(actor).send_messages:
        await ctx.send(
            f"❌ You can't send messages in {target.mention}, so you can't echo there.",
            ephemeral=True,
        )
        return

    # Anti-ping logic: strip @everyone/@here unless the invoker is privileged.
    privileged: bool = (
        is_superuser(actor)
        or actor.guild_permissions.administrator
        or actor.guild_permissions.mention_everyone
    )
    payload: str = message if privileged else sanitize_mass_pings(message)
    allowed: discord.AllowedMentions = discord.AllowedMentions(
        everyone=privileged, roles=privileged, users=True
    )

    try:
        await target.send(payload[:2000], allowed_mentions=allowed)
    except discord.Forbidden:
        await ctx.send(
            f"❌ I don't have permission to send messages in {target.mention}.",
            ephemeral=True,
        )
        return
    except discord.HTTPException as exc:
        log.error("Echo send failed: %s", exc)
        await ctx.send("⚠️ Echo failed due to a Discord API error.", ephemeral=True)
        return

    if ctx.interaction is not None:
        await ctx.send(f"✅ Echoed to {target.mention}.", ephemeral=True)
    else:
        # Prefix invocation: delete the triggering message after execution.
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass


@bot.hybrid_command(
    name="afk", description="Mark yourself AFK; pings are collected until you return."
)
@commands.guild_only()
@app_commands.describe(reason="Why you are going AFK (optional).")
async def afk(ctx: commands.Context, *, reason: str = "AFK") -> None:
    bot.afk_state[ctx.author.id] = AfkRecord(reason=reason[:300], since=time.time())
    await ctx.send(
        f"💤 {ctx.author.mention} is now AFK: **{discord.utils.escape_mentions(reason[:300])}**",
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def _handle_afk_return(message: discord.Message) -> None:
    """Clear AFK only after the 15s grace window, then print the ping summary."""
    record: Optional[AfkRecord] = bot.afk_state.get(message.author.id)
    if record is None:
        return
    elapsed: float = time.time() - record.since
    if elapsed < AFK_GRACE_SECONDS:
        return  # inside the grace window — AFK status stays
    bot.afk_state.pop(message.author.id, None)

    embed: discord.Embed = discord.Embed(
        title="👋 Welcome back!",
        description=(
            f"{message.author.mention}, your AFK status has been removed "
            f"(away for {humanize_seconds(elapsed)})."
        ),
        color=discord.Color.green(),
    )
    if record.pings:
        lines: List[str] = []
        for ping in record.pings[-10:]:
            ago: str = humanize_seconds(time.time() - ping.timestamp)
            content_text: str = (
                ping.content if len(ping.content) <= 80 else ping.content[:80] + "…"
            )
            lines.append(
                f"• **{ping.author}** — {ago} ago: {content_text or '*<no text>*'}"
            )
        overflow: str = (
            f"\n…and {len(record.pings) - 10} more." if len(record.pings) > 10 else ""
        )
        embed.add_field(
            name=f"📬 Pings while you were away ({len(record.pings)})",
            value=("\n".join(lines) + overflow)[:1024],
            inline=False,
        )
    else:
        embed.add_field(
            name="📬 Pings",
            value="Nobody pinged you while you were away.",
            inline=False,
        )
    await message.channel.send(
        embed=embed, allowed_mentions=discord.AllowedMentions.none()
    )


async def _handle_afk_mentions(message: discord.Message) -> None:
    """Log pings against AFK users and announce their AFK status."""
    notices: List[str] = []
    for user in message.mentions:
        record: Optional[AfkRecord] = bot.afk_state.get(user.id)
        if record is None or user.id == message.author.id:
            continue
        record.pings.append(
            AfkPing(
                author=str(message.author),
                content=message.content[:200],
                timestamp=time.time(),
                jump_url=message.jump_url,
            )
        )
        notices.append(
            f"💤 **{getattr(user, 'display_name', str(user))}** is AFK "
            f"({humanize_seconds(time.time() - record.since)} ago): {record.reason}"
        )
    if notices:
        await message.channel.send(
            "\n".join(notices[:5]), allowed_mentions=discord.AllowedMentions.none()
        )


# --------------------------------------------------------------------------- #
# Emoji tools
# --------------------------------------------------------------------------- #

CUSTOM_EMOJI_RE = re.compile(r"<(a?):([A-Za-z0-9_]{2,32}):(\d+)>")
STEAL_MAX: int = 5


@bot.hybrid_command(name="emojis", description="List this server's custom emojis")
@commands.guild_only()
async def emojis_cmd(ctx: commands.Context):
    guild_emojis = sorted(
        ctx.guild.emojis, key=lambda e: (e.animated, e.name.casefold())
    )
    if not guild_emojis:
        return await ctx.send("This server has no custom emojis.", ephemeral=True)

    lines: List[str] = []
    group: Optional[bool] = None
    for emoji in guild_emojis:
        if emoji.animated != group:
            group = emoji.animated
            lines.append(f"__**{'Animated' if group else 'Static'}**__")
        lines.append(f"{emoji} `:{emoji.name}:`")

    static_count: int = sum(1 for e in guild_emojis if not e.animated)
    animated_count: int = len(guild_emojis) - static_count
    limit: int = ctx.guild.emoji_limit
    pages = build_pages(
        f"Emojis in {ctx.guild.name}",
        lines,
        discord.Color.blurple(),
        per_page=16,
        footer=f"{static_count}/{limit} static · {animated_count}/{limit} animated",
    )
    await send_pages(ctx, pages)


@bot.hybrid_command(name="steal", description="Copy custom emojis into this server")
@app_commands.default_permissions(manage_emojis=True)
@commands.guild_only()
@app_commands.describe(
    emojis="One or more custom emojis to copy (up to 5)",
    name="Rename — only used when copying a single emoji",
)
async def steal_cmd(ctx: commands.Context, emojis: str, name: Optional[str] = None):
    if not member_has_perms(ctx.author, manage_emojis=True):
        return await ctx.send(
            "❌ You need the **Manage Expressions** permission.", ephemeral=True
        )
    if not ctx.guild.me.guild_permissions.manage_emojis:
        return await ctx.send(
            "❌ I need the **Manage Expressions** permission to add emojis.",
            ephemeral=True,
        )

    found: List[Tuple[str, str, str]] = CUSTOM_EMOJI_RE.findall(emojis)
    if not found:
        return await ctx.send(
            "❌ No custom emoji found there. Paste the emoji itself — standard Unicode "
            "emoji can't be copied.",
            ephemeral=True,
        )
    found = found[:STEAL_MAX]

    limit: int = ctx.guild.emoji_limit
    static_used: int = sum(1 for e in ctx.guild.emojis if not e.animated)
    animated_used: int = sum(1 for e in ctx.guild.emojis if e.animated)

    await ctx.defer(ephemeral=True)
    session: Optional[aiohttp.ClientSession] = bot.http_session
    results: List[str] = []

    for animated_flag, emoji_name, emoji_id in found:
        animated: bool = animated_flag == "a"
        if animated and animated_used >= limit:
            results.append(f"⚠️ `{emoji_name}` — animated slots are full ({limit}).")
            continue
        if not animated and static_used >= limit:
            results.append(f"⚠️ `{emoji_name}` — static slots are full ({limit}).")
            continue

        raw_name: str = name if (name and len(found) == 1) else emoji_name
        target_name: str = re.sub(r"[^A-Za-z0-9_]", "", raw_name)[:32]
        if len(target_name) < 2:
            target_name = "stolen_emoji"

        url: str = (
            f"https://cdn.discordapp.com/emojis/{emoji_id}.{'gif' if animated else 'png'}"
        )
        try:
            if session is None or session.closed:
                results.append(f"❌ `{emoji_name}` — no network session available.")
                continue
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10.0)
            ) as resp:
                if resp.status != 200:
                    results.append(f"❌ `{emoji_name}` — couldn't download it.")
                    continue
                payload: bytes = await resp.read()
            created = await ctx.guild.create_custom_emoji(
                name=target_name, image=payload, reason=f"Stolen by {ctx.author}"
            )
        except discord.HTTPException as exc:
            bot.log_error("steal", exc)
            results.append(f"❌ `{emoji_name}` — Discord rejected it ({exc.status}).")
            continue
        except Exception as exc:
            bot.log_error("steal", exc)
            results.append(f"❌ `{emoji_name}` — {type(exc).__name__}.")
            continue

        if animated:
            animated_used += 1
        else:
            static_used += 1
        results.append(f"✅ {created} added as `:{created.name}:`")

    await ctx.send("\n".join(results)[:2000], ephemeral=True)


# --------------------------------------------------------------------------- #
# Tags (custom saved responses)
# --------------------------------------------------------------------------- #


@bot.hybrid_group(
    name="tag", description="Saved snippets anyone can call up", fallback="show"
)
@commands.guild_only()
@app_commands.describe(name="Tag to show")
async def tag_group(ctx: commands.Context, name: Optional[str] = None):
    if ctx.invoked_subcommand is not None:
        return
    tags = (await bot.settings.fetch_settings(ctx.guild.id)).get("tags") or {}
    if not name:
        return await ctx.send(
            "Available tags: "
            + (", ".join(f"`{t}`" for t in sorted(tags)[:50]) or "none yet"),
            ephemeral=True,
        )
    content = tags.get(name.lower())
    if not content:
        return await ctx.send(f"❌ No tag called `{name}`.", ephemeral=True)
    await ctx.send(
        sanitize_mass_pings(str(content))[:2000],
        allowed_mentions=discord.AllowedMentions.none(),
    )


@tag_group.command(name="add", description="Create or overwrite a tag")
@app_commands.describe(name="Tag name", content="What it should say")
async def tag_add(ctx: commands.Context, name: str, *, content: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send(
            "❌ You need the **Manage Messages** permission.", ephemeral=True
        )
    tags = dict((await bot.settings.fetch_settings(ctx.guild.id)).get("tags") or {})
    if len(tags) >= 200 and name.lower() not in tags:
        return await ctx.send("❌ This server already has 200 tags.", ephemeral=True)
    tags[name.lower()[:50]] = content[:1800]
    saved = await bot.settings.push_fields(ctx.guild.id, {"tags": tags})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Tag `{name.lower()[:50]}` saved.", ephemeral=True
    )


@tag_group.command(name="remove", description="Delete a tag")
async def tag_remove(ctx: commands.Context, name: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send(
            "❌ You need the **Manage Messages** permission.", ephemeral=True
        )
    tags = dict((await bot.settings.fetch_settings(ctx.guild.id)).get("tags") or {})
    if tags.pop(name.lower(), None) is None:
        return await ctx.send(f"❌ No tag called `{name}`.", ephemeral=True)
    saved = await bot.settings.push_fields(ctx.guild.id, {"tags": tags})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Tag `{name.lower()}` deleted.", ephemeral=True
    )


@tag_group.command(name="list", description="List every tag in this server")
async def tag_list(ctx: commands.Context):
    tags = (await bot.settings.fetch_settings(ctx.guild.id)).get("tags") or {}
    if not tags:
        return await ctx.send(
            "ℹ️ No tags yet — add one with `/tag add`.", ephemeral=True
        )
    lines = [
        f"`{name}` — {str(content)[:100]}" for name, content in sorted(tags.items())
    ]
    pages = build_pages("Tags", lines, discord.Color.blurple(), per_page=10)
    await send_pages(ctx, pages, ephemeral=True)


# --------------------------------------------------------------------------- #
# Polls
# --------------------------------------------------------------------------- #

POLL_EMOJI: Tuple[str, ...] = (
    "1️⃣",
    "2️⃣",
    "3️⃣",
    "4️⃣",
    "5️⃣",
    "6️⃣",
    "7️⃣",
    "8️⃣",
    "9️⃣",
    "🔟",
)


@bot.hybrid_command(name="poll", description="Start a reaction poll")
@commands.guild_only()
@app_commands.describe(
    question="The question",
    options="Up to 10 options separated by | (leave empty for a yes/no poll)",
)
async def poll_cmd(
    ctx: commands.Context, question: str, *, options: Optional[str] = None
):
    choices = [o.strip() for o in (options or "").split("|") if o.strip()][:10]
    embed = discord.Embed(
        title="📊 " + question[:250],
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Poll by {ctx.author.display_name}")

    if choices:
        embed.description = "\n".join(
            f"{POLL_EMOJI[i]} {choice[:100]}" for i, choice in enumerate(choices)
        )
        reactions = POLL_EMOJI[: len(choices)]
    else:
        embed.description = "👍 yes · 👎 no · 🤷 not sure"
        reactions = ("👍", "👎", "🤷")

    message = await ctx.send(embed=embed)
    if ctx.interaction is not None:
        message = await ctx.interaction.original_response()
    for emoji in reactions:
        try:
            await message.add_reaction(emoji)
        except discord.DiscordException:
            break
