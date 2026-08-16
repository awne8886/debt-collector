"""
Debt Collector — multi-tenant Discord bot (discord.py 2.x, hybrid slash + "!" prefix).

Platform  : Render (ephemeral filesystem — ALL persistence lives in MongoDB Atlas).
Storage   : MongoDB Atlas via pymongo (mongodb+srv:// DNS seedlist, pooled).
Cache     : In-memory per-guild settings dictionary to prevent DB thrashing
            on high-frequency on_message loops.

Required environment variables (set in the Render dashboard):
    DISCORD_TOKEN — the bot token.
    MONGO_URI     — mongodb+srv:// connection string for Atlas.
    PORT          — (optional) set automatically by Render Web Services;
                    when present a tiny keepalive HTTP server is started.

Privileged intents required in the Discord Developer Portal:
    - MESSAGE CONTENT INTENT (prefix commands, AFK, snipe, echo)
    - SERVER MEMBERS INTENT  (role management, mention resolution)
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log: logging.Logger = logging.getLogger("debt-collector")

# --------------------------------------------------------------------------- #
# Global constants
# --------------------------------------------------------------------------- #

COMMAND_PREFIX: str = "!"
AFK_GRACE_SECONDS: float = 15.0
HTTP_TIMEOUT_SECONDS: float = 6.0
MONGO_DB_NAME: str = "debt_collector"
MONGO_COLLECTION_NAME: str = "guild_settings"

# Global superusers: bypass ALL hierarchy and permission checks everywhere.
SUPERUSER_IDS: frozenset = frozenset(
    {1120393965485703219, 600689350686146562, 760531428881465366}
)
SUPERUSER_NAMES: frozenset = frozenset({"modfs"})

# Canonical per-guild settings schema (guildid is injected on read/write).
DEFAULT_SETTINGS: Dict[str, Any] = {
    "echoset": False,
    "autoreact": {"enabled": False, "emojis": []},
    "autorespond": {"enabled": False, "triggers": {}},
}


def is_superuser(user: discord.abc.User) -> bool:
    """True when the user is a global superuser (bypasses every check)."""
    return user.id in SUPERUSER_IDS or user.name.lower() in SUPERUSER_NAMES


# --------------------------------------------------------------------------- #
# TASK 1 — Persistence layer
# --------------------------------------------------------------------------- #


class MultiTenantSettingsManager:
    """MongoDB-backed, memory-cached, multi-tenant guild settings store.

    Reads hit the in-memory cache first; only cache misses touch Atlas.
    Writes update the cache atomically and $set-upsert the document keyed
    by the stringified guild id (``guildid``).
    """

    def __init__(self) -> None:
        mongo_uri: Optional[str] = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        self._client: MongoClient = MongoClient(
            mongo_uri,
            maxPoolSize=50,
            serverSelectionTimeoutMS=10_000,
            retryWrites=True,
        )
        self._collection: Collection = self._client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
        self._cache: Dict[int, Dict[str, Any]] = {}

    # -- synchronous core (safe to call from a worker thread) --------------- #

    def get_settings(self, guild_id: int) -> Dict[str, Any]:
        """Return the guild's settings; cache hit -> RAM, miss -> Atlas."""
        if guild_id in self._cache:
            return self._cache[guild_id]

        settings: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        settings["guildid"] = str(guild_id)
        try:
            doc: Optional[Dict[str, Any]] = self._collection.find_one(
                {"guildid": str(guild_id)}
            )
        except PyMongoError as exc:
            log.error("Mongo read failed for guild %s: %s", guild_id, exc)
            return settings  # do NOT cache a failed read

        if doc is not None:
            doc.pop("_id", None)  # strip internal Mongo id
            for key, value in doc.items():
                settings[key] = value

        self._cache[guild_id] = settings
        return settings

    def update_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
        """Atomically update the cache and $set-upsert the payload to Atlas."""
        current: Dict[str, Any] = self.get_settings(guild_id)
        current.update(copy.deepcopy(payload))
        current["guildid"] = str(guild_id)
        self._cache[guild_id] = current
        try:
            self._collection.update_one(
                {"guildid": str(guild_id)},
                {"$set": {**payload, "guildid": str(guild_id)}},
                upsert=True,
            )
            return True
        except PyMongoError as exc:
            log.error("Mongo upsert failed for guild %s: %s", guild_id, exc)
            # Cache may now diverge from Atlas — evict so the next read re-syncs.
            self.evict_cache(guild_id)
            return False

    def evict_cache(self, guild_id: int) -> None:
        """Safely pop a guild's configuration out of RAM."""
        self._cache.pop(guild_id, None)

    # -- async wrappers (never block the event loop on a cache miss) -------- #

    async def fetch_settings(self, guild_id: int) -> Dict[str, Any]:
        if guild_id in self._cache:
            return self._cache[guild_id]
        return await asyncio.to_thread(self.get_settings, guild_id)

    async def push_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
        return await asyncio.to_thread(self.update_settings, guild_id, payload)


# --------------------------------------------------------------------------- #
# TASK 3 — Multi-tier GIF fetching with strict fallbacks
# --------------------------------------------------------------------------- #

JsonExtractor = Callable[[Dict[str, Any]], Optional[str]]


def _gif_tiers(reaction: str) -> List[Tuple[str, str, JsonExtractor]]:
    """Rigid priority order: OtakuGIFs -> PurrBot -> Gifukai -> nekos.*"""
    return [
        (
            "OtakuGIFs",
            f"https://api.otakugifs.xyz/gif?reaction={reaction}",
            lambda d: d.get("url"),
        ),
        (
            "PurrBot",
            f"https://api.purrbot.site/v2/img/sfw/{reaction}/gif",
            lambda d: None if d.get("error") else d.get("link"),
        ),
        (
            "Gifukai",
            f"https://gifukai.com/api/gif/{reaction}",
            lambda d: d.get("url") or d.get("gif") or d.get("link"),
        ),
        (
            "nekos.life",
            f"https://nekos.life/api/v2/img/{reaction}",
            lambda d: d.get("url"),
        ),
        (
            "nekos.best",
            f"https://nekos.best/api/v2/{reaction}",
            lambda d: (d.get("results") or [{}])[0].get("url"),
        ),
    ]


async def fetch_reaction_gif(
    session: aiohttp.ClientSession, reaction: str
) -> Optional[str]:
    """Walk the tier list; any timeout/DNS/HTTP/parse error falls through
    instantly to the next tier without breaking the interaction."""
    for tier_name, url, extract in _gif_tiers(reaction):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
            ) as resp:
                if resp.status != 200:
                    log.warning(
                        "GIF tier %s returned HTTP %s for '%s'",
                        tier_name, resp.status, reaction,
                    )
                    continue
                data: Any = await resp.json(content_type=None)
                if not isinstance(data, dict):
                    continue
                gif: Optional[str] = extract(data)
                if isinstance(gif, str) and gif.startswith("http"):
                    return gif
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            KeyError,
            IndexError,
            TypeError,
            ValueError,
        ) as exc:
            log.warning("GIF tier %s failed for '%s': %s", tier_name, reaction, exc)
            continue
    return None


# --------------------------------------------------------------------------- #
# In-memory state records (AFK + snipe live in RAM by design)
# --------------------------------------------------------------------------- #


@dataclass
class AfkPing:
    author: str
    content: str
    timestamp: float
    jump_url: str


@dataclass
class AfkRecord:
    reason: str
    since: float
    pings: List[AfkPing] = field(default_factory=list)


@dataclass
class SnipedMessage:
    author: str
    author_avatar: Optional[str]
    content: str
    created_at: datetime
    deleted_at: datetime


def humanize_seconds(seconds: float) -> str:
    total: int = int(max(seconds, 0))
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def sanitize_mass_pings(message: str) -> str:
    """Neutralise @everyone / @here with a zero-width space."""
    return message.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")


# --------------------------------------------------------------------------- #
# TASK 2 — Bot architecture (hybrid prefix "!" + slash, universal visibility)
# --------------------------------------------------------------------------- #


class DebtCollectorBot(commands.Bot):
    """Hybrid bot: every command is registered as BOTH a slash command and a
    sequentially-parsed "!" prefix command. Slash commands stay visible to
    everyone; restricted commands enforce permissions at runtime and reply
    with a clean (ephemeral where possible) error instead of being hidden."""

    def __init__(self, settings_manager: MultiTenantSettingsManager) -> None:
        intents: discord.Intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(
            command_prefix=commands.when_mentioned_or(COMMAND_PREFIX),
            intents=intents,
            case_insensitive=True,
            allowed_mentions=discord.AllowedMentions(
                everyone=False, roles=False, users=True, replied_user=True
            ),
        )
        self.settings: MultiTenantSettingsManager = settings_manager
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.afk_state: Dict[int, AfkRecord] = {}
        self.snipes: Dict[int, SnipedMessage] = {}

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession()
        try:
            synced: List[app_commands.AppCommand] = await self.tree.sync()
            log.info("Synced %d application commands.", len(synced))
        except discord.DiscordException as exc:
            log.error("Slash command sync failed: %s", exc)

    async def close(self) -> None:
        if self.http_session is not None and not self.http_session.closed:
            await self.http_session.close()
        await super().close()


settings_manager: MultiTenantSettingsManager = MultiTenantSettingsManager()
bot: DebtCollectorBot = DebtCollectorBot(settings_manager)


def member_has_perms(member: discord.Member, **perms: bool) -> bool:
    """Guild-permission gate with the global superuser override baked in."""
    if is_superuser(member):
        return True
    resolved: discord.Permissions = member.guild_permissions
    if resolved.administrator:
        return True
    return all(getattr(resolved, name, False) == value for name, value in perms.items())


def resolve_role(guild: discord.Guild, raw: str) -> Optional[discord.Role]:
    """Robust role parsing: mention -> ID -> exact name -> case-insensitive name."""
    raw = raw.strip()
    mention_match: Optional[re.Match] = re.fullmatch(r"<@&(\d+)>", raw)
    if mention_match is not None:
        return guild.get_role(int(mention_match.group(1)))
    if raw.isdigit():
        by_id: Optional[discord.Role] = guild.get_role(int(raw))
        if by_id is not None:
            return by_id
    for role in guild.roles:
        if role.name == raw:
            return role
    lowered: str = raw.casefold()
    for role in guild.roles:
        if role.name.casefold() == lowered:
            return role
    return None


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (%s) — %d guild(s).", bot.user, getattr(bot.user, "id", "?"), len(bot.guilds))


@bot.event
async def on_guild_remove(guild: discord.Guild) -> None:
    settings_manager.evict_cache(guild.id)


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
            content: str = ping.content if len(ping.content) <= 80 else ping.content[:80] + "…"
            lines.append(f"• **{ping.author}** — {ago} ago: {content or '*<no text>*'}")
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


async def _apply_guild_automations(message: discord.Message) -> None:
    """Cache-first settings lookup drives autoreact / autorespond."""
    assert message.guild is not None
    settings: Dict[str, Any] = await bot.settings.fetch_settings(message.guild.id)

    autoreact: Dict[str, Any] = settings.get("autoreact") or {}
    if autoreact.get("enabled") and autoreact.get("emojis"):
        for emoji in list(autoreact["emojis"])[:5]:
            try:
                await message.add_reaction(emoji)
            except (discord.HTTPException, discord.Forbidden, TypeError):
                continue

    autorespond: Dict[str, Any] = settings.get("autorespond") or {}
    if autorespond.get("enabled") and autorespond.get("triggers"):
        lowered: str = message.content.casefold()
        for trigger, response in dict(autorespond["triggers"]).items():
            if trigger.casefold() in lowered:
                await message.channel.send(
                    str(response)[:2000],
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                break  # one response per message


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or message.guild is None:
        return
    try:
        await _handle_afk_return(message)
        await _handle_afk_mentions(message)
        await _apply_guild_automations(message)
    except discord.DiscordException as exc:
        log.error("on_message handler error: %s", exc)
    except PyMongoError as exc:
        log.error("on_message database error: %s", exc)
    await bot.process_commands(message)


@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if message.guild is None or message.author.bot:
        return
    try:
        bot.snipes[message.channel.id] = SnipedMessage(
            author=str(message.author),
            author_avatar=(
                message.author.display_avatar.url
                if message.author.display_avatar
                else None
            ),
            content=message.content or "",
            created_at=message.created_at,
            deleted_at=datetime.now(timezone.utc),
        )
    except discord.DiscordException as exc:
        log.error("on_message_delete error: %s", exc)


# --------------------------------------------------------------------------- #
# TASK 4.1 — AFK system
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="afk", description="Mark yourself AFK; pings are collected until you return.")
@commands.guild_only()
@app_commands.describe(reason="Why you are going AFK (optional).")
async def afk(ctx: commands.Context, *, reason: str = "AFK") -> None:
    bot.afk_state[ctx.author.id] = AfkRecord(reason=reason[:300], since=time.time())
    await ctx.send(
        f"💤 {ctx.author.mention} is now AFK: **{discord.utils.escape_mentions(reason[:300])}**",
        allowed_mentions=discord.AllowedMentions.none(),
    )


# --------------------------------------------------------------------------- #
# TASK 4.2 — Snipe system
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="snipe", description="Show the last deleted message in this channel.")
@commands.guild_only()
async def snipe(ctx: commands.Context) -> None:
    sniped: Optional[SnipedMessage] = bot.snipes.get(ctx.channel.id)
    if sniped is None:
        await ctx.send("🔎 There is nothing to snipe in this channel.", ephemeral=True)
        return
    embed: discord.Embed = discord.Embed(
        description=sniped.content or "*<no text content — possibly an attachment or embed>*",
        color=discord.Color.orange(),
        timestamp=sniped.created_at,
    )
    embed.set_author(name=sniped.author, icon_url=sniped.author_avatar or None)
    deleted_ago: str = humanize_seconds(
        (datetime.now(timezone.utc) - sniped.deleted_at).total_seconds()
    )
    embed.set_footer(text=f"Deleted {deleted_ago} ago • sent")
    await ctx.send(embed=embed)


# --------------------------------------------------------------------------- #
# TASK 4.3 — Role management with hierarchy protection
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="role", description="Add or remove a role from a member.")
@commands.guild_only()
@app_commands.describe(
    action="Whether to add or remove the role.",
    member="The member to modify.",
    role="Role mention, role ID, or exact role name.",
)
async def role_cmd(
    ctx: commands.Context,
    action: Literal["add", "remove"],
    member: discord.Member,
    *,
    role: str,
) -> None:
    guild: Optional[discord.Guild] = ctx.guild
    if guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("❌ This command only works inside a server.", ephemeral=True)
        return
    actor: discord.Member = ctx.author

    if not member_has_perms(actor, manage_roles=True):
        await ctx.send(
            "❌ You need the **Manage Roles** permission to use this command.",
            ephemeral=True,
        )
        return

    resolved: Optional[discord.Role] = resolve_role(guild, role)
    if resolved is None:
        await ctx.send(
            f"❌ No role found matching `{discord.utils.escape_mentions(role[:100])}` "
            "(tried mention, ID, and name).",
            ephemeral=True,
        )
        return
    if resolved.is_default() or resolved.managed:
        await ctx.send("❌ That role is managed by Discord/an integration and cannot be assigned.", ephemeral=True)
        return

    # Hierarchy protection: even Administrators may not touch roles at or
    # above their own top role. Global superusers bypass entirely.
    if (
        not is_superuser(actor)
        and guild.owner_id != actor.id
        and resolved >= actor.top_role
    ):
        await ctx.send(
            f"❌ **Hierarchy protection:** {resolved.mention} is higher than or equal "
            "to your highest role — you cannot assign or remove it, even as an Administrator.",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return

    me: Optional[discord.Member] = guild.me
    if me is None or resolved >= me.top_role:
        await ctx.send(
            "❌ I can't manage that role — it is at or above **my** highest role.",
            ephemeral=True,
        )
        return

    try:
        if action == "add":
            if resolved in member.roles:
                await ctx.send(f"ℹ️ {member.display_name} already has **{resolved.name}**.", ephemeral=True)
                return
            await member.add_roles(resolved, reason=f"role add by {actor} ({actor.id})")
            verb: str = "Added"
            preposition: str = "to"
        else:
            if resolved not in member.roles:
                await ctx.send(f"ℹ️ {member.display_name} does not have **{resolved.name}**.", ephemeral=True)
                return
            await member.remove_roles(resolved, reason=f"role remove by {actor} ({actor.id})")
            verb = "Removed"
            preposition = "from"
        await ctx.send(
            f"✅ {verb} {resolved.mention} {preposition} {member.mention}.",
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.Forbidden:
        await ctx.send("❌ Discord refused that change (missing bot permissions).", ephemeral=True)
    except discord.HTTPException as exc:
        log.error("Role change failed: %s", exc)
        await ctx.send("⚠️ Role change failed due to a Discord API error.", ephemeral=True)


# --------------------------------------------------------------------------- #
# TASK 4.4 — Advanced echo system
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="echoset", description="Enable or disable the echo feature guild-wide.")
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
        await ctx.send(f"📢 Echo is now **{'enabled' if enabled else 'disabled'}** for this server.")
    else:
        await ctx.send(
            "⚠️ Echo setting could not be persisted to the database — please try again.",
            ephemeral=True,
        )


@bot.hybrid_command(name="echo", description="Make the bot say something, optionally in another channel.")
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
        await ctx.send(f"❌ I don't have permission to send messages in {target.mention}.", ephemeral=True)
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


# --------------------------------------------------------------------------- #
# Guild automation configuration (autoreact / autorespond schema support)
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="autoreact", description="Configure automatic reactions on every message.")
@commands.guild_only()
@app_commands.describe(
    state="Turn autoreact on or off.",
    emojis="Space-separated emojis to react with (max 5, optional).",
)
async def autoreact_cmd(
    ctx: commands.Context, state: Literal["on", "off"], *, emojis: Optional[str] = None
) -> None:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send("❌ You need the **Manage Server** permission for this.", ephemeral=True)
        return
    settings: Dict[str, Any] = await bot.settings.fetch_settings(ctx.guild.id)  # type: ignore[union-attr]
    conf: Dict[str, Any] = dict(settings.get("autoreact") or {"enabled": False, "emojis": []})
    conf["enabled"] = state == "on"
    if emojis:
        conf["emojis"] = emojis.split()[:5]
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"autoreact": conf})  # type: ignore[union-attr]
    status: str = "enabled" if conf["enabled"] else "disabled"
    emoji_list: str = " ".join(conf.get("emojis") or []) or "*(none set)*"
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Autoreact **{status}** — emojis: {emoji_list}"
        + ("" if saved else " (database write failed)"),
    )


@bot.hybrid_command(name="autorespond", description="Configure automatic trigger → response replies.")
@commands.guild_only()
@app_commands.describe(
    action="on/off to toggle, add/remove to manage triggers.",
    trigger="The trigger word or phrase (for add/remove).",
    response="The reply to send when the trigger is seen (for add).",
)
async def autorespond_cmd(
    ctx: commands.Context,
    action: Literal["on", "off", "add", "remove"],
    trigger: Optional[str] = None,
    *,
    response: Optional[str] = None,
) -> None:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send("❌ You need the **Manage Server** permission for this.", ephemeral=True)
        return
    settings: Dict[str, Any] = await bot.settings.fetch_settings(ctx.guild.id)  # type: ignore[union-attr]
    conf: Dict[str, Any] = dict(settings.get("autorespond") or {"enabled": False, "triggers": {}})
    triggers: Dict[str, str] = dict(conf.get("triggers") or {})

    if action in ("on", "off"):
        conf["enabled"] = action == "on"
    elif action == "add":
        if not trigger or not response:
            await ctx.send("❌ `add` needs both a trigger and a response.", ephemeral=True)
            return
        triggers[trigger[:100]] = response[:500]
        conf["triggers"] = triggers
    else:  # remove
        if not trigger or trigger not in triggers:
            await ctx.send("❌ That trigger does not exist.", ephemeral=True)
            return
        triggers.pop(trigger, None)
        conf["triggers"] = triggers

    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"autorespond": conf})  # type: ignore[union-attr]
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Autorespond is **{'enabled' if conf.get('enabled') else 'disabled'}** "
        f"with **{len(triggers)}** trigger(s)."
        + ("" if saved else " (database write failed)"),
        allowed_mentions=discord.AllowedMentions.none(),
    )


# --------------------------------------------------------------------------- #
# TASK 3 — Reaction / GIF commands (multi-tier fallback)
# --------------------------------------------------------------------------- #

# command name -> (API reaction keyword, flavour template)
REACTIONS: Dict[str, Tuple[str, str]] = {
    "hug": ("hug", "🤗 **{actor}** hugs **{target}**!"),
    "slap": ("slap", "👋 **{actor}** slaps **{target}**!"),
    "highfive": ("highfive", "🙌 **{actor}** high-fives **{target}**!"),
    "sleep": ("sleep", "😴 **{actor}** drifts off to sleep..."),
    "punch": ("punch", "👊 **{actor}** punches **{target}**!"),
    "wink": ("wink", "😉 **{actor}** winks at **{target}**!"),
    "poke": ("poke", "👉 **{actor}** pokes **{target}**!"),
}


def _register_reaction_commands(target_bot: DebtCollectorBot) -> None:
    """Dynamically register every reaction command as a hybrid command."""

    def build(name: str, reaction_key: str, template: str) -> None:
        async def callback(
            ctx: commands.Context, target: Optional[discord.Member] = None
        ) -> None:
            await ctx.defer()
            session: Optional[aiohttp.ClientSession] = target_bot.http_session
            gif_url: Optional[str] = None
            if session is not None and not session.closed:
                gif_url = await fetch_reaction_gif(session, reaction_key)
            actor_name: str = getattr(ctx.author, "display_name", str(ctx.author))
            target_name: str = target.display_name if target is not None else "the air"
            embed: discord.Embed = discord.Embed(
                description=template.format(actor=actor_name, target=target_name),
                color=discord.Color.random(),
            )
            if gif_url is not None:
                embed.set_image(url=gif_url)
            else:
                embed.set_footer(
                    text="All GIF providers are currently unavailable — try again soon."
                )
            await ctx.send(embed=embed)

        callback.__name__ = name
        decorated = app_commands.describe(target="Who to target (optional).")(callback)
        target_bot.hybrid_command(
            name=name, description=f"Send a {name} GIF (with multi-API fallback)."
        )(decorated)

    for command_name, (reaction_key, template) in REACTIONS.items():
        build(command_name, reaction_key, template)


_register_reaction_commands(bot)


# --------------------------------------------------------------------------- #
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
    if isinstance(error, commands.CheckFailure):
        await ctx.send("❌ You don't have permission to use this command.", ephemeral=True)
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
        await ctx.send("❌ I'm missing the Discord permissions to do that.", ephemeral=True)
        return

    log.exception("Unhandled error in command '%s'", ctx.command, exc_info=error)
    try:
        await ctx.send("⚠️ Something went wrong while running that command.", ephemeral=True)
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
    message: str = "⚠️ Something went wrong while running that command."
    if isinstance(error, app_commands.CheckFailure):
        message = "❌ You don't have permission to use this command."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.DiscordException:
        pass


# --------------------------------------------------------------------------- #
# Render keepalive (only when Render injects PORT, i.e. Web Service mode)
# --------------------------------------------------------------------------- #


async def _start_keepalive_server() -> None:
    port: Optional[str] = os.getenv("PORT")
    if not port:
        return
    from aiohttp import web

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="OK")

    app: web.Application = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner: web.AppRunner = web.AppRunner(app)
    await runner.setup()
    site: web.TCPSite = web.TCPSite(runner, "0.0.0.0", int(port))
    await site.start()
    log.info("Keepalive HTTP server listening on port %s.", port)


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #


async def main() -> None:
    token: Optional[str] = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN environment variable is not set.")
    async with bot:
        await _start_keepalive_server()
        await bot.start(token)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown requested — exiting cleanly.")
