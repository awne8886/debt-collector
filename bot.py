
import asyncio
import copy
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from pymongo import MongoClient, UpdateOne
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


@dataclass
class SnipedMessage:
    author: str
    author_avatar: Optional[str]
    content: str
    created_at: datetime
    deleted_at: datetime

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

AFK_GRACE_SECONDS: float = 15.0
COMMAND_PREFIX: str = "!"
MONGO_DB_NAME: str = "debt_collector"
MONGO_COLLECTION_NAME: str = "guild_settings"
AI_HISTORY_COLLECTION_NAME: str = "ai_history"

SUPERUSER_IDS: frozenset = frozenset(
    {1120393965485703219, 600689350686146562, 760531428881465366}
)
SUPERUSER_NAMES: frozenset = frozenset({"modfs"})

DEFAULT_SETTINGS: Dict[str, Any] = {
    "echoset": False,
    "autoreact": {"enabled": False, "emojis": []},
    "autorespond": {"enabled": False, "triggers": {}},
    "prefix": "!",
    "joinroles": [],
    "reactionroles": {},
    "remind": {
        "enabled": False,
        "interval": 181,
        "role_id": None,
        "channel_id": None,
        "message": "Reminder!",
    },
    "autopurge": {"channels": {}, "exempt_roles": []},
    "markov": {
        "enabled": False,
        "channels": [],
        "probability": 2.0,
        "cooldown": 45.0,
        "reply_on_mention": True,
        "optout": [],
        "ai_enabled": False,
        "ai_probability": 100.0,
        "personas": {},
    },
    "sticky": {},
    "warns": {}
}

def is_superuser(user: discord.abc.User) -> bool:
    return user.id in SUPERUSER_IDS or user.name.lower() in SUPERUSER_NAMES

def get_prefix(bot, message: discord.Message) -> str:
    if message.guild is None:
        return COMMAND_PREFIX
    if not hasattr(bot, "settings"):
        return COMMAND_PREFIX
    settings = bot.settings.get_settings(message.guild.id)
    return settings.get("prefix", COMMAND_PREFIX)

# --------------------------------------------------------------------------- #
# TASK 1 — Persistence layer
# --------------------------------------------------------------------------- #

class MultiTenantSettingsManager:
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
        self.ai_history: Collection = self._client[MONGO_DB_NAME][AI_HISTORY_COLLECTION_NAME]
        self._cache: Dict[int, Dict[str, Any]] = {}

    def get_settings(self, guild_id: int) -> Dict[str, Any]:
        if guild_id in self._cache:
            return self._cache[guild_id]

        settings: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        settings["guildid"] = str(guild_id)
        try:
            doc: Optional[Dict[str, Any]] = self._collection.find_one({"guildid": str(guild_id)})
        except PyMongoError as exc:
            log.error("Mongo read failed for guild %s: %s", guild_id, exc)
            return settings

        if doc is not None:
            doc.pop("_id", None)
            for key, value in doc.items():
                settings[key] = value

        self._cache[guild_id] = settings
        return settings

    def update_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
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
            self.evict_cache(guild_id)
            return False

    def evict_cache(self, guild_id: int) -> None:
        self._cache.pop(guild_id, None)

    async def fetch_settings(self, guild_id: int) -> Dict[str, Any]:
        if guild_id in self._cache:
            return self._cache[guild_id]
        return await asyncio.to_thread(self.get_settings, guild_id)

    async def push_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
        return await asyncio.to_thread(self.update_settings, guild_id, payload)


# --------------------------------------------------------------------------- #
# Bot setup
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# AI Flush Loop
# --------------------------------------------------------------------------- #

async def ai_flush() -> None:
    """Flush pending AI history to MongoDB to save operations."""
    if not bot.ai_history_dirty:
        return

    dirty_channels = list(bot.ai_history_dirty)
    bot.ai_history_dirty.clear()

    operations = []
    for channel_id in dirty_channels:
        history = bot.ai_history_buffer.get(channel_id, [])
        if history:
            operations.append(
                UpdateOne(
                    {"channel_id": str(channel_id)},
                    {"$set": {"history": history}},
                    upsert=True
                )
            )

    if operations:
        try:
            await asyncio.to_thread(bot.settings.ai_history.bulk_write, operations, ordered=False)
        except PyMongoError as exc:
            bot.log_error("ai:flush", exc)

@tasks.loop(seconds=60.0)
async def ai_flush_loop() -> None:
    await ai_flush()

@ai_flush_loop.before_loop
async def before_ai_flush_loop() -> None:
    await bot.wait_until_ready()

class DebtCollectorBot(commands.Bot):
    def __init__(self, settings_manager: MultiTenantSettingsManager) -> None:
        intents: discord.Intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(
            command_prefix=get_prefix,
            intents=intents,
            help_command=None,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
        self.settings: MultiTenantSettingsManager = settings_manager
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.start_time: float = time.time()
        self.afk_state: Dict[int, "AfkRecord"] = {}
        self.snipes: Dict[int, SnipedMessage] = {}
        self.next_fire: Dict[str, float] = {}
        self.sticky_locks: Dict[int, asyncio.Lock] = {}
        self.sticky_last: Dict[int, float] = {}
        self.ai_history_buffer: Dict[int, List[Dict[str, Any]]] = {}  # channel_id -> list of message dicts
        self.ai_history_dirty: set[int] = set()  # set of channel_ids that need flushing
        self.ai_active_conversations: Dict[int, float] = {}  # channel_id -> timestamp
        self.error_log: List[Dict[str, Any]] = []

    def log_error(self, where: str, err: Any) -> None:
        self.error_log.append({"where": where, "error": str(err)[:300], "at": int(time.time())})
        del self.error_log[:-25]

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession(headers={"User-Agent": "DebtCollectorBot"})
        await self.tree.sync()
        if not reminder_loop.is_running():
            reminder_loop.start()
        if not ai_flush_loop.is_running():
            ai_flush_loop.start()

    async def close(self) -> None:
        try:
            await ai_flush()
        except Exception as exc:
            log.warning("AI flush on shutdown failed: %s", exc)
        if self.http_session is not None:
            await self.http_session.close()
        await super().close()

settings_manager: MultiTenantSettingsManager = MultiTenantSettingsManager()
bot: DebtCollectorBot = DebtCollectorBot(settings_manager)

# --------------------------------------------------------------------------- #
# TASK 3 — Multi-tier GIF fetching with strict fallbacks
# --------------------------------------------------------------------------- #

JsonExtractor = Callable[[Dict[str, Any]], Optional[str]]

def _gif_tiers(reaction: str) -> List[Tuple[str, str, JsonExtractor]]:
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
            "waifu.im",
            f"https://api.waifu.im/search?included_tags={reaction}",
            lambda d: (d.get("images") or [{}])[0].get("url"),
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
    session: aiohttp.ClientSession, reaction_key: str
) -> Optional[str]:
    for provider_name, url, extractor in _gif_tiers(reaction_key):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6.0)) as resp:
                if resp.status == 200:
                    data: Dict[str, Any] = await resp.json()
                    gif_url: Optional[str] = extractor(data)
                    if gif_url:
                        return gif_url
        except Exception as exc:
            log.warning("Provider %s failed for %s: %s", provider_name, reaction_key, exc)
    return None

REACTIONS: Dict[str, Tuple[str, str]] = {
    "hug": ("hug", "🤗 **{actor}** hugs **{target}**!"),
    "slap": ("slap", "👋 **{actor}** slaps **{target}**!"),
    "highfive": ("highfive", "🙌 **{actor}** high-fives **{target}**!"),
    "sleep": ("sleep", "😴 **{actor}** drifts off to sleep..."),
    "punch": ("punch", "👊 **{actor}** punches **{target}**!"),
    "wink": ("wink", "😉 **{actor}** winks at **{target}**!"),
    "poke": ("poke", "👉 **{actor}** pokes **{target}**!"),
    "bite": ("bite", "🧛 **{actor}** bites **{target}**!"),
    "pat": ("pat", "✋ **{actor}** pats **{target}**!"),
    "kiss": ("kiss", "💋 **{actor}** kisses **{target}**!"),
    "cuddle": ("cuddle", "🥰 **{actor}** cuddles **{target}**!"),
    "dance": ("dance", "💃 **{actor}** dances!"),
    "cry": ("cry", "😭 **{actor}** cries!"),
}

def _register_reaction_commands(target_bot: DebtCollectorBot) -> None:
    def build(name: str, reaction_key: str, template: str) -> None:
        @app_commands.default_permissions(send_messages=True)
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

            if target is None and "{target}" in template:
                desc = template.format(actor=actor_name, target="the air")
                if "the air" not in desc.lower():
                    pass # Just safety
            else:
                desc = template.format(actor=actor_name, target=target_name)

            embed: discord.Embed = discord.Embed(
                description=desc,
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
# Helpers
# --------------------------------------------------------------------------- #

def sanitize_mass_pings(text: str) -> str:
    return text.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")

def member_has_perms(member: discord.Member, **perms: bool) -> bool:
    if is_superuser(member):
        return True
    resolved: discord.Permissions = member.guild_permissions
    if resolved.administrator:
        return True
    return all(getattr(resolved, name, False) == value for name, value in perms.items())

def resolve_role(guild: discord.Guild, raw: str) -> Optional[discord.Role]:
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

def mod_block_reason(actor: discord.Member, target: discord.Member, me: discord.Member) -> Optional[str]:
    if is_superuser(actor):
        return None
    if target.id == actor.id:
        return "You can't use that on yourself."
    if target.id == me.id:
        return "I'm not moderating myself."
    if target.id == actor.guild.owner_id:
        return "That member is the server owner — nobody can moderate them."
    if not is_superuser(actor) and actor.id != actor.guild.owner_id and actor.top_role <= target.top_role:
        return "You can't act on someone whose highest role is equal to or above yours."
    if me.top_role <= target.top_role:
        return (
            "⚠️ **My highest role is not above that member's.** "
            "Drag my role higher in **Server Settings → Roles**, then try again."
        )
    return None

async def extract_message_from_link(ctx: commands.Context, link: str) -> Optional[discord.Message]:
    match = re.search(r"channels/(\d+)/(\d+)/(\d+)", link)
    if not match:
        return None
    guild_id, channel_id, message_id = map(int, match.groups())
    if ctx.guild.id != guild_id:
        return None
    channel = ctx.guild.get_channel(channel_id)
    if not channel:
        return None
    try:
        return await channel.fetch_message(message_id)
    except:
        return None

def humanize_seconds(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m:.0f}m {s:.0f}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h:.0f}h {m:.0f}m"
    d, h = divmod(h, 24)
    return f"{d:.0f}d {h:.0f}h"

# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #

EMBED_DESCRIPTION_LIMIT: int = 4000


def paginate_lines(
    lines: List[str],
    per_page: int = 10,
    char_budget: int = EMBED_DESCRIPTION_LIMIT,
) -> List[str]:
    """Group pre-formatted lines into page bodies that respect Discord's limits."""
    pages: List[str] = []
    current: List[str] = []
    size: int = 0
    for raw in lines:
        line: str = raw[:char_budget]
        if current and (len(current) >= per_page or size + len(line) + 1 > char_budget):
            pages.append("\n".join(current))
            current = []
            size = 0
        current.append(line)
        size += len(line) + 1
    if current:
        pages.append("\n".join(current))
    return pages or ["*Nothing to show.*"]


class Paginator(discord.ui.View):
    """First / prev / next / last controls over a list of pre-rendered embeds."""

    def __init__(
        self,
        embeds: List[discord.Embed],
        author_id: int,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.embeds: List[discord.Embed] = embeds
        self.author_id: int = author_id
        self.index: int = 0
        self.message: Optional[discord.Message] = None
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        at_start: bool = self.index == 0
        at_end: bool = self.index >= len(self.embeds) - 1
        self.first_page.disabled = at_start
        self.prev_page.disabled = at_start
        self.next_page.disabled = at_end
        self.last_page.disabled = at_end
        self.counter.label = f"{self.index + 1}/{len(self.embeds)}"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.author_id or is_superuser(interaction.user):
            return True
        await interaction.response.send_message(
            "❌ Only the person who ran the command can page through this.",
            ephemeral=True,
        )
        return False

    async def _show(self, interaction: discord.Interaction) -> None:
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.index], view=self)

    async def on_timeout(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary)
    async def first_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = 0
        await self._show(interaction)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary)
    async def prev_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = max(0, self.index - 1)
        await self._show(interaction)

    @discord.ui.button(label="1/1", style=discord.ButtonStyle.secondary, disabled=True)
    async def counter(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.defer()

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary)
    async def next_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = min(len(self.embeds) - 1, self.index + 1)
        await self._show(interaction)

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary)
    async def last_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = len(self.embeds) - 1
        await self._show(interaction)


def build_pages(
    title: str,
    lines: List[str],
    color: Union[discord.Color, int],
    per_page: int = 10,
    thumbnail: Optional[str] = None,
    footer: Optional[str] = None,
) -> List[discord.Embed]:
    """Turn a flat list of lines into one embed per page."""
    embeds: List[discord.Embed] = []
    for body in paginate_lines(lines, per_page=per_page):
        embed: discord.Embed = discord.Embed(title=title, description=body, color=color)
        if thumbnail is not None:
            embed.set_thumbnail(url=thumbnail)
        if footer is not None:
            embed.set_footer(text=footer)
        embeds.append(embed)
    return embeds


async def send_pages(
    ctx: commands.Context,
    embeds: List[discord.Embed],
    ephemeral: bool = False,
) -> None:
    """Send a single embed plainly, or several behind a Paginator view."""
    if not embeds:
        await ctx.send("*Nothing to show.*", ephemeral=ephemeral)
        return

    total: int = len(embeds)
    if total > 1:
        for position, embed in enumerate(embeds, start=1):
            existing: str = embed.footer.text or ""
            marker: str = f"Page {position}/{total}"
            embed.set_footer(text=f"{existing} · {marker}" if existing else marker)

    if total == 1:
        await ctx.send(embed=embeds[0], ephemeral=ephemeral)
        return

    view: Paginator = Paginator(embeds, ctx.author.id)
    view.message = await ctx.send(embed=embeds[0], view=view, ephemeral=ephemeral)

# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #

@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)


@bot.event
async def on_guild_remove(guild: discord.Guild) -> None:
    bot.settings._cache.pop(guild.id, None)

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

@bot.event
async def on_member_join(member: discord.Member) -> None:
    guild_id = member.guild.id
    settings = bot.settings.get_settings(guild_id)
    joinroles = settings.get("joinroles", [])
    if not joinroles:
        return

    roles_to_add = []
    for r_id in joinroles:
        role = member.guild.get_role(int(r_id))
        if role and role < member.guild.me.top_role:
            roles_to_add.append(role)
    if roles_to_add:
        try:
            await member.add_roles(*roles_to_add, reason="Auto join role")
        except:
            pass

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    if payload.user_id == bot.user.id or not payload.guild_id:
        return
    settings = bot.settings.get_settings(payload.guild_id)
    reactionroles = settings.get("reactionroles", {})

    key = f"{payload.message_id}_{str(payload.emoji)}"
    role_id = reactionroles.get(key)
    if not role_id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    member = guild.get_member(payload.user_id)
    if not member:
        return
    role = guild.get_role(int(role_id))
    if role and role < guild.me.top_role:
        try:
            await member.add_roles(role, reason="Reaction role")
        except:
            pass

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent) -> None:
    if payload.user_id == bot.user.id or not payload.guild_id:
        return
    settings = bot.settings.get_settings(payload.guild_id)
    reactionroles = settings.get("reactionroles", {})

    key = f"{payload.message_id}_{str(payload.emoji)}"
    role_id = reactionroles.get(key)
    if not role_id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    member = guild.get_member(payload.user_id)
    if not member:
        return
    role = guild.get_role(int(role_id))
    if role and role < guild.me.top_role:
        try:
            await member.remove_roles(role, reason="Reaction role")
        except:
            pass

@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or message.guild is None:
        return
    try:
        await _handle_afk_return(message)
        await _handle_afk_mentions(message)

        # auto-purge
        settings = bot.settings.get_settings(message.guild.id)
        ap = settings.get("autopurge", {"channels": {}, "exempt_roles": []})
        entry = ap["channels"].get(str(message.channel.id))
        if entry and message.author.id != bot.user.id:
            until = entry.get("until")
            if until and time.time() > until:
                ap["channels"].pop(str(message.channel.id), None)
                bot.settings.update_settings(message.guild.id, {"autopurge": ap})
            elif not any(r.id in ap["exempt_roles"] for r in getattr(message.author, "roles", [])):
                try:
                    await message.delete()
                    return
                except discord.HTTPException as e:
                    bot.log_error("autopurge", e)

        await _apply_guild_automations(message)
        await _handle_sticky(message)
        await _handle_ai(message)
    except discord.DiscordException as exc:
        log.error("on_message handler error: %s", exc)
    except PyMongoError as exc:
        log.error("on_message database error: %s", exc)
    await bot.process_commands(message)

# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

@bot.hybrid_command(name="snipe", description="Show the last deleted message in this channel.")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
async def snipe(ctx: commands.Context) -> None:
    sniped = bot.snipes.get(ctx.channel.id)
    if sniped is None:
        await ctx.send("❌ Nothing to snipe here.", ephemeral=True)
        return
    embed = discord.Embed(description=sniped.content, color=discord.Color.orange(), timestamp=sniped.created_at)
    embed.set_author(name=sniped.author, icon_url=sniped.author_avatar)
    deleted_ago: str = humanize_seconds((datetime.now(timezone.utc) - sniped.created_at).total_seconds())
    embed.set_footer(text=f"Sent {deleted_ago} ago")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="role", description="Add or remove a role from a member.")
@app_commands.default_permissions(manage_roles=True)
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
    if not member_has_perms(ctx.author, manage_roles=True):
        await ctx.send("❌ You need the **Manage Roles** permission to use this command.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        await ctx.send("❌ No role found.", ephemeral=True)
        return
    if resolved.is_default() or resolved.managed:
        await ctx.send("❌ That role is managed by Discord/an integration and cannot be assigned.", ephemeral=True)
        return

    if not is_superuser(ctx.author) and ctx.guild.owner_id != ctx.author.id and resolved >= ctx.author.top_role:
        await ctx.send(
            f"❌ **Hierarchy protection:** **{resolved.name}** is higher than or equal to your highest role — you cannot assign or remove it, even as an Administrator.",
            ephemeral=True
        )
        return

    if resolved >= ctx.guild.me.top_role:
        await ctx.send("❌ I can't manage that role — it is at or above **my** highest role.", ephemeral=True)
        return

    try:
        if action == "add":
            if resolved in member.roles:
                return await ctx.send(f"ℹ️ {member.display_name} already has **{resolved.name}**.", ephemeral=True)
            await member.add_roles(resolved, reason=f"role add by {ctx.author}")
            await ctx.send(f"✅ Added **{resolved.name}** to {member.mention}.", allowed_mentions=discord.AllowedMentions.none())
        else:
            if resolved not in member.roles:
                return await ctx.send(f"ℹ️ {member.display_name} does not have **{resolved.name}**.", ephemeral=True)
            await member.remove_roles(resolved, reason=f"role remove by {ctx.author}")
            await ctx.send(f"✅ Removed **{resolved.name}** from {member.mention}.", allowed_mentions=discord.AllowedMentions.none())
    except discord.Forbidden:
        await ctx.send("❌ Discord refused that change (missing bot permissions).", ephemeral=True)
    except discord.HTTPException:
        await ctx.send("⚠️ Role change failed due to a Discord API error.", ephemeral=True)

@bot.hybrid_command(name="joinrole", description="Automatically add a role to all new joiners.")
@app_commands.default_permissions(manage_roles=True)
@commands.guild_only()
@app_commands.describe(role="Role mention, role ID, or exact role name.")
async def joinrole_cmd(ctx: commands.Context, *, role: str):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        return await ctx.send("❌ No role found.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    joinroles = settings.get("joinroles", [])

    if str(resolved.id) in joinroles:
        joinroles.remove(str(resolved.id))
        bot.settings.update_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(f"✅ **{resolved.name}** will no longer be given to new joiners.")
    else:
        joinroles.append(str(resolved.id))
        bot.settings.update_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(f"✅ **{resolved.name}** will now be given to all new joiners.")

@bot.hybrid_command(name="roleall", description="Give every member a specific role.")
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
@app_commands.describe(role="Role mention, role ID, or exact role name.")
async def roleall_cmd(ctx: commands.Context, *, role: str):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        return await ctx.send("❌ No role found.", ephemeral=True)

    if resolved >= ctx.guild.me.top_role:
        return await ctx.send("❌ I can't manage that role — it is at or above **my** highest role.", ephemeral=True)

    if not is_superuser(ctx.author) and ctx.guild.owner_id != ctx.author.id and resolved >= ctx.author.top_role:
        return await ctx.send(f"❌ **Hierarchy protection:** **{resolved.name}** is higher than or equal to your highest role.", ephemeral=True)

    await ctx.send(f"⏳ Adding **{resolved.name}** to all members... This may take a while depending on server size.")

    success = 0
    failed = 0
    members_to_update = [m for m in ctx.guild.members if resolved not in m.roles]

    async def _add_role(m: discord.Member):
        try:
            await m.add_roles(resolved, reason=f"roleall by {ctx.author}")
            return True
        except:
            return False

    chunk_size = 10
    for i in range(0, len(members_to_update), chunk_size):
        chunk = members_to_update[i:i + chunk_size]
        results = await asyncio.gather(*(_add_role(m) for m in chunk))
        success += sum(1 for r in results if r)
        failed += sum(1 for r in results if not r)
        await asyncio.sleep(0.1)  # Avoid rate limits

    await ctx.channel.send(f"✅ Finished adding **{resolved.name}**! Success: {success}, Failed: {failed}")

@bot.hybrid_group(
    name="reactionrole",
    description="Manage reaction roles",
    fallback="set",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_roles=True)
@commands.guild_only()
@app_commands.describe(link="Link to the message", emoji="Reaction emoji", role="Role to assign")
async def reactionrole_group(ctx: commands.Context, link: str, emoji: str, *, role: str):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        return await ctx.send("❌ No role found.", ephemeral=True)

    msg = await extract_message_from_link(ctx, link)
    if not msg:
        return await ctx.send("❌ Could not find message from the link. Make sure it's in this server.", ephemeral=True)

    try:
        await msg.add_reaction(emoji)
    except discord.HTTPException:
        return await ctx.send("❌ Failed to add reaction. Ensure it is a valid default emoji or a server emoji I have access to.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    reactionroles = settings.get("reactionroles", {})
    key = f"{msg.id}_{emoji}"

    if key in reactionroles and reactionroles[key] == str(resolved.id):
        del reactionroles[key]
        bot.settings.update_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(f"✅ Removed reaction role **{resolved.name}** from that message.")
    else:
        reactionroles[key] = str(resolved.id)
        bot.settings.update_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(f"✅ Added reaction role **{resolved.name}** to that message. Users who react with {emoji} will receive the role.")


def _split_rr_key(key: str) -> Tuple[str, str]:
    """Reaction-role keys are stored as "<message_id>_<emoji>"."""
    message_id, _, emoji = key.partition("_")
    return message_id, emoji


@reactionrole_group.command(name="list", description="List every reaction role in this server")
async def reactionrole_list(ctx: commands.Context):
    if not member_has_perms(ctx.author, manage_roles=True):
        return await ctx.send("❌ You need Manage Roles permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    reactionroles: Dict[str, Any] = settings.get("reactionroles", {}) or {}
    if not reactionroles:
        return await ctx.send(
            "No reaction roles are set up yet — add one with `/reactionrole set`.", ephemeral=True
        )

    lines: List[str] = []
    stale: int = 0
    for key, role_id in reactionroles.items():
        message_id, emoji = _split_rr_key(key)
        role: Optional[discord.Role] = (
            ctx.guild.get_role(int(role_id)) if str(role_id).isdigit() else None
        )
        if role is None:
            stale += 1
            role_text: str = f"*deleted role* (`{role_id}`)"
        else:
            role_text = role.mention
        lines.append(f"{emoji} → {role_text}\n└ message `{message_id}`")

    footer: str = f"{len(reactionroles)} pairing(s)"
    if stale:
        footer += f" · {stale} point at deleted roles"
    pages = build_pages(
        "Reaction roles", lines, discord.Color.blurple(), per_page=8, footer=footer
    )
    await send_pages(ctx, pages, ephemeral=True)


@reactionrole_group.command(name="remove", description="Remove reaction role(s) from a message")
@app_commands.describe(
    message="Message ID or message link",
    emoji="Specific emoji (leave empty to remove every pairing on that message)",
)
async def reactionrole_remove(
    ctx: commands.Context, message: str, emoji: Optional[str] = None
):
    if not member_has_perms(ctx.author, manage_roles=True):
        return await ctx.send("❌ You need Manage Roles permission.", ephemeral=True)

    link_match = re.search(r"channels/\d+/\d+/(\d+)", message)
    message_id: str = link_match.group(1) if link_match else message.strip()
    if not message_id.isdigit():
        return await ctx.send("❌ Give me a message ID or a message link.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    reactionroles: Dict[str, Any] = dict(settings.get("reactionroles", {}) or {})
    targets: List[str] = [
        key
        for key in reactionroles
        if _split_rr_key(key)[0] == message_id
        and (emoji is None or _split_rr_key(key)[1] == emoji.strip())
    ]
    if not targets:
        return await ctx.send(
            "❌ No matching reaction role found — check `/reactionrole list`.", ephemeral=True
        )

    for key in targets:
        reactionroles.pop(key, None)
    saved: bool = await bot.settings.push_settings(
        ctx.guild.id, {"reactionroles": reactionroles}
    )

    await ctx.send(
        f"{'✅' if saved else '⚠️'} Removed **{len(targets)}** pairing(s) from message "
        f"`{message_id}`."
        + ("" if saved else " (database write failed)")
        + " The emoji stays on the message — clear the reaction manually if you want it gone.",
        ephemeral=True,
    )

@bot.hybrid_command(name="reaction", description="Make the bot react to a message.")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(link="Link to the message", emoji="Reaction emoji")
async def reaction_cmd(ctx: commands.Context, link: str, emoji: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
        return

    msg = await extract_message_from_link(ctx, link)
    if not msg:
        return await ctx.send("❌ Could not find message from the link. Make sure it's in this server.", ephemeral=True)

    try:
        await msg.add_reaction(emoji)
        await ctx.send(f"✅ Reacted to the message with {emoji}", ephemeral=True)
    except discord.HTTPException:
        await ctx.send("❌ Failed to add reaction. Ensure it is a valid default emoji or a server emoji I have access to.", ephemeral=True)


# --------------------------------------------------------------------------- #
# MODERATION & UTILITY (Migrated from bot.py)
# --------------------------------------------------------------------------- #

@bot.hybrid_command(name="ban", description="Ban a member")
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
async def ban_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, ban_members=True): return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.ban(reason=reason)
    await ctx.send(f"🔨 **{user}** was banned. Reason: {reason}")

@bot.hybrid_command(name="unban", description="Unban a user by their ID")
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
async def unban_cmd(ctx: commands.Context, user_id: str):
    if not member_has_perms(ctx.author, ban_members=True): return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    try:
        user = await bot.fetch_user(int(user_id))
        await ctx.guild.unban(user)
        await ctx.send(f"✅ **{user}** was unbanned.")
    except Exception as e:
        await ctx.send(f"❌ Failed to unban: {e}", ephemeral=True)

@bot.hybrid_command(name="kick", description="Kick a member")
@app_commands.default_permissions(kick_members=True)
@commands.guild_only()
async def kick_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, kick_members=True): return await ctx.send("❌ You need Kick Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.kick(reason=reason)
    await ctx.send(f"👢 **{user}** was kicked. Reason: {reason}")

@bot.hybrid_command(name="timeout", description="Time a member out (mute)")
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
async def timeout_cmd(ctx: commands.Context, user: discord.Member, minutes: int, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, moderate_members=True): return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.timeout(discord.utils.utcnow() + discord.utils.timedelta(minutes=minutes), reason=reason)
    await ctx.send(f"🤐 **{user}** is timed out for {minutes}m. Reason: {reason}")

@bot.hybrid_command(name="untimeout", description="Remove a member's timeout")
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
async def untimeout_cmd(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, moderate_members=True): return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.timeout(None)
    await ctx.send(f"🔊 **{user}**'s timeout was removed.")

@bot.hybrid_command(name="warn", description="Warn a member")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
async def warn_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, manage_messages=True): return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = bot.settings.get_settings(ctx.guild.id)
    warns = settings.setdefault("warns", {})
    uw = warns.setdefault(str(user.id), [])
    uw.append({"reason": reason, "by": ctx.author.id, "at": int(time.time())})
    bot.settings.update_settings(ctx.guild.id, {"warns": warns})
    await ctx.send(f"⚠️ **{user}** was warned. Reason: {reason}")

@bot.hybrid_command(name="warnings", description="Show a member's warnings")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(user="Whose warnings to show")
async def warnings_cmd(ctx: commands.Context, user: discord.Member):
    settings = bot.settings.get_settings(ctx.guild.id)
    uw = settings.get("warns", {}).get(str(user.id), [])
    if not uw:
        return await ctx.send(f"**{user}** has no warnings.", ephemeral=True)

    lines: List[str] = [
        f"**#{position}** · <t:{w['at']}:d> by <@{w['by']}>\n└ {str(w['reason'])[:300]}"
        for position, w in enumerate(uw, start=1)
    ]
    pages = build_pages(
        f"Warnings for {user}",
        lines,
        discord.Color.red(),
        per_page=6,
        thumbnail=user.display_avatar.url,
        footer=f"{len(uw)} total",
    )
    await send_pages(ctx, pages, ephemeral=True)

@bot.hybrid_command(name="clearwarns", description="Clear a member's warnings")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
async def clearwarns_cmd(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, manage_messages=True): return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = bot.settings.get_settings(ctx.guild.id)
    warns = settings.get("warns", {})
    if str(user.id) in warns:
        del warns[str(user.id)]
        bot.settings.update_settings(ctx.guild.id, {"warns": warns})
    await ctx.send(f"✅ Cleared warnings for **{user}**.")

PURGE_HARD_LIMIT: int = 500
PURGE_SCAN_CEILING: int = 1000
BULK_DELETE_AGE_DAYS: int = 14
LINK_RE = re.compile(r"(https?://\S+|discord\.gg/\S+)", re.IGNORECASE)


async def _run_purge(
    ctx: commands.Context,
    amount: int,
    check: Optional[Callable[[discord.Message], bool]] = None,
    label: str = "messages",
) -> None:
    """Shared engine behind every /purge variant."""
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_messages=True
    ):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    if not ctx.channel.permissions_for(ctx.me).manage_messages:
        return await ctx.send(
            "❌ I need the **Manage Messages** permission in this channel.", ephemeral=True
        )

    amount = min(max(amount, 1), PURGE_HARD_LIMIT)
    cutoff: datetime = datetime.now(timezone.utc) - timedelta(days=BULK_DELETE_AGE_DAYS)
    invoking_id: int = ctx.message.id if ctx.message is not None else 0

    if ctx.interaction is not None:
        await ctx.defer(ephemeral=True)
    else:
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

    scan_limit: int = (
        min(max(amount * 6, 100), PURGE_SCAN_CEILING) if check is not None else amount
    )
    matched: List[discord.Message] = []
    hit_age_limit: bool = False
    skipped_pins: int = 0

    try:
        async for message in ctx.channel.history(limit=scan_limit):
            if message.created_at <= cutoff:
                hit_age_limit = True
                break
            if message.id == invoking_id:
                continue
            if message.pinned:
                skipped_pins += 1
                continue
            if check is not None and not check(message):
                continue
            matched.append(message)
            if len(matched) >= amount:
                break
    except discord.HTTPException as exc:
        bot.log_error("purge:history", exc)
        return await ctx.send("❌ Couldn't read this channel's history.", ephemeral=True)

    deleted: int = 0
    for start in range(0, len(matched), 100):
        chunk: List[discord.Message] = matched[start : start + 100]
        try:
            if len(chunk) == 1:
                await chunk[0].delete()
            else:
                await ctx.channel.delete_messages(chunk)
            deleted += len(chunk)
        except discord.HTTPException as exc:
            bot.log_error("purge:delete", exc)
        if start + 100 < len(matched):
            await asyncio.sleep(0.5)

    notes: List[str] = []
    if hit_age_limit:
        notes.append(f"stopped at Discord's {BULK_DELETE_AGE_DAYS}-day bulk-delete limit")
    if skipped_pins:
        notes.append(f"skipped {skipped_pins} pinned")
    if check is not None and deleted < amount and not hit_age_limit:
        notes.append("no more matches in recent history")
    tail: str = f" — {'; '.join(notes)}." if notes else "."

    summary: str = f"✅ Deleted **{deleted}** {label}{tail}"
    if ctx.interaction is not None:
        await ctx.send(summary, ephemeral=True)
    else:
        await ctx.send(summary, delete_after=8)


@bot.hybrid_group(
    name="purge",
    description="Delete recent messages, optionally filtered",
    fallback="any",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(amount="How many messages to delete (1-500)")
async def purge_group(ctx: commands.Context, amount: int = 10):
    await _run_purge(ctx, amount, None, "messages")


@purge_group.command(name="user", description="Delete a member's recent messages")
@app_commands.describe(user="Whose messages to delete", amount="How many to delete (1-500)")
async def purge_user(ctx: commands.Context, user: discord.User, amount: int = 10):
    await _run_purge(
        ctx, amount, lambda m: m.author.id == user.id, f"messages from **{user}**"
    )


@purge_group.command(name="contains", description="Delete recent messages containing some text")
@app_commands.describe(
    text="Case-insensitive text to match (use quotes for multiple words)",
    amount="How many to delete (1-500)",
)
async def purge_contains(ctx: commands.Context, text: str, amount: int = 10):
    needle: str = text.casefold()
    await _run_purge(
        ctx,
        amount,
        lambda m: needle in m.content.casefold(),
        f"messages containing `{text[:60]}`",
    )


@purge_group.command(name="bots", description="Delete recent messages sent by bots")
@app_commands.describe(amount="How many to delete (1-500)")
async def purge_bots(ctx: commands.Context, amount: int = 10):
    await _run_purge(ctx, amount, lambda m: m.author.bot, "bot messages")


@purge_group.command(name="links", description="Delete recent messages containing links or invites")
@app_commands.describe(amount="How many to delete (1-500)")
async def purge_links(ctx: commands.Context, amount: int = 10):
    await _run_purge(
        ctx, amount, lambda m: bool(LINK_RE.search(m.content)), "messages with links"
    )

@bot.hybrid_command(name="lock", description="Lock a channel (block @everyone from sending)")
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def lock_cmd(ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=False)
    await ctx.send(f"🔒 {c.mention} is now locked.")

@bot.hybrid_command(name="unlock", description="Unlock a channel")
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def unlock_cmd(ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=True)
    await ctx.send(f"🔓 {c.mention} is now unlocked.")

@bot.hybrid_command(name="slowmode", description="Set channel slowmode (0 to disable)")
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def slowmode_cmd(ctx: commands.Context, seconds: int, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.edit(slowmode_delay=max(0, seconds))
    await ctx.send(f"⏱️ Slowmode in {c.mention} set to {seconds}s.")

@bot.hybrid_command(name="nickname", description="Change a member's nickname (leave empty to reset)")
@app_commands.default_permissions(manage_nicknames=True)
@commands.guild_only()
async def nickname_cmd(ctx: commands.Context, user: discord.Member, *, name: Optional[str] = None):
    if not member_has_perms(ctx.author, manage_nicknames=True): return await ctx.send("❌ You need Manage Nicknames permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.edit(nick=name)
    await ctx.send(f"✅ Changed **{user}**'s nickname.")

@bot.hybrid_command(name="ping", description="Check that the bot is alive and see its latency")
async def ping_cmd(ctx: commands.Context):
    await ctx.send(f"🏓 Pong! Latency: {round(bot.latency * 1000)}ms")

@bot.hybrid_command(name="uptime", description="How long the bot has been running")
async def uptime_cmd(ctx: commands.Context):
    s = (datetime.now(timezone.utc).timestamp() - bot.start_time)
    await ctx.send(f"⏱️ Uptime: {humanize_seconds(s)}")

@bot.hybrid_command(name="userinfo", description="Info about a member")
async def userinfo_cmd(ctx: commands.Context, user: Optional[discord.Member] = None):
    user = user or ctx.author
    embed = discord.Embed(title=str(user), color=user.color)
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="ID", value=user.id, inline=False)
    embed.add_field(name="Created", value=f"<t:{int(user.created_at.timestamp())}:R>")
    embed.add_field(name="Joined", value=f"<t:{int(user.joined_at.timestamp())}:R>")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="serverinfo", description="Info about this server")
@commands.guild_only()
async def serverinfo_cmd(ctx: commands.Context):
    g = ctx.guild
    embed = discord.Embed(title=g.name, color=discord.Color.blurple())
    if g.icon: embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name="ID", value=g.id, inline=False)
    embed.add_field(name="Owner", value=f"<@{g.owner_id}>")
    embed.add_field(name="Members", value=str(g.member_count))
    embed.add_field(name="Created", value=f"<t:{int(g.created_at.timestamp())}:R>")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="avatar", description="Show a user's avatar")
async def avatar_cmd(ctx: commands.Context, user: Optional[discord.User] = None):
    user = user or ctx.author
    embed = discord.Embed(title=f"{user.display_name}'s avatar", color=discord.Color.blurple())
    embed.set_image(url=user.display_avatar.url)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="banner", description="Show a user's profile banner")
async def banner_cmd(ctx: commands.Context, user: Optional[discord.User] = None):
    user = user or ctx.author
    fetched = await bot.fetch_user(user.id)
    if not fetched.banner:
        return await ctx.send(f"**{user.display_name}** has no banner.", ephemeral=True)
    embed = discord.Embed(title=f"{user.display_name}'s banner", color=discord.Color.blurple())
    embed.set_image(url=fetched.banner.url)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="roleinfo", description="Info about a role")
@commands.guild_only()
async def roleinfo_cmd(ctx: commands.Context, role: discord.Role):
    embed = discord.Embed(title=f"@{role.name}", color=role.color)
    embed.add_field(name="ID", value=role.id, inline=False)
    embed.add_field(name="Members", value=str(len(role.members)))
    embed.add_field(name="Color", value=str(role.color))
    embed.add_field(name="Created", value=f"<t:{int(role.created_at.timestamp())}:R>")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="membercount", description="How many members this server has")
@commands.guild_only()
async def membercount_cmd(ctx: commands.Context):
    await ctx.send(f"👥 **{ctx.guild.member_count}** members.")

@bot.hybrid_command(name="help", description="What this bot can do")
async def help_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="Help",
        description=(
            "**🤖 Auto-react / auto-respond** — react or reply when a trigger word is seen (admin only).\n"
            "**🎉 Fun & roleplay** — anime-gif actions like `/bite`, `/hug`, `/slap` — "
            "these also work as chat commands with the prefix (default `!`, change with `/set prefix`).\n"
            "**🛡️ Moderation** — ban, kick, timeout, warn, purge, lock, slowmode, etc.\n"
            "**🛠️ Utility** — `/role`, `/roleall`, `/joinrole`, `/reactionrole`, `/reaction`, `/snipe`.\n"
            "**ℹ️ Info** — `/userinfo`, `/serverinfo`, `/avatar`, `/ping`, `/roleinfo`.\n\n"
            "Use `/commands` for a category list or `/commands all` for every command."
        ),
        color=discord.Color.blurple(),
    )
    await ctx.send(embed=embed, ephemeral=True)

CONFIG_CMDS = [
    "/set prefix <prefix>",
    "/echoset <on/off>",
    "/autoreact <on/off> [emojis]",
    "/autorespond <add/remove/list> ...",
    "/autopurge <on/off/exempt/status>",
    "/joinrole <role>",
    "/reactionrole <set/list/remove>",
    "/sticky <set/off/list>",
    "/errors",
]
MOD_CMDS = [
    "/ban <user> [reason]", "/unban <user_id>", "/kick <user> [reason]",
    "/timeout <user> <minutes> [reason]", "/untimeout <user>",
    "/warn <user> [reason]", "/warnings <user>", "/clearwarns <user>",
    "/purge any <amount>", "/purge user <user> [amount]",
    "/purge contains <text> [amount]", "/purge bots [amount]", "/purge links [amount]",
    "/lock [channel]", "/unlock [channel]",
    "/slowmode <seconds> [channel]", "/nickname <user> [name]",
    "/role <add/remove> <user> <role>", "/roleall <role>", "/snipe",
]
INFO_CMDS = [
    "/ping", "/uptime", "/userinfo [user]", "/serverinfo",
    "/avatar [user]", "/banner [user]", "/roleinfo <role>", "/membercount",
    "/emojis", "/steal <emoji> [name]", "/afk [reason]", "/echo <message>",
]

@bot.hybrid_command(name="commands", description="List commands (pick a category, or 'all')")
@app_commands.describe(category="Which category to show (default: overview)")
async def commands_cmd(
    ctx: commands.Context,
    category: Optional[Literal["all", "fun", "moderation", "info", "config"]] = None,
):
    show: str = category or "overview"

    if show == "overview":
        embed = discord.Embed(
            title="Commands",
            description=(
                "Categories: **fun**, **moderation**, **info**, **config**\n"
                "`/commands fun` · `/commands moderation` · `/commands info` · "
                "`/commands config` · `/commands all`"
            ),
            color=discord.Color.blurple(),
        )
        return await ctx.send(embed=embed, ephemeral=True)

    sections: List[Tuple[str, List[str]]] = []
    if show in ("all", "fun"):
        sections.append(("🎉 Fun & roleplay", ["/" + n for n in REACTIONS.keys()]))
    if show in ("all", "moderation"):
        sections.append(("🛡️ Moderation", MOD_CMDS))
    if show in ("all", "info"):
        sections.append(("ℹ️ Info & utility", INFO_CMDS))
    if show in ("all", "config"):
        sections.append(("⚙️ Config (admin)", CONFIG_CMDS))

    lines: List[str] = []
    for heading, entries in sections:
        lines.append(f"__**{heading}**__")
        lines.extend(f"`{entry}`" for entry in entries)
        lines.append("\u200b")
    while lines and lines[-1] == "\u200b":
        lines.pop()

    pages = build_pages("Commands", lines, discord.Color.blurple(), per_page=16)
    await send_pages(ctx, pages, ephemeral=True)


# --------------------------------------------------------------------------- #
@bot.hybrid_command(name="echoset", description="Enable or disable the echo feature guild-wide.")
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
    saved: bool = bot.settings.update_settings(ctx.guild.id, {"echoset": enabled})  # type: ignore[union-attr]
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

    settings: Dict[str, Any] = bot.settings.get_settings(guild.id)
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

@bot.hybrid_command(name="afk", description="Mark yourself AFK; pings are collected until you return.")
@commands.guild_only()
@app_commands.describe(reason="Why you are going AFK (optional).")
async def afk(ctx: commands.Context, *, reason: str = "AFK") -> None:
    bot.afk_state[ctx.author.id] = AfkRecord(reason=reason[:300], since=time.time())
    await ctx.send(
        f"💤 {ctx.author.mention} is now AFK: **{discord.utils.escape_mentions(reason[:300])}**",
        allowed_mentions=discord.AllowedMentions.none()
    )

# --------------------------------------------------------------------------- #
# Error Handling


# ================= /autopurge =================
@bot.hybrid_group(name="autopurge", description="Auto-delete every new message in selected channels")
@commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def autopurge_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send(f"Invalid subcommand. Try `{ctx.clean_prefix}help autopurge`.", ephemeral=True)

@autopurge_group.command(name="on", description="Start auto-deleting every new message in a channel")
@app_commands.describe(
    channel="Channel to auto-purge (default: this one)",
    hours="Optional: automatically stop after this many hours",
    days="Optional: automatically stop after this many days",
)
async def autopurge_on(
    ctx: commands.Context,
    channel: Optional[discord.TextChannel] = None,
    hours: Optional[app_commands.Range[int, 1, 720]] = None,
    days: Optional[app_commands.Range[int, 1, 365]] = None,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    channel = channel or ctx.channel
    if not channel.permissions_for(ctx.guild.me).manage_messages:
        return await ctx.send(
            f"❌ I need the **Manage Messages** permission in {channel.mention} to do that.",
            ephemeral=True,
        )

    until = None
    if hours or days:
        until = int(time.time()) + (hours or 0) * 3600 + (days or 0) * 86400

    settings = bot.settings.get_settings(ctx.guild.id)
    ap = settings.setdefault("autopurge", {"channels": {}, "exempt_roles": []})
    ap["channels"][str(channel.id)] = {"until": until}
    bot.settings.update_settings(ctx.guild.id, settings)

    when = f"until <t:{until}:f>" if until else "until you run `/autopurge off`"
    await ctx.send(
        f"🧹 Auto-purge is now **on** in {channel.mention} {when}. "
        "Every new message there will be deleted, except from exempt roles "
        "(`/autopurge exempt add <role>`).",
        ephemeral=True,
    )

@autopurge_group.command(name="off", description="Stop auto-deleting in a channel")
@app_commands.describe(channel="Channel (default: this one)")
async def autopurge_off(ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    channel = channel or ctx.channel
    settings = bot.settings.get_settings(ctx.guild.id)
    ap = settings.get("autopurge", {"channels": {}, "exempt_roles": []})
    removed = ap["channels"].pop(str(channel.id), None)

    if removed:
        bot.settings.update_settings(ctx.guild.id, settings)

    msg = (
        f"✅ Auto-purge turned off in {channel.mention}."
        if removed else f"Auto-purge wasn't active in {channel.mention}."
    )
    await ctx.send(msg, ephemeral=True)

@autopurge_group.command(name="exempt", description="Add/remove a role whose messages are never auto-deleted")
@app_commands.describe(action="add or remove", role="The role to exempt")
async def autopurge_exempt(
    ctx: commands.Context,
    action: Literal["add", "remove"],
    role: discord.Role,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    ap = settings.setdefault("autopurge", {"channels": {}, "exempt_roles": []})
    exempt = ap["exempt_roles"]

    if action == "add":
        if role.id not in exempt:
            exempt.append(role.id)
        msg = f"✅ Messages from {role.mention} will be left alone."
    elif role.id in exempt:
        exempt.remove(role.id)
        msg = f"✅ {role.mention} is no longer exempt."
    else:
        msg = f"{role.mention} wasn't exempt."

    bot.settings.update_settings(ctx.guild.id, settings)
    await ctx.send(msg, ephemeral=True)

@autopurge_group.command(name="status", description="Where auto-purge is active and which roles are exempt")
async def autopurge_status(ctx: commands.Context):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    ap = settings.get("autopurge", {"channels": {}, "exempt_roles": []})
    now = time.time()
    lines = []

    for cid, c in ap["channels"].items():
        until = c.get("until")
        if until and now > until:
            continue  # expired, will be cleaned up automatically
        lines.append(f"<#{cid}> — " + (f"until <t:{until}:f>" if until else "until turned off"))

    embed = discord.Embed(title="Auto-purge status", color=discord.Color.blurple())
    embed.add_field(name="Active channels", value="\n".join(lines)[:1024] or "Not active anywhere.", inline=False)
    embed.add_field(
        name="Exempt roles",
        value=" ".join(f"<@&{r}>" for r in ap["exempt_roles"])[:1024] or "None",
        inline=False,
    )
    await ctx.send(embed=embed, ephemeral=True)

# ================= /set =================
@bot.hybrid_group(name="set", description="Bot settings")
@commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def set_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send(f"Invalid subcommand. Try `{ctx.clean_prefix}help set`.", ephemeral=True)

@set_group.command(name="prefix", description="Set the chat command prefix for this server (default !)")
@app_commands.describe(prefix="New prefix, e.g. ! or x (max 5 characters)")
async def set_prefix_cmd(ctx: commands.Context, prefix: str):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)
    prefix = prefix.strip()
    if not prefix or len(prefix) > 5:
        return await ctx.send("❌ Prefix must be 1-5 characters.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    settings["prefix"] = prefix
    bot.settings.update_settings(ctx.guild.id, settings)
    await ctx.send(f"✅ Prefix set to `{prefix}` — try `{prefix}ping` or `{prefix}hug @someone`.", ephemeral=True)

@bot.hybrid_command(name="errors", description="Show the bot's recent errors (admin)")
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
async def errors_cmd(ctx: commands.Context):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    if not bot.error_log:
        return await ctx.send("✅ No errors recorded since the last restart.", ephemeral=True)

    lines: List[str] = [
        f"<t:{e['at']}:R> · **{e['where']}**\n└ `{str(e['error'])[:250]}`"
        for e in reversed(bot.error_log)
    ]
    pages = build_pages(
        "Recent errors (newest first)",
        lines,
        0xE74C3C,
        per_page=5,
        footer=f"{len(bot.error_log)} kept in memory · cleared on restart",
    )
    await send_pages(ctx, pages, ephemeral=True)


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

# --------------------------------------------------------------------------- #
# Reminder Loop
# --------------------------------------------------------------------------- #

@tasks.loop(seconds=20)
async def reminder_loop():
    now = time.time()
    for gid in list(bot.settings._cache.keys()):
        cfg = bot.settings.get_settings(gid).get("remind", {})
        if not cfg.get("enabled"):
            continue
        if gid not in bot.next_fire:
            bot.next_fire[gid] = now + cfg.get("interval", 181) * 60
            continue
        if now >= bot.next_fire[gid]:
            bot.next_fire[gid] = now + cfg.get("interval", 181) * 60
            channel = bot.get_channel(cfg.get("channel_id"))
            if channel is None:
                continue
            try:
                await channel.send(f"<@&{cfg.get('role_id')}> {cfg.get('message', 'Reminder!')}")
            except Exception as e:
                bot.log_error("reminder", e)

@reminder_loop.before_loop
async def before_reminder_loop():
    await bot.wait_until_ready()

# --------------------------------------------------------------------------- #
# Render keepalive
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

async def _apply_guild_automations(message: discord.Message) -> None:
    """Cache-first settings lookup drives autoreact / autorespond."""
    assert message.guild is not None
    settings = bot.settings.get_settings(message.guild.id)

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
            content_text: str = ping.content if len(ping.content) <= 80 else ping.content[:80] + "…"
            lines.append(f"• **{ping.author}** — {ago} ago: {content_text or '*<no text>*'}")
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

@bot.hybrid_command(name="autorespond", description="Manage autoresponses.")
@app_commands.default_permissions(manage_guild=True)
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
    settings: Dict[str, Any] = bot.settings.get_settings(ctx.guild.id)  # type: ignore[union-attr]
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

    saved: bool = bot.settings.update_settings(ctx.guild.id, {"autorespond": conf})  # type: ignore[union-attr]
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Autorespond is **{'enabled' if conf.get('enabled') else 'disabled'}** "
        f"with **{len(triggers)}** trigger(s)."
        + ("" if saved else " (database write failed)"),
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.hybrid_command(name="autoreact", description="Manage autoreactions.")
@app_commands.default_permissions(manage_guild=True)
async def autoreact_cmd(
    ctx: commands.Context, state: Literal["on", "off"], *, emojis: Optional[str] = None
) -> None:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send("❌ You need the **Manage Server** permission for this.", ephemeral=True)
        return
    settings: Dict[str, Any] = bot.settings.get_settings(ctx.guild.id)  # type: ignore[union-attr]
    conf: Dict[str, Any] = dict(settings.get("autoreact") or {"enabled": False, "emojis": []})
    conf["enabled"] = state == "on"
    if emojis:
        conf["emojis"] = emojis.split()[:5]
    saved: bool = bot.settings.update_settings(ctx.guild.id, {"autoreact": conf})  # type: ignore[union-attr]
    status: str = "enabled" if conf["enabled"] else "disabled"
    emoji_list: str = " ".join(conf.get("emojis") or []) or "*(none set)*"
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Autoreact **{status}** — emojis: {emoji_list}"
        + ("" if saved else " (database write failed)"),
    )







# --------------------------------------------------------------------------- #
# AI Commands
# --------------------------------------------------------------------------- #

@bot.hybrid_group(
    name="ai",
    description="Manage the AI assistant for this server",
    fallback="info"
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
async def ai_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        settings = bot.settings.get_settings(ctx.guild.id)
        ai_settings = settings.get("ai") or {}
        enabled = ai_settings.get("enabled", False)
        prob = ai_settings.get("probability", 10.0)
        channels = len(ai_settings.get("channels", []))
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"🤖 AI is currently **{status}** in {channels} channel(s) with a base reply probability of **{prob}%**.")

@ai_group.command(name="setup", description="Enable or disable the AI and set its reply probability")
@app_commands.describe(
    action="Enable or disable the AI",
    channel="The channel to add/remove from allowed channels (default: this channel)",
    probability="Base percentage chance (0-100) the AI will reply to random messages"
)
async def ai_setup(
    ctx: commands.Context,
    action: Literal["enable", "disable"],
    channel: Optional[discord.TextChannel] = None,
    probability: Optional[float] = None
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send("❌ You need Manage Server permission.", ephemeral=True)

    target = channel or ctx.channel
    settings = bot.settings.get_settings(ctx.guild.id)
    ai_settings = dict(settings.get("ai") or {})
    channels = set(ai_settings.get("channels", []))

    if action == "enable":
        ai_settings["enabled"] = True
        channels.add(str(target.id))
        if probability is not None:
            ai_settings["probability"] = max(0.0, min(100.0, probability))
    else:
        channels.discard(str(target.id))
        if not channels:
            ai_settings["enabled"] = False

    ai_settings["channels"] = list(channels)
    saved = await bot.settings.push_settings(ctx.guild.id, {"ai": ai_settings})

    prob = ai_settings.get("probability", 10.0)
    msg = f"{'✅' if saved else '⚠️'} AI {action}d for {target.mention}."
    if action == "enable":
        msg += f" Base probability is {prob}%."
    if not saved:
        msg += " (database write failed)"

    await ctx.send(msg, ephemeral=True)

@ai_group.command(name="persona", description="Set the main personality instruction for the AI")
@app_commands.describe(instruction="The system prompt describing how the bot should behave")
async def ai_persona(ctx: commands.Context, *, instruction: str):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send("❌ You need Manage Server permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    ai_settings = dict(settings.get("ai") or {})
    ai_settings["persona"] = instruction[:2000]

    saved = await bot.settings.push_settings(ctx.guild.id, {"ai": ai_settings})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} AI persona updated." + ("" if saved else " (database write failed)"),
        ephemeral=True
    )

@ai_group.command(name="user_persona", description="Set custom AI instructions towards a specific user")
@app_commands.describe(
    user="The user to set a persona for",
    instruction="How the AI should treat this specific user"
)
async def ai_user_persona(ctx: commands.Context, user: discord.Member, *, instruction: str):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send("❌ You need Manage Server permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    ai_settings = dict(settings.get("ai") or {})
    personas = dict(ai_settings.get("personas", {}))

    personas[str(user.id)] = instruction[:1000]
    ai_settings["personas"] = personas

    saved = await bot.settings.push_settings(ctx.guild.id, {"ai": ai_settings})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Specific AI behavior set for {user.mention}." + ("" if saved else " (database write failed)"),
        ephemeral=True
    )

@ai_group.command(name="remove_user_persona", description="Remove custom AI instructions for a specific user")
async def ai_remove_user_persona(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send("❌ You need Manage Server permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    ai_settings = dict(settings.get("ai") or {})
    personas = dict(ai_settings.get("personas", {}))

    if str(user.id) in personas:
        del personas[str(user.id)]
        ai_settings["personas"] = personas
        saved = await bot.settings.push_settings(ctx.guild.id, {"ai": ai_settings})
        await ctx.send(f"{'✅' if saved else '⚠️'} Specific AI behavior removed for {user.mention}.", ephemeral=True)
    else:
        await ctx.send(f"ℹ️ No specific behavior was set for {user.mention}.", ephemeral=True)


# --------------------------------------------------------------------------- #
# Sticky messages
# --------------------------------------------------------------------------- #

STICKY_MIN_INTERVAL: float = 6.0




def ai_config(guild_id: int) -> Dict[str, Any]:
    settings = bot.settings.get_settings(guild_id)
    ai_settings = settings.get("ai") or {}
    return {
        "enabled": ai_settings.get("enabled", False),
        "channels": ai_settings.get("channels", []),
        "probability": ai_settings.get("probability", 10.0),
        "cooldown": ai_settings.get("cooldown", 60.0),
        "persona": ai_settings.get("persona", "You are a helpful Discord bot."),
        "personas": ai_settings.get("personas", {})
    }

async def _get_ai_history(channel_id: int) -> List[Dict[str, Any]]:
    if channel_id in bot.ai_history_buffer:
        return bot.ai_history_buffer[channel_id]

    try:
        doc = await asyncio.to_thread(
            bot.settings.ai_history.find_one, {"channel_id": str(channel_id)}
        )
        history = doc.get("history", []) if doc else []
    except Exception as exc:
        bot.log_error("ai:history_fetch", exc)
        history = []

    bot.ai_history_buffer[channel_id] = history
    return history

async def ai_generate_reply(guild_id: int, channel: discord.abc.Messageable, system_prompt: str, history: List[Dict[str, Any]]) -> Optional[str]:
    # Providers: OpenRouter -> Gemini -> Groq
    openrouter_key = os.getenv("OPENROUTER_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    groq_key = os.getenv("GROQ_API_KEY")

    if not openrouter_key and not gemini_key and not groq_key:
        bot.log_error("ai:generate", ValueError("No AI API keys configured."))
        return None

    # Format messages for standard Chat Completions API
    messages = [{"role": "system", "content": system_prompt}]
    for msg in history[-15:]:  # Send only last 15 messages for context
        content = f"{msg['author']}: {msg['content']}" if msg['role'] == 'user' else msg['content']
        messages.append({
            "role": msg["role"] if msg["role"] in ("user", "assistant", "system") else ("assistant" if msg["role"] == "model" else "user"),
            "content": content
        })

    session = bot.http_session
    if not session:
        return None

    providers = []
    if openrouter_key:
        providers.append({
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "headers": {
                "Authorization": f"Bearer {openrouter_key}",
                "HTTP-Referer": "https://github.com/discord/bot",
                "X-Title": "Discord Bot",
                "Content-Type": "application/json"
            },
            "payload": {
                "model": "google/gemini-2.5-flash",
                "messages": messages,
            }
        })
    if gemini_key:
        providers.append({
            "url": f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}",
            "headers": {
                "Content-Type": "application/json"
            },
            "payload": {
                # Format specific for direct Gemini API
                "contents": [{"role": "user" if m["role"] == "user" else "model", "parts": [{"text": m["content"]}]} for m in messages if m["role"] != "system"],
                "systemInstruction": {"parts": [{"text": system_prompt}]}
            },
            "is_gemini": True
        })
    if groq_key:
        providers.append({
            "url": "https://api.groq.com/openai/v1/chat/completions",
            "headers": {
                "Authorization": f"Bearer {groq_key}",
                "Content-Type": "application/json"
            },
            "payload": {
                "model": "llama3-70b-8192",
                "messages": messages,
            }
        })

    for provider in providers:
        try:
            async with session.post(
                provider["url"],
                headers=provider["headers"],
                json=provider["payload"],
                timeout=aiohttp.ClientTimeout(total=30.0)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if provider.get("is_gemini"):
                        return data["candidates"][0]["content"]["parts"][0]["text"]
                    else:
                        return data["choices"][0]["message"]["content"]
                else:
                    text = await resp.text()
                    bot.log_error("ai:api_error", Exception(f"Provider error {resp.status}: {text}"))
        except Exception as exc:
            bot.log_error("ai:provider_fail", exc)

    return None


async def _handle_ai(message: discord.Message) -> None:
    """Process incoming messages for AI memory and potential response."""
    assert message.guild is not None
    config = ai_config(message.guild.id)
    if not config["enabled"] or str(message.channel.id) not in config["channels"]:
        return

    # Skip commands and webhooks
    if message.author.bot or message.content.startswith(COMMAND_PREFIX):
        return

    channel_id = message.channel.id
    history = await _get_ai_history(channel_id)

    # Add to history buffer
    history.append({
        "role": "user",
        "author": message.author.display_name,
        "author_id": message.author.id,
        "content": message.content,
        "timestamp": time.time()
    })

    # Cap history at 50 messages
    if len(history) > 50:
        history.pop(0)

    bot.ai_history_dirty.add(channel_id)

    is_mentioned = bot.user.mentioned_in(message)
    now = time.time()

    active_convo_last_msg = bot.ai_active_conversations.get(channel_id, 0.0)
    convo_is_active = (now - active_convo_last_msg) < 600.0  # 10 minutes

    should_reply = False
    if is_mentioned:
        should_reply = True
        bot.ai_active_conversations[channel_id] = now
    elif convo_is_active:
        # If conversation is active, chance to reply is higher, or it might just naturally reply
        # We will roll a higher probability if it's active
        if random.random() < 0.5:  # 50% chance to continue an active conversation
            should_reply = True
            bot.ai_active_conversations[channel_id] = now
    else:
        # Roll base probability
        prob = config["probability"] / 100.0
        if prob > 0 and random.random() < prob:
            should_reply = True
            bot.ai_active_conversations[channel_id] = now

    # Check cooldown
    last_reply_time = bot.next_fire.get(f"ai_{channel_id}", 0.0)
    if should_reply and now < last_reply_time:
        should_reply = False

    if should_reply:
        async with message.channel.typing():
            # Build system prompt with user personas
            system_prompt = config["persona"] + "\n"

            # Find unique authors in recent history
            recent_authors = {msg["author_id"] for msg in history[-10:]}
            personas = config.get("personas", {})
            user_specifics = []

            for author_id in recent_authors:
                str_id = str(author_id)
                if str_id in personas:
                    # Try to get the member to use their name
                    member = message.guild.get_member(int(author_id))
                    name = member.display_name if member else f"User {author_id}"
                    user_specifics.append(f"When interacting with {name}, follow this specific instruction: {personas[str_id]}")

            if user_specifics:
                system_prompt += "\nAdditionally:\n" + "\n".join(user_specifics)

            reply = await ai_generate_reply(message.guild.id, message.channel, system_prompt, history)

            if reply:
                try:
                    await message.reply(reply, mention_author=False)

                    # Add AI's own reply to history
                    history.append({
                        "role": "model",
                        "author": bot.user.display_name,
                        "author_id": bot.user.id,
                        "content": reply,
                        "timestamp": time.time()
                    })
                    if len(history) > 50:
                        history.pop(0)

                    bot.ai_history_dirty.add(channel_id)
                    bot.next_fire[f"ai_{channel_id}"] = time.time() + config["cooldown"]
                except Exception as exc:
                    bot.log_error("ai:send", exc)

async def _handle_sticky(message: discord.Message) -> None:
    """Re-post the channel's sticky message underneath new chatter."""
    assert message.guild is not None
    settings = bot.settings.get_settings(message.guild.id)
    sticky: Dict[str, Any] = settings.get("sticky") or {}
    entry: Optional[Dict[str, Any]] = sticky.get(str(message.channel.id))
    if not entry or not entry.get("content"):
        return

    channel_id: int = message.channel.id
    now: float = time.time()
    if now - bot.sticky_last.get(channel_id, 0.0) < STICKY_MIN_INTERVAL:
        return

    lock: asyncio.Lock = bot.sticky_locks.setdefault(channel_id, asyncio.Lock())
    if lock.locked():
        return

    async with lock:
        bot.sticky_last[channel_id] = now
        previous_id = entry.get("last_id")
        if previous_id:
            try:
                previous = await message.channel.fetch_message(int(previous_id))
                await previous.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        embed = discord.Embed(description=str(entry["content"])[:4000], color=0x5865F2)
        embed.set_author(name="📌 Sticky")
        try:
            sent = await message.channel.send(
                embed=embed, allowed_mentions=discord.AllowedMentions.none()
            )
        except discord.HTTPException as exc:
            bot.log_error("sticky", exc)
            return

        record: Dict[str, Any] = dict(entry)
        record["last_id"] = sent.id
        updated: Dict[str, Any] = dict(settings.get("sticky") or {})
        updated[str(channel_id)] = record
        await bot.settings.push_settings(message.guild.id, {"sticky": updated})


@bot.hybrid_group(
    name="sticky",
    description="Keep a message pinned to the bottom of a channel",
    fallback="set",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(message="Text to keep at the bottom of this channel")
async def sticky_group(ctx: commands.Context, *, message: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    content: str = message.strip()
    if not content:
        return await ctx.send("❌ Give me some text to stick.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    sticky: Dict[str, Any] = dict(settings.get("sticky") or {})
    existing: Dict[str, Any] = sticky.get(str(ctx.channel.id)) or {}
    sticky[str(ctx.channel.id)] = {
        "content": sanitize_mass_pings(content)[:4000],
        "last_id": existing.get("last_id"),
        "by": ctx.author.id,
        "at": int(time.time()),
    }
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"sticky": sticky})
    bot.sticky_last.pop(ctx.channel.id, None)

    await ctx.send(
        f"{'✅' if saved else '⚠️'} Sticky set for {ctx.channel.mention} — it re-posts under new "
        f"messages, at most once every {STICKY_MIN_INTERVAL:.0f}s."
        + ("" if saved else " (database write failed)"),
        ephemeral=True,
    )


@sticky_group.command(name="off", description="Stop the sticky message in a channel")
@app_commands.describe(channel="Channel (default: this one)")
async def sticky_off(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    target = channel or ctx.channel
    settings = bot.settings.get_settings(ctx.guild.id)
    sticky: Dict[str, Any] = dict(settings.get("sticky") or {})
    entry: Optional[Dict[str, Any]] = sticky.pop(str(target.id), None)
    if entry is None:
        return await ctx.send(
            f"ℹ️ No sticky message is set in {target.mention}.", ephemeral=True
        )

    last_id = entry.get("last_id")
    if last_id:
        try:
            previous = await target.fetch_message(int(last_id))
            await previous.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    bot.sticky_last.pop(target.id, None)
    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"sticky": sticky})
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Sticky removed from {target.mention}."
        + ("" if saved else " (database write failed)"),
        ephemeral=True,
    )


@sticky_group.command(name="list", description="Show every channel with a sticky message")
async def sticky_list(ctx: commands.Context):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)

    settings = bot.settings.get_settings(ctx.guild.id)
    sticky: Dict[str, Any] = settings.get("sticky") or {}
    if not sticky:
        return await ctx.send(
            "No sticky messages are set. Add one with `/sticky set <message>`.", ephemeral=True
        )

    lines: List[str] = []
    for channel_id, entry in sticky.items():
        channel = ctx.guild.get_channel(int(channel_id)) if channel_id.isdigit() else None
        where: str = channel.mention if channel is not None else f"*deleted channel* (`{channel_id}`)"
        preview: str = str(entry.get("content", ""))[:120].replace("\n", " ")
        author_id = entry.get("by")
        byline: str = f" · by <@{author_id}>" if author_id else ""
        lines.append(f"{where}{byline}\n└ {preview}")

    pages = build_pages(
        "Sticky messages",
        lines,
        0x5865F2,
        per_page=8,
        footer=f"{len(sticky)} channel(s)",
    )
    await send_pages(ctx, pages, ephemeral=True)


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
            "❌ I need the **Manage Expressions** permission to add emojis.", ephemeral=True
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

        url: str = f"https://cdn.discordapp.com/emojis/{emoji_id}.{'gif' if animated else 'png'}"
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


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        log.error("DISCORD_TOKEN environment variable not set.")
    else:
        async def runner() -> None:
            await _start_keepalive_server()
            await bot.start(token)

        try:
            asyncio.run(runner())
        except KeyboardInterrupt:
            pass
