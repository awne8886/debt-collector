
import asyncio
import copy
import logging
import os
import re
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
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
MONGO_DB_NAME: str = "debt_collector"
MONGO_COLLECTION_NAME: str = "guild_settings"

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
        self.sniped_messages: Dict[int, discord.Message] = {}
        self.next_fire: Dict[str, float] = {}
        self.error_log: List[Dict[str, Any]] = []

    def log_error(self, where: str, err: Any) -> None:
        self.error_log.append({"where": where, "error": str(err)[:300], "at": int(time.time())})
        del self.error_log[:-25]

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession()
        await self.tree.sync()
        if not reminder_loop.is_running():
            reminder_loop.start()

    async def close(self) -> None:
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
# Events
# --------------------------------------------------------------------------- #

@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if message.author.bot or message.guild is None:
        return
    bot.sniped_messages[message.channel.id] = message

@bot.event
async def on_member_join(member: discord.Member) -> None:
    guild_id = member.guild.id
    settings = await bot.settings.fetch_settings(guild_id)
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
    settings = await bot.settings.fetch_settings(payload.guild_id)
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
    settings = await bot.settings.fetch_settings(payload.guild_id)
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
    if message.guild is None or message.author.bot:
        return
    settings = bot.settings.get_settings(message.guild.id)
    content = message.content.lower()

    # auto-purge
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

    # autoreact
    autoreact = settings.get("autoreact", {})
    if autoreact.get("enabled"):
        for trigger, emoji in autoreact.get("triggers", {}).items():
            if trigger in content:
                try:
                    await message.add_reaction(emoji)
                except discord.HTTPException as e:
                    bot.log_error(f"autoreact '{trigger}'", e)
        # legacy autoreact emojis (from main.py config style)
        for emoji in autoreact.get("emojis", []):
            try:
                await message.add_reaction(emoji)
            except discord.HTTPException:
                pass

    # autorespond
    autorespond = settings.get("autorespond", {})
    if autorespond.get("enabled"):
        for trigger, response in autorespond.get("triggers", {}).items():
            if trigger in content:
                try:
                    await message.channel.send(response)
                except discord.HTTPException as e:
                    bot.log_error(f"autorespond '{trigger}'", e)
                break

    await bot.process_commands(message)

# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

@bot.hybrid_command(name="snipe", description="Show the last deleted message in this channel.")
@commands.guild_only()
async def snipe(ctx: commands.Context) -> None:
    sniped: Optional[discord.Message] = bot.sniped_messages.get(ctx.channel.id)
    if sniped is None:
        await ctx.send("❌ Nothing to snipe here.", ephemeral=True)
        return
    embed = discord.Embed(description=sniped.content, color=discord.Color.orange(), timestamp=sniped.created_at)
    embed.set_author(name=sniped.author.display_name, icon_url=sniped.author.display_avatar.url)
    deleted_ago: str = humanize_seconds((datetime.now(timezone.utc) - sniped.created_at).total_seconds())
    embed.set_footer(text=f"Sent {deleted_ago} ago")
    await ctx.send(embed=embed)

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
@commands.guild_only()
@app_commands.describe(role="Role mention, role ID, or exact role name.")
async def joinrole_cmd(ctx: commands.Context, *, role: str):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        return await ctx.send("❌ No role found.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    joinroles = settings.get("joinroles", [])

    if str(resolved.id) in joinroles:
        joinroles.remove(str(resolved.id))
        await bot.settings.push_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(f"✅ **{resolved.name}** will no longer be given to new joiners.")
    else:
        joinroles.append(str(resolved.id))
        await bot.settings.push_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(f"✅ **{resolved.name}** will now be given to all new joiners.")

@bot.hybrid_command(name="roleall", description="Give every member a specific role.")
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
    for member in ctx.guild.members:
        if resolved not in member.roles:
            try:
                await member.add_roles(resolved, reason=f"roleall by {ctx.author}")
                success += 1
            except:
                failed += 1
            await asyncio.sleep(0.1)  # Avoid rate limits

    await ctx.channel.send(f"✅ Finished adding **{resolved.name}**! Success: {success}, Failed: {failed}")

@bot.hybrid_command(name="reactionrole", description="Set up a reaction role on a message.")
@commands.guild_only()
@app_commands.describe(link="Link to the message", emoji="Reaction emoji", role="Role to assign")
async def reactionrole_cmd(ctx: commands.Context, link: str, emoji: str, *, role: str):
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

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    reactionroles = settings.get("reactionroles", {})
    key = f"{msg.id}_{emoji}"

    if key in reactionroles and reactionroles[key] == str(resolved.id):
        del reactionroles[key]
        await bot.settings.push_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(f"✅ Removed reaction role **{resolved.name}** from that message.")
    else:
        reactionroles[key] = str(resolved.id)
        await bot.settings.push_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(f"✅ Added reaction role **{resolved.name}** to that message. Users who react with {emoji} will receive the role.")

@bot.hybrid_command(name="reaction", description="Make the bot react to a message.")
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
@commands.guild_only()
async def ban_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, ban_members=True): return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.ban(reason=reason)
    await ctx.send(f"🔨 **{user}** was banned. Reason: {reason}")

@bot.hybrid_command(name="unban", description="Unban a user by their ID")
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
@commands.guild_only()
async def kick_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, kick_members=True): return await ctx.send("❌ You need Kick Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.kick(reason=reason)
    await ctx.send(f"👢 **{user}** was kicked. Reason: {reason}")

@bot.hybrid_command(name="timeout", description="Time a member out (mute)")
@commands.guild_only()
async def timeout_cmd(ctx: commands.Context, user: discord.Member, minutes: int, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, moderate_members=True): return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.timeout(discord.utils.utcnow() + discord.utils.timedelta(minutes=minutes), reason=reason)
    await ctx.send(f"🤐 **{user}** is timed out for {minutes}m. Reason: {reason}")

@bot.hybrid_command(name="untimeout", description="Remove a member's timeout")
@commands.guild_only()
async def untimeout_cmd(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, moderate_members=True): return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err: return await ctx.send(err, ephemeral=True)
    await user.timeout(None)
    await ctx.send(f"🔊 **{user}**'s timeout was removed.")

@bot.hybrid_command(name="warn", description="Warn a member")
@commands.guild_only()
async def warn_cmd(ctx: commands.Context, user: discord.Member, *, reason: Optional[str] = "No reason given"):
    if not member_has_perms(ctx.author, manage_messages=True): return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    warns = settings.setdefault("warns", {})
    uw = warns.setdefault(str(user.id), [])
    uw.append({"reason": reason, "by": ctx.author.id, "at": int(time.time())})
    await bot.settings.push_settings(ctx.guild.id, {"warns": warns})
    await ctx.send(f"⚠️ **{user}** was warned. Reason: {reason}")

@bot.hybrid_command(name="warnings", description="Show a member's warnings")
@commands.guild_only()
async def warnings_cmd(ctx: commands.Context, user: discord.Member):
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    uw = settings.get("warns", {}).get(str(user.id), [])
    if not uw: return await ctx.send(f"**{user}** has no warnings.")
    desc = "\n".join(f"<t:{w['at']}:d> by <@{w['by']}>: {w['reason']}" for w in uw)
    embed = discord.Embed(title=f"Warnings for {user}", description=desc, color=discord.Color.red())
    await ctx.send(embed=embed)

@bot.hybrid_command(name="clearwarns", description="Clear a member's warnings")
@commands.guild_only()
async def clearwarns_cmd(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, manage_messages=True): return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    warns = settings.get("warns", {})
    if str(user.id) in warns:
        del warns[str(user.id)]
        await bot.settings.push_settings(ctx.guild.id, {"warns": warns})
    await ctx.send(f"✅ Cleared warnings for **{user}**.")

@bot.hybrid_command(name="purge", description="Delete the last N messages in this channel")
@commands.guild_only()
async def purge_cmd(ctx: commands.Context, amount: int):
    if not member_has_perms(ctx.author, manage_messages=True): return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    amount = min(max(amount, 1), 100)
    await ctx.channel.purge(limit=amount + 1)
    await ctx.send(f"✅ Purged {amount} messages.", delete_after=3)

@bot.hybrid_command(name="lock", description="Lock a channel (block @everyone from sending)")
@commands.guild_only()
async def lock_cmd(ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=False)
    await ctx.send(f"🔒 {c.mention} is now locked.")

@bot.hybrid_command(name="unlock", description="Unlock a channel")
@commands.guild_only()
async def unlock_cmd(ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=True)
    await ctx.send(f"🔓 {c.mention} is now unlocked.")

@bot.hybrid_command(name="slowmode", description="Set channel slowmode (0 to disable)")
@commands.guild_only()
async def slowmode_cmd(ctx: commands.Context, seconds: int, channel: Optional[discord.TextChannel] = None):
    if not member_has_perms(ctx.author, manage_channels=True): return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.edit(slowmode_delay=max(0, seconds))
    await ctx.send(f"⏱️ Slowmode in {c.mention} set to {seconds}s.")

@bot.hybrid_command(name="nickname", description="Change a member's nickname (leave empty to reset)")
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
    "/autoreact ...",
    "/autorespond ...",
    "/set prefix <prefix>",
    "/autopurge ...",
    "/joinrole <role>",
    "/reactionrole <link> <emoji> <role>",
]
MOD_CMDS = [
    "/ban <user> [reason]", "/unban <user_id>", "/kick <user> [reason]",
    "/timeout <user> <minutes> [reason]", "/untimeout <user>",
    "/warn <user> [reason]", "/warnings <user>", "/clearwarns <user>",
    "/purge <amount>", "/lock [channel]", "/unlock [channel]",
    "/slowmode <seconds> [channel]", "/nickname <user> [name]",
    "/role <add/remove> <user> <role>", "/roleall <role>",
]
INFO_CMDS = [
    "/ping", "/uptime", "/userinfo [user]", "/serverinfo",
    "/avatar [user]", "/banner [user]", "/roleinfo <role>", "/membercount",
]

@bot.hybrid_command(name="commands", description="List commands (pick a category, or 'all')")
@app_commands.describe(category="Which category to show (default: overview)")
async def commands_cmd(
    ctx: commands.Context,
    category: Optional[Literal["all", "fun", "moderation", "info", "config"]] = None,
):
    embed = discord.Embed(title="Commands", color=discord.Color.blurple())
    show = category or "overview"
    if show in ("all", "fun"):
        embed.add_field(name="🎉 Fun & roleplay", value=" · ".join("/" + n for n in REACTIONS.keys()), inline=False)
    if show in ("all", "moderation"):
        embed.add_field(name="🛡️ Moderation", value="\n".join(MOD_CMDS)[:1024], inline=False)
    if show in ("all", "info"):
        embed.add_field(name="ℹ️ Info & utility", value="\n".join(INFO_CMDS)[:1024], inline=False)
    if show in ("all", "config"):
        embed.add_field(name="⚙️ Config (admin)", value="\n".join(CONFIG_CMDS)[:1024], inline=False)
    if show == "overview":
        embed.description = (
            "Categories: **fun**, **moderation**, **info**, **config**\n"
            "`/commands fun` · `/commands moderation` · `/commands info` · `/commands config` · `/commands all`"
        )
    await ctx.send(embed=embed, ephemeral=True)


# --------------------------------------------------------------------------- #
# Error Handling
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
            f"Check `!help {ctx.invoked_with}` for usage.",
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
        await ctx.send(f"⏳ Slow down — try again in {error.retry_after:.1f}s.", ephemeral=True)
        return
    if isinstance(error, commands.CheckFailure):
        await ctx.send("❌ You don't have permission to use this command.", ephemeral=True)
        return

    original: BaseException = getattr(error, "original", error)
    if isinstance(original, PyMongoError):
        log.error("Database error in command '%s': %s", ctx.command, original)
        await ctx.send("⚠️ The database is temporarily unreachable — please try again shortly.", ephemeral=True)
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
        return web.Response(text="Bot is alive")

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
