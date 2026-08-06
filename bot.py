import json
import os
import threading
import time
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Literal, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

TOKEN = os.environ["DISCORD_TOKEN"]
SETTINGS_FILE = "settings.json"
DEFAULT_INTERVAL_MIN = 181  # 3 hours 1 minute
START_TIME = time.time()
NEKOS_API = "https://nekos.best/api/v2/{}"
EMBED_COLOR = 0xFF9DD1


# ---------- persistence ----------
def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_settings():
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)


settings = load_settings()


def guild_cfg(guild_id: int):
    g = settings.setdefault(str(guild_id), {})
    g.setdefault("remind", {
        "enabled": False,
        "interval": DEFAULT_INTERVAL_MIN,
        "role_id": None,
        "channel_id": None,
        "message": "Reminder!",
    })
    g.setdefault("prefix", "!")      # chat command prefix
    g.setdefault("autoreact", {})    # trigger -> emoji
    g.setdefault("autorespond", {})  # trigger -> response text
    g.setdefault("warns", {})        # user_id -> [ {reason, by, at} ]
    return g


# ---------- bot ----------
intents = discord.Intents.default()
intents.message_content = True  # Required for trigger detection
intents.members = True          # REQUIRED for ban/kick/hierarchy checks

def get_prefix(_bot, message):
    if message.guild:
        return guild_cfg(message.guild.id).get("prefix", "!")
    return "!"


bot = commands.Bot(
    command_prefix=get_prefix,
    help_command=None,  # /help covers this
    intents=intents,
    allowed_mentions=discord.AllowedMentions(roles=True),
)

http_session: Optional[aiohttp.ClientSession] = None
next_fire: dict[str, float] = {}  # guild_id -> unix timestamp of next ping


# ---------- permission helpers ----------

# ---------- Hardened Permission Helpers ----------
def is_admin(interaction: discord.Interaction) -> bool:
    return interaction.user.guild_permissions.administrator

async def admin_gate(interaction: discord.Interaction) -> bool:
    if not is_admin(interaction):
        await interaction.response.send_message(
            "❌ You need Administrator permissions to use this.", ephemeral=True
        )
        return False
    return True

async def require_perm(interaction: discord.Interaction, perm: str) -> bool:
    # Safe fallback attribute fetch using getattr
    if getattr(interaction.user.guild_permissions, perm, False):
        return True
    await interaction.response.send_message(
        f"❌ You don't have the `{perm}` permission to use this.", ephemeral=True
    )
    return False

async def hierarchy_gate(interaction: discord.Interaction, target: discord.Member) -> bool:
    if target.id == interaction.user.id:
        await interaction.response.send_message("❌ You can't use that on yourself.", ephemeral=True)
        return False
        
    # Owner exception safety check
    if interaction.guild.owner_id == interaction.user.id:
        return True

    # Use .position value integers instead of comparing raw role objects directly
    if interaction.user.top_role.position <= target.top_role.position:
        await interaction.response.send_message(
            "❌ You can't act on someone with an equal or higher role.", ephemeral=True
        )
        return False
        
    # Check the bot's own role position relative to the target
    bot_member = interaction.guild.me
    if bot_member.top_role.position <= target.top_role.position:
        await interaction.response.send_message(
            "❌ My role rank is lower or equal to that user. Drag my role higher in Server Settings.", ephemeral=True
        )
        return False
        
    return True


# ================= /remind =================
remind = app_commands.Group(
    name="remind",
    description="Recurring role-ping reminder",
    default_permissions=discord.Permissions(administrator=True),
    guild_only=True,
)


@remind.command(name="on", description="Enable the recurring reminder")
async def remind_on(interaction: discord.Interaction):
    if not await admin_gate(interaction):
        return
    cfg = guild_cfg(interaction.guild_id)["remind"]
    if not cfg["role_id"] or not cfg["channel_id"]:
        return await interaction.response.send_message(
            "Set a role and a channel first: `/remind role` and `/remind channel`.",
            ephemeral=True,
        )
    cfg["enabled"] = True
    save_settings()
    next_fire[str(interaction.guild_id)] = time.time() + cfg["interval"] * 60
    h, m = divmod(cfg["interval"], 60)
    await interaction.response.send_message(
        f"✅ Reminder enabled — pinging every {h}h {m}m.", ephemeral=True
    )


@remind.command(name="off", description="Disable the recurring reminder")
async def remind_off(interaction: discord.Interaction):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["remind"]["enabled"] = False
    save_settings()
    next_fire.pop(str(interaction.guild_id), None)
    await interaction.response.send_message("🛑 Reminder disabled.", ephemeral=True)


@remind.command(name="interval", description="Set the interval in minutes (default 181 = 3h 1m)")
@app_commands.describe(minutes="Interval in minutes (minimum 1)")
async def remind_interval(interaction: discord.Interaction, minutes: app_commands.Range[int, 1, 10080]):
    if not await admin_gate(interaction):
        return
    cfg = guild_cfg(interaction.guild_id)["remind"]
    cfg["interval"] = minutes
    save_settings()
    if cfg["enabled"]:
        next_fire[str(interaction.guild_id)] = time.time() + minutes * 60
    h, m = divmod(minutes, 60)
    await interaction.response.send_message(f"⏱️ Interval set to {h}h {m}m.", ephemeral=True)


@remind.command(name="role", description="Set the role to ping")
async def remind_role(interaction: discord.Interaction, role: discord.Role):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["remind"]["role_id"] = role.id
    save_settings()
    await interaction.response.send_message(f"📌 Will ping {role.mention}.", ephemeral=True)


@remind.command(name="channel", description="Set the channel to post in")
async def remind_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["remind"]["channel_id"] = channel.id
    save_settings()
    await interaction.response.send_message(f"📌 Will post in {channel.mention}.", ephemeral=True)


@remind.command(name="message", description="Set the text sent alongside the ping")
async def remind_message(interaction: discord.Interaction, text: str):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["remind"]["message"] = text
    save_settings()
    await interaction.response.send_message("✏️ Reminder message updated.", ephemeral=True)


@remind.command(name="test", description="Send the reminder right now to verify everything works")
async def remind_test(interaction: discord.Interaction):
    if not await admin_gate(interaction):
        return
    cfg = guild_cfg(interaction.guild_id)["remind"]
    if not cfg["role_id"] or not cfg["channel_id"]:
        return await interaction.response.send_message(
            "Set a role and a channel first: `/remind role` and `/remind channel`.",
            ephemeral=True,
        )
    channel = bot.get_channel(cfg["channel_id"])
    if channel is None:
        return await interaction.response.send_message(
            "I can't see the configured channel — pick a new one with `/remind channel`.",
            ephemeral=True,
        )
    try:
        await channel.send(f"<@&{cfg['role_id']}> {cfg['message']} *(test)*")
    except discord.Forbidden:
        return await interaction.response.send_message(
            "I'm missing permission to send messages in that channel.", ephemeral=True
        )
    await interaction.response.send_message(f"✅ Test reminder sent to {channel.mention}.", ephemeral=True)


# ================= /autoreact =================
autoreact = app_commands.Group(
    name="autoreact",
    description="Auto-react to messages containing a trigger",
    default_permissions=discord.Permissions(administrator=True),
    guild_only=True,
)
autoreact_set = app_commands.Group(name="set", description="Set an auto-react", parent=autoreact)


@autoreact_set.command(name="trigger", description="React with an emoji when a trigger word appears")
@app_commands.describe(trigger="Word/phrase to watch for", emoji="Emoji to react with")
async def autoreact_set_trigger(interaction: discord.Interaction, trigger: str, emoji: str):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["autoreact"][trigger.lower()] = emoji.strip()
    save_settings()
    await interaction.response.send_message(
        f"✅ Will react with {emoji} to messages containing “{trigger}”.", ephemeral=True
    )


@autoreact.command(name="remove", description="Remove an auto-react trigger")
async def autoreact_remove(interaction: discord.Interaction, trigger: str):
    if not await admin_gate(interaction):
        return
    removed = guild_cfg(interaction.guild_id)["autoreact"].pop(trigger.lower(), None)
    save_settings()
    msg = f"🗑️ Removed “{trigger}”." if removed else f"“{trigger}” wasn’t set."
    await interaction.response.send_message(msg, ephemeral=True)


@autoreact.command(name="list", description="List auto-react triggers")
async def autoreact_list(interaction: discord.Interaction):
    if not await admin_gate(interaction):
        return
    items = guild_cfg(interaction.guild_id)["autoreact"]
    text = "\n".join(f"• “{t}” → {e}" for t, e in items.items()) or "No auto-reacts set."
    await interaction.response.send_message(text, ephemeral=True)


# ================= /autorespond =================
autorespond = app_commands.Group(
    name="autorespond",
    description="Auto-reply to messages containing a trigger",
    default_permissions=discord.Permissions(administrator=True),
    guild_only=True,
)
autorespond_set = app_commands.Group(name="set", description="Set an auto-response", parent=autorespond)


@autorespond_set.command(name="trigger", description="Reply with a message when a trigger word appears")
@app_commands.describe(trigger="Word/phrase to watch for", response="Text the bot replies with")
async def autorespond_set_trigger(interaction: discord.Interaction, trigger: str, response: str):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["autorespond"][trigger.lower()] = response
    save_settings()
    await interaction.response.send_message(
        f"✅ Will reply to messages containing “{trigger}”.", ephemeral=True
    )


@autorespond.command(name="remove", description="Remove an auto-response trigger")
async def autorespond_remove(interaction: discord.Interaction, trigger: str):
    if not await admin_gate(interaction):
        return
    removed = guild_cfg(interaction.guild_id)["autorespond"].pop(trigger.lower(), None)
    save_settings()
    msg = f"🗑️ Removed “{trigger}”." if removed else f"“{trigger}” wasn’t set."
    await interaction.response.send_message(msg, ephemeral=True)


@autorespond.command(name="list", description="List auto-response triggers")
async def autorespond_list(interaction: discord.Interaction):
    if not await admin_gate(interaction):
        return
    items = guild_cfg(interaction.guild_id)["autorespond"]
    text = "\n".join(f"• “{t}” → {r}" for t, r in items.items()) or "No auto-responses set."
    await interaction.response.send_message(text, ephemeral=True)


# ================= /set =================
set_group = app_commands.Group(
    name="set",
    description="Bot settings",
    default_permissions=discord.Permissions(administrator=True),
    guild_only=True,
)


@set_group.command(name="prefix", description="Set the chat command prefix for this server (default !)")
@app_commands.describe(prefix="New prefix, e.g. ! or x (max 5 characters)")
async def set_prefix(interaction: discord.Interaction, prefix: str):
    if not await admin_gate(interaction):
        return
    prefix = prefix.strip()
    if not prefix or len(prefix) > 5:
        return await interaction.response.send_message("Prefix must be 1-5 characters.", ephemeral=True)
    guild_cfg(interaction.guild_id)["prefix"] = prefix
    save_settings()
    await interaction.response.send_message(
        f"✅ Prefix set to `{prefix}` — try `{prefix}ping` or `{prefix}hug @someone`.", ephemeral=True
    )


# ================= fun / roleplay =================
TARGETED_ACTIONS = {
    "bite": "bit", "hug": "hugged", "kiss": "kissed", "slap": "slapped",
    "pat": "patted", "cuddle": "cuddled", "poke": "poked", "punch": "punched",
    "tickle": "tickled", "feed": "fed", "highfive": "high-fived",
    "wave": "waved at", "handhold": "held hands with", "yeet": "yeeted",
}
SOLO_ACTIONS = {
    "blush": "blushes", "cry": "cries", "dance": "dances", "laugh": "laughs",
    "smile": "smiles", "wink": "winks", "pout": "pouts", "shrug": "shrugs",
    "sleep": "sleeps", "facepalm": "facepalms",
}


async def fetch_gif(action: str) -> Optional[str]:
    try:
        async with http_session.get(
            NEKOS_API.format(action), timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
            return data["results"][0]["url"]
    except Exception:
        return None


def register_fun_commands():
    for action, verb in TARGETED_ACTIONS.items():
        def make_targeted(action=action, verb=verb):
            @app_commands.describe(user="Who to target")
            async def cb(interaction: discord.Interaction, user: discord.Member):
                await interaction.response.defer()
                gif = await fetch_gif(action)
                embed = discord.Embed(
                    title=f"{interaction.user.display_name} {verb} {user.display_name}!",
                    color=EMBED_COLOR,
                )
                if gif:
                    embed.set_image(url=gif)
                await interaction.followup.send(embed=embed)
            return cb

        bot.tree.add_command(app_commands.Command(
            name=action,
            description=f"{verb.capitalize()} someone (with an anime gif)",
            callback=make_targeted(),
        ))

    for action, verb in SOLO_ACTIONS.items():
        def make_solo(action=action, verb=verb):
            async def cb(interaction: discord.Interaction):
                await interaction.response.defer()
                gif = await fetch_gif(action)
                embed = discord.Embed(
                    title=f"{interaction.user.display_name} {verb}!",
                    color=EMBED_COLOR,
                )
                if gif:
                    embed.set_image(url=gif)
                await interaction.followup.send(embed=embed)
            return cb

        bot.tree.add_command(app_commands.Command(
            name=action,
            description=f"{verb.capitalize()} (with an anime gif)",
            callback=make_solo(),
        ))


def register_prefix_fun_commands():
    for action, verb in TARGETED_ACTIONS.items():
        def make_targeted(action=action, verb=verb):
            async def cb(ctx: commands.Context, user: discord.Member):
                gif = await fetch_gif(action)
                embed = discord.Embed(
                    title=f"{ctx.author.display_name} {verb} {user.display_name}!",
                    color=EMBED_COLOR,
                )
                if gif:
                    embed.set_image(url=gif)
                await ctx.send(embed=embed)
            return cb

        bot.add_command(commands.Command(make_targeted(), name=action))

    for action, verb in SOLO_ACTIONS.items():
        def make_solo(action=action, verb=verb):
            async def cb(ctx: commands.Context):
                gif = await fetch_gif(action)
                embed = discord.Embed(
                    title=f"{ctx.author.display_name} {verb}!",
                    color=EMBED_COLOR,
                )
                if gif:
                    embed.set_image(url=gif)
                await ctx.send(embed=embed)
            return cb

        bot.add_command(commands.Command(make_solo(), name=action))


@bot.command(name="ping")
async def ping_prefix(ctx: commands.Context):
    await ctx.send(f"🏓 Pong! Latency: **{round(bot.latency * 1000)}ms**")


# ================= moderation =================
@bot.tree.command(name="ban", description="Ban a member")
@app_commands.default_permissions(ban_members=True)
@app_commands.guild_only()
@app_commands.describe(user="Member to ban", reason="Reason (optional)")
async def ban_cmd(interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None):
    if not await require_perm(interaction, "ban_members"):
        return
    if not await hierarchy_gate(interaction, user):
        return
    try:
        await user.ban(reason=f"{interaction.user}: {reason or 'No reason given'}")
    except discord.Forbidden:
        return await interaction.response.send_message("I can't ban that member (role hierarchy).", ephemeral=True)
    await interaction.response.send_message(f"🔨 Banned **{user}** — {reason or 'no reason given'}.")


@bot.tree.command(name="unban", description="Unban a user by their ID")
@app_commands.default_permissions(ban_members=True)
@app_commands.guild_only()
@app_commands.describe(user_id="ID of the banned user")
async def unban_cmd(interaction: discord.Interaction, user_id: str):
    if not await require_perm(interaction, "ban_members"):
        return
    try:
        await interaction.guild.unban(discord.Object(id=int(user_id)))
    except (ValueError, discord.NotFound):
        return await interaction.response.send_message("No banned user with that ID.", ephemeral=True)
    except discord.Forbidden:
        return await interaction.response.send_message("I'm missing Ban Members permission.", ephemeral=True)
    await interaction.response.send_message(f"✅ Unbanned <@{user_id}>.")


@bot.tree.command(name="kick", description="Kick a member")
@app_commands.default_permissions(kick_members=True)
@app_commands.guild_only()
@app_commands.describe(user="Member to kick", reason="Reason (optional)")
async def kick_cmd(interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None):
    if not await require_perm(interaction, "kick_members"):
        return
    if not await hierarchy_gate(interaction, user):
        return
    try:
        await user.kick(reason=f"{interaction.user}: {reason or 'No reason given'}")
    except discord.Forbidden:
        return await interaction.response.send_message("I can't kick that member (role hierarchy).", ephemeral=True)
    await interaction.response.send_message(f"👢 Kicked **{user}** — {reason or 'no reason given'}.")


@bot.tree.command(name="timeout", description="Time a member out (mute)")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
@app_commands.describe(user="Member to mute", minutes="Duration in minutes (max 28 days)", reason="Reason (optional)")
async def timeout_cmd(
    interaction: discord.Interaction,
    user: discord.Member,
    minutes: app_commands.Range[int, 1, 40320],
    reason: Optional[str] = None,
):
    if not await require_perm(interaction, "moderate_members"):
        return
    if not await hierarchy_gate(interaction, user):
        return
    try:
        await user.timeout(timedelta(minutes=minutes), reason=f"{interaction.user}: {reason or 'No reason given'}")
    except discord.Forbidden:
        return await interaction.response.send_message("I can't time out that member (role hierarchy).", ephemeral=True)
    await interaction.response.send_message(f"🔇 Timed out **{user}** for {minutes} minute(s).")


@bot.tree.command(name="untimeout", description="Remove a member's timeout")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
async def untimeout_cmd(interaction: discord.Interaction, user: discord.Member):
    if not await require_perm(interaction, "moderate_members"):
        return
    try:
        await user.timeout(None)
    except discord.Forbidden:
        return await interaction.response.send_message("I can't edit that member.", ephemeral=True)
    await interaction.response.send_message(f"🔊 Removed timeout from **{user}**.")


@bot.tree.command(name="warn", description="Warn a member")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
@app_commands.describe(user="Member to warn", reason="Reason (optional)")
async def warn_cmd(interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None):
    if not await require_perm(interaction, "moderate_members"):
        return
    warns = guild_cfg(interaction.guild_id)["warns"].setdefault(str(user.id), [])
    warns.append({"reason": reason or "No reason given", "by": str(interaction.user), "at": int(time.time())})
    save_settings()
    await interaction.response.send_message(
        f"⚠️ Warned **{user}** — {reason or 'no reason given'} (warning #{len(warns)})."
    )


@bot.tree.command(name="warnings", description="Show a member's warnings")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
async def warnings_cmd(interaction: discord.Interaction, user: discord.Member):
    if not await require_perm(interaction, "moderate_members"):
        return
    warns = guild_cfg(interaction.guild_id)["warns"].get(str(user.id), [])
    if not warns:
        return await interaction.response.send_message(f"**{user}** has no warnings.", ephemeral=True)
    lines = [f"{i + 1}. {w['reason']} — by {w['by']} <t:{w['at']}:R>" for i, w in enumerate(warns)]
    embed = discord.Embed(title=f"Warnings for {user}", description="\n".join(lines)[:4000], color=EMBED_COLOR)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="clearwarns", description="Clear a member's warnings")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def clearwarns_cmd(interaction: discord.Interaction, user: discord.Member):
    if not await admin_gate(interaction):
        return
    guild_cfg(interaction.guild_id)["warns"].pop(str(user.id), None)
    save_settings()
    await interaction.response.send_message(f"🧹 Cleared all warnings for **{user}**.")


@bot.tree.command(name="purge", description="Delete the last N messages in this channel")
@app_commands.default_permissions(manage_messages=True)
@app_commands.guild_only()
@app_commands.describe(amount="How many messages to delete (1-100)")
async def purge_cmd(interaction: discord.Interaction, amount: app_commands.Range[int, 1, 100]):
    if not await require_perm(interaction, "manage_messages"):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        deleted = await interaction.channel.purge(limit=amount)
    except discord.Forbidden:
        return await interaction.followup.send("I'm missing Manage Messages permission here.")
    await interaction.followup.send(f"🧹 Deleted {len(deleted)} message(s).")


@bot.tree.command(name="lock", description="Lock a channel (block @everyone from sending)")
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
@app_commands.describe(channel="Channel to lock (default: current)")
async def lock_cmd(interaction: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if not await require_perm(interaction, "manage_channels"):
        return
    channel = channel or interaction.channel
    try:
        await channel.set_permissions(interaction.guild.default_role, send_messages=False)
    except discord.Forbidden:
        return await interaction.response.send_message("I'm missing Manage Channels permission.", ephemeral=True)
    await interaction.response.send_message(f"🔒 Locked {channel.mention}.")


@bot.tree.command(name="unlock", description="Unlock a channel")
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
@app_commands.describe(channel="Channel to unlock (default: current)")
async def unlock_cmd(interaction: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if not await require_perm(interaction, "manage_channels"):
        return
    channel = channel or interaction.channel
    try:
        await channel.set_permissions(interaction.guild.default_role, send_messages=None)
    except discord.Forbidden:
        return await interaction.response.send_message("I'm missing Manage Channels permission.", ephemeral=True)
    await interaction.response.send_message(f"🔓 Unlocked {channel.mention}.")


@bot.tree.command(name="slowmode", description="Set channel slowmode (0 to disable)")
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
@app_commands.describe(seconds="Delay in seconds (0-21600)", channel="Channel (default: current)")
async def slowmode_cmd(
    interaction: discord.Interaction,
    seconds: app_commands.Range[int, 0, 21600],
    channel: Optional[discord.TextChannel] = None,
):
    if not await require_perm(interaction, "manage_channels"):
        return
    channel = channel or interaction.channel
    try:
        await channel.edit(slowmode_delay=seconds)
    except discord.Forbidden:
        return await interaction.response.send_message("I'm missing Manage Channels permission.", ephemeral=True)
    msg = f"🐌 Slowmode set to {seconds}s in {channel.mention}." if seconds else f"🚀 Slowmode disabled in {channel.mention}."
    await interaction.response.send_message(msg)


@bot.tree.command(name="nickname", description="Change a member's nickname (leave empty to reset)")
@app_commands.default_permissions(manage_nicknames=True)
@app_commands.guild_only()
@app_commands.describe(user="Member to rename", name="New nickname (leave empty to reset)")
async def nickname_cmd(interaction: discord.Interaction, user: discord.Member, name: Optional[str] = None):
    if not await require_perm(interaction, "manage_nicknames"):
        return
    try:
        await user.edit(nick=name)
    except discord.Forbidden:
        return await interaction.response.send_message("I can't rename that member (role hierarchy).", ephemeral=True)
    await interaction.response.send_message(f"✏️ Nickname for **{user}** {'set to **' + name + '**' if name else 'reset'}.")


role_group = app_commands.Group(
    name="role",
    description="Add or remove roles from members",
    default_permissions=discord.Permissions(manage_roles=True),
    guild_only=True,
)


@role_group.command(name="add", description="Give a member a role")
async def role_add(interaction: discord.Interaction, user: discord.Member, role: discord.Role):
    if not await require_perm(interaction, "manage_roles"):
        return
    if role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id:
        return await interaction.response.send_message("That role is above your highest role.", ephemeral=True)
    try:
        await user.add_roles(role)
    except discord.Forbidden:
        return await interaction.response.send_message("I can't manage that role (it may be above mine).", ephemeral=True)
    await interaction.response.send_message(f"✅ Gave {role.mention} to **{user}**.")


@role_group.command(name="remove", description="Remove a role from a member")
async def role_remove(interaction: discord.Interaction, user: discord.Member, role: discord.Role):
    if not await require_perm(interaction, "manage_roles"):
        return
    if role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id:
        return await interaction.response.send_message("That role is above your highest role.", ephemeral=True)
    try:
        await user.remove_roles(role)
    except discord.Forbidden:
        return await interaction.response.send_message("I can't manage that role (it may be above mine).", ephemeral=True)
    await interaction.response.send_message(f"✅ Removed {role.mention} from **{user}**.")


# ================= info / utility =================
@bot.tree.command(name="ping", description="Check that the bot is alive and see its latency")
async def ping_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(f"🏓 Pong! Latency: **{round(bot.latency * 1000)}ms**")


@bot.tree.command(name="uptime", description="How long the bot has been running")
async def uptime_cmd(interaction: discord.Interaction):
    secs = int(time.time() - START_TIME)
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    await interaction.response.send_message(f"⏳ Uptime: **{d}d {h}h {m}m {s}s**")


@bot.tree.command(name="userinfo", description="Info about a member")
@app_commands.guild_only()
@app_commands.describe(user="Member (default: you)")
async def userinfo_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    user = user or interaction.user
    embed = discord.Embed(title=str(user), color=EMBED_COLOR)
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="ID", value=user.id, inline=False)
    embed.add_field(name="Created", value=f"<t:{int(user.created_at.timestamp())}:R>")
    if user.joined_at:
        embed.add_field(name="Joined", value=f"<t:{int(user.joined_at.timestamp())}:R>")
    embed.add_field(name="Top role", value=user.top_role.mention)
    embed.add_field(name="Roles", value=str(len(user.roles) - 1))
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="serverinfo", description="Info about this server")
@app_commands.guild_only()
async def serverinfo_cmd(interaction: discord.Interaction):
    g = interaction.guild
    embed = discord.Embed(title=g.name, color=EMBED_COLOR)
    if g.icon:
        embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name="ID", value=g.id, inline=False)
    embed.add_field(name="Owner", value=f"<@{g.owner_id}>")
    embed.add_field(name="Members", value=str(g.member_count))
    embed.add_field(name="Roles", value=str(len(g.roles)))
    embed.add_field(name="Channels", value=str(len(g.channels)))
    embed.add_field(name="Created", value=f"<t:{int(g.created_at.timestamp())}:R>")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="avatar", description="Show a user's avatar")
@app_commands.describe(user="User (default: you)")
async def avatar_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    user = user or interaction.user
    embed = discord.Embed(title=f"{user.display_name}'s avatar", color=EMBED_COLOR)
    embed.set_image(url=user.display_avatar.url)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="banner", description="Show a user's profile banner")
@app_commands.describe(user="User (default: you)")
async def banner_cmd(interaction: discord.Interaction, user: Optional[discord.User] = None):
    user = user or interaction.user
    fetched = await bot.fetch_user(user.id)  # banner needs a full fetch
    if not fetched.banner:
        return await interaction.response.send_message(f"**{user.display_name}** has no banner.", ephemeral=True)
    embed = discord.Embed(title=f"{user.display_name}'s banner", color=EMBED_COLOR)
    embed.set_image(url=fetched.banner.url)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="roleinfo", description="Info about a role")
@app_commands.guild_only()
async def roleinfo_cmd(interaction: discord.Interaction, role: discord.Role):
    embed = discord.Embed(title=f"@{role.name}", color=role.color if role.color.value else EMBED_COLOR)
    embed.add_field(name="ID", value=role.id, inline=False)
    embed.add_field(name="Members", value=str(len(role.members)))
    embed.add_field(name="Color", value=str(role.color))
    embed.add_field(name="Mentionable", value="Yes" if role.mentionable else "No")
    embed.add_field(name="Created", value=f"<t:{int(role.created_at.timestamp())}:R>")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="membercount", description="How many members this server has")
@app_commands.guild_only()
async def membercount_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(f"👥 **{interaction.guild.member_count}** members.")


# ================= /help and /commands =================
CONFIG_CMDS = [
    "/remind on · /remind off · /remind test",
    "/remind interval <minutes> · /remind role <role>",
    "/remind channel <channel> · /remind message <text>",
    "/autoreact set trigger <trigger> <emoji> · /autoreact remove · /autoreact list",
    "/autorespond set trigger <trigger> <response> · /autorespond remove · /autorespond list",
    "/set prefix <prefix>",
]
MOD_CMDS = [
    "/ban <user> [reason]", "/unban <user_id>", "/kick <user> [reason]",
    "/timeout <user> <minutes> [reason]", "/untimeout <user>",
    "/warn <user> [reason]", "/warnings <user>", "/clearwarns <user>",
    "/purge <amount>", "/lock [channel]", "/unlock [channel]",
    "/slowmode <seconds> [channel]", "/nickname <user> [name]",
    "/role add <user> <role>", "/role remove <user> <role>",
]
INFO_CMDS = [
    "/ping", "/uptime", "/userinfo [user]", "/serverinfo",
    "/avatar [user]", "/banner [user]", "/roleinfo <role>", "/membercount",
]


def fun_cmds_text() -> str:
    return " · ".join("/" + n for n in sorted(list(TARGETED_ACTIONS) + list(SOLO_ACTIONS)))


@bot.tree.command(name="help", description="What this bot can do")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Help",
        description=(
            "**🔔 Reminder** — pings a role in a channel on a repeating timer (admin only). "
            "Start with `/remind role`, `/remind channel`, then `/remind on`. Test with `/remind test`.\n"
            "**🤖 Auto-react / auto-respond** — react or reply when a trigger word is seen (admin only).\n"
            "**🎉 Fun & roleplay** — anime-gif actions like `/bite`, `/hug`, `/slap` — "
            "these also work as chat commands with the prefix (default `!`, change with `/set prefix`).\n"
            "**🛡️ Moderation** — ban, kick, timeout, warn, purge and more.\n"
            "**ℹ️ Info** — `/userinfo`, `/serverinfo`, `/avatar`, `/ping`…\n\n"
            "Use `/commands` for a category list or `/commands all` for every command."
        ),
        color=EMBED_COLOR,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="commands", description="List commands (pick a category, or 'all')")
@app_commands.describe(category="Which category to show (default: overview)")
async def commands_cmd(
    interaction: discord.Interaction,
    category: Optional[Literal["all", "fun", "moderation", "info", "config"]] = None,
):
    embed = discord.Embed(title="Commands", color=EMBED_COLOR)
    show = category or "overview"
    if show in ("all", "fun"):
        embed.add_field(name="🎉 Fun & roleplay", value=fun_cmds_text()[:1024], inline=False)
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
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ================= reminder loop =================
@tasks.loop(seconds=20)
async def reminder_loop():
    now = time.time()
    for gid, g in settings.items():
        cfg = g.get("remind", {})
        if not cfg.get("enabled"):
            continue
        if gid not in next_fire:  # e.g. after a restart
            next_fire[gid] = now + cfg["interval"] * 60
            continue
        if now >= next_fire[gid]:
            next_fire[gid] = now + cfg["interval"] * 60
            channel = bot.get_channel(cfg["channel_id"])
            if channel is None:
                continue
            try:
                await channel.send(f"<@&{cfg['role_id']}> {cfg['message']}")
            except (discord.Forbidden, discord.HTTPException):
                pass


@reminder_loop.before_loop
async def before_reminder_loop():
    await bot.wait_until_ready()


# ================= trigger detection =================
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None:
        return
    g = guild_cfg(message.guild.id)
    content = message.content.lower()

    for trigger, emoji in g["autoreact"].items():
        if trigger in content:
            try:
                await message.add_reaction(emoji)
            except discord.HTTPException:
                pass  # invalid emoji or missing permission

    for trigger, response in g["autorespond"].items():
        if trigger in content:
            try:
                await message.channel.send(response)
            except discord.HTTPException:
                pass
            break  # only one auto-response per message

    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return  # not every prefixed message is a command
    if isinstance(error, (commands.MissingRequiredArgument, commands.MemberNotFound, commands.BadArgument)):
        await ctx.send("You need to mention a valid member, e.g. `hug @someone`.")
        return
    raise error


# ================= keep-alive web server (for Render) =================
class Ping(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive")

    def log_message(self, *args):
        pass


def keep_alive():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(("0.0.0.0", port), Ping).serve_forever()


# ================= startup =================
@bot.event
async def setup_hook():
    global http_session
    http_session = aiohttp.ClientSession()
    bot.tree.add_command(remind)
    bot.tree.add_command(autoreact)
    bot.tree.add_command(autorespond)
    bot.tree.add_command(role_group)
    bot.tree.add_command(set_group)
    register_fun_commands()
    register_prefix_fun_commands()
    await bot.tree.sync()
    reminder_loop.start()


threading.Thread(target=keep_alive, daemon=True).start()
bot.run(TOKEN)
