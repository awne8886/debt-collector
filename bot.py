import json
import os
import time

import discord
from discord import app_commands
from discord.ext import commands, tasks

TOKEN = os.environ["DISCORD_TOKEN"]
SETTINGS_FILE = "settings.json"
DEFAULT_INTERVAL_MIN = 181  # 3 hours 1 minute


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
    g.setdefault("autoreact", {})    # trigger -> emoji
    g.setdefault("autorespond", {})  # trigger -> response text
    return g


# ---------- bot ----------
intents = discord.Intents.default()
intents.message_content = True  # required for trigger detection

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    allowed_mentions=discord.AllowedMentions(roles=True),
)

next_fire: dict[str, float] = {}  # guild_id -> unix timestamp of next ping


def is_admin(interaction: discord.Interaction) -> bool:
    return interaction.user.guild_permissions.administrator


async def admin_gate(interaction: discord.Interaction) -> bool:
    if not is_admin(interaction):
        await interaction.response.send_message(
            "You need Administrator permissions to use this.", ephemeral=True
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


# ================= startup =================
@bot.event
async def setup_hook():
    bot.tree.add_command(remind)
    bot.tree.add_command(autoreact)
    bot.tree.add_command(autorespond)
    await bot.tree.sync()
    reminder_loop.start()


bot.run(TOKEN)
