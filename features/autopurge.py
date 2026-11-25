"""The `autopurge` command group (the purging itself runs in features.pipeline)."""

import logging
import time
from typing import Literal, Optional

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import member_has_perms
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Error Handling


# ================= /autopurge =================
@bot.hybrid_group(
    name="autopurge", description="Auto-delete every new message in selected channels"
)
@commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def autopurge_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send(
            f"Invalid subcommand. Try `{ctx.clean_prefix}help autopurge`.",
            ephemeral=True,
        )


@autopurge_group.command(
    name="on", description="Start auto-deleting every new message in a channel"
)
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

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.setdefault("autopurge", {"channels": {}, "exempt_roles": []})
    ap.setdefault("channels", {})[str(channel.id)] = {"until": until}
    await bot.settings.push_fields(
        ctx.guild.id, {f"autopurge.channels.{channel.id}": {"until": until}}
    )

    when = f"until <t:{until}:f>" if until else "until you run `/autopurge off`"
    await ctx.send(
        f"🧹 Auto-purge is now **on** in {channel.mention} {when}. "
        "Every new message there will be deleted, except from exempt roles "
        "(`/autopurge exempt add <role>`).",
        ephemeral=True,
    )


@autopurge_group.command(name="off", description="Stop auto-deleting in a channel")
@app_commands.describe(channel="Channel (default: this one)")
async def autopurge_off(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    channel = channel or ctx.channel
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.get("autopurge", {"channels": {}, "exempt_roles": []})
    channels = ap.setdefault("channels", {})
    removed = channels.pop(str(channel.id), None)

    if removed:
        await bot.settings.push_fields(ctx.guild.id, {"autopurge.channels": channels})

    msg = (
        f"✅ Auto-purge turned off in {channel.mention}."
        if removed
        else f"Auto-purge wasn't active in {channel.mention}."
    )
    await ctx.send(msg, ephemeral=True)


@autopurge_group.command(
    name="exempt", description="Add/remove a role whose messages are never auto-deleted"
)
@app_commands.describe(action="add or remove", role="The role to exempt")
async def autopurge_exempt(
    ctx: commands.Context,
    action: Literal["add", "remove"],
    role: discord.Role,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
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

    await bot.settings.push_fields(ctx.guild.id, {"autopurge.exempt_roles": exempt})
    await ctx.send(msg, ephemeral=True)


@autopurge_group.command(
    name="msg_on",
    description="Start auto-deleting every new message matching text criteria",
)
@app_commands.describe(
    text="Text to match",
    match_type="How to match: 'contains', 'starts_with', or 'ends_with'",
    hours="Optional: automatically stop after this many hours",
    days="Optional: automatically stop after this many days",
)
async def autopurge_msg_on(
    ctx: commands.Context,
    text: str,
    match_type: Literal["contains", "starts_with", "ends_with"] = "contains",
    hours: Optional[app_commands.Range[int, 1, 720]] = None,
    days: Optional[app_commands.Range[int, 1, 365]] = None,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    until = None
    if hours or days:
        until = int(time.time()) + (hours or 0) * 3600 + (days or 0) * 86400

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.setdefault(
        "autopurge", {"channels": {}, "exempt_roles": [], "users": {}, "messages": {}}
    )
    messages_dict = ap.setdefault("messages", {})

    key = f"{match_type}:{text}"
    messages_dict[key] = {"until": until, "text": text, "match_type": match_type}
    # The key holds user text (may contain "." or "$"), so write the whole map.
    await bot.settings.push_fields(ctx.guild.id, {"autopurge.messages": messages_dict})

    when = f"until <t:{until}:f>" if until else "until you run `/autopurge msg_off`"

    desc_str = ""
    if match_type == "starts_with":
        desc_str = f"starting with `{text}`"
    elif match_type == "ends_with":
        desc_str = f"ending with `{text}`"
    else:
        desc_str = f"containing `{text}`"

    await ctx.send(
        f"🧹 Auto-purge is now **on** for messages {desc_str} {when}. "
        "Every new message matching this will be deleted.",
        ephemeral=True,
    )


@autopurge_group.command(
    name="msg_off", description="Stop auto-deleting messages matching text criteria"
)
@app_commands.describe(
    text="Text to match",
    match_type="How to match: 'contains', 'starts_with', or 'ends_with'",
)
async def autopurge_msg_off(
    ctx: commands.Context,
    text: str,
    match_type: Literal["contains", "starts_with", "ends_with"] = "contains",
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.get(
        "autopurge", {"channels": {}, "exempt_roles": [], "users": {}, "messages": {}}
    )
    messages_dict = ap.get("messages", {})

    key = f"{match_type}:{text}"
    removed = messages_dict.pop(key, None)

    if removed:
        await bot.settings.push_fields(
            ctx.guild.id, {"autopurge.messages": messages_dict}
        )

    msg = (
        f"✅ Auto-purge turned off for messages matching `{text}`."
        if removed
        else f"Auto-purge wasn't active for messages matching `{text}`."
    )
    await ctx.send(msg, ephemeral=True)


@autopurge_group.command(
    name="user_on",
    description="Start auto-deleting every new message from a specific user",
)
@app_commands.describe(
    user="The user to auto-purge",
    hours="Optional: automatically stop after this many hours",
    days="Optional: automatically stop after this many days",
)
async def autopurge_user_on(
    ctx: commands.Context,
    user: discord.Member,
    hours: Optional[app_commands.Range[int, 1, 720]] = None,
    days: Optional[app_commands.Range[int, 1, 365]] = None,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    until = None
    if hours or days:
        until = int(time.time()) + (hours or 0) * 3600 + (days or 0) * 86400

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.setdefault(
        "autopurge", {"channels": {}, "exempt_roles": [], "users": {}}
    )
    users = ap.setdefault("users", {})

    users[str(user.id)] = {"until": until, "exempt_channels": []}
    await bot.settings.push_fields(
        ctx.guild.id,
        {f"autopurge.users.{user.id}": {"until": until, "exempt_channels": []}},
    )

    when = (
        f"until <t:{until}:f>"
        if until
        else "until you run `/autopurge user_off <user>`"
    )
    await ctx.send(
        f"🧹 Auto-purge is now **on** for {user.mention} {when}. "
        "Every new message they send will be deleted, except in exempt channels "
        "(`/autopurge user_exempt add <user> <channel>`).",
        ephemeral=True,
    )


@autopurge_group.command(
    name="user_off", description="Stop auto-deleting messages from a user"
)
@app_commands.describe(user="The user to stop auto-purging")
async def autopurge_user_off(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.get("autopurge", {"channels": {}, "exempt_roles": [], "users": {}})
    users = ap.get("users", {})

    removed = users.pop(str(user.id), None)

    if removed:
        await bot.settings.push_fields(ctx.guild.id, {"autopurge.users": users})

    msg = (
        f"✅ Auto-purge turned off for {user.mention}."
        if removed
        else f"Auto-purge wasn't active for {user.mention}."
    )
    await ctx.send(msg, ephemeral=True)


@autopurge_group.command(
    name="user_exempt",
    description="Add/remove a channel where a user's messages are spared",
)
@app_commands.describe(
    action="add or remove", user="The auto-purged user", channel="The channel to exempt"
)
async def autopurge_user_exempt(
    ctx: commands.Context,
    action: Literal["add", "remove"],
    user: discord.Member,
    channel: discord.TextChannel,
):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.setdefault(
        "autopurge", {"channels": {}, "exempt_roles": [], "users": {}}
    )
    users = ap.setdefault("users", {})

    user_entry = users.get(str(user.id))
    if not user_entry:
        return await ctx.send(
            f"❌ {user.mention} is not currently being auto-purged.", ephemeral=True
        )

    exempt_channels = user_entry.setdefault("exempt_channels", [])

    if action == "add":
        if str(channel.id) not in exempt_channels:
            exempt_channels.append(str(channel.id))
        msg = (
            f"✅ Messages from {user.mention} will be left alone in {channel.mention}."
        )
    elif str(channel.id) in exempt_channels:
        exempt_channels.remove(str(channel.id))
        msg = f"✅ {channel.mention} is no longer exempt for {user.mention}."
    else:
        msg = f"{channel.mention} wasn't exempt for {user.mention}."

    await bot.settings.push_fields(
        ctx.guild.id,
        {f"autopurge.users.{user.id}.exempt_channels": exempt_channels},
    )
    await ctx.send(msg, ephemeral=True)


@autopurge_group.command(
    name="status", description="Where auto-purge is active and which roles are exempt"
)
async def autopurge_status(ctx: commands.Context):
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send("❌ You need Administrator permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    ap = settings.get("autopurge", {"channels": {}, "exempt_roles": [], "users": {}})
    now = time.time()
    lines = []

    for cid, c in ap.get("channels", {}).items():
        until = c.get("until")
        if until and now > until:
            continue  # expired, will be cleaned up automatically
        lines.append(
            f"<#{cid}> — " + (f"until <t:{until}:f>" if until else "until turned off")
        )

    user_lines = []
    for uid, u in ap.get("users", {}).items():
        until = u.get("until")
        if until and now > until:
            continue
        exempt = u.get("exempt_channels", [])
        exempt_str = (
            f" (Exempt: {', '.join(f'<#{c}>' for c in exempt)})" if exempt else ""
        )
        user_lines.append(
            f"<@{uid}> — "
            + (f"until <t:{until}:f>" if until else "until turned off")
            + exempt_str
        )

    embed = discord.Embed(title="Auto-purge status", color=discord.Color.blurple())
    embed.add_field(
        name="Active channels",
        value="\n".join(lines)[:1024] or "Not active anywhere.",
        inline=False,
    )
    embed.add_field(
        name="Exempt roles",
        value=" ".join(f"<@&{r}>" for r in ap.get("exempt_roles", []))[:1024] or "None",
        inline=False,
    )
    if user_lines:
        embed.add_field(
            name="Auto-purged Users",
            value="\n".join(user_lines)[:1024],
            inline=False,
        )

    msg_lines = []
    for _key, v in ap.get("messages", {}).items():
        until = v.get("until")
        mtype = v.get("match_type")
        txt = v.get("text")
        mtype_str = "containing"
        if mtype == "starts_with":
            mtype_str = "starting with"
        elif mtype == "ends_with":
            mtype_str = "ending with"

        msg_lines.append(
            f"Messages {mtype_str} `{txt}` — "
            + (f"until <t:{until}:f>" if until else "until turned off")
        )

    if msg_lines:
        embed.add_field(
            name="Auto-purged Messages",
            value="\n".join(msg_lines)[:1024],
            inline=False,
        )
    await ctx.send(embed=embed, ephemeral=True)
