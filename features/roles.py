"""Join roles, reaction roles and role commands."""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.permissions import is_superuser, member_has_perms
from core.bot import bot
from core.helpers import extract_message_from_link, parse_duration, resolve_role

log: logging.Logger = logging.getLogger("debt-collector")


@bot.event
async def on_member_join(member: discord.Member) -> None:
    settings: Dict[str, Any] = bot.settings.peek_settings(member.guild.id)
    join_roles: List[Any] = settings.get("joinroles") or []
    if not join_roles:
        return

    me: Optional[discord.Member] = member.guild.me
    if me is None or not me.guild_permissions.manage_roles:
        return

    roles_to_add: List[discord.Role] = []
    for raw_id in join_roles:
        try:
            role: Optional[discord.Role] = member.guild.get_role(int(raw_id))
        except (TypeError, ValueError):
            continue
        if role is not None and not role.managed and role < me.top_role:
            roles_to_add.append(role)

    if not roles_to_add:
        return

    try:
        await member.add_roles(*roles_to_add, reason="Auto join role")
    except discord.Forbidden:
        log.warning("Join roles blocked by hierarchy in guild %s.", member.guild.id)
    except discord.HTTPException as exc:
        bot.log_error("joinrole", exc, guild=member.guild, user=member)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    if payload.user_id == bot.user.id or not payload.guild_id:
        return
    settings = bot.settings.peek_settings(payload.guild_id)
    reactionroles = settings.get("reactionroles", {})

    key = f"{payload.message_id}_{str(payload.emoji)}"
    role_id = reactionroles.get(key)
    if not role_id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    member = payload.member or guild.get_member(payload.user_id)
    if not member:
        try:
            member = await guild.fetch_member(payload.user_id)
        except discord.HTTPException:
            return
    role = guild.get_role(int(role_id))
    if role and role < guild.me.top_role:
        try:
            await member.add_roles(role, reason="Reaction role")
        except discord.Forbidden:
            log.warning("Reaction role blocked by hierarchy in guild %s.", guild.id)
        except discord.HTTPException as exc:
            bot.log_error("reactionrole:add", exc, guild=guild, user=member)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent) -> None:
    if payload.user_id == bot.user.id or not payload.guild_id:
        return
    settings = bot.settings.peek_settings(payload.guild_id)
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
        try:
            member = await guild.fetch_member(payload.user_id)
        except discord.HTTPException:
            return
    role = guild.get_role(int(role_id))
    if role and role < guild.me.top_role:
        try:
            await member.remove_roles(role, reason="Reaction role")
        except discord.Forbidden:
            log.warning(
                "Reaction role removal blocked by hierarchy in guild %s.", guild.id
            )
        except discord.HTTPException as exc:
            bot.log_error("reactionrole:remove", exc, guild=guild, user=member)


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
        await ctx.send(
            "❌ You need the **Manage Roles** permission to use this command.",
            ephemeral=True,
        )
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        await ctx.send("❌ No role found.", ephemeral=True)
        return
    if resolved.is_default() or resolved.managed:
        await ctx.send(
            "❌ That role is managed by Discord/an integration and cannot be assigned.",
            ephemeral=True,
        )
        return

    if (
        not is_superuser(ctx.author)
        and ctx.guild.owner_id != ctx.author.id
        and resolved >= ctx.author.top_role
    ):
        await ctx.send(
            f"❌ **Hierarchy protection:** **{resolved.name}** is higher than or equal to your highest role — you cannot assign or remove it, even as an Administrator.",
            ephemeral=True,
        )
        return

    if resolved >= ctx.guild.me.top_role:
        await ctx.send(
            "❌ I can't manage that role — it is at or above **my** highest role.",
            ephemeral=True,
        )
        return

    try:
        if action == "add":
            if resolved in member.roles:
                return await ctx.send(
                    f"ℹ️ {member.display_name} already has **{resolved.name}**.",
                    ephemeral=True,
                )
            await member.add_roles(resolved, reason=f"role add by {ctx.author}")
            await ctx.send(
                f"✅ Added **{resolved.name}** to {member.mention}.",
                allowed_mentions=discord.AllowedMentions.none(),
            )
        else:
            if resolved not in member.roles:
                return await ctx.send(
                    f"ℹ️ {member.display_name} does not have **{resolved.name}**.",
                    ephemeral=True,
                )
            await member.remove_roles(resolved, reason=f"role remove by {ctx.author}")
            await ctx.send(
                f"✅ Removed **{resolved.name}** from {member.mention}.",
                allowed_mentions=discord.AllowedMentions.none(),
            )
    except discord.Forbidden:
        await ctx.send(
            "❌ Discord refused that change (missing bot permissions).", ephemeral=True
        )
    except discord.HTTPException:
        await ctx.send(
            "⚠️ Role change failed due to a Discord API error.", ephemeral=True
        )


@bot.hybrid_command(
    name="joinrole", description="Automatically add a role to all new joiners."
)
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

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    joinroles = settings.get("joinroles", [])

    if str(resolved.id) in joinroles:
        joinroles.remove(str(resolved.id))
        await bot.settings.push_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(
            f"✅ **{resolved.name}** will no longer be given to new joiners."
        )
    else:
        joinroles.append(str(resolved.id))
        await bot.settings.push_settings(ctx.guild.id, {"joinroles": joinroles})
        await ctx.send(f"✅ **{resolved.name}** will now be given to all new joiners.")


@bot.hybrid_command(
    name="roleall",
    description="Give members a role, optionally filtering by how recently they joined.",
)
@app_commands.default_permissions(administrator=True)
@commands.guild_only()
@app_commands.describe(
    role="Role mention, role ID, or exact role name.",
    time="Optional time period (e.g., '2 hours', '1d', '30m') to only give role to recent joiners.",
)
async def roleall_cmd(ctx: commands.Context, role: str, *, time: Optional[str] = None):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved: Optional[discord.Role] = resolve_role(ctx.guild, role)
    if resolved is None:
        return await ctx.send("❌ No role found.", ephemeral=True)

    if resolved >= ctx.guild.me.top_role:
        return await ctx.send(
            "❌ I can't manage that role — it is at or above **my** highest role.",
            ephemeral=True,
        )

    if (
        not is_superuser(ctx.author)
        and ctx.guild.owner_id != ctx.author.id
        and resolved >= ctx.author.top_role
    ):
        return await ctx.send(
            f"❌ **Hierarchy protection:** **{resolved.name}** is higher than or equal to your highest role.",
            ephemeral=True,
        )

    threshold_time = None
    if time:
        duration_seconds = parse_duration(time)
        if duration_seconds is None:
            return await ctx.send(
                f"❌ Invalid time format: `{time}`. Use things like `2 hours`, `30m`, `1d`.",
                ephemeral=True,
            )
        threshold_time = datetime.now(timezone.utc) - timedelta(
            seconds=duration_seconds
        )
        await ctx.send(
            f"⏳ Adding **{resolved.name}** to members who joined in the last **{time}**... This may take a while."
        )
    else:
        await ctx.send(
            f"⏳ Adding **{resolved.name}** to all members... This may take a while depending on server size."
        )

    success = 0
    failed = 0

    try:
        if not ctx.guild.chunked:
            await ctx.guild.chunk()
    except Exception as exc:
        bot.log_error("roleall:chunk", exc, guild=ctx.guild)
        return await ctx.send(
            "❌ Failed to fetch all members from Discord. Please try again later.",
            ephemeral=True,
        )

    if threshold_time:
        members_to_update = [
            m
            for m in ctx.guild.members
            if resolved not in m.roles and m.joined_at and m.joined_at >= threshold_time
        ]
    else:
        members_to_update = [m for m in ctx.guild.members if resolved not in m.roles]

    async def _add_role(m: discord.Member):
        try:
            await m.add_roles(resolved, reason=f"roleall by {ctx.author}")
            return True
        except Exception as exc:
            bot.log_error("roleall:add", exc, guild=ctx.guild, user=m)
            return False

    chunk_size = 10
    for i in range(0, len(members_to_update), chunk_size):
        chunk = members_to_update[i : i + chunk_size]

        # Run API calls and the rate-limit delay concurrently
        results, _ = await asyncio.gather(
            asyncio.gather(*(_add_role(m) for m in chunk)),
            asyncio.sleep(0.1),  # Minimum time per chunk to avoid rate limits
        )

        success += sum(1 for r in results if r)
        failed += sum(1 for r in results if not r)

    await ctx.channel.send(
        f"✅ Finished adding **{resolved.name}**! Success: {success}, Failed: {failed}"
    )


@bot.hybrid_group(
    name="reactionrole",
    description="Manage reaction roles",
    fallback="set",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_roles=True)
@commands.guild_only()
@app_commands.describe(
    link="Link to the message", emoji="Reaction emoji", role="Role to assign"
)
async def reactionrole_group(
    ctx: commands.Context, link: str, emoji: str, *, role: discord.Role
):
    if not member_has_perms(ctx.author, manage_roles=True, administrator=True):
        await ctx.send("❌ You need Administrator permission.", ephemeral=True)
        return

    resolved = role

    msg = await extract_message_from_link(ctx, link)
    if not msg:
        return await ctx.send(
            "❌ Could not find message from the link. Make sure it's in this server.",
            ephemeral=True,
        )

    try:
        await msg.add_reaction(emoji)
    except discord.HTTPException:
        return await ctx.send(
            "❌ Failed to add reaction. Ensure it is a valid default emoji or a server emoji I have access to.",
            ephemeral=True,
        )

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    reactionroles = settings.get("reactionroles", {})
    key = f"{msg.id}_{emoji}"

    if key in reactionroles and reactionroles[key] == str(resolved.id):
        del reactionroles[key]
        await bot.settings.push_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(
            f"✅ Removed reaction role **{resolved.name}** from that message."
        )
    else:
        reactionroles[key] = str(resolved.id)
        await bot.settings.push_settings(ctx.guild.id, {"reactionroles": reactionroles})
        await ctx.send(
            f"✅ Added reaction role **{resolved.name}** to that message. Users who react with {emoji} will receive the role."
        )


def _split_rr_key(key: str) -> Tuple[str, str]:
    """Reaction-role keys are stored as "<message_id>_<emoji>"."""
    message_id, _, emoji = key.partition("_")
    return message_id, emoji


@reactionrole_group.command(
    name="list", description="List every reaction role in this server"
)
async def reactionrole_list(ctx: commands.Context):
    if not member_has_perms(ctx.author, manage_roles=True):
        return await ctx.send("❌ You need Manage Roles permission.", ephemeral=True)

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    reactionroles: Dict[str, Any] = settings.get("reactionroles", {}) or {}
    if not reactionroles:
        return await ctx.send(
            "No reaction roles are set up yet — add one with `/reactionrole set`.",
            ephemeral=True,
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


@reactionrole_group.command(
    name="remove", description="Remove reaction role(s) from a message"
)
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
        return await ctx.send(
            "❌ Give me a message ID or a message link.", ephemeral=True
        )

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    reactionroles: Dict[str, Any] = dict(settings.get("reactionroles", {}) or {})
    targets: List[str] = [
        key
        for key in reactionroles
        if _split_rr_key(key)[0] == message_id
        and (emoji is None or _split_rr_key(key)[1] == emoji.strip())
    ]
    if not targets:
        return await ctx.send(
            "❌ No matching reaction role found — check `/reactionrole list`.",
            ephemeral=True,
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


@bot.hybrid_command(
    name="reaction",
    description="Make the bot add an emoji reaction to a linked message",
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(link="Link to the message", emoji="Reaction emoji")
async def reaction_cmd(ctx: commands.Context, link: str, emoji: str):
    if not member_has_perms(ctx.author, manage_messages=True):
        await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
        return

    msg = await extract_message_from_link(ctx, link)
    if not msg:
        return await ctx.send(
            "❌ Could not find message from the link. Make sure it's in this server.",
            ephemeral=True,
        )

    try:
        await msg.add_reaction(emoji)
        await ctx.send(f"✅ Reacted to the message with {emoji}", ephemeral=True)
    except discord.HTTPException:
        await ctx.send(
            "❌ Failed to add reaction. Ensure it is a valid default emoji or a server emoji I have access to.",
            ephemeral=True,
        )


# --------------------------------------------------------------------------- #
# Role audit
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="inrole", description="List every member holding a role")
@commands.guild_only()
@app_commands.describe(
    role="The role to audit", show_ids="Include user IDs for exports"
)
async def inrole_cmd(
    ctx: commands.Context,
    role: discord.Role,
    show_ids: bool = False,
) -> None:
    members: List[discord.Member] = sorted(
        role.members, key=lambda m: (m.joined_at or datetime.now(timezone.utc))
    )
    if not members:
        return await ctx.send(
            f"\U0001f4ed Nobody currently has {role.mention}.", ephemeral=True
        )

    if len(members) > 500 and not member_has_perms(ctx.author, manage_roles=True):
        return await ctx.send(
            f"\u274c {role.mention} has **{len(members)}** members - "
            "you need **Manage Roles** to list a role that large.",
            ephemeral=True,
        )

    lines: List[str] = []
    for index, member in enumerate(members, start=1):
        joined: str = (
            f"<t:{int(member.joined_at.timestamp())}:R>"
            if member.joined_at
            else "unknown"
        )
        identifier: str = f" - `{member.id}`" if show_ids else ""
        lines.append(
            f"**{index}.** {member.mention} - `{member}`{identifier} - joined {joined}"
        )

    pages = build_pages(
        f"{role.name} - {len(members)} member{'s' if len(members) != 1 else ''}",
        lines,
        role.color if role.color.value else discord.Color.blurple(),
        per_page=10,
        footer=f"Role created {role.created_at:%Y-%m-%d} - position {role.position}",
    )
    await send_pages(ctx, pages, ephemeral=True)
