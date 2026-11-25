"""Quarantine / unquarantine."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import custom_has_permissions, member_has_perms, mod_block_reason
from core.bot import bot
from core.modlog import record_case, send_modlog
from features.raid import _raid_config

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Reversible quarantine
# --------------------------------------------------------------------------- #


async def _ensure_quarantine_role(guild: discord.Guild) -> Optional[discord.Role]:
    """Return the configured quarantine role, creating and locking it down if absent."""
    config: Dict[str, Any] = _raid_config(guild.id)
    role_id: Any = config.get("quarantine_role_id")
    role: Optional[discord.Role] = guild.get_role(int(role_id)) if role_id else None
    if role is not None and role < guild.me.top_role:
        return role

    if not guild.me.guild_permissions.manage_roles:
        return None
    try:
        role = await guild.create_role(
            name="Quarantined",
            colour=discord.Colour.dark_grey(),
            reason="Quarantine role created automatically",
        )
    except discord.DiscordException as exc:
        bot.log_error("quarantine:create_role", exc, guild=guild)
        return None

    denied: discord.PermissionOverwrite = discord.PermissionOverwrite(
        send_messages=False,
        send_messages_in_threads=False,
        create_public_threads=False,
        create_private_threads=False,
        add_reactions=False,
        speak=False,
        connect=False,
    )
    for channel in guild.channels:
        try:
            await channel.set_permissions(
                role, overwrite=denied, reason="Quarantine lockdown"
            )
        except discord.DiscordException:
            continue
        await asyncio.sleep(0.3)

    await bot.settings.push_fields(guild.id, {"raid.quarantine_role_id": str(role.id)})
    log.info("Created quarantine role %s in guild %s.", role.id, guild.id)
    return role


@bot.hybrid_command(
    name="quarantine", description="Isolate a member without banning them (reversible)"
)
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
@custom_has_permissions(moderate_members=True)
@commands.bot_has_permissions(manage_roles=True)
@app_commands.describe(user="Who to isolate", reason="Why they are being isolated")
async def quarantine_cmd(
    ctx: commands.Context,
    user: discord.Member,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
) -> None:
    if not member_has_perms(ctx.author, moderate_members=True):
        return await ctx.send(
            "\u274c You need the **Timeout Members** permission.", ephemeral=True
        )
    block: Optional[str] = mod_block_reason(ctx.author, user, ctx.guild.me)
    if block:
        return await ctx.send(f"\u274c {block}", ephemeral=True)

    await ctx.defer(ephemeral=True)

    role: Optional[discord.Role] = await _ensure_quarantine_role(ctx.guild)
    if role is None:
        return await ctx.send(
            "\u274c I couldn't set up a quarantine role. Grant me **Manage Roles** "
            "or configure one with `/raid config quarantine_role:`.",
            ephemeral=True,
        )
    if role in user.roles:
        return await ctx.send(
            f"\u2139\ufe0f {user.mention} is already quarantined.", ephemeral=True
        )

    if dm:
        try:
            await user.send(
                f"You have been quarantined in **{ctx.guild.name}**. Reason: {reason}"
            )
        except discord.DiscordException:
            pass

    removable: List[discord.Role] = [
        r
        for r in user.roles
        if not r.is_default() and not r.managed and r < ctx.guild.me.top_role
    ]
    snapshot: List[str] = [str(r.id) for r in removable]

    try:
        if removable:
            await user.remove_roles(*removable, reason=f"Quarantine by {ctx.author}")
        await user.add_roles(role, reason=f"Quarantine by {ctx.author}: {reason}")
    except discord.Forbidden:
        return await ctx.send(
            "\u274c I'm missing the permissions or hierarchy to change that member's roles.",
            ephemeral=True,
        )
    except discord.HTTPException as exc:
        bot.log_error("quarantine", exc, guild=ctx.guild, user=user)
        return await ctx.send(
            "\u26a0\ufe0f Discord rejected the role change.", ephemeral=True
        )

    stored: Dict[str, Any] = dict(
        bot.settings.peek_settings(ctx.guild.id).get("quarantined") or {}
    )
    stored[str(user.id)] = {
        "roles": snapshot,
        "at": int(time.time()),
        "by": str(ctx.author),
        "reason": str(reason or "")[:300],
    }
    await bot.settings.push_fields(ctx.guild.id, {"quarantined": stored})

    clean: str = discord.utils.escape_mentions(str(reason or "No reason given"))[:400]
    case_id: Optional[int] = await record_case(
        ctx.guild, "quarantine", ctx.author, user, clean
    )
    try:
        await user.send(
            f"You have been quarantined in **{ctx.guild.name}**.\n"
            f"**Reason:** {clean}\n"
            "Your roles are stored and will be restored when a moderator releases you."
        )
    except (discord.Forbidden, discord.HTTPException):
        pass

    log.info("Quarantined %s in guild %s by %s.", user.id, ctx.guild.id, ctx.author)
    await send_modlog(
        ctx.guild,
        f"\U0001f512 Quarantine{f' - case #{case_id}' if case_id else ''}",
        f"**Reason:** {clean}",
        discord.Color.dark_orange(),
        [
            ("Moderator", f"{ctx.author.mention} (`{ctx.author}`)"),
            ("Target", f"{user.mention} (`{user}`)"),
            ("Roles stored", str(len(snapshot))),
        ],
    )
    await ctx.send(
        f"\U0001f512 Quarantined **{user}** and stored **{len(snapshot)}** role(s).",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.hybrid_command(
    name="unquarantine", description="Release a member and restore their roles"
)
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
@custom_has_permissions(moderate_members=True)
@commands.bot_has_permissions(manage_roles=True)
@app_commands.describe(user="Who to release")
async def unquarantine_cmd(ctx: commands.Context, user: discord.Member) -> None:
    if not member_has_perms(ctx.author, moderate_members=True):
        return await ctx.send(
            "\u274c You need the **Timeout Members** permission.", ephemeral=True
        )
    await ctx.defer(ephemeral=True)

    stored: Dict[str, Any] = dict(
        bot.settings.peek_settings(ctx.guild.id).get("quarantined") or {}
    )
    entry: Optional[Dict[str, Any]] = stored.pop(str(user.id), None)
    config: Dict[str, Any] = _raid_config(ctx.guild.id)
    role_id: Any = config.get("quarantine_role_id")
    role: Optional[discord.Role] = ctx.guild.get_role(int(role_id)) if role_id else None

    if entry is None and (role is None or role not in user.roles):
        return await ctx.send(
            f"\u2139\ufe0f {user.mention} isn't quarantined.", ephemeral=True
        )

    restore: List[discord.Role] = []
    for raw_id in (entry or {}).get("roles", []):
        candidate: Optional[discord.Role] = ctx.guild.get_role(int(raw_id))
        if (
            candidate is not None
            and not candidate.managed
            and candidate < ctx.guild.me.top_role
        ):
            restore.append(candidate)

    try:
        if role is not None and role in user.roles:
            await user.remove_roles(role, reason=f"Quarantine lifted by {ctx.author}")
        if restore:
            await user.add_roles(*restore, reason=f"Quarantine lifted by {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(
            "\u274c I'm missing the permissions or hierarchy to restore those roles.",
            ephemeral=True,
        )
    except discord.HTTPException as exc:
        bot.log_error("unquarantine", exc, guild=ctx.guild, user=user)
        return await ctx.send(
            "\u26a0\ufe0f Discord rejected the role change.", ephemeral=True
        )

    await bot.settings.push_fields(ctx.guild.id, {"quarantined": stored})
    case_id: Optional[int] = await record_case(
        ctx.guild, "unquarantine", ctx.author, user, "Quarantine lifted"
    )
    log.info("Released %s in guild %s by %s.", user.id, ctx.guild.id, ctx.author)
    await send_modlog(
        ctx.guild,
        f"\U0001f513 Quarantine lifted{f' - case #{case_id}' if case_id else ''}",
        f"{user.mention} was released by {ctx.author.mention}.",
        discord.Color.green(),
        [("Roles restored", str(len(restore)))],
    )
    await ctx.send(
        f"\U0001f513 Released **{user}** and restored **{len(restore)}** role(s).",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
