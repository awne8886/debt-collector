"""Raid detection and lockdown."""

import logging
import time
from typing import Any, Dict, List, Literal, Optional

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import custom_has_permissions, is_superuser, member_has_perms
from core.bot import bot
from core.modlog import record_case, send_modlog

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Raid detection and lockdown
# --------------------------------------------------------------------------- #

RAID_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "auto": True,
    "until": 0,
    "join_threshold": 8,
    "window": 20,
    "min_account_age_hours": 24,
    "action": "quarantine",
    "quarantine_role_id": None,
}
_RAID_JOINS: Dict[int, List[float]] = {}


def _raid_config(guild_id: int) -> Dict[str, Any]:
    stored: Dict[str, Any] = bot.settings.peek_settings(guild_id).get("raid") or {}
    config: Dict[str, Any] = dict(RAID_DEFAULTS)
    config.update({k: v for k, v in stored.items() if k in RAID_DEFAULTS})
    return config


@bot.hybrid_group(
    name="raid", description="Raid detection and lockdown", fallback="status"
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@custom_has_permissions(manage_guild=True)
async def raid_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _raid_config(ctx.guild.id)
    active: bool = bool(config["enabled"]) and float(config["until"]) > time.time()
    role: Optional[discord.Role] = (
        ctx.guild.get_role(int(config["quarantine_role_id"]))
        if config.get("quarantine_role_id")
        else None
    )
    recent: int = len(
        [t for t in _RAID_JOINS.get(ctx.guild.id, []) if time.time() - t < 60]
    )

    embed: discord.Embed = discord.Embed(
        title="\U0001f6e1\ufe0f Raid protection",
        color=discord.Color.red() if active else discord.Color.green(),
    )
    embed.add_field(
        name="State",
        value=(
            f"\U0001f534 **ACTIVE** until <t:{int(config['until'])}:R>"
            if active
            else "\U0001f7e2 standby"
        ),
        inline=False,
    )
    embed.add_field(
        name="Trigger",
        value=f"{config['join_threshold']} joins / {config['window']}s"
        + (" - auto-arm on" if config["auto"] else " - auto-arm off"),
        inline=True,
    )
    embed.add_field(
        name="Response",
        value=f"`{config['action']}` accounts younger than {config['min_account_age_hours']}h",
        inline=True,
    )
    embed.add_field(
        name="Quarantine role",
        value=role.mention if role is not None else "not configured",
        inline=True,
    )
    embed.set_footer(text=f"{recent} joins in the last 60 seconds")
    await ctx.send(embed=embed, ephemeral=True)


@raid_group.command(name="on", description="Engage raid mode manually")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(minutes="How long to stay engaged (default 30)")
async def raid_on(
    ctx: commands.Context,
    minutes: app_commands.Range[int, 1, 720] = 30,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    until: int = int(time.time()) + int(minutes) * 60
    await bot.settings.push_fields(
        ctx.guild.id, {"raid.enabled": True, "raid.until": until}
    )
    log.warning("Raid mode engaged in guild %s by %s.", ctx.guild.id, ctx.author)
    await send_modlog(
        ctx.guild,
        "\U0001f6e1\ufe0f Raid mode engaged",
        f"Engaged manually by {ctx.author.mention} until <t:{until}:f>.",
        discord.Color.red(),
    )
    await ctx.send(
        f"\U0001f6e1\ufe0f Raid mode **engaged** until <t:{until}:R>.", ephemeral=True
    )


@raid_group.command(name="off", description="Disengage raid mode")
@custom_has_permissions(manage_guild=True)
async def raid_off(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await bot.settings.push_fields(
        ctx.guild.id, {"raid.enabled": False, "raid.until": 0}
    )
    _RAID_JOINS.pop(ctx.guild.id, None)
    log.info("Raid mode disengaged in guild %s by %s.", ctx.guild.id, ctx.author)
    await send_modlog(
        ctx.guild,
        "\U0001f6e1\ufe0f Raid mode disengaged",
        f"Disengaged by {ctx.author.mention}.",
        discord.Color.green(),
    )
    await ctx.send("\U0001f7e2 Raid mode **disengaged**.", ephemeral=True)


@raid_group.command(name="config", description="Tune raid detection thresholds")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    join_threshold="Joins needed to auto-arm",
    window="Detection window in seconds",
    min_account_age_hours="Accounts younger than this are actioned",
    action="What to do with a flagged account",
    quarantine_role="Role applied when action is quarantine",
    auto="Whether to auto-arm on a join spike",
)
async def raid_config(
    ctx: commands.Context,
    join_threshold: Optional[app_commands.Range[int, 3, 100]] = None,
    window: Optional[app_commands.Range[int, 5, 300]] = None,
    min_account_age_hours: Optional[app_commands.Range[int, 0, 8760]] = None,
    action: Optional[Literal["quarantine", "kick", "ban"]] = None,
    quarantine_role: Optional[discord.Role] = None,
    auto: Optional[bool] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    fields: Dict[str, Any] = {}
    if join_threshold is not None:
        fields["raid.join_threshold"] = int(join_threshold)
    if window is not None:
        fields["raid.window"] = int(window)
    if min_account_age_hours is not None:
        fields["raid.min_account_age_hours"] = int(min_account_age_hours)
    if action is not None:
        fields["raid.action"] = str(action)
    if auto is not None:
        fields["raid.auto"] = bool(auto)
    if quarantine_role is not None:
        if quarantine_role.managed or quarantine_role >= ctx.guild.me.top_role:
            return await ctx.send(
                "\u274c I can't assign that role - it's managed or above me in the hierarchy.",
                ephemeral=True,
            )
        fields["raid.quarantine_role_id"] = str(quarantine_role.id)

    if not fields:
        return await ctx.send(
            "\u274c Give me at least one setting to change.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    mark: str = "\u2705" if saved else "\u26a0\ufe0f"
    tail: str = "." if saved else " in memory only - the database write failed."
    await ctx.send(
        f"{mark} Updated **{len(fields)}** raid setting(s){tail}",
        ephemeral=True,
    )


async def _raid_action(
    guild: discord.Guild, member: discord.Member, config: Dict[str, Any]
) -> None:
    action: str = str(config.get("action") or "quarantine")
    reason: str = "Raid protection - new account during active raid mode"
    try:
        if action == "ban":
            await guild.ban(member, reason=reason, delete_message_seconds=3600)
        elif action == "kick":
            await member.kick(reason=reason)
        else:
            role_id: Any = config.get("quarantine_role_id")
            role: Optional[discord.Role] = (
                guild.get_role(int(role_id)) if role_id else None
            )
            if role is None or role >= guild.me.top_role:
                log.warning(
                    "Quarantine role unusable in guild %s - falling back to kick.",
                    guild.id,
                )
                await member.kick(reason=reason + " (quarantine role unavailable)")
                action = "kick"
            else:
                await member.add_roles(role, reason=reason)
    except discord.Forbidden:
        log.warning("Raid action '%s' forbidden in guild %s.", action, guild.id)
        return
    except discord.HTTPException as exc:
        bot.log_error("raid:action", exc, guild=guild, user=member)
        return

    await record_case(guild, f"raid:{action}", guild.me, member, reason)
    await send_modlog(
        guild,
        f"\U0001f6e1\ufe0f Raid action - {action}",
        f"{member.mention} (`{member}`) - account created "
        f"<t:{int(member.created_at.timestamp())}:R>.",
        discord.Color.red(),
    )


@bot.listen("on_member_join")
async def raid_watch(member: discord.Member) -> None:
    if member.bot:
        return
    guild: discord.Guild = member.guild
    config: Dict[str, Any] = _raid_config(guild.id)
    now: float = time.time()

    window: int = int(config["window"])
    joins: List[float] = [t for t in _RAID_JOINS.get(guild.id, []) if now - t <= window]
    joins.append(now)
    _RAID_JOINS[guild.id] = joins[-200:]

    active: bool = bool(config["enabled"]) and float(config["until"]) > now

    if not active and config["auto"] and len(joins) >= int(config["join_threshold"]):
        until: int = int(now) + 1800
        await bot.settings.push_fields(
            guild.id, {"raid.enabled": True, "raid.until": until}
        )
        active = True
        log.warning(
            "Raid auto-armed in guild %s (%d joins in %ds).",
            guild.id,
            len(joins),
            window,
        )
        await send_modlog(
            guild,
            "\U0001f6a8 Raid mode auto-engaged",
            f"**{len(joins)}** joins in {window}s exceeded the threshold. "
            f"Active until <t:{until}:R>. Disable with `/raid off`.",
            discord.Color.red(),
        )

    if not active:
        return
    if len(member.roles) > 1 or is_superuser(member):
        return

    min_age: float = float(config["min_account_age_hours"]) * 3600
    account_age: float = (discord.utils.utcnow() - member.created_at).total_seconds()
    if account_age < min_age:
        await _raid_action(guild, member, config)
