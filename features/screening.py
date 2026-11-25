"""Join screening (ban-evasion and pattern scoring)."""

import asyncio
import copy
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from pymongo.errors import PyMongoError

from core.config import DEFAULT_SETTINGS
from core.permissions import custom_has_permissions, is_superuser, member_has_perms
from core.bot import bot
from core.modlog import record_case, send_modlog
from features.quarantine import _ensure_quarantine_role

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Join screening - alt accounts and ban evasion
# --------------------------------------------------------------------------- #

SCREENING_ACTIONS: Tuple[str, ...] = ("log", "quarantine", "kick")
SCREENING_WEIGHTS: Dict[str, int] = {
    "new_account": 3,
    "default_avatar": 1,
    "name_pattern": 3,
    "created_near_ban": 2,
    "no_flags": 1,
}
EVASION_BAN_ACTIONS: Tuple[str, ...] = ("ban", "tempban", "massban", "raid:ban")


def _screening_config(guild_id: int) -> Dict[str, Any]:
    stored: Dict[str, Any] = bot.settings.peek_settings(guild_id).get("screening") or {}
    config: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS["screening"])
    for key, value in stored.items():
        if key in config:
            config[key] = value
    if config["action"] not in SCREENING_ACTIONS:
        config["action"] = "log"
    return config


def _normalize_name(raw: str) -> str:
    """Fold a display name so lookalikes collapse together."""
    lowered: str = (raw or "").casefold()
    stripped: str = re.sub(r"[^a-z0-9]+", "", lowered)
    return stripped


async def _recent_ban_records(guild_id: int, days: int) -> List[Dict[str, Any]]:
    """Ban entries from the case book within the window, newest first."""
    since: datetime = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    try:
        return await asyncio.to_thread(
            lambda: list(
                bot.settings.cases.find(
                    {
                        "guildid": str(guild_id),
                        "action": {"$in": list(EVASION_BAN_ACTIONS)},
                        "at": {"$gte": since},
                    }
                )
                .sort("case_id", -1)
                .limit(300)
            )
        )
    except PyMongoError as exc:
        bot.log_error("screening:cases", exc)
        return []


async def _screen_member(
    member: discord.Member, config: Dict[str, Any]
) -> Tuple[int, List[str], List[Dict[str, Any]]]:
    """Score a joining account. Returns (score, reasons, evasion matches)."""
    score: int = 0
    reasons: List[str] = []
    now: datetime = discord.utils.utcnow()

    age_hours: float = (now - member.created_at).total_seconds() / 3600.0
    min_age: float = float(config["min_account_age_hours"])
    if min_age > 0 and age_hours < min_age:
        score += SCREENING_WEIGHTS["new_account"]
        reasons.append(f"account is {age_hours:.1f}h old (under {min_age:.0f}h)")

    if config["flag_default_avatar"] and member.avatar is None:
        score += SCREENING_WEIGHTS["default_avatar"]
        reasons.append("using the default avatar")

    patterns: List[str] = [str(p) for p in (config.get("name_patterns") or [])]
    haystack: str = f"{member.name} {member.display_name}"
    for raw_pattern in patterns:
        try:
            if re.search(raw_pattern, haystack, re.IGNORECASE):
                score += SCREENING_WEIGHTS["name_pattern"]
                reasons.append(f"name matches `{raw_pattern[:40]}`")
                break
        except re.error:
            log.warning(
                "Invalid screening name pattern in guild %s: %r",
                member.guild.id,
                raw_pattern,
            )

    if config["flag_no_public_flags"] and not member.public_flags.value:
        score += SCREENING_WEIGHTS["no_flags"]
        reasons.append("no account badges")

    evasion: List[Dict[str, Any]] = []
    records: List[Dict[str, Any]] = await _recent_ban_records(
        member.guild.id, int(config["evasion_days"])
    )
    joiner_key: str = _normalize_name(member.name)
    display_key: str = _normalize_name(member.display_name)
    for record in records:
        target_key: str = _normalize_name(str(record.get("target") or "").split("#")[0])
        if not target_key or len(target_key) < 3:
            continue
        if target_key in (joiner_key, display_key):
            evasion.append(record)
            continue
        banned_at: Any = record.get("at")
        if isinstance(banned_at, datetime):
            banned_utc: datetime = banned_at.replace(
                tzinfo=banned_at.tzinfo or timezone.utc
            )
            gap: float = abs((member.created_at - banned_utc).total_seconds())
            if gap < float(config["created_near_ban_minutes"]) * 60:
                score += SCREENING_WEIGHTS["created_near_ban"]
                reasons.append("account created around the time of a recent ban")
                break

    if evasion:
        score += SCREENING_WEIGHTS["name_pattern"]
        reasons.append(f"name matches {len(evasion)} recently banned account(s)")

    return score, reasons, evasion


async def _screening_act(
    member: discord.Member, config: Dict[str, Any], score: int, reasons: List[str]
) -> str:
    """Apply the configured response. Returns the action actually taken."""
    action: str = str(config["action"])
    reason_text: str = f"Join screening score {score}: " + "; ".join(reasons)[:300]

    if action == "log":
        return "log"
    try:
        if action == "kick":
            await member.kick(reason=reason_text)
            return "kick"
        role: Optional[discord.Role] = await _ensure_quarantine_role(member.guild)
        if role is None:
            log.warning(
                "Screening wanted to quarantine in guild %s but no role was usable.",
                member.guild.id,
            )
            return "log"
        await member.add_roles(role, reason=reason_text)
        return "quarantine"
    except discord.Forbidden:
        log.warning(
            "Screening action '%s' forbidden in guild %s.", action, member.guild.id
        )
        return "log"
    except discord.HTTPException as exc:
        bot.log_error("screening:action", exc, guild=member.guild, user=member)
        return "log"


@bot.listen("on_member_join")
async def screening_watch(member: discord.Member) -> None:
    if member.bot:
        return
    config: Dict[str, Any] = _screening_config(member.guild.id)
    if not config.get("enabled"):
        return
    if is_superuser(member) or len(member.roles) > 1:
        return

    score, reasons, evasion = await _screen_member(member, config)
    threshold: int = int(config["threshold"])
    if score < threshold and not evasion:
        return

    taken: str = "log"
    if score >= threshold:
        taken = await _screening_act(member, config, score, reasons)

    fields: List[Tuple[str, str]] = [
        ("Member", f"{member.mention} (`{member}`)"),
        ("Account created", f"<t:{int(member.created_at.timestamp())}:R>"),
        ("Score", f"{score} (threshold {threshold})"),
        ("Action", f"`{taken}`"),
    ]
    if evasion:
        listed: str = ", ".join(
            f"`{record.get('target')}`"
            for record in evasion[:4]
            if record.get("target")
        )
        fields.append(("Possible evasion of", listed[:1024] or "unknown"))

    await send_modlog(
        member.guild,
        "\U0001f50e Join flagged by screening",
        "\n".join(f"\u2022 {reason}" for reason in reasons)[:2000]
        or "No specific signals.",
        discord.Color.orange() if taken == "log" else discord.Color.red(),
        fields[:6],
    )
    if taken != "log":
        await record_case(
            member.guild,
            f"screening:{taken}",
            member.guild.me,
            member,
            "; ".join(reasons)[:400],
        )
    log.info(
        "Screening flagged %s in guild %s (score %d, action %s).",
        member.id,
        member.guild.id,
        score,
        taken,
    )


@bot.hybrid_group(
    name="screening",
    description="Screen new joins for alt accounts and ban evasion",
    fallback="status",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
@custom_has_permissions(manage_guild=True)
async def screening_group(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _screening_config(ctx.guild.id)
    patterns: List[str] = [str(p) for p in (config.get("name_patterns") or [])]

    embed: discord.Embed = discord.Embed(
        title="\U0001f50e Join screening",
        color=discord.Color.green() if config["enabled"] else discord.Color.greyple(),
    )
    embed.add_field(
        name="State",
        value="\U0001f7e2 active" if config["enabled"] else "\u26aa off",
        inline=True,
    )
    embed.add_field(name="Action", value=f"`{config['action']}`", inline=True)
    embed.add_field(
        name="Threshold", value=f"score \u2265 **{config['threshold']}**", inline=True
    )
    embed.add_field(
        name="Signals",
        value=(
            f"\u2022 younger than **{config['min_account_age_hours']}h** "
            f"(+{SCREENING_WEIGHTS['new_account']})\n"
            f"\u2022 default avatar: "
            f"{'on' if config['flag_default_avatar'] else 'off'} "
            f"(+{SCREENING_WEIGHTS['default_avatar']})\n"
            f"\u2022 no account badges: "
            f"{'on' if config['flag_no_public_flags'] else 'off'} "
            f"(+{SCREENING_WEIGHTS['no_flags']})\n"
            f"\u2022 created within **{config['created_near_ban_minutes']}m** of a ban "
            f"(+{SCREENING_WEIGHTS['created_near_ban']})\n"
            f"\u2022 name pattern match (+{SCREENING_WEIGHTS['name_pattern']})"
        ),
        inline=False,
    )
    embed.add_field(
        name=f"Name patterns ({len(patterns)})",
        value=", ".join(f"`{p[:30]}`" for p in patterns[:10]) or "*none*",
        inline=False,
    )
    embed.set_footer(
        text=f"Evasion lookback {config['evasion_days']}d \u00b7 "
        f"{ctx.clean_prefix}screening test <user> to dry-run"
    )
    await ctx.send(embed=embed, ephemeral=True)


@screening_group.command(name="config", description="Tune join screening")
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    enabled="Turn screening on or off",
    action="What to do when the score crosses the threshold",
    threshold="Score needed to act",
    min_account_age_hours="Accounts younger than this score points",
    default_avatar="Score accounts using the default avatar",
    no_badges="Score accounts with no profile badges",
    evasion_days="How far back to compare names against bans",
)
async def screening_config(
    ctx: commands.Context,
    enabled: Optional[bool] = None,
    action: Optional[Literal["log", "quarantine", "kick"]] = None,
    threshold: Optional[app_commands.Range[int, 1, 20]] = None,
    min_account_age_hours: Optional[app_commands.Range[int, 0, 8760]] = None,
    default_avatar: Optional[bool] = None,
    no_badges: Optional[bool] = None,
    evasion_days: Optional[app_commands.Range[int, 1, 365]] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    fields: Dict[str, Any] = {}
    if enabled is not None:
        fields["screening.enabled"] = bool(enabled)
    if action is not None:
        fields["screening.action"] = str(action)
    if threshold is not None:
        fields["screening.threshold"] = int(threshold)
    if min_account_age_hours is not None:
        fields["screening.min_account_age_hours"] = int(min_account_age_hours)
    if default_avatar is not None:
        fields["screening.flag_default_avatar"] = bool(default_avatar)
    if no_badges is not None:
        fields["screening.flag_no_public_flags"] = bool(no_badges)
    if evasion_days is not None:
        fields["screening.evasion_days"] = int(evasion_days)

    if not fields:
        return await ctx.send(
            "\u274c Give me at least one setting to change.", ephemeral=True
        )
    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    log.info("Screening config updated in guild %s by %s.", ctx.guild.id, ctx.author)
    mark: str = "\u2705" if saved else "\u26a0\ufe0f"
    tail: str = "." if saved else " in memory only - the database write failed."
    await ctx.send(
        f"{mark} Updated **{len(fields)}** screening setting(s){tail}", ephemeral=True
    )


@screening_group.command(
    name="pattern", description="Add or remove a suspicious-name pattern"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    action="Add or remove", pattern="A regular expression matched against names"
)
async def screening_pattern(
    ctx: commands.Context, action: Literal["add", "remove"], *, pattern: str
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    clean: str = pattern.strip()[:100]
    if not clean:
        return await ctx.send("\u274c Give me a pattern.", ephemeral=True)
    try:
        re.compile(clean)
    except re.error as exc:
        return await ctx.send(f"\u274c Invalid regex: `{exc}`.", ephemeral=True)

    config: Dict[str, Any] = _screening_config(ctx.guild.id)
    patterns: List[str] = [str(p) for p in (config.get("name_patterns") or [])]

    if action == "add":
        if clean in patterns:
            return await ctx.send(
                "\u2139\ufe0f That pattern is already listed.", ephemeral=True
            )
        if len(patterns) >= 25:
            return await ctx.send(
                "\u274c Limit of 25 patterns reached.", ephemeral=True
            )
        patterns.append(clean)
        outcome: str = f"\u2705 Added pattern `{clean[:60]}`."
    else:
        if clean not in patterns:
            return await ctx.send("\u274c That pattern isn't listed.", ephemeral=True)
        patterns.remove(clean)
        outcome = f"\U0001f5d1\ufe0f Removed pattern `{clean[:60]}`."

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"screening.name_patterns": patterns}
    )
    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@screening_group.command(
    name="test", description="Show how a member would score, without acting"
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(user="Which member to score")
async def screening_test(ctx: commands.Context, user: discord.Member) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    await ctx.defer(ephemeral=True)

    config: Dict[str, Any] = _screening_config(ctx.guild.id)
    score, reasons, evasion = await _screen_member(user, config)
    threshold: int = int(config["threshold"])
    would_act: bool = score >= threshold

    embed: discord.Embed = discord.Embed(
        title=f"Screening dry-run \u00b7 {user}",
        description="\n".join(f"\u2022 {reason}" for reason in reasons)
        or "No signals matched.",
        color=discord.Color.red() if would_act else discord.Color.green(),
    )
    embed.add_field(
        name="Score", value=f"**{score}** / threshold {threshold}", inline=True
    )
    embed.add_field(
        name="Would act",
        value=f"yes - `{config['action']}`" if would_act else "no",
        inline=True,
    )
    embed.add_field(
        name="Account created",
        value=f"<t:{int(user.created_at.timestamp())}:R>",
        inline=True,
    )
    if evasion:
        embed.add_field(
            name="Name matches recent bans",
            value=", ".join(f"`{r.get('target')}`" for r in evasion[:5])[:1024],
            inline=False,
        )
    embed.set_footer(text="Nothing was applied - this is a preview only")
    await ctx.send(embed=embed, ephemeral=True)
