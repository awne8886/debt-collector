"""Automod filters, escalation and the `automod` group."""

import logging
import re
import time
from datetime import timedelta
from typing import Any, Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import is_superuser, member_has_perms
from core.bot import bot
from core.modlog import record_case, send_modlog
from features.quarantine import _ensure_quarantine_role

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Automod
# --------------------------------------------------------------------------- #

INVITE_RE = re.compile(
    r"(discord\.(gg|io|me|li)/|discordapp\.com/invite/)", re.IGNORECASE
)
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

AUTOMOD_RULES: Tuple[str, ...] = ("invites", "links", "spam", "caps", "mentions")


def _automod_exempt(member: discord.Member, config: Dict[str, Any]) -> bool:
    if is_superuser(member) or member.guild_permissions.manage_messages:
        return True
    exempt = set(str(r) for r in (config.get("exempt_roles") or []))
    return bool(exempt and {str(r.id) for r in member.roles} & exempt)


async def _handle_automod(message: discord.Message) -> bool:
    """Returns True when the message was removed and further handling should stop."""
    assert message.guild is not None
    config = (await bot.settings.fetch_settings(message.guild.id)).get("automod") or {}
    if not any(config.get(rule) for rule in AUTOMOD_RULES):
        return False
    if not isinstance(message.author, discord.Member) or _automod_exempt(
        message.author, config
    ):
        return False

    content = message.content or ""
    violation: Optional[str] = None

    if config.get("invites") and INVITE_RE.search(content):
        violation = "server invite"
    elif config.get("links") and URL_RE.search(content):
        violation = "link"
    elif config.get("mentions") and len(message.mentions) + len(
        message.role_mentions
    ) >= int(config.get("mention_limit") or 5):
        violation = "mass mention"
    elif (
        config.get("caps")
        and len(content) >= 12
        and sum(1 for c in content if c.isupper())
        / max(1, sum(1 for c in content if c.isalpha()))
        > 0.7
    ):
        violation = "excessive caps"
    elif config.get("spam"):
        key = (message.guild.id, message.author.id)
        now = time.time()
        recent = [t for t in bot.automod_recent.get(key, []) if now - t < 7.0]
        recent.append(now)
        bot.automod_recent[key] = recent
        if len(bot.automod_recent) > 1000:
            bot.automod_recent.clear()
        if len(recent) >= int(config.get("spam_limit") or 6):
            violation = "spam"

    if violation is None:
        return False

    try:
        await message.delete()
    except discord.DiscordException:
        return False

    strike_key = (message.guild.id, message.author.id)
    strikes = [
        t for t in bot.automod_strikes.get(strike_key, []) if time.time() - t < 600.0
    ]
    strikes.append(time.time())
    bot.automod_strikes[strike_key] = strikes

    try:
        await message.channel.send(
            f"🛡️ {message.author.mention} — that message was removed ({violation}).",
            delete_after=6.0,
            allowed_mentions=discord.AllowedMentions(users=True),
        )
    except discord.DiscordException:
        pass

    await send_modlog(
        message.guild,
        "🛡️ Automod",
        f"Removed a message from {message.author.mention} in {message.channel.mention}.",
        discord.Color.dark_orange(),
        [("Rule", violation), ("Strikes (10 min)", str(len(strikes)))],
    )

    threshold = config.get("action_threshold", 3)
    if len(strikes) >= threshold:
        bot.automod_strikes[strike_key] = []
        action = config.get("action", "warn")
        duration = config.get("action_duration", 10)
        reason = f"Automod: repeated {violation}"
        dm_reason = config.get("dm_reason", False)

        # DM reason if enabled
        if dm_reason:
            try:
                await message.author.send(
                    f"You have received a {action} in **{message.guild.name}**. Reason: {reason}"
                )
            except discord.DiscordException:
                pass

        # Helper to warn
        async def apply_warn():
            settings = await bot.settings.fetch_settings(message.guild.id)
            warns = dict(settings.get("warns") or {})
            entries = list(warns.get(str(message.author.id)) or [])
            entries.append(
                {
                    "reason": reason,
                    "by": str(bot.user),
                    "at": int(time.time()),
                }
            )
            warns[str(message.author.id)] = entries[-25:]
            await bot.settings.push_fields(message.guild.id, {"warns": warns})
            await record_case(message.guild, "warn", bot.user, message.author, reason)
            try:
                await message.channel.send(
                    f"⚠️ {message.author.mention} has been warned for repeated {violation}.",
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except discord.DiscordException:
                pass

        if action == "warn":
            await apply_warn()
            # Escalate if needed
            escalate_threshold = config.get("escalate_threshold")
            if escalate_threshold:
                settings = await bot.settings.fetch_settings(message.guild.id)
                warns = settings.get("warns", {})
                entries = warns.get(str(message.author.id), [])
                if len(entries) >= escalate_threshold:
                    escalate_action = config.get("escalate_action", "timeout")
                    escalate_duration = config.get("escalate_duration", 10)
                    action = escalate_action
                    duration = escalate_duration
                    reason = f"Automod: reached {escalate_threshold} warns (repeated {violation})"
                    # Fall through to execute the escalated action

        if action == "timeout":
            try:
                await message.author.timeout(
                    discord.utils.utcnow() + timedelta(minutes=duration), reason=reason
                )
                try:
                    await message.channel.send(
                        f"🤐 {message.author.mention} has been timed out for {duration}m for repeated {violation}.",
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                except discord.DiscordException:
                    pass
            except discord.Forbidden:
                pass
        elif action == "quarantine":
            try:
                role: Optional[discord.Role] = await _ensure_quarantine_role(
                    message.guild
                )
                if role and role not in message.author.roles:
                    removable: List[discord.Role] = [
                        r
                        for r in message.author.roles
                        if not r.is_default()
                        and not r.managed
                        and r < message.guild.me.top_role
                    ]
                    snapshot: List[str] = [str(r.id) for r in removable]
                    if removable:
                        await message.author.remove_roles(*removable, reason=reason)
                    await message.author.add_roles(role, reason=reason)
                    quarantined = dict(
                        bot.settings.peek_settings(message.guild.id).get("quarantined")
                        or {}
                    )
                    quarantined[str(message.author.id)] = {
                        "roles": snapshot,
                        "at": int(time.time()),
                    }
                    await bot.settings.push_fields(
                        message.guild.id, {"quarantined": quarantined}
                    )

                    try:
                        await message.channel.send(
                            f"🛑 {message.author.mention} has been quarantined for repeated {violation}.",
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                    except discord.DiscordException:
                        pass
            except discord.Forbidden:
                pass
        elif action == "kick":
            try:
                await message.author.kick(reason=reason)
                try:
                    await message.channel.send(
                        f"👢 {message.author.mention} has been kicked for repeated {violation}.",
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                except discord.DiscordException:
                    pass
            except discord.Forbidden:
                pass
        elif action == "ban":
            try:
                await message.author.ban(reason=reason)
                try:
                    await message.channel.send(
                        f"🔨 {message.author.mention} has been banned for repeated {violation}.",
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                except discord.DiscordException:
                    pass
            except discord.Forbidden:
                pass
    return True


@bot.hybrid_group(
    name="automod", description="Automatic message filtering", fallback="status"
)
@commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def automod_group(ctx: commands.Context):
    if ctx.invoked_subcommand is not None:
        return
    config = (await bot.settings.fetch_settings(ctx.guild.id)).get("automod") or {}
    lines = [
        f"{'🟢' if config.get(rule) else '🔴'} **{rule}**" for rule in AUTOMOD_RULES
    ]
    exempt = " ".join(f"<@&{r}>" for r in (config.get("exempt_roles") or [])) or "none"
    action = config.get("action", "warn")
    threshold = config.get("action_threshold", 3)
    duration = config.get("action_duration", 10)
    action_text = f"automatic **{action}**"
    if action == "timeout":
        action_text += f" for {duration}m"

    await ctx.send(
        embed=discord.Embed(
            title="🛡️ Automod",
            description="\n".join(lines)
            + f"\n\nMention limit: **{config.get('mention_limit', 5)}** · "
            f"Spam limit: **{config.get('spam_limit', 6)}** messages / 7s"
            f"\nExempt roles: {exempt}"
            f"\n\n**{threshold}** removals in 10 minutes = {action_text}.",
            color=discord.Color.blurple(),
        ),
        ephemeral=True,
    )


@automod_group.command(
    name="action", description="Set what happens after repeated automod removals"
)
@app_commands.describe(
    action="What to do (warn, timeout, quarantine, kick, ban)",
    threshold="How many removals trigger the action (default 3)",
    duration="How long timeouts should last in minutes (default 10)",
)
async def automod_action(
    ctx: commands.Context,
    action: Literal["warn", "timeout", "kick", "ban", "quarantine"],
    threshold: Optional[app_commands.Range[int, 1, 10]] = None,
    duration: Optional[app_commands.Range[int, 1, 1440]] = None,
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )

    fields = {"automod.action": action}
    if threshold is not None:
        fields["automod.action_threshold"] = threshold
    if duration is not None:
        fields["automod.action_duration"] = duration

    saved = await bot.settings.push_fields(ctx.guild.id, fields)

    msg = (
        f"✅ Automod will now **{action}** members after **{threshold or 3}** removals"
    )
    if action == "timeout":
        msg += f" for **{duration or 10}m**."
    else:
        msg += "."

    await ctx.send(
        msg if saved else msg + " (database write failed)",
        ephemeral=True,
    )


@automod_group.command(
    name="escalate", description="Set what happens after reaching a warn threshold"
)
@app_commands.describe(
    threshold="How many warns trigger the action (e.g. 5)",
    action="What to do (timeout, quarantine, kick, ban)",
    duration="How long timeouts/quarantines should last in minutes",
)
async def automod_escalate(
    ctx: commands.Context,
    threshold: app_commands.Range[int, 1, 100],
    action: Literal["timeout", "quarantine", "kick", "ban"],
    duration: Optional[app_commands.Range[int, 1, 1440]] = None,
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )

    fields = {
        "automod.escalate_threshold": threshold,
        "automod.escalate_action": action,
    }
    if duration is not None:
        fields["automod.escalate_duration"] = duration

    saved = await bot.settings.push_fields(ctx.guild.id, fields)

    msg = f"✅ Automod will now **{action}** members after reaching **{threshold}** warnings"
    if action in ("timeout", "quarantine") and duration:
        msg += f" for **{duration}m**."
    else:
        msg += "."

    await ctx.send(
        msg if saved else msg + " (database write failed)",
        ephemeral=True,
    )


@automod_group.command(
    name="dm",
    description="Whether automod should DM the user the reason for the action",
)
@app_commands.describe(state="on or off")
async def automod_dm(ctx: commands.Context, state: Literal["on", "off"]):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )

    saved = await bot.settings.push_fields(
        ctx.guild.id, {"automod.dm_reason": state == "on"}
    )
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Automod DM reason is now **{state}**.",
        ephemeral=True,
    )


@automod_group.command(name="set", description="Turn an automod rule on or off")
@app_commands.describe(rule="Which filter", state="on or off")
async def automod_set(
    ctx: commands.Context,
    rule: Literal["invites", "links", "spam", "caps", "mentions"],
    state: Literal["on", "off"],
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )
    saved = await bot.settings.push_fields(
        ctx.guild.id, {f"automod.{rule}": state == "on"}
    )
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Automod **{rule}** is now **{state}**."
        + ("" if saved else " (database write failed)"),
        ephemeral=True,
    )


@automod_group.command(
    name="limits", description="Tune the spam and mention thresholds"
)
@app_commands.describe(
    spam_limit="Messages within 7 seconds before it counts as spam (default 6)",
    mention_limit="Mentions in one message before it is removed (default 5)",
)
async def automod_limits(
    ctx: commands.Context,
    spam_limit: Optional[app_commands.Range[int, 3, 20]] = None,
    mention_limit: Optional[app_commands.Range[int, 3, 30]] = None,
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )
    fields: Dict[str, Any] = {}
    if spam_limit is not None:
        fields["automod.spam_limit"] = int(spam_limit)
    if mention_limit is not None:
        fields["automod.mention_limit"] = int(mention_limit)
    if not fields:
        return await ctx.send(
            "❌ Give me at least one value to change.", ephemeral=True
        )
    saved = await bot.settings.push_fields(ctx.guild.id, fields)
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Updated: "
        + ", ".join(f"{k.split('.')[-1]} = {v}" for k, v in fields.items()),
        ephemeral=True,
    )


@automod_group.command(name="exempt", description="Roles automod should never touch")
@app_commands.describe(action="add or remove", role="The role")
async def automod_exempt(
    ctx: commands.Context, action: Literal["add", "remove"], role: discord.Role
):
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "❌ You need the **Manage Server** permission.", ephemeral=True
        )
    config = (await bot.settings.fetch_settings(ctx.guild.id)).get("automod") or {}
    exempt = [str(r) for r in (config.get("exempt_roles") or [])]
    if action == "add" and str(role.id) not in exempt:
        exempt.append(str(role.id))
    elif action == "remove" and str(role.id) in exempt:
        exempt.remove(str(role.id))
    saved = await bot.settings.push_fields(
        ctx.guild.id, {"automod.exempt_roles": exempt}
    )
    await ctx.send(
        f"{'✅' if saved else '⚠️'} {role.mention} {'is now exempt' if action == 'add' else 'is no longer exempt'} from automod.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
