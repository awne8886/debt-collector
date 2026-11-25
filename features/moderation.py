"""Moderation commands: bans, kicks, timeouts, warnings, cases, tempban and massban."""

import asyncio
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from pymongo.errors import PyMongoError

from utils.pagination import build_pages, send_pages
from core.permissions import (
    custom_has_permissions,
    is_superuser,
    member_has_perms,
    mod_block_reason,
)
from core.registry import STARTUP_LOOPS
from core.bot import bot
from core.helpers import parse_duration
from core.modlog import post_modlog, record_case, send_modlog

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# MODERATION & UTILITY
# --------------------------------------------------------------------------- #


@bot.hybrid_command(
    name="ban",
    description="Permanently ban a member from the server",
)
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
async def ban_cmd(
    ctx: commands.Context,
    user: discord.Member,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
):
    if not member_has_perms(ctx.author, ban_members=True):
        return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    if dm:
        try:
            await user.send(
                f"You have been banned from **{ctx.guild.name}**. Reason: {reason}"
            )
        except discord.DiscordException:
            pass
    await user.ban(reason=reason)
    await ctx.send(f"🔨 **{user}** was banned. Reason: {reason}")
    embed = discord.Embed(title="Member Banned", color=discord.Color.red())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(name="unban", description="Unban a user by their ID")
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
async def unban_cmd(ctx: commands.Context, user_id: str):
    if not member_has_perms(ctx.author, ban_members=True):
        return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    try:
        user = await bot.fetch_user(int(user_id))
        await ctx.guild.unban(user)
        await ctx.send(f"✅ **{user}** was unbanned.")
        embed = discord.Embed(title="Member Unbanned", color=discord.Color.green())
        embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
        embed.add_field(
            name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
        )
        await post_modlog(ctx.guild, embed)
    except Exception as e:
        await ctx.send(f"❌ Failed to unban: {e}", ephemeral=True)


@bot.hybrid_command(
    name="kick",
    description="Remove a member; they can rejoin with a new invite",
)
@app_commands.default_permissions(kick_members=True)
@commands.guild_only()
async def kick_cmd(
    ctx: commands.Context,
    user: discord.Member,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
):
    if not member_has_perms(ctx.author, kick_members=True):
        return await ctx.send("❌ You need Kick Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    if dm:
        try:
            await user.send(
                f"You have been kicked from **{ctx.guild.name}**. Reason: {reason}"
            )
        except discord.DiscordException:
            pass
    await user.kick(reason=reason)
    await ctx.send(f"👢 **{user}** was kicked. Reason: {reason}")
    embed = discord.Embed(title="Member Kicked", color=discord.Color.orange())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(
    name="softban",
    description="Ban and immediately unban a member to delete their recent messages",
)
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
async def softban_cmd(
    ctx: commands.Context,
    user: discord.Member,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
):
    if not member_has_perms(ctx.author, ban_members=True):
        return await ctx.send("❌ You need Ban Members permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    if dm:
        try:
            await user.send(
                f"You have been softbanned from **{ctx.guild.name}**. Reason: {reason}\nThis removes your recent messages, but you can rejoin with a new invite."
            )
        except discord.DiscordException:
            pass

    try:
        await user.ban(reason=f"Softban: {reason}", delete_message_days=7)
        await ctx.guild.unban(user, reason="Softban release")
        await ctx.send(f"🔨 **{user}** was softbanned. Reason: {reason}")
        embed = discord.Embed(title="Member Softbanned", color=discord.Color.orange())
        embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
        embed.add_field(
            name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        await post_modlog(ctx.guild, embed)
    except discord.Forbidden:
        await ctx.send("❌ I do not have permission to ban that user.", ephemeral=True)
    except discord.HTTPException as e:
        await ctx.send(f"❌ Failed to softban: {e}", ephemeral=True)


@bot.hybrid_command(name="timeout", description="Time a member out (mute)")
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
async def timeout_cmd(
    ctx: commands.Context,
    user: discord.Member,
    minutes: int,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
):
    if not member_has_perms(ctx.author, moderate_members=True):
        return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    if dm:
        try:
            await user.send(
                f"You have been timed out in **{ctx.guild.name}** for {minutes}m. Reason: {reason}"
            )
        except discord.DiscordException:
            pass
    await user.timeout(
        discord.utils.utcnow() + timedelta(minutes=minutes), reason=reason
    )
    await ctx.send(f"🤐 **{user}** is timed out for {minutes}m. Reason: {reason}")
    embed = discord.Embed(title="Member Timed Out", color=discord.Color.orange())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    embed.add_field(name="Duration", value=f"{minutes} minutes", inline=False)
    embed.add_field(name="Reason", value=reason, inline=False)
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(name="untimeout", description="Remove a member's timeout")
@app_commands.default_permissions(moderate_members=True)
@commands.guild_only()
async def untimeout_cmd(ctx: commands.Context, user: discord.Member):
    if not member_has_perms(ctx.author, moderate_members=True):
        return await ctx.send("❌ You need Timeout permission.", ephemeral=True)
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    await user.timeout(None)
    await ctx.send(f"🔊 **{user}**'s timeout was removed.")
    embed = discord.Embed(title="Member Timeout Removed", color=discord.Color.green())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(
    name="warn",
    description="Log a warning against a member and DM them the reason",
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
async def warn_cmd(
    ctx: commands.Context,
    user: discord.Member,
    *,
    reason: Optional[str] = "No reason given",
    dm: bool = False,
):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    warns = settings.setdefault("warns", {})
    uw = warns.setdefault(str(user.id), [])
    if dm:
        try:
            await user.send(
                f"You have been warned in **{ctx.guild.name}**. Reason: {reason}"
            )
        except discord.DiscordException:
            pass
    uw.append({"reason": reason, "by": ctx.author.id, "at": int(time.time())})
    await bot.settings.push_settings(ctx.guild.id, {"warns": warns})
    await ctx.send(f"⚠️ **{user}** was warned. Reason: {reason}")
    embed = discord.Embed(title="Member Warned", color=discord.Color.gold())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(name="warnings", description="Show a member's warnings")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(user="Whose warnings to show")
async def warnings_cmd(ctx: commands.Context, user: discord.Member):
    settings = await bot.settings.fetch_settings(ctx.guild.id)
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
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
    settings = await bot.settings.fetch_settings(ctx.guild.id)
    warns = settings.get("warns", {})
    if str(user.id) in warns:
        del warns[str(user.id)]
        await bot.settings.push_settings(ctx.guild.id, {"warns": warns})

    strike_key = (ctx.guild.id, user.id)
    if strike_key in bot.automod_strikes:
        bot.automod_strikes[strike_key] = []

    await ctx.send(f"✅ Cleared warnings for **{user}**.")
    embed = discord.Embed(title="Warnings Cleared", color=discord.Color.green())
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(
        name="Moderator", value=f"{ctx.author} ({ctx.author.id})", inline=False
    )
    await post_modlog(ctx.guild, embed)


@bot.hybrid_command(
    name="lock", description="Lock a channel (block @everyone from sending)"
)
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def lock_cmd(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, manage_channels=True):
        return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=False)
    await ctx.send(f"🔒 {c.mention} is now locked.")


@bot.hybrid_command(name="unlock", description="Unlock a channel")
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def unlock_cmd(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, manage_channels=True):
        return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.set_permissions(ctx.guild.default_role, send_messages=True)
    await ctx.send(f"🔓 {c.mention} is now unlocked.")


@bot.hybrid_command(name="slowmode", description="Set channel slowmode (0 to disable)")
@app_commands.default_permissions(manage_channels=True)
@commands.guild_only()
async def slowmode_cmd(
    ctx: commands.Context, seconds: int, channel: Optional[discord.TextChannel] = None
):
    if not member_has_perms(ctx.author, manage_channels=True):
        return await ctx.send("❌ You need Manage Channels permission.", ephemeral=True)
    c = channel or ctx.channel
    await c.edit(slowmode_delay=max(0, seconds))
    await ctx.send(f"⏱️ Slowmode in {c.mention} set to {seconds}s.")


@bot.hybrid_command(
    name="nickname", description="Change a member's nickname (leave empty to reset)"
)
@app_commands.default_permissions(manage_nicknames=True)
@commands.guild_only()
async def nickname_cmd(
    ctx: commands.Context, user: discord.Member, *, name: Optional[str] = None
):
    if not member_has_perms(ctx.author, manage_nicknames=True):
        return await ctx.send(
            "❌ You need Manage Nicknames permission.", ephemeral=True
        )
    err = mod_block_reason(ctx.author, user, ctx.guild.me)
    if err:
        return await ctx.send(err, ephemeral=True)
    await user.edit(nick=name)
    await ctx.send(f"✅ Changed **{user}**'s nickname.")


@bot.hybrid_command(name="case", description="Show moderation history for a member")
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(
    user="Whose history to show", limit="How many entries (default 15)"
)
async def case_cmd(
    ctx: commands.Context,
    user: discord.User,
    limit: app_commands.Range[int, 1, 50] = 15,
):
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send(
            "❌ You need the **Manage Messages** permission.", ephemeral=True
        )
    try:
        docs = await asyncio.to_thread(
            lambda: list(
                bot.settings.cases.find(
                    {"guildid": str(ctx.guild.id), "target_id": str(user.id)}
                )
                .sort("case_id", -1)
                .limit(int(limit))
            )
        )
    except PyMongoError as exc:
        bot.log_error("case", exc)
        return await ctx.send(
            "⚠️ The case book is unreachable right now.", ephemeral=True
        )

    if not docs:
        return await ctx.send(
            f"✅ No moderation history for {user.mention}.", ephemeral=True
        )

    lines = [
        f"**#{d['case_id']} · {d['action']}** <t:{int(d['at'].replace(tzinfo=timezone.utc).timestamp())}:R>\n"
        f"└ by `{d.get('actor')}` — {discord.utils.escape_mentions(str(d.get('reason') or ''))[:150]}"
        for d in docs
    ]
    pages = build_pages(f"Cases · {user}", lines, discord.Color.orange(), per_page=6)
    await send_pages(ctx, pages, ephemeral=True)


# --------------------------------------------------------------------------- #
# Temporary bans
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="tempban", description="Ban a member for a set amount of time")
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
@app_commands.describe(user="Who to ban", duration="e.g. 30m, 6h, 3d, 1w", reason="Why")
async def tempban_cmd(
    ctx: commands.Context,
    user: discord.Member,
    duration: str,
    *,
    reason: Optional[str] = "No reason given",
):
    if not member_has_perms(ctx.author, ban_members=True):
        return await ctx.send(
            "❌ You need the **Ban Members** permission.", ephemeral=True
        )
    block = mod_block_reason(ctx.author, user, ctx.guild.me)
    if block:
        return await ctx.send(f"❌ {block}", ephemeral=True)

    seconds = parse_duration(duration)
    if seconds is None:
        return await ctx.send(
            "❌ I couldn't read that duration. Try `30m`, `6h`, `3d` or `1w`.",
            ephemeral=True,
        )

    until = int(time.time()) + seconds
    await ctx.guild.ban(
        user,
        reason=f"{reason} (tempban by {ctx.author}, {duration})",
        delete_message_seconds=0,
    )

    settings = await bot.settings.fetch_settings(ctx.guild.id)
    tempbans = dict(settings.get("tempbans") or {})
    tempbans[str(user.id)] = {"until": until, "reason": reason, "by": str(ctx.author)}
    await bot.settings.push_fields(ctx.guild.id, {"tempbans": tempbans})

    await ctx.send(
        f"🔨 Banned **{user}** until <t:{until}:f> (<t:{until}:R>). Reason: {discord.utils.escape_mentions(reason or '')}",
        allowed_mentions=discord.AllowedMentions.none(),
    )


@tasks.loop(seconds=60.0)
async def tempban_loop() -> None:
    now = time.time()
    for guild in list(bot.guilds):
        settings = bot.settings.peek_settings(guild.id)
        tempbans = dict(settings.get("tempbans") or {})
        if not tempbans:
            continue
        expired = [uid for uid, data in tempbans.items() if data.get("until", 0) <= now]
        if not expired:
            continue
        for user_id in expired:
            tempbans.pop(user_id, None)
            try:
                await guild.unban(
                    discord.Object(id=int(user_id)), reason="Temporary ban expired"
                )
                await send_modlog(
                    guild,
                    "♻️ Temporary ban expired",
                    f"<@{user_id}> was unbanned automatically.",
                    discord.Color.green(),
                )
            except discord.NotFound:
                pass
            except discord.DiscordException as exc:
                bot.log_error("tempban:unban", exc)
        await bot.settings.push_fields(guild.id, {"tempbans": tempbans})


@tempban_loop.before_loop
async def before_tempban_loop() -> None:
    await bot.wait_until_ready()


# --------------------------------------------------------------------------- #
# Filtered mass ban
# --------------------------------------------------------------------------- #

MASSBAN_HARD_CAP: int = 100


class MassbanConfirm(discord.ui.View):
    """Single-use confirmation gate bound to the invoking moderator."""

    def __init__(self, author_id: int, timeout: float = 60.0) -> None:
        super().__init__(timeout=timeout)
        self.author_id: int = author_id
        self.value: Optional[bool] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            "\u274c Only the moderator who ran this command can confirm it.",
            ephemeral=True,
        )
        return False

    def _disable(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    async def on_timeout(self) -> None:
        self.value = False
        self._disable()

    @discord.ui.button(
        label="Confirm ban", style=discord.ButtonStyle.danger, emoji="\U0001f528"
    )
    async def confirm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.value = True
        self._disable()
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.value = False
        self._disable()
        await interaction.response.edit_message(view=self)
        self.stop()


@bot.hybrid_command(
    name="massban", description="Ban every member matching a filter (preview first)"
)
@app_commands.default_permissions(ban_members=True)
@commands.guild_only()
@custom_has_permissions(ban_members=True)
@commands.bot_has_permissions(ban_members=True)
@app_commands.describe(
    account_age_hours="Only accounts younger than this many hours",
    joined_within_minutes="Only members who joined in the last N minutes",
    name_pattern="Regex matched against username and nickname",
    no_avatar="Only members using the default avatar",
    reason="Audit-log reason",
)
async def massban_cmd(
    ctx: commands.Context,
    account_age_hours: Optional[app_commands.Range[int, 0, 8760]] = None,
    joined_within_minutes: Optional[app_commands.Range[int, 1, 10080]] = None,
    name_pattern: Optional[str] = None,
    no_avatar: bool = False,
    *,
    reason: Optional[str] = "Mass ban - raid cleanup",
) -> None:
    if not member_has_perms(ctx.author, ban_members=True):
        return await ctx.send(
            "\u274c You need the **Ban Members** permission.", ephemeral=True
        )

    if (
        account_age_hours is None
        and joined_within_minutes is None
        and not name_pattern
        and not no_avatar
    ):
        return await ctx.send(
            "\u274c Give me at least one filter. A filter-less mass ban is never run.",
            ephemeral=True,
        )

    pattern: Optional[re.Pattern] = None
    if name_pattern:
        try:
            pattern = re.compile(name_pattern, re.IGNORECASE)
        except re.error as exc:
            return await ctx.send(f"\u274c Invalid regex: `{exc}`.", ephemeral=True)

    await ctx.defer(ephemeral=True)

    now: datetime = discord.utils.utcnow()
    actor_top: discord.Role = ctx.author.top_role
    me: discord.Member = ctx.guild.me
    targets: List[discord.Member] = []

    for member in ctx.guild.members:
        if member.bot or member.id in (ctx.author.id, me.id, ctx.guild.owner_id):
            continue
        if is_superuser(member):
            continue
        if member.top_role >= actor_top and not is_superuser(ctx.author):
            continue
        if member.top_role >= me.top_role:
            continue
        if account_age_hours is not None:
            if (now - member.created_at).total_seconds() >= int(
                account_age_hours
            ) * 3600:
                continue
        if joined_within_minutes is not None:
            if member.joined_at is None:
                continue
            if (now - member.joined_at).total_seconds() > int(
                joined_within_minutes
            ) * 60:
                continue
        if pattern is not None:
            haystack: str = f"{member.name} {member.display_name}"
            if not pattern.search(haystack):
                continue
        if no_avatar and member.avatar is not None:
            continue
        targets.append(member)

    if not targets:
        return await ctx.send(
            "\u2705 No members matched those filters.", ephemeral=True
        )
    if len(targets) > MASSBAN_HARD_CAP:
        return await ctx.send(
            f"\u274c **{len(targets)}** members matched, above the {MASSBAN_HARD_CAP} hard cap. "
            "Tighten the filters - this guard exists to stop a runaway ban.",
            ephemeral=True,
        )

    preview: str = "\n".join(
        f"- `{member}` ({member.id}) - created <t:{int(member.created_at.timestamp())}:R>"
        for member in targets[:15]
    )
    if len(targets) > 15:
        preview += f"\n... and **{len(targets) - 15}** more."

    embed: discord.Embed = discord.Embed(
        title=f"\U0001f528 Mass ban preview - {len(targets)} target(s)",
        description=preview[:4000],
        color=discord.Color.red(),
    )
    embed.set_footer(
        text="Nothing has been banned yet. This preview expires in 60 seconds."
    )

    view: MassbanConfirm = MassbanConfirm(ctx.author.id)
    await ctx.send(embed=embed, view=view, ephemeral=True)
    await view.wait()

    if not view.value:
        return await ctx.send(
            "\u2705 Mass ban cancelled - nobody was banned.", ephemeral=True
        )

    clean_reason: str = discord.utils.escape_mentions(str(reason or "Mass ban"))[:400]
    banned: int = 0
    failed: int = 0
    for member in targets:
        try:
            await ctx.guild.ban(
                member,
                reason=f"{clean_reason} (massban by {ctx.author})",
                delete_message_seconds=3600,
            )
            banned += 1
            await record_case(ctx.guild, "massban", ctx.author, member, clean_reason)
        except discord.Forbidden:
            failed += 1
        except discord.HTTPException as exc:
            failed += 1
            bot.log_error("massban", exc, guild=ctx.guild, user=member)
        await asyncio.sleep(0.6)

    log.warning(
        "Mass ban in guild %s by %s: %d banned, %d failed.",
        ctx.guild.id,
        ctx.author,
        banned,
        failed,
    )
    await send_modlog(
        ctx.guild,
        "\U0001f528 Mass ban executed",
        f"**{banned}** banned, **{failed}** failed.\n**Reason:** {clean_reason}",
        discord.Color.red(),
        [("Moderator", f"{ctx.author.mention} (`{ctx.author}`)")],
    )
    await ctx.send(
        f"\U0001f528 Banned **{banned}** member(s)."
        + (f" **{failed}** could not be banned." if failed else ""),
        ephemeral=True,
    )


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
STARTUP_LOOPS.append(tempban_loop)
