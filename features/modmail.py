"""Modmail tickets, including the persistent panel and close views."""

import asyncio
import copy
import io
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from pymongo.errors import PyMongoError

from core.config import DEFAULT_SETTINGS
from core.permissions import custom_has_permissions, member_has_perms
from core.registry import PERSISTENT_VIEWS
from core.bot import bot
from core.modlog import send_modlog
from features.messagelog import _describe_attachments

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Modmail - button-opened private ticket channels
# --------------------------------------------------------------------------- #

MODMAIL_OPEN_ID: str = "modmail:open"
MODMAIL_CLOSE_ID: str = "modmail:close"
MODMAIL_COOLDOWN_SECONDS: float = 600.0
MODMAIL_DEFAULT_PANEL: str = (
    "Need help from the staff team? Press the button below and I'll open a private "
    "channel for you. Only you and the staff can see it."
)
MODMAIL_DEFAULT_WELCOME: str = (
    "Thanks for reaching out. A staff member will be with you shortly.\n\n"
    "Please describe your issue here, with any screenshots or links that help. "
    "Press **Close ticket** below when you're done."
)
_MODMAIL_LAST_OPEN: Dict[Tuple[int, int], float] = {}


def _modmail_config(guild_id: int) -> Dict[str, Any]:
    stored: Dict[str, Any] = bot.settings.peek_settings(guild_id).get("modmail") or {}
    config: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS["modmail"])
    for key, value in stored.items():
        if key in config:
            config[key] = value
    return config


def _modmail_staff_roles(
    guild: discord.Guild, config: Dict[str, Any]
) -> List[discord.Role]:
    roles: List[discord.Role] = []
    for raw_id in config.get("staff_roles") or []:
        role: Optional[discord.Role] = guild.get_role(int(raw_id))
        if role is not None:
            roles.append(role)
    return roles


def _modmail_ticket_for(config: Dict[str, Any], user_id: int) -> Optional[str]:
    """Channel id of this member's open ticket, if any."""
    for channel_id, record in (config.get("tickets") or {}).items():
        if str(record.get("user_id")) == str(user_id) and not record.get("closed"):
            return str(channel_id)
    return None


async def _modmail_next_number(guild_id: int) -> int:
    try:
        counter = await asyncio.to_thread(
            bot.settings.meta.find_one_and_update,
            {"key": f"modmail:{guild_id}"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=True,
        )
        return int((counter or {}).get("value", 1))
    except PyMongoError as exc:
        bot.log_error("ticket:counter", exc)
        return int(time.time()) % 100000


class TicketCloseView(discord.ui.View):
    """Persistent close button pinned at the top of every ticket channel."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Close ticket",
        style=discord.ButtonStyle.danger,
        emoji="\U0001f512",
        custom_id=MODMAIL_CLOSE_ID,
    )
    async def close_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None:
            return
        config: Dict[str, Any] = _modmail_config(interaction.guild.id)
        tickets: Dict[str, Any] = dict(config.get("tickets") or {})
        record: Optional[Dict[str, Any]] = tickets.get(str(interaction.channel_id))
        if record is None:
            return await interaction.response.send_message(
                "\u274c This doesn't look like an open ticket.", ephemeral=True
            )

        opener_id: int = int(record.get("user_id") or 0)
        is_staff: bool = isinstance(
            interaction.user, discord.Member
        ) and member_has_perms(interaction.user, manage_messages=True)
        if interaction.user.id != opener_id and not is_staff:
            return await interaction.response.send_message(
                "\u274c Only the person who opened this ticket or a staff member can close it.",
                ephemeral=True,
            )

        await interaction.response.defer()
        await _modmail_close(
            interaction.guild,
            interaction.channel,
            closed_by=interaction.user,
            reason="Closed via button",
        )


class ModmailPanelView(discord.ui.View):
    """Persistent panel button members press to open a ticket."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Open a ticket",
        style=discord.ButtonStyle.primary,
        emoji="\U0001f4e8",
        custom_id=MODMAIL_OPEN_ID,
    )
    async def open_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None or not isinstance(
            interaction.user, discord.Member
        ):
            return
        await interaction.response.defer(ephemeral=True)
        channel, problem = await _modmail_open(interaction.guild, interaction.user)
        if channel is None:
            return await interaction.followup.send(f"\u274c {problem}", ephemeral=True)
        await interaction.followup.send(
            f"\u2705 Your ticket is open: {channel.mention}", ephemeral=True
        )


async def _modmail_open(
    guild: discord.Guild, member: discord.Member
) -> Tuple[Optional[discord.TextChannel], str]:
    """Create a private ticket channel. Returns (channel, problem message)."""
    config: Dict[str, Any] = _modmail_config(guild.id)
    if not config.get("enabled"):
        return None, "Tickets aren't set up in this server yet."
    if str(member.id) in [str(u) for u in (config.get("blocked") or [])]:
        return None, "You can't open tickets in this server."

    existing_id: Optional[str] = _modmail_ticket_for(config, member.id)
    if existing_id:
        existing = guild.get_channel(int(existing_id))
        if isinstance(existing, discord.TextChannel):
            return None, f"You already have an open ticket: {existing.mention}"

    key: Tuple[int, int] = (guild.id, member.id)
    now: float = time.time()
    last: float = _MODMAIL_LAST_OPEN.get(key, 0.0)
    if now - last < MODMAIL_COOLDOWN_SECONDS and not member_has_perms(
        member, manage_messages=True
    ):
        wait: int = int(MODMAIL_COOLDOWN_SECONDS - (now - last))
        return (
            None,
            f"Please wait {wait // 60}m {wait % 60}s before opening another ticket.",
        )

    me: discord.Member = guild.me
    if not me.guild_permissions.manage_channels:
        return None, "I need the **Manage Channels** permission to open tickets."

    staff_roles: List[discord.Role] = _modmail_staff_roles(guild, config)
    if not staff_roles:
        return None, "No staff role is configured for tickets."

    category: Optional[discord.CategoryChannel] = None
    if config.get("category_id"):
        candidate = guild.get_channel(int(config["category_id"]))
        if isinstance(candidate, discord.CategoryChannel):
            category = candidate

    overwrites: Dict[Any, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
            embed_links=True,
            attach_files=True,
        ),
        member: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
            add_reactions=True,
        ),
    }
    for role in staff_roles:
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
            manage_messages=True,
        )

    number: int = await _modmail_next_number(guild.id)
    name: str = (
        f"ticket-{number:04d}-{re.sub(r'[^a-z0-9]+', '', member.name.casefold())[:20]}"
    )

    try:
        channel: discord.TextChannel = await guild.create_text_channel(
            name=name[:100],
            category=category,
            overwrites=overwrites,
            topic=f"Ticket #{number} for {member} ({member.id})",
            reason=f"Ticket opened by {member}",
        )
    except discord.Forbidden:
        return None, "I'm not allowed to create a channel there."
    except discord.HTTPException as exc:
        bot.log_error("ticket:create", exc, guild=guild, user=member)
        return None, "Discord refused to create the ticket channel."

    _MODMAIL_LAST_OPEN[key] = now

    welcome_text: str = str(config.get("welcome_message") or MODMAIL_DEFAULT_WELCOME)
    embed: discord.Embed = discord.Embed(
        title=str(config.get("welcome_title") or f"Ticket #{number}"),
        description=welcome_text[:4000],
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Opened by", value=f"{member.mention} (`{member}`)", inline=True
    )
    embed.add_field(
        name="Account created",
        value=f"<t:{int(member.created_at.timestamp())}:R>",
        inline=True,
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text=f"Ticket #{number}")

    mention_line: str = member.mention
    if config.get("ping_staff"):
        mention_line += " " + " ".join(role.mention for role in staff_roles[:3])

    try:
        opener: discord.Message = await channel.send(
            mention_line,
            embed=embed,
            view=TicketCloseView(),
            allowed_mentions=discord.AllowedMentions(users=True, roles=True),
        )
        await opener.pin(reason="Ticket header")
    except discord.DiscordException as exc:
        bot.log_error("ticket:header", exc, guild=guild, user=member)

    tickets: Dict[str, Any] = dict(config.get("tickets") or {})
    tickets[str(channel.id)] = {
        "user_id": str(member.id),
        "number": number,
        "opened_at": int(now),
        "closed": False,
        "participants": [str(member.id)],
    }
    await bot.settings.push_fields(guild.id, {"modmail.tickets": tickets})

    log.info(
        "Ticket #%d opened in guild %s by %s (channel %s).",
        number,
        guild.id,
        member,
        channel.id,
    )
    await send_modlog(
        guild,
        f"\U0001f4e8 Ticket #{number} opened",
        f"{member.mention} opened {channel.mention}.",
        discord.Color.blurple(),
    )
    return channel, ""


async def _ticket_transcript_text(channel: discord.TextChannel) -> Optional[str]:
    """Plain-text transcript of a ticket, oldest first."""
    try:
        lines: List[str] = []
        async for message in channel.history(limit=500, oldest_first=True):
            stamp: str = message.created_at.strftime("%Y-%m-%d %H:%M:%S")
            body: str = message.content or ""
            if message.attachments:
                body += (
                    " [attachments: "
                    + _describe_attachments(list(message.attachments))
                    + "]"
                )
            if message.embeds and not body.strip():
                body = "[embed]"
            lines.append(f"[{stamp}] {message.author}: {body}")
        if not lines:
            return None
        return "\n".join(lines)
    except discord.DiscordException as exc:
        bot.log_error("ticket:transcript", exc, guild=channel.guild)
        return None


def _transcript_file(text: str, name: str) -> discord.File:
    """A fresh File per recipient - a BytesIO can only be sent once."""
    return discord.File(io.BytesIO(text.encode("utf-8")), filename=f"{name}.txt")


async def _ticket_delete_later(
    channel: discord.TextChannel, delay: int, number: Any
) -> None:
    """Delete a closed ticket channel after the configured grace period."""
    await asyncio.sleep(max(1, delay))
    guild: discord.Guild = channel.guild
    try:
        await channel.delete(reason=f"Ticket #{number} closed")
        log.info("Ticket #%s channel deleted in guild %s.", number, guild.id)
    except discord.NotFound:
        pass
    except discord.Forbidden:
        log.warning("Can't delete ticket channel %s - missing permissions.", channel.id)
        return
    except discord.HTTPException as exc:
        bot.log_error("ticket:delete", exc, guild=guild)
        return

    config: Dict[str, Any] = _modmail_config(guild.id)
    tickets: Dict[str, Any] = dict(config.get("tickets") or {})
    if tickets.pop(str(channel.id), None) is not None:
        await bot.settings.push_fields(guild.id, {"modmail.tickets": tickets})


async def _modmail_close(
    guild: discord.Guild,
    channel: Any,
    *,
    closed_by: discord.abc.User,
    reason: str,
) -> bool:
    """Close a ticket: strip member access, log a transcript, archive the channel."""
    if not isinstance(channel, discord.TextChannel):
        return False
    config: Dict[str, Any] = _modmail_config(guild.id)
    tickets: Dict[str, Any] = dict(config.get("tickets") or {})
    record: Optional[Dict[str, Any]] = tickets.get(str(channel.id))
    if record is None or record.get("closed"):
        return False

    number: Any = record.get("number", "?")
    opener: Optional[discord.Member] = guild.get_member(int(record.get("user_id") or 0))
    participants: List[str] = [
        str(p) for p in (record.get("participants") or [record.get("user_id")]) if p
    ]

    transcript_text: Optional[str] = None
    if config.get("transcripts") or config.get("dm_transcript"):
        transcript_text = await _ticket_transcript_text(channel)

    closing: discord.Embed = discord.Embed(
        title=f"\U0001f512 Ticket #{number} closed",
        description=discord.utils.escape_mentions(reason)[:2000],
        color=discord.Color.greyple(),
        timestamp=datetime.now(timezone.utc),
    )
    closing.add_field(
        name="Closed by", value=f"{closed_by.mention} (`{closed_by}`)", inline=True
    )

    delete_after: int = int(config.get("delete_after") or 0)
    if delete_after > 0:
        closing.set_footer(
            text=f"This channel will be deleted in {delete_after} seconds."
        )
    try:
        await channel.send(
            embed=closing, allowed_mentions=discord.AllowedMentions.none()
        )
    except discord.DiscordException:
        pass

    # Notify everyone who had access, with the transcript when enabled.
    if config.get("dm_transcript"):
        for raw_id in participants[:15]:
            recipient: Optional[discord.abc.User] = guild.get_member(int(raw_id))
            if recipient is None:
                try:
                    recipient = await bot.fetch_user(int(raw_id))
                except discord.DiscordException:
                    continue
            attachment: Optional[discord.File] = (
                _transcript_file(transcript_text, channel.name)
                if transcript_text
                else None
            )
            try:
                await recipient.send(
                    f"Your ticket **#{number}** in **{guild.name}** has been closed.\n"
                    f"**Reason:** {discord.utils.escape_mentions(reason)[:500]}",
                    file=attachment,
                )
            except (discord.Forbidden, discord.HTTPException):
                continue
    elif opener is not None:
        try:
            await opener.send(
                f"Your ticket **#{number}** in **{guild.name}** has been closed.\n"
                f"**Reason:** {discord.utils.escape_mentions(reason)[:500]}"
            )
        except (discord.Forbidden, discord.HTTPException):
            pass

    if opener is not None:
        try:
            await channel.set_permissions(
                opener, overwrite=None, reason="Ticket closed"
            )
        except discord.DiscordException as exc:
            bot.log_error("ticket:revoke", exc, guild=guild, user=opener)

    record["closed"] = True
    record["closed_at"] = int(time.time())
    record["closed_by"] = str(closed_by)
    tickets[str(channel.id)] = record
    await bot.settings.push_fields(guild.id, {"modmail.tickets": tickets})

    log_channel: Optional[discord.TextChannel] = None
    if config.get("log_channel_id"):
        candidate = guild.get_channel(int(config["log_channel_id"]))
        if isinstance(candidate, discord.TextChannel):
            log_channel = candidate
    if log_channel is not None:
        try:
            await log_channel.send(
                embed=closing,
                file=(
                    _transcript_file(transcript_text, channel.name)
                    if (transcript_text and config.get("transcripts"))
                    else None
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.DiscordException as exc:
            bot.log_error("ticket:log", exc, guild=guild)

    await send_modlog(
        guild,
        f"\U0001f512 Ticket #{number} closed",
        f"`{channel.name}` was closed by {closed_by.mention}.",
        discord.Color.greyple(),
    )
    log.info("Ticket #%s closed in guild %s by %s.", number, guild.id, closed_by)

    if delete_after > 0:
        bot.spawn(
            _ticket_delete_later(channel, delete_after, number),
            name=f"ticketdelete:{channel.id}",
        )
    elif not channel.name.startswith("closed-"):
        try:
            await channel.edit(
                name=f"closed-{channel.name}"[:100], reason="Ticket closed"
            )
        except discord.DiscordException:
            pass
    return True


@bot.hybrid_group(
    name="ticket",
    description="Private staff tickets members open from a button panel",
    fallback="status",
)
@commands.guild_only()
async def ticket_group(ctx: commands.Context) -> None:
    config: Dict[str, Any] = _modmail_config(ctx.guild.id)
    is_staff: bool = member_has_perms(ctx.author, manage_messages=True)

    if not is_staff:
        existing: Optional[str] = _modmail_ticket_for(config, ctx.author.id)
        if existing:
            return await ctx.send(
                f"\U0001f4e8 Your open ticket: <#{existing}>", ephemeral=True
            )
        state: str = (
            "Press the **Open a ticket** button on the ticket panel to reach staff."
            if config.get("enabled")
            else "Tickets aren't set up in this server yet."
        )
        return await ctx.send(state, ephemeral=True)

    tickets: Dict[str, Any] = config.get("tickets") or {}
    open_tickets: List[Tuple[str, Dict[str, Any]]] = [
        (cid, rec) for cid, rec in tickets.items() if not rec.get("closed")
    ]
    staff_roles: List[discord.Role] = _modmail_staff_roles(ctx.guild, config)

    embed: discord.Embed = discord.Embed(
        title="\U0001f4e8 Tickets",
        color=(
            discord.Color.blurple()
            if config.get("enabled")
            else discord.Color.greyple()
        ),
    )
    embed.add_field(
        name="State",
        value="\U0001f7e2 enabled" if config.get("enabled") else "\u26aa not set up",
        inline=True,
    )
    embed.add_field(name="Open tickets", value=str(len(open_tickets)), inline=True)
    embed.add_field(
        name="Staff roles",
        value=", ".join(role.mention for role in staff_roles) or "*none set*",
        inline=False,
    )
    if config.get("category_id"):
        embed.add_field(
            name="Category", value=f"<#{config['category_id']}>", inline=True
        )
    if open_tickets:
        listing: List[str] = [
            f"#{rec.get('number')} \u00b7 <#{cid}> \u00b7 <@{rec.get('user_id')}>"
            for cid, rec in sorted(
                open_tickets, key=lambda kv: int(kv[1].get("number") or 0), reverse=True
            )[:10]
        ]
        embed.add_field(
            name="Currently open", value="\n".join(listing)[:1024], inline=False
        )
    embed.set_footer(
        text=f"{ctx.clean_prefix}ticket setup \u00b7 {ctx.clean_prefix}ticket panel"
    )
    await ctx.send(embed=embed, ephemeral=True)


@ticket_group.command(
    name="setup", description="Configure the ticket staff role and category"
)
@app_commands.default_permissions(administrator=True)
@custom_has_permissions(administrator=True)
@app_commands.describe(
    staff_role="Role that can see and answer every ticket",
    category="Category new ticket channels are created in",
    log_channel="Where closing summaries and transcripts are posted",
    ping_staff="Ping the staff role when a ticket opens",
    transcripts="Attach a text transcript when a ticket closes",
)
async def ticket_setup(
    ctx: commands.Context,
    staff_role: discord.Role,
    category: Optional[discord.CategoryChannel] = None,
    log_channel: Optional[discord.TextChannel] = None,
    ping_staff: bool = True,
    transcripts: bool = True,
) -> None:
    if not member_has_perms(ctx.author, administrator=True):
        return await ctx.send(
            "\u274c You need Administrator permission.", ephemeral=True
        )
    if not ctx.guild.me.guild_permissions.manage_channels:
        return await ctx.send(
            "\u274c I need the **Manage Channels** permission before tickets will work.",
            ephemeral=True,
        )

    fields: Dict[str, Any] = {
        "modmail.enabled": True,
        "modmail.staff_roles": [str(staff_role.id)],
        "modmail.category_id": str(category.id) if category else None,
        "modmail.log_channel_id": str(log_channel.id) if log_channel else None,
        "modmail.ping_staff": bool(ping_staff),
        "modmail.transcripts": bool(transcripts),
    }
    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    log.info("Tickets configured in guild %s by %s.", ctx.guild.id, ctx.author)

    detail: str = f"Staff role {staff_role.mention}"
    if category:
        detail += f", tickets in **{category.name}**"
    if log_channel:
        detail += f", logs to {log_channel.mention}"
    await ctx.send(
        f"\u2705 Tickets are set up. {detail}.\n"
        f"Post the panel with `{ctx.clean_prefix}ticket panel`."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@ticket_group.command(name="panel", description="Post the ticket panel with its button")
@app_commands.default_permissions(manage_guild=True)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(channel="Where to post the panel (defaults to here)")
async def ticket_panel(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _modmail_config(ctx.guild.id)
    if not config.get("enabled"):
        return await ctx.send(
            f"\u274c Run `{ctx.clean_prefix}ticket setup` first.", ephemeral=True
        )

    target: discord.TextChannel = channel or ctx.channel
    embed: discord.Embed = discord.Embed(
        title=str(config.get("panel_title") or "Contact staff"),
        description=str(config.get("panel_message") or MODMAIL_DEFAULT_PANEL)[:4000],
        color=discord.Color.blurple(),
    )
    try:
        await target.send(embed=embed, view=ModmailPanelView())
    except discord.Forbidden:
        return await ctx.send(
            f"\u274c I can't post in {target.mention}.", ephemeral=True
        )
    except discord.HTTPException as exc:
        bot.log_error("ticket:panel", exc, guild=ctx.guild)
        return await ctx.send(
            "\u26a0\ufe0f Discord rejected the panel message.", ephemeral=True
        )

    await ctx.send(f"\u2705 Panel posted in {target.mention}.", ephemeral=True)


@ticket_group.command(
    name="message", description="Set the panel text or the ticket welcome embed"
)
@app_commands.default_permissions(manage_guild=True)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    target="Which text to change",
    title="Embed title",
    text="Embed body. Leave empty to restore the default.",
)
async def ticket_message(
    ctx: commands.Context,
    target: Literal["panel", "welcome"],
    title: Optional[str] = None,
    *,
    text: Optional[str] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    fields: Dict[str, Any] = {}
    if target == "panel":
        fields["modmail.panel_message"] = (text or MODMAIL_DEFAULT_PANEL)[:2000]
        if title is not None:
            fields["modmail.panel_title"] = title[:200]
    else:
        fields["modmail.welcome_message"] = (text or MODMAIL_DEFAULT_WELCOME)[:2000]
        if title is not None:
            fields["modmail.welcome_title"] = title[:200]

    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    preview: discord.Embed = discord.Embed(
        title=(
            fields.get("modmail.panel_title")
            or fields.get("modmail.welcome_title")
            or ("Contact staff" if target == "panel" else "Ticket")
        ),
        description=(
            fields.get("modmail.panel_message") or fields.get("modmail.welcome_message")
        ),
        color=discord.Color.blurple(),
    )
    await ctx.send(
        f"\u2705 Updated the **{target}** text."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        embed=preview,
        ephemeral=True,
    )
    if target == "panel":
        await ctx.send(
            f"Re-post the panel with `{ctx.clean_prefix}ticket panel` to apply it.",
            ephemeral=True,
        )


@ticket_group.command(
    name="config", description="Auto-delete delay, transcript DMs and staff pings"
)
@app_commands.default_permissions(manage_guild=True)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    delete_after="Seconds before a closed ticket channel is deleted. 0 keeps it archived.",
    dm_transcript="DM everyone who had access a copy of the transcript on close",
    transcripts="Attach a transcript to the ticket log channel",
    ping_staff="Ping the staff role when a ticket opens",
)
async def ticket_config(
    ctx: commands.Context,
    delete_after: Optional[app_commands.Range[int, 0, 604800]] = None,
    dm_transcript: Optional[bool] = None,
    transcripts: Optional[bool] = None,
    ping_staff: Optional[bool] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )

    fields: Dict[str, Any] = {}
    if delete_after is not None:
        fields["modmail.delete_after"] = int(delete_after)
    if dm_transcript is not None:
        fields["modmail.dm_transcript"] = bool(dm_transcript)
    if transcripts is not None:
        fields["modmail.transcripts"] = bool(transcripts)
    if ping_staff is not None:
        fields["modmail.ping_staff"] = bool(ping_staff)

    if not fields:
        config: Dict[str, Any] = _modmail_config(ctx.guild.id)
        delay: int = int(config.get("delete_after") or 0)
        return await ctx.send(
            "\U0001f39f\ufe0f **Ticket settings**\n"
            f"\u2022 Auto-delete on close: "
            f"{f'after **{delay}s**' if delay else '**off** (channel is archived instead)'}\n"
            f"\u2022 DM transcript to participants: "
            f"**{'on' if config.get('dm_transcript') else 'off'}**\n"
            f"\u2022 Transcript in the log channel: "
            f"**{'on' if config.get('transcripts') else 'off'}**\n"
            f"\u2022 Ping staff on open: **{'on' if config.get('ping_staff') else 'off'}**",
            ephemeral=True,
        )

    saved: bool = await bot.settings.push_fields(ctx.guild.id, fields)
    log.info("Ticket config updated in guild %s by %s.", ctx.guild.id, ctx.author)
    notes: List[str] = []
    if delete_after is not None:
        notes.append(
            f"closed tickets are deleted after **{int(delete_after)}s**"
            if int(delete_after) > 0
            else "closed tickets are **archived**, not deleted"
        )
    if dm_transcript is not None:
        notes.append(f"transcript DMs **{'on' if dm_transcript else 'off'}**")
    await ctx.send(
        f"\u2705 Updated **{len(fields)}** setting(s)."
        + (" \u2014 " + "; ".join(notes) if notes else "")
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )


@ticket_group.command(name="close", description="Close the ticket in this channel")
@commands.guild_only()
@app_commands.describe(reason="Why the ticket is being closed")
async def ticket_close_cmd(
    ctx: commands.Context, *, reason: Optional[str] = "No reason given"
) -> None:
    config: Dict[str, Any] = _modmail_config(ctx.guild.id)
    record: Optional[Dict[str, Any]] = (config.get("tickets") or {}).get(
        str(ctx.channel.id)
    )
    if record is None or record.get("closed"):
        return await ctx.send(
            "\u274c This channel isn't an open ticket.", ephemeral=True
        )
    if str(ctx.author.id) != str(record.get("user_id")) and not member_has_perms(
        ctx.author, manage_messages=True
    ):
        return await ctx.send(
            "\u274c Only the ticket opener or a staff member can close it.",
            ephemeral=True,
        )

    await ctx.defer(ephemeral=True)
    closed: bool = await _modmail_close(
        ctx.guild,
        ctx.channel,
        closed_by=ctx.author,
        reason=str(reason or "No reason given"),
    )
    if closed:
        await ctx.send("\u2705 Ticket closed.", ephemeral=True)
    else:
        await ctx.send("\u26a0\ufe0f I couldn't close that ticket.", ephemeral=True)


@ticket_group.command(
    name="add", description="Give another member access to this ticket"
)
@app_commands.default_permissions(manage_messages=True)
@custom_has_permissions(manage_messages=True)
@app_commands.describe(user="Who to add to this ticket")
async def ticket_add(ctx: commands.Context, user: discord.Member) -> None:
    if not member_has_perms(ctx.author, manage_messages=True):
        return await ctx.send(
            "\u274c You need the **Manage Messages** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _modmail_config(ctx.guild.id)
    if str(ctx.channel.id) not in (config.get("tickets") or {}):
        return await ctx.send(
            "\u274c This channel isn't a ticket channel.", ephemeral=True
        )
    try:
        await ctx.channel.set_permissions(
            user,
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            reason=f"Added to ticket by {ctx.author}",
        )
    except discord.Forbidden:
        return await ctx.send("\u274c I can't change permissions here.", ephemeral=True)

    tickets: Dict[str, Any] = dict(config.get("tickets") or {})
    record: Dict[str, Any] = dict(tickets.get(str(ctx.channel.id)) or {})
    people: List[str] = [str(p) for p in (record.get("participants") or []) if p]
    if str(user.id) not in people:
        people.append(str(user.id))
        record["participants"] = people
        tickets[str(ctx.channel.id)] = record
        await bot.settings.push_fields(ctx.guild.id, {"modmail.tickets": tickets})

    await ctx.send(f"\u2705 {user.mention} can now see this ticket.", ephemeral=True)


@ticket_group.command(name="block", description="Stop a member from opening tickets")
@app_commands.default_permissions(manage_guild=True)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(user="Who to block or unblock")
async def ticket_block(ctx: commands.Context, user: discord.Member) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission.", ephemeral=True
        )
    config: Dict[str, Any] = _modmail_config(ctx.guild.id)
    blocked: List[str] = [str(u) for u in (config.get("blocked") or [])]

    if str(user.id) in blocked:
        blocked.remove(str(user.id))
        outcome: str = f"\u2705 {user.mention} can open tickets again."
    else:
        blocked.append(str(user.id))
        outcome = f"\U0001f6ab {user.mention} can no longer open tickets."

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"modmail.blocked": blocked}
    )
    log.info("Ticket block toggled for %s in guild %s.", user.id, ctx.guild.id)
    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
PERSISTENT_VIEWS.append(ModmailPanelView)
PERSISTENT_VIEWS.append(TicketCloseView)
