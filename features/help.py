"""The help system: categories, summaries, /help and /commands."""

import logging
from typing import Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.config import COMMAND_PREFIX
from core.permissions import _can_run, _required_permissions
from core.bot import bot
from features.reactions import REACTIONS

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Help system
# --------------------------------------------------------------------------- #

CATEGORY_LABELS: Dict[str, str] = {
    "moderation": "\U0001f6e1\ufe0f Moderation",
    "roles": "\U0001f3ad Roles",
    "utility": "\U0001f6e0\ufe0f Utility",
    "info": "\u2139\ufe0f Info",
    "config": "\u2699\ufe0f Configuration (admin)",
    "ai": "\U0001f9e0 AI",
    "fun": "\U0001f389 Fun & roleplay",
    "music": "\U0001f3b5 Music",
}

# Root command -> category. Every root command in the tree must appear here;
# _uncategorised_roots() reports any that do not so the listing cannot drift.
COMMAND_CATEGORY: Dict[str, str] = {
    # Moderation
    "ban": "moderation",
    "unban": "moderation",
    "kick": "moderation",
    "softban": "moderation",
    "timeout": "moderation",
    "untimeout": "moderation",
    "warn": "moderation",
    "warnings": "moderation",
    "clearwarns": "moderation",
    "tempban": "moderation",
    "massban": "moderation",
    "quarantine": "moderation",
    "unquarantine": "moderation",
    "purge": "moderation",
    "lock": "moderation",
    "unlock": "moderation",
    "slowmode": "moderation",
    "nickname": "moderation",
    "case": "moderation",
    "snipe": "moderation",
    "raid": "moderation",
    # Roles
    "role": "roles",
    "roleall": "roles",
    "joinrole": "roles",
    "reactionrole": "roles",
    "inrole": "roles",
    # Utility
    "afk": "utility",
    "echo": "utility",
    "poll": "utility",
    "reaction": "utility",
    "remindme": "utility",
    "ticket": "utility",
    "welcomer": "config",
    "leaver": "config",
    "steal": "utility",
    "tag": "utility",
    # Info
    "ping": "info",
    "uptime": "info",
    "userinfo": "info",
    "serverinfo": "info",
    "avatar": "info",
    "banner": "info",
    "roleinfo": "info",
    "membercount": "info",
    "emojis": "info",
    "permcheck": "info",
    "help": "info",
    "commands": "info",
    # Configuration
    "set": "config",
    "echoset": "config",
    "autoreact": "config",
    "autorespond": "config",
    "autopurge": "config",
    "automod": "config",
    "sticky": "config",
    "starboard": "config",
    "remind": "config",
    "export": "config",
    "import": "config",
    "clone_server": "config",
    "rollback_clone": "config",
    "errors": "config",
    "diagnose": "config",
    "screening": "config",
    "messagelog": "config",
    "modlog": "config",
    # AI
    "ai": "ai",
    "ask": "ai",
    "aiopt": "ai",
    # Music
    "play": "music",
    "playnext": "music",
    "pause": "music",
    "resume": "music",
    "skip": "music",
    "stop": "music",
    "leave": "music",
    "nowplaying": "music",
    "volume": "music",
    "loop": "music",
    "seek": "music",
    "queue": "music",
}

# Precise one-line summaries. Keys are fully qualified names, so subcommands are
# addressed as "group sub". Falls back to the command's own description.
COMMAND_SUMMARY: Dict[str, str] = {
    # --- Moderation ---
    "ban": "Permanently ban a member; they cannot rejoin until unbanned.",
    "unban": "Lift a ban using the user's numeric ID (they are no longer in the member list).",
    "kick": "Remove a member from the server; they can rejoin with a new invite.",
    "softban": "Ban and immediately unban a member to wipe their recent messages. They can rejoin.",
    "timeout": "Mute a member for a number of minutes; they cannot talk, react or join voice.",
    "untimeout": "End a member's timeout immediately.",
    "warn": "Log a warning against a member and DM them the reason.",
    "warnings": "List every warning recorded against a member, newest first.",
    "clearwarns": "Erase all stored warnings for a member.",
    "tempban": "Ban a member and unban them automatically once the duration expires.",
    "massban": "Ban everyone matching a filter (account age, join time, name pattern, no avatar). "
    "Shows a preview and requires confirmation before anything happens.",
    "quarantine": "Strip a member's roles and isolate them behind a locked-down role. "
    "Fully reversible - the original roles are stored.",
    "unquarantine": "Release a quarantined member and restore the roles they had.",
    "purge": "Bulk-delete recent messages in this channel.",
    "purge user": "Delete only the messages a specific member sent recently.",
    "purge contains": "Delete recent messages containing a given piece of text.",
    "purge bots": "Delete recent messages posted by bots.",
    "purge links": "Delete recent messages containing links or server invites.",
    "lock": "Stop @everyone from sending messages in a channel.",
    "unlock": "Reverse a lock and let @everyone post again.",
    "slowmode": "Set the seconds each member must wait between messages; 0 disables it.",
    "nickname": "Change a member's nickname, or reset it by leaving the name empty.",
    "case": "Show a member's full moderation history from the numbered case book.",
    "snipe": "Show the most recently deleted message in this channel.",
    "raid": "Raid detection: auto-arms on a join spike and screens new accounts.",
    "raid on": "Engage raid mode by hand for a set number of minutes.",
    "raid off": "Disengage raid mode and clear the join counter.",
    "raid config": "Set the join threshold, detection window, minimum account age, "
    "response action and quarantine role.",
    # --- Roles ---
    "role": "Add or remove a single role from one member.",
    "roleall": "Give one role to every member in the server; paced to respect rate limits.",
    "joinrole": "Choose roles applied automatically to everyone who joins.",
    "reactionrole": "Bind an emoji on a message to a role members can self-assign.",
    "reactionrole list": "Show every reaction role configured in this server.",
    "reactionrole remove": "Unbind one emoji, or every emoji, from a message.",
    "inrole": "List every member who currently holds a role, with join dates.",
    # --- Utility ---
    "afk": "Mark yourself away; the bot collects your pings and reports them when you return.",
    "echo": "Make the bot post a message, optionally in a different channel.",
    "poll": "Start a reaction poll with up to ten options.",
    "reaction": "Make the bot add an emoji reaction to a linked message.",
    "remindme": "Schedule a personal reminder delivered by DM; survives a bot restart.",
    "remindme list": "Show your pending reminders and when each one fires.",
    "remindme cancel": "Cancel one pending reminder by its short id.",
    "steal": "Copy one or more custom emojis from another server into this one.",
    "tag": "Call up a saved text snippet by name.",
    "tag add": "Create or overwrite a saved snippet.",
    "tag remove": "Delete a saved snippet.",
    "tag list": "List every saved snippet in this server.",
    # --- Info ---
    "ping": "Show the bot's gateway latency and confirm it is responding.",
    "uptime": "How long the bot has been running since its last restart.",
    "userinfo": "Account age, join date, roles and key permissions for a member.",
    "serverinfo": "Member counts, channels, roles, boost level and creation date.",
    "avatar": "Show a user's avatar at full size.",
    "banner": "Show a user's profile banner, if they have one.",
    "roleinfo": "Colour, position, member count and permissions for a role.",
    "membercount": "Current member total for this server.",
    "emojis": "List every custom emoji in this server.",
    "permcheck": "Resolve a member's effective permissions in a channel, including which "
    "role or overwrite is responsible.",
    "help": "This help. Pass a command name for its full usage and options.",
    "commands": "Browse every command grouped by category.",
    # --- Configuration ---
    "set": "Core server configuration.",
    "set prefix": "Change the chat command prefix for this server (default `!`).",
    "echoset": "Allow or block the /echo command server-wide.",
    "autoreact": "React with chosen emojis whenever a message contains a trigger word.",
    "autoreact add": "Add a trigger plus the emojis to react with, and whether it matches "
    "as a whole word or anywhere inside longer words.",
    "autoreact remove": "Remove one emoji from a trigger, or delete the trigger entirely.",
    "autoreact list": "Show every trigger, its emojis and its match mode.",
    "autoreact clear": "Delete every autoreact trigger in this server.",
    "autorespond": "Automatic replies triggered by keywords in chat.",
    "autopurge": "Auto-delete every new message in chosen channels.",
    "autopurge on": "Start auto-deleting in a channel, optionally for a limited time.",
    "autopurge off": "Stop auto-deleting in a channel.",
    "autopurge exempt": "Add or remove a role whose messages are never auto-deleted.",
    "autopurge status": "Show where auto-purge is active and which roles are exempt.",
    "automod": "Automatic filtering of invites, links, spam, caps and mass mentions.",
    "automod set": "Turn one automod rule on or off.",
    "automod action": "Set what happens after repeated automod removals.",
    "automod limits": "Tune the spam message count and mass-mention thresholds.",
    "automod exempt": "Roles automod should never act on.",
    "sticky": "Keep a message pinned to the bottom of a channel as people talk.",
    "sticky off": "Stop the sticky message in a channel.",
    "sticky list": "Show every channel with a sticky message.",
    "starboard": "Repost popular messages to a highlights channel, sent as the "
    "original author via a webhook.",
    "starboard set": "Choose the highlights channel and how many reactions are needed.",
    "starboard threshold": "How many of any one emoji a message needs to be relayed.",
    "starboard block": "Stop one emoji from ever triggering the starboard, or unblock it.",
    "starboard ignore": "Stop watching reactions in one channel, or watch it again.",
    "starboard options": "Count self-reactions, relay bot messages, show the count line.",
    "starboard off": "Turn the starboard off, keeping the channel and settings.",
    "remind": "A recurring announcement posted to a channel on a fixed interval.",
    "remind set": "Create or update the recurring reminder.",
    "remind off": "Stop the recurring reminder.",
    "export": "Download this server's bot settings as a JSON backup.",
    "import": "Restore settings from an /export backup; merges by default.",
    "errors": "Recent runtime errors, deduplicated and newest first.",
    "errors detail": "Full traceback and context for one error id.",
    "errors stats": "Error totals grouped by exception type.",
    "errors clear": "Empty the in-memory error buffer (bot superuser only).",
    "screening": "Screen new joins for alt accounts and ban evasion, and act on the risky ones.",
    "screening config": "Turn screening on or off and tune the threshold, action and signals.",
    "screening pattern": "Add or remove a regex matched against joining members' names.",
    "screening test": "Score an existing member as if they had just joined, without acting.",
    "messagelog": "Log edited and deleted messages as embeds in a channel.",
    "messagelog ignore": "Stop or resume logging one channel.",
    "messagelog cache": "Hold deleted attachments in memory to prevent broken images.",
    "ticket": "Private staff tickets members open from a button panel.",
    "ticket setup": "Set the staff role, ticket category, log channel and ping behaviour.",
    "ticket panel": "Post the panel members press to open a ticket.",
    "ticket message": "Set the panel text or the welcome embed shown inside new tickets.",
    "ticket config": "Auto-delete delay after closing, transcript DMs and staff pings.",
    "ticket close": "Close the ticket in this channel; the channel is then deleted or archived.",
    "ticket add": "Give another member access to the current ticket.",
    "ticket block": "Stop a member from opening tickets, or let them again.",
    "clone_server": "Clone roles, channels, emojis, and settings from a reference server.",
    "rollback_clone": "Restore the server back to its state before the last clone operation.",
    "welcomer": "Greet members in a channel when they join.",
    "welcomer set": "Choose the channel joins are announced in, and optionally the text.",
    "welcomer message": "Set the join greeting, with placeholders for name, server and count.",
    "welcomer style": "Embed mode, title, colour, banner image, thumbnail and ping.",
    "welcomer dm": "Also send new members a private message when they join.",
    "welcomer test": "Preview the greeting on yourself without waiting for a join.",
    "welcomer off": "Stop announcing joins, keeping the message and styling.",
    "leaver": "Announce in a channel when a member leaves.",
    "leaver set": "Choose the channel leaves are announced in, and optionally the text.",
    "leaver message": "Set the farewell text, with placeholders for name, server and count.",
    "leaver style": "Embed mode, title, colour, banner image and thumbnail.",
    "leaver test": "Preview the farewell on yourself.",
    "leaver off": "Stop announcing leaves, keeping the message and styling.",
    "set messagelog": "Choose the channel edited and deleted messages are logged to.",
    "set modlog": "Choose the channel moderation actions are logged to.",
    "diagnose": "Health check: gateway, database, memory, event-loop lag, music/WARP, "
    "background loops and the bot's missing permissions.",
    # --- AI ---
    "ai": "Configure the AI assistant for this server.",
    "ai setup": "Enable or disable the AI in a channel and set its reply chance.",
    "ai status": "Show the complete AI configuration for this server.",
    "ai persona": "Set the server-wide personality the AI writes with.",
    "ai persona_show": "Show the current persona in full.",
    "ai persona_clear": "Reset the persona to the default.",
    "ai preset": "Apply a ready-made persona preset.",
    "ai channel_persona": "Add an extra instruction that applies in one channel only.",
    "ai user_persona": "Set how the AI treats one specific member.",
    "ai remove_user_persona": "Remove a member's custom AI behaviour.",
    "ai user_persona_list": "List every per-user AI instruction.",
    "ai probability": "Base percentage chance the AI replies without being addressed.",
    "ai cooldown": "Minimum seconds between AI replies in the same channel.",
    "ai tuning": "Reply length (max tokens) and creativity (temperature).",
    "ai wordlimit": "Discard AI replies longer than a set number of words, so "
    "hallucinated essays and leaked instructions never reach chat.",
    "ai limit": "Cap how many AI replies this server may use per day; 0 means unlimited.",
    "ai models": "List the models available from each provider.",
    "ai model": "Choose which model a provider should use.",
    "ai providers": "Provider health, API key state and fallback order.",
    "ai order": "Set the order providers are tried in when one fails.",
    "ai ignore": "Exclude specific users or roles from the AI entirely.",
    "ai reset": "Wipe the AI's memory of a channel's conversation.",
    "ai stats": "AI usage counters since the last restart.",
    "ask": "Ask the AI a one-off question with no conversation memory.",
    "aiopt": "Opt yourself out of, or back into, the AI reading and replying to you.",
    # --- Music ---
    "play": "Join your voice channel and play a YouTube / YouTube Music link, or the top result for a search.",
    "playnext": "Like play, but the track goes to the front of the queue.",
    "pause": "Pause the current track.",
    "resume": "Resume a paused track.",
    "skip": "Skip the current track. The requester and staff skip instantly; otherwise listeners vote.",
    "stop": "Stop playback and clear the queue (staff or the only listener).",
    "leave": "Disconnect from voice and clear the queue (staff or the only listener).",
    "nowplaying": "Show the current track with a progress bar.",
    "volume": "Show or set the playback volume (0-150%).",
    "loop": "Loop the current track or the whole queue; with no argument it cycles off, track, queue.",
    "seek": "Jump to a time in the current track, e.g. `1:30` or `90`.",
    "queue": "Show the queue.",
    "queue show": "Show the queue with durations and who requested each track.",
    "queue shuffle": "Shuffle the waiting tracks.",
    "queue remove": "Remove a waiting track by position (its requester or staff).",
    "queue move": "Move a waiting track to a new position.",
    "queue clear": "Remove every waiting track but keep the current one playing (staff or the only listener).",
    "queue skipto": "Skip ahead to a queue position (staff or the only listener).",
}


PEASANT_TAUNTS: Tuple[str, ...] = ("peasant", "lol, imagine having no perms")


def _visible_commands(
    user: Optional[discord.abc.User] = None,
) -> List[commands.Command]:
    """Registered commands, deduplicated and sorted.

    When a user is supplied, commands gated behind permissions they lack are
    omitted entirely - they never appear in help, listings or autocomplete.
    """
    seen: Dict[str, commands.Command] = {}
    for command in bot.walk_commands():
        if command.hidden:
            continue
        if user is not None and not _can_run(user, command):
            continue
        seen[command.qualified_name] = command
    return sorted(seen.values(), key=lambda c: c.qualified_name)


def _category_for(command: commands.Command) -> str:
    root: str = command.qualified_name.split(" ")[0]
    if root in REACTIONS:
        return "fun"
    return COMMAND_CATEGORY.get(root, "uncategorised")


def _uncategorised_roots() -> List[str]:
    """Root commands with no category. Empty is the healthy state."""
    return sorted(
        {
            command.qualified_name
            for command in _visible_commands()
            if command.parent is None and _category_for(command) == "uncategorised"
        }
    )


def _summary_for(command: commands.Command) -> str:
    return (
        COMMAND_SUMMARY.get(command.qualified_name)
        or command.description
        or command.short_doc
        or "No description."
    )


def _usage_for(command: commands.Command) -> str:
    parts: List[str] = [f"/{command.qualified_name}"]
    for name, parameter in command.clean_params.items():
        parts.append(f"<{name}>" if parameter.required else f"[{name}]")
    return " ".join(parts)


def _permission_note(command: commands.Command) -> str:
    """Human-readable form of the command's permission gate."""
    perms: Optional[discord.Permissions] = _required_permissions(command)
    if perms is None:
        return "Anyone can use this"
    names: List[str] = [
        name.replace("_", " ").title() for name, value in perms if value
    ]
    return "Requires " + ", ".join(f"**{name}**" for name in names)


def _parameter_lines(command: commands.Command) -> List[str]:
    described: Dict[str, str] = {}
    choices: Dict[str, List[str]] = {}
    app_command = getattr(command, "app_command", None)
    for parameter in getattr(app_command, "parameters", None) or []:
        described[parameter.name] = parameter.description or ""
        if parameter.choices:
            choices[parameter.name] = [
                str(choice.value) for choice in parameter.choices
            ]

    lines: List[str] = []
    for name, parameter in command.clean_params.items():
        flag: str = "required" if parameter.required else "optional"
        text: str = described.get(name, "")
        detail: str = f"`{name}` ({flag})"
        if text:
            detail += f" - {text}"
        options: List[str] = choices.get(name, [])
        if options:
            detail += "\n\u2003choices: " + ", ".join(f"`{o}`" for o in options[:12])
        default = getattr(parameter, "displayed_default", None)
        if not parameter.required and default not in (None, "", "None"):
            detail += f"\n\u2003default: `{default}`"
        lines.append(detail)
    return lines


async def _command_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    needle: str = (current or "").casefold().lstrip("/")
    matches: List[str] = [
        command.qualified_name
        for command in _visible_commands(interaction.user)
        if needle in command.qualified_name.casefold()
    ]
    matches.sort(key=lambda n: (not n.casefold().startswith(needle), len(n), n))
    return [app_commands.Choice(name=f"/{name}", value=name) for name in matches[:25]]


@bot.hybrid_command(
    name="help",
    description="Show help, or full usage for one command",
)
@app_commands.describe(command="A command name, e.g. ban, purge user, ai persona")
@app_commands.autocomplete(command=_command_name_autocomplete)
async def help_cmd(ctx: commands.Context, *, command: Optional[str] = None) -> None:
    prefix: str = ctx.clean_prefix or COMMAND_PREFIX

    if command:
        query: str = command.strip().lstrip("/").strip()
        target: Optional[commands.Command] = bot.get_command(query)
        if target is not None and not _can_run(ctx.author, target):
            target = None  # gated commands stay invisible, not merely refused
        if target is None:
            suggestions: List[str] = [
                c.qualified_name
                for c in _visible_commands(ctx.author)
                if query.casefold() in c.qualified_name.casefold()
            ][:6]
            hint: str = (
                "\nDid you mean: " + ", ".join(f"`/{s}`" for s in suggestions)
                if suggestions
                else f"\nRun `{prefix}commands all` to see everything."
            )
            return await ctx.send(
                f"\u274c No command called `{query}`.{hint}", ephemeral=True
            )

        category: str = _category_for(target)
        embed: discord.Embed = discord.Embed(
            title=f"/{target.qualified_name}",
            description=_summary_for(target),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Usage",
            value=f"`{_usage_for(target)}`\n`{prefix}{target.qualified_name} ...`",
            inline=False,
        )

        parameters: List[str] = _parameter_lines(target)
        if parameters:
            embed.add_field(
                name="Options", value="\n".join(parameters)[:1024], inline=False
            )

        if isinstance(target, commands.Group) and target.commands:
            children: List[str] = sorted(
                f"`/{child.qualified_name}` - {_summary_for(child)}"
                for child in target.commands
                if not child.hidden
            )
            embed.add_field(
                name=f"Subcommands ({len(children)})",
                value="\n".join(children)[:1024],
                inline=False,
            )

        if target.aliases:
            embed.add_field(
                name="Aliases",
                value=", ".join(f"`{alias}`" for alias in target.aliases),
                inline=True,
            )
        embed.add_field(name="Permissions", value=_permission_note(target), inline=True)
        embed.add_field(
            name="Category",
            value=CATEGORY_LABELS.get(category, "\U0001f9ea Uncategorised"),
            inline=True,
        )
        embed.set_footer(
            text=f"Works as a slash command and as {prefix}{target.qualified_name}"
        )
        return await ctx.send(embed=embed, ephemeral=True)

    counts: Dict[str, int] = {}
    for entry in _visible_commands(ctx.author):
        key: str = _category_for(entry)
        counts[key] = counts.get(key, 0) + 1

    overview: List[str] = [
        f"{CATEGORY_LABELS[key]} - **{counts.get(key, 0)}** commands"
        for key in CATEGORY_LABELS
        if counts.get(key)
    ]
    stray: int = counts.get("uncategorised", 0)
    if stray:
        overview.append(f"\U0001f9ea Uncategorised - **{stray}** commands")

    embed = discord.Embed(
        title="Help",
        description=(
            "\n".join(overview)
            + f"\n\nEvery command works two ways: as a slash command (`/ban`) "
            f"and as a chat command (`{prefix}ban`).\n"
            f"\u2022 `{prefix}help <command>` - full usage, options and permissions\n"
            f"\u2022 `{prefix}commands <category>` - browse one category\n"
            f"\u2022 `{prefix}commands all` - every command at once"
        ),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Getting started",
        value=(
            f"`{prefix}help ban` - how one command works\n"
            f"`{prefix}set prefix` - change the chat prefix\n"
            f"`{prefix}set modlog` - log moderation to a channel\n"
            f"`{prefix}diagnose` - check the bot is healthy"
        ),
        inline=False,
    )
    embed.set_footer(text=f"{sum(counts.values())} commands available")
    await ctx.send(embed=embed, ephemeral=True)


@bot.hybrid_command(name="commands", description="Browse every command by category")
@app_commands.describe(category="Which category to show (default: overview)")
async def commands_cmd(
    ctx: commands.Context,
    category: Optional[
        Literal[
            "all",
            "moderation",
            "roles",
            "utility",
            "info",
            "config",
            "ai",
            "fun",
            "music",
        ]
    ] = None,
) -> None:
    prefix: str = ctx.clean_prefix or COMMAND_PREFIX
    everything: List[commands.Command] = _visible_commands(ctx.author)

    if category is None:
        counts: Dict[str, int] = {}
        for entry in everything:
            key: str = _category_for(entry)
            counts[key] = counts.get(key, 0) + 1
        lines: List[str] = [
            f"`{prefix}commands {key}` - {CATEGORY_LABELS[key]} ({counts.get(key, 0)})"
            for key in CATEGORY_LABELS
            if counts.get(key)
        ]
        embed: discord.Embed = discord.Embed(
            title="Commands",
            description="\n".join(lines)
            + f"\n\n`{prefix}commands all` - every command\n"
            f"`{prefix}help <command>` - usage for one command",
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"{len(everything)} commands available")
        return await ctx.send(embed=embed, ephemeral=True)

    wanted: List[str] = (
        list(CATEGORY_LABELS.keys()) if category == "all" else [str(category)]
    )
    if category == "all":
        stray: List[str] = _uncategorised_roots()
        if stray:
            wanted.append("uncategorised")

    rendered: List[str] = []
    for key in wanted:
        members: List[commands.Command] = [
            entry
            for entry in everything
            if _category_for(entry) == key and entry.parent is None
        ]
        if not members:
            continue

        label: str = CATEGORY_LABELS.get(key, "\U0001f9ea Uncategorised")
        rendered.append(f"__**{label}**__")

        if key == "fun":
            rendered.append(
                "Anime reaction GIFs, each takes an optional member: "
                + ", ".join(f"`/{name}`" for name in sorted(REACTIONS.keys()))
            )
            rendered.append("\u200b")
            continue

        for entry in sorted(members, key=lambda c: c.qualified_name):
            rendered.append(f"`{_usage_for(entry)}`\n\u2003{_summary_for(entry)}")
            if isinstance(entry, commands.Group):
                for child in sorted(entry.commands, key=lambda c: c.qualified_name):
                    if child.hidden:
                        continue
                    rendered.append(
                        f"\u2003\u21b3 `{_usage_for(child)}`\n\u2003\u2003{_summary_for(child)}"
                    )
        rendered.append("\u200b")

    while rendered and rendered[-1] == "\u200b":
        rendered.pop()

    if not rendered:
        return await ctx.send("\u274c Nothing in that category.", ephemeral=True)

    title: str = (
        "All commands"
        if category == "all"
        else CATEGORY_LABELS.get(str(category), "Commands")
    )
    pages = build_pages(
        title,
        rendered,
        discord.Color.blurple(),
        per_page=10,
        footer=f"{prefix}help <command> for full usage and options",
    )
    await send_pages(ctx, pages, ephemeral=True)
