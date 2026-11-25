"""Auto-responses and auto-reactions."""

import logging
import re
from typing import Any, Dict, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.permissions import custom_has_permissions, member_has_perms
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


async def _apply_guild_automations(message: discord.Message) -> None:
    """Cache-first settings lookup drives autoreact / autorespond."""
    assert message.guild is not None
    settings = bot.settings.peek_settings(message.guild.id)

    content: str = message.content or ""
    if content:
        queued: List[str] = []
        for trigger, spec in _autoreact_triggers(message.guild.id).items():
            if not _autoreact_pattern(trigger, str(spec["mode"])).search(content):
                continue
            for emoji in spec["emojis"]:
                if emoji not in queued:
                    queued.append(emoji)
        for emoji in queued[:AUTOREACT_REACTION_CAP]:
            try:
                await message.add_reaction(emoji)
            except (discord.HTTPException, discord.Forbidden, TypeError):
                continue

    autorespond: Dict[str, Any] = settings.get("autorespond") or {}
    if autorespond.get("enabled") and autorespond.get("triggers"):
        lowered: str = message.content.casefold()
        for trigger, response in dict(autorespond["triggers"]).items():
            if trigger.casefold() in lowered:
                await message.channel.send(
                    str(response)[:2000],
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                break  # one response per message


@bot.hybrid_command(
    name="autorespond",
    description="Add, remove or list automatic replies to trigger words",
)
@app_commands.default_permissions(manage_guild=True)
async def autorespond_cmd(
    ctx: commands.Context,
    action: Literal["on", "off", "add", "remove"],
    trigger: Optional[str] = None,
    *,
    response: Optional[str] = None,
) -> None:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send(
            "❌ You need the **Manage Server** permission for this.", ephemeral=True
        )
        return
    settings: Dict[str, Any] = await bot.settings.fetch_settings(ctx.guild.id)  # type: ignore[union-attr]
    conf: Dict[str, Any] = dict(
        settings.get("autorespond") or {"enabled": False, "triggers": {}}
    )
    triggers: Dict[str, str] = dict(conf.get("triggers") or {})

    if action in ("on", "off"):
        conf["enabled"] = action == "on"
    elif action == "add":
        if not trigger or not response:
            await ctx.send(
                "❌ `add` needs both a trigger and a response.", ephemeral=True
            )
            return
        triggers[trigger[:100]] = response[:500]
        conf["triggers"] = triggers
    else:  # remove
        if not trigger or trigger not in triggers:
            await ctx.send("❌ That trigger does not exist.", ephemeral=True)
            return
        triggers.pop(trigger, None)
        conf["triggers"] = triggers

    saved: bool = await bot.settings.push_settings(ctx.guild.id, {"autorespond": conf})  # type: ignore[union-attr]
    await ctx.send(
        f"{'✅' if saved else '⚠️'} Autorespond is **{'enabled' if conf.get('enabled') else 'disabled'}** "
        f"with **{len(triggers)}** trigger(s)."
        + ("" if saved else " (database write failed)"),
        allowed_mentions=discord.AllowedMentions.none(),
    )


AUTOREACT_MAX_TRIGGERS: int = 50
AUTOREACT_MAX_EMOJIS: int = 5
AUTOREACT_REACTION_CAP: int = 6
AUTOREACT_MODES: Tuple[str, ...] = ("word", "anywhere")

_AUTOREACT_CUSTOM_EMOJI_RE: re.Pattern = re.compile(r"<a?:\w{2,32}:(\d{15,25})>")
_AUTOREACT_UNICODE_EMOJI_RE: re.Pattern = re.compile(
    "(?:[\U0001f1e6-\U0001f1ff]{2})"
    "|(?:[0-9#*]\ufe0f?\u20e3)"
    "|(?:[\U0001f000-\U0001faff\u2600-\u27bf\u2b00-\u2bff\u2190-\u21ff\u2300-\u23ff]"
    "[\ufe00-\ufe0f\U0001f3fb-\U0001f3ff]*"
    "(?:\u200d[\U0001f000-\U0001faff\u2600-\u27bf\u2b00-\u2bff\u2300-\u23ff]"
    "[\ufe00-\ufe0f\U0001f3fb-\U0001f3ff]*)*)"
)
_AUTOREACT_ANY_EMOJI_RE: re.Pattern = re.compile(
    "(?:" + _AUTOREACT_CUSTOM_EMOJI_RE.pattern + ")"
    "|(?:" + _AUTOREACT_UNICODE_EMOJI_RE.pattern + ")"
)
_AUTOREACT_PATTERN_CACHE: Dict[Tuple[str, str], re.Pattern] = {}


def _autoreact_pattern(trigger: str, mode: str) -> re.Pattern:
    """Compiled matcher for one trigger, cached across messages.

    "word" requires the trigger to stand alone, so `a` does not fire on `maze`.
    "anywhere" is a plain substring match, so `a` does fire on `maze`.
    """
    key: Tuple[str, str] = (trigger, mode)
    cached: Optional[re.Pattern] = _AUTOREACT_PATTERN_CACHE.get(key)
    if cached is not None:
        return cached

    escaped: str = re.escape(trigger)
    if mode == "word":
        pattern = re.compile(r"(?<!\w)" + escaped + r"(?!\w)", re.IGNORECASE)
    else:
        pattern = re.compile(escaped, re.IGNORECASE)

    if len(_AUTOREACT_PATTERN_CACHE) > 500:
        _AUTOREACT_PATTERN_CACHE.clear()
    _AUTOREACT_PATTERN_CACHE[key] = pattern
    return pattern


def _parse_emoji_tokens(raw: str) -> List[str]:
    """Pull custom and unicode emojis out of free text, left to right.

    One scan over a combined pattern, so the returned order is exactly the
    order they appear in the input - mixing custom and unicode does not
    reshuffle them. Accepts them run together or space separated.
    """
    tokens: List[str] = []
    for match in _AUTOREACT_ANY_EMOJI_RE.finditer(raw or ""):
        token: str = match.group(0).strip()
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def _emoji_is_usable(token: str) -> bool:
    """Custom emojis must be reachable by the bot; unicode is always fine."""
    match: Optional[re.Match] = _AUTOREACT_CUSTOM_EMOJI_RE.fullmatch(token)
    if match is None:
        return True
    return bot.get_emoji(int(match.group(1))) is not None


def _autoreact_triggers(guild_id: int) -> Dict[str, Dict[str, Any]]:
    """Normalized trigger map: {trigger: {"emojis": [...], "mode": "word"}}."""
    stored: Any = (bot.settings.peek_settings(guild_id).get("autoreact") or {}).get(
        "triggers"
    )
    result: Dict[str, Dict[str, Any]] = {}
    if not isinstance(stored, dict):
        return result
    for trigger, spec in stored.items():
        if isinstance(spec, dict):
            emojis: List[str] = [str(e) for e in (spec.get("emojis") or [])]
            mode: str = str(spec.get("mode") or "word")
        elif isinstance(spec, list):  # tolerate an older shape
            emojis = [str(e) for e in spec]
            mode = "word"
        else:
            continue
        if not emojis:
            continue
        result[str(trigger)] = {
            "emojis": emojis[:AUTOREACT_MAX_EMOJIS],
            "mode": mode if mode in AUTOREACT_MODES else "word",
        }
    return result


async def _autoreact_trigger_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    if interaction.guild_id is None:
        return []
    needle: str = (current or "").casefold()
    triggers: Dict[str, Dict[str, Any]] = _autoreact_triggers(interaction.guild_id)
    matches: List[str] = [t for t in triggers if needle in t.casefold()]
    matches.sort()
    return [
        app_commands.Choice(
            name=f"{trigger} ({''.join(triggers[trigger]['emojis'])})"[:100],
            value=trigger,
        )
        for trigger in matches[:25]
    ]


@bot.hybrid_group(
    name="autoreact",
    description="React with chosen emojis when a message contains a trigger",
    fallback="list",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
async def autoreact_group(ctx: commands.Context) -> None:
    triggers: Dict[str, Dict[str, Any]] = _autoreact_triggers(ctx.guild.id)
    if not triggers:
        return await ctx.send(
            "\U0001f4ed No autoreact triggers yet.\n"
            f"Add one with `{ctx.clean_prefix}autoreact add <trigger> <emojis> [mode]`.",
            ephemeral=True,
        )

    lines: List[str] = []
    for trigger in sorted(triggers):
        spec: Dict[str, Any] = triggers[trigger]
        note: str = (
            "whole word only"
            if spec["mode"] == "word"
            else "anywhere, even inside words"
        )
        lines.append(
            f"**{discord.utils.escape_markdown(trigger)}** \u2192 {' '.join(spec['emojis'])}\n"
            f"\u2003matches: {note}"
        )

    pages = build_pages(
        f"Autoreact \u00b7 {len(triggers)} trigger(s)",
        lines,
        discord.Color.blurple(),
        per_page=10,
        footer=f"{ctx.clean_prefix}autoreact add / remove / clear",
    )
    await send_pages(ctx, pages, ephemeral=True)


@autoreact_group.command(
    name="add",
    description="Add a trigger and the emojis to react with",
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    trigger="Text to watch for. Wrap in quotes for more than one word.",
    emojis="One or more emojis, run together or space separated (max 5)",
    mode="word = only as a standalone word · anywhere = also inside longer words",
)
async def autoreact_add(
    ctx: commands.Context,
    trigger: str,
    emojis: str,
    mode: Literal["word", "anywhere"] = "word",
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission for this.", ephemeral=True
        )

    clean_trigger: str = trigger.strip()[:100]
    if not clean_trigger:
        return await ctx.send("\u274c Give me a trigger to watch for.", ephemeral=True)

    parsed: List[str] = _parse_emoji_tokens(emojis)
    if not parsed:
        return await ctx.send(
            "\u274c I couldn't find any emojis in that. Pass them like "
            "`\U0001f44b\U0001f389` or `\U0001f44b \U0001f389`.",
            ephemeral=True,
        )

    usable: List[str] = [token for token in parsed if _emoji_is_usable(token)]
    rejected: List[str] = [token for token in parsed if token not in usable]
    if not usable:
        return await ctx.send(
            "\u274c I can't use those emojis \u2014 custom ones must be from a server I'm in.",
            ephemeral=True,
        )

    triggers: Dict[str, Dict[str, Any]] = _autoreact_triggers(ctx.guild.id)
    existing: Optional[Dict[str, Any]] = triggers.get(clean_trigger)
    if existing is None and len(triggers) >= AUTOREACT_MAX_TRIGGERS:
        return await ctx.send(
            f"\u274c This server already has {AUTOREACT_MAX_TRIGGERS} triggers. "
            f"Remove one first with `{ctx.clean_prefix}autoreact remove`.",
            ephemeral=True,
        )

    merged: List[str] = list((existing or {}).get("emojis") or [])
    added: List[str] = []
    for token in usable:
        if token in merged:
            continue
        if len(merged) >= AUTOREACT_MAX_EMOJIS:
            break
        merged.append(token)
        added.append(token)

    triggers[clean_trigger] = {"emojis": merged, "mode": mode}
    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"autoreact.triggers": triggers}
    )
    _AUTOREACT_PATTERN_CACHE.pop((clean_trigger, "word"), None)
    _AUTOREACT_PATTERN_CACHE.pop((clean_trigger, "anywhere"), None)

    log.info(
        "Autoreact trigger %r set in guild %s by %s (mode=%s, %d emoji).",
        clean_trigger,
        ctx.guild.id,
        ctx.author,
        mode,
        len(merged),
    )

    note: str = (
        "will fire only as a standalone word"
        if mode == "word"
        else "will fire anywhere, even inside longer words"
    )
    detail: str = ""
    if not added and existing is not None:
        detail = (
            "\nThose emojis were already on this trigger; the match mode was updated."
        )
    elif len(added) < len(usable):
        detail = f"\nStopped at the {AUTOREACT_MAX_EMOJIS}-emoji limit for one trigger."
    if rejected:
        detail += f"\n\u26a0\ufe0f Skipped {len(rejected)} emoji I can't access."
    if not saved:
        detail += (
            "\n\u26a0\ufe0f Saved in memory only \u2014 the database write failed."
        )

    await ctx.send(
        f"\u2705 **{discord.utils.escape_markdown(clean_trigger)}** \u2192 "
        f"{' '.join(merged)}\nIt {note}.{detail}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@autoreact_group.command(
    name="remove",
    description="Remove one emoji from a trigger, or the whole trigger",
)
@custom_has_permissions(manage_guild=True)
@app_commands.describe(
    trigger="Which trigger to change",
    emoji="Leave empty to delete the trigger entirely",
)
@app_commands.autocomplete(trigger=_autoreact_trigger_autocomplete)
async def autoreact_remove(
    ctx: commands.Context,
    trigger: str,
    emoji: Optional[str] = None,
) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission for this.", ephemeral=True
        )

    triggers: Dict[str, Dict[str, Any]] = _autoreact_triggers(ctx.guild.id)
    key: str = trigger.strip()
    if key not in triggers:
        match: Optional[str] = next(
            (t for t in triggers if t.casefold() == key.casefold()), None
        )
        if match is None:
            return await ctx.send(
                f"\u274c No trigger called **{discord.utils.escape_markdown(key[:60])}**.",
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        key = match

    if emoji is None:
        triggers.pop(key, None)
        outcome: str = (
            f"\U0001f5d1\ufe0f Removed the trigger **{discord.utils.escape_markdown(key)}**."
        )
    else:
        wanted: List[str] = _parse_emoji_tokens(emoji)
        remaining: List[str] = [
            token for token in triggers[key]["emojis"] if token not in wanted
        ]
        if len(remaining) == len(triggers[key]["emojis"]):
            return await ctx.send(
                "\u274c That emoji isn't on this trigger. Current: "
                + " ".join(triggers[key]["emojis"]),
                ephemeral=True,
            )
        if remaining:
            triggers[key]["emojis"] = remaining
            outcome = (
                f"\u2705 **{discord.utils.escape_markdown(key)}** \u2192 "
                + " ".join(remaining)
            )
        else:
            triggers.pop(key, None)
            outcome = (
                f"\U0001f5d1\ufe0f That was the last emoji, so the trigger "
                f"**{discord.utils.escape_markdown(key)}** was removed."
            )

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"autoreact.triggers": triggers}
    )
    _AUTOREACT_PATTERN_CACHE.pop((key, "word"), None)
    _AUTOREACT_PATTERN_CACHE.pop((key, "anywhere"), None)
    log.info(
        "Autoreact trigger %r edited in guild %s by %s.", key, ctx.guild.id, ctx.author
    )

    await ctx.send(
        outcome + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@autoreact_group.command(name="clear", description="Delete every autoreact trigger")
@custom_has_permissions(manage_guild=True)
async def autoreact_clear(ctx: commands.Context) -> None:
    if not member_has_perms(ctx.author, manage_guild=True):
        return await ctx.send(
            "\u274c You need the **Manage Server** permission for this.", ephemeral=True
        )

    count: int = len(_autoreact_triggers(ctx.guild.id))
    if not count:
        return await ctx.send(
            "\u2705 There were no autoreact triggers.", ephemeral=True
        )

    saved: bool = await bot.settings.push_fields(
        ctx.guild.id, {"autoreact.triggers": {}}
    )
    _AUTOREACT_PATTERN_CACHE.clear()
    log.info(
        "Autoreact cleared in guild %s by %s (%d triggers).",
        ctx.guild.id,
        ctx.author,
        count,
    )
    await ctx.send(
        f"\U0001f9f9 Removed **{count}** autoreact trigger(s)."
        + ("" if saved else "\n\u26a0\ufe0f The database write failed."),
        ephemeral=True,
    )
