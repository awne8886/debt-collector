"""Helpers shared by two or more feature modules."""

import logging
import re
from typing import Dict, Optional

import discord
from discord.ext import commands

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def sanitize_mass_pings(text: str) -> str:
    return text.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")


def resolve_role(guild: discord.Guild, raw: str) -> Optional[discord.Role]:
    raw = raw.strip()
    mention_match: Optional[re.Match] = re.fullmatch(r"<@&(\d+)>", raw)
    if mention_match is not None:
        return guild.get_role(int(mention_match.group(1)))
    if raw.isdigit():
        by_id: Optional[discord.Role] = guild.get_role(int(raw))
        if by_id is not None:
            return by_id
    for role in guild.roles:
        if role.name == raw:
            return role
    lowered: str = raw.casefold()
    for role in guild.roles:
        if role.name.casefold() == lowered:
            return role
    return None


async def extract_message_from_link(
    ctx: commands.Context, link: str
) -> Optional[discord.Message]:
    match = re.search(r"channels/(\d+)/(\d+)/(\d+)", link)
    if not match:
        return None
    guild_id, channel_id, message_id = map(int, match.groups())
    if ctx.guild.id != guild_id:
        return None
    channel = ctx.guild.get_channel(channel_id)
    if not channel:
        return None
    try:
        return await channel.fetch_message(message_id)
    except Exception:
        return None


def humanize_seconds(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m:.0f}m {s:.0f}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h:.0f}h {m:.0f}m"
    d, h = divmod(h, 24)
    return f"{d:.0f}d {h:.0f}h"


def _extract_message_text(message: discord.Message) -> str:
    parts = [message.content] if message.content else []
    for embed in message.embeds:
        if embed.title:
            parts.append(embed.title)
        if embed.description:
            parts.append(embed.description)
        for embed_field in embed.fields:
            if embed_field.name:
                parts.append(embed_field.name)
            if embed_field.value:
                parts.append(embed_field.value)
        if embed.author and embed.author.name:
            parts.append(embed.author.name)
        if embed.footer and embed.footer.text:
            parts.append(embed.footer.text)
    return " ".join(parts).casefold()


# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Shared helpers — durations, mod log, case book
# --------------------------------------------------------------------------- #

DURATION_RE = re.compile(r"(\d+)\s*([smhdw])", re.IGNORECASE)
DURATION_UNITS: Dict[str, int] = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(raw: str) -> Optional[int]:
    """Turn '10m', '2h30m', '3d' into seconds. Returns None when nothing parses."""
    if not raw:
        return None
    total = 0
    matched = False
    for amount, unit in DURATION_RE.findall(raw):
        matched = True
        total += int(amount) * DURATION_UNITS[unit.lower()]
    if not matched:
        if raw.strip().isdigit():
            return int(raw.strip()) * 60  # bare number = minutes
        return None
    return total if 0 < total <= 3600 * 24 * 365 else None
