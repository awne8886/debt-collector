"""Global command checks: per-command rate limits and the staff gate."""

import logging
import time
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import commands

from core.permissions import NotStaff, _can_run, _required_permissions, is_superuser
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Global rate limiting
# --------------------------------------------------------------------------- #

COMMAND_RATE_LIMITS: Dict[str, Tuple[int, float, str]] = {
    "roleall": (1, 300.0, "guild"),
    "steal": (2, 60.0, "guild"),
    "export": (1, 120.0, "guild"),
    "import": (1, 120.0, "guild"),
    "massban": (1, 120.0, "guild"),
    "purge user": (3, 30.0, "guild"),
    "purge contains": (3, 30.0, "guild"),
    "purge bots": (3, 30.0, "guild"),
    "purge links": (3, 30.0, "guild"),
    "snipe": (3, 10.0, "channel"),
    "poll": (2, 30.0, "channel"),
    "ask": (1, 10.0, "user"),
    "diagnose": (2, 30.0, "guild"),
    "remindme": (5, 60.0, "user"),
}
DEFAULT_RATE_LIMIT: Tuple[int, float, str] = (5, 10.0, "user")
_COOLDOWN_STATE: Dict[Tuple[str, str, int], List[float]] = {}


def _bucket_key(ctx: commands.Context, scope: str) -> int:
    if scope == "guild":
        return ctx.guild.id if ctx.guild is not None else ctx.author.id
    if scope == "channel":
        return ctx.channel.id
    return ctx.author.id


@bot.check
async def global_rate_limit(ctx: commands.Context) -> bool:
    """One dynamic bucket per command, plus a catch-all anti-spam bucket."""
    if ctx.command is None or is_superuser(ctx.author):
        return True

    if not _can_run(ctx.author, ctx.command):
        return True  # the staff gate rejects this; don't burn a cooldown slot

    name: str = ctx.command.qualified_name
    rate, per, scope = COMMAND_RATE_LIMITS.get(name, DEFAULT_RATE_LIMIT)
    key: Tuple[str, str, int] = (name, scope, _bucket_key(ctx, scope))
    now: float = time.monotonic()

    hits: List[float] = [t for t in _COOLDOWN_STATE.get(key, []) if now - t < per]
    if len(hits) >= rate:
        retry_after: float = per - (now - hits[0])
        _COOLDOWN_STATE[key] = hits
        raise commands.CommandOnCooldown(
            commands.Cooldown(rate, per), retry_after, commands.BucketType.default
        )

    hits.append(now)
    _COOLDOWN_STATE[key] = hits
    if len(_COOLDOWN_STATE) > 20_000:
        for stale_key in [
            k for k, v in _COOLDOWN_STATE.items() if not v or now - v[-1] > 600
        ]:
            _COOLDOWN_STATE.pop(stale_key, None)
    return True


@bot.check
async def staff_command_gate(ctx: commands.Context) -> bool:
    """Reject gated commands before the body runs, so non-staff get the taunt."""
    if ctx.command is None or ctx.guild is None:
        return True
    if _can_run(ctx.author, ctx.command):
        return True
    perms: Optional[discord.Permissions] = _required_permissions(ctx.command)
    required: List[str] = (
        [name.replace("_", " ") for name, value in perms if value] if perms else []
    )
    raise NotStaff(required)
