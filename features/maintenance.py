"""Housekeeping loops: the idle-state janitor and the periodic memory log."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from discord.ext import tasks

from core.registry import STARTUP_LOOPS
from core.bot import bot
from core.checks import _COOLDOWN_STATE
from core.stats import _format_memory_line, collect_runtime_stats
from features.modmail import _MODMAIL_LAST_OPEN

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# State janitor - drops per-channel / per-user state nobody has touched lately
# --------------------------------------------------------------------------- #

STATE_IDLE_SECONDS: float = 3600.0


def _latest_stamp(value: Any) -> Optional[float]:
    """The most recent timestamp held by a tracker value, if it has one."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (list, tuple)):
        stamps = [v for v in value if isinstance(v, (int, float))]
        return max(stamps) if stamps else None
    if isinstance(value, dict):
        at = value.get("at")
        return float(at) if isinstance(at, (int, float)) else None
    return None


def _prune_stamped(
    mapping: Dict[Any, Any], now: float, idle: float = STATE_IDLE_SECONDS
) -> int:
    """Remove entries whose newest timestamp is older than ``idle`` (or missing)."""
    stale = [
        key
        for key, value in mapping.items()
        if (stamp := _latest_stamp(value)) is None or now - stamp > idle
    ]
    for key in stale:
        mapping.pop(key, None)
    return len(stale)


def prune_idle_state(
    target: Any,
    *,
    now: float,
    today: str,
    idle: float = STATE_IDLE_SECONDS,
    extra_stamped: Optional[Dict[str, Dict[Any, Any]]] = None,
    monotonic_now: Optional[float] = None,
    monotonic_stamped: Optional[Dict[str, Dict[Any, Any]]] = None,
) -> Dict[str, int]:
    """Drop idle per-channel / per-user state from ``target`` (the bot). Pure: no I/O.

    ``now`` is wall-clock (time.time()) and ``today`` is the UTC date
    (YYYY-MM-DD). Never removes a lock that is held. Returns removals per store.
    """
    removed: Dict[str, int] = {}

    for name in ("automod_recent", "automod_strikes", "ai_user_recent", "ai_repeat"):
        mapping = getattr(target, name, None)
        if isinstance(mapping, dict):
            removed[name] = _prune_stamped(mapping, now, idle)

    sticky_last: Dict[int, float] = getattr(target, "sticky_last", {})
    removed["sticky_last"] = _prune_stamped(sticky_last, now, idle)
    sticky_locks: Dict[int, asyncio.Lock] = getattr(target, "sticky_locks", {})
    idle_locks = [
        key
        for key, lock in sticky_locks.items()
        if key not in sticky_last and not lock.locked()
    ]
    for key in idle_locks:
        sticky_locks.pop(key, None)
    removed["sticky_locks"] = len(idle_locks)

    ai_daily: Dict[str, int] = getattr(target, "ai_daily", {})
    old_days = [key for key in ai_daily if not key.endswith(f":{today}")]
    for key in old_days:
        ai_daily.pop(key, None)
    removed["ai_daily"] = len(old_days)

    snipes: Dict[int, Any] = getattr(target, "snipes", {})
    old_snipes = []
    for key, sniped in snipes.items():
        deleted_at = getattr(sniped, "deleted_at", None)
        if deleted_at is None or now - deleted_at.timestamp() > idle:
            old_snipes.append(key)
    for key in old_snipes:
        snipes.pop(key, None)
    removed["snipes"] = len(old_snipes)

    # next_fire holds the *next* due time; one overdue by more than `idle` is no
    # longer serviced by the reminder loop (reminder disabled or guild left).
    next_fire: Dict[str, float] = getattr(target, "next_fire", {})
    overdue = [key for key, due in next_fire.items() if now - float(due) > idle]
    for key in overdue:
        next_fire.pop(key, None)
    removed["next_fire"] = len(overdue)

    # Cached starboard webhooks for channels the bot can no longer see.
    webhooks: Dict[int, Any] = getattr(target, "starboard_webhooks", {})
    is_ready = getattr(target, "is_ready", None)
    if callable(is_ready) and is_ready():
        gone = [cid for cid in webhooks if target.get_channel(cid) is None]
        for cid in gone:
            webhooks.pop(cid, None)
        removed["starboard_webhooks"] = len(gone)

    for name, mapping in (extra_stamped or {}).items():
        removed[name] = _prune_stamped(mapping, now, idle)
    if monotonic_now is not None:
        for name, mapping in (monotonic_stamped or {}).items():
            removed[name] = _prune_stamped(mapping, monotonic_now, idle)
    return removed


@tasks.loop(minutes=10)
async def state_janitor_loop() -> None:
    removed = prune_idle_state(
        bot,
        now=time.time(),
        today=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        extra_stamped={"modmail_last_open": _MODMAIL_LAST_OPEN},
        monotonic_now=time.monotonic(),
        monotonic_stamped={"command_cooldowns": _COOLDOWN_STATE},
    )
    music = bot.get_cog("Music")
    prune_players = getattr(music, "prune_idle_players", None)
    if callable(prune_players):
        removed["music_players"] = await prune_players(STATE_IDLE_SECONDS)
    total = sum(removed.values())
    if total:
        log.info(
            "State janitor removed %d idle entries (%s).",
            total,
            ", ".join(f"{k}={v}" for k, v in sorted(removed.items()) if v),
        )


@state_janitor_loop.before_loop
async def before_state_janitor_loop() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=30)
async def memory_report_loop() -> None:
    log.info("Memory: %s", _format_memory_line(collect_runtime_stats(bot)))


@memory_report_loop.before_loop
async def before_memory_report_loop() -> None:
    await bot.wait_until_ready()


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
STARTUP_LOOPS.append(state_janitor_loop)
STARTUP_LOOPS.append(memory_report_loop)
