"""Runtime stats: one source for the memory log, /diagnose and /health."""

import asyncio
import logging
from typing import Any, Dict, List

from utils.runtime_stats import (
    LoopLagMonitor,
    peak_rss_bytes,
    rss_bytes,
    thread_count,
    to_mb,
)

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Runtime stats - one source for the memory log, /diagnose and /health
# --------------------------------------------------------------------------- #


def collect_runtime_stats(target: Any) -> Dict[str, Any]:
    """Process and in-memory state numbers. Never touches the database or network."""
    rss = rss_bytes()
    stats: Dict[str, Any] = {
        "rss_mb": to_mb(rss),
        "peak_rss_mb": to_mb(peak_rss_bytes()),
        "guilds": len(getattr(target, "guilds", None) or []),
        "threads": thread_count(),
    }
    settings = getattr(target, "settings", None)
    try:
        stats["settings_cache_entries"] = int(settings.cache_stats()["entries"])
    except Exception:
        stats["settings_cache_entries"] = None
    cache = getattr(target, "attachment_cache", None)
    try:
        stats["attachment_cache"] = cache.stats()
    except Exception:
        stats["attachment_cache"] = None
    buffers: Dict[int, List[Any]] = getattr(target, "ai_history_buffer", None) or {}
    pending: Dict[int, List[Any]] = getattr(target, "ai_pending", None) or {}
    stats["ai_buffer_channels"] = len(buffers)
    stats["ai_buffer_messages"] = sum(len(v) for v in buffers.values())
    stats["ai_pending_messages"] = sum(len(v) for v in pending.values())
    stats["snipes"] = len(getattr(target, "snipes", None) or {})
    stats["background_tasks"] = len(getattr(target, "_background_tasks", None) or ())
    try:
        stats["asyncio_tasks"] = len(asyncio.all_tasks())
    except RuntimeError:
        stats["asyncio_tasks"] = None
    stats["voice_clients"] = len(getattr(target, "voice_clients", None) or [])
    lag = getattr(target, "loop_lag", None)
    stats["loop_lag"] = lag.stats() if isinstance(lag, LoopLagMonitor) else None
    return stats


def _format_memory_line(stats: Dict[str, Any]) -> str:
    cache = stats.get("attachment_cache") or {}
    return (
        f"rss_mb={stats.get('rss_mb')} peak_rss_mb={stats.get('peak_rss_mb')} "
        f"guilds={stats.get('guilds')} "
        f"settings_cache={stats.get('settings_cache_entries')} "
        f"attachment_cache_bytes={cache.get('bytes', 0)} "
        f"attachment_cache_entries={cache.get('entries', 0)} "
        f"ai_buffer_channels={stats.get('ai_buffer_channels')} "
        f"ai_buffer_messages={stats.get('ai_buffer_messages')} "
        f"ai_pending={stats.get('ai_pending_messages')} "
        f"snipes={stats.get('snipes')} threads={stats.get('threads')} "
        f"tasks={stats.get('asyncio_tasks')}"
    )
