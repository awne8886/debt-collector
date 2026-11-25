"""Render keepalive HTTP server (/ and /health) and the pull-request preview probe."""

import asyncio
import logging
import math
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from core.bot import bot
from core.lifecycle import SHUTDOWN
from core.stats import collect_runtime_stats

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Render keepalive
# --------------------------------------------------------------------------- #


HEALTH_STARTUP_GRACE_S: float = 180.0
_KEEPALIVE_RUNNER: Optional[Any] = None


def _deploy_version() -> Optional[str]:
    commit: str = os.getenv("RENDER_GIT_COMMIT", "").strip()
    return commit[:7] or None


def build_health_payload(
    target: Any, *, now: float, preview: bool = False
) -> Tuple[int, Dict[str, Any]]:
    """(HTTP status, JSON body) for /health. Reads memory only - never the database.

    200 while the gateway is ready with a finite latency, and during the first
    HEALTH_STARTUP_GRACE_S seconds after start (previews included); 503 otherwise.
    """
    uptime: float = max(0.0, now - float(getattr(target, "start_time", now)))
    try:
        ready: bool = bool(target.is_ready()) and not bool(target.is_closed())
    except Exception:
        ready = False
    latency: Any = getattr(target, "latency", float("nan"))
    latency_ok: bool = (
        isinstance(latency, (int, float))
        and not isinstance(latency, bool)
        and math.isfinite(latency)
    )
    shutting_down: bool = SHUTDOWN.requested
    if shutting_down:
        status, code = "shutting_down", 503
    elif ready and latency_ok:
        status, code = "ok", 200
    elif uptime < HEALTH_STARTUP_GRACE_S:
        status, code = "starting", 200
    else:
        status, code = "unavailable", 503

    stats: Dict[str, Any] = collect_runtime_stats(target)
    cache: Dict[str, Any] = stats.get("attachment_cache") or {}
    lag: Dict[str, Any] = stats.get("loop_lag") or {}
    payload: Dict[str, Any] = {
        "status": status,
        "ready": ready,
        "latency_ms": round(latency * 1000, 1) if latency_ok else None,
        "guilds": stats["guilds"],
        "uptime_s": int(uptime),
        "rss_mb": stats["rss_mb"],
        "peak_rss_mb": stats["peak_rss_mb"],
        "preview": preview,
        "version": _deploy_version(),
        "loop_lag_ms": {"last": lag.get("last_ms"), "max_5m": lag.get("max_ms")},
        "memory": {
            "settings_cache_entries": stats["settings_cache_entries"],
            "attachment_cache_bytes": cache.get("bytes", 0),
            "attachment_cache_entries": cache.get("entries", 0),
            "ai_buffer_channels": stats["ai_buffer_channels"],
            "ai_buffer_messages": stats["ai_buffer_messages"],
            "ai_pending_messages": stats["ai_pending_messages"],
            "snipes": stats["snipes"],
        },
        "runtime": {
            "threads": stats["threads"],
            "asyncio_tasks": stats["asyncio_tasks"],
            "background_tasks": stats["background_tasks"],
            "voice_clients": stats["voice_clients"],
        },
    }
    return code, payload


async def _keepalive_root(request: Any) -> Any:
    from aiohttp import web

    # Uptime monitors hit this to keep the free instance awake: always 200.
    return web.Response(text="OK")


async def _health_handler(request: Any) -> Any:
    from aiohttp import web

    code, payload = build_health_payload(
        bot, now=time.time(), preview=_is_preview_deploy()
    )
    return web.json_response(
        payload, status=code, headers={"Cache-Control": "no-store"}
    )


async def _start_keepalive_server() -> None:
    global _KEEPALIVE_RUNNER
    port: Optional[str] = os.getenv("PORT")
    if not port:
        return
    from aiohttp import web

    app: web.Application = web.Application()
    app.router.add_get("/", _keepalive_root)  # GET and HEAD
    app.router.add_get("/health", _health_handler)
    # No access log: two uptime monitors would otherwise add ~576 lines a day.
    runner: web.AppRunner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site: web.TCPSite = web.TCPSite(runner, "0.0.0.0", int(port))
    await site.start()
    _KEEPALIVE_RUNNER = runner
    log.info("Keepalive HTTP server listening on port %s.", port)


async def _stop_keepalive_server() -> None:
    global _KEEPALIVE_RUNNER
    runner, _KEEPALIVE_RUNNER = _KEEPALIVE_RUNNER, None
    if runner is not None:
        try:
            await runner.cleanup()
        except Exception as exc:
            log.debug("Keepalive server cleanup failed: %s", exc)


def _is_preview_deploy() -> bool:
    """True on a Render pull-request preview, unless PREVIEW_RUN_BOT=1 asks otherwise.

    Previews copy the production environment, bot token included, so without this each
    one logs in as a second copy of the bot: both answer commands, and in voice they
    keep kicking each other out of the channel.
    """
    if os.getenv("PREVIEW_RUN_BOT", "").strip().lower() in ("1", "true", "yes"):
        return False
    if os.getenv("IS_PULL_REQUEST", "").strip().lower() == "true":
        return True
    return bool(re.search(r"\bPR #\d+\b", os.getenv("RENDER_SERVICE_NAME", "")))


async def _run_preview_diagnostics() -> None:
    """On a preview: test YouTube from this host instead of logging in, then idle."""
    from utils.ytdl import probe

    log.warning(
        "Pull-request preview: not logging in to Discord (set PREVIEW_RUN_BOT=1 to override). "
        "Probing YouTube clients from this host instead."
    )
    urls: List[str] = [
        u.strip()
        for u in os.getenv("YTDL_PROBE_URLS", "").split(",")
        if u.strip()
    ] or [
        "https://www.youtube.com/watch?v=jNQXAC9IVRw",  # a plain upload
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",  # a label music video
    ]
    async def run(route: str) -> None:
        for url in urls:
            log.warning("Probing YouTube clients %s with %s", route, url)
            try:
                # Groups only: each single client can start deno, and running them all
                # back to back has hit the 512 MB limit of Render's free plan.
                results = await asyncio.to_thread(probe, url, False)
                working: List[str] = [label for label, ok, _, _ in results if ok]
                log.warning(
                    "YouTube probe finished %s for %s: %d of %d client setups work%s",
                    route, url, len(working), len(results),
                    f" ({'; '.join(working)})" if working else "",
                )
            except Exception:
                log.exception("YouTube probe crashed")

    from utils import warp

    await run("directly" if not os.getenv("YTDL_PROXY") else "through YTDL_PROXY")
    if warp.available():
        if await asyncio.to_thread(warp.ensure_started):
            await run("through Cloudflare WARP")
        else:
            log.warning("WARP didn't start, so it wasn't probed (see the lines above)")
    await SHUTDOWN.wait()  # idle until Render stops the preview
