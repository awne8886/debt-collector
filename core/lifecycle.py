"""Clean shutdown (SIGTERM/SIGINT) and the reconnecting gateway supervisor."""

import asyncio
import logging
import random
import signal
import time
from typing import Any, Callable, List, Optional

import aiohttp
import discord

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Clean shutdown (SIGTERM from Render on every deploy / restart)
# --------------------------------------------------------------------------- #

SHUTDOWN_TIMEOUT_S: float = 20.0  # Render waits 30 s after SIGTERM before SIGKILL


class ShutdownState:
    """Process-wide "we are shutting down" flag, with an event to wait on."""

    def __init__(self) -> None:
        self.requested: bool = False
        self.reason: Optional[str] = None
        self.task: Optional["asyncio.Task[None]"] = None
        self._event: Optional[asyncio.Event] = None

    def _get_event(self) -> asyncio.Event:
        if self._event is None:
            self._event = asyncio.Event()
            if self.requested:
                self._event.set()
        return self._event

    def request(self, reason: str) -> bool:
        """Set the flag. Returns False if a shutdown was already requested."""
        if self.requested:
            return False
        self.requested = True
        self.reason = reason
        if self._event is not None:
            self._event.set()
        return True

    async def wait(self, within: Optional[float] = None) -> bool:
        """Wait until shutdown is requested (or ``within`` seconds). Returns the flag."""
        if self.requested:
            return True
        try:
            await asyncio.wait_for(self._get_event().wait(), within)
        except asyncio.TimeoutError:
            pass
        return self.requested

    def reset(self) -> None:  # tests only
        self.__init__()


SHUTDOWN: ShutdownState = ShutdownState()


async def _close_within(
    close: Callable[[], Any], budget: float = SHUTDOWN_TIMEOUT_S
) -> None:
    try:
        await asyncio.wait_for(close(), budget)
    except asyncio.TimeoutError:
        log.warning("Shutdown took longer than %.0fs; exiting anyway.", budget)
    except Exception:
        log.exception("Shutdown failed part-way; exiting anyway.")


def handle_shutdown_signal(
    signal_name: str, close: Callable[[], Any]
) -> Optional["asyncio.Task[None]"]:
    """Signal handler body: set the flag once and schedule ``close()`` (time-boxed)."""
    if not SHUTDOWN.request(signal_name):
        return None
    log.info("Shutting down (%s).", signal_name)
    SHUTDOWN.task = asyncio.get_running_loop().create_task(
        _close_within(close), name="shutdown"
    )
    return SHUTDOWN.task


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop, close: Callable[[], Any]
) -> List[str]:
    """Route SIGTERM/SIGINT to handle_shutdown_signal where the platform allows it."""
    installed: List[str] = []
    for name in ("SIGTERM", "SIGINT"):
        sig: Any = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, handle_shutdown_signal, name, close)
        except (NotImplementedError, RuntimeError, ValueError, AttributeError):
            continue  # e.g. Windows, or not in the main thread
        installed.append(name)
    return installed


async def supervise(
    start: Callable[[], Any],
    *,
    is_closed: Callable[[], bool] = lambda: False,
    base_backoff: float = 15.0,
    max_backoff: float = 300.0,
) -> str:
    """Run ``start()`` and reconnect with backoff, unless a shutdown was requested.

    Returns why it stopped: "clean", "shutdown" or "closed".
    """
    backoff: float = base_backoff
    while not SHUTDOWN.requested:
        attempt_started: float = time.monotonic()
        try:
            await start()
            if SHUTDOWN.requested:
                break
            log.info("Gateway session closed cleanly - exiting supervisor.")
            return "clean"
        except (discord.LoginFailure, discord.PrivilegedIntentsRequired) as exc:
            log.critical("Unrecoverable startup failure: %s", exc)
            raise SystemExit(1) from exc
        except discord.HTTPException as exc:
            if exc.status == 429:
                log.warning("Rate limited (HTTP 429) during startup: %s", exc)
            else:
                log.warning(
                    "HTTP error during startup (status %s): %s", exc.status, exc
                )
        except (
            discord.ConnectionClosed,
            discord.GatewayNotFound,
            aiohttp.ClientError,
            OSError,
        ) as exc:
            log.warning("Transport failure: %s", exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Unexpected supervisor failure.")

        if SHUTDOWN.requested:
            break
        if is_closed():
            # discord.py closed the client itself; it can't log in again in-process.
            log.warning("Client was closed - exiting so the host restarts the process.")
            return "closed"

        if time.monotonic() - attempt_started > 600:
            backoff = base_backoff  # A long-lived session earns a fresh budget.

        delay: float = backoff + random.uniform(0.0, 5.0)
        log.info("Reconnecting in %.1fs.", delay)
        if await SHUTDOWN.wait(delay):
            break
        backoff = min(backoff * 2, max_backoff)
    log.info("Shutdown requested - not reconnecting.")
    return "shutdown"
