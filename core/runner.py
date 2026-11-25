"""Process entry: what `python main.py` runs once every module is imported."""

import asyncio
import logging
import os
from typing import Optional

from core.bot import bot
from core.lifecycle import SHUTDOWN, install_signal_handlers, supervise
from core.web import (
    _is_preview_deploy,
    _run_preview_diagnostics,
    _start_keepalive_server,
    _stop_keepalive_server,
)

log: logging.Logger = logging.getLogger("debt-collector")


def run() -> None:
    """Run the bot: what main.py's `if __name__ == "__main__":` calls."""
    token: Optional[str] = os.getenv("DISCORD_TOKEN")
    if not token:
        log.critical("DISCORD_TOKEN environment variable not set - refusing to start.")
        raise SystemExit(1)

    async def runner() -> None:
        install_signal_handlers(asyncio.get_running_loop(), bot.close)
        await _start_keepalive_server()
        try:
            if _is_preview_deploy():
                # Stop probing as soon as Render asks the preview to stop.
                probe = asyncio.ensure_future(_run_preview_diagnostics())
                stop = asyncio.ensure_future(SHUTDOWN.wait())
                await asyncio.wait({probe, stop}, return_when=asyncio.FIRST_COMPLETED)
                for pending in (probe, stop):
                    pending.cancel()
            else:
                await supervise(lambda: bot.start(token), is_closed=bot.is_closed)
            if SHUTDOWN.task is not None:
                await SHUTDOWN.task
        finally:
            await _stop_keepalive_server()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("Interrupted - shutting down.")
