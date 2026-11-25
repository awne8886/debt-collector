"""Smoke test: setup_hook starts every registered loop, adds the persistent views and
the dynamic items, and loads the music extension (network and database mocked)."""

import os
import unittest
from unittest.mock import AsyncMock, patch

EXPECTED_LOOPS = [
    "reminder_loop",
    "ai_flush_loop",
    "tempban_loop",
    "personal_reminder_loop",
    "state_janitor_loop",
    "memory_report_loop",
]


class TestSetupHook(unittest.IsolatedAsyncioTestCase):
    async def test_setup_hook_starts_loops_views_and_music(self):
        import main  # noqa: F401  # registers every feature
        from core import registry
        from core.bot import bot

        self.assertEqual([loop.coro.__name__ for loop in registry.STARTUP_LOOPS], EXPECTED_LOOPS)
        self.assertEqual(
            [view.__name__ for view in registry.PERSISTENT_VIEWS],
            ["ModmailPanelView", "TicketCloseView"],
        )
        self.assertEqual([item.__name__ for item in registry.DYNAMIC_ITEMS], ["CountingSaveButton"])

        env = {"MUSIC_WARMUP": "0", "YTDL_WARP": "off"}
        try:
            with patch.dict(os.environ, env), \
                    patch.object(bot.settings, "ensure_indexes") as ensure_indexes, \
                    patch.object(bot.settings, "get_meta", return_value=None), \
                    patch.object(bot.settings, "set_meta") as set_meta, \
                    patch.object(bot.tree, "sync", AsyncMock(return_value=[])) as sync, \
                    patch.object(bot, "add_dynamic_items", wraps=bot.add_dynamic_items) as add_items:
                await bot.setup_hook()

                ensure_indexes.assert_called_once()
                sync.assert_awaited_once()
                set_meta.assert_called_once()
                add_items.assert_called_once_with(*registry.DYNAMIC_ITEMS)
            for loop in registry.STARTUP_LOOPS:
                with self.subTest(loop=loop.coro.__name__):
                    self.assertTrue(loop.is_running())
            views = {type(view).__name__ for view in bot.persistent_views}
            self.assertTrue({"ModmailPanelView", "TicketCloseView"} <= views, views)
            self.assertIn("cogs.music", bot.extensions)
            self.assertIsNotNone(bot.get_cog("Music"))
            self.assertTrue(bot.loop_lag.running)
        finally:
            for loop in registry.STARTUP_LOOPS:
                loop.cancel()
            bot.loop_lag.stop()
            if "cogs.music" in bot.extensions:
                await bot.unload_extension("cogs.music")
            if bot.http_session is not None:
                await bot.http_session.close()
                bot.http_session = None
        for loop in registry.STARTUP_LOOPS:
            self.assertFalse(loop.is_running())


if __name__ == "__main__":
    unittest.main()
