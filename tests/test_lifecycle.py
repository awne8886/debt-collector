import asyncio
import copy
import json
import math
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import discord


def _fake_bot(*, ready=True, latency=0.05, start_time=0.0, closed=False):
    settings = MagicMock()
    settings.cache_stats.return_value = {"entries": 3, "hits": 9, "misses": 1}
    # /health must never touch the database.
    settings.get_settings.side_effect = AssertionError("database touched")
    settings.ping.side_effect = AssertionError("database touched")
    from utils.attachment_cache import AttachmentCache
    from utils.runtime_stats import LoopLagMonitor

    lag = LoopLagMonitor()
    lag.record(0.004)
    return SimpleNamespace(
        is_ready=lambda: ready,
        is_closed=lambda: closed,
        latency=latency,
        start_time=start_time,
        guilds=[object(), object()],
        settings=settings,
        attachment_cache=AttachmentCache(max_bytes=1000, ttl_s=0),
        ai_history_buffer={1: [{}, {}], 2: [{}]},
        ai_pending={1: [{}]},
        snipes={5: object()},
        _background_tasks=set(),
        voice_clients=[],
        loop_lag=lag,
    )


class TestRuntimeStats(unittest.TestCase):
    def test_rss_reads_proc_and_falls_back(self):
        from utils import runtime_stats

        rss = runtime_stats.rss_bytes()
        self.assertIsInstance(rss, int)
        self.assertGreater(rss, 1024 * 1024)
        with patch("builtins.open", side_effect=OSError("no /proc")):
            self.assertEqual(runtime_stats.rss_bytes(), runtime_stats.peak_rss_bytes())

    def test_collect_runtime_stats(self):
        from core.stats import _format_memory_line, collect_runtime_stats

        stats = collect_runtime_stats(_fake_bot())
        self.assertEqual(stats["guilds"], 2)
        self.assertEqual(stats["settings_cache_entries"], 3)
        self.assertEqual(stats["ai_buffer_messages"], 3)
        self.assertEqual(stats["ai_pending_messages"], 1)
        self.assertEqual(stats["snipes"], 1)
        self.assertEqual(stats["attachment_cache"]["bytes"], 0)
        line = _format_memory_line(stats)
        for key in ("rss_mb=", "guilds=2", "settings_cache=3", "attachment_cache_bytes=0",
                    "ai_buffer_messages=3", "snipes=1"):
            self.assertIn(key, line)

    def test_loop_lag_stats(self):
        from utils.runtime_stats import LoopLagMonitor

        monitor = LoopLagMonitor(interval=2.0)
        self.assertIsNone(monitor.stats()["max_ms"])
        for lag in (0.001, 0.002, 1.5):
            monitor.record(lag)
        monitor.record(-0.5)  # clock noise never goes negative
        stats = monitor.stats()
        self.assertEqual(stats["max_ms"], 1500.0)
        self.assertEqual(stats["last_ms"], 0.0)
        self.assertEqual(stats["window_s"], 8.0)


class TestHealth(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from core.lifecycle import SHUTDOWN

        SHUTDOWN.reset()

    tearDown = setUp

    def test_ready_is_ok(self):
        from core.web import build_health_payload

        code, body = build_health_payload(_fake_bot(), now=1000.0)
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["ready"])
        self.assertEqual(body["latency_ms"], 50.0)
        self.assertEqual(body["guilds"], 2)
        self.assertEqual(body["uptime_s"], 1000)
        self.assertIsInstance(body["rss_mb"], float)
        json.dumps(body)  # serialisable

    def test_startup_grace_then_503(self):
        from core.web import build_health_payload

        bot = _fake_bot(ready=False, latency=float("inf"))
        code, body = build_health_payload(bot, now=60.0, preview=True)
        self.assertEqual((code, body["status"]), (200, "starting"))
        self.assertIsNone(body["latency_ms"])
        self.assertTrue(body["preview"])
        code, body = build_health_payload(bot, now=181.0, preview=True)
        self.assertEqual((code, body["status"]), (503, "unavailable"))

    def test_infinite_latency_after_grace_is_503(self):
        from core.web import build_health_payload

        code, _ = build_health_payload(_fake_bot(latency=math.inf), now=10_000.0)
        self.assertEqual(code, 503)
        code, _ = build_health_payload(_fake_bot(closed=True), now=10_000.0)
        self.assertEqual(code, 503)

    def test_shutting_down_is_503(self):
        from core.lifecycle import SHUTDOWN
        from core.web import build_health_payload

        SHUTDOWN.request("SIGTERM")
        code, body = build_health_payload(_fake_bot(), now=10.0)
        self.assertEqual((code, body["status"]), (503, "shutting_down"))

    async def test_handler_returns_json(self):
        import core.web as web

        with patch("core.web.bot", _fake_bot(ready=False, start_time=0.0)), \
             patch("core.web.time.time", return_value=10_000.0):
            response = await web._health_handler(MagicMock())
        self.assertEqual(response.status, 503)
        self.assertEqual(json.loads(response.text)["status"], "unavailable")

    async def test_server_root_always_ok_and_no_access_log(self):
        import core.web as web

        with patch.dict(os.environ, {"PORT": "0"}), patch("core.web.bot", _fake_bot(ready=False)), \
             patch("core.web.time.time", return_value=10_000.0):
            await web._start_keepalive_server()
            try:
                port = web._KEEPALIVE_RUNNER.addresses[0][1]
                base = f"http://127.0.0.1:{port}"
                with self.assertNoLogs("aiohttp.access", level="DEBUG"):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(base + "/") as resp:
                            self.assertEqual(resp.status, 200)
                        async with session.head(base + "/") as resp:
                            self.assertEqual(resp.status, 200)
                        async with session.get(base + "/health") as resp:
                            self.assertEqual(resp.status, 503)
                            self.assertFalse((await resp.json())["ready"])
            finally:
                await web._stop_keepalive_server()


class TestShutdown(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from core.lifecycle import SHUTDOWN

        SHUTDOWN.reset()

    tearDown = setUp

    async def test_signal_sets_flag_and_closes_once(self):
        from core.lifecycle import SHUTDOWN, handle_shutdown_signal

        close = AsyncMock()
        with self.assertLogs("debt-collector", level="INFO") as logs:
            task = handle_shutdown_signal("SIGTERM", close)
        self.assertTrue(SHUTDOWN.requested)
        self.assertIn("Shutting down (SIGTERM)", "\n".join(logs.output))
        await task
        close.assert_awaited_once()
        self.assertIsNone(handle_shutdown_signal("SIGINT", close))
        close.assert_awaited_once()

    async def test_close_is_time_boxed(self):
        from core.lifecycle import _close_within

        async def slow():
            await asyncio.sleep(10)

        with self.assertLogs("debt-collector", level="WARNING"):
            await _close_within(slow, budget=0.05)

    async def test_install_signal_handlers(self):
        from core.lifecycle import install_signal_handlers

        loop = MagicMock()
        self.assertEqual(install_signal_handlers(loop, AsyncMock()), ["SIGTERM", "SIGINT"])
        loop.add_signal_handler.side_effect = NotImplementedError
        self.assertEqual(install_signal_handlers(loop, AsyncMock()), [])

    async def test_supervisor_does_not_start_after_shutdown(self):
        from core.lifecycle import SHUTDOWN, supervise

        SHUTDOWN.request("SIGTERM")
        start = AsyncMock()
        self.assertEqual(await supervise(start), "shutdown")
        start.assert_not_awaited()

    async def test_supervisor_stops_reconnecting_when_signalled_during_backoff(self):
        from core.lifecycle import SHUTDOWN, supervise

        start = AsyncMock(side_effect=OSError("network down"))
        loop = asyncio.get_running_loop()
        loop.call_later(0.05, SHUTDOWN.request, "SIGTERM")
        with patch("core.lifecycle.random.uniform", return_value=0.0):
            result = await asyncio.wait_for(supervise(start, base_backoff=30.0), 2.0)
        self.assertEqual(result, "shutdown")
        self.assertEqual(start.await_count, 1)

    async def test_supervisor_retries_then_exits_on_clean_close(self):
        from core.lifecycle import supervise

        start = AsyncMock(side_effect=[OSError("blip"), None])
        with patch("core.lifecycle.random.uniform", return_value=0.0):
            result = await supervise(start, base_backoff=0.01)
        self.assertEqual(result, "clean")
        self.assertEqual(start.await_count, 2)

    async def test_supervisor_exits_when_start_returns_during_shutdown(self):
        from core.lifecycle import SHUTDOWN, supervise

        async def start():
            SHUTDOWN.request("SIGTERM")

        self.assertEqual(await supervise(start), "shutdown")

    async def test_supervisor_exits_if_client_closed(self):
        from core.lifecycle import supervise

        start = AsyncMock(side_effect=OSError("gone"))
        self.assertEqual(await supervise(start, is_closed=lambda: True), "closed")

    async def test_bot_close_runs_steps_in_order(self):
        import core.bot
        from discord.ext import commands

        order = []
        bot = core.bot.DebtCollectorBot(MagicMock())
        bot.settings.close = MagicMock(side_effect=lambda: order.append("mongo"))

        async def record(name):
            order.append(name)

        with patch("core.bot.ai_flush", AsyncMock(side_effect=lambda: order.append("flush"))), \
             patch.object(bot, "_leave_voice", AsyncMock(side_effect=lambda: order.append("voice"))), \
             patch.object(bot, "_close_http_session", AsyncMock(side_effect=lambda: order.append("http"))), \
             patch.object(core.bot.DebtCollectorBot, "_stop_warp", staticmethod(lambda: order.append("warp"))), \
             patch.object(commands.Bot, "close", AsyncMock(side_effect=lambda: order.append("gateway"))):
            await bot.close()
            await bot.close()  # a second close does not repeat the teardown
        self.assertEqual(order, ["flush", "voice", "http", "mongo", "warp", "gateway", "gateway"])

    async def test_bot_close_survives_a_failing_step(self):
        import core.bot
        from discord.ext import commands

        bot = core.bot.DebtCollectorBot(MagicMock())
        bot.settings.close = MagicMock(side_effect=RuntimeError("boom"))
        with patch("core.bot.ai_flush", AsyncMock(side_effect=RuntimeError("flush failed"))), \
             patch.object(core.bot.DebtCollectorBot, "_stop_warp", staticmethod(lambda: None)), \
             patch.object(commands.Bot, "close", AsyncMock()) as gateway_close:
            with self.assertLogs("debt-collector", level="WARNING"):
                await bot.close()
        gateway_close.assert_awaited_once()

    async def test_leave_voice_unloads_music_and_disconnects(self):
        import core.bot

        bot = core.bot.DebtCollectorBot(MagicMock())
        voice = MagicMock()
        voice.disconnect = AsyncMock()
        with patch.object(type(bot), "extensions", new={"cogs.music": object()}), \
             patch.object(bot, "unload_extension", AsyncMock()) as unload, \
             patch.object(type(bot), "voice_clients", new=[voice]):
            await bot._leave_voice()
        unload.assert_awaited_once_with("cogs.music")
        voice.disconnect.assert_awaited_once_with(force=True)


class TestDiagnose(unittest.TestCase):
    def test_embed_has_the_new_sections(self):
        import main as info

        bot = _fake_bot()
        bot.errors = MagicMock()
        bot.errors.stats.return_value = {"unique": 0, "total": 0, "by_type": []}
        bot.shard_count = None
        bot.users = []
        bot.get_cog = lambda name: None
        guild = MagicMock()
        guild.me.guild_permissions = discord.Permissions.all()
        from core.config import DEFAULT_SETTINGS

        settings = copy.deepcopy(DEFAULT_SETTINGS)
        settings["counting"]["channel_id"] = 123
        embed = info.build_diagnose_embed(
            bot, guild, db_ok=True, db_ms=12.0, guild_settings=settings
        )
        names = [field.name for field in embed.fields]
        for name in ("Gateway", "Database", "Errors", "Memory", "Runtime", "Music",
                     "Background loops", "This server", "Permissions in this server"):
            self.assertIn(name, names)
        self.assertLessEqual(len(embed), 6000)
        for field in embed.fields:
            self.assertLessEqual(len(field.value), 1024)
        server = next(f for f in embed.fields if f.name == "This server").value
        self.assertIn("<#123>", server)
        memory = next(f for f in embed.fields if f.name == "Memory").value
        self.assertIn("RSS", memory)
        self.assertIn("Attachment cache", memory)

    def test_database_down_is_red(self):
        import main as info

        bot = _fake_bot()
        bot.errors = MagicMock()
        bot.errors.stats.return_value = {"unique": 0, "total": 0, "by_type": []}
        bot.shard_count = 1
        bot.users = []
        bot.get_cog = lambda name: None
        guild = MagicMock()
        guild.me.guild_permissions = discord.Permissions.none()
        embed = info.build_diagnose_embed(bot, guild, db_ok=False, db_ms=5000.0)
        self.assertEqual(embed.color, discord.Color.red())
        self.assertIn("database unreachable", embed.description.lower())


if __name__ == "__main__":
    unittest.main()
