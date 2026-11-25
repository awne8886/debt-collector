import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from utils.attachment_cache import AttachmentCache


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class TestAttachmentCache(unittest.TestCase):
    def test_byte_budget_and_lru_order(self):
        cache = AttachmentCache(max_bytes=400, ttl_s=0)
        self.assertEqual(cache.per_file_limit, 100)
        for message_id in (1, 2, 3, 4):
            self.assertTrue(cache.put(message_id, f"{message_id}.png", "image/png", b"x" * 100))
        self.assertEqual(cache.bytes, 400)
        cache.get(1)  # 1 becomes the most recently used
        cache.put(5, "5.png", "image/png", b"x" * 100)
        self.assertNotIn(2, cache)  # oldest untouched entry went first
        self.assertIn(1, cache)
        self.assertLessEqual(cache.bytes, 400)
        self.assertEqual(cache.stats()["evictions"], 1)

    def test_oversize_file_is_skipped(self):
        cache = AttachmentCache(max_bytes=400, ttl_s=0)
        self.assertFalse(cache.accepts(101))
        self.assertFalse(cache.accepts(None))
        self.assertFalse(cache.put(1, "big.mp4", "video/mp4", b"x" * 101))
        self.assertEqual(len(cache), 0)
        self.assertEqual(cache.stats()["skipped"], 1)

    def test_per_file_limit_caps_at_seven_megabytes(self):
        cache = AttachmentCache(max_bytes=1024 * 1024 * 1024, ttl_s=0)
        self.assertEqual(cache.per_file_limit, 7 * 1024 * 1024)

    def test_ttl_expiry(self):
        clock = FakeClock()
        cache = AttachmentCache(max_bytes=1000, ttl_s=60, clock=clock)
        cache.put(1, "a.png", "image/png", b"abc")
        clock.now += 30
        self.assertEqual(len(cache.get(1)), 1)
        clock.now += 31
        self.assertEqual(cache.get(1), [])
        self.assertEqual(cache.bytes, 0)
        self.assertEqual(cache.stats()["expired"], 1)

    def test_pop_removes_entry(self):
        cache = AttachmentCache(max_bytes=1000, ttl_s=0)
        cache.put(7, "a.png", "image/png", b"abc")
        cache.put(7, "b.png", "image/png", b"de")
        items = cache.pop(7)
        self.assertEqual([i["filename"] for i in items], ["a.png", "b.png"])
        self.assertEqual(cache.bytes, 0)
        self.assertEqual(cache.pop(7), [])

    def test_env_configuration(self):
        with patch.dict("os.environ", {"ATTACHMENT_CACHE_MB": "8", "ATTACHMENT_CACHE_TTL_S": "60"}):
            cache = AttachmentCache()
        self.assertEqual(cache.max_bytes, 8 * 1024 * 1024)
        self.assertEqual(cache.ttl_s, 60)
        self.assertEqual(cache.per_file_limit, 2 * 1024 * 1024)
        with patch.dict("os.environ", {"ATTACHMENT_CACHE_MB": "junk"}):
            self.assertEqual(AttachmentCache().max_bytes, 24 * 1024 * 1024)


def _media(size: int, content_type: str = "image/png") -> MagicMock:
    attachment = MagicMock()
    attachment.size = size
    attachment.content_type = content_type
    attachment.filename = "file.png"
    attachment.read = AsyncMock(return_value=b"x" * size)
    return attachment


class TestAttachmentCacheWiring(unittest.IsolatedAsyncioTestCase):
    def _message(self, attachments):
        message = MagicMock()
        message.guild.id = 1
        message.channel.id = 2
        message.author.bot = False
        message.attachments = attachments
        return message

    async def test_selection_respects_config_and_size(self):
        import main

        small, huge, text = _media(10), _media(50 * 1024 * 1024), _media(10, "text/plain")
        message = self._message([small, huge, text])
        enabled = {"enabled": True, "channel_id": 9, "ignored_channels": [], "cache_attachments": True}
        with patch("main._messagelog_config", return_value=enabled):
            self.assertEqual(main._attachments_to_cache(message), [small])
        with patch("main._messagelog_config", return_value={**enabled, "enabled": False}):
            self.assertEqual(main._attachments_to_cache(message), [])
        with patch("main._messagelog_config", return_value={**enabled, "ignored_channels": ["2"]}):
            self.assertEqual(main._attachments_to_cache(message), [])
        with patch("main._messagelog_config", return_value={**enabled, "cache_attachments": False}):
            self.assertEqual(main._attachments_to_cache(message), [])

    async def test_download_goes_into_the_cache(self):
        import main

        attachment = _media(10)
        await main._cache_single_attachment(4242, attachment)
        try:
            self.assertEqual(len(main.bot.attachment_cache.get(4242)), 1)
        finally:
            main.bot.attachment_cache.pop(4242)

        oversize = _media(main.bot.attachment_cache.per_file_limit + 1)
        await main._cache_single_attachment(4243, oversize)
        oversize.read.assert_not_awaited()

    async def test_delete_pops_the_cached_entry(self):
        import main

        main.bot.attachment_cache.put(555, "a.png", "image/png", b"abc")
        payload = MagicMock()
        payload.guild_id = 1
        payload.message_id = 555
        with patch.object(main.bot, "get_guild", return_value=None):
            await main.messagelog_delete(payload)
        self.assertNotIn(555, main.bot.attachment_cache)


class TestStateJanitor(unittest.IsolatedAsyncioTestCase):
    def _target(self):
        import asyncio
        from datetime import datetime, timezone
        from types import SimpleNamespace

        from main import SnipedMessage

        now = 100_000.0

        def snipe(age):
            return SnipedMessage(
                author="a", author_avatar=None, content="x",
                created_at=datetime.fromtimestamp(now - age, timezone.utc),
                deleted_at=datetime.fromtimestamp(now - age, timezone.utc),
            )

        held = asyncio.Lock()
        target = SimpleNamespace(
            automod_recent={"fresh": [now - 5], "old": [now - 4000], "empty": []},
            automod_strikes={"old": [now - 7200, now - 3700]},
            ai_user_recent={(1, 2): [now - 10], (1, 3): [now - 3601]},
            ai_repeat={1: {"key": "k", "count": 1, "at": now - 4000}, 2: {"key": "k", "count": 1, "at": now}},
            sticky_last={1: now - 4000, 2: now - 10},
            sticky_locks={1: asyncio.Lock(), 2: asyncio.Lock(), 3: held},
            ai_daily={"5:2026-11-24": 3, "5:2026-11-25": 1},
            snipes={10: snipe(4000), 11: snipe(60)},
            next_fire={"remind_1": now + 600, "remind_2": now - 4000},
            starboard_webhooks={77: object(), 78: object()},
            is_ready=lambda: True,
            get_channel=lambda cid: object() if cid == 77 else None,
        )
        return target, held, now

    async def test_prunes_idle_entries_only(self):
        from main import prune_idle_state

        target, held, now = self._target()
        async with held:
            removed = prune_idle_state(target, now=now, today="2026-11-25")
        self.assertEqual(set(target.automod_recent), {"fresh"})
        self.assertEqual(target.automod_strikes, {})
        self.assertEqual(set(target.ai_user_recent), {(1, 2)})
        self.assertEqual(set(target.ai_repeat), {2})
        self.assertEqual(set(target.sticky_last), {2})
        # lock 1 is idle, lock 2 is recent, lock 3 was held while pruning
        self.assertEqual(set(target.sticky_locks), {2, 3})
        self.assertEqual(target.ai_daily, {"5:2026-11-25": 1})
        self.assertEqual(set(target.snipes), {11})
        self.assertEqual(set(target.next_fire), {"remind_1"})
        self.assertEqual(set(target.starboard_webhooks), {77})
        self.assertEqual(removed["ai_daily"], 1)
        self.assertEqual(removed["sticky_locks"], 1)

    async def test_extra_and_monotonic_maps(self):
        from types import SimpleNamespace

        from main import prune_idle_state

        modmail = {(1, 1): 0.0, (1, 2): 9_999.0}
        cooldowns = {("ping", "user", 1): [10.0], ("ping", "user", 2): [5_000.0]}
        prune_idle_state(
            SimpleNamespace(), now=10_000.0, today="2026-11-25",
            extra_stamped={"modmail": modmail},
            monotonic_now=5_100.0, monotonic_stamped={"cooldowns": cooldowns},
        )
        self.assertEqual(set(modmail), {(1, 2)})
        self.assertEqual(set(cooldowns), {("ping", "user", 2)})

    async def test_music_players_without_voice_are_pruned(self):
        import time as _time

        from cogs.music import MusicCog

        from types import SimpleNamespace

        cog = MusicCog.__new__(MusicCog)

        def player(locked=False, voice=None):
            return SimpleNamespace(
                voice=voice,
                _lock=SimpleNamespace(locked=lambda: locked),
                last_used=_time.monotonic() - 7200,
                shutdown=MagicMock(),
            )

        idle, busy, connected = player(), player(locked=True), player(voice=object())
        cog.players = {1: idle, 2: busy, 3: connected}
        self.assertEqual(await cog.prune_idle_players(3600), 1)
        self.assertEqual(set(cog.players), {2, 3})
        idle.shutdown.assert_called_once()


if __name__ == "__main__":
    unittest.main()
