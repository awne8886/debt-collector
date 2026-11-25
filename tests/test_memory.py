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


if __name__ == "__main__":
    unittest.main()
