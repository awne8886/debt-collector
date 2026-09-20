import asyncio
import unittest
from unittest.mock import MagicMock

import aiohttp
from main import fetch_reaction_gif

class TestFetchReactionGif(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_reaction_gif_fallback(self):
        # Create a mock session
        session = MagicMock(spec=aiohttp.ClientSession)

        # Mock responses
        class MockResp2:
            status = 200
            async def json(self):
                # The second provider is PurrBot: lambda d: None if d.get("error") else d.get("link")
                return {"link": "http://success.gif"}

        class AsyncContextManager1:
            async def __aenter__(self):
                # Simulate timeout on first provider
                raise asyncio.TimeoutError("Timeout")
            async def __aexit__(self, exc_type, exc, tb):
                pass

        class AsyncContextManager2:
            async def __aenter__(self):
                # Simulate success on second provider
                return MockResp2()
            async def __aexit__(self, exc_type, exc, tb):
                pass

        # Use side_effect on session.get to return different context managers
        session.get.side_effect = [AsyncContextManager1(), AsyncContextManager2()]

        result = await fetch_reaction_gif(session, "hug")

        # Assert that it falls back to the second provider and returns the correct GIF
        self.assertEqual(result, "http://success.gif")

        # Assert that it tried the first provider (which failed) and then the second provider
        self.assertEqual(session.get.call_count, 2)

if __name__ == '__main__':
    unittest.main()

class TestAIRateLimitHandling(unittest.IsolatedAsyncioTestCase):
    async def test_typing_indicator_http_exception(self):
        import discord
        from unittest.mock import AsyncMock, patch

        mock_message = MagicMock(spec=discord.Message)
        mock_guild = MagicMock()
        mock_guild.id = 123
        mock_message.guild = mock_guild
        mock_message.author = MagicMock()
        mock_message.author.bot = False
        mock_message.author.id = 456
        mock_message.author.display_name = "TestUser"
        mock_message.channel = MagicMock()
        mock_message.channel.id = 789
        mock_message.content = "hello bot"
        mock_message.attachments = []
        mock_message.mention_everyone = False
        mock_message.mentions = []

        class FailingTyping:
            async def __aenter__(self):
                raise discord.HTTPException(response=MagicMock(status=429), message="Rate limited")
            async def __aexit__(self, exc_type, exc, tb):
                pass

        mock_message.channel.typing.return_value = FailingTyping()
        mock_message.reply = AsyncMock()

        with patch("main.bot") as mock_bot, \
             patch("main.ai_config", return_value={"enabled": True, "channels": ["789"], "probability": 100.0, "cooldown": 0, "daily_limit": 0, "provider_order": ["openrouter"], "models": {}}), \
             patch("main._get_ai_history", return_value=[{"role": "user", "author": "TestUser", "author_id": 456, "content": "hello bot"}]), \
             patch("main._reply_target_is_bot", return_value=(False, None)), \
             patch("main.ai_generate_reply", return_value=("Hello response", "openrouter")):

            mock_bot.user = MagicMock()
            mock_bot.user.id = 999
            mock_bot.user.display_name = "BotName"
            mock_bot.user.mentioned_in.return_value = True
            mock_bot.settings.peek_settings.return_value = {"prefix": "!"}
            mock_bot.ai_active_conversations = {}
            mock_bot.ai_next_fire = {}
            mock_bot.ai_locks = {}
            mock_bot.log_error = MagicMock()

            from main import _handle_ai
            await _handle_ai(mock_message)

            mock_bot.log_error.assert_called_with("ai:typing_ratelimit", unittest.mock.ANY)
            mock_message.reply.assert_called_once_with("Hello response", mention_author=False, allowed_mentions=unittest.mock.ANY)


class TestAITargeting(unittest.IsolatedAsyncioTestCase):
    async def test_reply_targets_trigger_message(self):
        import discord
        from unittest.mock import AsyncMock, patch

        mock_msg1 = MagicMock(spec=discord.Message)
        mock_msg1.guild = MagicMock(id=123)
        mock_msg1.author = MagicMock(bot=False, id=111, display_name="User1")
        mock_msg1.channel = MagicMock(id=789)
        mock_msg1.content = "hello bot from user 1"
        mock_msg1.attachments = []
        mock_msg1.mention_everyone = False
        mock_msg1.reply = AsyncMock()

        mock_msg2 = MagicMock(spec=discord.Message)
        mock_msg2.guild = MagicMock(id=123)
        mock_msg2.author = MagicMock(bot=False, id=222, display_name="User2")
        mock_msg2.channel = MagicMock(id=789)
        mock_msg2.content = "hello bot from user 2"
        mock_msg2.attachments = []
        mock_msg2.mention_everyone = False
        mock_msg2.reply = AsyncMock()

        class DummyTyping:
            async def __aenter__(self): pass
            async def __aexit__(self, exc_type, exc, tb): pass

        mock_msg1.channel.typing.return_value = DummyTyping()

        with patch("main.bot") as mock_bot, \
             patch("main.ai_config", return_value={"enabled": True, "channels": ["789"], "probability": 100.0, "cooldown": 0, "daily_limit": 0, "provider_order": ["openrouter"], "models": {}}), \
             patch("main._get_ai_history", return_value=[{"role": "user", "author": "User1", "author_id": 111, "content": "hello bot from user 1"}]), \
             patch("main._reply_target_is_bot", return_value=(False, None)), \
             patch("main.ai_generate_reply", return_value=("AI response to user 1", "openrouter")):

            mock_bot.user = MagicMock(id=999, display_name="BotName")
            mock_bot.user.mentioned_in.return_value = True
            mock_bot.settings.peek_settings.return_value = {"prefix": "!"}
            mock_bot.ai_active_conversations = {}
            mock_bot.ai_next_fire = {}
            mock_bot.ai_locks = {}
            mock_bot.log_error = MagicMock()

            from main import _handle_ai
            await _handle_ai(mock_msg1)

            mock_msg1.reply.assert_called_once_with("AI response to user 1", mention_author=False, allowed_mentions=unittest.mock.ANY)
            mock_msg2.reply.assert_not_called()


class TestConcurrentAIMessages(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_messages_both_replied(self):
        import discord
        from unittest.mock import AsyncMock, patch

        mock_msg1 = MagicMock(spec=discord.Message)
        mock_msg1.guild = MagicMock(id=123)
        mock_msg1.author = MagicMock(bot=False, id=111, display_name="User1")
        mock_msg1.channel = MagicMock(id=789)
        mock_msg1.content = "message 1"
        mock_msg1.attachments = []
        mock_msg1.mention_everyone = False
        mock_msg1.mentions = []
        mock_msg1.reply = AsyncMock()

        mock_msg2 = MagicMock(spec=discord.Message)
        mock_msg2.guild = MagicMock(id=123)
        mock_msg2.author = MagicMock(bot=False, id=222, display_name="User2")
        mock_msg2.channel = MagicMock(id=789)
        mock_msg2.content = "message 2"
        mock_msg2.attachments = []
        mock_msg2.mention_everyone = False
        mock_msg2.mentions = []
        mock_msg2.reply = AsyncMock()

        class DelayTyping:
            async def __aenter__(self):
                await asyncio.sleep(0.05)
            async def __aexit__(self, exc_type, exc, tb):
                pass

        mock_msg1.channel.typing.side_effect = lambda: DelayTyping()
        mock_msg2.channel.typing.side_effect = lambda: DelayTyping()

        with patch("main.bot") as mock_bot,              patch("main.ai_config", return_value={"enabled": True, "channels": ["789"], "probability": 100.0, "cooldown": 0, "daily_limit": 0, "provider_order": ["openrouter"], "models": {}}),              patch("main._get_ai_history", side_effect=lambda cid: mock_bot.ai_history_buffer.get(cid, [])),              patch("main._reply_target_is_bot", return_value=(False, None)),              patch("main.ai_generate_reply", side_effect=[("Reply 1", "openrouter"), ("Reply 2", "openrouter")]):

            mock_bot.user = MagicMock(id=999, display_name="BotName")
            mock_bot.user.mentioned_in.return_value = True
            mock_bot.settings.peek_settings.return_value = {"prefix": "!"}
            mock_bot.ai_active_conversations = {}
            mock_bot.ai_next_fire = {}
            mock_bot.ai_locks = {}
            mock_bot.ai_history_buffer = {}
            mock_bot.ai_pending = {}
            mock_bot.ai_history_dirty = set()
            mock_bot.ai_lru = {}
            mock_bot.ai_daily = {}

            from main import _handle_ai
            await asyncio.gather(_handle_ai(mock_msg1), _handle_ai(mock_msg2))

            mock_msg1.reply.assert_called_once_with("Reply 1", mention_author=False, allowed_mentions=unittest.mock.ANY)
            mock_msg2.reply.assert_called_once_with("Reply 2", mention_author=False, allowed_mentions=unittest.mock.ANY)

class TestSanitizeMassPings(unittest.TestCase):
    def test_sanitize_mass_pings(self):
        from main import sanitize_mass_pings

        # Test basic replacements
        self.assertEqual(sanitize_mass_pings("@everyone"), "@\u200beveryone")
        self.assertEqual(sanitize_mass_pings("@here"), "@\u200bhere")

        # Test strings containing pings
        self.assertEqual(sanitize_mass_pings("Hello @everyone!"), "Hello @\u200beveryone!")
        self.assertEqual(sanitize_mass_pings("Is anyone @here?"), "Is anyone @\u200bhere?")

        # Test multiple and mixed
        self.assertEqual(
            sanitize_mass_pings("@everyone and @here please read"),
            "@\u200beveryone and @\u200bhere please read"
        )
        self.assertEqual(
            sanitize_mass_pings("@everyone@everyone"),
            "@\u200beveryone@\u200beveryone"
        )

        # Test no pings
        self.assertEqual(sanitize_mass_pings("Hello world"), "Hello world")
        self.assertEqual(sanitize_mass_pings(""), "")
        self.assertEqual(sanitize_mass_pings("user@example.com"), "user@example.com")
        self.assertEqual(sanitize_mass_pings("@ everyone"), "@ everyone") # Only exact matches

class TestDeepMerge(unittest.TestCase):
    def test_deep_merge_basic(self):
        from main import _deep_merge
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = _deep_merge(base, override)
        self.assertEqual(result, {"a": 1, "b": 3, "c": 4})

    def test_deep_merge_nested(self):
        from main import _deep_merge
        base = {"a": {"x": 1, "y": 2}, "b": 2}
        override = {"a": {"y": 3, "z": 4}, "c": 5}
        result = _deep_merge(base, override)
        self.assertEqual(result, {"a": {"x": 1, "y": 3, "z": 4}, "b": 2, "c": 5})

    def test_deep_merge_type_mismatch(self):
        from main import _deep_merge
        # Override dict with scalar
        base1 = {"a": {"x": 1}}
        override1 = {"a": 2}
        result1 = _deep_merge(base1, override1)
        self.assertEqual(result1, {"a": 2})

        # Override scalar with dict
        base2 = {"a": 1}
        override2 = {"a": {"x": 2}}
        result2 = _deep_merge(base2, override2)
        self.assertEqual(result2, {"a": {"x": 2}})

    def test_deep_merge_in_place_modification(self):
        from main import _deep_merge
        base = {"a": {"x": 1}}
        override = {"a": {"y": 2}}
        result = _deep_merge(base, override)
        self.assertIs(result, base)
        self.assertEqual(base, {"a": {"x": 1, "y": 2}})

class TestHumanizeSeconds(unittest.TestCase):
    def test_humanize_seconds(self):
        from main import humanize_seconds
        cases = [
            (-5, "0s"),
            (0, "0s"),
            (45, "45s"),
            (60, "1m 0s"),
            (65, "1m 5s"),
            (3600, "1h 0m"),
            (3665, "1h 1m"),
            (86400, "1d 0h"),
            (90000, "1d 1h"),
            (100000, "1d 3h"),
        ]
        for seconds, expected in cases:
            with self.subTest(seconds=seconds):
                self.assertEqual(humanize_seconds(seconds), expected)

class TestPaginateLines(unittest.TestCase):
    def test_empty_lines(self):
        from main import paginate_lines
        self.assertEqual(paginate_lines([]), ["*Nothing to show.*"])

    def test_single_page(self):
        from main import paginate_lines
        lines = ["line 1", "line 2", "line 3"]
        self.assertEqual(paginate_lines(lines, per_page=5, char_budget=100), ["line 1\nline 2\nline 3"])

    def test_per_page_limit(self):
        from main import paginate_lines
        lines = ["1", "2", "3", "4", "5"]
        self.assertEqual(paginate_lines(lines, per_page=2, char_budget=100), ["1\n2", "3\n4", "5"])

    def test_char_budget_limit(self):
        from main import paginate_lines
        # If budget is smaller than two lines combined, they should split
        lines = ["aaaaa", "bbbbb"]
        self.assertEqual(paginate_lines(lines, per_page=5, char_budget=11), ["aaaaa", "bbbbb"])
        self.assertEqual(paginate_lines(lines, per_page=5, char_budget=12), ["aaaaa\nbbbbb"])

    def test_line_truncation(self):
        from main import paginate_lines
        # A single line exceeding char_budget should be truncated
        lines = ["a" * 20]
        self.assertEqual(paginate_lines(lines, per_page=5, char_budget=10), ["a" * 10])

class TestParseCountingNumber(unittest.TestCase):
    def test_parse_counting_number(self):
        from main import _parse_counting_number

        # Test basic numeric strings
        self.assertEqual(_parse_counting_number("123"), 123)
        self.assertEqual(_parse_counting_number("  456  "), 456)

        # Test math expressions
        self.assertEqual(_parse_counting_number("2+2"), 4)
        self.assertEqual(_parse_counting_number("10 * 5"), 50)
        self.assertEqual(_parse_counting_number("10 / 2"), 5)
        self.assertEqual(_parse_counting_number("2**3"), 8)

        # Test word to number conversions
        self.assertEqual(_parse_counting_number("twenty two"), 22)
        self.assertEqual(_parse_counting_number("one hundred"), 100)
        self.assertEqual(_parse_counting_number("zero"), 0)

        # Test invalid strings and edge cases
        self.assertIsNone(_parse_counting_number(None))
        self.assertIsNone(_parse_counting_number(""))
        self.assertIsNone(_parse_counting_number("   "))
        self.assertIsNone(_parse_counting_number("not a number"))
        self.assertIsNone(_parse_counting_number("1.5"))
        self.assertIsNone(_parse_counting_number("2.5 + 3.1"))

class TestParseIdSet(unittest.TestCase):
    def test_parse_id_set(self):
        from main import _parse_id_set

        cases = [
            (None, frozenset()),
            ("", frozenset()),
            ("   ", frozenset()),
            ("123", frozenset({123})),
            ("123, 456", frozenset({123, 456})),
            ("123 456", frozenset({123, 456})),
            ("123,abc,456", frozenset({123, 456})),
            ("123, 456, 123", frozenset({123, 456})), # Dedup
            ("abc", frozenset()),
        ]

        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(_parse_id_set(raw), expected)
