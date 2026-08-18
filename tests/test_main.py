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
            mock_bot.settings.get_settings.return_value = {"prefix": "!"}
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
            mock_bot.settings.get_settings.return_value = {"prefix": "!"}
            mock_bot.ai_active_conversations = {}
            mock_bot.ai_next_fire = {}
            mock_bot.ai_locks = {}
            mock_bot.log_error = MagicMock()

            from main import _handle_ai
            await _handle_ai(mock_msg1)

            mock_msg1.reply.assert_called_once_with("AI response to user 1", mention_author=False, allowed_mentions=unittest.mock.ANY)
            mock_msg2.reply.assert_not_called()
