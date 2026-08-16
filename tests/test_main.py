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
