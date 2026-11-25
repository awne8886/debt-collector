"""Regression test: async code in main.py must not make blocking MongoDB calls.

A synchronous pymongo call on the event loop freezes the whole bot (gateway
heartbeats, music, every command) for as long as Atlas takes to answer. Async
code must use the async helpers (fetch_settings / push_settings / push_fields)
or wrap the call in asyncio.to_thread(...).
"""

import ast
import os
import unittest
from typing import List, Set

MAIN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py")

BLOCKING_SETTINGS_METHODS = {
    "get_settings",
    "update_settings",
    "update_fields",
    "get_meta",
    "set_meta",
}
BLOCKING_COLLECTION_METHODS = {
    "find",
    "find_one",
    "update_one",
    "update_many",
    "insert_one",
    "delete_one",
    "delete_many",
    "count_documents",
    "aggregate",
    "bulk_write",
}


def _is_to_thread(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "to_thread"
        and isinstance(func.value, ast.Name)
        and func.value.id == "asyncio"
    )


def _is_settings_collection_call(func: ast.Attribute) -> bool:
    # <bot|self>.settings.<collection>.<method>(...)
    collection = func.value
    if not isinstance(collection, ast.Attribute):
        return False
    settings = collection.value
    return (
        isinstance(settings, ast.Attribute)
        and settings.attr == "settings"
        and isinstance(settings.value, ast.Name)
        and settings.value.id in ("bot", "self")
    )


def blocking_calls(source: str) -> List[str]:
    tree = ast.parse(source)
    found: List[str] = []
    seen: Set[int] = set()

    def visit(node: ast.AST, function: str) -> None:
        if isinstance(node, ast.Call) and _is_to_thread(node):
            return  # everything handed to a worker thread is fine
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = node.func.attr
            if (name in BLOCKING_SETTINGS_METHODS) or (
                name in BLOCKING_COLLECTION_METHODS
                and _is_settings_collection_call(node.func)
            ):
                key = id(node)
                if key not in seen:
                    seen.add(key)
                    found.append(f"main.py:{node.lineno} {function}() calls .{name}(")
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            for statement in node.body:
                visit(statement, node.name)
    return found


class TestNoBlockingDatabaseCalls(unittest.TestCase):
    def test_async_code_does_not_call_sync_mongo(self):
        with open(MAIN, encoding="utf-8") as handle:
            offenders = blocking_calls(handle.read())
        self.assertEqual(offenders, [], "\n".join(offenders))

    def test_detector_catches_and_allows_the_right_things(self):
        sample = '''
async def bad():
    bot.settings.get_settings(1)
    bot.settings.cases.find_one({})
    self.settings.update_fields(1, {})

async def good():
    await asyncio.to_thread(bot.settings.get_settings, 1)
    await asyncio.to_thread(lambda: list(bot.settings.cases.find({})))
    bot.settings.peek_settings(1)
    await bot.settings.fetch_settings(1)

def sync_is_fine():
    bot.settings.get_settings(1)
'''
        offenders = blocking_calls(sample)
        self.assertEqual(len(offenders), 3, offenders)
        self.assertTrue(all("bad()" in line for line in offenders))


class TestMongoClientOptions(unittest.TestCase):
    def test_defaults(self):
        from main import mongo_client_options

        options = mongo_client_options("mongodb+srv://user:pw@cluster.example.net/db")
        self.assertEqual(options["timeoutMS"], 8000)
        self.assertEqual(options["connectTimeoutMS"], 5000)
        self.assertEqual(options["serverSelectionTimeoutMS"], 5000)

    def test_uri_options_win(self):
        from main import mongo_client_options

        options = mongo_client_options(
            "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200&TIMEOUTMS=100"
        )
        self.assertNotIn("serverSelectionTimeoutMS", options)
        self.assertNotIn("timeoutMS", options)
        self.assertEqual(options["connectTimeoutMS"], 5000)

    def test_live_client_uses_them(self):
        import main

        client_options = main.settings_manager._client.options
        self.assertEqual(client_options.timeout, 8.0)
        self.assertEqual(client_options.pool_options.connect_timeout, 5.0)


class TestAutopurgeWritesExactPaths(unittest.IsolatedAsyncioTestCase):
    async def test_autopurge_on_writes_one_dotted_field(self):
        from unittest.mock import AsyncMock, MagicMock, patch

        import main

        bot = MagicMock()
        bot.settings.fetch_settings = AsyncMock(
            return_value={"autopurge": {"channels": {}, "exempt_roles": []}}
        )
        bot.settings.push_fields = AsyncMock(return_value=True)
        bot.settings.push_settings = AsyncMock(return_value=True)
        ctx = MagicMock()
        ctx.send = AsyncMock()
        ctx.guild.id = 1
        channel = MagicMock()
        channel.id = 55
        with patch("main.bot", bot), patch("main.member_has_perms", return_value=True):
            await main.autopurge_on.callback(ctx, channel, None, None)
            await main.autopurge_off.callback(ctx, channel)
        bot.settings.push_settings.assert_not_awaited()
        calls = [c.args[1] for c in bot.settings.push_fields.await_args_list]
        self.assertEqual(calls[0], {"autopurge.channels.55": {"until": None}})
        self.assertEqual(calls[1], {"autopurge.channels": {}})


if __name__ == "__main__":
    unittest.main()
