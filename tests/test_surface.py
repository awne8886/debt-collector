"""Snapshot of the bot's whole command surface, taken from a fresh import of main.

The fixture (tests/fixtures/command_surface.json) was generated from the unsplit
main.py. If this test fails, a refactor changed what users or Discord see: a
command, option, description, permission, check, cooldown, listener, loop or view.

Regenerate (only for an intentional change):

    MONGO_URI="mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200" \
        python tests/test_surface.py --write
"""

import json
import os
import subprocess
import sys
import unittest
from typing import Any, Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "command_surface.json")
DUMMY_MONGO_URI = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200"


def _lookup(name: str) -> Any:
    """Find a name in main or in any core.* / features.* module (layout-agnostic)."""
    candidates = ["main"] + sorted(
        m for m in sys.modules if m.startswith(("core.", "features."))
    )
    for module_name in candidates:
        module = sys.modules.get(module_name)
        if module is not None and hasattr(module, name):
            return getattr(module, name)
    raise LookupError(name)


def _app_command(command: Any) -> Dict[str, Any]:
    from discord import app_commands

    perms = getattr(command, "default_permissions", None)
    entry: Dict[str, Any] = {
        "qualified_name": command.qualified_name,
        "group": isinstance(command, app_commands.Group),
        "description": command.description,
        "default_permissions": perms.value if perms is not None else None,
        "guild_only": getattr(command, "guild_only", False),
        "nsfw": getattr(command, "nsfw", False),
        "checks": sorted(c.__qualname__ for c in getattr(command, "checks", [])),
    }
    if not entry["group"]:
        entry["parameters"] = [
            {
                "name": p.name,
                "type": getattr(p.type, "name", str(p.type)),
                "required": p.required,
                "description": p.description,
                "choices": [[c.name, c.value] for c in (p.choices or [])],
                "min_value": p.min_value,
                "max_value": p.max_value,
                "channel_types": [t.name for t in (p.channel_types or [])],
                "autocomplete": bool(p.autocomplete),
            }
            for p in command.parameters
        ]
    return entry


def _prefix_command(command: Any) -> Dict[str, Any]:
    from discord.ext import commands

    cooldown = command.cooldown
    buckets = getattr(command, "_buckets", None)
    return {
        "qualified_name": command.qualified_name,
        "kind": type(command).__name__,
        "cog": command.cog_name,
        "aliases": list(command.aliases),
        "hidden": command.hidden,
        "enabled": command.enabled,
        "description": command.description,
        "brief": command.brief,
        "help": command.help,
        "usage": command.usage,
        "signature": command.signature,
        "invoke_without_command": getattr(command, "invoke_without_command", None),
        "checks": sorted(c.__qualname__ for c in command.checks),
        "cooldown": (
            [cooldown.rate, cooldown.per, buckets.type.name]
            if cooldown is not None and buckets is not None
            else None
        ),
        "is_group": isinstance(command, commands.Group),
    }


async def _run_setup_hook(bot: Any) -> Dict[str, Any]:
    """Run the real setup_hook with the network and the loops stubbed out."""
    from unittest.mock import AsyncMock, patch

    from discord.ext import tasks

    started: List[str] = []
    views: List[str] = []
    dynamic: List[str] = []
    meta: Dict[str, Any] = {}
    logged: List[str] = []

    def fake_start(loop: Any, *args: Any, **kwargs: Any) -> None:
        started.append(loop.coro.__name__)

    bot.settings.ensure_indexes = lambda: None
    bot.settings.get_meta = lambda key: meta.get(key)
    bot.settings.set_meta = lambda key, value: meta.__setitem__(key, value)
    bot.tree.sync = AsyncMock(return_value=[])
    bot.add_view = lambda view, **kwargs: views.append(type(view).__name__)
    bot.add_dynamic_items = lambda *items: dynamic.extend(i.__name__ for i in items)
    bot.loop_lag.start = lambda: None

    import logging

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            logged.append(record.getMessage())

    handler = Capture(level=logging.INFO)
    logging.getLogger("debt-collector").addHandler(handler)
    try:
        with patch.object(tasks.Loop, "start", fake_start):
            await bot.setup_hook()
            first_sync = bot.tree.sync.await_count
            # A second pass with the stored digest must skip the sync.
            await bot._sync_commands_if_changed()
        second_sync = bot.tree.sync.await_count - first_sync
    finally:
        logging.getLogger("debt-collector").removeHandler(handler)
        if bot.http_session is not None:
            await bot.http_session.close()

    digest = meta.get("command_hash")
    try:
        signature_fn = _lookup("command_signature")
    except LookupError:
        signature_fn = None
    return {
        "digest": digest,
        "pure_digest": signature_fn(bot.tree) if signature_fn else digest,
        "first_sync_calls": first_sync,
        "second_sync_calls": second_sync,
        "skip_logged": "Command signature unchanged — skipping sync." in logged,
        "loops": started,
        "views": views,
        "dynamic_items": dynamic,
        "extensions": sorted(bot.extensions),
    }


def build_snapshot() -> Dict[str, Any]:
    import asyncio

    sys.path.insert(0, ROOT)
    import main  # noqa: F401  # the fresh import under test

    bot = _lookup("bot")
    hook = asyncio.run(_run_setup_hook(bot))

    tree = bot.tree
    app_commands_list = sorted(
        (_app_command(c) for c in tree.walk_commands()),
        key=lambda e: e["qualified_name"],
    )
    payloads = sorted(
        (c.to_dict(tree) for c in tree.get_commands()), key=lambda d: d["name"]
    )
    context_menus = sorted(
        c.name for c in tree.get_commands() if not hasattr(c, "walk_commands")
        and type(c).__name__ == "ContextMenu"
    )
    prefix = sorted(
        (_prefix_command(c) for c in bot.walk_commands()),
        key=lambda e: e["qualified_name"],
    )
    events = {
        name: sorted(fn.__name__ for fn in listeners)
        for name, listeners in sorted(bot.extra_events.items())
    }
    overrides = sorted(k for k in vars(bot) if k.startswith("on_"))
    tree_error = vars(tree).get("on_error")
    after_invoke = getattr(bot, "_after_invoke", None)
    before_invoke = getattr(bot, "_before_invoke", None)

    return {
        "command_signature": hook["pure_digest"],
        "setup_hook": {
            "sync_digest_matches_signature": hook["digest"] == hook["pure_digest"],
            "first_sync_calls": hook["first_sync_calls"],
            "second_sync_calls": hook["second_sync_calls"],
            "skip_logged": hook["skip_logged"],
            "loops": hook["loops"],
            "views": hook["views"],
            "dynamic_items": hook["dynamic_items"],
            "extensions": hook["extensions"],
        },
        "app_commands": app_commands_list,
        "app_payloads": payloads,
        "context_menus": context_menus,
        "prefix_commands": prefix,
        "listeners": events,
        "event_overrides": overrides,
        "global_checks": [c.__name__ for c in bot._checks],
        "global_check_once": [c.__name__ for c in bot._check_once],
        "after_invoke": after_invoke.__name__ if after_invoke else None,
        "before_invoke": before_invoke.__name__ if before_invoke else None,
        "tree_error_handler": tree_error.__name__ if tree_error else None,
        "uncategorised_roots": _lookup("_uncategorised_roots")(),
        "command_category": dict(sorted(_lookup("COMMAND_CATEGORY").items())),
    }


def _snapshot_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.setdefault("MONGO_URI", DUMMY_MONGO_URI)
    env.pop("SYNC_COMMANDS", None)
    env["MUSIC_WARMUP"] = "0"  # no YouTube warm-up thread
    env["YTDL_WARP"] = "off"  # never start WARP
    env["PYTHONHASHSEED"] = "0"
    return env


def fresh_snapshot() -> Dict[str, Any]:
    """Build the snapshot in a fresh interpreter, isolated from other tests."""
    result = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--dump"],
        cwd=ROOT,
        env=_snapshot_env(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"snapshot subprocess failed ({result.returncode}):\n{result.stderr[-4000:]}"
        )
    return json.loads(result.stdout)


class TestCommandSurface(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        with open(FIXTURE, encoding="utf-8") as handle:
            cls.expected = json.load(handle)
        cls.actual = fresh_snapshot()

    def test_signature_digest_unchanged(self) -> None:
        self.assertEqual(self.actual["command_signature"], self.expected["command_signature"])

    def test_setup_hook_unchanged(self) -> None:
        self.assertEqual(self.actual["setup_hook"], self.expected["setup_hook"])
        self.assertTrue(self.actual["setup_hook"]["sync_digest_matches_signature"])
        self.assertTrue(self.actual["setup_hook"]["skip_logged"])
        self.assertEqual(self.actual["setup_hook"]["second_sync_calls"], 0)

    def test_every_section_unchanged(self) -> None:
        self.assertEqual(sorted(self.actual), sorted(self.expected))
        for key in self.expected:
            with self.subTest(section=key):
                self.assertEqual(self.actual[key], self.expected[key])


if __name__ == "__main__":
    if "--dump" in sys.argv:
        snapshot = build_snapshot()
        sys.stdout.write(json.dumps(snapshot, indent=1, sort_keys=False, default=str))
    elif "--write" in sys.argv:
        os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
        data = fresh_snapshot()
        with open(FIXTURE, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=1, ensure_ascii=False)
            handle.write("\n")
        print(f"wrote {FIXTURE}")
    else:
        unittest.main()
