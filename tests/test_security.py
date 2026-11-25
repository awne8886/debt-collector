import os
import sys
import time
import unittest
from unittest.mock import Mock, patch


class TestSafeCountingParser(unittest.TestCase):
    def test_existing_calculator_syntax_still_works(self):
        from main import _parse_counting_number

        self.assertEqual(_parse_counting_number("2^3"), 8)
        self.assertEqual(_parse_counting_number("sqrt(16)"), 4)
        self.assertEqual(_parse_counting_number("7//2"), 3)
        self.assertEqual(_parse_counting_number("1e2"), 100)
        self.assertEqual(_parse_counting_number("0x10"), 16)
        self.assertEqual(_parse_counting_number("abs(-5)"), 5)
        self.assertEqual(_parse_counting_number("-3+4"), 1)
        self.assertEqual(_parse_counting_number("SQRT(81)"), 9)
        self.assertEqual(_parse_counting_number("7 % 4"), 3)
        self.assertIsNone(_parse_counting_number("sqrt(2)"))
        self.assertIsNone(_parse_counting_number("sqrt(-4)"))

    def test_code_is_never_run(self):
        from main import _parse_counting_number

        hostile = [
            "__import__('os').getpid()",
            "(lambda: 7)()",
            "().__class__",
            "getattr(1,'real')",
            "[x for x in (1,)][0]",
            "sqrt(x=4)",
            "abs(1, 2)",
            "1 if True else 2",
            "f'{1}'",
            "1 < 2",
            "True + 1",
            "'1'",
        ]
        getpid = Mock(return_value=4242)
        with patch.object(os, "getpid", getpid):
            for text in hostile:
                with self.subTest(text=text):
                    self.assertIsNone(_parse_counting_number(text))
        getpid.assert_not_called()

    def test_expensive_inputs_are_refused_quickly(self):
        from main import _parse_counting_number

        for text in ("9**9**9", "10**100", "2**64**2", "1/0", "5//0", "5%0", "1e400"):
            with self.subTest(text=text):
                started = time.perf_counter()
                self.assertIsNone(_parse_counting_number(text))
                self.assertLess(time.perf_counter() - started, 0.1)

    def test_guardrails(self):
        from utils.safe_math import safe_eval

        self.assertEqual(safe_eval("2**64"), None)  # > 10**15
        self.assertEqual(safe_eval("1000000**2"), 10**12)
        self.assertIsNone(safe_eval("1000001**2"))  # base too large
        self.assertIsNone(safe_eval("2**-1"))  # negative exponent
        self.assertIsNone(safe_eval("2**0.5"))  # non-integer exponent
        self.assertIsNone(safe_eval("1000000.0**64"))  # float overflow is caught
        self.assertIsNone(safe_eval("1" + "+1" * 30))  # too many nodes
        self.assertIsNone(safe_eval("1" * 101))
        self.assertIsNone(safe_eval(None))  # type: ignore[arg-type]

    def test_thousand_hostile_inputs_under_a_second(self):
        from main import _extract_and_parse_number, _parse_counting_number

        samples = [
            "9**9**9",
            "__import__('os').system('true')",
            "2**64**2",
            "10**100",
            "(" * 60 + "1" + ")" * 39,
            "sqrt(" * 12 + "4" + ")" * 12,
            "1/0",
            "-" * 99 + "1",
            "lambda: 1",
            "word " * 40,
        ]
        started = time.perf_counter()
        for i in range(1000):
            text = samples[i % len(samples)]
            _parse_counting_number(text)
        _extract_and_parse_number("9**9**9 " * 50)
        self.assertLess(time.perf_counter() - started, 1.0)

    def test_extract_only_tries_first_twelve_words(self):
        from main import _extract_and_parse_number

        with patch("main._parse_counting_number", return_value=None) as parser:
            _extract_and_parse_number(" ".join(["x"] * 50))
        self.assertEqual(parser.call_count, 12)
        self.assertEqual(_extract_and_parse_number("5 is the number"), 5)
        self.assertIsNone(_extract_and_parse_number(""))

    def test_sympy_is_not_imported(self):
        import main  # noqa: F401

        self.assertNotIn("sympy", sys.modules)


class TestSuperuserIds(unittest.TestCase):
    def test_env_set_is_used_exactly(self):
        from main import resolve_superuser_ids

        with self.assertNoLogs("debt-collector", level="WARNING"):
            result = resolve_superuser_ids("111, 222")
        self.assertEqual(result, frozenset({111, 222}))
        self.assertNotIn(1120393965485703219, result)

    def test_unset_falls_back_with_one_warning(self):
        from main import DEFAULT_SUPERUSER_IDS, resolve_superuser_ids

        for raw in (None, "", "   "):
            with self.subTest(raw=raw):
                with self.assertLogs("debt-collector", level="WARNING") as logs:
                    result = resolve_superuser_ids(raw)
                self.assertEqual(len(logs.records), 1)
                self.assertEqual(result, DEFAULT_SUPERUSER_IDS)
        self.assertEqual(
            DEFAULT_SUPERUSER_IDS,
            frozenset({1120393965485703219, 600689350686146562, 760531428881465366}),
        )

    def test_only_invalid_entries_fall_back(self):
        from main import DEFAULT_SUPERUSER_IDS, resolve_superuser_ids

        with self.assertLogs("debt-collector", level="WARNING"):
            self.assertEqual(resolve_superuser_ids("abc"), DEFAULT_SUPERUSER_IDS)


class TestPaginatorSuperuserLookup(unittest.IsolatedAsyncioTestCase):
    async def test_superuser_found_when_bot_runs_as_main_script(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, MagicMock

        import discord

        from utils.pagination import Paginator

        paginator = Paginator([discord.Embed(title="a"), discord.Embed(title="b")], author_id=1)
        interaction = MagicMock()
        interaction.user.id = 99
        interaction.response.send_message = AsyncMock()
        fake_main = SimpleNamespace(is_superuser=lambda user: user.id == 99)
        modules = {k: v for k, v in sys.modules.items() if k != "main"}
        modules["__main__"] = fake_main
        with patch.dict(sys.modules, modules, clear=True):
            self.assertTrue(await paginator.interaction_check(interaction))
        interaction.user.id = 100
        with patch.dict(sys.modules, modules, clear=True):
            self.assertFalse(await paginator.interaction_check(interaction))


class TestCloneCommandsCanRun(unittest.TestCase):
    def test_asyncio_is_not_shadowed_by_a_local_import(self):
        # A late `import asyncio` inside the function made the first
        # asyncio.to_thread call raise UnboundLocalError (rollback_clone always failed).
        import main

        for command in (main.clone_server, main.rollback_clone):
            with self.subTest(command=command.name):
                self.assertNotIn("asyncio", command.callback.__code__.co_varnames)


if __name__ == "__main__":
    unittest.main()
