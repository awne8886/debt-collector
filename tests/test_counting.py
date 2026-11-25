import asyncio
import copy
import threading
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord


class FakeCollection:
    """A tiny thread-safe stand-in for a pymongo collection (find_one / update_one)."""

    def __init__(self, delay: float = 0.0):
        self.docs = []
        self.delay = delay
        self._lock = threading.Lock()

    @staticmethod
    def _matches(doc, flt):
        return all(doc.get(k) == v for k, v in flt.items())

    def find_one(self, flt):
        time.sleep(self.delay)  # widen the race window in worker threads
        with self._lock:
            for doc in self.docs:
                if self._matches(doc, flt):
                    return copy.deepcopy(doc)
        return None

    def update_one(self, flt, update, upsert=False):
        time.sleep(self.delay)
        with self._lock:
            doc = next((d for d in self.docs if self._matches(d, flt)), None)
            inserted = doc is None
            if inserted:
                if not upsert:
                    return
                doc = dict(flt)
                self.docs.append(doc)
            for key, value in (update.get("$set") or {}).items():
                doc[key] = copy.deepcopy(value)
            for key, value in (update.get("$inc") or {}).items():
                doc[key] = doc.get(key, 0) + value
            if inserted:
                for key, value in (update.get("$setOnInsert") or {}).items():
                    doc[key] = value


def make_bot(saves=3, channel_id=50, delay=0.0):
    bot = MagicMock()
    settings = {"counting": {"channel_id": channel_id, "default_saves": saves, "ruiner_role_id": None}}
    bot.settings.peek_settings.return_value = settings
    bot.settings.fetch_settings = AsyncMock(return_value=settings)
    bot.settings.counting = FakeCollection(delay)
    bot.settings.counting_saves = FakeCollection(delay)
    return bot


def make_message(content, author_id, guild_id=1, channel_id=50):
    message = MagicMock()
    message.content = content
    message.author = MagicMock(spec=discord.Member)
    message.author.bot = False
    message.author.id = author_id
    message.author.mention = f"<@{author_id}>"
    message.guild.id = guild_id
    message.channel.id = channel_id
    message.channel.send = AsyncMock()
    message.add_reaction = AsyncMock()
    return message


def make_interaction(guild_id, user_id=7):
    interaction = MagicMock()
    interaction.guild_id = guild_id
    interaction.user.id = user_id
    interaction.user.mention = f"<@{user_id}>"
    interaction.response.send_message = AsyncMock()
    interaction.response.edit_message = AsyncMock()
    interaction.message.edit = AsyncMock()
    interaction.message.embeds = [discord.Embed(title="Streak Broken!")]
    return interaction


class CountingTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import main

        main._COUNTING_LOCKS.clear()


class TestCountingConcurrency(CountingTestCase):
    async def test_same_number_at_once_only_one_counts(self):
        import main

        bot = make_bot(delay=0.02)
        first, second = make_message("1", 10), make_message("1", 11)
        with patch("main.bot", bot):
            await asyncio.gather(main.handle_counting(first), main.handle_counting(second))

        ticks = [m for m in (first, second) if m.add_reaction.await_args_list
                 and m.add_reaction.await_args_list[0].args[0] == "✅"]
        self.assertEqual(len(ticks), 1)
        state = bot.settings.counting.find_one({"guild_id": 1})
        self.assertEqual(state["current"], 1)
        self.assertTrue(state["broken"])  # the slower one broke the streak

    async def test_sequence_counts_up(self):
        import main

        bot = make_bot()
        with patch("main.bot", bot):
            for number, author in ((1, 10), (2, 11), (3, 10)):
                await main.handle_counting(make_message(str(number), author))
        state = bot.settings.counting.find_one({"guild_id": 1})
        self.assertEqual((state["current"], state["streak"], state["broken"]), (3, 3, False))

    async def test_counting_twice_in_a_row_breaks(self):
        import main

        bot = make_bot()
        with patch("main.bot", bot):
            await main.handle_counting(make_message("1", 10))
            again = make_message("2", 10)
            await main.handle_counting(again)
        again.channel.send.assert_awaited_once()
        self.assertTrue(bot.settings.counting.find_one({"guild_id": 1})["broken"])

    async def test_other_channel_is_ignored(self):
        import main

        bot = make_bot()
        with patch("main.bot", bot):
            await main.handle_counting(make_message("1", 10, channel_id=99))
        self.assertIsNone(bot.settings.counting.find_one({"guild_id": 1}))

    async def test_deleting_the_current_count_breaks_the_streak(self):
        import main

        bot = make_bot()
        with patch("main.bot", bot):
            await main.handle_counting(make_message("1", 10))
            deleted = make_message("1", 10)
            self.assertTrue(await main.handle_counting_retraction(deleted, deleted, "deleted"))
            self.assertFalse(await main.handle_counting_retraction(deleted, deleted, "deleted"))
        embed = deleted.channel.send.await_args.kwargs["embed"]
        self.assertIn("deleted their valid count", embed.description)


class TestCountingSaves(CountingTestCase):
    def _break(self, bot, guild_id):
        bot.settings.counting.update_one(
            {"guild_id": guild_id},
            {"$set": {"guild_id": guild_id, "current": 5, "streak": 5, "last_user": 1,
                      "broken": True, "broken_time": time.time()}},
            upsert=True,
        )

    async def test_saves_are_counted_per_server(self):
        import main

        bot = make_bot(saves=1)
        for guild_id in (1, 2):
            self._break(bot, guild_id)
        with patch("main.bot", bot):
            one = make_interaction(1)
            await main._counting_use_save(one, 1)
            one.response.edit_message.assert_awaited_once()

            two = make_interaction(2)
            await main._counting_use_save(two, 2)  # a different server: its own budget
            two.response.edit_message.assert_awaited_once()

            self._break(bot, 1)
            again = make_interaction(1)
            await main._counting_use_save(again, 1)
            again.response.edit_message.assert_not_awaited()
            self.assertIn("all 1 of your saves", again.response.send_message.await_args.args[0])

            today = main._counting_today()
            self.assertEqual(await main._counting_saves_used(1, 7, today), 1)
            self.assertEqual(await main._counting_saves_used(2, 7, today), 1)
            self.assertEqual(await main._counting_saves_used(3, 7, today), 0)

    async def test_save_answers_with_edit_message_and_reads_db_state(self):
        import main

        bot = make_bot()
        self._break(bot, 1)
        interaction = make_interaction(1)
        with patch("main.bot", bot):
            await main._counting_use_save(interaction, 1)
        interaction.response.edit_message.assert_awaited_once()
        interaction.message.edit.assert_not_awaited()
        kwargs = interaction.response.edit_message.await_args.kwargs
        self.assertEqual(kwargs["embed"].title, "Streak Saved!")
        self.assertIn("**6**", kwargs["embed"].description)
        self.assertTrue(all(child.disabled for child in kwargs["view"].children))
        self.assertFalse(bot.settings.counting.find_one({"guild_id": 1})["broken"])

    async def test_save_when_not_broken(self):
        import main

        bot = make_bot()
        interaction = make_interaction(1)
        with patch("main.bot", bot):
            await main._counting_use_save(interaction, 1)
        self.assertIn("no longer broken", interaction.response.send_message.await_args.args[0])

    async def test_button_from_another_guild_is_refused(self):
        import main

        bot = make_bot()
        self._break(bot, 1)
        interaction = make_interaction(2)
        with patch("main.bot", bot):
            await main._counting_use_save(interaction, 1)
        interaction.response.edit_message.assert_not_awaited()
        self.assertTrue(bot.settings.counting.find_one({"guild_id": 1})["broken"])


class TestCountingButton(unittest.IsolatedAsyncioTestCase):
    async def test_custom_id_round_trip(self):
        from main import CountingSaveButton, CountingSaveView

        button = CountingSaveButton(123456789012345678)
        self.assertEqual(button.custom_id, "counting:save:123456789012345678")
        match = CountingSaveButton.__discord_ui_compiled_template__.fullmatch(button.custom_id)
        self.assertIsNotNone(match)
        rebuilt = await CountingSaveButton.from_custom_id(MagicMock(), button.item, match)
        self.assertEqual(rebuilt.guild_id, 123456789012345678)
        view = CountingSaveView(42)
        self.assertIsNone(view.timeout)
        self.assertTrue(view.is_persistent())

    async def test_registered_on_setup(self):
        import inspect

        import main

        self.assertIn("add_dynamic_items(CountingSaveButton)", inspect.getsource(main.DebtCollectorBot.setup_hook))

    def test_broken_embed_shows_configured_saves(self):
        from main import _counting_broken_embed

        user = MagicMock(mention="<@1>")
        state = {"streak": 9}
        self.assertIn("5 saves per day", _counting_broken_embed(user, state, "r", 5).description)
        self.assertIn("1 save per day", _counting_broken_embed(user, state, "r", 1).description)
        self.assertNotIn("3 saves", _counting_broken_embed(user, state, "r", 5).description)


if __name__ == "__main__":
    unittest.main()
