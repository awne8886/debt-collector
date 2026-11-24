import asyncio
import random
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from cogs.music import GuildPlayer, MusicCog
from utils.music import (
    InvalidQuery,
    LoopMode,
    MusicQueue,
    PlaybackClock,
    QueryKind,
    QueueFull,
    Track,
    format_duration,
    parse_query,
    parse_timestamp,
    progress_bar,
    required_votes,
)
from utils.ytdl import (
    ExtractionError,
    StreamInfo,
    _base_options,
    _find_cookie_file,
    _writable_cookie_file,
    _entry_to_track,
    friendly_error,
)

VID = "dQw4w9WgXcQ"
CANON = f"https://www.youtube.com/watch?v={VID}"


def make_track(name: str, duration: int = 100, requester: int = 1) -> Track:
    return Track(
        title=name, url=f"https://www.youtube.com/watch?v={name:_<11.11}",
        duration=duration, requester_id=requester, requester_name="tester",
    )


class TestParseQuery(unittest.TestCase):
    def test_watch_links_are_canonicalised(self):
        for url in (
            f"https://www.youtube.com/watch?v={VID}",
            f"https://youtube.com/watch?v={VID}&list=PL1234567890abc&index=3",
            f"https://music.youtube.com/watch?v={VID}&si=abc",
            f"https://m.youtube.com/watch?v={VID}",
            f"https://youtu.be/{VID}?t=42",
            f"https://www.youtube.com/shorts/{VID}",
            f"https://www.youtube.com/embed/{VID}",
            f"https://www.youtube.com/live/{VID}",
            f"<https://youtu.be/{VID}>",
            f"youtu.be/{VID}",
            f"music.youtube.com/watch?v={VID}",
        ):
            with self.subTest(url=url):
                parsed = parse_query(url)
                self.assertEqual(parsed.kind, QueryKind.VIDEO)
                self.assertEqual(parsed.value, CANON)

    def test_playlist_link(self):
        parsed = parse_query("https://music.youtube.com/playlist?list=PLabcdefghij123")
        self.assertEqual(parsed.kind, QueryKind.PLAYLIST)
        self.assertEqual(parsed.value, "https://www.youtube.com/playlist?list=PLabcdefghij123")

    def test_plain_text_is_a_search(self):
        parsed = parse_query("  never gonna give you up  ")
        self.assertEqual(parsed.kind, QueryKind.SEARCH)
        self.assertEqual(parsed.value, "never gonna give you up")

    def test_non_youtube_links_are_rejected(self):
        for bad in (
            "https://example.com/watch?v=" + VID,
            "file:///etc/passwd",
            "http://localhost:8080/x",
            "https://youtube.com.evil.com/watch?v=" + VID,
            "https://notyoutube.com/watch?v=" + VID,
            "ftp://youtube.com/watch?v=" + VID,
            "https://soundcloud.com/a/b",
        ):
            with self.subTest(url=bad):
                with self.assertRaises(InvalidQuery):
                    parse_query(bad)

    def test_unplayable_youtube_links_are_rejected(self):
        for bad in (
            "https://www.youtube.com/",
            "https://www.youtube.com/@somechannel",
            "https://www.youtube.com/watch?v=short",
            "https://www.youtube.com/playlist",
        ):
            with self.subTest(url=bad):
                with self.assertRaises(InvalidQuery):
                    parse_query(bad)

    def test_empty_and_oversized(self):
        with self.assertRaises(InvalidQuery):
            parse_query("   ")
        with self.assertRaises(InvalidQuery):
            parse_query("a" * 500)


class TestTimes(unittest.TestCase):
    def test_format_duration(self):
        self.assertEqual(format_duration(0), "0:00")
        self.assertEqual(format_duration(75), "1:15")
        self.assertEqual(format_duration(3725), "1:02:05")
        self.assertEqual(format_duration(None), "LIVE")

    def test_parse_timestamp(self):
        self.assertEqual(parse_timestamp("90"), 90)
        self.assertEqual(parse_timestamp("1:30"), 90)
        self.assertEqual(parse_timestamp("1:02:03"), 3723)
        self.assertEqual(parse_timestamp("1m30s"), 90)
        self.assertEqual(parse_timestamp("1h2m3s"), 3723)
        self.assertEqual(parse_timestamp("45s"), 45)
        for bad in ("", "abc", "1:xx", "-5", "1:2:3:4", "5 minutes"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    parse_timestamp(bad)

    def test_progress_bar(self):
        self.assertEqual(len(progress_bar(0, 100, width=10).replace("🔘", "x")), 10)
        self.assertTrue(progress_bar(0, 100).startswith("🔘"))
        self.assertTrue(progress_bar(100, 100).endswith("🔘"))
        self.assertEqual(progress_bar(5, None), "🔴 LIVE")

    def test_required_votes(self):
        self.assertEqual([required_votes(n) for n in (0, 1, 2, 3, 4, 5)], [1, 1, 1, 2, 2, 3])


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.q = MusicQueue()
        self.a, self.b, self.c = make_track("a"), make_track("b"), make_track("c")

    def fill(self):
        self.q.add_many([self.a, self.b, self.c])

    def test_add_positions_and_front(self):
        self.assertEqual(self.q.add(self.a), 1)
        self.assertEqual(self.q.add(self.b), 2)
        self.assertEqual(self.q.add(self.c, front=True), 1)
        self.assertEqual(self.q.upcoming, [self.c, self.a, self.b])

    def test_capacity(self):
        small = MusicQueue(max_size=2)
        small.add(self.a)
        small.add(self.b)
        with self.assertRaises(QueueFull):
            small.add(self.c)
        self.assertEqual(small.add_many([self.c]), 0)
        big = MusicQueue(max_size=2)
        self.assertEqual(big.add_many([self.a, self.b, self.c]), 2)

    def test_add_many_front_keeps_order(self):
        self.q.add(self.c)
        self.q.add_many([self.a, self.b], front=True)
        self.assertEqual(self.q.upcoming, [self.a, self.b, self.c])

    def test_advance_plain(self):
        self.fill()
        self.assertIs(self.q.advance(), self.a)
        self.assertIs(self.q.advance(), self.b)
        self.assertIs(self.q.advance(), self.c)
        self.assertIsNone(self.q.advance())
        self.assertIsNone(self.q.current)

    def test_loop_track_repeats_on_end_but_not_on_skip(self):
        self.fill()
        self.q.advance()
        self.q.loop = LoopMode.TRACK
        self.assertIs(self.q.advance(), self.a)
        self.assertIs(self.q.advance(), self.a)
        self.assertIs(self.q.advance(skipped=True), self.b)

    def test_loop_queue_recycles(self):
        self.fill()
        self.q.advance()
        self.q.loop = LoopMode.QUEUE
        order = [self.q.advance() for _ in range(5)]
        self.assertEqual(order, [self.b, self.c, self.a, self.b, self.c])

    def test_loop_queue_single_track(self):
        self.q.add(self.a)
        self.q.loop = LoopMode.QUEUE
        self.assertIs(self.q.advance(), self.a)
        self.assertIs(self.q.advance(), self.a)

    def test_remove_move_peek(self):
        self.fill()
        self.assertIs(self.q.peek(2), self.b)
        self.assertIs(self.q.move(3, 1), self.c)
        self.assertEqual(self.q.upcoming, [self.c, self.a, self.b])
        self.assertIs(self.q.remove(2), self.a)
        self.assertEqual(self.q.upcoming, [self.c, self.b])

    def test_bad_positions(self):
        self.fill()
        for position in (0, 4, -1):
            with self.subTest(position=position):
                with self.assertRaises(IndexError):
                    self.q.remove(position)
        with self.assertRaises(IndexError):
            self.q.move(1, 9)

    def test_jump(self):
        self.fill()
        self.assertIs(self.q.jump(3), self.c)
        self.assertEqual(self.q.upcoming, [self.c])

    def test_jump_wraps_in_queue_loop(self):
        self.fill()
        self.q.loop = LoopMode.QUEUE
        self.q.jump(3)
        self.assertEqual(self.q.upcoming, [self.c, self.a, self.b])

    def test_shuffle_keeps_contents(self):
        tracks = [make_track(f"t{i}") for i in range(10)]
        self.q.add_many(tracks)
        self.q.shuffle(random.Random(7))
        self.assertCountEqual(self.q.upcoming, tracks)
        self.assertNotEqual(self.q.upcoming, tracks)

    def test_clear_stop_and_remaining(self):
        self.fill()
        self.assertEqual(self.q.remaining_seconds(), 300)
        self.q.advance()
        self.assertEqual(self.q.clear(), 2)
        self.assertIs(self.q.current, self.a)
        self.q.add(self.b)
        self.q.stop()
        self.assertIsNone(self.q.current)
        self.assertEqual(len(self.q), 0)

    def test_loop_mode_cycle(self):
        self.assertIs(LoopMode.OFF.next(), LoopMode.TRACK)
        self.assertIs(LoopMode.TRACK.next(), LoopMode.QUEUE)
        self.assertIs(LoopMode.QUEUE.next(), LoopMode.OFF)


class TestClock(unittest.TestCase):
    def test_position_pause_resume_seek(self):
        now = [100.0]
        clock = PlaybackClock(lambda: now[0])
        self.assertEqual(clock.position(), 0.0)
        clock.start()
        now[0] = 110.0
        self.assertEqual(clock.position(), 10.0)
        clock.pause()
        now[0] = 150.0
        self.assertEqual(clock.position(), 10.0)
        self.assertTrue(clock.paused)
        clock.resume()
        now[0] = 155.0
        self.assertEqual(clock.position(), 15.0)
        clock.start(60)
        now[0] = 165.0
        self.assertEqual(clock.position(), 70.0)
        clock.reset()
        self.assertEqual(clock.position(), 0.0)


class TestYtdlHelpers(unittest.TestCase):
    def test_friendly_errors(self):
        with patch.dict("os.environ", {}, clear=True):
            bot_check = friendly_error("ERROR: Sign in to confirm you're not a bot")
        self.assertIn("YTDL_PROXY", bot_check)
        self.assertIn("YTDL_COOKIES_FILE", bot_check)
        self.assertIn("private", friendly_error("ERROR: Private video. Sign in"))
        self.assertIn("age-restricted", friendly_error("Sign in to confirm your age"))
        self.assertEqual(friendly_error("???"), "I couldn't load that from YouTube.")
        self.assertIn("JavaScript runtime", friendly_error("ERROR: Requested format is not available"))
        self.assertIn("403", friendly_error("Unable to download API page: HTTP Error 403: Forbidden"))

    def test_base_options_defaults(self):
        with patch.dict("os.environ", {}, clear=True):
            options = _base_options()
        self.assertEqual(options["source_address"], "0.0.0.0")
        self.assertNotIn("proxy", options)
        self.assertIn("deno", options["js_runtimes"])
        self.assertNotIn("extractor_args", options)

    def test_base_options_proxy_and_clients(self):
        env = {
            "YTDL_PROXY": " socks5://127.0.0.1:40000 ",
            "YTDL_PLAYER_CLIENTS": "tv_simply, web_embedded,,",
        }
        with patch.dict("os.environ", env, clear=True):
            options = _base_options()
        self.assertEqual(options["proxy"], "socks5://127.0.0.1:40000")
        self.assertNotIn("source_address", options)
        self.assertEqual(
            options["extractor_args"], {"youtube": {"player_client": ["tv_simply", "web_embedded"]}}
        )

    def test_read_only_cookies_are_copied(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(src_dir, "cookies.txt")
            with open(source, "w") as handle:
                handle.write("# Netscape HTTP Cookie File\n")
            _writable_cookie_file.cache_clear()
            with patch("utils.ytdl.os.access", return_value=False), patch(
                "utils.ytdl.tempfile.gettempdir", return_value=tmp
            ), patch.dict("os.environ", {"YTDL_COOKIES_FILE": source}, clear=True):
                options = _base_options()
            _writable_cookie_file.cache_clear()
            self.assertEqual(options["cookiefile"], os.path.join(tmp, "ytdl-cookies.txt"))
            with open(options["cookiefile"]) as handle:
                self.assertIn("Netscape", handle.read())

    def test_missing_cookie_file_is_reported(self):
        _find_cookie_file.cache_clear()
        env = {"YTDL_COOKIES_FILE": "no/such/dir/cookies.txt"}
        with patch.dict("os.environ", env, clear=True), self.assertLogs(
            "debt-collector.music", "WARNING"
        ):
            options = _base_options()
            message = friendly_error("Sign in to confirm you're not a bot")
        _find_cookie_file.cache_clear()
        self.assertNotIn("cookiefile", options)
        self.assertIn("doesn't exist", message)
        self.assertIn("no/such/dir/cookies.txt", message)

    def test_cookie_path_missing_leading_slash_is_fixed(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as src_dir:
            source = os.path.join(src_dir, "cookies.txt")
            open(source, "w").close()
            relative = source.lstrip(os.sep)
            _find_cookie_file.cache_clear()
            _writable_cookie_file.cache_clear()
            with patch.dict("os.environ", {"YTDL_COOKIES_FILE": relative}, clear=True):
                options = _base_options()
                message = friendly_error("Sign in to confirm you're not a bot")
            _find_cookie_file.cache_clear()
            _writable_cookie_file.cache_clear()
            self.assertEqual(options["cookiefile"], source)
            self.assertIn("didn't accept the cookies", message)

    def test_writable_cookies_are_used_in_place(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as src_dir:
            source = os.path.join(src_dir, "cookies.txt")
            open(source, "w").close()
            _writable_cookie_file.cache_clear()
            with patch.dict("os.environ", {"YTDL_COOKIES_FILE": source}, clear=True):
                options = _base_options()
            _writable_cookie_file.cache_clear()
            self.assertEqual(options["cookiefile"], source)

    def test_resolve_stream_retries_with_fallback_clients(self):
        import yt_dlp

        from utils.ytdl import resolve_stream

        calls = []

        class FakeYDL:
            def __init__(self, options):
                self.options = options

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def extract_info(self, url, download=False):
                calls.append(self.options)
                if len(calls) == 1:
                    raise yt_dlp.utils.DownloadError("ERROR: Requested format is not available")
                return {"url": "https://audio.invalid/a", "duration": 10}

        with patch.dict("os.environ", {}, clear=True), patch("yt_dlp.YoutubeDL", FakeYDL), \
                self.assertLogs("debt-collector.music", "WARNING"):
            stream = resolve_stream(make_track("song"))
        self.assertEqual(stream.url, "https://audio.invalid/a")
        self.assertEqual(len(calls), 2)
        self.assertNotIn("extractor_args", calls[0])
        self.assertEqual(calls[1]["extractor_args"]["youtube"]["player_client"][0], "default")

    def test_resolve_stream_does_not_retry_other_errors(self):
        import yt_dlp

        from utils.ytdl import resolve_stream

        calls = []

        class FakeYDL:
            def __init__(self, options):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def extract_info(self, url, download=False):
                calls.append(url)
                raise yt_dlp.utils.DownloadError("ERROR: Private video")

        with patch.dict("os.environ", {}, clear=True), patch("yt_dlp.YoutubeDL", FakeYDL), \
                self.assertLogs("debt-collector.music", "WARNING"), \
                self.assertRaises(ExtractionError):
            resolve_stream(make_track("song"))
        self.assertEqual(len(calls), 1)

    def test_entry_to_track(self):
        entry = {"id": VID, "title": "Song", "duration": 212.0, "channel": "Artist"}
        track = _entry_to_track(entry, 5, "Me")
        self.assertEqual(track.url, CANON)
        self.assertEqual(track.duration, 212)
        self.assertEqual(track.requester_id, 5)
        self.assertIsNone(_entry_to_track({"id": "bad", "title": "x"}, 1, "x"))
        self.assertIsNone(_entry_to_track({"id": VID, "title": "[Private video]"}, 1, "x"))
        live = _entry_to_track({"id": VID, "title": "Radio", "is_live": True, "duration": 5}, 1, "x")
        self.assertTrue(live.is_live)
        self.assertIsNone(live.duration)


# --------------------------------------------------------------------------- #
# Player state machine, driven with a fake voice connection
# --------------------------------------------------------------------------- #


class FakeVoice:
    """Mimics VoiceClient: stop() fires the `after` callback shortly afterwards."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.playing = False
        self.paused = False
        self.after = None
        self.play_calls = 0
        self.source = None
        member = MagicMock(spec=discord.Member)
        member.bot = False
        self.channel = MagicMock()
        self.channel.members = [member]

    def is_connected(self):
        return True

    def is_playing(self):
        return self.playing and not self.paused

    def is_paused(self):
        return self.paused

    def play(self, source, after=None):
        assert not self.playing, "play() while already playing"
        self.playing, self.paused, self.after, self.source = True, False, after, source
        self.play_calls += 1

    def stop(self):
        after, self.playing, self.paused = self.after, False, False
        if after:
            self.loop.call_soon(after, None)

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def finish_naturally(self):
        after, self.playing = self.after, False
        after(None)


class TestGuildPlayer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        loop = asyncio.get_running_loop()
        self.vc = FakeVoice(loop)
        bot = MagicMock()
        bot.loop = loop
        self.cog = MusicCog(bot)
        guild = MagicMock()
        guild.id = 1
        self.guild = guild
        self.player = GuildPlayer(self.cog, guild)
        self.channel = MagicMock()
        self.channel.send = AsyncMock()
        self.player.text_channel = self.channel

        self.failing: set = set()

        def fake_resolve(track):
            if track.title in self.failing:
                raise ExtractionError("nope")
            return StreamInfo(url="https://example.invalid/audio", duration=track.duration)

        patches = [
            patch("cogs.music.resolve_stream", side_effect=fake_resolve),
            patch.object(GuildPlayer, "_build_source", lambda self_, stream, pos: object()),
            # `voice` normally reads guild.voice_client; point it at the fake instead.
            patch.object(GuildPlayer, "voice", property(lambda self_: self.vc)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.player.shutdown)

    async def until(self, predicate, timeout: float = 3.0):
        """Wait for something that involves a worker thread (stream resolution)."""
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                self.fail("timed out waiting for the player")
            await asyncio.sleep(0.005)

    async def settle(self):
        """Give stray callbacks time to (wrongly) do something before asserting nothing did."""
        await asyncio.sleep(0.1)

    def titles_played(self):
        return self.player.queue.current.title if self.player.queue.current else None

    async def test_starts_and_advances_on_natural_end(self):
        a, b = make_track("a"), make_track("b")
        self.player.queue.add_many([a, b])
        self.assertTrue(await self.player.ensure_playing())
        self.assertIs(self.player.queue.current, a)
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 2)
        self.assertIs(self.player.queue.current, b)
        self.vc.finish_naturally()
        await self.until(lambda: self.player.queue.current is None)
        self.assertFalse(self.player.active)

    async def test_ensure_playing_is_idempotent_while_playing(self):
        self.player.queue.add_many([make_track("a"), make_track("b")])
        self.assertTrue(await self.player.ensure_playing())
        self.assertFalse(await self.player.ensure_playing())
        self.assertEqual(self.vc.play_calls, 1)
        self.assertEqual(self.titles_played(), "a")

    async def test_skip_ignores_the_stale_end_callback(self):
        self.player.queue.add_many([make_track("a"), make_track("b"), make_track("c")])
        await self.player.ensure_playing()
        await self.player.skip()
        await self.settle()  # the stopped source's `after` fires here and must be ignored
        self.assertEqual(self.titles_played(), "b")
        self.assertEqual(self.vc.play_calls, 2)

    async def test_skip_last_track_stops_cleanly(self):
        self.player.queue.add(make_track("a"))
        await self.player.ensure_playing()
        await self.player.skip()
        await self.settle()
        self.assertIsNone(self.player.queue.current)
        self.assertFalse(self.player.active)

    async def test_loop_track_replays_but_skip_moves_on(self):
        self.player.queue.add_many([make_track("a"), make_track("b")])
        await self.player.ensure_playing()
        self.player.queue.loop = LoopMode.TRACK
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 2)
        self.assertEqual(self.titles_played(), "a")
        await self.player.skip()
        await self.settle()
        self.assertEqual(self.titles_played(), "b")

    async def test_broken_track_is_skipped(self):
        self.failing.add("bad")
        self.player.queue.add_many([make_track("bad"), make_track("good")])
        self.assertTrue(await self.player.ensure_playing())
        self.assertEqual(self.titles_played(), "good")
        self.assertEqual(self.vc.play_calls, 1)
        self.channel.send.assert_awaited()  # the user is told why it was skipped

    async def test_all_tracks_broken_reports_failure(self):
        self.failing.update({"x", "y"})
        self.player.queue.add_many([make_track("x"), make_track("y")])
        self.assertFalse(await self.player.ensure_playing())
        self.assertIsNone(self.player.queue.current)
        self.assertEqual(self.vc.play_calls, 0)

    async def test_three_instant_deaths_halt_the_queue(self):
        self.player.queue.add_many([make_track(f"t{i}") for i in range(6)])
        await self.player.ensure_playing()
        for expected_plays in (2, 3):
            self.vc.finish_naturally()  # ends immediately: position ~0s on a 100s track
            await self.until(lambda: self.vc.play_calls == expected_plays)
        self.vc.finish_naturally()
        await self.until(lambda: self.player.queue.current is None)
        self.assertEqual(self.vc.play_calls, 3)
        self.assertEqual(len(self.player.queue), 0)
        sent = " ".join(str(c) for c in self.channel.send.await_args_list)
        self.assertIn("Three tracks in a row", sent)

    async def test_normal_length_playback_resets_the_failure_count(self):
        self.player.queue.add_many([make_track(f"t{i}") for i in range(4)])
        await self.player.ensure_playing()
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 2)
        self.assertEqual(self.player._quick_failures, 1)
        self.player.clock.start(50)  # pretend the second track really played for a while
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 3)
        self.assertEqual(self.player._quick_failures, 0)

    async def test_stop_clears_everything(self):
        self.player.queue.add_many([make_track("a"), make_track("b")])
        await self.player.ensure_playing()
        await self.player.stop()
        await self.settle()
        self.assertIsNone(self.player.queue.current)
        self.assertEqual(len(self.player.queue), 0)
        self.assertEqual(self.vc.play_calls, 1)

    async def test_seek_restarts_current_track_without_advancing(self):
        self.player.queue.add_many([make_track("a"), make_track("b")])
        await self.player.ensure_playing()
        self.assertTrue(await self.player.seek(30))
        await self.settle()
        self.assertEqual(self.titles_played(), "a")
        self.assertEqual(self.vc.play_calls, 2)
        self.assertAlmostEqual(self.player.clock.position(), 30, delta=1)

    async def test_pause_and_resume_track_the_clock(self):
        self.player.queue.add(make_track("a"))
        await self.player.ensure_playing()
        self.player.pause()
        self.assertTrue(self.vc.is_paused())
        self.assertTrue(self.player.clock.paused)
        self.player.resume()
        self.assertFalse(self.vc.is_paused())
        self.assertFalse(self.player.clock.paused)

    async def test_skip_votes_are_cleared_on_new_track(self):
        self.player.queue.add_many([make_track("a"), make_track("b")])
        await self.player.ensure_playing()
        self.player.skip_votes.add(42)
        await self.player.skip()
        self.assertEqual(self.player.skip_votes, set())


class TestMusicCommands(unittest.IsolatedAsyncioTestCase):
    """The command layer: replies, validation, voting and permission gates."""

    async def asyncSetUp(self):
        loop = asyncio.get_running_loop()
        self.vc = FakeVoice(loop)
        self.vc.channel.id = 10
        self.vc.channel.members = []  # each test adds exactly the listeners it needs
        bot = MagicMock()
        bot.loop = loop
        self.cog = MusicCog(bot)
        self.guild = MagicMock()
        self.guild.id = 1
        self.player = GuildPlayer(self.cog, self.guild)
        self.cog.players[1] = self.player
        self.resolved = []

        def fake_resolve_tracks(parsed, requester_id, requester_name):
            from utils.ytdl import ResolvedQuery
            return ResolvedQuery(self.resolved, playlist_title="Mix")

        patches = [
            patch("cogs.music.resolve_stream", side_effect=lambda t: StreamInfo(url="https://x.invalid/a")),
            patch("cogs.music.resolve_tracks", side_effect=fake_resolve_tracks),
            patch.object(GuildPlayer, "_build_source", lambda self_, stream, pos: object()),
            patch.object(GuildPlayer, "voice", property(lambda self_: self.vc)),
            patch.object(MusicCog, "_connect", AsyncMock(return_value=self.player)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.player.shutdown)

    def member(self, user_id=1, name="me", perms=None, listening=True):
        member = MagicMock(spec=discord.Member)
        member.id, member.display_name, member.bot = user_id, name, False
        member.guild_permissions = perms or discord.Permissions.none()
        member.voice = MagicMock()
        member.voice.channel = self.vc.channel
        if listening:
            self.vc.channel.members.append(member)
        return member

    def ctx(self, author):
        ctx = MagicMock()
        ctx.author, ctx.guild, ctx.channel = author, self.guild, MagicMock()
        ctx.channel.send = AsyncMock()
        ctx.send = AsyncMock()
        ctx.typing.return_value.__aexit__ = AsyncMock(return_value=False)
        return ctx

    def last_send(self, ctx):
        return ctx.send.await_args

    def embed_of(self, ctx):
        return self.last_send(ctx).kwargs["embed"]

    async def run_cmd(self, command, ctx, *args):
        if command in (MusicCog.play, MusicCog.playnext):  # `query` is keyword-only
            await command.callback(self.cog, ctx, query=args[0])
        else:
            await command.callback(self.cog, ctx, *args)

    async def test_first_play_starts_and_second_queues(self):
        me = self.member()
        self.resolved = [make_track("first")]
        ctx = self.ctx(me)
        await self.run_cmd(MusicCog.play, ctx, "some song")
        self.assertEqual(self.player.queue.current.title, "first")
        self.assertIn("Now playing", self.embed_of(ctx).title)

        self.resolved = [make_track("second")]
        ctx = self.ctx(me)
        await self.run_cmd(MusicCog.play, ctx, "another")
        self.assertEqual([t.title for t in self.player.queue.upcoming], ["second"])
        embed = self.embed_of(ctx)
        self.assertIn("Added to the queue", embed.title)
        self.assertEqual(embed.fields[0].value, "#1")

    async def test_playnext_goes_to_the_front(self):
        me = self.member()
        self.resolved = [make_track("one")]
        await self.run_cmd(MusicCog.play, self.ctx(me), "x")
        self.player.queue.add_many([make_track("later")])
        self.resolved = [make_track("urgent")]
        await self.run_cmd(MusicCog.playnext, self.ctx(me), "x")
        self.assertEqual([t.title for t in self.player.queue.upcoming], ["urgent", "later"])

    async def test_playlist_queues_the_rest(self):
        me = self.member()
        self.resolved = [make_track("p1"), make_track("p2"), make_track("p3")]
        ctx = self.ctx(me)
        await self.run_cmd(MusicCog.play, ctx, "https://www.youtube.com/playlist?list=PLabcdefghij123")
        self.assertEqual(self.player.queue.current.title, "p1")
        self.assertEqual(len(self.player.queue), 2)
        self.assertEqual(self.embed_of(ctx).fields[-1].name, "Playlist")

    async def test_non_youtube_link_is_refused_privately(self):
        ctx = self.ctx(self.member())
        await self.run_cmd(MusicCog.play, ctx, "https://example.com/song.mp3")
        self.assertTrue(self.last_send(ctx).kwargs["ephemeral"])
        self.assertIn("YouTube", self.last_send(ctx).args[0])
        self.assertIsNone(self.player.queue.current)

    async def test_over_length_tracks_are_refused(self):
        ctx = self.ctx(self.member())
        self.resolved = [make_track("huge", duration=99 * 3600)]
        await self.run_cmd(MusicCog.play, ctx, "x")
        self.assertIn("limit", self.last_send(ctx).args[0])
        self.assertIsNone(self.player.queue.current)

    async def test_requester_skips_instantly(self):
        me = self.member(1)
        self.member(2)
        self.member(3)
        self.player.queue.add_many([make_track("a", requester=1), make_track("b")])
        await self.player.ensure_playing()
        await self.run_cmd(MusicCog.skip, self.ctx(me))
        self.assertEqual(self.player.queue.current.title, "b")

    async def test_others_must_vote_to_skip(self):
        self.member(1)
        two, three = self.member(2), self.member(3)
        self.player.queue.add_many([make_track("a", requester=1), make_track("b")])
        await self.player.ensure_playing()
        ctx = self.ctx(two)
        await self.run_cmd(MusicCog.skip, ctx)
        self.assertEqual(self.player.queue.current.title, "a")  # 1 of 2 votes
        self.assertIn("1/2", self.last_send(ctx).args[0])
        await self.run_cmd(MusicCog.skip, self.ctx(three))
        self.assertEqual(self.player.queue.current.title, "b")  # second vote carries it

    async def test_votes_from_people_who_left_do_not_count(self):
        self.member(1)  # requester
        voter, stayer, _ = self.member(2), self.member(3), self.member(4)
        self.player.queue.add_many([make_track("a", requester=1), make_track("b")])
        await self.player.ensure_playing()
        await self.run_cmd(MusicCog.skip, self.ctx(voter))  # 1 of 2 needed (4 listeners)
        self.vc.channel.members.remove(voter)  # the voter leaves; 3 listeners, 2 needed
        ctx = self.ctx(stayer)
        await self.run_cmd(MusicCog.skip, ctx)
        self.assertEqual(self.player.queue.current.title, "a")  # the departed vote was dropped
        self.assertIn("1/2", self.last_send(ctx).args[0])

    async def test_stop_needs_staff_or_sole_listener(self):
        plain = self.member(5)
        self.member(6)
        self.player.queue.add_many([make_track("a"), make_track("b")])
        await self.player.ensure_playing()
        ctx = self.ctx(plain)
        await self.run_cmd(MusicCog.stop, ctx)
        self.assertTrue(self.last_send(ctx).kwargs["ephemeral"])
        self.assertEqual(self.player.queue.current.title, "a")

        staff = self.member(7, perms=discord.Permissions(manage_channels=True))
        await self.run_cmd(MusicCog.stop, self.ctx(staff))
        self.assertIsNone(self.player.queue.current)

    async def test_sole_listener_can_stop(self):
        me = self.member(5)
        self.player.queue.add_many([make_track("a")])
        await self.player.ensure_playing()
        await self.run_cmd(MusicCog.stop, self.ctx(me))
        self.assertIsNone(self.player.queue.current)

    async def test_user_outside_the_voice_channel_cannot_control(self):
        outsider = self.member(9, listening=False)
        outsider.voice.channel = MagicMock()
        outsider.voice.channel.id = 99
        self.player.queue.add_many([make_track("a")])
        await self.player.ensure_playing()
        ctx = self.ctx(outsider)
        await self.run_cmd(MusicCog.pause, ctx)
        self.assertTrue(self.last_send(ctx).kwargs["ephemeral"])
        self.assertFalse(self.vc.is_paused())

    async def test_volume_validation_and_set(self):
        me = self.member()
        self.player.queue.add_many([make_track("a")])
        await self.player.ensure_playing()
        ctx = self.ctx(me)
        await self.run_cmd(MusicCog.volume, ctx, 999)
        self.assertTrue(self.last_send(ctx).kwargs["ephemeral"])
        self.assertNotEqual(self.player.volume, 999)
        await self.run_cmd(MusicCog.volume, self.ctx(me), 80)
        self.assertEqual(self.player.volume, 80)

    async def test_loop_cycles_without_argument(self):
        me = self.member()
        modes = []
        for _ in range(3):
            await self.run_cmd(MusicCog.loop, self.ctx(me), None)
            modes.append(self.player.queue.loop)
        self.assertEqual(modes, [LoopMode.TRACK, LoopMode.QUEUE, LoopMode.OFF])

    async def test_queue_remove_is_limited_to_the_requester_or_staff(self):
        mine = self.member(1)
        self.member(2)
        self.member(3)
        self.player.queue.add_many([make_track("m", requester=1), make_track("o", requester=2)])
        ctx = self.ctx(mine)
        await self.run_cmd(MusicCog.queue_remove, ctx, 2)  # someone else's track
        self.assertEqual(len(self.player.queue), 2)
        self.assertTrue(self.last_send(ctx).kwargs["ephemeral"])
        await self.run_cmd(MusicCog.queue_remove, self.ctx(mine), 1)  # own track
        self.assertEqual([t.title for t in self.player.queue.upcoming], ["o"])

    async def test_seek_rejects_bad_input_and_live_streams(self):
        me = self.member()
        self.player.queue.add_many([make_track("a", duration=100)])
        await self.player.ensure_playing()
        for bad in ("nonsense", "500"):
            ctx = self.ctx(me)
            await self.run_cmd(MusicCog.seek, ctx, bad)
            self.assertTrue(self.last_send(ctx).kwargs["ephemeral"], bad)
        ctx = self.ctx(me)
        await self.run_cmd(MusicCog.seek, ctx, "0:30")
        self.assertIn("0:30", self.last_send(ctx).args[0])
        self.assertAlmostEqual(self.player.clock.position(), 30, delta=1)


if __name__ == "__main__":
    unittest.main()
