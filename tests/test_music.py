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
import utils.ytdl as ytdl
from utils.ytdl import (
    ExtractionError,
    StreamInfo,
    Strategy,
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
        self.assertIn("no playable audio", friendly_error("ERROR: Requested format is not available"))
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

    def test_ffmpeg_uses_the_same_http_proxy(self):
        from utils.ytdl import _warn_socks_once, ffmpeg_input_options

        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(ffmpeg_input_options(), [])
        with patch.dict("os.environ", {"YTDL_PROXY": "http://u:p@host:8080"}, clear=True):
            self.assertEqual(ffmpeg_input_options(), ["-http_proxy", "http://u:p@host:8080"])
        _warn_socks_once.cache_clear()
        with patch.dict("os.environ", {"YTDL_PROXY": "socks5://127.0.0.1:40000"}, clear=True), \
                self.assertLogs("debt-collector.music", "WARNING"):
            self.assertEqual(ffmpeg_input_options(), [])
        _warn_socks_once.cache_clear()

    def test_region_lock_is_final_and_explained(self):
        message = "ERROR: [youtube] x: The uploader has not made this video available in your country"
        self.assertTrue(ytdl._is_final(message))
        self.assertIn("region-locked", friendly_error(message))

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


class FakeYoutubeDL:
    """Stands in for yt_dlp.YoutubeDL; `behaviour` maps a client tuple to a result or error."""

    behaviour: dict = {}
    calls: list = []

    def __init__(self, options):
        self.options = options

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_info(self, target, download=False, process=True):
        import yt_dlp

        clients = tuple(self.options.get("extractor_args", {}).get("youtube", {}).get("player_client", ()))
        cookies = "cookiefile" in self.options
        FakeYoutubeDL.calls.append((clients, cookies))
        result = FakeYoutubeDL.behaviour.get((clients, cookies), "ERROR: Requested format is not available")
        if isinstance(result, str):
            raise yt_dlp.utils.DownloadError(result)
        return result


class TestClientRotation(unittest.TestCase):
    INFO = {"id": VID, "title": "Song", "duration": 200, "url": "https://audio.example/a"}

    def setUp(self):
        ytdl._preferred = None
        ytdl._stream_cache.clear()
        FakeYoutubeDL.calls = []
        FakeYoutubeDL.behaviour = {}
        self.ydl_patch = patch("yt_dlp.YoutubeDL", FakeYoutubeDL)
        self.ydl_patch.start()
        self.cookie_patch = patch("utils.ytdl._cookie_file", return_value="/tmp/cookies.txt")
        self.cookie_patch.start()
        self.writable_patch = patch("utils.ytdl._writable_cookie_file", side_effect=lambda p: p)
        self.writable_patch.start()

    def tearDown(self):
        self.ydl_patch.stop()
        self.cookie_patch.stop()
        self.writable_patch.stop()
        ytdl._preferred = None
        ytdl._stream_cache.clear()

    def test_strategy_order_with_cookies(self):
        with patch.dict("os.environ", {}, clear=True):
            order = ytdl.strategies()
        self.assertEqual(order, [
            Strategy(ytdl._JSLESS_CLIENTS, False),  # no deno: fast and light
            Strategy((), True),
            Strategy(ytdl._COOKIE_CLIENTS, True),
            Strategy(ytdl._JS_CLIENTS, False),
        ])
        with patch.dict("os.environ", {"YTDL_PLAYER_CLIENTS": "mweb"}, clear=True):
            self.assertEqual(ytdl.strategies()[0], Strategy(("mweb",), True))
        with patch.dict("os.environ", {"YTDL_CLIENT_ROTATION": "off"}, clear=True):
            self.assertEqual(ytdl.strategies(), [Strategy((), True)])

    def test_falls_back_to_cookieless_clients_and_remembers(self):
        FakeYoutubeDL.behaviour = {(ytdl._COOKIE_CLIENTS, True): dict(self.INFO)}
        track = make_track("song")
        with patch.dict("os.environ", {}, clear=True), self.assertLogs("debt-collector.music", "INFO"):
            stream = ytdl.resolve_stream(track)
        self.assertEqual(stream.url, "https://audio.example/a")
        self.assertEqual(
            FakeYoutubeDL.calls,
            [(ytdl._JSLESS_CLIENTS, False), ((), True), (ytdl._COOKIE_CLIENTS, True)],
        )
        FakeYoutubeDL.calls = []
        with patch.dict("os.environ", {}, clear=True):
            ytdl.resolve_stream(track)
        self.assertEqual(FakeYoutubeDL.calls, [(ytdl._COOKIE_CLIENTS, True)])

    def test_final_errors_stop_the_rotation(self):
        FakeYoutubeDL.behaviour = {
            (ytdl._JSLESS_CLIENTS, False): "ERROR: [youtube] x: Private video. Sign in"
        }
        with patch.dict("os.environ", {}, clear=True), self.assertLogs("debt-collector.music", "WARNING"):
            with self.assertRaises(ExtractionError) as caught:
                ytdl.resolve_stream(make_track("song"))
        self.assertIn("private", str(caught.exception))
        self.assertEqual(len(FakeYoutubeDL.calls), 1)

    def test_result_without_audio_moves_on(self):
        FakeYoutubeDL.behaviour = {
            (ytdl._JSLESS_CLIENTS, False): {"id": VID, "title": "Song"},  # only images
            ((), True): dict(self.INFO),
        }
        with patch.dict("os.environ", {}, clear=True), self.assertLogs("debt-collector.music", "INFO"):
            stream = ytdl.resolve_stream(make_track("song"))
        self.assertEqual(stream.url, "https://audio.example/a")

    def test_video_lookup_caches_the_stream_once(self):
        from utils.music import ParsedQuery

        FakeYoutubeDL.behaviour = {(ytdl._JSLESS_CLIENTS, False): dict(self.INFO)}
        with patch.dict("os.environ", {}, clear=True):
            resolved = ytdl.resolve_tracks(ParsedQuery(QueryKind.VIDEO, CANON), 1, "me")
            track = resolved.tracks[0]
            self.assertEqual(track.duration, 200)
            ytdl.resolve_stream(track)
            self.assertEqual(len(FakeYoutubeDL.calls), 1)  # served from the cache
            ytdl.resolve_stream(track)
            self.assertEqual(len(FakeYoutubeDL.calls), 2)  # cache entries are single-use

    def test_warm_up_prefers_the_fastest_working_group(self):
        FakeYoutubeDL.behaviour = {
            ((), True): dict(self.INFO),
            (ytdl._JSLESS_CLIENTS, False): dict(self.INFO),
        }
        clock = {"now": 0.0}

        def slow_for_cookies(*_):
            return clock["now"]

        original = FakeYoutubeDL.extract_info

        def timed(self_, target, download=False, process=True):
            cookies = "cookiefile" in self_.options
            clock["now"] += 15.0 if cookies else 3.0
            return original(self_, target, download, process)

        with patch.dict("os.environ", {"YTDL_WARP": "off"}, clear=True), \
                patch.object(FakeYoutubeDL, "extract_info", timed), \
                patch("utils.ytdl.time.monotonic", side_effect=slow_for_cookies), \
                patch("utils.ytdl._check_playable", return_value=None), \
                self.assertLogs("debt-collector.music", "INFO"):
            chosen = ytdl.warm_up()
        self.assertEqual(chosen, Strategy(ytdl._JSLESS_CLIENTS, False))
        # The 3 s group was fast enough, so the slow cookie groups weren't timed.
        timed_after_warm = [c for c in FakeYoutubeDL.calls[1:]]
        self.assertEqual(timed_after_warm, [(ytdl._JSLESS_CLIENTS, False)])

    def test_all_failing_reports_first_error(self):
        with patch.dict("os.environ", {}, clear=True), self.assertLogs("debt-collector.music", "WARNING"):
            with self.assertRaises(ExtractionError) as caught:
                ytdl.resolve_stream(make_track("song"))
        self.assertIn("no playable audio", str(caught.exception))
        self.assertEqual(len(FakeYoutubeDL.calls), len(ytdl.strategies()))


class TestWarp(unittest.TestCase):
    def setUp(self):
        from utils import warp

        self.warp = warp
        warp._process = None
        warp._proxy = None
        warp._failed_at = 0.0

    def test_modes_and_availability(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(self.warp.mode(), "auto")
        with patch.dict("os.environ", {"YTDL_WARP": "off"}, clear=True):
            self.assertFalse(self.warp.available())
        with patch.dict("os.environ", {"YTDL_WARP": "1"}, clear=True):
            self.assertEqual(self.warp.mode(), "on")
        with patch.dict("os.environ", {"YTDL_PROXY": "http://p:1"}, clear=True):
            self.assertFalse(self.warp.available())  # an explicit proxy always wins
        with patch.dict("os.environ", {}, clear=True), patch("utils.warp._machine", return_value="mips"):
            self.assertFalse(self.warp.available())

    def test_registration_and_config(self):
        import json
        import tempfile

        registration = {
            "id": "dev", "token": "tok",
            "config": {
                "peers": [{"public_key": "PEERKEY=", "endpoint": {
                    "v4": "162.159.192.8:0", "host": "engage.cloudflareclient.com:2408"}}],
                "interface": {"addresses": {"v4": "172.16.0.2", "v6": "2606:4700::1"}},
            },
        }
        calls = []

        def fake_api(method, path, body, token=""):
            calls.append((method, path))
            return registration if method == "POST" else {}

        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"WARP_DIR": tmp}), \
                patch("utils.warp._api", side_effect=fake_api):
            account = self.warp._account()
            again = self.warp._account()  # kept, not registered twice
            with open(f"{tmp}/account.json") as handle:
                self.assertEqual(json.load(handle), account)
        self.assertEqual(calls, [("POST", "/reg"), ("PATCH", "/reg/dev")])
        self.assertEqual(again, account)
        self.assertEqual(account["endpoint"], "162.159.192.8:2408")
        config = self.warp._wireproxy_config(account, 4000)
        self.assertIn("Address = 172.16.0.2/32\n", config)  # IPv4 only by default
        self.assertIn("PublicKey = PEERKEY=", config)
        self.assertIn("[http]\nBindAddress = 127.0.0.1:4000", config)
        self.assertEqual(len(__import__("base64").b64decode(account["private_key"])), 32)

    def test_download_must_match_the_pinned_checksum(self):
        import io
        import tempfile

        fake = MagicMock()
        fake.__enter__.return_value = io.BytesIO(b"not the real archive")
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"WARP_DIR": tmp}), \
                patch("utils.warp._machine", return_value="x86_64"), \
                patch("utils.warp.urllib.request.urlopen", return_value=fake):
            with self.assertRaises(self.warp.WarpError) as caught:
                self.warp._wireproxy_binary()
        self.assertIn("SHA-256", str(caught.exception))

    def test_tunnel_is_ipv4_only_unless_asked(self):
        account = {"private_key": "K", "peer_public_key": "P", "address_v4": "172.16.0.2",
                   "address_v6": "2606::1", "endpoint": "1.2.3.4:2408"}
        with patch.dict("os.environ", {}, clear=True):
            config = self.warp._wireproxy_config(account, 1)
        self.assertIn("Address = 172.16.0.2/32\n", config)
        self.assertIn("AllowedIPs = 0.0.0.0/0\n", config)
        with patch.dict("os.environ", {"WARP_IPV6": "1"}, clear=True):
            config = self.warp._wireproxy_config(account, 1)
        self.assertIn("2606::1/128", config)

    def test_failed_start_is_not_retried_at_once(self):
        with patch.dict("os.environ", {}, clear=True), \
                patch("utils.warp.available", return_value=True), \
                patch("utils.warp._start", side_effect=self.warp.WarpError("no udp")) as start, \
                self.assertLogs("debt-collector.music", "WARNING"):
            self.assertIsNone(self.warp.ensure_started())
            self.assertIsNone(self.warp.ensure_started())
        start.assert_called_once()


class TestWarpFallback(unittest.TestCase):
    """When YouTube refuses the host's IP, the rotation hands over to WARP."""

    INFO = TestClientRotation.INFO
    setUp = TestClientRotation.setUp
    tearDown = TestClientRotation.tearDown

    def test_ip_block_switches_to_warp_and_retries(self):
        bot_check = "ERROR: [youtube] x: Sign in to confirm you're not a bot"
        state = {"warp": None}

        class RoutedYoutubeDL(FakeYoutubeDL):
            def extract_info(self, target, download=False, process=True):
                if state["warp"] is None:
                    import yt_dlp

                    FakeYoutubeDL.calls.append(("direct",))
                    raise yt_dlp.utils.DownloadError(bot_check)
                return super().extract_info(target, download, process)

        def start():
            state["warp"] = "http://127.0.0.1:9"
            return state["warp"]

        FakeYoutubeDL.behaviour = {(ytdl._JSLESS_CLIENTS, False): dict(self.INFO)}
        with patch("yt_dlp.YoutubeDL", RoutedYoutubeDL), \
                patch.dict("os.environ", {}, clear=True), \
                patch("utils.warp.available", return_value=True), \
                patch("utils.warp.proxy_url", side_effect=lambda: state["warp"]), \
                patch("utils.warp.ensure_started", side_effect=start), \
                self.assertLogs("debt-collector.music", "INFO"):
            stream = ytdl.resolve_stream(make_track("song"))
            options = ytdl._base_options()
            self.assertEqual(ytdl.ffmpeg_input_options(), ["-http_proxy", "http://127.0.0.1:9"])
        self.assertEqual(stream.url, "https://audio.example/a")
        self.assertEqual(FakeYoutubeDL.calls[0], ("direct",))  # one direct try, then WARP
        self.assertEqual(options["proxy"], "http://127.0.0.1:9")

    def test_rotation_continues_when_warp_cannot_start(self):
        import yt_dlp

        FakeYoutubeDL.behaviour = {
            (ytdl._JSLESS_CLIENTS, False): "ERROR: [youtube] x: Sign in to confirm you're not a bot",
            ((), True): dict(self.INFO),
        }
        with patch.dict("os.environ", {}, clear=True), \
                patch("utils.warp.available", return_value=True), \
                patch("utils.warp.proxy_url", return_value=None), \
                patch("utils.warp.ensure_started", return_value=None) as start, \
                self.assertLogs("debt-collector.music", "INFO"):
            stream = ytdl.resolve_stream(make_track("song"))
        start.assert_called_once()
        self.assertEqual(stream.url, "https://audio.example/a")


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
        self.player.clock.start(99)  # it played to the end
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 2)
        self.assertIs(self.player.queue.current, b)
        self.player.clock.start(99)
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
        # Each track ends immediately (position ~0s on a 100s track), is restarted once
        # through other clients, dies again, and only then counts as a failure.
        with patch("cogs.music.stream_failed") as demote:
            for expected_plays in (2, 3, 4, 5, 6):
                self.vc.finish_naturally()
                await self.until(lambda: self.vc.play_calls == expected_plays)
            self.vc.finish_naturally()
            await self.until(lambda: self.player.queue.current is None)
        self.assertEqual(demote.call_count, 3)
        self.assertEqual(self.vc.play_calls, 6)
        self.assertEqual(len(self.player.queue), 0)
        sent = " ".join(str(c) for c in self.channel.send.await_args_list)
        self.assertIn("Three tracks in a row", sent)

    async def test_normal_length_playback_resets_the_failure_count(self):
        self.player.queue.add_many([make_track(f"t{i}") for i in range(4)])
        await self.player.ensure_playing()
        with patch("cogs.music.stream_failed"):
            self.vc.finish_naturally()  # dies at once: restarted
            await self.until(lambda: self.vc.play_calls == 2)
            self.vc.finish_naturally()  # dies again: counted, next track
            await self.until(lambda: self.vc.play_calls == 3)
        self.assertEqual(self.player._quick_failures, 1)
        self.player.clock.start(50)  # pretend the second track really played for a while
        self.vc.finish_naturally()
        await self.until(lambda: self.vc.play_calls == 4)
        self.assertEqual(self.player._quick_failures, 0)

    async def test_instant_death_restarts_the_same_track_once(self):
        a, b = make_track("a"), make_track("b")
        self.player.queue.add_many([a, b])
        await self.player.ensure_playing()
        with patch("cogs.music.stream_failed") as demote:
            self.vc.finish_naturally()
            await self.until(lambda: self.vc.play_calls == 2)
        demote.assert_called_once()
        self.assertIs(self.player.queue.current, a)
        self.assertEqual(a.stream_retries, 1)
        self.assertEqual(self.player._quick_failures, 0)
        self.channel.send.assert_not_awaited()  # a silent recovery

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
