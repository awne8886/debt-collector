# debt-collector
bot

## Music

Play YouTube and YouTube Music in a voice channel. The bot joins the channel you are
in, so just run `/play` (or `!play`) with a link or a search.

| Command | What it does |
| --- | --- |
| `/play <link or search>` (`!p`) | Play a YouTube / YouTube Music video or playlist link, or the top search result |
| `/playnext <link or search>` (`!pn`) | Same, but queued to play right after the current track |
| `/pause`, `/resume` | Pause / resume |
| `/skip` (`!s`) | Skip. The requester and staff skip instantly, everyone else votes (majority of listeners) |
| `/stop` | Stop and clear the queue, staying in the channel |
| `/leave` (`!dc`) | Disconnect and clear the queue |
| `/nowplaying` (`!np`) | Current track with a progress bar |
| `/queue` (`!q`) | Show the queue |
| `/queue shuffle`, `/queue move <from> <to>` | Reorder waiting tracks |
| `/queue remove <n>` | Remove a track (its requester, or staff) |
| `/queue clear`, `/queue skipto <n>` | Staff, or the only listener |
| `/volume [0-150]` (`!vol`) | Show or set the volume (default 50) |
| `/loop [off\|track\|queue]` | Loop the track or the queue; no argument cycles through the modes |
| `/seek <time>` | Jump within the track: `1:30`, `90` or `1m30s` |

"Staff" means Manage Channels, Move Members or Administrator. The bot leaves 3 minutes
after the queue finishes, or 2 minutes after everyone else has left.

Only YouTube links are accepted (`youtube.com`, `music.youtube.com`, `youtu.be`);
anything else is treated as a search or rejected.

### Hosting requirements

* **Bot permissions:** Connect and Speak in the voice channels it should use.
* **PyNaCl**, **davey** and **yt-dlp** come from `requirements.txt`. `davey` (Discord's voice
  encryption, DAVE) is pinned below 0.2: with 0.2 and discord.py 2.7.1 the bot shows as
  speaking but nobody hears it. The bot logs a warning at startup if that combination
  comes back, and logs `DAVE ... ready=` for each track it plays.
* **ffmpeg:** used from `FFMPEG_PATH`, then `PATH`, then the static build bundled by
  `imageio-ffmpeg`, so a plain `pip install -r requirements.txt` is enough.
* **A JavaScript runtime** (`deno`, `node` or `bun`) must be on `PATH`: current yt-dlp
  needs one to read YouTube. Whichever is installed is used automatically.
* **libopus** is optional. Without it the bot falls back to ffmpeg's Opus output and a
  volume change restarts the current track from where it was.
* **YouTube bot checks:** many datacenter IPs get "Sign in to confirm you're not a bot".
  YouTube decides this by the host's IP address, so a fix needs one of the options under
  [Getting past YouTube's bot check](#getting-past-youtubes-bot-check).
* Keep `yt-dlp` current (`pip install -U yt-dlp`); YouTube changes break old versions.
  Hosts that cache packages between builds (Render) keep an old copy until the minimum
  version in `requirements.txt` is raised or the build cache is cleared. The bot logs
  the yt-dlp version it is running the first time it uses YouTube.

### Optional environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `YTDL_PROXY` | unset | Proxy for all YouTube traffic, e.g. `http://user:pass@host:port`. Use `http://`: ffmpeg can't fetch audio through `socks5://` |
| `YTDL_PLAYER_CLIENTS` | yt-dlp's choice | YouTube clients for the first attempt, e.g. `tv_simply,web_embedded` |
| `YTDL_CLIENT_ROTATION` | `on` | `off` uses only the first attempt (see [How YouTube playback works](#how-youtube-playback-works)) |
| `YTDL_ATTEMPT_BUDGET` | `75` | Seconds after which no further client groups are tried for one request |
| `YTDL_COOKIES_FILE` | unset | Cookies file passed to yt-dlp |
| `YTDL_CACHE_DIR` | temp folder | Where yt-dlp caches YouTube's player code between plays |
| `YTDL_WARP` | `auto` | Cloudflare WARP for YouTube: `auto` when YouTube blocks the host's IP, `on` always, `off` never (see below) |
| `WARP_IPV6` | off | `1` also sends IPv6 through WARP. Off by default: YouTube treats WARP's IPv6 exits as region-locked and bot-checks them more |
| `WARP_DIR` | temp folder | Where the WARP registration and the `wireproxy` binary are kept |
| `WIREPROXY_PATH` | download | Use this `wireproxy` binary instead of downloading the pinned release |
| `MUSIC_WARMUP` | `on` | `off` skips the YouTube check the bot runs once at startup |
| `PREVIEW_RUN_BOT` | unset | `1` lets a Render pull-request preview log in to Discord (see below) |
| `FFMPEG_PATH` | auto | Path to an ffmpeg binary |
| `MUSIC_DEFAULT_VOLUME` | `50` | Starting volume, 0-150 |
| `MUSIC_MAX_DURATION` | `10800` | Longest single track in seconds |
| `MUSIC_MAX_PLAYLIST` | `50` | Most tracks taken from one playlist link |

The queue holds up to 100 tracks per server.

### How YouTube playback works

YouTube answers its different apps ("clients": web, TV, iOS, Vision Pro, Android VR, ...)
differently, and which ones work depends on the host's IP address, on whether cookies
are sent, and on YouTube's changes from week to week. Like the Lavalink YouTube plugin
most large music bots use, the bot doesn't rely on one client:

1. It first asks with `visionos, ios_producer, android_vr` without cookies: they need no
   JavaScript, so yt-dlp doesn't start deno, which is fast and keeps memory low (deno
   can push a 512 MB Render instance over its limit). `YTDL_PLAYER_CLIENTS`, if set,
   goes first instead.
2. If that gives no playable audio, it tries yt-dlp's own choice (with your cookies),
   then with cookies `tv_samsung, tv, web_embedded, mweb, web_music`, then without
   cookies `tv_samsung, web_embedded, mweb, tv_simply, web_music`. Cookie-less clients
   only work in a request without cookies, so those attempts drop them.
3. Whichever group worked is tried first from then on, and the log says
   `YouTube is answering through ...` when that changes.
4. If a track stops within seconds of starting (YouTube refused the stream), that group
   is set aside and the track is restarted once with the next one.

At startup the bot resolves one known video in the background (switching to WARP if
YouTube blocks the host) and settles on a group that plays quickly, so the first `/play`
doesn't wait through failing clients. Each failed attempt is logged with yt-dlp's
reasons (`yt-dlp failed for ... via ...`), which is the first place to look when music
stops working.

**Render pull-request previews** copy the production environment, bot token included.
A preview therefore doesn't log in to Discord (two copies of the bot would answer every
command and keep kicking each other out of voice). Instead it tests every YouTube client
from Render's network, including fetching a few seconds of audio with ffmpeg, and logs
lines starting with `probe OK` / `probe FAIL` and a `YouTube probe finished` summary. Set
`PREVIEW_RUN_BOT=1` on a preview to run the bot there anyway.

### Built-in Cloudflare WARP (free)

Many hosts, Render included, have IP addresses YouTube refuses outright (`HTTP Error 403`
on its API, or "Sign in to confirm you're not a bot" for every client). When the bot
sees that, it routes YouTube through [Cloudflare WARP](https://one.one.one.one/), a free
VPN, by itself:

1. It registers a free WARP device with Cloudflare (a new WireGuard key, made locally).
2. It downloads [`wireproxy`](https://github.com/windtf/wireproxy) v1.1.3, a userspace
   WireGuard client that needs no root, and checks it against the release's SHA-256.
3. It runs it as an HTTP proxy on `127.0.0.1` and confirms through Cloudflare's trace page
   that traffic really leaves through WARP.

yt-dlp and ffmpeg then both use the tunnel, so the audio comes from the same IP as the
lookup. The log shows `WARP is up` with the exit location. Nothing needs configuring; set
`YTDL_WARP=off` to disable it, or `on` to use it from the start. `YTDL_PROXY` always takes
precedence. YouTube blocks some WARP addresses too; if it does, a residential proxy
(below) is the next step.

### Getting past YouTube's bot check

"Sign in to confirm you're not a bot" means YouTube has flagged the IP address the bot
runs on. Most cloud and free hosts share flagged addresses. Try these in order:

1. **Update yt-dlp** (`pip install -U yt-dlp`) and restart. Old versions fail more often.
2. **Let the client rotation work.** The bot already tries the clients listed under
   [How YouTube playback works](#how-youtube-playback-works). To put a specific group
   first, set `YTDL_PLAYER_CLIENTS`.
3. **Use a proxy (no cookies needed).** Set `YTDL_PROXY` and the bot sends all YouTube
   traffic, audio included, through it. The bot check then sees the proxy's IP. YouTube
   ties each audio URL to the IP that asked for it, and ffmpeg (which fetches the audio)
   only supports HTTP proxies, so give an `http://` proxy URL:
   * A **residential proxy** is the most reliable. Use its `http://user:pass@host:port` URL.
   * **Cloudflare WARP** is free. Run [`wgcf`](https://github.com/ViRb3/wgcf) to make a
     WireGuard profile, then [`wireproxy`](https://github.com/whyvl/wireproxy) to expose
     it as a local HTTP proxy (an `[http]` section), and set
     `YTDL_PROXY=http://127.0.0.1:<port>`. YouTube flags some WARP addresses too, so
     results vary.
   * A home PC or Raspberry Pi running a small proxy (for example `microsocks`) also works,
     because home IPs are rarely flagged.
4. **Proof-of-origin tokens.** Install the
   [`bgutil-ytdlp-pot-provider`](https://github.com/Brainicism/bgutil-ytdlp-pot-provider)
   yt-dlp plugin and run its token server. yt-dlp picks the plugin up on its own. This helps
   with YouTube 403 errors and on lightly flagged IPs, but it won't clear a hard IP block.
5. **Cookies** (`YTDL_COOKIES_FILE`, Netscape format). Use a throwaway Google account:
   YouTube can ban accounts it sees used this way. yt-dlp updates the file as it goes;
   if the file is read-only, the bot copies it to the temp folder and uses the copy.
   * **Export:** sign in to YouTube in a private window, export the youtube.com cookies
     in Netscape format (for example with the Cookie-Editor extension), then close the
     window without signing out, so the browser doesn't rotate the exported cookies.
   * **Render:** add the file under the service's Environment > Secret Files as
     `cookies.txt`, and set `YTDL_COOKIES_FILE=/etc/secrets/cookies.txt`.
   * Cookies expire. When the bot check comes back, export a fresh file.
   * Cookies can also make things worse: with them, yt-dlp can't use the clients that
     don't take cookies, and YouTube may restrict a flagged account. The rotation also
     tries without cookies, so playback can still work when the cookies don't.

Running the bot on a home connection instead of a datacenter also avoids the check.

The bot is at 94 of Discord's 100 global slash commands, so there are 6 left before new
commands need to be grouped.
