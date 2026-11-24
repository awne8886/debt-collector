<div align="center">

# 💸 Debt Collector

**A moderation, community and music bot for Discord — with a sense of humour and a very long memory.**

![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![discord.py](https://img.shields.io/badge/discord.py-2.7%2B-5865F2?logo=discord&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-47A248?logo=mongodb&logoColor=white)
![yt-dlp](https://img.shields.io/badge/music-yt--dlp%20%2B%20ffmpeg-FF0000?logo=youtube&logoColor=white)
![Hosted on Render](https://img.shields.io/badge/hosted%20on-Render-46E3B7?logo=render&logoColor=black)

*Slash commands **and** prefix commands. Per-server settings. Zero bill collectors required.*

</div>

---

## ✨ What it does

| | Feature | Highlights |
| --- | --- | --- |
| 🛡️ | **Moderation** | `ban`, `kick`, `softban`, `tempban`, `massban` (with preview), `timeout`, warnings, numbered case book, `purge` filters, `lock`, `slowmode`, `quarantine` |
| 🚨 | **Protection** | Raid mode (auto-arms on join spikes), join screening for alts / ban evasion, automod for invites, links, spam, caps and mass mentions |
| 🎶 | **Music** | YouTube & YouTube Music in voice: queue, loop, seek, volume, vote-skip, auto-leave |
| 🤖 | **AI chat** | Configurable assistant with personas, per-channel / per-user behaviour and provider fallback (OpenRouter, Gemini, Groq) |
| 🎫 | **Tickets** | Button-panel private tickets with staff roles, logs and transcripts |
| 🏷️ | **Roles** | Reaction roles, join roles, bulk role tools |
| 📣 | **Community** | Welcomer / leaver messages, starboard, polls, sticky messages, reminders, AFK, tags |
| 🔧 | **Admin tooling** | Settings export / import, modlog and message log, `/diagnose` health check, `/errors` runtime error browser |

Everything is a **hybrid command**: use `/ban` or `!ban`. The prefix is per-server (`!` by default) and changes with `/set prefix`.

---

## 🚀 Quick start

### 1. Requirements

* **Python 3.11**
* A **Discord bot token** ([Developer Portal](https://discord.com/developers/applications)). Under **Bot → Privileged Gateway Intents**, enable **Server Members Intent** and **Message Content Intent** before starting the bot, otherwise it exits at startup with `PrivilegedIntentsRequired`
* A **MongoDB** database (a free Atlas cluster is plenty) — all per-server settings live there
* For music: **ffmpeg** (bundled via `imageio-ffmpeg`) and a **JavaScript runtime** (`deno`, `node` or `bun`) on `PATH`

### 2. Install

```bash
git clone https://github.com/awne8886/debt-collector.git
cd debt-collector
pip install -r requirements.txt
```

### 3. Configure

Set these environment variables (a `.env` loader is not used, so export them or set them in your host's dashboard):

```bash
export DISCORD_TOKEN="your-bot-token"
export MONGO_URI="mongodb+srv://user:pass@cluster/db"
```

### 4. Add the bot to a server

In the [Developer Portal](https://discord.com/developers/applications), open your application and use its **Installation** (or **OAuth2**) page to generate an invite link. Include the `bot` and `applications.commands` scopes, grant the permissions the bot needs in your server (for music: **Connect** and **Speak** in the voice channel), and open the link to add it to your server.

### 5. Run

```bash
python main.py
```

Slash commands sync automatically whenever the command set changes (set `SYNC_COMMANDS=1` to force a sync on the next start).

---

## 🔑 Environment variables

### Required

| Variable | Meaning |
| --- | --- |
| `DISCORD_TOKEN` | Your bot token |
| `MONGO_URI` | MongoDB connection string |

### Optional

| Variable | Meaning |
| --- | --- |
| `DEFAULT_PREFIX` | Fallback prefix used in DMs and when a server's stored prefix is missing. New servers are saved with `!` (change it per server with the prefix command) |
| `SUPERUSER_IDS` | Comma-separated Discord user IDs with bot-owner powers (see all servers' errors, clear the error buffer) |
| `SYNC_COMMANDS` | Set to `1` to force a slash-command sync on start (otherwise it only syncs when commands change) |
| `MONGO_DB_NAME`, `MONGO_SETTINGS_COLLECTION`, `MONGO_AI_COLLECTION` | Override database / collection names |
| `OPENROUTER_API_KEY`, `GEMINI_API_KEY`, `GROQ_API_KEY` | AI providers; any subset works, the rest are skipped |
| `OPENROUTER_MODEL`, `GEMINI_MODEL`, `GROQ_MODEL` | Pick the model each provider uses |
| `LOG_LEVEL` | Logging verbosity |
| `ERROR_BUFFER_SIZE` | How many distinct errors `/errors` remembers |
| `ERROR_PERSIST` | Also store errors in MongoDB |
| `AFK_GRACE_SECONDS` | Grace period before a message cancels AFK |
| `PORT` | Port for the keep-alive web server (set automatically by Render) |

### Music

| Variable | Default | Meaning |
| --- | --- | --- |
| `YTDL_PROXY` | unset | Proxy for all YouTube traffic, e.g. `socks5://127.0.0.1:40000` or `http://user:pass@host:port` |
| `YTDL_PLAYER_CLIENTS` | yt-dlp's choice | Comma-separated YouTube clients to try, e.g. `tv_simply,web_embedded` |
| `YTDL_COOKIES_FILE` | unset | Path to a Netscape-format YouTube cookies file for yt-dlp (a read-only file is copied to a writable one) |
| `FFMPEG_PATH` | auto | Use a specific ffmpeg binary |
| `MUSIC_DEFAULT_VOLUME` | `50` | Starting volume, 0–150 |
| `MUSIC_MAX_DURATION` | `10800` | Longest single track in seconds |
| `MUSIC_MAX_PLAYLIST` | `50` | Most tracks taken from one playlist link |

---

## 🎶 Music

Play YouTube and YouTube Music in a voice channel. The bot joins the channel you are in, so just run `/play` (or `!play`) with a link or a search.

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

"Staff" means Manage Channels, Move Members or Administrator. The bot leaves 3 minutes after the queue finishes, or 2 minutes after everyone else has left. The queue holds up to 100 tracks per server. Only YouTube links are accepted (`youtube.com`, `music.youtube.com`, `youtu.be`); anything else is treated as a search or rejected.

### Music hosting requirements

* **Bot permissions:** *Connect* and *Speak* in the voice channels it should use (and it must not be server-muted).
* **PyNaCl**, **davey** (voice end-to-end encryption) and **yt-dlp** come from `requirements.txt`.
* **ffmpeg** is used from `FFMPEG_PATH`, then `PATH`, then the static build bundled by `imageio-ffmpeg`, so a plain `pip install -r requirements.txt` is enough.
* **A JavaScript runtime** (`deno`, `node` or `bun`): current yt-dlp needs one to read YouTube. The bot also looks in `/opt/render/project/.deno/bin`, `~/.deno/bin` and `~/.bun/bin` and adds them to `PATH` itself, so a Deno installed during a Render build is found even though Render doesn't put it on `PATH`.
* **libopus** is optional. Without it the bot falls back to ffmpeg's Opus output and a volume change restarts the current track from where it was.
* **YouTube bot checks:** many datacenter IPs get *"Sign in to confirm you're not a bot"*. YouTube decides this by the host's IP address, so a fix needs one of the options under [Getting past YouTube's bot check](#getting-past-youtubes-bot-check).
* Keep `yt-dlp` current (`pip install -U yt-dlp`); YouTube changes break old versions.

### 🩺 Music troubleshooting

When something goes wrong, ask the bot first. Admins can run:

| Command | Shows |
| --- | --- |
| `/errors music [lines]` | The music player's recent log: voice state, yt-dlp, the stream host, every ffmpeg message and whether audio actually flowed |
| `/errors` | Deduplicated runtime errors (music warnings and ffmpeg output land here too) |
| `/errors detail <id>` | Full traceback for one error |
| `/diagnose` | Gateway latency, database, background loops and missing permissions |

The same lines are printed to the host's logs (Render → **Logs**), tagged `[g:<server id>]`, with ffmpeg's own output prefixed `[ffmpeg pcm]` / `[ffmpeg opus]`.

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Says "Now playing" but silent, log has `NO JavaScript runtime` | yt-dlp can't solve YouTube's challenge, so the stream is refused or throttled | Install Deno in the build step (below); check the `music env:` lines at startup |
| `/errors music` shows `ffmpeg delivered NO data` | YouTube blocked or stalled the stream | Refresh `YTDL_COOKIES_FILE`, update yt-dlp |
| `ffmpeg: ... 403 Forbidden` | Expired or refused stream URL | Same as above; URLs are resolved right before playback, so it's the runtime or cookies |
| `I might not be heard: missing the Speak permission` | Channel permission | Grant *Speak*, un-mute the bot; in Stage channels invite it to speak |
| `DAVE session still not ready` | Voice encryption handshake slow | Usually clears on the next track; make sure `davey` is installed |
| Sign in to confirm you're not a bot | YouTube flags the host's IP | See [Getting past YouTube's bot check](#getting-past-youtubes-bot-check): player clients, a proxy, or cookies |
| Memory near 512 MB on Render | yt-dlp + ffmpeg at once | Only one yt-dlp run happens at a time; check the `Memory:` log lines |

---

### Getting past YouTube's bot check

"Sign in to confirm you're not a bot" means YouTube has flagged the IP address the bot
runs on. Most cloud and free hosts share flagged addresses. Try these in order:

1. **Update yt-dlp** (`pip install -U yt-dlp`) and restart. Old versions fail more often.
2. **Try other YouTube clients.** Set `YTDL_PLAYER_CLIENTS`, for example
   `tv_simply,web_embedded` or `mweb`. This costs nothing but helps only on some IPs,
   and which clients work changes as YouTube changes.
3. **Use a proxy (no cookies needed).** Set `YTDL_PROXY` and the bot sends all YouTube
   traffic, audio included, through it. The bot check then sees the proxy's IP:
   * A **residential proxy** is the most reliable. Use its `http://user:pass@host:port` URL.
   * **Cloudflare WARP** is free. Run [`wgcf`](https://github.com/ViRb3/wgcf) to make a
     WireGuard profile, then [`wireproxy`](https://github.com/whyvl/wireproxy) to expose
     it as a local SOCKS5 proxy, and set `YTDL_PROXY=socks5://127.0.0.1:<port>`. YouTube
     flags some WARP addresses too, so results vary.
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

Running the bot on a home connection instead of a datacenter also avoids the check.

The bot is at 94 of Discord's 100 global slash commands, so there are 6 left before new
commands need to be grouped.

---

## ☁️ Deploying on Render

1. Create a **Web Service** (or Background Worker) from this repo, runtime **Python 3**.
2. **Build command** — installs dependencies and Deno:

   ```bash
   pip install -r requirements.txt && curl -fsSL -o /tmp/deno-install.sh https://deno.land/install.sh && DENO_INSTALL=/opt/render/project/.deno sh /tmp/deno-install.sh
   ```

3. **Start command:** `python main.py`
4. Add `DISCORD_TOKEN` and `MONGO_URI` (plus any optional variables) under **Environment**.
5. For YouTube cookies, add a **Secret File** and set `YTDL_COOKIES_FILE` to its path.

The bot runs a small keep-alive web server on `$PORT`, so Render's health checks pass.

---

## 🗂️ Project layout

```
debt-collector/
├── main.py            # Entry point: the bot, commands, help, /errors, /diagnose
├── bot.py             # Smaller standalone variant (not used by the Render start command)
├── cogs/
│   └── music.py       # Music cog: queue, voice, playback, ffmpeg wiring, logging
├── utils/
│   ├── music.py       # Query parsing, queue, playback clock, formatting
│   ├── ytdl.py        # yt-dlp / ffmpeg helpers, PATH fix-ups, diagnostics
│   └── pagination.py  # Shared paginator UI
├── tests/             # Unit tests
└── requirements.txt
```

---

## 🧪 Tests

```bash
python -m unittest tests.test_music
```

`tests/test_main.py` imports `main.py`, so it needs `MONGO_URI` set to run.

---

<div align="center">

Built with [discord.py](https://github.com/Rapptz/discord.py), [yt-dlp](https://github.com/yt-dlp/yt-dlp) and [imageio-ffmpeg](https://github.com/imageio/imageio-ffmpeg).

</div>
