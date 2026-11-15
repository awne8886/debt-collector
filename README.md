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
* **PyNaCl** and **yt-dlp** come from `requirements.txt`.
* **ffmpeg:** used from `FFMPEG_PATH`, then `PATH`, then the static build bundled by
  `imageio-ffmpeg`, so a plain `pip install -r requirements.txt` is enough.
* **A JavaScript runtime** (`deno`, `node` or `bun`) must be on `PATH`: current yt-dlp
  needs one to read YouTube. Whichever is installed is used automatically.
* **libopus** is optional. Without it the bot falls back to ffmpeg's Opus output and a
  volume change restarts the current track from where it was.
* **YouTube bot checks:** many datacenter IPs get "Sign in to confirm you're not a bot".
  If that happens, export your YouTube cookies (Netscape format) to a file on the host
  and set `YTDL_COOKIES_FILE` to its path. Keep it somewhere writable, yt-dlp updates it.
* Keep `yt-dlp` current (`pip install -U yt-dlp`); YouTube changes break old versions.

### Optional environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `YTDL_COOKIES_FILE` | unset | Cookies file passed to yt-dlp |
| `FFMPEG_PATH` | auto | Path to an ffmpeg binary |
| `MUSIC_DEFAULT_VOLUME` | `50` | Starting volume, 0-150 |
| `MUSIC_MAX_DURATION` | `10800` | Longest single track in seconds |
| `MUSIC_MAX_PLAYLIST` | `50` | Most tracks taken from one playlist link |

The queue holds up to 100 tracks per server.

The bot is at 94 of Discord's 100 global slash commands, so there are 6 left before new
commands need to be grouped.
