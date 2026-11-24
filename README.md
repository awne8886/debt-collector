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
  YouTube decides this by the host's IP address, so a fix needs one of the options under
  [Getting past YouTube's bot check](#getting-past-youtubes-bot-check).
* Keep `yt-dlp` current (`pip install -U yt-dlp`); YouTube changes break old versions.
  Hosts that cache packages between builds (Render) keep an old copy until the minimum
  version in `requirements.txt` is raised or the build cache is cleared. The bot logs
  the yt-dlp version it is running the first time it uses YouTube.

### Optional environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `YTDL_PROXY` | unset | Proxy for all YouTube traffic, e.g. `socks5://127.0.0.1:40000` or `http://user:pass@host:port` |
| `YTDL_PLAYER_CLIENTS` | yt-dlp's choice | Comma-separated YouTube clients to try, e.g. `tv_simply,web_embedded` |
| `YTDL_COOKIES_FILE` | unset | Cookies file passed to yt-dlp |
| `FFMPEG_PATH` | auto | Path to an ffmpeg binary |
| `MUSIC_DEFAULT_VOLUME` | `50` | Starting volume, 0-150 |
| `MUSIC_MAX_DURATION` | `10800` | Longest single track in seconds |
| `MUSIC_MAX_PLAYLIST` | `50` | Most tracks taken from one playlist link |

The queue holds up to 100 tracks per server.

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
