"""Gateway event overrides, including the ordered on_message pipeline. Imported last."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord

from core.config import SnipedMessage
from core.bot import bot
from core.helpers import _extract_message_text
from features.help import _uncategorised_roots
from features.counting import handle_counting, handle_counting_retraction
from features.utility import _handle_afk_mentions, _handle_afk_return
from features.automations import _apply_guild_automations
from features.ai import _handle_ai
from features.sticky import _handle_sticky
from features.automod import _handle_automod
from features.messagelog import _attachments_to_cache, _cache_single_attachment

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)
    stray: List[str] = _uncategorised_roots()
    if stray:
        log.warning(
            "%d command(s) missing from COMMAND_CATEGORY and shown as uncategorised: %s",
            len(stray),
            ", ".join(stray),
        )


@bot.event
async def on_guild_remove(guild: discord.Guild) -> None:
    bot.settings._cache.pop(guild.id, None)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message) -> None:
    if before.guild is None or before.author.bot:
        return
    if before.content == after.content:
        return
    try:
        await handle_counting_retraction(before, after, "edited")
    except Exception as exc:
        log.error("on_message_edit counting error: %s", exc)


@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if message.guild is None or message.author.bot:
        return
    try:
        bot.snipes[message.channel.id] = SnipedMessage(
            author=str(message.author),
            author_avatar=(
                message.author.display_avatar.url
                if message.author.display_avatar
                else None
            ),
            content=message.content or "",
            created_at=message.created_at,
            deleted_at=datetime.now(timezone.utc),
        )
    except discord.DiscordException as exc:
        log.error("on_message_delete error: %s", exc)

    try:
        await handle_counting_retraction(message, message, "deleted")
    except Exception as exc:
        log.error("on_message_delete counting error: %s", exc)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild is None:
        return

    try:
        if not message.author.bot:
            await _handle_afk_return(message)
            await _handle_afk_mentions(message)

        for attachment in _attachments_to_cache(message):
            bot.spawn(
                _cache_single_attachment(message.id, attachment),
                name=f"attachment-cache:{message.id}",
            )

        settings: Dict[str, Any] = bot.settings.peek_settings(message.guild.id)
        autopurge: Dict[str, Any] = settings.get("autopurge") or {
            "channels": {},
            "exempt_roles": [],
            "users": {},
        }
        entry: Optional[Dict[str, Any]] = (autopurge.get("channels") or {}).get(
            str(message.channel.id)
        )

        if entry and message.author.id != bot.user.id:
            until: Optional[float] = entry.get("until")
            if until and time.time() > until:
                channels: Dict[str, Any] = dict(autopurge.get("channels") or {})
                channels.pop(str(message.channel.id), None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.channels": channels}
                )
            elif not any(
                role.id in (autopurge.get("exempt_roles") or [])
                for role in getattr(message.author, "roles", [])
            ):
                try:
                    await message.delete()
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge", exc, guild=message.guild, user=message.author
                    )
                return

        msg_entries = autopurge.get("messages") or {}
        for key, m_entry in list(msg_entries.items()):
            if message.author.id == bot.user.id:
                break

            until: Optional[float] = m_entry.get("until")
            if until and time.time() > until:
                msgs_dict = dict(autopurge.get("messages") or {})
                msgs_dict.pop(key, None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.messages": msgs_dict}
                )
                continue

            txt = m_entry.get("text", "")
            mtype = m_entry.get("match_type", "contains")
            content_lower = _extract_message_text(message)
            needle = txt.casefold()

            is_match = False
            if mtype == "starts_with" and content_lower.startswith(needle):
                is_match = True
            elif mtype == "ends_with" and content_lower.endswith(needle):
                is_match = True
            elif mtype == "contains" and needle in content_lower:
                is_match = True

            if is_match:
                try:
                    await message.delete()
                    return  # Stop processing this message since it's deleted
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge_msg", exc, guild=message.guild, user=message.author
                    )

        user_entry = (autopurge.get("users") or {}).get(str(message.author.id))
        if user_entry and message.author.id != bot.user.id:
            until: Optional[float] = user_entry.get("until")
            if until and time.time() > until:
                users_dict = dict(autopurge.get("users") or {})
                users_dict.pop(str(message.author.id), None)
                await bot.settings.push_fields(
                    message.guild.id, {"autopurge.users": users_dict}
                )
            elif str(message.channel.id) not in (
                user_entry.get("exempt_channels") or []
            ):
                try:
                    await message.delete()
                except discord.HTTPException as exc:
                    bot.log_error(
                        "autopurge_user", exc, guild=message.guild, user=message.author
                    )
                return

        if message.author.bot:
            return

        if await _handle_automod(message):
            return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        bot.log_error("on_message:gate", exc, guild=message.guild, user=message.author)

    # Commands dispatch before any long-running enrichment (AI calls take seconds).
    await bot.process_commands(message)

    async def _enrich() -> None:
        try:
            await _apply_guild_automations(message)
            await _handle_sticky(message)
            await _handle_ai(message)
            await handle_counting(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.log_error(
                "on_message:enrich", exc, guild=message.guild, user=message.author
            )

    bot.spawn(_enrich(), name=f"enrich:{message.id}")
