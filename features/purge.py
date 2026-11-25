"""The `purge` command group and its helpers."""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Literal, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core.permissions import member_has_perms
from core.bot import bot
from core.helpers import _extract_message_text

log: logging.Logger = logging.getLogger("debt-collector")


PURGE_HARD_LIMIT: int = 500
PURGE_SCAN_CEILING: int = 1000
BULK_DELETE_AGE_DAYS: int = 14
LINK_RE = re.compile(r"(https?://\S+|discord\.gg/\S+)", re.IGNORECASE)


async def _check_purge_permissions(ctx: commands.Context) -> bool:
    """Check if the user and bot have the required permissions for purging."""
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_messages=True
    ):
        await ctx.send("❌ You need Manage Messages permission.", ephemeral=True)
        return False
    if not ctx.channel.permissions_for(ctx.me).manage_messages:
        await ctx.send(
            "❌ I need the **Manage Messages** permission in this channel.",
            ephemeral=True,
        )
        return False
    return True


async def _gather_purge_messages(
    ctx: commands.Context,
    amount: int,
    check: Optional[Callable[[discord.Message], bool]],
    cutoff: datetime,
    invoking_id: int,
) -> Tuple[List[discord.Message], bool, int]:
    """Scan channel history to gather messages to delete."""
    scan_limit: int = (
        min(max(amount * 6, 100), PURGE_SCAN_CEILING) if check is not None else amount
    )

    matched: List[discord.Message] = []
    hit_age_limit: bool = False
    skipped_pins: int = 0

    try:
        async for message in ctx.channel.history(limit=scan_limit):
            if message.created_at <= cutoff:
                hit_age_limit = True
                break
            if message.id == invoking_id:
                continue
            if message.pinned:
                skipped_pins += 1
                continue
            if check is not None and not check(message):
                continue
            matched.append(message)
            if len(matched) >= amount:
                break
    except discord.HTTPException as exc:
        bot.log_error("purge:history", exc)
        await ctx.send("❌ Couldn't read this channel's history.", ephemeral=True)
        raise  # Re-raise to signal failure so the parent handles it

    return matched, hit_age_limit, skipped_pins


async def _delete_purge_messages(
    ctx: commands.Context, matched: List[discord.Message]
) -> int:
    """Delete the gathered messages in chunks."""
    deleted: int = 0
    for start in range(0, len(matched), 100):
        chunk: List[discord.Message] = matched[start : start + 100]
        try:
            if len(chunk) == 1:
                await chunk[0].delete()
            else:
                await ctx.channel.delete_messages(chunk)
            deleted += len(chunk)
        except discord.HTTPException as exc:
            bot.log_error("purge:delete", exc)
        if start + 100 < len(matched):
            await asyncio.sleep(0.5)
    return deleted


async def _send_purge_summary(
    ctx: commands.Context,
    deleted: int,
    amount: int,
    hit_age_limit: bool,
    skipped_pins: int,
    check: Optional[Callable[[discord.Message], bool]],
    label: str,
) -> None:
    """Generate and send the final summary message."""
    notes: List[str] = []
    if hit_age_limit:
        notes.append(
            f"stopped at Discord's {BULK_DELETE_AGE_DAYS}-day bulk-delete limit"
        )
    if skipped_pins:
        notes.append(f"skipped {skipped_pins} pinned")
    if check is not None and deleted < amount and not hit_age_limit:
        notes.append("no more matches in recent history")
    tail: str = f" — {'; '.join(notes)}." if notes else "."

    summary: str = f"✅ Deleted **{deleted}** {label}{tail}"
    if ctx.interaction is not None:
        await ctx.send(summary, ephemeral=True)
    else:
        await ctx.send(summary, delete_after=8)


async def _run_purge(
    ctx: commands.Context,
    amount: int,
    check: Optional[Callable[[discord.Message], bool]] = None,
    label: str = "messages",
) -> None:
    """Shared engine behind every /purge variant."""
    if not await _check_purge_permissions(ctx):
        return

    amount = min(max(amount, 1), PURGE_HARD_LIMIT)
    cutoff: datetime = datetime.now(timezone.utc) - timedelta(days=BULK_DELETE_AGE_DAYS)
    invoking_id: int = ctx.message.id if ctx.message is not None else 0

    if ctx.interaction is not None:
        await ctx.defer(ephemeral=True)
    else:
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

    try:
        matched, hit_age_limit, skipped_pins = await _gather_purge_messages(
            ctx, amount, check, cutoff, invoking_id
        )
    except discord.HTTPException:
        return

    deleted = await _delete_purge_messages(ctx, matched)

    await _send_purge_summary(
        ctx, deleted, amount, hit_age_limit, skipped_pins, check, label
    )


@bot.hybrid_group(
    name="purge",
    description="Delete recent messages, optionally filtered",
    fallback="any",
    invoke_without_command=True,
)
@app_commands.default_permissions(manage_messages=True)
@commands.guild_only()
@app_commands.describe(amount="How many messages to delete (1-500)")
async def purge_group(ctx: commands.Context, amount: int = 10):
    await _run_purge(ctx, amount, None, "messages")


@purge_group.command(
    name="msg",
    aliases=["purgemsg"],
    description="Delete recent messages matching text criteria",
)
@app_commands.describe(
    text="Text to match",
    match_type="How to match: 'contains', 'starts_with', or 'ends_with'",
    amount="How many to delete (1-500)",
)
async def purge_msg(
    ctx: commands.Context,
    text: str,
    match_type: Literal["contains", "starts_with", "ends_with"] = "contains",
    amount: int = 10,
):
    needle = text.casefold()
    if match_type == "starts_with":
        check = lambda m: _extract_message_text(m).startswith(needle)
        label = f"messages starting with `{text[:60]}`"
    elif match_type == "ends_with":
        check = lambda m: _extract_message_text(m).endswith(needle)
        label = f"messages ending with `{text[:60]}`"
    else:
        check = lambda m: needle in _extract_message_text(m)
        label = f"messages containing `{text[:60]}`"
    await _run_purge(ctx, amount, check, label)


@purge_group.command(name="user", description="Delete a member's recent messages")
@app_commands.describe(
    user="Whose messages to delete", amount="How many to delete (1-500)"
)
async def purge_user(ctx: commands.Context, user: discord.User, amount: int = 10):
    await _run_purge(
        ctx, amount, lambda m: m.author.id == user.id, f"messages from **{user}**"
    )


@purge_group.command(
    name="contains", description="Delete recent messages containing some text"
)
@app_commands.describe(
    text="Case-insensitive text to match (use quotes for multiple words)",
    amount="How many to delete (1-500)",
)
async def purge_contains(ctx: commands.Context, text: str, amount: int = 10):
    needle: str = text.casefold()
    await _run_purge(
        ctx,
        amount,
        lambda m: needle in _extract_message_text(m),
        f"messages containing `{text[:60]}`",
    )


@purge_group.command(name="bots", description="Delete recent messages sent by bots")
@app_commands.describe(amount="How many to delete (1-500)")
async def purge_bots(ctx: commands.Context, amount: int = 10):
    await _run_purge(ctx, amount, lambda m: m.author.bot, "bot messages")


@purge_group.command(
    name="links", description="Delete recent messages containing links or invites"
)
@app_commands.describe(amount="How many to delete (1-500)")
async def purge_links(ctx: commands.Context, amount: int = 10):
    await _run_purge(
        ctx, amount, lambda m: bool(LINK_RE.search(m.content)), "messages with links"
    )
