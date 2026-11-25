"""The counting channel: parsing, saves, milestones and the `counting` group."""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import discord
from discord import app_commands
from word2number import w2n

from utils.safe_math import safe_eval_int
from core.registry import DYNAMIC_ITEMS
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# Counting System
# --------------------------------------------------------------------------- #


COUNTING_SAVE_ID_PREFIX: str = "counting:save:"

_COUNTING_LOCKS: Dict[int, asyncio.Lock] = {}


def _counting_lock(guild_id: int) -> asyncio.Lock:
    """One lock per guild around every counting read-modify-write."""
    lock = _COUNTING_LOCKS.get(guild_id)
    if lock is None:
        lock = _COUNTING_LOCKS[guild_id] = asyncio.Lock()
    return lock


def _counting_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _counting_fresh_state(guild_id: int) -> Dict[str, Any]:
    return {
        "guild_id": guild_id,
        "current": 0,
        "last_user": None,
        "streak": 0,
        "broken": False,
        "broken_time": None,
    }


async def _counting_load(guild_id: int) -> Optional[Dict[str, Any]]:
    state = await asyncio.to_thread(
        bot.settings.counting.find_one, {"guild_id": guild_id}
    )
    if state is not None:
        state.pop("_id", None)
    return state


async def _counting_store(guild_id: int, state: Dict[str, Any]) -> None:
    fields = {k: v for k, v in state.items() if k != "_id"}
    await asyncio.to_thread(
        bot.settings.counting.update_one,
        {"guild_id": guild_id},
        {"$set": fields},
        upsert=True,
    )


def _counting_save_key(guild_id: int, user_id: int, day: str) -> Dict[str, Any]:
    # guild_id is part of the key: a save used in one server is not used up in others.
    return {"guild_id": guild_id, "user_id": user_id, "date": day}


async def _counting_saves_used(guild_id: int, user_id: int, day: str) -> int:
    record = await asyncio.to_thread(
        bot.settings.counting_saves.find_one,
        _counting_save_key(guild_id, user_id, day),
    )
    return int((record or {}).get("used", 0))


async def _counting_spend_save(guild_id: int, user_id: int, day: str) -> None:
    await asyncio.to_thread(
        bot.settings.counting_saves.update_one,
        _counting_save_key(guild_id, user_id, day),
        {
            "$inc": {"used": 1},
            "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )


def _counting_max_saves(settings: Dict[str, Any]) -> int:
    try:
        return int((settings.get("counting") or {}).get("default_saves", 3))
    except (TypeError, ValueError):
        return 3


class CountingSaveButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"counting:save:(?P<guild_id>[0-9]{1,20})",
):
    """The "Save Streak" button. Persistent: it works after a restart because the
    guild is in its custom_id and the state is read from the database on click."""

    def __init__(self, guild_id: int) -> None:
        super().__init__(
            discord.ui.Button(
                label="Save Streak (-1 Save)",
                style=discord.ButtonStyle.success,
                emoji="\U0001f691",
                custom_id=f"{COUNTING_SAVE_ID_PREFIX}{guild_id}",
            )
        )
        self.guild_id: int = guild_id

    @classmethod
    async def from_custom_id(
        cls,
        interaction: discord.Interaction,
        item: discord.ui.Button,
        match: "re.Match[str]",
    ) -> "CountingSaveButton":
        return cls(int(match["guild_id"]))

    async def callback(self, interaction: discord.Interaction) -> None:
        await _counting_use_save(interaction, self.guild_id)


class CountingSaveView(discord.ui.View):
    def __init__(self, guild_id: int) -> None:
        super().__init__(timeout=None)
        self.add_item(CountingSaveButton(guild_id))


def _counting_saved_embed(
    original: Optional[discord.Embed], user: discord.abc.User, state: Dict[str, Any]
) -> discord.Embed:
    embed = original.copy() if original is not None else discord.Embed()
    embed.color = discord.Color.green()
    embed.title = "Streak Saved!"
    embed.description = (
        f"{user.mention} used a save! The streak continues at "
        f"**{state['streak']}**. Next number is **{state['current'] + 1}**."
    )
    return embed


async def _counting_use_save(interaction: discord.Interaction, guild_id: int) -> None:
    if interaction.guild_id != guild_id:
        await interaction.response.send_message(
            "This button belongs to another server.", ephemeral=True
        )
        return
    user_id: int = interaction.user.id
    today: str = _counting_today()
    async with _counting_lock(guild_id):
        settings = await bot.settings.fetch_settings(guild_id)
        max_saves: int = _counting_max_saves(settings)
        used_saves: int = await _counting_saves_used(guild_id, user_id, today)
        if used_saves >= max_saves:
            await interaction.response.send_message(
                f"You have already used all {max_saves} of your saves today!",
                ephemeral=True,
            )
            return

        state = await _counting_load(guild_id)
        if not state or not state.get("broken"):
            await interaction.response.send_message(
                "The streak is no longer broken!", ephemeral=True
            )
            return

        await _counting_spend_save(guild_id, user_id, today)
        state["broken"] = False
        state["broken_time"] = None
        await _counting_store(guild_id, state)

    message = interaction.message
    original = message.embeds[0] if message is not None and message.embeds else None
    disabled = CountingSaveView(guild_id)
    for child in disabled.children:
        child.disabled = True
    # Answer the interaction itself (not message.edit + defer), within Discord's 3 s.
    await interaction.response.edit_message(
        embed=_counting_saved_embed(original, interaction.user, state), view=disabled
    )


def _parse_counting_number(text: str):
    """Parse a counting message: a plain number, a restricted calculator expression
    (see utils.safe_math - no eval), or a spelled-out number. Never raises."""
    if not text or len(text) > 100:
        return None
    text = text.lower().strip()
    if text.isdigit():
        return int(text)
    value = safe_eval_int(text)
    if value is not None:
        return value
    try:
        val = w2n.word_to_num(text)
        if isinstance(val, (int, float)) and float(val).is_integer():
            return int(val)
    except Exception:
        pass
    return None


COUNTING_MAX_WORDS = 12


def _extract_and_parse_number(content: str):
    # Only the first few words are tried, longest prefix first, so a long message
    # costs a bounded amount of parsing.
    words = (content or "").split()[:COUNTING_MAX_WORDS]
    for i in range(len(words), 0, -1):
        prefix = " ".join(words[:i]).rstrip(",.!?")
        parsed = _parse_counting_number(prefix)
        if parsed is not None:
            return parsed
    return None


def _counting_mark_broken(state: Dict[str, Any]) -> None:
    state["broken"] = True
    state["broken_time"] = time.time()


def _counting_broken_embed(
    author: discord.abc.User, state: Dict[str, Any], reason: str, saves: int
) -> discord.Embed:
    per_day: str = f"{saves} save{'s' if saves != 1 else ''} per day"
    return discord.Embed(
        title="Streak Broken!",
        description=(
            f"{author.mention} ruined the streak at **{state['streak']}**.\n\n"
            f"{reason}\n\n"
            f"You have {per_day} to restore the streak. Click below to use one!"
        ),
        color=discord.Color.red(),
    )


async def _announce_counting_break(
    message: discord.Message, state: Dict[str, Any], reason: str
) -> None:
    """Ruiner role, reaction and the "Streak Broken" message. Runs outside the lock."""
    settings = await bot.settings.fetch_settings(message.guild.id)
    counting_settings = settings.get("counting") or {}
    ruiner_role_id = counting_settings.get("ruiner_role_id")
    if ruiner_role_id:
        role = message.guild.get_role(int(ruiner_role_id))
        if role:
            # Remove role from anyone who currently has it
            for member in role.members:
                try:
                    if member.id != message.author.id:
                        await member.remove_roles(
                            role, reason="Someone else ruined the counting streak"
                        )
                except discord.HTTPException:
                    pass
            # Add role to the new ruiner
            try:
                if isinstance(message.author, discord.Member):
                    await message.author.add_roles(
                        role, reason="Ruined the counting streak"
                    )
            except discord.HTTPException:
                pass

    embed = _counting_broken_embed(
        message.author, state, reason, _counting_max_saves(settings)
    )
    try:
        await message.add_reaction("\u274c")
    except discord.HTTPException:
        pass
    await message.channel.send(embed=embed, view=CountingSaveView(message.guild.id))


def _is_counting_channel(guild_id: int, channel_id: int) -> bool:
    counting_settings = bot.settings.peek_settings(guild_id).get("counting") or {}
    configured = counting_settings.get("channel_id")
    try:
        return bool(configured) and int(configured) == channel_id
    except (TypeError, ValueError):
        return False


COUNTING_MILESTONES: Dict[int, str] = {
    69: "\U0001f60f",
    77: "\U0001f3b0",
    100: "\U0001f4af",
    420: "\U0001f33f",
    666: "\U0001f608",
}


def _counting_reaction(number: int) -> str:
    if number in COUNTING_MILESTONES:
        return COUNTING_MILESTONES[number]
    if number % 1000 == 0:
        return "\U0001f389"
    if number % 100 == 0:
        return "\U0001f31f"
    return "\u2705"


async def handle_counting(message: discord.Message) -> None:
    if message.author.bot or not message.guild:
        return
    if not _is_counting_channel(message.guild.id, message.channel.id):
        return

    parsed = _extract_and_parse_number(message.content)
    if parsed is None:
        return

    guild_id: int = message.guild.id
    broke_reason: Optional[str] = None
    async with _counting_lock(guild_id):
        state = await _counting_load(guild_id) or _counting_fresh_state(guild_id)

        if state.get("broken"):
            if state.get("broken_time") and time.time() - state["broken_time"] > 86400:
                state.update(_counting_fresh_state(guild_id))
                await _counting_store(guild_id, state)
            else:
                return

        expected = state["current"] + 1
        if parsed != expected:
            broke_reason = f"Expected {expected}, but got {parsed}."
        elif state["last_user"] == message.author.id:
            broke_reason = f"{message.author.mention} tried to count twice in a row!"

        if broke_reason is not None:
            _counting_mark_broken(state)
        else:
            state["current"] = expected
            state["last_user"] = message.author.id
            state["streak"] += 1
        await _counting_store(guild_id, state)

    if broke_reason is not None:
        await _announce_counting_break(message, state, broke_reason)
        return
    try:
        await message.add_reaction(_counting_reaction(expected))
    except discord.HTTPException:
        pass


async def handle_counting_retraction(
    message: discord.Message, announce_on: discord.Message, verb: str
) -> bool:
    """A valid count was edited or deleted: break the streak. Returns True if it did."""
    if message.guild is None or message.author.bot:
        return False
    if not _is_counting_channel(message.guild.id, message.channel.id):
        return False
    parsed = _extract_and_parse_number(message.content)
    if parsed is None:
        return False
    guild_id: int = message.guild.id
    async with _counting_lock(guild_id):
        state = await _counting_load(guild_id)
        if not state or state.get("broken") or parsed != state.get("current"):
            return False
        _counting_mark_broken(state)
        await _counting_store(guild_id, state)
    await _announce_counting_break(
        announce_on, state, f"{message.author.mention} {verb} their valid count!"
    )
    return True


# --------------------------------------------------------------------------- #

counting_group = app_commands.Group(
    name="counting",
    description="Configure the server's counting channel and settings.",
    default_permissions=discord.Permissions(administrator=True),
)
bot.tree.add_command(counting_group)


@counting_group.command(name="set_channel", description="Set the counting channel")
@app_commands.describe(channel="The channel for counting (or leave empty to disable)")
async def counting_set_channel(
    interaction: discord.Interaction, channel: Optional[discord.TextChannel] = None
):
    guild_id = interaction.guild_id
    if not guild_id:
        return
    if channel:
        value: Optional[int] = channel.id
        msg = f"Counting channel set to {channel.mention}."
    else:
        value = None
        msg = "Counting disabled."
    await bot.settings.push_fields(guild_id, {"counting.channel_id": value})
    await interaction.response.send_message(msg, ephemeral=True)


@counting_group.command(
    name="set_saves", description="Set the number of saves members get per day"
)
@app_commands.describe(saves="Number of saves (default is 3)")
async def counting_set_saves(
    interaction: discord.Interaction, saves: app_commands.Range[int, 0, 100]
):
    guild_id = interaction.guild_id
    if not guild_id:
        return
    await bot.settings.push_fields(guild_id, {"counting.default_saves": saves})
    await interaction.response.send_message(
        f"Counting default saves per day set to {saves}.", ephemeral=True
    )


@counting_group.command(
    name="set_ruiner_role", description="Set the streak ruiner role"
)
@app_commands.describe(
    role="The role to give to whoever ruins the streak (or leave empty to disable)"
)
async def counting_set_ruiner_role(
    interaction: discord.Interaction, role: Optional[discord.Role] = None
):
    guild_id = interaction.guild_id
    if not guild_id:
        return
    if role:
        value: Optional[int] = role.id
        msg = f"Streak ruiner role set to {role.mention}."
    else:
        value = None
        msg = "Streak ruiner role disabled."
    await bot.settings.push_fields(guild_id, {"counting.ruiner_role_id": value})
    await interaction.response.send_message(msg, ephemeral=True)


@counting_group.command(
    name="reset", description="Reset the counting streak and current number"
)
async def counting_reset(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if not guild_id:
        return
    async with _counting_lock(guild_id):
        await _counting_store(guild_id, _counting_fresh_state(guild_id))
    await interaction.response.send_message(
        "The counting streak has been reset to 0.", ephemeral=True
    )


# Started / added by DebtCollectorBot.setup_hook (see core/registry.py).
DYNAMIC_ITEMS.append(CountingSaveButton)
