"""Permission predicates and checks. Imports core.config only, never the bot."""

import logging
from typing import List, Optional

import discord
from discord.ext import commands

from core.config import SUPERUSER_IDS

log: logging.Logger = logging.getLogger("debt-collector")


def is_superuser(user: discord.abc.User) -> bool:
    """Snowflake-only superuser check."""
    return user.id in SUPERUSER_IDS


def is_guild_owner():
    async def predicate(ctx: commands.Context) -> bool:
        if is_superuser(ctx.author):
            return True
        if ctx.guild is None:
            raise commands.NoPrivateMessage()
        if ctx.author.id != ctx.guild.owner_id:
            raise commands.CheckFailure(
                "This command can only be used by the server owner."
            )
        return True

    return commands.check(predicate)


def custom_has_permissions(**perms: bool):
    async def predicate(ctx: commands.Context) -> bool:
        if is_superuser(ctx.author):
            return True
        # Emulate standard behavior
        permissions = ctx.channel.permissions_for(ctx.author)
        missing = [
            perm for perm, value in perms.items() if getattr(permissions, perm) != value
        ]
        if not missing:
            return True
        raise commands.MissingPermissions(missing)

    return commands.check(predicate)


def member_has_perms(member: discord.Member, **perms: bool) -> bool:
    if is_superuser(member):
        return True
    resolved: discord.Permissions = member.guild_permissions
    if resolved.administrator:
        return True
    return all(getattr(resolved, name, False) == value for name, value in perms.items())


def mod_block_reason(
    actor: discord.Member, target: discord.Member, me: discord.Member
) -> Optional[str]:
    if is_superuser(actor):
        return None
    if target.id == actor.id:
        return "You can't use that on yourself."
    if target.id == me.id:
        return "I'm not moderating myself."
    if target.id == actor.guild.owner_id:
        return "That member is the server owner — nobody can moderate them."
    if (
        not is_superuser(actor)
        and actor.id != actor.guild.owner_id
        and actor.top_role <= target.top_role
    ):
        return "You can't act on someone whose highest role is equal to or above yours."
    if me.top_role <= target.top_role:
        return (
            "⚠️ **My highest role is not above that member's.** "
            "Drag my role higher in **Server Settings → Roles**, then try again."
        )
    return None


class NotStaff(commands.CheckFailure):
    """Raised when a member invokes a command gated behind permissions they lack."""

    def __init__(self, required: List[str]) -> None:
        self.required: List[str] = required
        super().__init__("Missing staff permissions.")


def _required_permissions(command: commands.Command) -> Optional[discord.Permissions]:
    """The default_permissions gate on a command, inherited from its parent group."""
    node: Optional[commands.Command] = command
    while node is not None:
        app_command = getattr(node, "app_command", None)
        perms = getattr(app_command, "default_permissions", None)
        if perms is not None and perms.value:
            return perms
        node = node.parent
    return None


def _can_run(user: discord.abc.User, command: commands.Command) -> bool:
    """Whether this user satisfies the command's permission gate."""
    perms: Optional[discord.Permissions] = _required_permissions(command)
    if perms is None:
        return True
    if not isinstance(user, discord.Member):
        return False
    return member_has_perms(user, **{name: True for name, value in perms if value})
