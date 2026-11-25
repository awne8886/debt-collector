"""Server backup, clone and rollback."""

import asyncio
import logging

import discord
from discord.ext import commands

from core.permissions import is_guild_owner
from core.bot import bot

log: logging.Logger = logging.getLogger("debt-collector")


async def backup_guild(guild: discord.Guild, bot: commands.Bot) -> dict:
    from datetime import datetime, timezone

    backup_data = {
        "guild_id": guild.id,
        "name": guild.name,
        "icon": None,
        "banner": None,
        "createdAt": datetime.now(timezone.utc),
        "roles": [],
        "categories": [],
        "channels": [],
        "emojis": [],
        "bot_settings": None,
    }

    if guild.icon:
        try:
            backup_data["icon"] = await guild.icon.read()
        except Exception:
            pass
    if guild.banner:
        try:
            backup_data["banner"] = await guild.banner.read()
        except Exception:
            pass

    for role in guild.roles:
        if (
            role.is_default()
            or role.is_bot_managed()
            or role.is_premium_subscriber()
            or role.is_integration()
        ):
            continue
        backup_data["roles"].append(
            {
                "id": role.id,
                "name": role.name,
                "permissions": role.permissions.value,
                "color": role.color.value,
                "hoist": role.hoist,
                "mentionable": role.mentionable,
                "position": role.position,
            }
        )

    for category in guild.categories:
        overwrites_data = []
        for target, overwrite in category.overwrites.items():
            if isinstance(target, discord.Role):
                target_type = "role"
            elif isinstance(target, discord.Member):
                target_type = "member"
            else:
                continue
            overwrites_data.append(
                {
                    "target_type": target_type,
                    "target_id": target.id,
                    "is_default": getattr(target, "is_default", lambda: False)(),
                    "allow": overwrite.pair()[0].value,
                    "deny": overwrite.pair()[1].value,
                }
            )

        backup_data["categories"].append(
            {
                "id": category.id,
                "name": category.name,
                "position": category.position,
                "nsfw": category.nsfw,
                "overwrites": overwrites_data,
            }
        )

    for channel in guild.channels:
        if isinstance(channel, discord.CategoryChannel):
            continue
        overwrites_data = []
        for target, overwrite in channel.overwrites.items():
            if isinstance(target, discord.Role):
                target_type = "role"
            elif isinstance(target, discord.Member):
                target_type = "member"
            else:
                continue
            overwrites_data.append(
                {
                    "target_type": target_type,
                    "target_id": target.id,
                    "is_default": getattr(target, "is_default", lambda: False)(),
                    "allow": overwrite.pair()[0].value,
                    "deny": overwrite.pair()[1].value,
                }
            )

        chan_data = {
            "id": channel.id,
            "name": channel.name,
            "position": channel.position,
            "type": str(channel.type),
            "category_id": channel.category.id if channel.category else None,
            "overwrites": overwrites_data,
        }
        if isinstance(channel, discord.TextChannel):
            chan_data["topic"] = channel.topic
            chan_data["nsfw"] = channel.nsfw
            chan_data["slowmode_delay"] = channel.slowmode_delay
        elif isinstance(channel, discord.VoiceChannel):
            chan_data["bitrate"] = channel.bitrate
            chan_data["user_limit"] = channel.user_limit
        backup_data["channels"].append(chan_data)

    for emoji in guild.emojis:
        if emoji.managed:
            continue
        try:
            backup_data["emojis"].append(
                {"name": emoji.name, "url": emoji.url, "bytes": await emoji.read()}
            )
        except Exception:
            pass

    if hasattr(bot, "settings"):
        settings = await bot.settings.fetch_settings(guild.id)
        # Deepcopy to avoid reference issues, just in case
        import copy

        backup_data["bot_settings"] = copy.deepcopy(settings)

    try:
        await asyncio.to_thread(bot.settings.backups.insert_one, backup_data)
        return backup_data
    except Exception as e:
        import logging

        logging.getLogger(__name__).error(f"Failed to save backup for {guild.id}: {e}")
        return backup_data


@bot.hybrid_command(
    name="clone_server",
    description="Clone a server's roles, channels, and settings. Owner only.",
)
@is_guild_owner()
async def clone_server(ctx: commands.Context, reference_guild_id: str):
    await ctx.defer()

    try:
        ref_guild_id = int(reference_guild_id)
    except ValueError:
        return await ctx.send("Invalid server ID provided.")

    reference_guild = bot.get_guild(ref_guild_id)
    if not reference_guild:
        return await ctx.send("I cannot find that server, or I am not a member of it.")

    if not ctx.guild.me.guild_permissions.administrator:
        return await ctx.send("I need Administrator permissions to clone the server.")

    await ctx.send(
        "Starting the cloning process... this may take up to 10 minutes. I will ping you when it's complete."
    )

    # 1. Backup the current server
    await backup_guild(ctx.guild, bot)

    # 2. Deletion Process
    # Channels
    for channel in ctx.guild.channels:
        try:
            await channel.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:delete_channel",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Categories
    for category in ctx.guild.categories:
        try:
            await category.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:delete_category",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Roles
    for role in ctx.guild.roles:
        if (
            role.is_default()
            or role.is_bot_managed()
            or role.is_premium_subscriber()
            or role.is_integration()
        ):
            continue
        try:
            await role.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:delete_role",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Emojis
    for emoji in ctx.guild.emojis:
        try:
            await emoji.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:delete_emoji",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Dictionary to map old IDs to new IDs
    id_map = {}

    def map_overwrites(old_overwrites, id_map, guild):
        import discord

        new_overwrites = {}
        for target, overwrite in old_overwrites.items():
            if isinstance(target, discord.Role):
                if target.is_default():
                    new_overwrites[guild.default_role] = overwrite
                else:
                    new_id = id_map.get(str(target.id))
                    if new_id:
                        new_role = guild.get_role(int(new_id))
                        if new_role:
                            new_overwrites[new_role] = overwrite
            elif isinstance(target, discord.Member):
                new_id = id_map.get(str(target.id))
                if new_id:
                    new_member = guild.get_member(int(new_id))
                    if new_member:
                        new_overwrites[new_member] = overwrite
        return new_overwrites

    # 3. Creation Process
    # Guild Info
    try:
        icon_bytes = await reference_guild.icon.read() if reference_guild.icon else None
        banner_bytes = (
            await reference_guild.banner.read() if reference_guild.banner else None
        )

        await ctx.guild.edit(
            name=reference_guild.name, icon=icon_bytes, banner=banner_bytes
        )
    except Exception as e:
        bot.log_error(
            "clone_server:edit_guild",
            e,
            guild=ctx.guild,
            channel=ctx.channel,
            user=ctx.author,
        )

    # Roles
    roles_to_copy = [
        r
        for r in reference_guild.roles
        if not r.is_default()
        and not r.is_bot_managed()
        and not r.is_premium_subscriber()
        and not r.is_integration()
    ]
    roles_to_copy.sort(key=lambda r: r.position)

    bot_highest_role = ctx.guild.me.top_role

    for old_role in roles_to_copy:
        try:
            new_role = await ctx.guild.create_role(
                name=old_role.name,
                permissions=old_role.permissions,
                color=old_role.color,
                hoist=old_role.hoist,
                mentionable=old_role.mentionable,
            )
            id_map[str(old_role.id)] = str(new_role.id)

            # Re-position slightly below the bot's highest role if necessary
            if new_role.position >= bot_highest_role.position:
                await new_role.edit(position=bot_highest_role.position - 1)

            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:create_role",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Categories
    categories_to_copy = sorted(reference_guild.categories, key=lambda c: c.position)
    for old_cat in categories_to_copy:
        try:
            new_cat = await ctx.guild.create_category(
                name=old_cat.name,
                position=old_cat.position,
                overwrites=map_overwrites(old_cat.overwrites, id_map, ctx.guild),
            )
            try:
                if old_cat.nsfw:
                    await new_cat.edit(nsfw=old_cat.nsfw)
            except Exception:
                pass
            id_map[str(old_cat.id)] = str(new_cat.id)
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:create_category",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Channels
    channels_to_copy = [
        c
        for c in reference_guild.channels
        if not isinstance(c, discord.CategoryChannel)
    ]
    channels_to_copy.sort(key=lambda c: c.position)
    for old_chan in channels_to_copy:
        try:
            category = None
            if old_chan.category_id:
                new_cat_id = id_map.get(str(old_chan.category_id))
                if new_cat_id:
                    category = ctx.guild.get_channel(int(new_cat_id))

            chan_overwrites = map_overwrites(old_chan.overwrites, id_map, ctx.guild)

            if isinstance(old_chan, discord.TextChannel):
                new_chan = await ctx.guild.create_text_channel(
                    name=old_chan.name,
                    category=category,
                    position=old_chan.position,
                    topic=old_chan.topic,
                    nsfw=old_chan.nsfw,
                    slowmode_delay=old_chan.slowmode_delay,
                    overwrites=chan_overwrites,
                )
            elif isinstance(old_chan, discord.VoiceChannel):
                new_chan = await ctx.guild.create_voice_channel(
                    name=old_chan.name,
                    category=category,
                    position=old_chan.position,
                    bitrate=old_chan.bitrate,
                    user_limit=old_chan.user_limit,
                    overwrites=chan_overwrites,
                )
            elif isinstance(old_chan, discord.StageChannel):
                new_chan = await ctx.guild.create_stage_channel(
                    name=old_chan.name,
                    category=category,
                    position=old_chan.position,
                    topic=old_chan.topic,
                    overwrites=chan_overwrites,
                )
            elif isinstance(old_chan, discord.ForumChannel):
                new_chan = await ctx.guild.create_forum(
                    name=old_chan.name,
                    category=category,
                    position=old_chan.position,
                    topic=old_chan.topic,
                    nsfw=old_chan.nsfw,
                    overwrites=chan_overwrites,
                )
            else:
                continue

            id_map[str(old_chan.id)] = str(new_chan.id)
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "clone_server:create_channel",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Emojis
    for old_emoji in reference_guild.emojis:
        if old_emoji.managed:
            continue
        try:
            emoji_bytes = await old_emoji.read()
            await ctx.guild.create_custom_emoji(name=old_emoji.name, image=emoji_bytes)
            await asyncio.sleep(1)
        except Exception as e:
            bot.log_error(
                "clone_server:create_emoji",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    # Migrate Settings
    try:
        ref_settings = await bot.settings.fetch_settings(reference_guild.id)

        # We recursively replace any string value that appears in id_map
        def replace_ids(obj):
            if isinstance(obj, dict):
                new_obj = {}
                for k, v in obj.items():
                    # Check if the key itself is an ID
                    new_k = id_map.get(str(k), k)
                    new_obj[new_k] = replace_ids(v)
                return new_obj
            elif isinstance(obj, list):
                return [replace_ids(i) for i in obj]
            elif isinstance(obj, str):
                return id_map.get(obj, obj)
            elif isinstance(obj, int):
                mapped = id_map.get(str(obj))
                return int(mapped) if mapped else obj
            return obj

        new_settings = replace_ids(ref_settings)
        # Drop the guildid so it gets updated properly by update_settings
        new_settings.pop("guildid", None)
        new_settings.pop("_id", None)

        await bot.settings.push_settings(ctx.guild.id, new_settings)

    except Exception as e:
        bot.log_error(
            "clone_server:migrate_settings",
            e,
            guild=ctx.guild,
            channel=ctx.channel,
            user=ctx.author,
        )

    # Find any existing channel to send completion ping, if we deleted all channels there might be none.
    # We should have created new channels. Let's find the first text channel.
    completion_channel = next(
        (
            c
            for c in ctx.guild.text_channels
            if c.permissions_for(ctx.guild.me).send_messages
        ),
        None,
    )

    if completion_channel:
        await completion_channel.send(
            f"{ctx.author.mention}, the server clone is complete!"
        )


@bot.hybrid_command(
    name="rollback_clone",
    description="Rollback the server to the last backup. Owner only.",
)
@is_guild_owner()
async def rollback_clone(ctx: commands.Context):
    await ctx.defer()

    if not ctx.guild.me.guild_permissions.administrator:
        return await ctx.send(
            "I need Administrator permissions to rollback the server."
        )

    try:
        # Find the latest backup for this guild
        backup = await asyncio.to_thread(
            bot.settings.backups.find_one,
            {"guild_id": ctx.guild.id},
            sort=[("createdAt", -1)],
        )
    except Exception as e:
        bot.log_error(
            "rollback_clone:fetch_backup",
            e,
            guild=ctx.guild,
            channel=ctx.channel,
            user=ctx.author,
        )
        return await ctx.send("Failed to query backups.")

    if not backup:
        return await ctx.send("No recent backup found for this server.")

    await ctx.send("Starting the rollback process... this may take up to 10 minutes.")

    # Deletion Process (Delete current state)
    for channel in ctx.guild.channels:
        try:
            await channel.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:delete_channel",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    for category in ctx.guild.categories:
        try:
            await category.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:delete_category",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    for role in ctx.guild.roles:
        if (
            role.is_default()
            or role.is_bot_managed()
            or role.is_premium_subscriber()
            or role.is_integration()
        ):
            continue
        try:
            await role.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:delete_role",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    for emoji in ctx.guild.emojis:
        try:
            await emoji.delete()
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:delete_emoji",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    id_map = {}

    # Creation Process (Restore from backup)
    try:
        await ctx.guild.edit(
            name=backup.get("name", ctx.guild.name),
            icon=backup.get("icon"),
            banner=backup.get("banner"),
        )
    except Exception as e:
        bot.log_error(
            "rollback_clone:edit_guild",
            e,
            guild=ctx.guild,
            channel=ctx.channel,
            user=ctx.author,
        )

    bot_highest_role = ctx.guild.me.top_role

    roles_data = backup.get("roles", [])
    roles_data.sort(key=lambda r: r.get("position", 0))
    for r_data in roles_data:
        try:
            new_role = await ctx.guild.create_role(
                name=r_data["name"],
                permissions=discord.Permissions(r_data["permissions"]),
                color=discord.Color(r_data["color"]),
                hoist=r_data["hoist"],
                mentionable=r_data["mentionable"],
            )
            id_map[str(r_data["id"])] = str(new_role.id)
            if new_role.position >= bot_highest_role.position:
                await new_role.edit(position=bot_highest_role.position - 1)
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:create_role",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    def restore_overwrites(overwrites_data, id_map, guild):
        import discord

        new_overwrites = {}
        for ow_data in overwrites_data:
            target = None
            if ow_data.get("is_default"):
                target = guild.default_role
            else:
                new_id_str = id_map.get(str(ow_data["target_id"]))
                if not new_id_str:
                    continue

                new_id = int(new_id_str)
                if ow_data["target_type"] == "role":
                    target = guild.get_role(new_id)
                elif ow_data["target_type"] == "member":
                    target = guild.get_member(new_id)

            if target:
                allow = discord.Permissions(ow_data["allow"])
                deny = discord.Permissions(ow_data["deny"])
                new_overwrites[target] = discord.PermissionOverwrite.from_pair(
                    allow, deny
                )

        return new_overwrites

    categories_data = backup.get("categories", [])
    categories_data.sort(key=lambda c: c.get("position", 0))
    for c_data in categories_data:
        try:
            new_cat = await ctx.guild.create_category(
                name=c_data["name"],
                position=c_data.get("position"),
                overwrites=restore_overwrites(
                    c_data.get("overwrites", []), id_map, ctx.guild
                ),
            )
            try:
                if c_data.get("nsfw", False):
                    await new_cat.edit(nsfw=True)
            except Exception:
                pass
            id_map[str(c_data["id"])] = str(new_cat.id)
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:create_category",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    channels_data = backup.get("channels", [])
    channels_data.sort(key=lambda c: c.get("position", 0))
    for ch_data in channels_data:
        try:
            category = None
            if ch_data.get("category_id"):
                new_cat_id = id_map.get(str(ch_data["category_id"]))
                if new_cat_id:
                    category = ctx.guild.get_channel(int(new_cat_id))

            ch_type = ch_data.get("type", "text")

            chan_overwrites = restore_overwrites(
                ch_data.get("overwrites", []), id_map, ctx.guild
            )

            if "text" in ch_type:
                new_chan = await ctx.guild.create_text_channel(
                    name=ch_data["name"],
                    category=category,
                    position=ch_data.get("position"),
                    topic=ch_data.get("topic"),
                    nsfw=ch_data.get("nsfw", False),
                    slowmode_delay=ch_data.get("slowmode_delay", 0),
                    overwrites=chan_overwrites,
                )
            elif "voice" in ch_type:
                new_chan = await ctx.guild.create_voice_channel(
                    name=ch_data["name"],
                    category=category,
                    position=ch_data.get("position"),
                    bitrate=ch_data.get("bitrate", 64000),
                    user_limit=ch_data.get("user_limit", 0),
                    overwrites=chan_overwrites,
                )
            elif "stage" in ch_type:
                new_chan = await ctx.guild.create_stage_channel(
                    name=ch_data["name"],
                    category=category,
                    position=ch_data.get("position"),
                    topic=ch_data.get("topic"),
                    overwrites=chan_overwrites,
                )
            elif "forum" in ch_type:
                new_chan = await ctx.guild.create_forum(
                    name=ch_data["name"],
                    category=category,
                    position=ch_data.get("position"),
                    topic=ch_data.get("topic"),
                    nsfw=ch_data.get("nsfw", False),
                    overwrites=chan_overwrites,
                )
            else:
                continue

            id_map[str(ch_data["id"])] = str(new_chan.id)
            await asyncio.sleep(0.5)
        except Exception as e:
            bot.log_error(
                "rollback_clone:create_channel",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    for emoji_data in backup.get("emojis", []):
        try:
            if emoji_data.get("bytes"):
                await ctx.guild.create_custom_emoji(
                    name=emoji_data["name"], image=emoji_data["bytes"]
                )
                await asyncio.sleep(1)
        except Exception as e:
            bot.log_error(
                "rollback_clone:create_emoji",
                e,
                guild=ctx.guild,
                channel=ctx.channel,
                user=ctx.author,
            )

    try:
        old_settings = backup.get("bot_settings")
        if old_settings:

            def replace_ids(obj):
                if isinstance(obj, dict):
                    new_obj = {}
                    for k, v in obj.items():
                        new_k = id_map.get(str(k), k)
                        new_obj[new_k] = replace_ids(v)
                    return new_obj
                elif isinstance(obj, list):
                    return [replace_ids(i) for i in obj]
                elif isinstance(obj, str):
                    return id_map.get(obj, obj)
                elif isinstance(obj, int):
                    mapped = id_map.get(str(obj))
                    return int(mapped) if mapped else obj
                return obj

            new_settings = replace_ids(old_settings)
            new_settings.pop("guildid", None)
            new_settings.pop("_id", None)
            await bot.settings.push_settings(ctx.guild.id, new_settings)
    except Exception as e:
        bot.log_error(
            "rollback_clone:migrate_settings",
            e,
            guild=ctx.guild,
            channel=ctx.channel,
            user=ctx.author,
        )

    completion_channel = next(
        (
            c
            for c in ctx.guild.text_channels
            if c.permissions_for(ctx.guild.me).send_messages
        ),
        None,
    )
    if completion_channel:
        await completion_channel.send(
            f"{ctx.author.mention}, the rollback is complete!"
        )
