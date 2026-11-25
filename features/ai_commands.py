"""AI commands: the `ai` group, /ask and /aiopt."""

import logging
import os
from typing import Any, Dict, List, Literal, Optional, Union

import discord
from discord import app_commands
from discord.ext import commands

from utils.pagination import build_pages, send_pages
from core.config import DEFAULT_SETTINGS
from core.permissions import member_has_perms
from core.bot import bot
from core.helpers import sanitize_mass_pings
from features.ai import (
    AI_PRESETS,
    AI_WORD_LIMIT_MAX,
    DEFAULT_PROVIDER_ORDER,
    MODEL_CATALOG,
    PROVIDER_KEYS,
    _ai_daily_key,
    _ai_is_ignored,
    _ai_quota_left,
    _ai_quota_spend,
    _ai_reply_leaks_prompt,
    _ai_save,
    _ai_word_limit,
    _clear_ai_history,
    _format_history,
    _strip_thought_blocks,
    ai_config,
    ai_generate_reply,
    build_system_prompt,
    chunk_text,
    resolve_model,
)

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# /ai command group (admin)
# --------------------------------------------------------------------------- #


async def _require_ai_admin(ctx: commands.Context) -> bool:
    if not isinstance(ctx.author, discord.Member) or not member_has_perms(
        ctx.author, manage_guild=True
    ):
        await ctx.send("❌ You need the **Manage Server** permission.", ephemeral=True)
        return False
    return True


def _saved_mark(saved: bool) -> str:
    return "✅" if saved else "⚠️"


def _saved_suffix(saved: bool) -> str:
    return "" if saved else " (database write failed — change is live but not stored)"


@bot.hybrid_group(
    name="ai",
    description="Manage the AI assistant for this server",
    fallback="info",
)
@app_commands.default_permissions(manage_guild=True)
@commands.guild_only()
async def ai_group(ctx: commands.Context):
    if ctx.invoked_subcommand is not None:
        return
    config = ai_config(ctx.guild.id)
    channels = " ".join(f"<#{c}>" for c in config.get("channels", [])[:10]) or "none"
    await ctx.send(
        f"🤖 AI is **{'enabled' if config['enabled'] else 'disabled'}** in {channels}\n"
        f"Base reply chance **{config['probability']}%** · cooldown **{config['cooldown']:.0f}s**\n"
        f"Use `/ai status` for the full configuration, `/ai setup` to turn it on.",
        ephemeral=True,
    )


@ai_group.command(name="setup", description="Enable or disable the AI in a channel")
@app_commands.describe(
    action="Enable or disable the AI",
    channel="Channel to add/remove (default: this channel)",
    probability="Base percentage chance (0-100) of replying to random messages",
)
async def ai_setup(
    ctx: commands.Context,
    action: Literal["enable", "disable"],
    channel: Optional[discord.TextChannel] = None,
    probability: Optional[app_commands.Range[float, 0.0, 100.0]] = None,
):
    if not await _require_ai_admin(ctx):
        return

    target = channel or ctx.channel
    config = ai_config(ctx.guild.id)
    channels = set(config.get("channels") or [])
    fields: Dict[str, Any] = {}

    if action == "enable":
        channels.add(str(target.id))
        fields["enabled"] = True
        if probability is not None:
            fields["probability"] = float(probability)
    else:
        channels.discard(str(target.id))
        if not channels:
            fields["enabled"] = False

    fields["channels"] = list(channels)
    saved = await _ai_save(ctx.guild.id, **fields)

    detail = ""
    if action == "enable":
        prob = fields.get("probability", config.get("probability"))
        detail = f" Base chance **{prob}%**, cooldown **{config['cooldown']:.0f}s**."
    await ctx.send(
        f"{_saved_mark(saved)} AI {action}d for {target.mention}.{detail}{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(name="status", description="Show the full AI configuration")
async def ai_status(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return

    config = ai_config(ctx.guild.id)
    word_limit: int = _ai_word_limit(config)
    word_limit_text: str = f"{word_limit} words" if word_limit else "off"
    embed = discord.Embed(
        title="🤖 AI configuration",
        color=(
            discord.Color.blurple() if config["enabled"] else discord.Color.dark_grey()
        ),
    )
    embed.add_field(
        name="State",
        value=(
            f"{'🟢 enabled' if config['enabled'] else '🔴 disabled'}\n"
            f"Chance **{config['probability']}%** · cooldown **{config['cooldown']:.0f}s**\n"
            f"Max tokens **{config['max_tokens']}** · temperature **{config['temperature']}**\n"
            f"Reply word limit **{word_limit_text}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Channels",
        value=" ".join(f"<#{c}>" for c in config.get("channels", [])[:20]) or "none",
        inline=False,
    )
    available = [p for p in config["provider_order"] if os.getenv(PROVIDER_KEYS[p])]
    order_text = " → ".join(
        f"**{p}**" if p in available else f"~~{p}~~" for p in config["provider_order"]
    )
    embed.add_field(
        name="Providers",
        value=(
            f"{order_text}\n"
            + "\n".join(
                f"`{p}`: {config['models'].get(p)}" for p in config["provider_order"]
            )
        ),
        inline=False,
    )
    persona = str(config.get("persona") or "")
    embed.add_field(
        name="Persona",
        value=f"```{persona[:500] or 'not set'}```",
        inline=False,
    )
    limit = int(config.get("daily_limit") or 0)
    embed.add_field(
        name="Limits & exclusions",
        value=(
            f"Daily cap: {'unlimited' if limit <= 0 else f'{limit} replies'}"
            f" (used today: {bot.ai_daily.get(_ai_daily_key(ctx.guild.id), 0)})\n"
            f"User personas: {len(config.get('personas') or {})} · "
            f"Channel personas: {len(config.get('channel_personas') or {})}\n"
            f"Opted out: {len(config.get('optout') or [])} · "
            f"Ignored users: {len(config.get('ignored_users') or [])} · "
            f"Ignored roles: {len(config.get('ignored_roles') or [])}"
        ),
        inline=False,
    )
    await ctx.send(embed=embed, ephemeral=True)


@ai_group.command(
    name="persona", description="Set the server-wide personality of the AI"
)
@app_commands.describe(instruction="How the bot should behave (max 2000 characters)")
async def ai_persona(ctx: commands.Context, *, instruction: str):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, persona=instruction[:2000])
    await ctx.send(
        f"{_saved_mark(saved)} AI persona updated ({len(instruction[:2000])} chars)."
        f"{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(
    name="persona_show", description="Show the current AI persona in full"
)
async def ai_persona_show(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    persona = str(config.get("persona") or "")
    channel_personas = config.get("channel_personas") or {}
    lines = [f"```{persona[:1800] or 'not set'}```"]
    if channel_personas:
        lines.append("**Channel overrides:**")
        lines.extend(
            f"<#{cid}> — {text[:120]}"
            for cid, text in list(channel_personas.items())[:10]
        )
    await ctx.send("\n".join(lines)[:2000], ephemeral=True)


@ai_group.command(
    name="persona_clear", description="Reset the AI persona to the default"
)
async def ai_persona_clear(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, persona=DEFAULT_SETTINGS["ai"]["persona"])
    await ctx.send(
        f"{_saved_mark(saved)} Persona reset to default.{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(name="preset", description="Apply a ready-made persona preset")
@app_commands.describe(name="Which preset to apply")
async def ai_preset(
    ctx: commands.Context,
    name: Literal[
        "debt_collector",
        "friendly",
        "sarcastic",
        "professional",
        "unhinged",
        "lore_keeper",
    ],
):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, persona=AI_PRESETS[name])
    await ctx.send(
        f"{_saved_mark(saved)} Persona set to **{name}**:\n```{AI_PRESETS[name][:900]}```"
        f"{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(
    name="channel_persona", description="Set an extra instruction for one channel"
)
@app_commands.describe(
    channel="Which channel (default: this one)",
    instruction="Leave empty to clear this channel's override",
)
async def ai_channel_persona(
    ctx: commands.Context,
    channel: Optional[discord.TextChannel] = None,
    *,
    instruction: Optional[str] = None,
):
    if not await _require_ai_admin(ctx):
        return
    target = channel or ctx.channel
    config = ai_config(ctx.guild.id)
    channel_personas = dict(config.get("channel_personas") or {})
    if instruction:
        channel_personas[str(target.id)] = instruction[:1000]
        message = f"set for {target.mention}"
    else:
        channel_personas.pop(str(target.id), None)
        message = f"cleared for {target.mention}"
    saved = await _ai_save(ctx.guild.id, channel_personas=channel_personas)
    await ctx.send(
        f"{_saved_mark(saved)} Channel persona {message}.{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(
    name="user_persona", description="Set how the AI treats one specific member"
)
@app_commands.describe(user="The member", instruction="How the AI should treat them")
async def ai_user_persona(
    ctx: commands.Context, user: discord.Member, *, instruction: str
):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    personas = dict(config.get("personas") or {})
    if len(personas) >= 300 and str(user.id) not in personas:
        return await ctx.send(
            "❌ This server already has 300 user personas — remove some first.",
            ephemeral=True,
        )
    personas[str(user.id)] = instruction[:1000]
    saved = await _ai_save(ctx.guild.id, personas=personas)
    await ctx.send(
        f"{_saved_mark(saved)} AI behaviour set for {user.mention}.{_saved_suffix(saved)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@ai_group.command(
    name="remove_user_persona", description="Remove a member's custom AI behaviour"
)
async def ai_remove_user_persona(ctx: commands.Context, user: discord.Member):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    personas = dict(config.get("personas") or {})
    if str(user.id) not in personas:
        return await ctx.send(f"ℹ️ Nothing was set for {user.mention}.", ephemeral=True)
    personas.pop(str(user.id), None)
    saved = await _ai_save(ctx.guild.id, personas=personas)
    await ctx.send(
        f"{_saved_mark(saved)} Custom behaviour removed for {user.mention}.{_saved_suffix(saved)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@ai_group.command(
    name="user_persona_list", description="List every per-user AI instruction"
)
async def ai_user_persona_list(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return
    personas = ai_config(ctx.guild.id).get("personas") or {}
    if not personas:
        return await ctx.send("ℹ️ No per-user instructions are set.", ephemeral=True)
    lines: List[str] = []
    for user_id, instruction in personas.items():
        member = ctx.guild.get_member(int(user_id)) if user_id.isdigit() else None
        name = member.display_name if member else f"user {user_id}"
        lines.append(f"**{name}** — {instruction[:150]}")
    pages = build_pages(
        "Per-user AI instructions", lines, discord.Color.blurple(), per_page=8
    )
    await send_pages(ctx, pages, ephemeral=True)


@ai_group.command(
    name="probability", description="Set the base chance the AI replies unprompted"
)
@app_commands.describe(percent="0 = only when mentioned or replied to, 100 = always")
async def ai_probability(
    ctx: commands.Context, percent: app_commands.Range[float, 0.0, 100.0]
):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, probability=float(percent))
    await ctx.send(
        f"{_saved_mark(saved)} Base reply chance set to **{percent}%**.{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(
    name="cooldown", description="Minimum seconds between AI replies in a channel"
)
@app_commands.describe(seconds="0-3600 seconds")
async def ai_cooldown(ctx: commands.Context, seconds: app_commands.Range[int, 0, 3600]):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, cooldown=float(seconds))
    await ctx.send(
        f"{_saved_mark(saved)} Cooldown set to **{seconds}s**.{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(
    name="wordlimit",
    description="Refuse AI replies longer than this many words",
)
@app_commands.describe(
    words="Max words allowed in a reply. 0 turns the limit off (not recommended)."
)
async def ai_wordlimit(
    ctx: commands.Context,
    words: Optional[app_commands.Range[int, 0, AI_WORD_LIMIT_MAX]] = None,
):
    if not await _require_ai_admin(ctx):
        return

    config = ai_config(ctx.guild.id)
    current: int = _ai_word_limit(config)

    if words is None:
        state: str = f"**{current} words**" if current else "**off**"
        return await ctx.send(
            f"U0001f4cf The AI reply word limit is {state}.\n"
            f"Replies longer than this are discarded instead of sent, which stops "
            f"hallucinated essays and leaked instructions from reaching chat.\n"
            f"Change it with `{ctx.clean_prefix}ai wordlimit <words>`.",
            ephemeral=True,
        )

    saved = await _ai_save(ctx.guild.id, word_limit=int(words))
    if int(words) == 0:
        body: str = (
            "⚠️ Word limit **disabled**. Long hallucinated replies will no "
            "longer be blocked by length — the instruction-leak checks still apply."
        )
    else:
        body = (
            f"U0001f4cf Word limit set to **{int(words)} words**. Anything longer is "
            "discarded rather than sent."
        )
    log.info(
        "AI word limit set to %s in guild %s by %s.",
        int(words),
        ctx.guild.id,
        ctx.author,
    )
    await ctx.send(f"{_saved_mark(saved)} {body}{_saved_suffix(saved)}", ephemeral=True)


@ai_group.command(name="tuning", description="Set reply length and creativity")
@app_commands.describe(
    max_tokens="Maximum reply length in tokens (50-1000, default 300)",
    temperature="Creativity, 0.0 = predictable, 2.0 = chaotic (default 0.9)",
)
async def ai_tuning(
    ctx: commands.Context,
    max_tokens: Optional[app_commands.Range[int, 50, 1000]] = None,
    temperature: Optional[app_commands.Range[float, 0.0, 2.0]] = None,
):
    if not await _require_ai_admin(ctx):
        return
    if max_tokens is None and temperature is None:
        config = ai_config(ctx.guild.id)
        return await ctx.send(
            f"Current tuning — max tokens **{config['max_tokens']}**, "
            f"temperature **{config['temperature']}**.",
            ephemeral=True,
        )
    fields: Dict[str, Any] = {}
    if max_tokens is not None:
        fields["max_tokens"] = int(max_tokens)
    if temperature is not None:
        fields["temperature"] = float(temperature)
    saved = await _ai_save(ctx.guild.id, **fields)
    await ctx.send(
        f"{_saved_mark(saved)} Tuning updated: "
        + ", ".join(f"{k} = {v}" for k, v in fields.items())
        + _saved_suffix(saved),
        ephemeral=True,
    )


@ai_group.command(
    name="limit", description="Cap how many AI replies this server can use per day"
)
@app_commands.describe(replies="0 = unlimited")
async def ai_limit(ctx: commands.Context, replies: app_commands.Range[int, 0, 10000]):
    if not await _require_ai_admin(ctx):
        return
    saved = await _ai_save(ctx.guild.id, daily_limit=int(replies))
    text = "unlimited" if replies == 0 else f"{replies} replies/day"
    await ctx.send(
        f"{_saved_mark(saved)} Daily limit set to **{text}**.{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_group.command(name="models", description="List the AI models you can pick from")
@app_commands.describe(provider="Optional: only show one provider's models")
async def ai_models(
    ctx: commands.Context,
    provider: Optional[Literal["openrouter", "gemini", "groq"]] = None,
):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    providers = [provider] if provider else list(MODEL_CATALOG.keys())
    embed = discord.Embed(
        title="Available AI models",
        description=(
            "Set one with `/ai model <provider> <model>`. Any model string the provider "
            "accepts will work — this list is just the curated shortlist."
        ),
        color=discord.Color.blurple(),
    )
    for name in providers:
        current = config["models"].get(name)
        options = "\n".join(
            f"{'▶️' if option == current else '•'} `{option}`"
            for option in MODEL_CATALOG[name]
        )
        if current and current not in MODEL_CATALOG[name]:
            options += f"\n▶️ `{current}` *(custom)*"
        key_state = "🔑 key set" if os.getenv(PROVIDER_KEYS[name]) else "🚫 no API key"
        embed.add_field(
            name=f"{name} — {key_state}", value=options[:1024], inline=False
        )
    await ctx.send(embed=embed, ephemeral=True)


@ai_group.command(name="model", description="Set which model a provider should use")
@app_commands.describe(provider="Which provider", model="Model name (autocompletes)")
async def ai_model(
    ctx: commands.Context,
    provider: Literal["openrouter", "gemini", "groq"],
    model: str,
):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    models = dict(config.get("models") or {})
    models[provider] = resolve_model(provider, model[:100])
    saved = await _ai_save(ctx.guild.id, models=models)
    known = (
        " "
        if models[provider] in MODEL_CATALOG[provider]
        else " *(custom model — untested)* "
    )
    await ctx.send(
        f"{_saved_mark(saved)} `{provider}` will now use `{models[provider]}`.{known}"
        f"{_saved_suffix(saved)}",
        ephemeral=True,
    )


@ai_model.autocomplete("model")
async def _ai_model_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    provider = getattr(interaction.namespace, "provider", None)
    options = MODEL_CATALOG.get(provider) or [
        m for v in MODEL_CATALOG.values() for m in v
    ]
    lowered = (current or "").lower()
    return [
        app_commands.Choice(name=option[:100], value=option[:100])
        for option in options
        if lowered in option.lower()
    ][:25]


@ai_group.command(
    name="providers", description="Show provider health and fallback order"
)
async def ai_providers(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    embed = discord.Embed(
        title="AI providers",
        description=(
            "Fallback order: "
            + " → ".join(f"**{p}**" for p in config["provider_order"])
            + "\nChange it with `/ai order`."
        ),
        color=discord.Color.blurple(),
    )
    for name in config["provider_order"]:
        stats = bot.ai_stats.get(name, {})
        embed.add_field(
            name=name,
            value=(
                f"{'🔑 key configured' if os.getenv(PROVIDER_KEYS[name]) else '🚫 no API key'}\n"
                f"model: `{config['models'].get(name)}`\n"
                f"since restart — ok **{stats.get('ok', 0)}**, "
                f"failed **{stats.get('fail', 0)}**, blocked **{stats.get('blocked', 0)}**\n"
                f"last error: `{(stats.get('last_error') or 'none')[:100]}`"
            ),
            inline=False,
        )
    await ctx.send(embed=embed, ephemeral=True)


@ai_group.command(name="order", description="Set the provider fallback order")
@app_commands.describe(
    first="Tried first", second="Tried if the first fails", third="Last resort"
)
async def ai_order(
    ctx: commands.Context,
    first: Literal["openrouter", "gemini", "groq"],
    second: Optional[Literal["openrouter", "gemini", "groq"]] = None,
    third: Optional[Literal["openrouter", "gemini", "groq"]] = None,
):
    if not await _require_ai_admin(ctx):
        return
    order: List[str] = []
    for provider in (first, second, third):
        if provider and provider not in order:
            order.append(provider)
    for provider in DEFAULT_PROVIDER_ORDER:
        if provider not in order:
            order.append(provider)
    saved = await _ai_save(ctx.guild.id, provider_order=order)
    await ctx.send(
        f"{_saved_mark(saved)} Provider order: "
        + " → ".join(f"**{p}**" for p in order)
        + _saved_suffix(saved),
        ephemeral=True,
    )


@ai_group.command(
    name="ignore", description="Exclude users or roles from the AI entirely"
)
@app_commands.describe(action="add, remove or list", target="A member or a role")
async def ai_ignore(
    ctx: commands.Context,
    action: Literal["add", "remove", "list"],
    target: Optional[Union[discord.Member, discord.Role]] = None,
):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    users = list(config.get("ignored_users") or [])
    roles = list(config.get("ignored_roles") or [])

    if action == "list":
        text = "**Users:** " + (" ".join(f"<@{u}>" for u in users[:20]) or "none")
        text += "\n**Roles:** " + (" ".join(f"<@&{r}>" for r in roles[:20]) or "none")
        return await ctx.send(
            text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
        )

    if target is None:
        return await ctx.send(
            "❌ Give me a member or a role to add/remove.", ephemeral=True
        )

    bucket = roles if isinstance(target, discord.Role) else users
    target_id = str(target.id)
    if action == "add":
        if target_id not in bucket:
            bucket.append(target_id)
        verb = "will be ignored by"
    else:
        if target_id in bucket:
            bucket.remove(target_id)
        verb = "is no longer ignored by"

    saved = await _ai_save(ctx.guild.id, ignored_users=users, ignored_roles=roles)
    await ctx.send(
        f"{_saved_mark(saved)} {target.mention} {verb} the AI.{_saved_suffix(saved)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@ai_group.command(name="reset", description="Wipe the AI's memory of a channel")
@app_commands.describe(channel="Channel to forget (default: this one)")
async def ai_reset(
    ctx: commands.Context, channel: Optional[discord.TextChannel] = None
):
    if not await _require_ai_admin(ctx):
        return
    target = channel or ctx.channel
    await _clear_ai_history(target.id)
    await ctx.send(f"🧠 Memory wiped for {target.mention}.", ephemeral=True)


@ai_group.command(name="stats", description="AI usage since the last restart")
async def ai_stats_cmd(ctx: commands.Context):
    if not await _require_ai_admin(ctx):
        return
    config = ai_config(ctx.guild.id)
    used = bot.ai_daily.get(_ai_daily_key(ctx.guild.id), 0)
    limit = int(config.get("daily_limit") or 0)
    totals = {
        key: sum(stat.get(key, 0) for stat in bot.ai_stats.values())
        for key in ("ok", "fail", "blocked")
    }
    tracked = sum(len(v) for v in bot.ai_history_buffer.values())
    await ctx.send(
        embed=discord.Embed(
            title="AI stats",
            description=(
                f"**Today in this server:** {used} replies"
                + (f" / {limit} allowed" if limit else " (no cap)")
                + f"\n**Since restart (all servers):** {totals['ok']} ok · "
                f"{totals['fail']} failed · {totals['blocked']} blocked\n"
                f"**Channels in memory:** {len(bot.ai_history_buffer)} "
                f"({tracked} messages buffered, {len(bot.ai_history_dirty)} awaiting save)"
            ),
            color=discord.Color.blurple(),
        ),
        ephemeral=True,
    )


# --------------------------------------------------------------------------- #
# Public AI commands (anyone can use these)
# --------------------------------------------------------------------------- #


@bot.hybrid_command(name="ask", description="Ask the AI a one-off question (no memory)")
@app_commands.describe(prompt="What do you want to ask?")
@commands.guild_only()
@commands.cooldown(1, 10.0, commands.BucketType.user)
async def ask_cmd(ctx: commands.Context, *, prompt: str):
    config = ai_config(ctx.guild.id)
    if not any(os.getenv(PROVIDER_KEYS[p]) for p in config["provider_order"]):
        return await ctx.send(
            "❌ No AI provider is configured on this bot.", ephemeral=True
        )
    if _ai_is_ignored(config, ctx.author):
        return await ctx.send(
            "❌ You are excluded from AI features in this server.", ephemeral=True
        )
    if _ai_quota_left(ctx.guild.id, int(config.get("daily_limit") or 0)) <= 0:
        return await ctx.send("❌ This server hit its daily AI limit.", ephemeral=True)

    await ctx.defer(ephemeral=True)
    _ai_quota_spend(ctx.guild.id)
    asker: Dict[str, str] = {str(ctx.author.id): ctx.author.display_name}
    system_prompt = build_system_prompt(
        ctx.guild,
        ctx.channel.id,
        config,
        [str(ctx.author.id)],
        speaker_id=str(ctx.author.id),
        names=asker,
    )
    messages = _format_history(
        [],
        asker,
        {
            "role": "user",
            "author": ctx.author.display_name,
            "author_id": ctx.author.id,
            "content": prompt[:1500],
        },
    )
    reply, provider = await ai_generate_reply(
        ctx.guild.id, system_prompt, messages, config
    )
    if not reply:
        return await ctx.send(
            "⚠️ Every AI provider failed. Try again shortly.", ephemeral=True
        )

    reply = _strip_thought_blocks(reply)
    limit: int = _ai_word_limit(config)
    leak: Optional[str] = _ai_reply_leaks_prompt(reply, system_prompt, limit)
    if leak is not None:
        log.warning("Suppressed /ask reply in guild %s (%s).", ctx.guild.id, leak)
        bot.log_error(f"ai:leak_suppressed ({leak})", reply[:200], guild=ctx.guild)
        hint: str = (
            f" It ran past the **{limit}-word** limit — raise it with "
            f"`{ctx.clean_prefix}ai wordlimit` or ask something narrower."
            if leak.startswith("over the word limit")
            else " It looked like the bot's own instructions rather than an answer."
        )
        return await ctx.send(
            f"⚠️ I blocked that answer before sending it.{hint}", ephemeral=True
        )

    chunks = chunk_text(sanitize_mass_pings(reply))
    await ctx.send(
        chunks[0],
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    for extra in chunks[1:]:
        await ctx.send(
            extra, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
        )


@bot.hybrid_command(
    name="aiopt", description="Control whether the AI may read or reply to you"
)
@app_commands.describe(choice="out = ignore me completely, in = normal, status = check")
@commands.guild_only()
async def aiopt_cmd(
    ctx: commands.Context, choice: Literal["out", "in", "status"] = "status"
):
    config = ai_config(ctx.guild.id)
    optout = list(config.get("optout") or [])
    user_id = str(ctx.author.id)

    if choice == "status":
        state = (
            "**excluded** from AI memory and replies"
            if user_id in optout
            else "included normally"
        )
        return await ctx.send(f"🤖 You are currently {state}.", ephemeral=True)

    if choice == "out" and user_id not in optout:
        optout.append(user_id)
    elif choice == "in" and user_id in optout:
        optout.remove(user_id)

    saved = await _ai_save(ctx.guild.id, optout=optout)
    text = (
        "🚫 Your messages will no longer be stored or answered by the AI."
        if choice == "out"
        else "✅ The AI will treat your messages normally again."
    )
    await ctx.send(f"{_saved_mark(saved)} {text}{_saved_suffix(saved)}", ephemeral=True)
