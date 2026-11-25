"""Reaction GIF commands (multi-tier GIF fetching with fallbacks)."""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from core.bot import DebtCollectorBot, bot

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# TASK 3 — Multi-tier GIF fetching with strict fallbacks
# --------------------------------------------------------------------------- #

JsonExtractor = Callable[[Dict[str, Any]], Optional[str]]


def _gif_tiers(reaction: str) -> List[Tuple[str, str, JsonExtractor]]:
    return [
        (
            "OtakuGIFs",
            f"https://api.otakugifs.xyz/gif?reaction={reaction}",
            lambda d: d.get("url"),
        ),
        (
            "PurrBot",
            f"https://api.purrbot.site/v2/img/sfw/{reaction}/gif",
            lambda d: None if d.get("error") else d.get("link"),
        ),
        (
            "Gifukai",
            f"https://gifukai.com/api/gif/{reaction}",
            lambda d: d.get("url") or d.get("gif") or d.get("link"),
        ),
        (
            "waifu.im",
            f"https://api.waifu.im/search?included_tags={reaction}",
            lambda d: (d.get("images") or [{}])[0].get("url"),
        ),
        (
            "nekos.best",
            f"https://nekos.best/api/v2/{reaction}",
            lambda d: (d.get("results") or [{}])[0].get("url"),
        ),
        (
            "nekos.life",
            f"https://nekos.life/api/v2/img/{reaction}",
            lambda d: d.get("url"),
        ),
        (
            "waifu.pics",
            f"https://api.waifu.pics/sfw/{reaction}",
            lambda d: d.get("url"),
        ),
        (
            "nekos.life",
            f"https://nekos.life/api/v2/img/{reaction}",
            lambda d: d.get("url"),
        ),
        (
            "nekos.best",
            f"https://nekos.best/api/v2/{reaction}",
            lambda d: (d.get("results") or [{}])[0].get("url"),
        ),
    ]


async def fetch_reaction_gif(
    session: aiohttp.ClientSession, reaction_key: str
) -> Optional[str]:
    for provider_name, url, extractor in _gif_tiers(reaction_key):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=6.0)
            ) as resp:
                if resp.status == 200:
                    data: Dict[str, Any] = await resp.json()
                    gif_url: Optional[str] = extractor(data)
                    if gif_url:
                        return gif_url
        except Exception as exc:
            log.warning(
                "Provider %s failed for %s: %s", provider_name, reaction_key, exc
            )
    return None


REACTIONS: Dict[str, Tuple[str, str]] = {
    "hug": ("hug", "🤗 **{actor}** hugs **{target}**!"),
    "slap": ("slap", "👋 **{actor}** slaps **{target}**!"),
    "highfive": ("highfive", "🙌 **{actor}** high-fives **{target}**!"),
    "sleep": ("sleep", "😴 **{actor}** drifts off to sleep..."),
    "punch": ("punch", "👊 **{actor}** punches **{target}**!"),
    "wink": ("wink", "😉 **{actor}** winks at **{target}**!"),
    "poke": ("poke", "👉 **{actor}** pokes **{target}**!"),
    "bite": ("bite", "🧛 **{actor}** bites **{target}**!"),
    "pat": ("pat", "✋ **{actor}** pats **{target}**!"),
    "kiss": ("kiss", "💋 **{actor}** kisses **{target}**!"),
    "cuddle": ("cuddle", "🥰 **{actor}** cuddles **{target}**!"),
    "dance": ("dance", "💃 **{actor}** dances!"),
    "cry": ("cry", "😭 **{actor}** cries!"),
}


def _register_reaction_commands(target_bot: DebtCollectorBot) -> None:
    def build(name: str, reaction_key: str, template: str) -> None:
        @app_commands.default_permissions(send_messages=True)
        async def callback(
            ctx: commands.Context, target: Optional[discord.Member] = None
        ) -> None:
            await ctx.defer()
            session: Optional[aiohttp.ClientSession] = target_bot.http_session
            gif_url: Optional[str] = None
            if session is not None and not session.closed:
                gif_url = await fetch_reaction_gif(session, reaction_key)
            actor_name: str = getattr(ctx.author, "display_name", str(ctx.author))
            target_name: str = target.display_name if target is not None else "the air"

            if target is None and "{target}" in template:
                desc = template.format(actor=actor_name, target="the air")
                if "the air" not in desc.lower():
                    pass  # Just safety
            else:
                desc = template.format(actor=actor_name, target=target_name)

            embed: discord.Embed = discord.Embed(
                description=desc,
                color=discord.Color.random(),
            )
            if gif_url is not None:
                embed.set_image(url=gif_url)
            else:
                embed.set_footer(
                    text="All GIF providers are currently unavailable — try again soon."
                )
            await ctx.send(embed=embed)

        callback.__name__ = name
        decorated = app_commands.describe(target="Who to target (optional).")(callback)
        target_bot.hybrid_command(
            name=name, description=f"Send a {name} GIF (with multi-API fallback)."
        )(decorated)

    for command_name, (reaction_key, template) in REACTIONS.items():
        build(command_name, reaction_key, template)


_register_reaction_commands(bot)
