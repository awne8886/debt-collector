import discord
from discord.ext import commands
from typing import List, Optional, Union

# This import assumes we need is_superuser for interaction_check
# But to avoid circular imports we can either import it inside the method or inject it.
# We will import it locally or from main if needed. Since utils is separate, let's see where is_superuser comes from.
# In bot.py and main.py, it's defined at the top. Let's just import it locally inside the method to avoid circular imports.

EMBED_DESCRIPTION_LIMIT: int = 4000

def paginate_lines(
    lines: List[str],
    per_page: int = 10,
    char_budget: int = EMBED_DESCRIPTION_LIMIT,
) -> List[str]:
    """Group pre-formatted lines into page bodies that respect Discord's limits."""
    pages: List[str] = []
    current: List[str] = []
    size: int = 0
    for raw in lines:
        line: str = raw[:char_budget]
        if current and (len(current) >= per_page or size + len(line) + 1 > char_budget):
            pages.append("\n".join(current))
            current = []
            size = 0
        current.append(line)
        size += len(line) + 1
    if current:
        pages.append("\n".join(current))
    return pages or ["*Nothing to show.*"]

class Paginator(discord.ui.View):
    """First / prev / next / last controls over a list of pre-rendered embeds."""

    def __init__(
        self,
        embeds: List[discord.Embed],
        author_id: int,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.embeds: List[discord.Embed] = embeds
        self.author_id: int = author_id
        self.index: int = 0
        self.message: Optional[discord.Message] = None
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        at_start: bool = self.index == 0
        at_end: bool = self.index >= len(self.embeds) - 1
        self.first_page.disabled = at_start
        self.prev_page.disabled = at_start
        self.next_page.disabled = at_end
        self.last_page.disabled = at_end
        self.counter.label = f"{self.index + 1}/{len(self.embeds)}"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Avoid circular imports by importing is_superuser here
        # is_superuser is in bot.py / main.py
        import sys

        is_superuser = None
        if "main" in sys.modules:
            is_superuser = getattr(sys.modules["main"], "is_superuser", None)
        if is_superuser is None and "bot" in sys.modules:
            is_superuser = getattr(sys.modules["bot"], "is_superuser", None)

        is_su = is_superuser(interaction.user) if is_superuser else False

        if interaction.user.id == self.author_id or is_su:
            return True
        await interaction.response.send_message(
            "❌ Only the person who ran the command can page through this.",
            ephemeral=True,
        )
        return False

    async def _show(self, interaction: discord.Interaction) -> None:
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=self.embeds[self.index], view=self
        )

    async def on_timeout(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary)
    async def first_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = 0
        await self._show(interaction)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary)
    async def prev_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = max(0, self.index - 1)
        await self._show(interaction)

    @discord.ui.button(label="1/1", style=discord.ButtonStyle.secondary, disabled=True)
    async def counter(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.defer()

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary)
    async def next_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = min(len(self.embeds) - 1, self.index + 1)
        await self._show(interaction)

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary)
    async def last_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.index = len(self.embeds) - 1
        await self._show(interaction)

def build_pages(
    title: str,
    lines: List[str],
    color: Union[discord.Color, int],
    per_page: int = 10,
    thumbnail: Optional[str] = None,
    footer: Optional[str] = None,
) -> List[discord.Embed]:
    """Turn a flat list of lines into one embed per page."""
    embeds: List[discord.Embed] = []
    for body in paginate_lines(lines, per_page=per_page):
        embed: discord.Embed = discord.Embed(title=title, description=body, color=color)
        if thumbnail is not None:
            embed.set_thumbnail(url=thumbnail)
        if footer is not None:
            embed.set_footer(text=footer)
        embeds.append(embed)
    return embeds

async def send_pages(
    ctx: commands.Context,
    embeds: List[discord.Embed],
    ephemeral: bool = False,
) -> None:
    """Send a single embed plainly, or several behind a Paginator view."""
    if not embeds:
        await ctx.send("*Nothing to show.*", ephemeral=ephemeral)
        return

    total: int = len(embeds)
    if total > 1:
        for position, embed in enumerate(embeds, start=1):
            existing: str = embed.footer.text or ""
            marker: str = f"Page {position}/{total}"
            embed.set_footer(text=f"{existing} · {marker}" if existing else marker)

    if total == 1:
        await ctx.send(embed=embeds[0], ephemeral=ephemeral)
        return

    view: Paginator = Paginator(embeds, ctx.author.id)
    view.message = await ctx.send(embed=embeds[0], view=view, ephemeral=ephemeral)
