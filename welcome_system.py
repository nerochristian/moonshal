import asyncio
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib import request as urllib_request

import discord
from PIL import Image, ImageDraw, ImageFont

LOGGER = logging.getLogger("theseus-bot.welcome")

CARD_W = 800
CARD_H = 250
AVATAR_SIZE = 150
AVATAR_X = 40
AVATAR_Y = (CARD_H - AVATAR_SIZE) // 2


def _fetch_bytes(url: str) -> bytes:
    request = urllib_request.Request(url, headers={"User-Agent": "ZyphraxHubCommunityBot/1.0"})
    with urllib_request.urlopen(request, timeout=10) as response:
        return response.read()


def _circle_crop(img_bytes: bytes, size: int) -> Image.Image:
    image = Image.open(io.BytesIO(img_bytes)).convert("RGBA").resize((size, size))
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    image.putalpha(mask)
    return image


def _load_font(path: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return ImageFont.load_default()


def _format_joined_at(dt: datetime) -> str:
    return f"{dt.day} {dt.strftime('%B %Y %H:%M')}"


def _build_welcome_card(
    *,
    avatar_bytes: bytes,
    username: str,
    member_number: int,
    server_tag: str,
    background_path: Optional[Path],
) -> bytes:
    if background_path and background_path.exists():
        background = Image.open(background_path).convert("RGBA").resize((CARD_W, CARD_H))
    else:
        background = Image.new("RGBA", (CARD_W, CARD_H), (18, 18, 28, 255))

    overlay = Image.new("RGBA", (CARD_W, CARD_H), (0, 0, 0, 130))
    background = Image.alpha_composite(background, overlay)
    draw = ImageDraw.Draw(background)

    ring_size = AVATAR_SIZE + 10
    ring = Image.new("RGBA", (ring_size, ring_size), (0, 0, 0, 0))
    ImageDraw.Draw(ring).ellipse((0, 0, ring_size, ring_size), fill=(255, 255, 255, 255))
    background.paste(ring, (AVATAR_X - 5, AVATAR_Y - 5), ring)

    avatar = _circle_crop(avatar_bytes, AVATAR_SIZE)
    background.paste(avatar, (AVATAR_X, AVATAR_Y), avatar)

    font_name = _load_font("arialbd.ttf", 44)
    font_tag = _load_font("arial.ttf", 26)
    font_badge = _load_font("arialbd.ttf", 20)

    text_x = AVATAR_X + AVATAR_SIZE + 30

    draw.text(
        (text_x, CARD_H // 2 - 42),
        f"{username[:22]}  |  #{member_number}",
        font=font_name,
        fill=(255, 255, 255, 255),
    )
    draw.text(
        (text_x, CARD_H // 2 + 14),
        server_tag,
        font=font_tag,
        fill=(180, 210, 255, 255),
    )

    badge = "WELCOME!"
    badge_box = draw.textbbox((0, 0), badge, font=font_badge)
    badge_w = badge_box[2] - badge_box[0]
    badge_h = badge_box[3] - badge_box[1]
    padding = 8
    badge_x = CARD_W - badge_w - padding * 2 - 15
    badge_y = CARD_H - badge_h - padding * 2 - 12

    draw.rounded_rectangle(
        [badge_x, badge_y, badge_x + badge_w + padding * 2, badge_y + badge_h + padding * 2],
        radius=6,
        fill=(52, 152, 219, 230),
    )
    draw.text(
        (badge_x + padding, badge_y + padding),
        badge,
        font=font_badge,
        fill=(255, 255, 255, 255),
    )

    buffer = io.BytesIO()
    background.convert("RGB").save(buffer, format="PNG")
    buffer.seek(0)
    return buffer.read()


class WelcomeSystem:
    def __init__(
        self,
        bot: discord.Client,
        *,
        welcome_channel_id: int,
        server_name: str,
        server_tag: str,
        accent_color: int = 0x3498DB,
        background_path: Optional[Path] = None,
    ) -> None:
        self._bot = bot
        self.welcome_channel_id = welcome_channel_id
        self.server_name = server_name
        self.server_tag = server_tag
        self.accent_color = accent_color
        self.background_path = background_path

    def setup(self) -> None:
        @self._bot.event
        async def on_member_join(member: discord.Member) -> None:
            await self.send_welcome(member)

    async def _resolve_channel(
        self,
        channel: Optional[discord.abc.GuildChannel | discord.abc.PrivateChannel] = None,
    ) -> Optional[discord.TextChannel]:
        if channel is None:
            channel = self._bot.get_channel(self.welcome_channel_id)
            if channel is None:
                try:
                    channel = await self._bot.fetch_channel(self.welcome_channel_id)
                except (discord.Forbidden, discord.HTTPException, discord.NotFound):
                    LOGGER.warning("Cannot access welcome channel %s", self.welcome_channel_id)
                    return None
        if not isinstance(channel, discord.TextChannel):
            return None
        return channel

    async def send_welcome(
        self,
        member: discord.Member,
        *,
        channel: Optional[discord.abc.GuildChannel | discord.abc.PrivateChannel] = None,
    ) -> bool:
        channel = await self._resolve_channel(channel)
        if channel is None:
            return False

        avatar_bytes: Optional[bytes] = None
        try:
            avatar_url = member.display_avatar.replace(size=256, format="png").url
            avatar_bytes = await asyncio.to_thread(_fetch_bytes, avatar_url)
        except Exception as exc:
            LOGGER.warning("Avatar fetch failed for %s: %s", member, exc)

        joined_at = member.joined_at or discord.utils.utcnow()
        joined_str = _format_joined_at(joined_at)

        embed = discord.Embed(
            title=f"Welcome System - {self.server_name}",
            color=self.accent_color,
        )
        embed.description = (
            f"Welcome to **{self.server_name}**!\n\n"
            f"**| User:** {member.mention}\n"
            f"**| Joined On:** {joined_str}"
        )

        file: Optional[discord.File] = None
        if avatar_bytes:
            try:
                card_bytes = await asyncio.to_thread(
                    _build_welcome_card,
                    avatar_bytes=avatar_bytes,
                    username=member.display_name,
                    member_number=member.guild.member_count or 0,
                    server_tag=self.server_tag,
                    background_path=self.background_path,
                )
                file = discord.File(io.BytesIO(card_bytes), filename="welcome.png")
                embed.set_image(url="attachment://welcome.png")
            except Exception as exc:
                LOGGER.warning("Card generation failed for %s: %s", member, exc)

        try:
            await channel.send(embed=embed, file=file)
            return True
        except discord.HTTPException as exc:
            LOGGER.warning("Failed to send welcome message for %s: %s", member, exc)
            return False


def init_welcome_system(
    bot: discord.Client,
    *,
    welcome_channel_id: int,
    server_name: str,
    server_tag: str,
    accent_color: int = 0x3498DB,
    background_path: Optional[Path] = None,
) -> WelcomeSystem:
    return WelcomeSystem(
        bot,
        welcome_channel_id=welcome_channel_id,
        server_name=server_name,
        server_tag=server_tag,
        accent_color=accent_color,
        background_path=background_path,
    )
