import asyncio
from collections import Counter
import io
import logging
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands

from components_v2 import branded_panel_container, ensure_layout_view_action_rows
from transcript import generate_html_transcript


LOGGER = logging.getLogger("theseus-bot.ticket-system")
THESEUS_BLUE = 0x3498DB
SUCCESS_GREEN = 0x2ECC71
WARNING_GOLD = 0xF1C40F
ERROR_RED = 0xE74C3C
TICKET_CLOSE_DELAY_SECONDS = 5
DEFAULT_TICKET_CATEGORY = "general"
CATEGORY_ALIASES = {
    "other": "general",
    "support": "general",
}
CATEGORY_CONFIGS: dict[str, dict[str, str]] = {
    "utility": {
        "panel_label": "ZyphraxHub Community Utility Issues",
        "display_label": "Utility Issue",
        "description": "Issues with ZyphraxHub Community systems or tools",
        "channel_prefix": "utility",
        "emoji_name": "theseus_ticket_utility",
        "icon_file": "magnifining glass.png",
    },
    "appeal": {
        "panel_label": "Ban Appeal",
        "display_label": "Ban Appeal",
        "description": "Appeal a moderation action",
        "channel_prefix": "appeal",
        "emoji_name": "theseus_ticket_appeal",
        "icon_file": "court.png",
    },
    "report": {
        "panel_label": "User / Staff Reports",
        "display_label": "Report",
        "description": "Report a user or staff member",
        "channel_prefix": "report",
        "emoji_name": "theseus_ticket_report",
        "icon_file": "stop.png",
    },
    "creator": {
        "panel_label": "Apply for Content Creator",
        "display_label": "Content Creator Application",
        "description": "Apply for the content creator team",
        "channel_prefix": "creator",
        "emoji_name": "theseus_ticket_creator",
        "icon_file": "media.png",
    },
    "general": {
        "panel_label": "General Support",
        "display_label": "General Support",
        "description": "General help and questions",
        "channel_prefix": "support",
        "emoji_name": "theseus_ticket_support",
        "icon_file": "message.png",
    },
}


def make_embed(title: str, description: str, color: int = THESEUS_BLUE) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


def success_embed(title: str, description: str) -> discord.Embed:
    return make_embed(title, description, SUCCESS_GREEN)


def warning_embed(title: str, description: str) -> discord.Embed:
    return make_embed(title, description, WARNING_GOLD)


def error_embed(title: str, description: str) -> discord.Embed:
    return make_embed(title, description, ERROR_RED)


async def send_interaction_message(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    ephemeral: bool = False,
    file: Optional[discord.File] = None,
) -> None:
    payload: dict[str, Any] = {"ephemeral": ephemeral}
    if content is not None:
        payload["content"] = content
    if embed is not None:
        payload["embed"] = embed
    if file is not None:
        payload["file"] = file

    if interaction.response.is_done():
        await interaction.followup.send(**payload)
        return

    await interaction.response.send_message(**payload)


def _panel_category_label(category: str) -> str:
    normalized = CATEGORY_ALIASES.get((category or DEFAULT_TICKET_CATEGORY).strip().lower(), (category or DEFAULT_TICKET_CATEGORY).strip().lower())
    config = CATEGORY_CONFIGS.get(normalized, CATEGORY_CONFIGS[DEFAULT_TICKET_CATEGORY])
    return config["panel_label"]


def _slugify_display_name(name: str) -> str:
    value = (name or "").strip()
    if not value:
        return "user"

    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[0-9]+", "", value)
    value = re.sub(r"[^a-z\s-]", "", value)
    value = re.sub(r"\s+", "-", value).strip("-")
    value = re.sub(r"-{2,}", "-", value)
    return value or "user"


def _unique_channel_name(base: str, existing_names: set[str]) -> str:
    candidate = (base or "ticket").strip().lower()
    if candidate and candidate not in existing_names:
        return candidate

    alphabet = "abcdefghijklmnopqrstuvwxyz"
    suffix = 0
    while True:
        suffix += 1
        current = suffix
        letters: list[str] = []
        while current > 0:
            current -= 1
            letters.append(alphabet[current % 26])
            current //= 26
        candidate = f"{base}-" + "".join(reversed(letters))
        if candidate not in existing_names:
            return candidate


def _truncate_for_field(text: str, limit: int = 1024) -> str:
    value = (text or "").strip()
    if not value:
        return "No details provided."
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


class TicketStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_settings (
                    guild_id INTEGER PRIMARY KEY,
                    category_id INTEGER,
                    support_role_id INTEGER,
                    log_channel_id INTEGER
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tickets (
                    channel_id INTEGER PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    ticket_number INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    details TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    claimed_by INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT,
                    control_message_id INTEGER
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tickets_guild_status
                ON tickets(guild_id, status)
                """
            )
            connection.commit()

    def get_settings(self, guild_id: int) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT guild_id, category_id, support_role_id, log_channel_id
                FROM ticket_settings
                WHERE guild_id = ?
                """,
                (guild_id,),
            ).fetchone()
        return dict(row) if row is not None else {}

    def save_settings(
        self,
        guild_id: int,
        *,
        category_id: int,
        support_role_id: int,
        log_channel_id: Optional[int],
    ) -> dict[str, Any]:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ticket_settings (guild_id, category_id, support_role_id, log_channel_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    category_id = excluded.category_id,
                    support_role_id = excluded.support_role_id,
                    log_channel_id = excluded.log_channel_id
                """,
                (guild_id, category_id, support_role_id, log_channel_id),
            )
            connection.commit()
        return self.get_settings(guild_id)

    def get_ticket(self, channel_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM tickets WHERE channel_id = ?", (channel_id,)).fetchone()
        return dict(row) if row is not None else None

    def get_open_ticket_for_user(self, guild_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM tickets
                WHERE guild_id = ? AND user_id = ? AND status = 'open'
                ORDER BY ticket_number DESC
                LIMIT 1
                """,
                (guild_id, user_id),
            ).fetchone()
        return dict(row) if row is not None else None

    def create_ticket(
        self,
        *,
        guild_id: int,
        channel_id: int,
        user_id: int,
        category: str,
        details: str,
    ) -> int:
        with self._connect() as connection:
            next_row = connection.execute(
                "SELECT COALESCE(MAX(ticket_number), 0) + 1 AS next_number FROM tickets WHERE guild_id = ?",
                (guild_id,),
            ).fetchone()
            ticket_number = int(next_row["next_number"])
            connection.execute(
                """
                INSERT INTO tickets (channel_id, guild_id, user_id, ticket_number, category, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (channel_id, guild_id, user_id, ticket_number, category, details),
            )
            connection.commit()
        return ticket_number

    def set_control_message_id(self, channel_id: int, message_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE tickets SET control_message_id = ? WHERE channel_id = ?",
                (message_id, channel_id),
            )
            connection.commit()

    def claim_ticket(self, channel_id: int, staff_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tickets
                SET claimed_by = ?
                WHERE channel_id = ? AND status = 'open' AND claimed_by IS NULL
                """,
                (staff_id, channel_id),
            )
            connection.commit()
        return cursor.rowcount > 0

    def close_ticket(self, channel_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tickets
                SET status = 'closed', closed_at = CURRENT_TIMESTAMP
                WHERE channel_id = ?
                """,
                (channel_id,),
            )
            connection.commit()
        return cursor.rowcount > 0


class TicketSystem:
    def __init__(
        self,
        bot: commands.Bot,
        *,
        base_dir: Path,
        store_path: Path,
        allowed_role_id: int,
    ) -> None:
        self.bot = bot
        self.base_dir = base_dir
        self.icon_dir = base_dir / "icon pack"
        self.store = TicketStore(store_path)
        self.allowed_role_id = allowed_role_id

    def setup(self) -> None:
        self.store.initialize()
        self.bot.add_view(TicketPanelView(self))
        self.bot.add_view(TicketThreadView(self))

    @staticmethod
    def normalize_category(category: Optional[str]) -> str:
        value = (category or DEFAULT_TICKET_CATEGORY).strip().lower()
        value = CATEGORY_ALIASES.get(value, value)
        if value not in CATEGORY_CONFIGS:
            return DEFAULT_TICKET_CATEGORY
        return value

    @staticmethod
    def normalize_channel_name(name: str) -> Optional[str]:
        value = (name or "").strip().lower()
        value = re.sub(r"[^a-z0-9-]", "-", value)
        value = re.sub(r"-{2,}", "-", value).strip("-")
        return value or None

    def get_panel_logo_url(self) -> Optional[str]:
        user = getattr(self.bot, "user", None)
        if user is None:
            return None
        try:
            return str(user.display_avatar.url)
        except Exception:
            return None

    def get_panel_banner_url(self, guild: discord.Guild) -> Optional[str]:
        banner = getattr(guild, "banner", None)
        if banner is None:
            return None
        try:
            return str(banner.replace(size=1024).url)
        except Exception:
            try:
                return str(banner.url)
            except Exception:
                return None

    def get_member_avatar_url(self, guild: Optional[discord.Guild], user_id: int) -> Optional[str]:
        if guild is None:
            return None
        member = guild.get_member(user_id)
        if member is None:
            return None
        try:
            return str(member.display_avatar.url)
        except Exception:
            return None

    def format_ticket_user(self, guild: Optional[discord.Guild], user_id: Optional[int]) -> str:
        if not user_id:
            return "Unassigned"

        if guild is not None:
            member = guild.get_member(int(user_id))
            if member is not None:
                return f"<@{member.id}> `{member.display_name}`"

        user = self.bot.get_user(int(user_id))
        if user is not None:
            return f"<@{user.id}> `{user}`"

        return f"`{user_id}`"

    def build_ticket_thread_view(
        self,
        *,
        guild: Optional[discord.Guild],
        ticket: dict[str, Any],
    ) -> "TicketThreadView":
        return TicketThreadView(
            self,
            opener_avatar_url=self.get_member_avatar_url(guild, int(ticket["user_id"])),
            category=str(ticket["category"]),
            details=(ticket.get("details") or "").strip(),
            claimed_by=ticket.get("claimed_by"),
        )

    def build_ticket_close_dm_view(
        self,
        *,
        guild: discord.Guild,
        ticket: dict[str, Any],
        closer: discord.Member,
        reason: Optional[str],
        messages: list[discord.Message],
    ) -> discord.ui.LayoutView:
        participant_counts = Counter(message.author.id for message in messages)
        participant_lines = []
        for user_id, count in participant_counts.most_common(10):
            suffix = "msg" if count == 1 else "msgs"
            participant_lines.append(f"{self.format_ticket_user(guild, user_id)} - {count} {suffix}")

        details_block = "\n".join(
            [
                "**Ticket Details**",
                f"**Category:** `{_panel_category_label(ticket['category'])}`",
                f"**Close Reason:** `{(reason or 'No reason provided').strip()}`",
                f"**Closed by:** {self.format_ticket_user(guild, closer.id)}",
                f"**Claimed by:** {self.format_ticket_user(guild, ticket.get('claimed_by'))}",
                f"**Total Messages:** `{len(messages)}`",
            ]
        )

        participants_block = "**Participants**\n" + (
            "\n".join(participant_lines) if participant_lines else "No participants recorded."
        )

        container = branded_panel_container(
            title="Ticket Closed",
            description=(
                "Thank you for opening a support ticket. We appreciate you reaching out to us.\n"
                "If you need any further assistance or have additional questions, please don't hesitate to open another ticket and we'll be happy to help."
            ),
            logo_url=self.get_panel_logo_url(),
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.TextDisplay(details_block))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.TextDisplay(participants_block))

        view = discord.ui.LayoutView(timeout=None)
        view.add_item(container)
        return ensure_layout_view_action_rows(view)

    def build_ticket_embed(self, ticket: dict[str, Any]) -> discord.Embed:
        opener_id = ticket["user_id"]
        claimed_by = ticket.get("claimed_by")
        details = _truncate_for_field(discord.utils.escape_markdown(ticket.get("details") or ""), limit=1024)
        category_label = _panel_category_label(ticket["category"])

        embed = make_embed(
            category_label,
            "Please wait for a staff member to respond.",
        )
        embed.add_field(name="Ticket #", value=str(ticket["ticket_number"]), inline=True)
        embed.add_field(name="Category", value=category_label, inline=True)
        embed.add_field(name="Opened By", value=f"<@{opener_id}> (`{opener_id}`)", inline=False)
        embed.add_field(
            name="Assigned Staff",
            value=f"<@{claimed_by}> (`{claimed_by}`)" if claimed_by else "Unassigned",
            inline=False,
        )
        embed.add_field(name="Details", value=details, inline=False)
        embed.set_footer(text="ZyphraxHub Community Team")
        return embed

    def build_settings_embed(self, guild: discord.Guild, settings: dict[str, Any]) -> discord.Embed:
        category_id = settings.get("category_id")
        support_role_id = settings.get("support_role_id")
        log_channel_id = settings.get("log_channel_id")

        category = guild.get_channel(category_id) if isinstance(category_id, int) else None
        support_role = guild.get_role(support_role_id) if isinstance(support_role_id, int) else None
        log_channel = guild.get_channel(log_channel_id) if isinstance(log_channel_id, int) else None

        embed = make_embed("Ticket Settings", "Current ticket configuration for this server.")
        embed.add_field(
            name="Ticket Category",
            value=f"{category.name} (`{category.id}`)" if isinstance(category, discord.CategoryChannel) else "Not configured",
            inline=False,
        )
        embed.add_field(
            name="Support Role",
            value=support_role.mention if support_role is not None else f"<@&{self.allowed_role_id}> (fallback)",
            inline=False,
        )
        embed.add_field(
            name="Log Channel",
            value=log_channel.mention if isinstance(log_channel, discord.TextChannel) else "Not configured",
            inline=False,
        )
        return embed

    def is_management_member(self, member: discord.Member) -> bool:
        if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
            return True
        return any(role.id == self.allowed_role_id for role in member.roles)

    def is_ticket_staff(self, member: discord.Member, settings: Optional[dict[str, Any]] = None) -> bool:
        if member.guild_permissions.administrator or member.guild_permissions.manage_channels:
            return True

        role_ids = {self.allowed_role_id}
        if settings is not None:
            support_role_id = settings.get("support_role_id")
            if isinstance(support_role_id, int) and support_role_id > 0:
                role_ids.add(support_role_id)

        member_role_ids = {role.id for role in member.roles}
        return bool(role_ids & member_role_ids)

    async def ensure_panel_emojis(self, guild: discord.Guild) -> dict[str, discord.Emoji]:
        emojis: dict[str, discord.Emoji] = {}
        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)
        can_manage_emojis = me is not None and me.guild_permissions.manage_emojis_and_stickers

        for category, config in CATEGORY_CONFIGS.items():
            emoji_name = config["emoji_name"]
            existing = discord.utils.get(guild.emojis, name=emoji_name)
            if existing is not None:
                emojis[category] = existing
                continue

            if not can_manage_emojis:
                continue

            icon_path = self.icon_dir / config["icon_file"]
            if not icon_path.exists():
                LOGGER.warning("Missing ticket icon for %s at %s", category, icon_path)
                continue

            try:
                emoji = await guild.create_custom_emoji(
                    name=emoji_name,
                    image=icon_path.read_bytes(),
                    reason="Upload ZyphraxHub Community ticket panel icons",
                )
            except (discord.Forbidden, discord.HTTPException) as exc:
                LOGGER.warning("Failed to create ticket emoji %s in guild %s: %s", emoji_name, guild.id, exc)
                continue

            emojis[category] = emoji

        return emojis

    async def ensure_management_access(self, interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This command can only be used in a server."),
                ephemeral=True,
            )
            return False

        if self.is_management_member(interaction.user):
            return True

        await send_interaction_message(
            interaction,
            embed=error_embed(
                "Permission Denied",
                f"You need the <@&{self.allowed_role_id}> role or server management permissions to use this command.",
            ),
            ephemeral=True,
        )
        return False

    async def ensure_ticket_staff_access(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This command can only be used in a server."),
                ephemeral=True,
            )
            return False

        settings = self.store.get_settings(interaction.guild.id)
        if self.is_ticket_staff(interaction.user, settings):
            return True

        await send_interaction_message(
            interaction,
            embed=error_embed("Permission Denied", "You need ticket staff permissions for this action."),
            ephemeral=True,
        )
        return False

    async def create_ticket_channel(
        self,
        *,
        guild: discord.Guild,
        opener: discord.Member,
        category: str,
        details: str,
    ) -> tuple[Optional[discord.TextChannel], Optional[str]]:
        settings = self.store.get_settings(guild.id)
        category_id = settings.get("category_id")
        if not isinstance(category_id, int):
            return None, "Ticket system is not configured. Run `/ticket setup` first."

        ticket_category = guild.get_channel(category_id)
        if not isinstance(ticket_category, discord.CategoryChannel):
            return None, "The configured ticket category no longer exists. Run `/ticket setup` again."

        existing_ticket = self.store.get_open_ticket_for_user(guild.id, opener.id)
        if existing_ticket is not None:
            existing_channel = guild.get_channel(existing_ticket["channel_id"])
            if isinstance(existing_channel, discord.TextChannel):
                return None, f"You already have an open ticket: {existing_channel.mention}"
            self.store.close_ticket(existing_ticket["channel_id"])

        normalized_category = self.normalize_category(category)
        display_slug = _slugify_display_name(opener.display_name)
        category_config = CATEGORY_CONFIGS[normalized_category]
        base_name = f"{category_config['channel_prefix']}-{display_slug}"
        existing_names = {channel.name for channel in ticket_category.channels}
        channel_name = _unique_channel_name(base_name, existing_names)

        bot_member = guild.me
        if bot_member is None and self.bot.user is not None:
            bot_member = guild.get_member(self.bot.user.id)

        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            opener: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
            ),
        }
        if bot_member is not None:
            overwrites[bot_member] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
            )

        role_ids = {self.allowed_role_id}
        support_role_id = settings.get("support_role_id")
        if isinstance(support_role_id, int) and support_role_id > 0:
            role_ids.add(support_role_id)

        for role_id in role_ids:
            role = guild.get_role(role_id)
            if role is None:
                continue
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
            )

        channel = await guild.create_text_channel(
            channel_name,
            category=ticket_category,
            overwrites=overwrites,
            topic=f"Ticket for {opener} ({opener.id}) | Category: {normalized_category}",
        )

        ticket_number = self.store.create_ticket(
            guild_id=guild.id,
            channel_id=channel.id,
            user_id=opener.id,
            category=normalized_category,
            details=details,
        )
        ticket = self.store.get_ticket(channel.id)
        if ticket is None:
            return None, "The ticket channel was created, but the ticket record could not be saved."

        support_role = None
        if isinstance(support_role_id, int) and support_role_id > 0:
            support_role = guild.get_role(support_role_id)
        if support_role is not None:
            await channel.send(
                support_role.mention,
                allowed_mentions=discord.AllowedMentions(
                    everyone=False,
                    users=False,
                    roles=[support_role],
                ),
            )

        control_message = await channel.send(view=self.build_ticket_thread_view(guild=guild, ticket=ticket))
        self.store.set_control_message_id(channel.id, control_message.id)

        try:
            await control_message.pin(reason=f"Ticket #{ticket_number} control panel")
        except discord.HTTPException:
            LOGGER.warning("Failed to pin ticket control message in channel %s", channel.id)

        return channel, None

    async def create_ticket_from_modal(
        self,
        interaction: discord.Interaction,
        *,
        category: str,
        details: str,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "Tickets can only be created in a server."),
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        channel, error = await self.create_ticket_channel(
            guild=interaction.guild,
            opener=interaction.user,
            category=category,
            details=(details or "").strip() or "No details provided.",
        )
        if error is not None:
            await interaction.followup.send(embed=error_embed("Ticket Error", error), ephemeral=True)
            return

        await interaction.followup.send(
            embed=success_embed("Ticket Created", f"Your ticket has been created: {channel.mention}"),
            ephemeral=True,
        )

    async def send_ticket_panel(self, channel: discord.TextChannel, guild: discord.Guild) -> None:
        category_emojis = await self.ensure_panel_emojis(guild)
        view = TicketPanelView(
            self,
            banner_url=self.get_panel_banner_url(guild),
            logo_url=self.get_panel_logo_url(),
            category_emojis=category_emojis,
        )
        view = ensure_layout_view_action_rows(view)
        await channel.send(view=view)

    async def update_ticket_control_message(
        self,
        message: discord.Message,
        ticket: dict[str, Any],
    ) -> None:
        await message.edit(
            embed=None,
            view=self.build_ticket_thread_view(guild=message.guild, ticket=ticket),
        )

    async def handle_ticket_claim(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This can only be used in a server."),
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This action only works in ticket channels."),
                ephemeral=True,
            )
            return

        settings = self.store.get_settings(interaction.guild.id)
        if not self.is_ticket_staff(interaction.user, settings):
            await send_interaction_message(
                interaction,
                embed=error_embed("Permission Denied", "Only ticket staff can claim tickets."),
                ephemeral=True,
            )
            return

        ticket = self.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await send_interaction_message(
                interaction,
                embed=error_embed("Not a Ticket", "This channel is not a ticket."),
                ephemeral=True,
            )
            return

        claimed_by = ticket.get("claimed_by")
        if claimed_by:
            description = "You already claimed this ticket." if claimed_by == interaction.user.id else f"This ticket is already claimed by <@{claimed_by}>."
            await send_interaction_message(
                interaction,
                embed=warning_embed("Already Claimed", description),
                ephemeral=True,
            )
            return

        if not self.store.claim_ticket(interaction.channel.id, interaction.user.id):
            await send_interaction_message(
                interaction,
                embed=error_embed("Claim Failed", "The ticket could not be claimed."),
                ephemeral=True,
            )
            return

        updated_ticket = self.store.get_ticket(interaction.channel.id)
        if updated_ticket is not None and interaction.message is not None:
            await self.update_ticket_control_message(interaction.message, updated_ticket)

        await send_interaction_message(
            interaction,
            embed=success_embed("Ticket Claimed", "You are now assigned to this ticket."),
            ephemeral=True,
        )

    async def close_ticket_interaction(
        self,
        interaction: discord.Interaction,
        *,
        reason: Optional[str],
        button_mode: bool,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This command can only be used in a server."),
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await send_interaction_message(
                interaction,
                embed=error_embed("Unavailable", "This action only works in text channels."),
                ephemeral=True,
            )
            return

        ticket = self.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await send_interaction_message(
                interaction,
                embed=error_embed("Not a Ticket", "This channel is not a ticket."),
                ephemeral=True,
            )
            return

        settings = self.store.get_settings(interaction.guild.id)
        is_staff = self.is_ticket_staff(interaction.user, settings)
        if not is_staff and interaction.user.id != ticket["user_id"]:
            await send_interaction_message(
                interaction,
                embed=error_embed(
                    "Permission Denied",
                    "Only ticket staff or the ticket creator can close this ticket.",
                ),
                ephemeral=True,
            )
            return

        close_reason = (reason or "No reason provided").strip()
        description = f"This ticket will be closed in {TICKET_CLOSE_DELAY_SECONDS} seconds."
        if reason:
            description = f"{description}\n**Reason:** {close_reason}"

        if button_mode:
            await interaction.response.send_message(
                f"Closing ticket in {TICKET_CLOSE_DELAY_SECONDS} seconds...",
                ephemeral=True,
            )
            await interaction.channel.send(embed=warning_embed("Closing Ticket", description))
        else:
            await interaction.response.send_message(embed=warning_embed("Closing Ticket", description))

        await asyncio.sleep(TICKET_CLOSE_DELAY_SECONDS)
        ok, error = await self.finalize_ticket_close(
            guild=interaction.guild,
            channel=interaction.channel,
            closer=interaction.user,
            reason=close_reason,
        )
        if ok:
            return

        await interaction.followup.send(embed=error_embed("Close Failed", error), ephemeral=True)

    async def finalize_ticket_close(
        self,
        *,
        guild: discord.Guild,
        channel: discord.TextChannel,
        closer: discord.Member,
        reason: Optional[str],
    ) -> tuple[bool, str]:
        ticket = self.store.get_ticket(channel.id)
        if ticket is None:
            return False, "This channel is not a ticket."

        messages = [message async for message in channel.history(limit=None, oldest_first=True)]
        transcript_file = self.build_transcript(guild, channel, messages)
        transcript_bytes = transcript_file.getvalue()
        self.store.close_ticket(channel.id)
        await self.send_ticket_close_log(guild, ticket, closer, transcript_bytes, reason=reason)
        await self.send_ticket_close_dm(guild, ticket, closer, reason=reason, messages=messages, transcript_bytes=transcript_bytes)

        delete_reason = f"Ticket closed by {closer}"
        if reason:
            delete_reason = f"{delete_reason}: {reason}"

        try:
            await channel.delete(reason=delete_reason)
        except discord.Forbidden:
            return False, "The ticket was closed in storage, but I do not have permission to delete the channel."
        except discord.HTTPException as exc:
            return False, f"The ticket was closed in storage, but channel deletion failed: {exc}"

        return True, ""

    def build_transcript(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        messages: list[discord.Message],
    ) -> io.BytesIO:
        return generate_html_transcript(guild, channel, messages)

    async def send_ticket_close_log(
        self,
        guild: discord.Guild,
        ticket: dict[str, Any],
        closer: discord.Member,
        transcript_bytes: bytes,
        *,
        reason: Optional[str],
    ) -> None:
        settings = self.store.get_settings(guild.id)
        log_channel_id = settings.get("log_channel_id")
        if not isinstance(log_channel_id, int):
            return

        log_channel = guild.get_channel(log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
            return

        embed = make_embed(f"Ticket #{ticket['ticket_number']} Closed", "A ticket has been closed.")
        embed.timestamp = discord.utils.utcnow()
        embed.add_field(name="Created By", value=f"<@{ticket['user_id']}> (`{ticket['user_id']}`)", inline=False)
        embed.add_field(name="Closed By", value=f"{closer.mention} (`{closer.id}`)", inline=False)
        embed.add_field(name="Category", value=_panel_category_label(ticket["category"]), inline=True)
        embed.add_field(
            name="Claimed By",
            value=f"<@{ticket['claimed_by']}> (`{ticket['claimed_by']}`)" if ticket.get("claimed_by") else "Unassigned",
            inline=True,
        )
        embed.add_field(name="Reason", value=_truncate_for_field(reason or "No reason provided."), inline=False)

        await log_channel.send(
            embed=embed,
            file=discord.File(io.BytesIO(transcript_bytes), filename=f"ticket-{ticket['ticket_number']}.html"),
        )

    async def send_ticket_close_dm(
        self,
        guild: discord.Guild,
        ticket: dict[str, Any],
        closer: discord.Member,
        *,
        reason: Optional[str],
        messages: list[discord.Message],
        transcript_bytes: bytes,
    ) -> None:
        opener = guild.get_member(int(ticket["user_id"])) if guild is not None else None
        if opener is None:
            user = self.bot.get_user(int(ticket["user_id"]))
            if user is None:
                try:
                    user = await self.bot.fetch_user(int(ticket["user_id"]))
                except (discord.NotFound, discord.HTTPException):
                    return
            opener = user

        try:
            await opener.send(
                view=self.build_ticket_close_dm_view(
                    guild=guild,
                    ticket=ticket,
                    closer=closer,
                    reason=reason,
                    messages=messages,
                ),
                file=discord.File(io.BytesIO(transcript_bytes), filename=f"ticket-{ticket['ticket_number']}.html"),
            )
        except (discord.Forbidden, discord.HTTPException):
            LOGGER.info("Could not DM ticket closure summary to user %s", ticket["user_id"])


class TicketPanelSelect(discord.ui.Select):
    def __init__(
        self,
        system: TicketSystem,
        *,
        category_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        self.system = system
        options: list[discord.SelectOption] = []
        for category, config in CATEGORY_CONFIGS.items():
            option = discord.SelectOption(
                label=config["panel_label"],
                value=category,
                description=config["description"],
            )
            emoji = (category_emojis or {}).get(category)
            if emoji is not None:
                option.emoji = emoji
            options.append(option)

        super().__init__(
            placeholder="Select a ticket category...",
            min_values=1,
            max_values=1,
            custom_id="ticket_panel_select",
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        category = self.values[0] if self.values else "general"
        await interaction.response.send_modal(
            TicketDetailsModal(self.system, category=self.system.normalize_category(category))
        )


class TicketPanelView(discord.ui.LayoutView):
    def __init__(
        self,
        system: TicketSystem,
        *,
        banner_url: Optional[str] = None,
        logo_url: Optional[str] = None,
        category_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        super().__init__(timeout=None)
        select = TicketPanelSelect(system, category_emojis=category_emojis)
        container = branded_panel_container(
            title="ZyphraxHub Community Tickets",
            description=(
                "If you need help, click on the option corresponding to the type of ticket you want to open.\n"
                "**Response time may vary due to many factors, so please be patient.**"
            ),
            banner_url=banner_url,
            logo_url=logo_url,
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.ActionRow(select))
        self.add_item(container)


class TicketDetailsModal(discord.ui.Modal):
    def __init__(self, system: TicketSystem, *, category: str) -> None:
        self.system = system
        self.category = system.normalize_category(category)
        super().__init__(title=_panel_category_label(self.category), timeout=300)

        if self.category == "report":
            self.reported = discord.ui.TextInput(
                label="Reported username",
                placeholder="Who are you reporting? Please list their actual username",
                required=True,
                max_length=100,
            )
            self.reason = discord.ui.TextInput(
                label="Report reason",
                placeholder="What are you reporting them for? Please show evidence if needed",
                required=True,
                style=discord.TextStyle.paragraph,
                max_length=1000,
            )
            self.add_item(self.reported)
            self.add_item(self.reason)
        elif self.category == "appeal":
            self.punishment = discord.ui.TextInput(
                label="Punishment type",
                placeholder="Ban, timeout, mute, etc.",
                required=True,
                max_length=100,
            )
            self.why = discord.ui.TextInput(
                label="Why should it be lifted?",
                placeholder="Explain your appeal clearly and honestly",
                required=True,
                style=discord.TextStyle.paragraph,
                max_length=1000,
            )
            self.add_item(self.punishment)
            self.add_item(self.why)
        elif self.category == "creator":
            self.links = discord.ui.TextInput(
                label="Social media links",
                placeholder="Please link your social media/s",
                required=True,
                style=discord.TextStyle.paragraph,
                max_length=1000,
            )
            self.add_item(self.links)
        elif self.category == "utility":
            self.details = discord.ui.TextInput(
                label="What issue are you having?",
                placeholder="Describe the ZyphraxHub Community utility issue you are having",
                required=True,
                style=discord.TextStyle.paragraph,
                max_length=1000,
            )
            self.add_item(self.details)
        else:
            self.details = discord.ui.TextInput(
                label="Your question",
                placeholder="What is your question?",
                required=True,
                style=discord.TextStyle.paragraph,
                max_length=1000,
            )
            self.add_item(self.details)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if self.category == "report":
            details = (
                f"Reported: {self.reported.value.strip()}\n"
                f"Reason: {self.reason.value.strip()}"
            )
        elif self.category == "appeal":
            details = (
                f"Punishment: {self.punishment.value.strip()}\n"
                f"Appeal: {self.why.value.strip()}"
            )
        elif self.category == "creator":
            details = f"Social media links: {self.links.value.strip()}"
        else:
            details = self.details.value.strip()

        await self.system.create_ticket_from_modal(interaction, category=self.category, details=details)


class TicketThreadView(discord.ui.LayoutView):
    def __init__(
        self,
        system: TicketSystem,
        *,
        opener_avatar_url: Optional[str] = None,
        category: str = DEFAULT_TICKET_CATEGORY,
        details: str = "",
        claimed_by: Optional[int] = None,
    ) -> None:
        super().__init__(timeout=None)

        category_label = _panel_category_label(category)
        assigned_text = f"<@{claimed_by}> (`{claimed_by}`)" if claimed_by else "*Unassigned*"
        sanitized_details = (details or "").strip() or "No details provided."

        container_children: list[discord.ui.Item[Any]] = []
        header_text = (
            f"**{category_label} Ticket**\n"
            "Please wait until one of our support team members can help you.\n"
            "**Response time may vary due to many factors, so please be patient.**"
        )

        if opener_avatar_url:
            container_children.append(
                discord.ui.Section(
                    discord.ui.TextDisplay(header_text),
                    accessory=discord.ui.Thumbnail(opener_avatar_url),
                )
            )
        else:
            container_children.append(discord.ui.TextDisplay(header_text))

        container_children.append(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container_children.append(discord.ui.TextDisplay(f"**Assigned staff**\n{assigned_text}"))
        container_children.append(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container_children.append(
            discord.ui.TextDisplay(
                f"**How can we help you?**\n```{discord.utils.escape_markdown(sanitized_details)}```"
            )
        )

        close_button = TicketCloseButton(system)
        claim_button = TicketClaimButton(system)
        if claimed_by:
            claim_button.disabled = True

        container_children.append(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container_children.append(discord.ui.ActionRow(close_button, claim_button))

        self.add_item(discord.ui.Container(*container_children, accent_color=THESEUS_BLUE))


class TicketCloseButton(discord.ui.Button):
    def __init__(self, system: TicketSystem) -> None:
        super().__init__(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_close",
        )
        self.system = system

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.system.close_ticket_interaction(interaction, reason=None, button_mode=True)


class TicketClaimButton(discord.ui.Button):
    def __init__(self, system: TicketSystem) -> None:
        super().__init__(
            label="Assign me",
            style=discord.ButtonStyle.success,
            custom_id="ticket_claim",
        )
        self.system = system

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.system.handle_ticket_claim(interaction)


def init_ticket_system(bot: commands.Bot, *, base_dir: Path, allowed_role_id: int) -> TicketSystem:
    system = TicketSystem(
        bot,
        base_dir=base_dir,
        store_path=base_dir / "tickets.db",
        allowed_role_id=allowed_role_id,
    )

    ticket_group = app_commands.Group(name="ticket", description="Ticket management commands")

    async def _post_ticket_panel(interaction: discord.Interaction) -> None:
        if not await system.ensure_ticket_staff_access(interaction):
            return
        if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                embed=error_embed("Unavailable", "This command can only be used in a text channel."),
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        await system.send_ticket_panel(interaction.channel, interaction.guild)
        await interaction.followup.send(
            embed=success_embed("Panel Created", "The ticket panel has been posted."),
            ephemeral=True,
        )

    @ticket_group.command(name="setup", description="Configure the ticket system")
    @app_commands.guild_only()
    @app_commands.describe(
        category="Category where ticket channels should be created",
        support_role="Role that can manage tickets",
        log_channel="Channel where closed ticket logs should be sent",
    )
    async def ticket_setup(
        interaction: discord.Interaction,
        category: discord.CategoryChannel,
        support_role: discord.Role,
        log_channel: Optional[discord.TextChannel] = None,
    ) -> None:
        if not await system.ensure_management_access(interaction):
            return

        settings = system.store.save_settings(
            interaction.guild_id,
            category_id=category.id,
            support_role_id=support_role.id,
            log_channel_id=log_channel.id if log_channel is not None else None,
        )
        await interaction.response.send_message(
            embed=success_embed("Ticket System Updated", "The ticket system configuration has been saved."),
            ephemeral=True,
        )
        await interaction.followup.send(embed=system.build_settings_embed(interaction.guild, settings), ephemeral=True)

    @ticket_group.command(name="settings", description="Show the current ticket configuration")
    @app_commands.guild_only()
    async def ticket_settings(interaction: discord.Interaction) -> None:
        if not await system.ensure_management_access(interaction):
            return
        settings = system.store.get_settings(interaction.guild_id)
        await interaction.response.send_message(embed=system.build_settings_embed(interaction.guild, settings), ephemeral=True)

    @ticket_group.command(name="create", description="Create a support ticket")
    @app_commands.guild_only()
    @app_commands.describe(category="Ticket category")
    @app_commands.choices(
        category=[
            app_commands.Choice(name="ZyphraxHub Community Utility Issues", value="utility"),
            app_commands.Choice(name="Ban Appeal", value="appeal"),
            app_commands.Choice(name="User / Staff Reports", value="report"),
            app_commands.Choice(name="Apply for Content Creator", value="creator"),
            app_commands.Choice(name="General Support", value="general"),
        ]
    )
    async def ticket_create(
        interaction: discord.Interaction,
        category: str = DEFAULT_TICKET_CATEGORY,
    ) -> None:
        await interaction.response.send_modal(TicketDetailsModal(system, category=category))

    @ticket_group.command(name="close", description="Close the current ticket")
    @app_commands.guild_only()
    @app_commands.describe(reason="Reason for closing the ticket")
    async def ticket_close(interaction: discord.Interaction, reason: Optional[str] = None) -> None:
        await system.close_ticket_interaction(interaction, reason=reason, button_mode=False)

    @ticket_group.command(name="add", description="Add a user to this ticket")
    @app_commands.guild_only()
    async def ticket_add(interaction: discord.Interaction, user: discord.Member) -> None:
        if not await system.ensure_ticket_staff_access(interaction):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(embed=error_embed("Unavailable", "This command can only be used in a ticket channel."), ephemeral=True)
            return
        ticket = system.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message(embed=error_embed("Not a Ticket", "This channel is not a ticket."), ephemeral=True)
            return
        await interaction.channel.set_permissions(
            user,
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        )
        await interaction.response.send_message(embed=success_embed("User Added", f"{user.mention} has been added to this ticket."))

    @ticket_group.command(name="remove", description="Remove a user from this ticket")
    @app_commands.guild_only()
    async def ticket_remove(interaction: discord.Interaction, user: discord.Member) -> None:
        if not await system.ensure_ticket_staff_access(interaction):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(embed=error_embed("Unavailable", "This command can only be used in a ticket channel."), ephemeral=True)
            return
        ticket = system.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message(embed=error_embed("Not a Ticket", "This channel is not a ticket."), ephemeral=True)
            return
        if user.id == ticket["user_id"]:
            await interaction.response.send_message(embed=error_embed("Cannot Remove", "You cannot remove the ticket creator."), ephemeral=True)
            return
        await interaction.channel.set_permissions(user, overwrite=None)
        await interaction.response.send_message(embed=success_embed("User Removed", f"{user.mention} has been removed from this ticket."))

    @ticket_group.command(name="rename", description="Rename this ticket channel")
    @app_commands.guild_only()
    async def ticket_rename(interaction: discord.Interaction, name: str) -> None:
        if not await system.ensure_ticket_staff_access(interaction):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(embed=error_embed("Unavailable", "This command can only be used in a ticket channel."), ephemeral=True)
            return
        ticket = system.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message(embed=error_embed("Not a Ticket", "This channel is not a ticket."), ephemeral=True)
            return
        normalized_name = system.normalize_channel_name(name)
        if normalized_name is None:
            await interaction.response.send_message(embed=error_embed("Invalid Name", "Please provide a valid ticket channel name."), ephemeral=True)
            return
        await interaction.channel.edit(name=normalized_name)
        await interaction.response.send_message(embed=success_embed("Ticket Renamed", f"Ticket renamed to **{normalized_name}**."))

    @ticket_group.command(name="transcript", description="Generate a transcript for this ticket")
    @app_commands.guild_only()
    async def ticket_transcript(interaction: discord.Interaction) -> None:
        if not await system.ensure_ticket_staff_access(interaction):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(embed=error_embed("Unavailable", "This command can only be used in a ticket channel."), ephemeral=True)
            return
        ticket = system.store.get_ticket(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message(embed=error_embed("Not a Ticket", "This channel is not a ticket."), ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        messages = [message async for message in interaction.channel.history(limit=None, oldest_first=True)]
        transcript_file = system.build_transcript(interaction.guild, interaction.channel, messages)
        transcript_file.seek(0)
        await interaction.followup.send(
            embed=success_embed("Transcript Generated", "Here is the transcript for this ticket."),
            ephemeral=True,
            file=discord.File(transcript_file, filename=f"ticket-{ticket['ticket_number']}.html"),
        )

    @ticket_group.command(name="panel", description="Post the ticket creation panel")
    @app_commands.guild_only()
    async def ticket_panel(interaction: discord.Interaction) -> None:
        await _post_ticket_panel(interaction)

    @bot.tree.command(name="ticketpanel", description="Post the ticket creation panel")
    @app_commands.guild_only()
    async def ticketpanel_alias(interaction: discord.Interaction) -> None:
        await _post_ticket_panel(interaction)

    bot.tree.add_command(ticket_group)
    return system
