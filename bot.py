import asyncio
import io
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import discord
from discord import app_commands
from discord.ext import commands

from components_v2 import branded_panel_container, ensure_layout_view_action_rows
from ticket_system import init_ticket_system
from whitelist_system import KEY_PREFIX, LuarmorSyncError, build_store_from_env
from welcome_system import init_welcome_system

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("theseus-bot")

def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


BASE_DIR = Path(__file__).resolve().parent
load_env_file(BASE_DIR / ".env")

BOT_TOKEN: Optional[str] = os.getenv("DISCORD_BOT_TOKEN")
DEV_GUILD_ID: Optional[str] = os.getenv("DISCORD_GUILD_ID")

THESEUS_BLUE = 0x3498DB
ALLOWED_ROLE_ID = 1459425844165345423
ANNOUNCEMENT_CHANNEL_ID = 1479833160136130632
UPDATE_CHANNEL_ID = 1479834369249247323
DOWNLOAD_CHANNEL_ID = 1479835427484729404
SUPPORTED_CHANNEL_ID = 1481525548055531520
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "1480307153549000869"))
UPDATE_PANEL_TITLE = "ZYPHRAXHUB COMMUNITY WINDOWS UPDATE"
SERVER_NAME = os.getenv("SERVER_NAME", "ZyphraxHub Community")
SERVER_TAG = os.getenv("SERVER_TAG", SERVER_NAME)
PAYPAL_URL = os.getenv("PAYPAL_URL", "")
BITCOIN_ADDRESS = "1H8gHv7vSc1VpWbEGtZmQEW5frhJWqHy2R"
LITECOIN_ADDRESS = "LW1dMKWFAwK4nBZKuhTcx1HyMeDdjgSY2M"

REPORT_BUGS_URL = "https://discord.com/channels/1459424753721806940/1480330103673192582"
ROBLOX_LIVE_DOWNLOAD_URL = "https://rdd.weao.gg/"
VCREDIST_URL = "https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist"
DIRECTX_RUNTIME_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=35"
D3DCOMPILER_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=6812"

ROBLOX_VERSION_DOMAINS = (
    "https://weao.xyz",
    "https://whatexpsare.online",
    "https://whatexploitsaretra.sh",
    "https://weao.gg",
)
ROBLOX_CURRENT_VERSION_PATH = "/api/versions/current"
WEAO_USER_AGENT = "WEAO-3PService"
ROBLOX_VERSION_CACHE_TTL_SECONDS = 300

ICON_PACK_DIR = BASE_DIR / "icon pack"
SUPPORTED_GAMES_FILE = BASE_DIR / "supported_games.json"
WELCOME_BG_PATH = BASE_DIR / "welcome_bg.png"
WHITELIST_DB_PATH = BASE_DIR / "whitelist.db"
PAYPANEL_BANNER_FILENAMES = (
    "paypanel_banner.png",
    "paypanel_banner.jpg",
    "paypanel_banner.jpeg",
    "server_banner.png",
    "server_banner.jpg",
    "server_banner.jpeg",
    "banner.png",
    "banner.jpg",
    "banner.jpeg",
)
PAYPANEL_QRIS_FILENAMES = (
    "qris.png",
    "qris.jpg",
    "qris.jpeg",
    "payment_qris.png",
    "payment_qris.jpg",
    "payment_qris.jpeg",
)
PAYPANEL_ICON_CONFIGS: dict[str, dict[str, str]] = {
    "key": {"emoji_name": "zyphraxhub_pay_key", "icon_file": "key.png"},
    "paypal": {"emoji_name": "zyphraxhub_pay_paypal", "icon_file": "coin.png"},
    "crypto": {"emoji_name": "zyphraxhub_pay_crypto", "icon_file": "btc.png"},
    "qris": {"emoji_name": "zyphraxhub_pay_qris", "icon_file": "uy.png"},
    "proof": {"emoji_name": "zyphraxhub_pay_proof", "icon_file": "message.png"},
    "done": {"emoji_name": "zyphraxhub_pay_done", "icon_file": "tick.png"},
}

UPDATE_ICON_CONFIGS: dict[str, dict[str, str]] = {
    "windows":  {"emoji_name": "theseus_update_windows",  "icon_file": "windows.png"},
    "settings": {"emoji_name": "theseus_update_settings", "icon_file": "setting.png"},
    "download": {"emoji_name": "theseus_update_download", "icon_file": "download.png"},
    "notes":    {"emoji_name": "theseus_update_notes",    "icon_file": "message.png"},
    "roblox":   {"emoji_name": "theseus_update_roblox",   "icon_file": "roblox.png"},
    "premium":  {},
}

class TheseusBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.ticket_system = init_ticket_system(
            self, base_dir=BASE_DIR, allowed_role_id=ALLOWED_ROLE_ID
        )
        self.welcome_system = (
            init_welcome_system(
                self,
                welcome_channel_id=WELCOME_CHANNEL_ID,
                server_name=SERVER_NAME,
                server_tag=SERVER_TAG,
                accent_color=THESEUS_BLUE,
                background_path=WELCOME_BG_PATH,
            )
            if WELCOME_CHANNEL_ID > 0
            else None
        )

    async def setup_hook(self) -> None:
        self.ticket_system.setup()
        if self.welcome_system is not None:
            self.welcome_system.setup()
        self.add_view(ensure_layout_view_action_rows(PayPanelView()))
        self.add_view(
            ensure_layout_view_action_rows(
                DownloadPanelView(download_url=ROBLOX_LIVE_DOWNLOAD_URL)
            )
        )
        await self._sync_commands()

    async def _sync_commands(self) -> None:
        if DEV_GUILD_ID:
            guild = discord.Object(id=int(DEV_GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            try:
                synced = await self.tree.sync(guild=guild)
            except discord.Forbidden:
                LOGGER.warning(
                    "Discord rejected guild command sync for guild %s. Skipping.", DEV_GUILD_ID
                )
            except discord.HTTPException as exc:
                LOGGER.warning("Guild command sync failed for guild %s: %s", DEV_GUILD_ID, exc)
            else:
                LOGGER.info("Synced %s guild command(s) to %s", len(synced), DEV_GUILD_ID)
            return

        try:
            synced = await self.tree.sync()
        except discord.Forbidden:
            LOGGER.warning("Discord rejected global command sync. Skipping.")
        except discord.HTTPException as exc:
            LOGGER.warning("Global command sync failed: %s", exc)
        else:
            LOGGER.info("Synced %s global command(s)", len(synced))

    async def on_ready(self) -> None:
        if self.user is not None:
            LOGGER.info("Logged in as %s (%s)", self.user, self.user.id)


bot = TheseusBot()
whitelist_store = build_store_from_env(WHITELIST_DB_PATH)

def allowed_role_only() -> app_commands.check:
    async def predicate(interaction: discord.Interaction) -> bool:
        return (
            isinstance(interaction.user, discord.Member)
            and (
                _member_has_role(interaction.user, ALLOWED_ROLE_ID)
                or interaction.user.guild_permissions.administrator
            )
        )
    return app_commands.check(predicate)


def _member_has_role(member: discord.Member, role_id: int) -> bool:
    return any(role.id == role_id for role in member.roles)


def _channel_jump_url(guild_id: Optional[int], channel_id: int) -> Optional[str]:
    if guild_id is None:
        return None
    return f"https://discord.com/channels/{guild_id}/{channel_id}"


def _find_icon_pack_asset(filenames: tuple[str, ...]) -> Optional[Path]:
    for filename in filenames:
        path = ICON_PACK_DIR / filename
        if path.exists():
            return path
    return None


def _find_largest_icon_pack_image() -> Optional[Path]:
    if not ICON_PACK_DIR.exists():
        return None
    candidates = [
        path
        for path in ICON_PACK_DIR.iterdir()
        if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_size)


def _resolve_paypanel_banner_path() -> Optional[Path]:
    return _find_icon_pack_asset(PAYPANEL_BANNER_FILENAMES) or _find_largest_icon_pack_image()


def _resolve_paypanel_qris_path() -> Optional[Path]:
    return _find_icon_pack_asset(PAYPANEL_QRIS_FILENAMES)

def _sanitize_panel_text(value: str) -> str:
    return (value or "").replace("```", "'''").strip()


def _split_panel_items(value: str) -> list[str]:
    return [
        item
        for item in (
            part.strip() for part in re.split(r"[\r\n,]+", _sanitize_panel_text(value))
        )
        if item
    ]


def _format_changelog(version: str, changelog: Optional[str]) -> str:
    lines = [f"+ Updated to {_sanitize_panel_text(version)}"]
    changelog_items = _split_panel_items(changelog or "")
    if changelog_items:
        lines.append("")
        lines.extend(f"+ {entry}" for entry in changelog_items)
    return "```diff\n" + "\n".join(lines) + "\n```"


def _format_notes(notes: str) -> str:
    return "\n".join(
        f"- {discord.utils.escape_markdown(entry)}"
        for entry in _split_panel_items(notes)
    )


def _format_luarmor_status(user: Optional[dict[str, object]]) -> str:
    if not user:
        return "Not linked"
    luarmor_key = user.get("luarmor_user_key")
    if not luarmor_key:
        return "Not linked"
    status = str(user.get("luarmor_status") or "synced").strip().title()
    ban_reason = str(user.get("luarmor_ban_reason") or "").strip()
    if ban_reason:
        return (
            f"{status}\n"
            f"Key: `{_mask_key(str(luarmor_key))}`\n"
            f"Reason: {discord.utils.escape_markdown(ban_reason)}"
        )
    return f"{status}\nKey: `{_mask_key(str(luarmor_key))}`"



def _validate_whitelist_key_format(key: str) -> bool:
    pattern = rf"^{KEY_PREFIX}-[A-Z0-9]{{3}}-[A-Z0-9]{{3}}-[A-Z0-9]{{3}}$"
    normalized = key.strip()
    if re.match(pattern, normalized, flags=re.IGNORECASE):
        return True
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{16,64}", normalized))


def _whitelist_embed(
    title: str, description: str = "", color: int = THESEUS_BLUE
) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="ZyphraxHub Community Team")
    return embed


def _lookup_discord_id(value: str) -> Optional[str]:
    normalized = value.strip()
    if normalized.startswith("<@") and normalized.endswith(">"):
        normalized = normalized.strip("<@!>")
    return normalized if normalized.isdigit() else None


def _redeem_format_hint() -> str:
    if whitelist_store.uses_luarmor_keys:
        return "Invalid key format. Use the Luarmor key exactly as provided."
    return f"Invalid key format. Expected `{KEY_PREFIX}-XXX-XXX-XXX`."


def _mask_key(key: Optional[str]) -> str:
    if not key:
        return "`None`"
    return f"||`{key}`||"


def _format_access_expiry(user: Optional[dict[str, object]]) -> str:
    if user is None:
        return "Permanent"
    expires_at = str(user.get("access_expires_at") or "").strip()
    return expires_at or "Permanent"


def _format_key_duration(days: Optional[int]) -> str:
    if days is None:
        return "Permanent"
    total_seconds = int(days)
    if total_seconds <= 0:
        return "Permanent"
    units = (
        ("year", 31536000),
        ("month", 2592000),
        ("week", 604800),
        ("day", 86400),
        ("hour", 3600),
        ("minute", 60),
    )
    for label, size in units:
        if total_seconds % size == 0:
            value = total_seconds // size
            return f"{value} {label}{'s' if value != 1 else ''}"
    return f"{total_seconds} seconds"


def _parse_duration_input(value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    normalized = (value or "").strip().lower()
    if not normalized:
        return None, None
    compact = re.sub(r"\s+", "", normalized)
    if compact in {"lifetime", "life", "permanent", "forever", "perm"}:
        return None, None

    match = re.fullmatch(r"(\d+)([a-z]+)", compact)
    if match is None:
        return None, "Invalid duration. Use values like `1 minute`, `1w`, `1month`, `1year`, or `lifetime`."

    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        return None, "Duration must be greater than zero."
    if unit == "m":
        return None, "AMBIGUOUS_MINUTE_MONTH"

    seconds_per_unit = {
        "min": 60,
        "mins": 60,
        "minute": 60,
        "minutes": 60,
        "h": 3600,
        "hr": 3600,
        "hrs": 3600,
        "hour": 3600,
        "hours": 3600,
        "d": 86400,
        "day": 86400,
        "days": 86400,
        "w": 604800,
        "week": 604800,
        "weeks": 604800,
        "mo": 2592000,
        "mon": 2592000,
        "month": 2592000,
        "months": 2592000,
        "y": 31536000,
        "yr": 31536000,
        "yrs": 31536000,
        "year": 31536000,
        "years": 31536000,
    }
    multiplier = seconds_per_unit.get(unit)
    if multiplier is None:
        return None, "Invalid duration unit. Use minute, week, month, year, or `lifetime`."
    return amount * multiplier, None


def _build_key_export(keys: list[dict[str, object]]) -> str:
    if not keys:
        return "No keys found."
    return "\n".join(
        (
            f"{row['key']} | "
            f"{'used' if row['used'] else 'free'} | "
            f"duration={_format_key_duration(row.get('duration_seconds') or (int(row.get('duration_days') or 0) * 86400) or None)}"
        )
        for row in keys
    )


async def _send_generated_keys_response(
    interaction: discord.Interaction,
    *,
    count: int,
    duration_seconds: Optional[int],
) -> None:
    keys = await whitelist_store.create_keys(
        count,
        interaction.user.id,
        duration_seconds=duration_seconds,
    )
    title = "Keys Generated" if count == 1 else f"Generated {len(keys)} Keys"
    description = (
        f"Created **{len(keys)}** ZyphraxHub Community key(s)."
        if count == 1
        else "The keys were added to the local whitelist database."
    )
    embed = _whitelist_embed(title, description, color=0x2ECC71)
    embed.add_field(name="Duration", value=_format_key_duration(duration_seconds), inline=True)
    if count == 1 and len(keys) <= 5:
        embed.add_field(name="Keys", value="\n".join(f"`{key}`" for key in keys), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    if count != 1 and len(keys) <= 10:
        embed.add_field(name="Keys", value="\n".join(f"`{key}`" for key in keys), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    file = discord.File(
        fp=io.BytesIO("\n".join(keys).encode("utf-8")),
        filename=f"zyphrax_keys_{int(time.time())}.txt",
    )
    await interaction.response.send_message(embed=embed, file=file, ephemeral=True)


class DurationAmbiguityButton(discord.ui.Button):
    def __init__(self, *, label: str, count: int, duration_seconds: int) -> None:
        super().__init__(style=discord.ButtonStyle.primary, label=label)
        self.count = count
        self.duration_seconds = duration_seconds

    async def callback(self, interaction: discord.Interaction) -> None:
        await _send_generated_keys_response(
            interaction,
            count=self.count,
            duration_seconds=self.duration_seconds,
        )


class DurationAmbiguityView(discord.ui.View):
    def __init__(self, *, count: int) -> None:
        super().__init__(timeout=300)
        self.add_item(
            DurationAmbiguityButton(
                label="1 Minute",
                count=count,
                duration_seconds=60,
            )
        )
        self.add_item(
            DurationAmbiguityButton(
                label="1 Month",
                count=count,
                duration_seconds=2592000,
            )
        )


async def unused_key_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    normalized_current = current.strip().lower()
    keys = await whitelist_store.get_all_keys(include_used=False)
    ranked: list[str] = []
    for row in keys:
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        if normalized_current and normalized_current not in key.lower():
            continue
        ranked.append(key)

    ranked.sort(key=lambda value: (not value.lower().startswith(normalized_current), value))
    return [
        app_commands.Choice(name=key[:100], value=key)
        for key in ranked[:25]
    ]


class KeylistDownloadButton(discord.ui.Button):
    def __init__(
        self,
        *,
        export_text: str,
        filename: str,
        emoji: Optional[discord.Emoji] = None,
    ) -> None:
        super().__init__(
            label="Download Keys",
            style=discord.ButtonStyle.primary,
            emoji=emoji,
        )
        self.export_text = export_text
        self.filename = filename

    async def callback(self, interaction: discord.Interaction) -> None:
        file = discord.File(
            fp=io.BytesIO(self.export_text.encode("utf-8")),
            filename=self.filename,
        )
        await interaction.response.send_message(
            content="Full key export:",
            file=file,
            ephemeral=True,
        )


class KeylistDownloadView(discord.ui.View):
    def __init__(
        self,
        *,
        export_text: str,
        filename: str,
        emoji: Optional[discord.Emoji] = None,
    ) -> None:
        super().__init__(timeout=300)
        self.add_item(
            KeylistDownloadButton(
                export_text=export_text,
                filename=filename,
                emoji=emoji,
            )
        )


class KeylistPanelView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        stats: dict[str, int],
        keys: list[dict[str, object]],
        export_text: str,
        filename: str,
        panel_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        super().__init__(timeout=300)
        panel_emojis = panel_emojis or {}
        preview = "No keys found."
        if keys:
            preview = "\n".join(
                (
                    f"`{row['key']}` "
                    f"{'used' if row['used'] else 'free'} "
                    f"({_format_key_duration(row.get('duration_seconds') or (int(row.get('duration_days') or 0) * 86400) or None)})"
                )
                for row in keys[:10]
            )

        description = (
            "ZyphraxHub Community license overview\n\n"
            f"**Available**\n`{stats['available_keys']}`\n\n"
            f"**Used**\n`{stats['used_keys']}`\n\n"
            f"**Total**\n`{stats['total_keys']}`\n\n"
            f"**Preview**\n{preview}\n\n"
            f"**Download**\nUse the button below for the full export."
        )
        container = branded_panel_container(
            title=f"{_panel_emoji_text(panel_emojis, 'settings')}Key Statistics",
            description=description,
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                KeylistDownloadButton(
                    export_text=export_text,
                    filename=filename,
                    emoji=_panel_button_emoji(panel_emojis, "download"),
                )
            )
        )
        self.add_item(container)


def _dashboard_status_text(user: Optional[dict[str, object]], *, is_banned: bool) -> str:
    license_value = user.get("key") if user is not None else None
    hwid_value = user.get("hwid") if user is not None else None
    access_text = "Blacklisted" if is_banned else ("Active" if license_value else "Inactive")
    return (
        f"**Status**\n"
        f"License: {'Set' if license_value else 'None'} {license_value or ''}\n"
        f"HWID: {'Set' if hwid_value else 'Not Set'} {hwid_value or ''}\n"
        f"Access: {access_text}"
    )


def _dashboard_summary_text(user: Optional[dict[str, object]], *, is_banned: bool) -> str:
    if is_banned:
        return "Your account is blacklisted. Contact staff if you think this is a mistake."
    if user is None or not user.get("key"):
        return "Use the `Redeem Key` button below to activate your ZyphraxHub Community license."
    return (
        "Your ZyphraxHub Community license is active.\n"
        "Use `My Info` to inspect your account details or `Create Ticket` if you need help."
    )


def _panel_emoji_text(panel_emojis: dict[str, discord.Emoji], key: str) -> str:
    emoji = panel_emojis.get(key)
    return f"{emoji} " if emoji is not None else ""


def _panel_button_emoji(
    panel_emojis: dict[str, discord.Emoji], key: str
) -> Optional[discord.Emoji]:
    emoji = panel_emojis.get(key)
    return emoji

async def _get_text_channel(
    client: discord.Client, channel_id: int
) -> discord.TextChannel:
    channel = client.get_channel(channel_id)
    if channel is None:
        try:
            channel = await client.fetch_channel(channel_id)
        except (discord.Forbidden, discord.HTTPException, discord.NotFound) as exc:
            raise RuntimeError(f"I could not access <#{channel_id}>.") from exc

    if not isinstance(channel, discord.TextChannel):
        raise RuntimeError(f"<#{channel_id}> is not a text channel.")

    return channel


async def _ensure_update_panel_emojis(
    guild: discord.Guild, client: discord.Client
) -> dict[str, discord.Emoji]:
    emojis: dict[str, discord.Emoji] = {}

    me = guild.me
    if me is None and client.user is not None:
        me = guild.get_member(client.user.id)
    can_manage = me is not None and me.guild_permissions.manage_emojis_and_stickers

    for key, config in UPDATE_ICON_CONFIGS.items():
        emoji_name = config.get("emoji_name")
        if not emoji_name:
            continue

        existing = discord.utils.get(guild.emojis, name=emoji_name)
        if existing is not None:
            emojis[key] = existing
            continue

        if not can_manage:
            continue

        icon_file = config.get("icon_file")
        if not icon_file:
            continue

        icon_path = ICON_PACK_DIR / icon_file
        if not icon_path.exists():
            LOGGER.warning("Missing update icon for %s at %s", key, icon_path)
            continue

        try:
            emoji = await guild.create_custom_emoji(
                name=emoji_name,
                image=icon_path.read_bytes(),
                reason="Upload ZyphraxHub Community update panel icons",
            )
            emojis[key] = emoji
        except (discord.Forbidden, discord.HTTPException) as exc:
            LOGGER.warning(
                "Failed to create emoji %s in guild %s: %s", emoji_name, guild.id, exc
            )

    return emojis


async def _ensure_paypanel_emojis(
    guild: discord.Guild, client: discord.Client
) -> dict[str, discord.Emoji]:
    emojis: dict[str, discord.Emoji] = {}

    me = guild.me
    if me is None and client.user is not None:
        me = guild.get_member(client.user.id)
    can_manage = me is not None and me.guild_permissions.manage_emojis_and_stickers

    for key, config in PAYPANEL_ICON_CONFIGS.items():
        emoji_name = config.get("emoji_name")
        if not emoji_name:
            continue

        existing = discord.utils.get(guild.emojis, name=emoji_name)
        if existing is not None:
            emojis[key] = existing
            continue

        if not can_manage:
            continue

        icon_file = config.get("icon_file")
        if not icon_file:
            continue

        icon_path = ICON_PACK_DIR / icon_file
        if not icon_path.exists():
            LOGGER.warning("Missing paypanel icon for %s at %s", key, icon_path)
            continue

        try:
            emoji = await guild.create_custom_emoji(
                name=emoji_name,
                image=icon_path.read_bytes(),
                reason="Upload ZyphraxHub Community pay panel icons",
            )
            emojis[key] = emoji
        except (discord.Forbidden, discord.HTTPException) as exc:
            LOGGER.warning(
                "Failed to create paypanel emoji %s in guild %s: %s", emoji_name, guild.id, exc
            )

    return emojis


async def _clear_channel_messages(channel: discord.TextChannel) -> None:
    try:
        async for message in channel.history(limit=None):
            await message.delete()
    except (discord.Forbidden, discord.HTTPException) as exc:
        raise RuntimeError(
            f"I couldn't clear <#{channel.id}> before posting the new panel."
        ) from exc


_roblox_version_cache: dict[str, Optional[str] | float] = {
    "value": None,
    "expires_at": 0.0,
}


async def _get_latest_roblox_windows_version() -> Optional[str]:
    def fetch_version() -> Optional[str]:
        now = time.time()
        cached_value = _roblox_version_cache["value"]
        expires_at = float(_roblox_version_cache["expires_at"])

        if isinstance(cached_value, str) and now < expires_at:
            return cached_value

        saw_rate_limit = False
        for base_url in ROBLOX_VERSION_DOMAINS:
            req = urllib_request.Request(
                f"{base_url}{ROBLOX_CURRENT_VERSION_PATH}",
                headers={"User-Agent": WEAO_USER_AGENT},
                method="GET",
            )
            try:
                with urllib_request.urlopen(req, timeout=10) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except urllib_error.HTTPError as exc:
                if exc.code == 429:
                    retry_info = exc.headers.get("Retry-After") or exc.headers.get(
                        "X-RateLimit-Reset"
                    )
                    LOGGER.warning(
                        "Rate-limited by WEAO on %s (retry info: %s)",
                        base_url,
                        retry_info or "none",
                    )
                    saw_rate_limit = True
                else:
                    LOGGER.warning(
                        "WEAO version request failed on %s with HTTP %s", base_url, exc.code
                    )
                continue
            except urllib_error.URLError as exc:
                LOGGER.warning("WEAO version request failed on %s: %s", base_url, exc)
                continue
            except (TimeoutError, json.JSONDecodeError, ValueError) as exc:
                LOGGER.warning("WEAO version response from %s was invalid: %s", base_url, exc)
                continue

            version = payload.get("Windows")
            if not isinstance(version, str) or not version.strip():
                LOGGER.warning("WEAO response from %s had no Windows version", base_url)
                continue

            version = version.strip()
            _roblox_version_cache["value"] = version
            _roblox_version_cache["expires_at"] = now + ROBLOX_VERSION_CACHE_TTL_SECONDS
            return version

        return cached_value if saw_rate_limit and isinstance(cached_value, str) else None

    try:
        return await asyncio.to_thread(fetch_version)
    except Exception as exc:
        LOGGER.warning("WEAO version fetch failed unexpectedly: %s", exc)
    return None


def _load_supported_games() -> list[dict[str, str | int]]:
    if not SUPPORTED_GAMES_FILE.exists():
        return []

    try:
        payload = json.loads(SUPPORTED_GAMES_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOGGER.warning(
            "Supported games file at %s is unreadable. Starting with an empty list.",
            SUPPORTED_GAMES_FILE,
        )
        return []

    if not isinstance(payload, list):
        return []

    games: list[dict[str, str | int]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        name, url, place_id = item.get("name"), item.get("url"), item.get("place_id")
        if not isinstance(name, str) or not isinstance(url, str) or not isinstance(place_id, int):
            continue
        name, url = name.strip(), url.strip()
        if name and url:
            games.append({"name": name, "url": url, "place_id": place_id})

    games.sort(key=lambda g: str(g["name"]).lower())
    return games


def _save_supported_games(games: list[dict[str, str | int]]) -> None:
    ordered = sorted(games, key=lambda g: str(g["name"]).lower())
    SUPPORTED_GAMES_FILE.write_text(json.dumps(ordered, indent=2), encoding="utf-8")


async def _refresh_supported_games_channel(
    client: discord.Client,
    guild: Optional[discord.Guild],
) -> None:
    try:
        channel = await _get_text_channel(client, SUPPORTED_CHANNEL_ID)
    except RuntimeError as exc:
        raise RuntimeError(
            f"{exc} I also couldn't refresh the supported games panel."
        ) from exc

    panel_emojis: dict[str, discord.Emoji] = {}
    if guild is not None:
        panel_emojis = await _ensure_update_panel_emojis(guild, client)

    try:
        await _clear_channel_messages(channel)
    except RuntimeError as exc:
        raise RuntimeError(
            f"{exc} I also couldn't refresh the supported games panel."
        ) from exc

    try:
        await channel.send(
            view=ensure_layout_view_action_rows(
                SupportedGamesView(
                    guild_id=guild.id if guild is not None else None,
                    panel_emojis=panel_emojis,
                )
            )
        )
    except discord.HTTPException as exc:
        raise RuntimeError(
            "I saved the supported games list, but I couldn't post the panel refresh."
        ) from exc


def _extract_roblox_place_id(game_link: str) -> Optional[int]:
    text = game_link.strip()
    if not text:
        return None

    parsed = urllib_parse.urlparse(text)
    candidates: list[str] = []

    if parsed.scheme and parsed.netloc:
        parts = [p for p in parsed.path.split("/") if p]
        for i, part in enumerate(parts):
            if part.lower() in ("games", "place") and i + 1 < len(parts):
                candidates.append(parts[i + 1])
        qs = urllib_parse.parse_qs(parsed.query)
        candidates.extend(qs.get("placeId", []))
        candidates.extend(qs.get("placeid", []))
    else:
        candidates.append(text)

    for candidate in candidates:
        match = re.search(r"\d+", candidate)
        if match:
            return int(match.group(0))

    return None


async def _resolve_roblox_game(place_id: int) -> Optional[dict[str, str | int]]:
    def fetch_game() -> Optional[dict[str, str | int]]:
        universe_req = urllib_request.Request(
            f"https://apis.roblox.com/universes/v1/places/{place_id}/universe",
            headers={"User-Agent": WEAO_USER_AGENT},
            method="GET",
        )
        with urllib_request.urlopen(universe_req, timeout=10) as resp:
            universe_payload = json.loads(resp.read().decode("utf-8"))

        universe_id = universe_payload.get("universeId")
        if not isinstance(universe_id, int):
            return None

        game_req = urllib_request.Request(
            f"https://games.roblox.com/v1/games?universeIds={universe_id}",
            headers={"User-Agent": WEAO_USER_AGENT},
            method="GET",
        )
        with urllib_request.urlopen(game_req, timeout=10) as resp:
            game_payload = json.loads(resp.read().decode("utf-8"))

        data = game_payload.get("data")
        if not isinstance(data, list) or not data or not isinstance(data, dict):
            return None

        game = data
        name = game.get("name")
        root_place_id = game.get("rootPlaceId")
        if not isinstance(name, str) or not isinstance(root_place_id, int):
            return None

        return {
            "name": name.strip(),
            "place_id": root_place_id,
            "url": f"https://www.roblox.com/games/{root_place_id}",
        }

    try:
        return await asyncio.to_thread(fetch_game)
    except (
        urllib_error.HTTPError,
        urllib_error.URLError,
        TimeoutError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        LOGGER.warning("Roblox game lookup failed for place %s: %s", place_id, exc)
        return None


class SupportedGamesView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        guild_id: Optional[int],
        panel_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        super().__init__(timeout=None)
        panel_emojis = panel_emojis or {}
        games = _load_supported_games()

        list_block = (
            "\n".join(
                f"`{i:02}` [{discord.utils.escape_markdown(str(g['name']))}]({g['url']})"
                for i, g in enumerate(games, start=1)
            )
            if games
            else "`--` No supported games have been added yet."
        )

        container = branded_panel_container(
            title=f"{_panel_emoji_text(panel_emojis, 'roblox')} SUPPORTED GAMES",
            description="Browse the Roblox experiences currently supported by ZyphraxHub Community.",
            accent_color=THESEUS_BLUE,
        )
        container.add_item(
            discord.ui.TextDisplay(
                f"{_panel_emoji_text(panel_emojis, 'settings')} **Supported Games**\n{list_block}"
            )
        )

        buttons: list[discord.ui.Button] = []
        for label, key, channel_id in (
            ("Download", "download", DOWNLOAD_CHANNEL_ID),
            ("Changelog", "notes", UPDATE_CHANNEL_ID),
        ):
            url = _channel_jump_url(guild_id, channel_id)
            if url is not None:
                buttons.append(
                    discord.ui.Button(
                        style=discord.ButtonStyle.link,
                        label=label,
                        url=url,
                        emoji=_panel_button_emoji(panel_emojis, key),
                    )
                )

        if buttons:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
            container.add_item(discord.ui.ActionRow(*buttons))

        self.add_item(container)


class UpdatePanelView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        version: str,
        roblox_version: Optional[str],
        changelog: str,
        notes: str,
        download_url: str,
        panel_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        super().__init__(timeout=None)
        panel_emojis = panel_emojis or {}
        version_text = _sanitize_panel_text(version)
        live_version = _sanitize_panel_text(roblox_version or version_text)

        container = branded_panel_container(
            title=f"{_panel_emoji_text(panel_emojis, 'windows')} {UPDATE_PANEL_TITLE}",
            description=(
                f"{_panel_emoji_text(panel_emojis, 'settings')} **Build Version**\n`{version_text}`"
            ),
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(
            discord.ui.TextDisplay(
                f"{_panel_emoji_text(panel_emojis, 'settings')} **Changelog**\n"
                f"{_format_changelog(live_version, changelog)}"
            )
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(
            discord.ui.TextDisplay(
                f"{_panel_emoji_text(panel_emojis, 'download')} **Download Link**\n"
                f"[Download ZyphraxHub Community Windows]({download_url})"
            )
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(
            discord.ui.TextDisplay(
                f"{_panel_emoji_text(panel_emojis, 'notes')} **Notes**\n{_format_notes(notes)}"
            )
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label="Download ZyphraxHub Community Windows",
                    url=download_url,
                    emoji=_panel_button_emoji(panel_emojis, "download"),
                ),
                discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label="Report Bugs",
                    url=REPORT_BUGS_URL,
                    emoji=_panel_button_emoji(panel_emojis, "settings"),
                ),
            )
        )
        container.add_item(
            discord.ui.ActionRow(
                discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label="Download Roblox LIVE",
                    url=ROBLOX_LIVE_DOWNLOAD_URL,
                    emoji=_panel_button_emoji(panel_emojis, "roblox"),
                ),
            )
        )

        self.add_item(container)



def _build_download_embed(
    *, version: str, roblox_version: Optional[str], download_url: str
) -> discord.Embed:
    live = _sanitize_panel_text(roblox_version or version)
    embed = discord.Embed(title="ZyphraxHub Community Windows FAQ", color=THESEUS_BLUE)
    embed.add_field(name="Build Version", value=f"`{_sanitize_panel_text(version)}`", inline=False)
    embed.add_field(name="Current Roblox Version", value=f"`{live}`", inline=False)
    embed.add_field(
        name="Compatibility & Performance",
        value=(
            "If you are encountering a missing dependency messagebox, "
            "use the Dependencies button on the download panel."
        ),
        inline=False,
    )
    embed.add_field(
        name="Antivirus Issues",
        value=(
            "If you use Kaspersky or Bitdefender, they may impact performance. "
            "Add ZyphraxHub Community to your antivirus whitelist.\n"
            "BitDefender: <https://www.bitdefender.com/consumer/support/answer/13427/>\n"
            "Kaspersky: <https://www.kaspersky.com/blog/kaspersky-add-exclusion/14765/>"
        ),
        inline=False,
    )
    embed.add_field(
        name="Download",
        value=f"[Download ZyphraxHub Community Windows]({download_url})",
        inline=False,
    )
    embed.set_footer(text="ZyphraxHub Community Team")
    return embed


def _build_dependencies_embed() -> discord.Embed:
    embed = discord.Embed(title="ZyphraxHub Community Windows Dependencies", color=THESEUS_BLUE)
    embed.add_field(
        name="Required Downloads",
        value=(
            f"[VCRUNTIME (x86_64 Visual C++ Redistributable)]({VCREDIST_URL})\n"
            f"[DirectX End User Runtime]({DIRECTX_RUNTIME_URL})\n"
            f"[D3DCompiler Update]({D3DCOMPILER_URL})"
        ),
        inline=False,
    )
    embed.set_footer(text="ZyphraxHub Community Team")
    return embed


class DownloadFaqButton(discord.ui.Button):
    def __init__(
        self,
        panel_emojis: dict[str, discord.Emoji],
        *,
        version: str,
        roblox_version: Optional[str],
        download_url: str,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="FAQ",
            custom_id="theseus_download_faq",
            emoji=_panel_button_emoji(panel_emojis, "notes"),
        )
        self.version = version
        self.roblox_version = roblox_version
        self.download_url = download_url

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            embed=_build_download_embed(
                version=self.version,
                roblox_version=self.roblox_version,
                download_url=self.download_url,
            ),
            ephemeral=True,
        )


class DownloadDependenciesButton(discord.ui.Button):
    def __init__(self, panel_emojis: dict[str, discord.Emoji]) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Dependencies",
            custom_id="theseus_download_dependencies",
            emoji=_panel_button_emoji(panel_emojis, "settings"),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            embed=_build_dependencies_embed(), ephemeral=True
        )


class DownloadPanelView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        download_url: str,
        version: str = "Latest",
        roblox_version: Optional[str] = None,
        panel_emojis: Optional[dict[str, discord.Emoji]] = None,
    ) -> None:
        super().__init__(timeout=None)
        panel_emojis = panel_emojis or {}
        version_text = _sanitize_panel_text(version)
        live_version = _sanitize_panel_text(roblox_version or version_text)

        container = branded_panel_container(
            title=f"{_panel_emoji_text(panel_emojis, 'download')} Download ZyphraxHub Community",
            description=(
                f"**Build Version:** `{version_text}`\n"
                f"**Current Roblox Version:** `{live_version}`"
            ),
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label="Download",
                    url=download_url,
                    emoji=_panel_button_emoji(panel_emojis, "download"),
                ),
                DownloadFaqButton(
                    panel_emojis,
                    version=version_text,
                    roblox_version=live_version,
                    download_url=download_url,
                ),
                DownloadDependenciesButton(panel_emojis),
            )
        )

        self.add_item(container)


def _build_paypanel_banner_url() -> Optional[str]:
    banner_path = _resolve_paypanel_banner_path()
    if banner_path is None:
        return None
    return f"attachment://{banner_path.name}"


def _paypanel_emoji_text(panel_emojis: dict[str, discord.Emoji], key: str) -> str:
    emoji = panel_emojis.get(key)
    return f"{emoji} " if emoji is not None else ""


def _paypanel_button_emoji(
    panel_emojis: dict[str, discord.Emoji], key: str
) -> Optional[discord.Emoji]:
    emoji = panel_emojis.get(key)
    return emoji


def _build_paypanel_description_v2(panel_emojis: dict[str, discord.Emoji]) -> str:
    return (
        "Choose plan -> Pay -> Get key\n\n"
        f"{_paypanel_emoji_text(panel_emojis, 'paypal')} **Pricing**\n"
        "Weekly - $1 / Rp15K\n"
        "Monthly - $1.5 / Rp25K\n"
        "Lifetime - $2.5 / Rp40K\n\n"
        f"{_paypanel_emoji_text(panel_emojis, 'proof')} **Proof Needed**\n"
        "Transaction ID, amount, name, timestamp, screenshot\n\n"
        f"{_paypanel_emoji_text(panel_emojis, 'done')} **Process**\n"
        "Choose plan and pay\n"
        "Open ticket\n"
        "Attach proof\n"
        "Wait Whitelist Manager response\n\n"
        "`ZyphraxHub | Secure | Fast`"
    )


def _build_paypanel_description() -> str:
    return (
        "Choose plan -> Pay -> Get key\n\n"
        "**Pricing**\n"
        "Weekly - $1 / Rp15K\n"
        "Monthly - $1.5 / Rp25K\n"
        "Lifetime - $2.5 / Rp40K\n\n"
        "**Proof Needed**\n"
        "Transaction ID, amount, name, timestamp, screenshot\n\n"
        "**Process**\n"
        "Choose plan and pay\n"
        "Open ticket\n"
        "Attach proof\n"
        "Wait Whitelist Manager response\n\n"
        "`ZyphraxHub  •  Secure  •  Fast`"
    )


def _build_paypanel_paypal_text() -> tuple[str, str]:
    title = "PayPal Payment"
    if PAYPAL_URL:
        description = (
            "Use the link below to pay with PayPal.\n\n"
            f"[PayPal (Click Blue Text)]({PAYPAL_URL})\n\n"
            "Attach proof in a ticket after payment."
        )
    else:
        description = (
            "PayPal is not configured yet.\n\n"
            "Set `PAYPAL_URL` in `.env` to enable the PayPal button.\n\n"
            "Attach proof in a ticket after payment."
        )
    return title, description


def _build_paypanel_crypto_text() -> tuple[str, str]:
    return (
        "Crypto Payment",
        (
            "**Bitcoin**\n"
            f"`{BITCOIN_ADDRESS}`\n\n"
            "**Litecoin**\n"
            f"`{LITECOIN_ADDRESS}`\n\n"
            "**Proof Needed**\n"
            "Transaction ID, amount, name, timestamp, screenshot\n\n"
            "Pay, then open a ticket and attach your proof."
        ),
    )


def _build_paypanel_qris_text() -> tuple[str, str]:
    qris_path = _resolve_paypanel_qris_path()
    missing_note = (
        "\n\n**QRIS Image Missing**\n"
        "Add a QRIS image in `icon pack` using `qris.png` or `payment_qris.png`."
        if qris_path is None
        else ""
    )
    return (
        "QRIS Payment",
        (
            "Scan the QR code, open a ticket, and attach your proof.\n\n"
            "**Proof Needed**\n"
            "Transaction ID, amount, name, timestamp, screenshot\n\n"
            "After payment, open a ticket and attach your proof."
            f"{missing_note}"
        ),
    )


class PaymentProofModal(discord.ui.Modal):
    def __init__(self, *, payment_method: str) -> None:
        self.payment_method = payment_method
        super().__init__(title=f"{payment_method} Payment Proof", timeout=300)

        self.plan = discord.ui.TextInput(
            label="Plan",
            placeholder="Weekly, Monthly, or Lifetime",
            required=True,
            max_length=50,
        )
        self.proof_details = discord.ui.TextInput(
            label="Proof Details",
            placeholder="Transaction ID, amount, name, timestamp, and any extra details",
            required=True,
            style=discord.TextStyle.paragraph,
            max_length=1000,
        )
        self.add_item(self.plan)
        self.add_item(self.proof_details)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        details = (
            f"Payment Method: {self.payment_method}\n"
            f"Plan: {self.plan.value.strip()}\n"
            f"Proof Details: {self.proof_details.value.strip()}\n"
            "Reminder: Attach your payment screenshot in the created ticket."
        )
        await bot.ticket_system.create_ticket_from_modal(
            interaction,
            category="general",
            details=details,
        )


class PayPanelCreateTicketButton(discord.ui.Button):
    def __init__(self, *, payment_method: str) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Create Ticket",
            custom_id=f"zyphraxhub_paypanel_ticket_{payment_method.lower()}",
        )
        self.payment_method = payment_method

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(
            PaymentProofModal(payment_method=self.payment_method)
        )


class PayPanelMethodView(discord.ui.LayoutView):
    def __init__(self, panel_emojis: dict[str, discord.Emoji], *, selected_method: str) -> None:
        super().__init__(timeout=300)
        if selected_method == "PayPal":
            title, description = _build_paypanel_paypal_text()
            banner_url = None
        elif selected_method == "Crypto":
            title, description = _build_paypanel_crypto_text()
            banner_url = None
        else:
            title, description = _build_paypanel_qris_text()
            qris_path = _resolve_paypanel_qris_path()
            banner_url = f"attachment://{qris_path.name}" if qris_path is not None else None

        container = branded_panel_container(
            title=title,
            description=description,
            banner_url=banner_url,
            accent_color=THESEUS_BLUE,
            banner_separated=True,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                PayPanelPaypalButton(panel_emojis),
                PayPanelCryptoButton(panel_emojis),
                PayPanelQrisButton(panel_emojis),
                PayPanelCreateTicketButton(payment_method=selected_method),
            )
        )
        self.add_item(container)


class PayPanelPaypalButton(discord.ui.Button):
    def __init__(self, panel_emojis: dict[str, discord.Emoji]) -> None:
        self.panel_emojis = panel_emojis
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="PayPal",
            custom_id="zyphraxhub_paypanel_paypal",
            emoji=_paypanel_button_emoji(panel_emojis, "paypal"),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            view=ensure_layout_view_action_rows(
                PayPanelMethodView(self.panel_emojis, selected_method="PayPal")
            ),
            ephemeral=True,
        )


class PayPanelCryptoButton(discord.ui.Button):
    def __init__(self, panel_emojis: dict[str, discord.Emoji]) -> None:
        self.panel_emojis = panel_emojis
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Crypto",
            custom_id="zyphraxhub_paypanel_crypto",
            emoji=_paypanel_button_emoji(panel_emojis, "crypto"),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            view=ensure_layout_view_action_rows(
                PayPanelMethodView(self.panel_emojis, selected_method="Crypto")
            ),
            ephemeral=True,
        )


class PayPanelQrisButton(discord.ui.Button):
    def __init__(self, panel_emojis: dict[str, discord.Emoji]) -> None:
        self.panel_emojis = panel_emojis
        super().__init__(
            style=discord.ButtonStyle.success,
            label="QRIS",
            custom_id="zyphraxhub_paypanel_qris",
            emoji=_paypanel_button_emoji(panel_emojis, "qris"),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        qris_path = _resolve_paypanel_qris_path()
        file = discord.File(qris_path, filename=qris_path.name) if qris_path is not None else None
        kwargs: dict[str, object] = {
            "view": ensure_layout_view_action_rows(
                PayPanelMethodView(self.panel_emojis, selected_method="QRIS")
            ),
            "ephemeral": True,
        }
        if file is not None:
            kwargs["file"] = file
        await interaction.response.send_message(**kwargs)


class PayPanelView(discord.ui.LayoutView):
    def __init__(self, panel_emojis: Optional[dict[str, discord.Emoji]] = None) -> None:
        super().__init__(timeout=None)
        panel_emojis = panel_emojis or {}
        container = branded_panel_container(
            title=f"{_paypanel_emoji_text(panel_emojis, 'key')} Purchase Script Key",
            description=_build_paypanel_description_v2(panel_emojis),
            banner_url=_build_paypanel_banner_url(),
            accent_color=THESEUS_BLUE,
            banner_separated=True,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                PayPanelPaypalButton(panel_emojis),
                PayPanelCryptoButton(panel_emojis),
                PayPanelQrisButton(panel_emojis),
                PayPanelCreateTicketButton(payment_method="Payment"),
            )
        )
        self.add_item(container)


class RedeemKeyModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Redeem ZyphraxHub Community Key", timeout=300)
        self.key_input = discord.ui.TextInput(
            label="License Key",
            placeholder="ZyphraxHub-XXX-XXX-XXX",
            required=True,
            max_length=64,
        )
        self.add_item(self.key_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        normalized_key = self.key_input.value.strip()
        if not _validate_whitelist_key_format(normalized_key):
            await interaction.response.send_message(
                f"Invalid key format. Expected `{KEY_PREFIX}-XXX-XXX-XXX`.",
                ephemeral=True,
            )
            return

        if await whitelist_store.is_blacklisted(interaction.user.id):
            await interaction.response.send_message(
                "Your account is blacklisted and cannot redeem keys.",
                ephemeral=True,
            )
            return

        if not await whitelist_store.key_exists_and_unused(normalized_key):
            await interaction.response.send_message(
                "That key is invalid or already used.",
                ephemeral=True,
            )
            return

        if not await whitelist_store.redeem_key(interaction.user.id, normalized_key):
            await interaction.response.send_message(
                "I could not redeem that key right now.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            embed=_whitelist_embed(
                "Key Redeemed",
                f"Your key `{normalized_key}` has been activated.",
                color=0x2ECC71,
            ),
            ephemeral=True,
        )


class DashboardRedeemButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Redeem Key",
            custom_id="zyphraxhub_dashboard_redeem",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(RedeemKeyModal())


class DashboardMyInfoButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="My Info",
            custom_id="zyphraxhub_dashboard_myinfo",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        user = await whitelist_store.get_user_with_stats(interaction.user.id)
        is_banned = await whitelist_store.is_blacklisted(interaction.user.id)
        has_active_key = bool(user and user.get("key"))
        color = 0xE74C3C if is_banned else (0x2ECC71 if has_active_key else 0xF1C40F)
        embed = _whitelist_embed("Your Whitelist Account", color=color)
        embed.add_field(
            name="User",
            value=f"{interaction.user.mention}\nID: `{interaction.user.id}`",
            inline=False,
        )
        if user is not None and user.get("key"):
            embed.add_field(name="Key", value=_mask_key(user.get("key")), inline=False)
            embed.add_field(name="HWID", value=user.get("hwid") or "Not set", inline=True)
            embed.add_field(name="Joined", value=user.get("joined_at") or "Unknown", inline=True)
            embed.add_field(name="Last Login", value=user.get("last_login") or "Never", inline=True)
            embed.add_field(name="Access Expires", value=_format_access_expiry(user), inline=True)
            embed.add_field(name="Luarmor", value=_format_luarmor_status(user), inline=False)
            embed.add_field(
                name="Stats",
                value=(
                    f"Redeems: `{user.get('redeem_count', 0)}`\n"
                    f"Logins: `{user.get('login_count', 0)}`"
                ),
                inline=False,
            )
        else:
            embed.add_field(
                name="Status",
                value="No active key. Use `Redeem Key` to activate one.",
                inline=False,
            )
        embed.add_field(
            name="Blacklist",
            value="Blacklisted" if is_banned else "Active",
            inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


class DashboardRefreshButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Refresh",
            custom_id="zyphraxhub_dashboard_refresh",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            view=ensure_layout_view_action_rows(
                await UserDashboardView.build_for_user(interaction.user.id)
            )
        )


class UserDashboardView(discord.ui.LayoutView):
    def __init__(self, *, user: Optional[dict[str, object]], is_banned: bool) -> None:
        super().__init__(timeout=300)
        container = branded_panel_container(
            title="Your ZyphraxHub Community Dashboard",
            description=(
                f"{_dashboard_status_text(user, is_banned=is_banned)}\n\n"
                f"{_dashboard_summary_text(user, is_banned=is_banned)}"
            ),
            accent_color=THESEUS_BLUE,
        )
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(
            discord.ui.ActionRow(
                DashboardRedeemButton(),
                DashboardMyInfoButton(),
                PayPanelCreateTicketButton(payment_method="General Support"),
                DashboardRefreshButton(),
            )
        )
        self.add_item(container)

    @classmethod
    async def build_for_user(cls, user_id: int) -> "UserDashboardView":
        user = await whitelist_store.get_user_with_stats(user_id)
        is_banned = await whitelist_store.is_blacklisted(user_id)
        return cls(user=user, is_banned=is_banned)


def _build_help_embed(interaction: discord.Interaction) -> discord.Embed:
    embed = discord.Embed(
        title="ZyphraxHub Community Bot Help",
        description=(
            "Commands are grouped by what you can use right now.\n"
            "Most responses are sent privately, while announcement actions post in their configured channels."
        ),
        color=THESEUS_BLUE,
    )

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    is_staff = member is not None and _member_has_role(member, ALLOWED_ROLE_ID)
    is_ticket_mgmt = (
        member is not None
        and interaction.guild is not None
        and bot.ticket_system.is_management_member(member)
    )
    is_ticket_staff = member is not None and bot.ticket_system.is_ticket_staff(member)

    embed.add_field(
        name="Everyone",
        value=(
            "`/help` Show this command list.\n"
            "`/panel` Open your ZyphraxHub Community dashboard.\n"
            "`/supported` Show the current supported games list.\n"
            "`/redeem` Redeem a ZyphraxHub Community key.\n"
            "`/myinfo` View your whitelist account information.\n"
            "`/ticket create` Open a support ticket from a category picker.\n"
            "`/ticket close` Close your current ticket when it is resolved."
        ),
        inline=False,
    )
    embed.add_field(
        name="Ticket Staff",
        value=(
            "`/ticket add` Add a member to the current ticket.\n"
            "`/ticket remove` Remove a member from the current ticket.\n"
            "`/ticket rename` Rename the current ticket channel.\n"
            "`/ticket transcript` Generate an HTML transcript.\n"
            "`/ticket panel` Post the ticket creation panel.\n"
            "`/ticketpanel` Legacy alias for `/ticket panel`."
        ),
        inline=False,
    )
    embed.add_field(
        name="Ticket Management",
        value=(
            "`/ticket setup` Configure the ticket category, support role, and optional log channel.\n"
            "`/ticket settings` Show the current ticket configuration."
        ),
        inline=False,
    )
    embed.add_field(
        name="Announcement Staff",
        value=(
            f"`/say` Send plain text or one attachment in the current channel.\n"
            f"`/announce` Post an announcement embed with an optional attachment in <#{ANNOUNCEMENT_CHANNEL_ID}>.\n"
            "`/paypanel` Post the purchase panel in the current channel.\n"
            "`/whitelist` Auto-whitelist a user and assign a key.\n"
            "`/unwhitelist` Remove a user and release their key.\n"
            "`/blacklist`, `/unblacklist`, `/resethwid` manage access state.\n"
            "`/lookup` Inspect a whitelist record.\n"
            "`/luarmorsync`, `/luarmoraudit` inspect Luarmor sync.\n"
            "`/genkey`, `/masskey`, `/keylist`, `/delkey`, `/purgekeys` manage keys.\n"
            f"`/update` Upload a build and post the update panel in <#{UPDATE_CHANNEL_ID}>.\n"
            f"`/support add` Add a Roblox game to the supported list.\n"
            f"`/support remove` Remove a supported game from the list."
        ),
        inline=False,
    )
    embed.add_field(
        name="Your Access",
        value="\n".join([
            f"Announcement role: <@&{ALLOWED_ROLE_ID}>",
            f"Ticket staff access: {'Yes' if is_ticket_staff else 'No'}",
            f"Ticket management access: {'Yes' if is_ticket_mgmt else 'No'}",
            f"Announcement access: {'Yes' if is_staff else 'No'}",
        ]),
        inline=False,
    )
    embed.add_field(
        name="Usage Notes",
        value=(
            "`/announce` can optionally ping `@everyone` and include one attachment.\n"
            "`/paypanel` looks for `icon pack/qris.*` and uses the largest image in `icon pack` as the banner fallback.\n"
            "`/update` requires notes and a build attachment; changelog entries are optional.\n"
            "`/panel` and `/myinfo` show your current local whitelist status.\n"
            "`/redeem` accepts either the local ZyphraxHub key format or Luarmor-issued keys.\n"
            "`/support add` expects a Roblox game link and resolves the game name automatically.\n"
            "Ticket panel actions and ticket commands must be used inside a server."
        ),
        inline=False,
    )
    embed.set_footer(text="ZyphraxHub Community Team")
    return embed


async def supported_game_name_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    search = current.strip().lower()
    choices: list[app_commands.Choice[str]] = []
    for game in _load_supported_games():
        name = str(game["name"])
        if search and search not in name.lower():
            continue
        choices.append(app_commands.Choice(name=name[:100], value=name))
        if len(choices) >= 25:
            break
    return choices


@bot.tree.command(name="help", description="Show the available ZyphraxHub Community bot commands")
async def help_command(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(
        embed=_build_help_embed(interaction), ephemeral=True
    )


@bot.tree.command(name="supported", description="Show the current supported Roblox games")
async def supported(interaction: discord.Interaction) -> None:
    if interaction.channel is None:
        await interaction.response.send_message(
            "This command can only be used in a server channel.", ephemeral=True
        )
        return

    panel_emojis: dict[str, discord.Emoji] = {}
    if interaction.guild is not None:
        panel_emojis = await _ensure_update_panel_emojis(
            interaction.guild, interaction.client
        )

    await interaction.response.defer(ephemeral=True)
    await interaction.channel.send(
        view=ensure_layout_view_action_rows(
            SupportedGamesView(
                guild_id=interaction.guild_id,
                panel_emojis=panel_emojis,
            )
        )
    )
    await interaction.followup.send("Posted supported titles in this channel.", ephemeral=True)


@bot.tree.command(name="testwelcome", description="Send a test welcome card for a member")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(user="The member to generate the welcome message for")
async def testwelcome(interaction: discord.Interaction, user: discord.Member) -> None:
    if interaction.channel is None:
        await interaction.response.send_message(
            "This command can only be used in a server channel.", ephemeral=True
        )
        return

    if bot.welcome_system is None:
        await interaction.response.send_message(
            "The welcome system is not configured.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    sent = await bot.welcome_system.send_welcome(user, channel=interaction.channel)
    if not sent:
        await interaction.followup.send(
            "I couldn't send the test welcome message in this channel.", ephemeral=True
        )
        return

    await interaction.followup.send(
        f"Posted a test welcome message for {user.mention}.", ephemeral=True
    )


@bot.tree.command(name="say", description="Send a plain text message to this channel")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(
    message="The message for the bot to send",
    attachment="Optional file attachment, like an image or video",
)
async def say(
    interaction: discord.Interaction,
    message: str,
    attachment: Optional[discord.Attachment] = None,
) -> None:
    if interaction.channel is None:
        await interaction.response.send_message(
            "This command can only be used in a server channel.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    file = await attachment.to_file() if attachment is not None else None
    await interaction.channel.send(
        message,
        file=file,
        allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
    )
    await interaction.followup.send("Message sent.", ephemeral=True)


@bot.tree.command(name="announce", description="Create a ZyphraxHub Community announcement embed")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.rename(bottom_text="footer", ping_everyone="everyone")
@app_commands.describe(
    title="The title of the announcement",
    body="The main text of the announcement",
    bottom_text="Text shown above the ZyphraxHub Community footer signoff",
    attachment="Optional file attachment, like an image or video",
    ping_everyone="Ping @everyone before the embed",
)
async def announce(
    interaction: discord.Interaction,
    title: str,
    body: str,
    bottom_text: str,
    attachment: Optional[discord.Attachment] = None,
    ping_everyone: bool = False,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.", ephemeral=True
        )
        return

    embed = discord.Embed(title=title, description=body, color=THESEUS_BLUE)
    embed.set_footer(text=f"{bottom_text}\n\u2014 ZyphraxHub Community Team")

    try:
        target_channel = await _get_text_channel(interaction.client, ANNOUNCEMENT_CHANNEL_ID)
    except RuntimeError as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    file = await attachment.to_file() if attachment is not None else None
    await target_channel.send(
        content="@everyone" if ping_everyone else None,
        embed=embed,
        file=file,
        allowed_mentions=discord.AllowedMentions(
            everyone=ping_everyone, roles=False, users=False
        ),
    )
    await interaction.followup.send(
        f"Announcement posted in <#{ANNOUNCEMENT_CHANNEL_ID}>.", ephemeral=True
    )


@bot.tree.command(name="paypanel", description="Post the purchase payment panel")
@app_commands.guild_only()
@allowed_role_only()
async def paypanel(interaction: discord.Interaction) -> None:
    if interaction.channel is None:
        await interaction.response.send_message(
            "This command can only be used in a server channel.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    panel_emojis: dict[str, discord.Emoji] = {}
    if interaction.guild is not None:
        panel_emojis = await _ensure_paypanel_emojis(interaction.guild, interaction.client)

    banner_path = _resolve_paypanel_banner_path()
    file = discord.File(banner_path, filename=banner_path.name) if banner_path is not None else None

    await interaction.channel.send(
        file=file,
        view=ensure_layout_view_action_rows(PayPanelView(panel_emojis)),
    )

    warnings: list[str] = []
    if banner_path is None:
        warnings.append("No pay panel banner found in `icon pack`.")
    if not PAYPAL_URL:
        warnings.append("`PAYPAL_URL` is empty, so the PayPal button shows a setup notice.")

    status = "Payment panel posted in this channel."
    if warnings:
        status += " " + " ".join(warnings)
    await interaction.followup.send(status, ephemeral=True)


@bot.tree.command(name="update", description="Post the ZyphraxHub Community Windows update panel")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.rename(ping_everyone="everyone")
@app_commands.describe(
    version="The build version shown in the panel",
    notes="Comma-separated notes shown under the panel",
    release_file="The latest ZyphraxHub Community Windows build to upload to Discord",
    changelog="Optional comma-separated changelog entries",
    ping_everyone="Ping @everyone after posting the update panel",
)
async def update(
    interaction: discord.Interaction,
    version: str,
    notes: str,
    release_file: discord.Attachment,
    changelog: Optional[str] = None,
    ping_everyone: bool = False,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.", ephemeral=True
        )
        return

    version_text = _sanitize_panel_text(version)
    note_items = _split_panel_items(notes)

    if not version_text or not note_items:
        await interaction.response.send_message(
            "Version and notes are required. "
            "Use commas to separate note items.",
            ephemeral=True,
        )
        return

    try:
        update_channel = await _get_text_channel(interaction.client, UPDATE_CHANNEL_ID)
        download_channel = await _get_text_channel(interaction.client, DOWNLOAD_CHANNEL_ID)
    except RuntimeError as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    panel_emojis = await _ensure_update_panel_emojis(interaction.guild, interaction.client)
    roblox_version = await _get_latest_roblox_windows_version()

    try:
        sent = await update_channel.send(file=await release_file.to_file())
    except discord.HTTPException:
        await interaction.followup.send(
            "I couldn't upload that build to the announcement channel.", ephemeral=True
        )
        return

    if not sent.attachments:
        await interaction.followup.send(
            "The build uploaded, but I couldn't attach it to the update panel.", ephemeral=True
        )
        return

    download_url = sent.attachments.url
    panel_view = ensure_layout_view_action_rows(
        UpdatePanelView(
            version=version_text,
            roblox_version=roblox_version,
            changelog=changelog,
            notes=", ".join(note_items),
            download_url=download_url,
            panel_emojis=panel_emojis,
        )
    )

    try:
        await sent.edit(view=panel_view)
    except discord.HTTPException:
        await interaction.followup.send(
            "I uploaded the build, but I couldn't attach the update panel to it.", ephemeral=True
        )
        return

    try:
        await _clear_channel_messages(download_channel)
    except RuntimeError as exc:
        await interaction.followup.send(str(exc), ephemeral=True)
        return

    try:
        await download_channel.send(
            view=ensure_layout_view_action_rows(
                DownloadPanelView(
                    download_url=download_url,
                    version=version_text,
                    roblox_version=roblox_version,
                    panel_emojis=panel_emojis,
                )
            )
        )
    except discord.HTTPException:
        await interaction.followup.send(
            "I posted the update panel, but I couldn't create the download panel.", ephemeral=True
        )
        return

    if ping_everyone:
        await update_channel.send(
            content="@everyone",
            allowed_mentions=discord.AllowedMentions(everyone=True, roles=False, users=False),
            reference=sent.to_reference(fail_if_not_exists=False),
        )

    await interaction.followup.send(
        f"Update panel posted in <#{UPDATE_CHANNEL_ID}> and "
        f"download panel refreshed in <#{DOWNLOAD_CHANNEL_ID}>.",
        ephemeral=True,
    )


@bot.tree.command(name="redeem", description="Redeem your ZyphraxHub Community key")
@app_commands.guild_only()
@app_commands.describe(key="Your ZyphraxHub Community key")
async def redeem(interaction: discord.Interaction, key: str) -> None:
    normalized_key = key.strip()
    if not _validate_whitelist_key_format(normalized_key):
        await interaction.response.send_message(
            _redeem_format_hint(),
            ephemeral=True,
        )
        return

    if await whitelist_store.is_blacklisted(interaction.user.id):
        await interaction.response.send_message(
            "Your account is blacklisted and cannot redeem keys.",
            ephemeral=True,
        )
        return

    if not await whitelist_store.key_exists_and_unused(normalized_key):
        await interaction.response.send_message(
            "That key is invalid or already used.",
            ephemeral=True,
        )
        return

    if not await whitelist_store.redeem_key(interaction.user.id, normalized_key):
        await interaction.response.send_message(
            "I could not redeem that key right now.",
            ephemeral=True,
        )
        return

    embed = _whitelist_embed(
        "Key Redeemed",
        "Your ZyphraxHub Community key has been activated successfully.",
        color=0x2ECC71,
    )
    embed.add_field(name="Key", value=_mask_key(normalized_key), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="panel", description="Open your ZyphraxHub Community dashboard")
@app_commands.guild_only()
async def panel(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(
        view=ensure_layout_view_action_rows(
            await UserDashboardView.build_for_user(interaction.user.id)
        ),
        ephemeral=True,
    )


@bot.tree.command(name="myinfo", description="View your whitelist information")
@app_commands.guild_only()
async def myinfo(interaction: discord.Interaction) -> None:
    user = await whitelist_store.get_user_with_stats(interaction.user.id)
    is_banned = await whitelist_store.is_blacklisted(interaction.user.id)
    has_active_key = bool(user and user.get("key"))
    color = 0xE74C3C if is_banned else (0x2ECC71 if has_active_key else 0xF1C40F)
    embed = _whitelist_embed("Your Whitelist Account", color=color)
    embed.add_field(
        name="User",
        value=f"{interaction.user.mention}\nID: `{interaction.user.id}`",
        inline=False,
    )
    if user is not None and user.get("key"):
        embed.add_field(name="Key", value=_mask_key(user.get("key")), inline=False)
        embed.add_field(name="HWID", value=user.get("hwid") or "Not set", inline=True)
        embed.add_field(name="Joined", value=user.get("joined_at") or "Unknown", inline=True)
        embed.add_field(name="Last Login", value=user.get("last_login") or "Never", inline=True)
        embed.add_field(name="Access Expires", value=_format_access_expiry(user), inline=True)
        embed.add_field(name="Luarmor", value=_format_luarmor_status(user), inline=False)
        embed.add_field(
            name="Stats",
            value=(
                f"Redeems: `{user.get('redeem_count', 0)}`\n"
                f"Logins: `{user.get('login_count', 0)}`"
            ),
            inline=False,
        )
    else:
        embed.add_field(name="Status", value="No active key. Use `/redeem` to activate one.", inline=False)

    embed.add_field(
        name="Blacklist",
        value="Blacklisted" if is_banned else "Active",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="whitelist", description="Auto-whitelist a user and assign a key")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(member="The user to whitelist", send_dm="Send the assigned key by DM")
async def whitelist(
    interaction: discord.Interaction,
    member: discord.Member,
    send_dm: bool = True,
) -> None:
    if member.bot:
        await interaction.response.send_message("Bots cannot be whitelisted.", ephemeral=True)
        return

    if await whitelist_store.is_blacklisted(member.id):
        reason = await whitelist_store.get_blacklist_reason(member.id)
        embed = _whitelist_embed(
            "Cannot Whitelist",
            f"{member.mention} is blacklisted and cannot receive a key.",
            color=0xE74C3C,
        )
        if reason:
            embed.add_field(name="Blacklist Reason", value=discord.utils.escape_markdown(reason), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        key = await whitelist_store.whitelist_user(member.id, created_by=interaction.user.id)
    except ValueError:
        reason = await whitelist_store.get_blacklist_reason(member.id)
        embed = _whitelist_embed(
            "Cannot Whitelist",
            f"{member.mention} is blacklisted and cannot receive a key.",
            color=0xE74C3C,
        )
        if reason:
            embed.add_field(name="Blacklist Reason", value=discord.utils.escape_markdown(reason), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    dm_status = "Not sent"
    if send_dm:
        try:
            dm_embed = _whitelist_embed(
                "Welcome to ZyphraxHub Community",
                f"You were whitelisted by **{interaction.user.display_name}**.",
                color=0x2ECC71,
            )
            dm_embed.add_field(name="Your Key", value=f"`{key}`", inline=False)
            dm_embed.add_field(
                name="How To Use It",
                value="Run `/redeem` in the server and paste this key.",
                inline=False,
            )
            await member.send(embed=dm_embed)
            dm_status = "Sent"
        except discord.Forbidden:
            dm_status = "DMs disabled"

    embed = _whitelist_embed(
        "User Whitelisted",
        f"{member.mention} now has an active ZyphraxHub Community key.",
        color=0x2ECC71,
    )
    embed.add_field(name="Key", value=f"`{key}`", inline=False)
    embed.add_field(name="DM Status", value=dm_status, inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="unwhitelist", description="Remove a user from the whitelist")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(member="The user to remove from whitelist")
async def unwhitelist(interaction: discord.Interaction, member: discord.Member) -> None:
    await interaction.response.defer(ephemeral=True)
    released_key = await whitelist_store.unwhitelist_user(member.id)
    if released_key is None:
        await interaction.followup.send(
            embed=_whitelist_embed("Not Whitelisted", f"{member.mention} does not have an active key.", color=0xE74C3C),
            ephemeral=True,
        )
        return

    embed = _whitelist_embed(
        "User Removed",
        f"{member.mention} was removed from the whitelist.",
        color=0x2ECC71,
    )
    embed.add_field(name="Released Key", value=f"`{released_key}`", inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="blacklist", description="Blacklist a user locally and on Luarmor")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(member="The user to blacklist", reason="Why they are being blacklisted")
async def blacklist(interaction: discord.Interaction, member: discord.Member, reason: str) -> None:
    await interaction.response.defer(ephemeral=True)
    await whitelist_store.blacklist_user(member.id, reason=reason, ban_expire=-1)
    embed = _whitelist_embed(
        "User Blacklisted",
        f"{member.mention} was blacklisted.",
        color=0xE74C3C,
    )
    embed.add_field(
        name="Reason",
        value=discord.utils.escape_markdown(reason.strip() or "Blacklisted by staff."),
        inline=False,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="unblacklist", description="Remove a user blacklist locally and on Luarmor")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(member="The user to unblacklist")
async def unblacklist(interaction: discord.Interaction, member: discord.Member) -> None:
    await interaction.response.defer(ephemeral=True)
    await whitelist_store.unblacklist_user(member.id)
    embed = _whitelist_embed(
        "User Unblacklisted",
        f"{member.mention} can redeem and run again.",
        color=0x2ECC71,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="resethwid", description="Reset a user's HWID locally and on Luarmor")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(member="The user whose HWID should be reset", force="Force Luarmor cooldown bypass")
async def resethwid(
    interaction: discord.Interaction,
    member: discord.Member,
    force: bool = False,
) -> None:
    await interaction.response.defer(ephemeral=True)
    if not await whitelist_store.reset_hwid(member.id, force=force):
        await interaction.followup.send(
            embed=_whitelist_embed(
                "No Active Key",
                f"{member.mention} does not have an active key.",
                color=0xF1C40F,
            ),
            ephemeral=True,
        )
        return

    embed = _whitelist_embed(
        "HWID Reset",
        f"HWID reset completed for {member.mention}.",
        color=0x2ECC71,
    )
    embed.add_field(name="Forced", value="Yes" if force else "No", inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="lookup", description="Look up a whitelist user by mention or ID")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(user="Discord mention or raw ID")
async def lookup(interaction: discord.Interaction, user: str) -> None:
    discord_id = _lookup_discord_id(user)
    if discord_id is None:
        await interaction.response.send_message("Invalid Discord mention or ID.", ephemeral=True)
        return

    record = await whitelist_store.get_user_with_stats(discord_id)
    if record is None:
        await interaction.response.send_message(
            embed=_whitelist_embed("Not Found", f"No whitelist record exists for `{discord_id}`.", color=0xE74C3C),
            ephemeral=True,
        )
        return

    is_banned = await whitelist_store.is_blacklisted(discord_id)
    embed = _whitelist_embed(
        f"Whitelist Lookup: {discord_id}",
        color=0xE74C3C if is_banned else THESEUS_BLUE,
    )
    try:
        discord_user = await interaction.client.fetch_user(int(discord_id))
        embed.set_thumbnail(url=discord_user.display_avatar.url)
        embed.description = f"**{discord_user.name}**"
    except (discord.NotFound, discord.HTTPException, ValueError):
        pass
    embed.add_field(name="Key", value=f"`{record.get('key') or 'None'}`", inline=False)
    embed.add_field(name="HWID", value=record.get("hwid") or "Not set", inline=True)
    embed.add_field(name="Joined", value=record.get("joined_at") or "Unknown", inline=True)
    embed.add_field(name="Last Login", value=record.get("last_login") or "Never", inline=True)
    embed.add_field(name="Access Expires", value=_format_access_expiry(record), inline=True)
    embed.add_field(name="Luarmor", value=_format_luarmor_status(record), inline=False)
    embed.add_field(
        name="Stats",
        value=f"Redeems: `{record.get('redeem_count', 0)}`\nLogins: `{record.get('login_count', 0)}`",
        inline=False,
    )
    status_text = "Blacklisted" if is_banned else ("Active" if record.get("key") else "Inactive")
    embed.add_field(name="Status", value=status_text, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="luarmorsync", description="Resync one whitelist user to Luarmor")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(user="Discord mention or raw ID")
async def luarmorsync(interaction: discord.Interaction, user: str) -> None:
    discord_id = _lookup_discord_id(user)
    if discord_id is None:
        await interaction.response.send_message("Invalid Discord mention or ID.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    remote_user = await whitelist_store.resync_user_to_luarmor(discord_id)
    embed = _whitelist_embed(
        "Luarmor Resynced",
        f"Resync completed for `{discord_id}`.",
        color=0x2ECC71,
    )
    if remote_user is None:
        embed.add_field(name="Remote State", value="No remote user remains after resync.", inline=False)
    else:
        embed.add_field(name="Remote Key", value=_mask_key(str(remote_user.get("user_key"))), inline=False)
        embed.add_field(
            name="Remote Status",
            value=str(remote_user.get("status") or "unknown").title(),
            inline=True,
        )
        embed.add_field(name="Banned", value="Yes" if remote_user.get("banned") else "No", inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="luarmoraudit", description="Audit local whitelist users against Luarmor")
@app_commands.guild_only()
@allowed_role_only()
async def luarmoraudit(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    audit = await whitelist_store.audit_luarmor()
    embed = _whitelist_embed("Luarmor Audit", color=THESEUS_BLUE)
    embed.add_field(name="Local Active Users", value=f"`{audit['local_users']}`", inline=True)
    embed.add_field(name="Remote Users", value=f"`{audit['remote_users']}`", inline=True)
    embed.add_field(name="Missing Remote", value=f"`{len(audit['missing_remote'])}`", inline=True)
    embed.add_field(name="Remote Only", value=f"`{len(audit['remote_only'])}`", inline=True)
    embed.add_field(name="Key Mismatches", value=f"`{len(audit['mismatched_keys'])}`", inline=True)
    embed.add_field(name="Ban Mismatches", value=f"`{len(audit['ban_mismatches'])}`", inline=True)

    preview_lines: list[str] = []
    if audit["missing_remote"]:
        preview_lines.append(
            "Missing remote: " + ", ".join(f"`{value}`" for value in audit["missing_remote"][:5])
        )
    if audit["mismatched_keys"]:
        preview_lines.append(
            "Key mismatch: " + ", ".join(f"`{value}`" for value in audit["mismatched_keys"][:5])
        )
    if audit["ban_mismatches"]:
        preview_lines.append(
            "Ban mismatch: " + ", ".join(f"`{value}`" for value in audit["ban_mismatches"][:5])
        )
    if audit["remote_only"]:
        preview_lines.append(
            "Remote only: " + ", ".join(f"`{value}`" for value in audit["remote_only"][:5])
        )
    if preview_lines:
        embed.add_field(name="Preview", value="\n".join(preview_lines), inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="genkey", description="Generate one or more ZyphraxHub Community keys")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(
    amount="Number of keys to generate (1-25)",
    time="Optional access duration like `1 minute`, `1w`, `1month`, `1year`, or `lifetime`",
)
async def genkey(
    interaction: discord.Interaction,
    amount: int = 1,
    time: Optional[str] = None,
) -> None:
    if amount < 1 or amount > 25:
        await interaction.response.send_message("Amount must be between 1 and 25.", ephemeral=True)
        return
    duration_seconds, error = _parse_duration_input(time)
    if error == "AMBIGUOUS_MINUTE_MONTH":
        await interaction.response.send_message(
            "You entered `1m`. Confirm whether you mean **1 minute** or **1 month**.",
            view=DurationAmbiguityView(count=amount),
            ephemeral=True,
        )
        return
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return
    await _send_generated_keys_response(
        interaction,
        count=amount,
        duration_seconds=duration_seconds,
    )


@bot.tree.command(name="masskey", description="Generate many ZyphraxHub Community keys at once")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(
    count="Number of keys to generate (1-50)",
    time="Optional access duration like `1 minute`, `1w`, `1month`, `1year`, or `lifetime`",
)
async def masskey(
    interaction: discord.Interaction,
    count: int = 5,
    time: Optional[str] = None,
) -> None:
    count = max(1, min(count, 50))
    duration_seconds, error = _parse_duration_input(time)
    if error == "AMBIGUOUS_MINUTE_MONTH":
        await interaction.response.send_message(
            "You entered `1m`. Confirm whether you mean **1 minute** or **1 month**.",
            view=DurationAmbiguityView(count=count),
            ephemeral=True,
        )
        return
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return
    await _send_generated_keys_response(
        interaction,
        count=count,
        duration_seconds=duration_seconds,
    )


@bot.tree.command(name="keylist", description="View ZyphraxHub Community key statistics")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(show_used="Include used keys in the export file")
async def keylist(interaction: discord.Interaction, show_used: bool = False) -> None:
    stats = await whitelist_store.get_stats()
    keys = await whitelist_store.get_all_keys(include_used=show_used)
    export_text = _build_key_export(keys)
    export_name = f"zyphrax_keys_{'all' if show_used else 'unused'}_{int(time.time())}.txt"
    panel_emojis: dict[str, discord.Emoji] = {}
    if interaction.guild is not None:
        panel_emojis = await _ensure_update_panel_emojis(interaction.guild, interaction.client)
    await interaction.response.send_message(
        view=ensure_layout_view_action_rows(
            KeylistPanelView(
                stats=stats,
                keys=keys,
                export_text=export_text,
                filename=export_name,
                panel_emojis=panel_emojis,
            )
        ),
        ephemeral=True,
    )


@bot.tree.command(name="delkey", description="Delete one unused ZyphraxHub Community key")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(key="The unused key to delete")
@app_commands.autocomplete(key=unused_key_autocomplete)
async def delkey(interaction: discord.Interaction, key: str) -> None:
    normalized_key = key.strip()
    if not normalized_key:
        await interaction.response.send_message("Provide a key to delete.", ephemeral=True)
        return

    deleted = await whitelist_store.delete_unused_key(normalized_key)
    if not deleted:
        await interaction.response.send_message(
            embed=_whitelist_embed(
                "Key Not Deleted",
                "That key was not found, or it is currently assigned to a user.",
                color=0xE74C3C,
            ),
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        embed=_whitelist_embed(
            "Key Deleted",
            f"Deleted unused key `{normalized_key}`.",
            color=0x2ECC71,
        ),
        ephemeral=True,
    )


@bot.tree.command(name="purgekeys", description="Delete all unused ZyphraxHub Community keys")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(confirm="Type CONFIRM to proceed")
async def purgekeys(interaction: discord.Interaction, confirm: str = "") -> None:
    if confirm != "CONFIRM":
        await interaction.response.send_message(
            embed=_whitelist_embed(
                "Confirmation Required",
                "This deletes every unused key. Re-run with `confirm: CONFIRM`.",
                color=0xF1C40F,
            ),
            ephemeral=True,
        )
        return

    deleted = await whitelist_store.purge_unused_keys()
    await interaction.response.send_message(
        embed=_whitelist_embed(
            "Unused Keys Deleted",
            f"Deleted **{deleted}** unused ZyphraxHub Community key(s).",
            color=0x2ECC71,
        ),
        ephemeral=True,
    )


support_group = app_commands.Group(
    name="support", description="Manage the supported Roblox games list"
)


@support_group.command(name="add", description="Add a Roblox game to the supported list")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.describe(game_link="A Roblox game link, e.g. https://www.roblox.com/games/1818")
async def support_add(interaction: discord.Interaction, game_link: str) -> None:
    place_id = _extract_roblox_place_id(game_link)
    if place_id is None:
        await interaction.response.send_message(
            "Send a valid Roblox game link so I can resolve the game name.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    game = await _resolve_roblox_game(place_id)
    if game is None:
        await interaction.followup.send(
            "I couldn't resolve that Roblox game. Check the link and try again.", ephemeral=True
        )
        return

    games = _load_supported_games()
    duplicate = next(
        (
            g for g in games
            if int(g["place_id"]) == int(game["place_id"])
            or str(g["name"]).lower() == str(game["name"]).lower()
        ),
        None,
    )
    if duplicate is not None:
        await interaction.followup.send(
            f"`{game['name']}` is already in the supported list.", ephemeral=True
        )
        return

    games.append(game)
    try:
        _save_supported_games(games)
    except OSError:
        LOGGER.exception("Failed to save supported games list")
        await interaction.followup.send(
            "I resolved the game, but I couldn't save the supported list.", ephemeral=True
        )
        return

    try:
        await _refresh_supported_games_channel(interaction.client, interaction.guild)
    except RuntimeError as exc:
        await interaction.followup.send(str(exc), ephemeral=True)
        return

    await interaction.followup.send(
        f"Added `{game['name']}` to the supported games list and refreshed <#{SUPPORTED_CHANNEL_ID}>.",
        ephemeral=True,
    )


@support_group.command(name="remove", description="Remove a supported Roblox game from the list")
@app_commands.guild_only()
@allowed_role_only()
@app_commands.autocomplete(game=supported_game_name_autocomplete)
@app_commands.describe(game="The supported game to remove")
async def support_remove(interaction: discord.Interaction, game: str) -> None:
    games = _load_supported_games()
    target_index = next(
        (i for i, g in enumerate(games) if str(g["name"]).lower() == game.strip().lower()),
        None,
    )
    if target_index is None:
        await interaction.response.send_message(
            "That game is not in the supported list.", ephemeral=True
        )
        return

    removed = games.pop(target_index)
    try:
        _save_supported_games(games)
    except OSError:
        LOGGER.exception("Failed to save supported games list")
        await interaction.response.send_message(
            "I removed the game in memory, but I couldn't save the updated list.", ephemeral=True
        )
        return

    try:
        await _refresh_supported_games_channel(interaction.client, interaction.guild)
    except RuntimeError as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return

    await interaction.response.send_message(
        f"Removed `{removed['name']}` from the supported games list and refreshed <#{SUPPORTED_CHANNEL_ID}>.",
        ephemeral=True,
    )


bot.tree.add_command(support_group)


@supported.error
@support_add.error
@support_remove.error
@paypanel.error
@panel.error
@redeem.error
@myinfo.error
@whitelist.error
@unwhitelist.error
@blacklist.error
@unblacklist.error
@resethwid.error
@lookup.error
@luarmorsync.error
@luarmoraudit.error
@genkey.error
@masskey.error
@keylist.error
@delkey.error
@purgekeys.error
@say.error
@announce.error
@update.error
async def command_error(
    interaction: discord.Interaction, error: app_commands.AppCommandError
) -> None:
    responder = (
        interaction.followup.send
        if interaction.response.is_done()
        else interaction.response.send_message
    )

    if isinstance(error, app_commands.CheckFailure):
        await responder(
            (
                f"You need the <@&{ALLOWED_ROLE_ID}> role or Administrator permission "
                "to use this command."
            ),
            ephemeral=True,
        )
        return

    original = (
        error.original if isinstance(error, app_commands.CommandInvokeError) else error
    )
    if isinstance(original, discord.Forbidden):
        await responder(
            "I do not have permission to send that message in this channel.", ephemeral=True
        )
        return

    if isinstance(original, discord.NotFound):
        LOGGER.warning("Interaction expired before the bot could respond.")
        return

    if isinstance(original, LuarmorSyncError):
        await responder(str(original), ephemeral=True)
        return

    LOGGER.exception("Unhandled application command error", exc_info=error)
    await responder(
        "That command failed unexpectedly. Check the bot logs if this keeps happening.",
        ephemeral=True,
    )


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError(
            "Set DISCORD_BOT_TOKEN in your shell, or create a .env file "
            "next to bot.py using .env.example."
        )
    bot.run(BOT_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
