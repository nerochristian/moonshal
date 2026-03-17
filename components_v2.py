from __future__ import annotations

from typing import Any, Optional

import discord


_V2_TOP_LEVEL_TYPES = {1, 9, 10, 12, 13, 14, 17}


def _component_type(item: discord.ui.Item[Any]) -> Optional[int]:
    try:
        data = item.to_component_dict()
        value = data.get("type")
        return int(value) if value is not None else None
    except Exception:
        return None


def ensure_layout_view_action_rows(view: discord.ui.LayoutView) -> discord.ui.LayoutView:
    children = list(getattr(view, "children", []))
    if not children:
        return view

    needs_fix = any((_component_type(child) not in _V2_TOP_LEVEL_TYPES) for child in children)
    if not needs_fix:
        return view

    action_rows: dict[int, list[discord.ui.Item[Any]]] = {}
    layout_items: list[discord.ui.Item[Any]] = []

    for child in children:
        component_type = _component_type(child)
        if component_type in _V2_TOP_LEVEL_TYPES:
            layout_items.append(child)
            continue

        row = getattr(child, "row", None)
        row_index = row if isinstance(row, int) and row >= 0 else 0
        action_rows.setdefault(row_index, []).append(child)

    view.clear_items()
    for item in layout_items:
        view.add_item(item)
    for row_index in sorted(action_rows):
        view.add_item(discord.ui.ActionRow(*action_rows[row_index]))

    try:
        children = list(getattr(view, "children", []))
        containers = [child for child in children if isinstance(child, discord.ui.Container)]
        action_row_items = [child for child in children if isinstance(child, discord.ui.ActionRow)]
        if containers and action_row_items:
            last_container = containers[-1]
            view.clear_items()
            for item in children:
                if not isinstance(item, discord.ui.ActionRow):
                    view.add_item(item)
            for row in action_row_items:
                last_container.add_item(row)
    except Exception:
        pass

    return view


def branded_panel_container(
    *,
    title: str,
    description: str,
    banner_url: Optional[str] = None,
    logo_url: Optional[str] = None,
    accent_color: Optional[int] = None,
    banner_separated: bool = False,
) -> discord.ui.Container:
    children: list[discord.ui.Item[Any]] = []

    title = (title or "").strip()
    description = (description or "").strip()
    header = "\n".join(part for part in [f"**{title}**" if title else "", description] if part).strip()

    has_banner = False
    if banner_url:
        normalized_banner = banner_url.strip()
        if normalized_banner:
            children.append(discord.ui.MediaGallery(discord.MediaGalleryItem(normalized_banner)))
            has_banner = True

    if has_banner and banner_separated and header:
        children.append(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))

    if header:
        normalized_logo = logo_url.strip() if logo_url else None
        if normalized_logo:
            children.append(
                discord.ui.Section(
                    discord.ui.TextDisplay(header),
                    accessory=discord.ui.Thumbnail(normalized_logo),
                )
            )
        else:
            children.append(discord.ui.TextDisplay(header))

    if accent_color is not None:
        return discord.ui.Container(*children, accent_color=accent_color)
    return discord.ui.Container(*children)
