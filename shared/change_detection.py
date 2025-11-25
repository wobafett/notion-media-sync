from typing import Any, Dict, List, Optional, Tuple

PropertyMap = Dict[str, Any]
ChangeResult = Tuple[bool, List[str]]


def has_property_changes(
    page_properties: PropertyMap,
    new_properties: PropertyMap,
    last_updated_key: Optional[str] = None,
) -> ChangeResult:
    """Compare current page properties with impending updates."""
    differences: List[str] = []

    for prop_key, new_value in new_properties.items():
        if prop_key == last_updated_key:
            continue

        current_value = page_properties.get(prop_key)
        reason = _detect_difference(current_value, new_value)
        if reason:
            differences.append(f"{prop_key}: {reason}")

    return (bool(differences), differences)


def _detect_difference(current_value: Any, new_value: Any) -> Optional[str]:
    new_type, new_formatted = _extract_update_value(new_value)
    current_type, current_formatted = _extract_page_value(current_value)

    if new_type is None:
        return None

    if current_type is None:
        if new_formatted is None:
            return None
        return f"{new_type} added"

    if new_formatted != current_formatted:
        if current_formatted is None:
            return f"{new_type} added"
        if new_formatted is None:
            return f"{new_type} cleared"
        return f"{new_type} changed"

    return None


def _extract_update_value(value: Any) -> Tuple[Optional[str], Any]:
    if not isinstance(value, dict):
        return None, None

    if "rich_text" in value:
        return "rich_text", _rich_text_from_update(value.get("rich_text"))
    if "title" in value:
        return "title", _rich_text_from_update(value.get("title"))
    if "date" in value:
        date_payload = value.get("date") or {}
        return "date", date_payload.get("start")
    if "multi_select" in value:
        return "multi_select", _multi_select_names(value.get("multi_select"))
    if "status" in value:
        status_payload = value.get("status") or {}
        return "status", status_payload.get("name")
    if "url" in value:
        return "url", value.get("url")
    if "number" in value:
        return "number", value.get("number")
    if "checkbox" in value:
        return "checkbox", bool(value.get("checkbox"))

    return None, None


def _extract_page_value(value: Any) -> Tuple[Optional[str], Any]:
    if not isinstance(value, dict):
        return None, None

    value_type = value.get("type")

    if value_type == "rich_text" or "rich_text" in value:
        return "rich_text", _rich_text_from_page(value.get("rich_text"))
    if value_type == "title" or "title" in value:
        return "title", _rich_text_from_page(value.get("title"))
    if value_type == "date" or "date" in value:
        date_payload = value.get("date") or {}
        return "date", date_payload.get("start")
    if value_type == "multi_select" or "multi_select" in value:
        return "multi_select", _multi_select_names(value.get("multi_select"))
    if value_type == "status" or "status" in value:
        status_payload = value.get("status") or {}
        return "status", status_payload.get("name")
    if value_type == "url" or "url" in value:
        return "url", value.get("url")
    if value_type == "number" or "number" in value:
        return "number", value.get("number")
    if value_type == "checkbox" or "checkbox" in value:
        return "checkbox", bool(value.get("checkbox"))

    return None, None


def _rich_text_from_update(blocks: Any) -> Optional[str]:
    if not blocks:
        return None
    return "".join(block.get("text", {}).get("content", "") for block in blocks)


def _rich_text_from_page(blocks: Any) -> Optional[str]:
    if not blocks:
        return None
    parts: List[str] = []
    for block in blocks:
        if block.get("plain_text"):
            parts.append(block["plain_text"])
        elif block.get("text"):
            parts.append(block["text"].get("content", ""))
    return "".join(parts) if parts else None


def _multi_select_names(options: Any) -> Optional[Tuple[str, ...]]:
    if not options:
        return None
    names = [opt.get("name", "") for opt in options if opt.get("name")]
    if not names:
        return None
    return tuple(sorted(names))


