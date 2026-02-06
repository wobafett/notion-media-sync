import logging
import os
from typing import Dict, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


def normalize_id(raw_id: Optional[str]) -> Optional[str]:
    """Normalize Notion IDs by stripping dashes and lowercasing."""
    if not raw_id:
        return None
    return raw_id.replace("-", "").lower()


def clean_multi_select_value(value: str) -> str:
    """Clean multi-select values to be compatible with Notion."""
    cleaned = (
        value.replace(",", "")
        .replace(";", "")
        .replace("\n", " ")
        .replace("\r", " ")
    )
    cleaned = " ".join(cleaned.split())
    if len(cleaned) > 100:
        cleaned = f"{cleaned[:97]}..."
    return cleaned


def build_multi_select_options(
    values: Iterable[str],
    *,
    limit: Optional[int] = None,
    context: str = "multi-select",
) -> List[Dict[str, str]]:
    """Return sanitized Notion multi-select option payloads."""
    options: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for raw_value in values:
        if raw_value is None:
            continue
        string_value = str(raw_value)
        cleaned_value = clean_multi_select_value(string_value)
        if not cleaned_value or cleaned_value in seen:
            continue
        if cleaned_value != string_value:
            logger.debug(
                "Sanitized %s value '%s' -> '%s'", context, string_value, cleaned_value
            )
        options.append({"name": cleaned_value})
        seen.add(cleaned_value)
        if limit and len(options) >= limit:
            break
    return options


def get_notion_token() -> Optional[str]:
    """Return the Notion token, preferring the internal secret if available."""
    return os.getenv("NOTION_INTERNAL_INTEGRATION_SECRET") or os.getenv("NOTION_TOKEN")


def get_database_id(*env_names: str) -> Optional[str]:
    """Return the first populated database ID across the provided env names."""
    candidates = env_names or ("NOTION_DATABASE_ID",)
    for name in candidates:
        value = os.getenv(name)
        if value:
            return value
    return None


