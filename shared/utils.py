import os
from typing import Optional


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


