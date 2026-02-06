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


def find_page_by_property(
    notion_api,
    database_id: str,
    property_key: str,
    property_type: str,
    value: str,
) -> Optional[str]:
    """
    Find existing Notion page by property value.
    
    Args:
        notion_api: NotionAPI instance
        database_id: Database to search
        property_key: Property key to filter on
        property_type: Notion property type ('rich_text', 'url', 'number')
        value: Value to match
    
    Returns:
        Page ID if found, None otherwise
    
    Example:
        page_id = find_page_by_property(
            notion,
            database_id,
            'google_books_id',
            'rich_text',
            'ABC123'
        )
    """
    if not database_id or not value or not property_key:
        return None
    
    try:
        filter_params = {
            'property': property_key,
            property_type: {'equals': value}
        }
        existing_pages = notion_api.query_database(database_id, filter_params)
        if existing_pages:
            logger.debug(f"Found existing page by {property_type} property: {existing_pages[0]['id']}")
            return existing_pages[0]['id']
    except Exception as e:
        logger.debug(f"Error searching for page by {property_type}: {e}")
    
    return None


