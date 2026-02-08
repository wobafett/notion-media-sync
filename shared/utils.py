import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


def normalize_id(raw_id: Optional[str]) -> Optional[str]:
    """Normalize Notion IDs by stripping dashes and lowercasing."""
    if not raw_id:
        return None
    return raw_id.replace("-", "").lower()


def extract_page_id_from_url(page_id_or_url: str) -> Optional[str]:
    """
    Extract Notion page ID from a URL or return the ID if already in correct format.
    
    Supports:
    - Plain page IDs: "123abc456def..." or "123abc45-6def-7890-abcd-ef1234567890"
    - Web URLs: "https://www.notion.so/Page-Title-123abc456def..."
    - Share URLs: "https://notion.so/123abc456def...?pvs=4"
    
    Returns:
        32-character page ID without dashes, or None if invalid
    """
    if not page_id_or_url or not isinstance(page_id_or_url, str):
        return None
    
    # Strip whitespace
    input_str = page_id_or_url.strip()
    
    # If it's a URL, extract the path
    if input_str.startswith('http://') or input_str.startswith('https://'):
        # Parse URL to get path (remove query params)
        if '?' in input_str:
            input_str = input_str.split('?')[0]
        
        # Extract last segment of path
        # Format: https://www.notion.so/workspace/Page-Title-{id}
        # or: https://notion.so/{id}
        path_parts = input_str.rstrip('/').split('/')
        if path_parts:
            last_segment = path_parts[-1]
            
            # The ID is typically at the end, after the last dash
            # Format: "Page-Title-123abc456def..." or just "123abc456def..."
            if '-' in last_segment:
                # Split and take the last part which should be the ID
                potential_id = last_segment.split('-')[-1]
                # Check if it's long enough to be a page ID (32 chars)
                if len(potential_id) >= 32:
                    input_str = potential_id[:32]
                else:
                    # Maybe the entire segment is the ID with dashes
                    potential_id = last_segment.replace('-', '')
                    if len(potential_id) >= 32:
                        input_str = potential_id[:32]
            else:
                # No dashes, might be the raw ID
                input_str = last_segment
    
    # Remove all dashes and lowercase
    clean_id = input_str.replace('-', '').replace('_', '').lower()
    
    # Validate: should be 32 hex characters
    if len(clean_id) == 32 and all(c in '0123456789abcdef' for c in clean_id):
        return clean_id
    
    logger.warning(f"Could not extract valid page ID from: {page_id_or_url[:50]}...")
    return None


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


def detect_url_type(url: str) -> Optional[str]:
    """
    Detect URL type and return 'spotify' or 'google_books'.
    
    Args:
        url: URL to analyze
    
    Returns:
        'spotify' for Spotify URLs (track/album/artist)
        'google_books' for Google Books edition URLs
        None if URL is not recognized
    
    Example:
        >>> detect_url_type('https://open.spotify.com/track/abc123')
        'spotify'
        >>> detect_url_type('https://www.google.com/books/edition/Title/abc123')
        'google_books'
    """
    if not url:
        return None
    
    url = url.strip()
    
    if 'spotify.com/' in url and any(x in url for x in ['/track/', '/album/', '/artist/']):
        return 'spotify'
    elif 'google.com/books/edition/' in url:
        return 'google_books'
    
    return None


def parse_created_after_date(created_after: Optional[str]) -> Optional[str]:
    """
    Parse created_after input into ISO timestamp format for Notion API.
    
    Args:
        created_after: 'today' or 'YYYY-MM-DD' format
    
    Returns:
        ISO timestamp string (e.g., '2025-01-15T00:00:00Z') or None
    
    Raises:
        ValueError: If date format is invalid
    
    Example:
        >>> parse_created_after_date('today')
        '2026-02-08T00:00:00Z'
        >>> parse_created_after_date('2025-12-25')
        '2025-12-25T00:00:00Z'
    """
    if not created_after:
        return None
    
    input_str = created_after.strip()
    if input_str.lower() == 'today':
        today = datetime.now(timezone.utc).date()
        return f"{today.isoformat()}T00:00:00Z"
    else:
        try:
            parsed_date = datetime.strptime(input_str, '%Y-%m-%d').date()
            return f"{parsed_date.isoformat()}T00:00:00Z"
        except ValueError:
            raise ValueError("created_after must be in YYYY-MM-DD format or 'today'")


def build_created_after_filter(created_after: Optional[str]) -> Optional[Dict]:
    """
    Build Notion API filter params for created_after timestamp.
    
    Args:
        created_after: ISO timestamp from parse_created_after_date()
    
    Returns:
        Filter dict for notion.query_database() or None
    
    Example:
        >>> build_created_after_filter('2025-12-25T00:00:00Z')
        {'timestamp': 'created_time', 'created_time': {'on_or_after': '2025-12-25T00:00:00Z'}}
    """
    if not created_after:
        return None
    
    return {
        'timestamp': 'created_time',
        'created_time': {'on_or_after': created_after}
    }


