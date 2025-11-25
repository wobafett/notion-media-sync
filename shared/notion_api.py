from typing import Dict, List, Optional, Union
import logging

from notion_client import Client

logger = logging.getLogger(__name__)


class NotionAPI:
    """Notion API client for database operations."""

    def __init__(self, token: str):
        self.client = Client(auth=token)

    def get_database(self, database_id: str) -> Optional[Dict]:
        """Get database information."""
        try:
            return self.client.databases.retrieve(database_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error retrieving database %s: %s", database_id, exc)
            return None

    def query_database(
        self, database_id: str, filter_params: Optional[Dict] = None
    ) -> List[Dict]:
        """Query database for pages."""
        try:
            pages: List[Dict] = []
            has_more = True
            start_cursor = None

            while has_more:
                params: Dict[str, Union[str, Dict]] = {}
                if start_cursor:
                    params["start_cursor"] = start_cursor
                if filter_params:
                    params["filter"] = filter_params

                response = self.client.databases.query(database_id, **params)
                pages.extend(response["results"])
                has_more = response["has_more"]
                start_cursor = response.get("next_cursor")

            return pages
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error querying database %s: %s", database_id, exc)
            return []

    def get_page(self, page_id: str) -> Optional[Dict]:
        """Get a single page by ID."""
        try:
            return self.client.pages.retrieve(page_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error retrieving page %s: %s", page_id, exc)
            return None

    def create_page(
        self,
        database_id: str,
        properties: Dict,
        cover_url: Optional[str] = None,
        icon: Optional[Union[str, Dict]] = None,
    ) -> Optional[str]:
        """Create a page inside a database."""
        try:
            page_data: Dict[str, Union[Dict, List, str]] = {
                "parent": {"database_id": database_id},
                "properties": properties,
            }

            if cover_url:
                page_data["cover"] = {
                    "type": "external",
                    "external": {"url": cover_url},
                }

            if icon:
                if isinstance(icon, str):
                    page_data["icon"] = {"type": "emoji", "emoji": icon}
                elif isinstance(icon, dict):
                    page_data["icon"] = icon

            page = self.client.pages.create(**page_data)
            return page["id"]
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error creating page in database %s: %s", database_id, exc)
            return None

    def update_page(
        self,
        page_id: str,
        properties: Dict,
        cover_url: Optional[str] = None,
        icon: Optional[Union[str, Dict]] = None,
    ) -> bool:
        """Update a page with new properties and optionally set the cover image and icon."""
        try:
            update_data: Dict[str, Union[Dict, List, str]] = {"properties": properties}

            if cover_url:
                update_data["cover"] = {
                    "type": "external",
                    "external": {"url": cover_url},
                }

            if icon:
                if isinstance(icon, str):
                    update_data["icon"] = {"type": "emoji", "emoji": icon}
                elif isinstance(icon, dict):
                    update_data["icon"] = icon

            self.client.pages.update(page_id, **update_data)
            return True
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error updating page %s: %s", page_id, exc)
            return False


