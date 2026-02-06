#!/usr/bin/env python3
"""Unified webhook entry point that routes a single page to the correct sync."""

import argparse
import os
import sys

from dotenv import load_dotenv

import router
from shared.logging_config import get_logger, setup_logging
from shared.notion_api import NotionAPI
from shared.utils import get_notion_token

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Route a single Notion page to the correct sync target")
    parser.add_argument("--page-id", required=False, help="Notion page ID to sync (optional if --spotify-url is provided)")
    parser.add_argument("--force-icons", action="store_true", help="Force update page icons if supported")
    parser.add_argument("--force-all", action="store_true", help="Process page even if marked complete")
    parser.add_argument("--force-update", action="store_true", help="Movies/books targets: force update completed entries")
    parser.add_argument("--force-research", action="store_true", help="Books target: re-search even when IDs exist")
    parser.add_argument("--force-scraping", action="store_true", help="Books target: force ComicVine scraping")
    parser.add_argument("--dry-run", action="store_true", help="Books target: simulate sync without writing to Notion")
    parser.add_argument("--spotify-url", type=str, help="Music target: Spotify URL to create new page (track, album, or artist)")
    return parser


def main():
    load_dotenv()
    setup_logging(os.getenv("LOG_FILE", "notion_webhook.log"))
    logger = get_logger(__name__)

    parser = build_parser()
    args = parser.parse_args()

    # Validation: require either page_id or spotify_url
    if not args.page_id and not args.spotify_url:
        logger.error("Either --page-id or --spotify-url must be provided")
        sys.exit(1)

    notion_token = get_notion_token()
    if not notion_token:
        logger.error("NOTION_INTERNAL_INTEGRATION_SECRET (or NOTION_TOKEN) must be set")
        sys.exit(1)

    # Spotify URL-only mode: create new page from URL
    if args.spotify_url and not args.page_id:
        logger.info("Spotify URL creation mode: %s", args.spotify_url)
        target = router.get_target("music")
        if not target:
            logger.error("Music target not available")
            sys.exit(1)
        
        if not target.validate_environment():
            sys.exit(1)
        
        try:
            result = target.run_sync(spotify_url=args.spotify_url)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Spotify URL creation failed: %s", exc)
            raise
        
        if result.get("success"):
            logger.info("Successfully created page from Spotify URL")
            logger.info("Entity: %s | Page ID: %s | Created: %s",
                        result.get("entity_type", "unknown"),
                        result.get("page_id", "unknown"),
                        result.get("created", False))
            sys.exit(0)
        
        logger.error("Failed to create page from Spotify URL: %s", result.get("message", "Unknown error"))
        sys.exit(1)

    # Standard page-specific mode
    notion = NotionAPI(notion_token)
    page = notion.get_page(args.page_id)
    if not page:
        logger.error("Unable to retrieve Notion page %s", args.page_id)
        sys.exit(1)

    target = router.find_target_for_page(page)
    if not target:
        parent_db = page.get("parent", {}).get("database_id")
        logger.error("No registered sync target for database %s", parent_db)
        sys.exit(1)

    logger.info("Routing page %s to %s target", args.page_id, target.name)

    if not target.validate_environment():
        sys.exit(1)

    options = {
        "page_id": args.page_id,
        "force_icons": args.force_icons,
        "force_all": args.force_all,
        "force_update": args.force_update,
        "force_research": args.force_research,
        "force_scraping": args.force_scraping,
        "dry_run": args.dry_run,
        "spotify_url": args.spotify_url,
    }

    # Remove None values so adapters don't see extraneous kwargs
    filtered_options = {k: v for k, v in options.items() if v not in (None, False)}

    try:
        result = target.run_sync(**filtered_options)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Sync failed: %s", exc)
        raise

    if result.get("success"):
        logger.info("Synchronization completed successfully")
        logger.info("Updated: %s | Failed: %s | Skipped: %s",
                    result.get("successful_updates", 0),
                    result.get("failed_updates", 0),
                    result.get("skipped_updates", 0))
        sys.exit(0)

    logger.error("Synchronization failed")
    sys.exit(1)


if __name__ == "__main__":
    main()


