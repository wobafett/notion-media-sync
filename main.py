#!/usr/bin/env python3

import argparse
import os
import sys
from typing import List, Optional

from dotenv import load_dotenv

import router
from shared.logging_config import get_logger, setup_logging


def build_parser(targets) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified Notion media sync entry point"
    )
    parser.add_argument(
        "--target",
        choices=sorted(targets),
        help="Sync target to run (default: environment or first in list)",
    )
    parser.add_argument(
        "--force-icons",
        action="store_true",
        help="Force update all page icons (one-time operation)",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Process all pages including completed content",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        metavar="N",
        help="Number of parallel workers (default: 3, max recommended: 4)",
    )
    parser.add_argument(
        "--last-page",
        action="store_true",
        help="Sync only the most recently edited page",
    )
    parser.add_argument(
        "--page-id",
        type=str,
        help="Sync only the specified Notion page ID",
    )
    parser.add_argument(
        "--database",
        type=str,
        help="Target-specific database selector (music only; defaults to 'all')",
    )
    parser.add_argument(
        "--created-after",
        type=str,
        help="Filter pages created on/after YYYY-MM-DD (music only; accepts 'today')",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force update even if content is completed (movies target only)",
    )
    parser.add_argument(
        "--force-research",
        action="store_true",
        help="Books target: re-search even when IDs exist",
    )
    parser.add_argument(
        "--force-scraping",
        action="store_true",
        help="Books target: force ComicVine scraping even when IDs exist",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Books target: simulate sync without updating Notion",
    )
    parser.add_argument(
        "--spotify-url",
        type=str,
        help="Music target: Spotify URL for identification (track, album, or artist)",
    )
    return parser


def _resolve_target_name(
    args_target: Optional[str], default_target: Optional[str], available: List[str]
) -> str:
    if args_target:
        return args_target
    if default_target:
        return default_target
    env_target = os.getenv("SYNC_TARGET")
    if env_target:
        return env_target
    return available[0]


def main(default_target: Optional[str] = None):
    load_dotenv()
    setup_logging(os.getenv("LOG_FILE", "notion_sync.log"))
    logger = get_logger(__name__)

    targets = router.available_targets()
    parser = build_parser(targets)
    args = parser.parse_args()
    args_target = getattr(args, "target", None)

    if not args.page_id and not args_target and not default_target and not os.getenv("SYNC_TARGET"):
        logger.error("--target (or SYNC_TARGET env) is required when page-id is not provided")
        sys.exit(1)

    target_name = _resolve_target_name(args_target, default_target, targets)
    if target_name not in targets:
        logger.error("Unknown target '%s'. Valid options: %s", target_name, ", ".join(targets))
        sys.exit(1)

    target = router.get_target(target_name)

    if not target.validate_environment():
        sys.exit(1)

    try:
        run_options = {
            "force_icons": args.force_icons,
            "force_all": args.force_all,
            "workers": args.workers,
            "last_page": args.last_page,
            "page_id": args.page_id,
        }
        if args.database:
            run_options["database"] = args.database
        if args.created_after:
            run_options["created_after"] = args.created_after
        if getattr(args, "force_update", False):
            run_options["force_update"] = True
        if getattr(args, "force_research", False):
            run_options["force_research"] = True
        if getattr(args, "force_scraping", False):
            run_options["force_scraping"] = True
        if getattr(args, "dry_run", False):
            run_options["dry_run"] = True
        if getattr(args, "spotify_url", None):
            run_options["spotify_url"] = args.spotify_url

        result = target.run_sync(**run_options)
    except (RuntimeError, ValueError) as exc:
        logger.error(str(exc))
        sys.exit(1)

    if result.get("success"):
        logger.info(
            "Synchronization completed successfully | updated=%s failed=%s skipped=%s",
            result.get("successful_updates", 0),
            result.get("failed_updates", 0),
            result.get("skipped_updates", 0),
        )
        sys.exit(0)

    logger.error("Synchronization failed")
    sys.exit(1)


if __name__ == "__main__":
    main()


