#!/usr/bin/env python3
"""
Sync Last Page Script for Notion IGDb Sync
This script syncs only the most recently edited page in your Notion database.
Useful for iOS shortcuts or quick updates.
"""

import os
import sys
from notion_igdb_sync import NotionIGDbSync, validate_environment
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    """Main function to sync only the last edited page."""
    try:
        print("🎮 Notion IGDb Sync - Last Page Mode")
        print("=" * 50)
        
        # Validate environment
        if not validate_environment():
            sys.exit(1)
        
        # Get environment variables
        notion_token = os.getenv('NOTION_TOKEN')
        igdb_client_id = os.getenv('IGDB_CLIENT_ID')
        igdb_client_secret = os.getenv('IGDB_CLIENT_SECRET')
        database_id = os.getenv('NOTION_DATABASE_ID')
        
        # Create sync instance with optimized settings for single page
        sync = NotionIGDbSync(notion_token, igdb_client_id, igdb_client_secret, database_id)
        
        # Optimize for single page processing - reduce delays
        sync.request_delay = 0.3  # Faster for single page
        sync.igdb.request_delay = 0.3
        sync.igdb.adaptive_delay = 0.3
        
        # Get the last edited page
        print("🔍 Finding the most recently edited page...")
        pages = sync.get_notion_pages()
        
        if not pages:
            print("❌ No pages found in database")
            sys.exit(1)
        
        # Sort by last_edited_time (most recent first)
        pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
        last_page = pages[0]
        
        print(f"📄 Found last edited page: {last_page.get('id')}")
        
        # Sync the single page
        result = sync.sync_page(last_page)
        
        if result is True:
            print("✅ Last page sync completed successfully")
        elif result is False:
            print("❌ Last page sync failed")
            sys.exit(1)
        else:
            print("⏭️ Last page sync skipped")
            
    except KeyboardInterrupt:
        print("\n⏹️ Sync interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
