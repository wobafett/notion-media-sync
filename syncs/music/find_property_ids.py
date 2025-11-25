#!/usr/bin/env python3
"""
Find Property IDs Script for Notion MusicBrainz Sync
This script helps you find the property IDs for your Notion databases.
"""

import os
import sys
from notion_client import Client
from dotenv import load_dotenv

from shared.utils import get_notion_token

# Load environment variables
load_dotenv()

def find_property_ids(database_id: str, database_name: str):
    """Find and display property IDs for a Notion database."""
    
    notion_token = get_notion_token()
    
    if not notion_token:
        print("❌ NOTION_INTERNAL_INTEGRATION_SECRET (or NOTION_TOKEN) not found in environment variables")
        print("Please add NOTION_INTERNAL_INTEGRATION_SECRET to your .env file")
        return False
    
    if not database_id:
        print(f"❌ {database_name} database ID not found")
        return False
    
    try:
        # Initialize Notion client
        client = Client(auth=notion_token)
        
        # Get database information
        print(f"\n🔍 Fetching {database_name} database information...")
        database = client.databases.retrieve(database_id)
        
        print(f"\n📊 Database: {database.get('title', [{}])[0].get('plain_text', 'Untitled')}")
        print(f"🆔 Database ID: {database_id}")
        
        # Get properties
        properties = database.get('properties', {})
        
        print(f"\n📋 Found {len(properties)} properties:")
        print("=" * 80)
        
        # Display properties with their IDs
        for prop_key, prop_data in properties.items():
            prop_name = prop_data.get('name', 'Unnamed')
            prop_type = prop_data.get('type', 'unknown')
            prop_id = prop_data.get('id', 'No ID')
            
            print(f"Property: {prop_name}")
            print(f"  Type: {prop_type}")
            print(f"  Key: {prop_key}")
            print(f"  ID: {prop_id}")
            print("-" * 40)
        
        return True
        
    except Exception as e:
        print(f"❌ Error fetching {database_name}: {e}")
        return False

def main():
    """Main function."""
    print("🎵 Notion MusicBrainz Sync - Property ID Finder")
    print("=" * 50)
    
    # Get environment variables
    artists_db = os.getenv('NOTION_ARTISTS_DATABASE_ID')
    albums_db = os.getenv('NOTION_ALBUMS_DATABASE_ID')
    songs_db = os.getenv('NOTION_SONGS_DATABASE_ID')
    labels_db = os.getenv('NOTION_LABELS_DATABASE_ID')
    
    success = True
    
    if artists_db:
        if not find_property_ids(artists_db, "Artists"):
            success = False
    else:
        print("\n⚠️  NOTION_ARTISTS_DATABASE_ID not set, skipping Artists database")
    
    if albums_db:
        if not find_property_ids(albums_db, "Albums"):
            success = False
    else:
        print("\n⚠️  NOTION_ALBUMS_DATABASE_ID not set, skipping Albums database")
    
    if songs_db:
        if not find_property_ids(songs_db, "Songs"):
            success = False
    else:
        print("\n⚠️  NOTION_SONGS_DATABASE_ID not set, skipping Songs database")
    
    if labels_db:
        if not find_property_ids(labels_db, "Labels"):
            success = False
    else:
        print("\n⚠️  NOTION_LABELS_DATABASE_ID not set, skipping Labels database")
    
    if success:
        print("\n📝 Copy the property IDs above to your property_config.py file")
        print("💡 Property IDs are stable and won't change when you rename properties")
        print("\n✅ Property IDs found successfully!")
    else:
        print("\n❌ Some databases failed to load")
        sys.exit(1)

if __name__ == "__main__":
    main()

