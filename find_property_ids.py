#!/usr/bin/env python3
"""
Find Property IDs Script for Notion IGDb Sync
This script helps you find the property IDs for your Notion database.
"""

import os
import sys

from dotenv import load_dotenv
from notion_client import Client

from shared.utils import get_database_id, get_notion_token

# Load environment variables
load_dotenv()

def find_property_ids():
    """Find and display property IDs for the Notion database."""
    
    # Get environment variables
    notion_token = get_notion_token()
    database_id = get_database_id(
        'NOTION_GAMES_DATABASE_ID',
        'NOTION_MOVIETV_DATABASE_ID',
        'NOTION_BOOKS_DATABASE_ID',
        'NOTION_DATABASE_ID',
    )
    
    if not notion_token:
        print("❌ NOTION_INTERNAL_INTEGRATION_SECRET (or NOTION_TOKEN) not found in environment variables")
        print("Please add NOTION_INTERNAL_INTEGRATION_SECRET to your .env file")
        return False
    
    if not database_id:
        print("❌ Notion database ID not found in environment variables.")
        print("Please add NOTION_GAMES_DATABASE_ID, NOTION_MOVIETV_DATABASE_ID, or NOTION_DATABASE_ID to your .env file")
        return False
    
    try:
        # Initialize Notion client
        client = Client(auth=notion_token)
        
        # Get database information
        print(f"🔍 Fetching database information...")
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
        
        print("\n📝 Copy the property IDs above to your property_config.py file")
        print("💡 Property IDs are stable and won't change when you rename properties")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main function."""
    print("🎮 Notion IGDb Sync - Property ID Finder")
    print("=" * 50)
    
    if find_property_ids():
        print("\n✅ Property IDs found successfully!")
    else:
        print("\n❌ Failed to find property IDs")
        sys.exit(1)

if __name__ == "__main__":
    main()
