#!/usr/bin/env python3
"""
Notion TMDb Sync Script
Synchronizes movie and TV show information from TMDb to Notion database pages.
"""

import os
import argparse
import logging
import sys
import time
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional, Union

import requests
from notion_client import Client

from shared.logging_config import get_logger, setup_logging
from shared.utils import build_multi_select_options, get_database_id, get_notion_token, normalize_id

setup_logging('notion_tmdb_sync.log')
logger = get_logger(__name__)

# Try to import custom property configuration
try:
    from syncs.movies.property_config import (
        TITLE_PROPERTY_ID, CONTENT_TYPE_PROPERTY_ID, DESCRIPTION_PROPERTY_ID,
        RELEASE_DATE_PROPERTY_ID, RATING_PROPERTY_ID, VOTE_COUNT_PROPERTY_ID,
        RUNTIME_PROPERTY_ID, SEASONS_PROPERTY_ID, GENRES_PROPERTY_ID,
        STATUS_PROPERTY_ID, TMDB_ID_PROPERTY_ID,
        LAST_UPDATED_PROPERTY_ID, EPISODES_PROPERTY_ID, WEBSITE_PROPERTY_ID,
        HOMEPAGE_PROPERTY_ID, CAST_PROPERTY_ID, DIRECTOR_PROPERTY_ID,
        CREATOR_PROPERTY_ID, PRODUCTION_COMPANIES_PROPERTY_ID, BUDGET_PROPERTY_ID, REVENUE_PROPERTY_ID,
        ORIGINAL_LANGUAGE_PROPERTY_ID, PRODUCTION_COUNTRIES_PROPERTY_ID,
        TAGLINE_PROPERTY_ID, POPULARITY_PROPERTY_ID, RUNTIME_MINUTES_PROPERTY_ID,
        ADULT_CONTENT_PROPERTY_ID, WATCH_PROVIDERS_PROPERTY_ID, RELEASED_EPISODES_PROPERTY_ID, NEXT_EPISODE_PROPERTY_ID,
        COLLECTION_PROPERTY_ID, FIELD_BEHAVIOR
    )
except ImportError:
    logger.error("property_config.py not found. Please create this file with your property IDs.")
    logger.error("Copy property_config.example.py to property_config.py and update with your property IDs.")
    sys.exit(1)

class TMDbAPI:
    """TMDb API client for fetching movie and TV show data."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        self.session.params = {'api_key': api_key}
    
    def search_movie(self, title: str) -> Optional[Dict]:
        """Search for a movie by title."""
        try:
            response = self.session.get(f"{self.base_url}/search/movie", params={
                'query': title,
                'language': 'en-US',
                'page': 1,
                'include_adult': False
            })
            response.raise_for_status()
            data = response.json()
            
            if data.get('results'):
                return data['results'][0]  # Return first (most relevant) result
            return None
        except Exception as e:
            logger.error(f"Error searching for movie '{title}': {e}")
            return None
    
    def search_tv(self, title: str) -> Optional[Dict]:
        """Search for a TV show by title."""
        try:
            response = self.session.get(f"{self.base_url}/search/tv", params={
                'query': title,
                'language': 'en-US',
                'page': 1,
                'include_adult': False
            })
            response.raise_for_status()
            data = response.json()
            
            if data.get('results'):
                return data['results'][0]  # Return first (most relevant) result
            return None
        except Exception as e:
            logger.error(f"Error searching for TV show '{title}': {e}")
            return None
    
    def get_movie_details(self, movie_id: int) -> Optional[Dict]:
        """Get detailed information for a movie."""
        try:
            response = self.session.get(f"{self.base_url}/movie/{movie_id}", params={
                'language': 'en-US',
                'append_to_response': 'credits,images,videos'
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting movie details for ID {movie_id}: {e}")
            return None
    
    def get_tv_details(self, tv_id: int) -> Optional[Dict]:
        """Get detailed information for a TV show."""
        try:
            response = self.session.get(f"{self.base_url}/tv/{tv_id}", params={
                'language': 'en-US',
                'append_to_response': 'credits,images,videos'
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting TV details for ID {tv_id}: {e}")
            return None
    
    def get_watch_providers(self, content_type: str, content_id: int) -> Optional[Dict]:
        """Get watch providers for a movie or TV show."""
        try:
            endpoint = f"{self.base_url}/{content_type}/{content_id}/watch/providers"
            response = self.session.get(endpoint)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting watch providers for {content_type} {content_id}: {e}")
            return None
    
    def normalize_provider_name(self, provider_name: str) -> str:
        """Normalize provider names to condense similar services using smart pattern matching."""
        if not provider_name:
            return provider_name
        
        # Start with the original name
        normalized = provider_name.strip()
        
        # Pattern 1: Handle "Plus" vs "+" variations
        # Convert "Plus" to "+" for consistency
        normalized = normalized.replace(' Plus', '+')
        normalized = normalized.replace('Plus ', '+')
        
        # Pattern 2: Remove channel suffixes (order matters - most specific first)
        channel_patterns = [
            ' Premium Amazon Channel',
            ' Premium Plus',
            ' Free with Ads',
            ' Standard with Ads',
            ' with Ads',
            ' Amazon Channel',
            ' Roku Premium Channel', 
            ' Apple TV Channel',
            ' Premium Channel',
            ' Channel',
            ' Premium',
            ' Standard',
            ' Free',
        ]
        
        for pattern in channel_patterns:
            if normalized.endswith(pattern):
                normalized = normalized[:-len(pattern)]
                break
        
        # Pattern 3: Handle specific service name variations
        service_mappings = {
            'Apple TV': 'Apple TV+',
            'Disney Plus': 'Disney+',
            'MGM Plus': 'MGM+',
            'AMC Plus': 'AMC+',
            'FXNow': 'FX',
            'Spectrum On Demand': 'Spectrum',
            'Shout! Factory Amazon Channel': 'Shout! Factory',
            'MUBI Amazon Channel': 'MUBI',
            'Cinemax Amazon Channel': 'Cinemax',
            'Cinemax Apple TV Channel': 'Cinemax',
            'Showtime Amazon Channel': 'Showtime',
            'Showtime Roku Premium Channel': 'Showtime',
            'Starz Amazon Channel': 'Starz',
            'Starz Roku Premium Channel': 'Starz',
        }
        
        # Apply specific service mappings
        normalized = service_mappings.get(normalized, normalized)
        
        # Pattern 4: Handle "with Showtime" and similar combinations
        if ' with Showtime' in normalized:
            normalized = normalized.replace(' with Showtime', '')
        
        return normalized
    

class NotionAPI:
    """Notion API client for database operations."""
    
    def __init__(self, token: str):
        self.client = Client(auth=token)
    
    def get_database(self, database_id: str) -> Optional[Dict]:
        """Get database information."""
        try:
            return self.client.databases.retrieve(database_id)
        except Exception as e:
            logger.error(f"Error retrieving database {database_id}: {e}")
            return None
    
    def query_database(self, database_id: str, filter_params: Optional[Dict] = None) -> List[Dict]:
        """Query database for pages."""
        try:
            pages = []
            has_more = True
            start_cursor = None
            
            while has_more:
                params = {}
                if start_cursor:
                    params['start_cursor'] = start_cursor
                if filter_params:
                    params['filter'] = filter_params
                
                response = self.client.databases.query(database_id, **params)
                pages.extend(response['results'])
                has_more = response['has_more']
                start_cursor = response.get('next_cursor')
            
            return pages
        except Exception as e:
            logger.error(f"Error querying database {database_id}: {e}")
            return []
    
    def get_page(self, page_id: str) -> Optional[Dict]:
        """Get a single page by ID."""
        try:
            return self.client.pages.retrieve(page_id)
        except Exception as e:
            logger.error(f"Error retrieving page {page_id}: {e}")
            return None
    
    def update_page(self, page_id: str, properties: Dict, cover_url: Optional[str] = None, icon: Optional[Union[str, Dict]] = None) -> bool:
        """Update a page with new properties and optionally set the cover image and icon."""
        try:
            update_data = {'properties': properties}
            
            # Set cover image if provided
            if cover_url:
                update_data['cover'] = {
                    'type': 'external',
                    'external': {
                        'url': cover_url
                    }
                }
            
            # Set page icon if provided
            if icon:
                if isinstance(icon, str):
                    # Emoji icon
                    update_data['icon'] = {
                        'type': 'emoji',
                        'emoji': icon
                    }
                elif isinstance(icon, dict):
                    # External image icon
                    update_data['icon'] = icon
            
            self.client.pages.update(page_id, **update_data)
            return True
        except Exception as e:
            logger.error(f"Error updating page {page_id}: {e}")
            return False

class NotionTMDbSync:
    """Main class for synchronizing Notion database with TMDb data."""
    
    def __init__(self, notion_token: str, tmdb_api_key: str, database_id: str):
        self.notion = NotionAPI(notion_token)
        self.tmdb = TMDbAPI(tmdb_api_key)
        self.database_id = database_id
        
        # Rate limiting
        self.request_delay = 0.25  # 250ms between requests to respect API limits
        
        # Property mapping - will be populated from database schema
        self.property_mapping = {}
        
        # Field behavior configuration
        self.field_behavior = FIELD_BEHAVIOR
        
        self._load_database_schema()
        self._type_cache: Dict[str, str] = {}
    
    def _load_database_schema(self):
        """Load and analyze the database schema to create property mappings."""
        try:
            database = self.notion.get_database(self.database_id)
            if not database:
                logger.error("Could not retrieve database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create a mapping from property IDs to property keys
            self.property_id_to_key = {}
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.property_id_to_key[prop_id] = prop_key
            
            # Create mappings for different property types using property IDs
            self.property_mapping = {
                # Required properties
                'title_property_id': TITLE_PROPERTY_ID,
                'content_type_property_id': CONTENT_TYPE_PROPERTY_ID,
                
                # Core properties
                'description_property_id': DESCRIPTION_PROPERTY_ID,
                'release_date_property_id': RELEASE_DATE_PROPERTY_ID,
                'rating_property_id': RATING_PROPERTY_ID,
                'vote_count_property_id': VOTE_COUNT_PROPERTY_ID,
                'runtime_property_id': RUNTIME_PROPERTY_ID,
                'seasons_property_id': SEASONS_PROPERTY_ID,
                'genres_property_id': GENRES_PROPERTY_ID,
                'status_property_id': STATUS_PROPERTY_ID,
                'tmdb_id_property_id': TMDB_ID_PROPERTY_ID,
                'last_updated_property_id': LAST_UPDATED_PROPERTY_ID,
                
                # Extended properties
                'episodes_property_id': EPISODES_PROPERTY_ID,
                'website_property_id': WEBSITE_PROPERTY_ID,
                'homepage_property_id': HOMEPAGE_PROPERTY_ID,
                'cast_property_id': CAST_PROPERTY_ID,
                'director_property_id': DIRECTOR_PROPERTY_ID,
                'creator_property_id': CREATOR_PROPERTY_ID,
                'production_companies_property_id': PRODUCTION_COMPANIES_PROPERTY_ID,
                'budget_property_id': BUDGET_PROPERTY_ID,
                'revenue_property_id': REVENUE_PROPERTY_ID,
                'original_language_property_id': ORIGINAL_LANGUAGE_PROPERTY_ID,
                'production_countries_property_id': PRODUCTION_COUNTRIES_PROPERTY_ID,
                'tagline_property_id': TAGLINE_PROPERTY_ID,
                'popularity_property_id': POPULARITY_PROPERTY_ID,
                'runtime_minutes_property_id': RUNTIME_MINUTES_PROPERTY_ID,
                'adult_content_property_id': ADULT_CONTENT_PROPERTY_ID,
                'watch_providers_property_id': WATCH_PROVIDERS_PROPERTY_ID,
                'released_episodes_property_id': RELEASED_EPISODES_PROPERTY_ID,
                'next_episode_property_id': NEXT_EPISODE_PROPERTY_ID,
                'collection_property_id': COLLECTION_PROPERTY_ID,
            }
            
            # Log the property mapping (no validation needed - trust the config)
            for prop_key, prop_id in self.property_mapping.items():
                if prop_id is not None:
                    property_key = self.property_id_to_key.get(prop_id, "NOT_FOUND")
                    logger.info(f"✓ {prop_key}: {prop_id} -> {property_key}")
                else:
                    logger.info(f"⏭️  {prop_key}: NOT CONFIGURED (skipped)")
            
            # Validate required properties
            if not self.property_mapping['title_property_id']:
                logger.error("❌ No title property found - this is required!")
            if not self.property_mapping['content_type_property_id']:
                logger.error("❌ No content type property found - this is required!")
                
        except Exception as e:
            logger.error(f"Error loading database schema: {e}")
            self.property_mapping = {}
    
    def get_notion_pages(self) -> List[Dict]:
        """Get all pages from the Notion database."""
        logger.info(f"Fetching pages from database {self.database_id}")
        return self.notion.query_database(self.database_id)
    
    def extract_current_data(self, page: Dict) -> Dict:
        """Extract current data from a Notion page for comparison."""
        try:
            if not page:
                logger.warning("Page is None or empty")
                return {}
                
            properties = page.get('properties', {})
            current_data = {}
            
            # Extract all configured properties
            for prop_key, prop_id in self.property_mapping.items():
                if prop_id is None:
                    continue
                    
                property_key = self._get_property_key(prop_id)
                if not property_key:
                    continue
                    
                prop_data = properties.get(property_key)
                if not prop_data or prop_data is None:
                    continue
                
                # Extract value based on property type
                prop_type = prop_data.get('type')
                if not prop_type:
                    continue
                    
                if prop_type == 'title' and prop_data.get('title') and len(prop_data['title']) > 0:
                    current_data[prop_key] = prop_data['title'][0]['plain_text']
                elif prop_type == 'rich_text' and prop_data.get('rich_text') and len(prop_data['rich_text']) > 0:
                    current_data[prop_key] = prop_data['rich_text'][0]['plain_text']
                elif prop_type == 'number' and prop_data.get('number') is not None:
                    current_data[prop_key] = prop_data['number']
                elif prop_type == 'date' and prop_data.get('date') and prop_data['date'].get('start'):
                    current_data[prop_key] = prop_data['date']['start']
                elif prop_type == 'select' and prop_data.get('select') and prop_data['select'].get('name'):
                    current_data[prop_key] = prop_data['select']['name']
                elif prop_type == 'multi_select':
                    multi_select_data = prop_data.get('multi_select', [])
                    current_data[prop_key] = [item['name'] for item in multi_select_data if item and item.get('name')]
                elif prop_type == 'url' and prop_data.get('url'):
                    current_data[prop_key] = prop_data['url']
                elif prop_type == 'checkbox' and prop_data.get('checkbox') is not None:
                    current_data[prop_key] = prop_data['checkbox']
                elif prop_type == 'status' and prop_data.get('status') and prop_data['status'].get('name'):
                    current_data[prop_key] = prop_data['status']['name']
            
            # Extract current cover URL
            cover = page.get('cover')
            if cover and cover.get('type') == 'external':
                current_data['_cover_url'] = cover['external']['url']
            
            return current_data
            
        except Exception as e:
            logger.error(f"Error extracting current data from page {page.get('id')}: {e}")
            return {}
    
    def extract_title_and_type(self, page: Dict, current_data: Dict) -> Optional[tuple]:
        """Extract title and content type from a Notion page. Infer content type if missing."""
        try:
            properties = page.get('properties', {})
            page_id = page.get('id')
            
            # Get title using mapped property ID
            title = None
            if self.property_mapping['title_property_id']:
                property_key = self.property_id_to_key.get(self.property_mapping['title_property_id'])
                if property_key:
                    title_prop = properties.get(property_key)
                    if title_prop and title_prop.get('type') == 'title' and title_prop.get('title'):
                        title = title_prop['title'][0]['plain_text']
            
            # Get content type using mapped property ID
            content_type = None
            if self.property_mapping['content_type_property_id']:
                property_key = self.property_id_to_key.get(self.property_mapping['content_type_property_id'])
                if property_key:
                    type_prop = properties.get(property_key)
                    if type_prop and type_prop.get('type') == 'select' and type_prop.get('select'):
                        content_type = type_prop['select']['name'].lower()
            
            if title and content_type:
                return (title, content_type)
            
            inferred_type = self._infer_content_type(page_id, title, current_data)
            if title and inferred_type:
                logger.info("Inferred content type '%s' for page %s", inferred_type, page_id)
                return (title, inferred_type)
            
            logger.warning(
                "Missing title or content type for page %s (title=%s, inferred_type=%s)",
                page_id,
                "present" if title else "missing",
                inferred_type or "unavailable",
            )
            return None
                
        except Exception as e:
            logger.error(f"Error extracting data from page {page.get('id')}: {e}")
            return None

    def _infer_content_type(self, page_id: Optional[str], title: Optional[str], current_data: Dict) -> Optional[str]:
        """Infer whether an entry is a movie or TV show when the select property is missing."""
        if not title:
            return None

        cached = self._type_cache.get(page_id)
        if cached:
            return cached

        tmdb_id = current_data.get('tmdb_id_property_id')
        if tmdb_id:
            details = self.tmdb.get_movie_details(tmdb_id)
            if details:
                self._type_cache[page_id] = 'movie'
                return 'movie'
            details = self.tmdb.get_tv_details(tmdb_id)
            if details:
                self._type_cache[page_id] = 'tv'
                return 'tv'

        movie_search = self.tmdb.search_movie(title)
        if movie_search:
            self._type_cache[page_id] = 'movie'
            return 'movie'

        tv_search = self.tmdb.search_tv(title)
        if tv_search:
            self._type_cache[page_id] = 'tv'
            return 'tv'

        return None

    def _normalize_content_type_value(self, content_type: Optional[str]) -> Optional[str]:
        """Convert internal content type strings to the exact Notion select values."""
        if not content_type:
            return None
        lowered = content_type.lower()
        if lowered == 'movie':
            return 'Movie'
        if lowered == 'tv':
            return 'TV'
        return content_type.title()

    def _ensure_content_type_property(self, page_id: str, content_type: str) -> None:
        """Backfill the Type select when we inferred the content type."""
        select_value = self._normalize_content_type_value(content_type)
        if not select_value:
            return

        property_id = self.property_mapping.get('content_type_property_id')
        if not property_id:
            return

        property_key = self._get_property_key(property_id)
        if not property_key:
            return

        properties = {
            property_key: {
                'select': {'name': select_value}
            }
        }

        if self.notion.update_page(page_id, properties):
            logger.info("Backfilled content type '%s' for page %s", select_value, page_id)
        else:
            logger.warning("Failed to backfill content type for page %s", page_id)
    
    def compare_and_format_properties(self, current_data: Dict, tmdb_data: Dict, content_type: str) -> tuple[Dict, bool]:
        """Compare current data with TMDb data and return only changed properties."""
        try:
            new_properties = {}
            has_changes = False
            
            # Helper function to compare values
            def values_differ(current_val, new_val):
                if current_val is None and new_val is not None:
                    return True
                if current_val is not None and new_val is None:
                    return True
                if current_val != new_val:
                    return True
                return False
            
            # Helper function to compare lists (for multi-select)
            def lists_differ(current_list, new_list):
                if not current_list and not new_list:
                    return False
                if not current_list or not new_list:
                    return True
                return set(current_list) != set(new_list)
            
            # Title
            if tmdb_data.get('title') or tmdb_data.get('name'):
                new_title = tmdb_data.get('title') or tmdb_data.get('name')
                current_title = current_data.get('title_property_id')
                if values_differ(current_title, new_title) and self.property_mapping['title_property_id']:
                    property_key = self._get_property_key(self.property_mapping['title_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'title': [{'text': {'content': new_title}}]
                        }
                        has_changes = True
            
            # Description
            if tmdb_data.get('overview') and self.property_mapping['description_property_id']:
                current_desc = current_data.get('description_property_id')
                if values_differ(current_desc, tmdb_data['overview']):
                    property_key = self._get_property_key(self.property_mapping['description_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'rich_text': [{'text': {'content': tmdb_data['overview']}}]
                        }
                        has_changes = True
            
            # Release Date
            if (tmdb_data.get('release_date') or tmdb_data.get('first_air_date')) and self.property_mapping['release_date_property_id']:
                new_date = tmdb_data.get('release_date') or tmdb_data.get('first_air_date')
                current_date = current_data.get('release_date_property_id')
                if values_differ(current_date, new_date):
                    property_key = self._get_property_key(self.property_mapping['release_date_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'date': {'start': new_date}
                        }
                        has_changes = True
            
            # Genres
            if tmdb_data.get('genres') and self.property_mapping['genres_property_id']:
                behavior = self.field_behavior.get('genres_property_id', 'default')
                tmdb_genres = [genre['name'] for genre in tmdb_data['genres']]
                current_genres = current_data.get('genres_property_id', [])
                
                result_genres = self._handle_field_behavior(tmdb_genres, current_genres, 'genres', behavior)
                
                if result_genres is not None and lists_differ(current_genres, result_genres):
                    property_key = self._get_property_key(self.property_mapping['genres_property_id'])
                    if property_key:
                        genre_options = build_multi_select_options(result_genres, context='genres')
                        new_properties[property_key] = {'multi_select': genre_options}
                        has_changes = True
            
            # Status
            if tmdb_data.get('status') and self.property_mapping['status_property_id']:
                current_status = current_data.get('status_property_id')
                if values_differ(current_status, tmdb_data['status']):
                    property_key = self._get_property_key(self.property_mapping['status_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'status': {'name': tmdb_data['status']}
                        }
                        has_changes = True

            # Rating (vote average)
            if tmdb_data.get('vote_average') is not None and self.property_mapping['rating_property_id']:
                current_rating = current_data.get('rating_property_id')
                new_rating = tmdb_data['vote_average']
                if values_differ(current_rating, new_rating):
                    property_key = self._get_property_key(self.property_mapping['rating_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': new_rating
                        }
                        has_changes = True
            
            # TMDb ID
            if tmdb_data.get('id') and self.property_mapping['tmdb_id_property_id']:
                current_tmdb_id = current_data.get('tmdb_id_property_id')
                if values_differ(current_tmdb_id, tmdb_data['id']):
                    property_key = self._get_property_key(self.property_mapping['tmdb_id_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': tmdb_data['id']
                        }
                        has_changes = True
            
            # Runtime/Seasons
            if content_type == 'movie' and tmdb_data.get('runtime') and self.property_mapping['runtime_minutes_property_id']:
                current_runtime = current_data.get('runtime_minutes_property_id')
                if values_differ(current_runtime, tmdb_data['runtime']):
                    property_key = self._get_property_key(self.property_mapping['runtime_minutes_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': tmdb_data['runtime']
                        }
                        has_changes = True
            elif content_type == 'tv' and tmdb_data.get('number_of_seasons') and self.property_mapping['seasons_property_id']:
                current_seasons = current_data.get('seasons_property_id')
                if values_differ(current_seasons, tmdb_data['number_of_seasons']):
                    property_key = self._get_property_key(self.property_mapping['seasons_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': tmdb_data['number_of_seasons']
                        }
                        has_changes = True

            # Content Type select (backfill when inferred)
            select_value = self._normalize_content_type_value(content_type)
            if select_value and self.property_mapping['content_type_property_id']:
                current_type = current_data.get('content_type_property_id')
                if values_differ(current_type, select_value):
                    property_key = self._get_property_key(self.property_mapping['content_type_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'select': {'name': select_value}
                        }
                        has_changes = True
            
            # Extended properties
            # Check if this is an initial sync (no TMDb ID) or subsequent sync
            current_tmdb_id = current_data.get('tmdb_id_property_id')
            
            if current_tmdb_id is None:
                # Initial sync - format all extended properties
                logger.info("Initial sync detected - formatting all extended properties")
                self._format_extended_properties(tmdb_data, content_type, new_properties)
                has_changes = True  # Always has changes on initial sync
            else:
                # Subsequent sync - only compare changed properties
                has_changes = self._compare_extended_properties(current_data, tmdb_data, content_type, new_properties, has_changes)
            
            return new_properties, has_changes
            
        except Exception as e:
            logger.error(f"Error comparing properties: {e}")
            return {}, False
    
    def _compare_extended_properties(self, current_data: Dict, tmdb_data: Dict, content_type: str, new_properties: Dict, has_changes: bool) -> bool:
        """Compare extended properties and add changes to new_properties."""
        try:
            # Episodes (TV shows) - Total planned episodes (including future seasons)
            if content_type == 'tv' and tmdb_data.get('number_of_episodes') and self.property_mapping['episodes_property_id']:
                current_episodes = current_data.get('episodes_property_id')
                if current_episodes != tmdb_data['number_of_episodes']:
                    property_key = self._get_property_key(self.property_mapping['episodes_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': tmdb_data['number_of_episodes']
                        }
                        has_changes = True
            
            # Released Episodes (TV shows) - Last episode number that has aired
            if content_type == 'tv' and self.property_mapping['released_episodes_property_id']:
                released_episodes = None
                
                # Use last_episode_to_air to get the actual last aired episode
                last_episode = tmdb_data.get('last_episode_to_air')
                if last_episode and last_episode.get('episode_number') and last_episode.get('season_number'):
                    # Calculate cumulative episode number across all seasons
                    seasons = tmdb_data.get('seasons', [])
                    if seasons:
                        cumulative_episodes = 0
                        last_season = last_episode.get('season_number', 0)
                        
                        # Sum episodes from all seasons up to and including the last aired season
                        for season in seasons:
                            season_number = season.get('season_number', 0)
                            if season_number > 0 and season_number < last_season:
                                # For completed seasons, count all episodes
                                if season.get('air_date'):
                                    cumulative_episodes += season.get('episode_count', 0)
                            elif season_number == last_season:
                                # For the last aired season, only count up to the last aired episode
                                cumulative_episodes += last_episode.get('episode_number', 0)
                                break
                        
                        if cumulative_episodes > 0:
                            released_episodes = cumulative_episodes
                
                # Update if we found released episodes data
                if released_episodes is not None:
                    current_released_episodes = current_data.get('released_episodes_property_id')
                    if current_released_episodes != released_episodes:
                        property_key = self._get_property_key(self.property_mapping['released_episodes_property_id'])
                        if property_key:
                            new_properties[property_key] = {
                                'number': released_episodes
                            }
                            has_changes = True
            
            # Next Episode Air Date (TV shows)
            if content_type == 'tv' and self.property_mapping['next_episode_property_id']:
                next_episode = tmdb_data.get('next_episode_to_air')
                next_air_date = None
                
                if next_episode and next_episode.get('air_date'):
                    next_air_date = next_episode['air_date']
                
                # Update if we found next episode air date data
                if next_air_date is not None:
                    current_next_episode = current_data.get('next_episode_property_id')
                    if current_next_episode != next_air_date:
                        property_key = self._get_property_key(self.property_mapping['next_episode_property_id'])
                        if property_key:
                            new_properties[property_key] = {
                                'date': {'start': next_air_date}
                            }
                            has_changes = True
                elif current_data.get('next_episode_property_id') is not None:
                    # Clear the field if no next episode data is available
                    property_key = self._get_property_key(self.property_mapping['next_episode_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'date': None
                        }
                        has_changes = True
            
            # Cast
            if tmdb_data.get('credits', {}).get('cast') and self.property_mapping['cast_property_id']:
                behavior = self.field_behavior.get('cast_property_id', 'default')
                tmdb_cast = [person['name'] for person in tmdb_data['credits']['cast'][:5]]
                current_cast = current_data.get('cast_property_id', [])
                
                result_cast = self._handle_field_behavior(tmdb_cast, current_cast, 'cast', behavior)
                
                if result_cast is not None and set(current_cast) != set(result_cast):
                    property_key = self._get_property_key(self.property_mapping['cast_property_id'])
                    if property_key:
                        cast_options = build_multi_select_options(result_cast, context='cast')
                        new_properties[property_key] = {'multi_select': cast_options}
                        has_changes = True
            
            # Director(s) (Movies only - TV shows use creators)
            if content_type == 'movie' and tmdb_data.get('credits', {}).get('crew') and self.property_mapping['director_property_id']:
                behavior = self.field_behavior.get('director_property_id', 'default')
                tmdb_directors = [person['name'] for person in tmdb_data['credits']['crew'] if person['job'] == 'Director'][:3]
                current_directors = current_data.get('director_property_id', [])
                
                result_directors = self._handle_field_behavior(tmdb_directors, current_directors, 'directors', behavior)
                
                if result_directors is not None and set(current_directors) != set(result_directors):
                    property_key = self._get_property_key(self.property_mapping['director_property_id'])
                    if property_key:
                        director_options = build_multi_select_options(result_directors, context='directors')
                        new_properties[property_key] = {'multi_select': director_options}
                        has_changes = True
            
            # Creator(s) (TV shows) - Show creators/showrunners
            if content_type == 'tv' and self.property_mapping['creator_property_id']:
                behavior = self.field_behavior.get('creator_property_id', 'default')
                tmdb_creators = []
                if tmdb_data.get('created_by'):
                    tmdb_creators = [creator['name'] for creator in tmdb_data['created_by'][:5]]
                
                current_creators = current_data.get('creator_property_id', [])
                result_creators = self._handle_field_behavior(tmdb_creators, current_creators, 'creators', behavior)
                
                if result_creators is not None and set(current_creators) != set(result_creators):
                    property_key = self._get_property_key(self.property_mapping['creator_property_id'])
                    if property_key:
                        creator_options = build_multi_select_options(result_creators, context='creators')
                        new_properties[property_key] = {'multi_select': creator_options}
                        has_changes = True
            
            # Production Companies
            if tmdb_data.get('production_companies') and self.property_mapping['production_companies_property_id']:
                behavior = self.field_behavior.get('production_companies_property_id', 'default')
                tmdb_companies = [company['name'] for company in tmdb_data['production_companies'][:5]]
                current_companies = current_data.get('production_companies_property_id', [])
                
                result_companies = self._handle_field_behavior(tmdb_companies, current_companies, 'production companies', behavior)
                
                if result_companies is not None and set(current_companies) != set(result_companies):
                    property_key = self._get_property_key(self.property_mapping['production_companies_property_id'])
                    if property_key:
                        company_options = build_multi_select_options(result_companies, context='production_companies')
                        new_properties[property_key] = {'multi_select': company_options}
                        has_changes = True
            
            # TMDb Homepage
            if tmdb_data.get('id') and self.property_mapping['homepage_property_id']:
                new_homepage = f"https://www.themoviedb.org/{content_type}/{tmdb_data['id']}"
                current_homepage = current_data.get('homepage_property_id')
                if current_homepage != new_homepage:
                    property_key = self._get_property_key(self.property_mapping['homepage_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'url': new_homepage
                        }
                        has_changes = True
            
            # Tagline
            if tmdb_data.get('tagline') and self.property_mapping['tagline_property_id']:
                current_tagline = current_data.get('tagline_property_id')
                if current_tagline != tmdb_data['tagline']:
                    property_key = self._get_property_key(self.property_mapping['tagline_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'rich_text': [{'text': {'content': tmdb_data['tagline']}}]
                        }
                        has_changes = True
            
            # Watch Providers (using TMDb)
            if self.property_mapping['watch_providers_property_id'] and tmdb_data.get('id'):
                # Get watch providers from TMDb
                watch_providers_data = self.tmdb.get_watch_providers(content_type, tmdb_data['id'])
                if watch_providers_data:
                    us_providers = watch_providers_data.get('results', {}).get('US', {})
                    new_providers = []
                    
                    # Extract providers from flatrate, free, and ads categories
                    for category in ['flatrate', 'free', 'ads']:
                        providers = us_providers.get(category, [])
                        for provider in providers:
                            provider_name = provider.get('provider_name', 'Unknown')
                            # Normalize provider name to condense similar services
                            normalized_name = self.tmdb.normalize_provider_name(provider_name)
                            new_providers.append(normalized_name)
                    
                    # Remove duplicates and limit to top 10
                    new_providers = list(dict.fromkeys(new_providers))[:10]
                    current_providers = current_data.get('watch_providers_property_id', [])
                    
                    if set(current_providers) != set(new_providers):
                        property_key = self._get_property_key(self.property_mapping['watch_providers_property_id'])
                        if property_key:
                            provider_options = build_multi_select_options(new_providers, context='watch_providers')
                            new_properties[property_key] = {'multi_select': provider_options}
                            has_changes = True
            
            # Original Language
            if tmdb_data.get('original_language') and self.property_mapping['original_language_property_id']:
                current_language = current_data.get('original_language_property_id')
                tmdb_language = tmdb_data['original_language'].upper()
                
                if current_language != tmdb_language:
                    property_key = self._get_property_key(self.property_mapping['original_language_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'select': {'name': tmdb_language}
                        }
                        has_changes = True
            
            # Production Countries
            if tmdb_data.get('production_countries') and self.property_mapping['production_countries_property_id']:
                tmdb_countries = [country['name'] for country in tmdb_data['production_countries'][:5]]  # Top 5 countries
                current_countries = current_data.get('production_countries_property_id', [])
                
                if set(current_countries) != set(tmdb_countries):
                    property_key = self._get_property_key(self.property_mapping['production_countries_property_id'])
                    if property_key:
                        country_options = build_multi_select_options(tmdb_countries, context='production_countries')
                        new_properties[property_key] = {'multi_select': country_options}
                        has_changes = True
            
            # Collection (Movies only)
            if content_type == 'movie' and tmdb_data.get('belongs_to_collection') and self.property_mapping['collection_property_id']:
                behavior = self.field_behavior.get('collection_property_id', 'default')
                collection = tmdb_data['belongs_to_collection']
                tmdb_collections = [collection['name']] if collection and collection.get('name') else []
                current_collections = current_data.get('collection_property_id', [])
                
                result_collections = self._handle_field_behavior(tmdb_collections, current_collections, 'collection', behavior)
                
                if result_collections is not None and set(current_collections) != set(result_collections):
                    property_key = self._get_property_key(self.property_mapping['collection_property_id'])
                    if property_key:
                        collection_options = build_multi_select_options(result_collections, context='collection')
                        new_properties[property_key] = {'multi_select': collection_options}
                        has_changes = True
            
            # Rating
            if tmdb_data.get('vote_average') is not None and self.property_mapping['rating_property_id']:
                current_rating = current_data.get('rating_property_id')
                tmdb_rating = tmdb_data['vote_average']
                
                if current_rating != tmdb_rating:
                    property_key = self._get_property_key(self.property_mapping['rating_property_id'])
                    if property_key:
                        new_properties[property_key] = {
                            'number': tmdb_rating
                        }
                        has_changes = True
            
            return has_changes
            
        except Exception as e:
            logger.error(f"Error comparing extended properties: {e}")
            return has_changes
    
    def _handle_field_behavior(self, tmdb_data: List[str], current_data: List[str], field_name: str, behavior: str) -> Optional[List[str]]:
        """Handle field behavior based on configuration."""
        if behavior == 'skip':
            logger.info(f"Skipping {field_name} field (configured to skip)")
            return None
        
        elif behavior == 'default':
            # Always overwrite with TMDb data (even if empty)
            if tmdb_data:
                logger.info(f"Default behavior for {field_name}: overwriting with TMDb data: {tmdb_data}")
                return tmdb_data
            else:
                logger.info(f"Default behavior for {field_name}: clearing field (TMDb has no data)")
                return []
        
        elif behavior == 'merge':
            # Merge TMDb data with existing data
            if not tmdb_data:
                logger.info(f"No TMDb {field_name} data - preserving existing: {current_data}")
                return current_data
            
            merged = list(set(tmdb_data + current_data))
            logger.info(f"Merging {field_name}: TMDb={tmdb_data}, Existing={current_data}, Merged={merged}")
            return merged
        
        elif behavior == 'preserve':
            # Only update if TMDb has data
            if tmdb_data:
                logger.info(f"Preserve behavior for {field_name}: updating with TMDb data: {tmdb_data}")
                return tmdb_data
            else:
                logger.info(f"Preserve behavior for {field_name}: preserving existing data: {current_data}")
                return None  # Don't update the field
        
        else:
            logger.warning(f"Unknown behavior '{behavior}' for {field_name}, using default")
            return tmdb_data if tmdb_data else []

    def _get_property_key(self, property_id: str) -> Optional[str]:
        """Get the property key for a given property ID."""
        return self.property_id_to_key.get(property_id)
    
    def _format_extended_properties(self, tmdb_data: Dict, content_type: str, properties: Dict):
        """Format extended TMDb properties."""
        try:
            # Episodes (TV shows) - Total planned episodes (including future seasons)
            if content_type == 'tv' and tmdb_data.get('number_of_episodes') and self.property_mapping['episodes_property_id']:
                property_key = self._get_property_key(self.property_mapping['episodes_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['number_of_episodes']
                    }
            
            # Released Episodes (TV shows) - Last episode number that has aired
            if content_type == 'tv' and self.property_mapping['released_episodes_property_id']:
                released_episodes = None
                
                # Use last_episode_to_air to get the actual last aired episode
                last_episode = tmdb_data.get('last_episode_to_air')
                if last_episode and last_episode.get('episode_number') and last_episode.get('season_number'):
                    # Calculate cumulative episode number across all seasons
                    seasons = tmdb_data.get('seasons', [])
                    if seasons:
                        cumulative_episodes = 0
                        last_season = last_episode.get('season_number', 0)
                        
                        # Sum episodes from all seasons up to and including the last aired season
                        for season in seasons:
                            season_number = season.get('season_number', 0)
                            if season_number > 0 and season_number < last_season:
                                # For completed seasons, count all episodes
                                if season.get('air_date'):
                                    cumulative_episodes += season.get('episode_count', 0)
                            elif season_number == last_season:
                                # For the last aired season, only count up to the last aired episode
                                cumulative_episodes += last_episode.get('episode_number', 0)
                                break
                        
                        if cumulative_episodes > 0:
                            released_episodes = cumulative_episodes
                
                # Set property if we found released episodes data
                if released_episodes is not None:
                    property_key = self._get_property_key(self.property_mapping['released_episodes_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'number': released_episodes
                        }
            
            # Next Episode Air Date (TV shows)
            if content_type == 'tv' and self.property_mapping['next_episode_property_id']:
                next_episode = tmdb_data.get('next_episode_to_air')
                if next_episode and next_episode.get('air_date'):
                    property_key = self._get_property_key(self.property_mapping['next_episode_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'date': {'start': next_episode['air_date']}
                        }
            
            # Website
            if tmdb_data.get('homepage') and self.property_mapping['website_property_id']:
                property_key = self._get_property_key(self.property_mapping['website_property_id'])
                if property_key:
                    properties[property_key] = {
                        'url': tmdb_data['homepage']
                    }
            
            # TMDb Homepage
            if tmdb_data.get('id') and self.property_mapping['homepage_property_id']:
                tmdb_url = f"https://www.themoviedb.org/{content_type}/{tmdb_data['id']}"
                property_key = self._get_property_key(self.property_mapping['homepage_property_id'])
                if property_key:
                    properties[property_key] = {
                        'url': tmdb_url
                    }
            
            # Cast (top 5 cast members)
            if tmdb_data.get('credits', {}).get('cast') and self.property_mapping['cast_property_id']:
                cast = tmdb_data['credits']['cast'][:5]  # Top 5 cast members
                cast_names = [person['name'] for person in cast]
                property_key = self._get_property_key(self.property_mapping['cast_property_id'])
                if property_key:
                    cast_options = build_multi_select_options(cast_names, context='cast')
                    properties[property_key] = {'multi_select': cast_options}
            
            # Director(s)
            if tmdb_data.get('credits', {}).get('crew') and self.property_mapping['director_property_id']:
                directors = [person for person in tmdb_data['credits']['crew'] if person['job'] == 'Director']
                director_names = [person['name'] for person in directors[:3]]  # Top 3 directors
                if director_names:
                    property_key = self._get_property_key(self.property_mapping['director_property_id'])
                    if property_key:
                        director_options = build_multi_select_options(director_names, context='directors')
                        properties[property_key] = {'multi_select': director_options}
            
            # Creator(s) (TV shows) - Show creators/showrunners
            if content_type == 'tv' and tmdb_data.get('created_by') and self.property_mapping['creator_property_id']:
                creators = tmdb_data['created_by']
                creator_names = [creator['name'] for creator in creators[:5]]  # Top 5 creators
                if creator_names:
                    property_key = self._get_property_key(self.property_mapping['creator_property_id'])
                    if property_key:
                        creator_options = build_multi_select_options(creator_names, context='creators')
                        properties[property_key] = {'multi_select': creator_options}
            
            # Production Companies
            if tmdb_data.get('production_companies') and self.property_mapping['production_companies_property_id']:
                companies = [company['name'] for company in tmdb_data['production_companies'][:5]]  # Top 5 companies
                property_key = self._get_property_key(self.property_mapping['production_companies_property_id'])
                if property_key:
                    company_options = build_multi_select_options(companies, context='production_companies')
                    properties[property_key] = {'multi_select': company_options}
            
            # Budget
            if tmdb_data.get('budget') and self.property_mapping['budget_property_id']:
                property_key = self._get_property_key(self.property_mapping['budget_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['budget']
                    }
            
            # Revenue
            if tmdb_data.get('revenue') and self.property_mapping['revenue_property_id']:
                property_key = self._get_property_key(self.property_mapping['revenue_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['revenue']
                    }
            
            # Original Language
            if tmdb_data.get('original_language') and self.property_mapping['original_language_property_id']:
                property_key = self._get_property_key(self.property_mapping['original_language_property_id'])
                if property_key:
                    properties[property_key] = {
                        'select': {'name': tmdb_data['original_language'].upper()}
                    }
            
            # Production Countries
            if tmdb_data.get('production_countries') and self.property_mapping['production_countries_property_id']:
                countries = [country['name'] for country in tmdb_data['production_countries'][:5]]  # Top 5 countries
                property_key = self._get_property_key(self.property_mapping['production_countries_property_id'])
                if property_key:
                    country_options = build_multi_select_options(countries, context='production_countries')
                    properties[property_key] = {'multi_select': country_options}
            
            # Tagline
            if tmdb_data.get('tagline') and self.property_mapping['tagline_property_id']:
                property_key = self._get_property_key(self.property_mapping['tagline_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{'text': {'content': tmdb_data['tagline']}}]
                    }
            
            # Popularity
            if tmdb_data.get('popularity') and self.property_mapping['popularity_property_id']:
                property_key = self._get_property_key(self.property_mapping['popularity_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['popularity']
                    }
            
            # Runtime in Minutes
            if tmdb_data.get('runtime') and self.property_mapping['runtime_minutes_property_id']:
                property_key = self._get_property_key(self.property_mapping['runtime_minutes_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['runtime']
                    }
            
            # Adult Content
            if tmdb_data.get('adult') and self.property_mapping['adult_content_property_id']:
                property_key = self._get_property_key(self.property_mapping['adult_content_property_id'])
                if property_key:
                    properties[property_key] = {
                        'checkbox': tmdb_data['adult']
                    }
            
            # Collection (Movies only)
            if content_type == 'movie' and tmdb_data.get('belongs_to_collection') and self.property_mapping['collection_property_id']:
                collection = tmdb_data['belongs_to_collection']
                if collection and collection.get('name'):
                    property_key = self._get_property_key(self.property_mapping['collection_property_id'])
                    if property_key:
                        collection_options = build_multi_select_options([collection['name']], context='collection')
                        properties[property_key] = {'multi_select': collection_options}
            
            # Watch Providers (using TMDb)
            if self.property_mapping['watch_providers_property_id'] and tmdb_data.get('id'):
                # Get watch providers from TMDb
                watch_providers_data = self.tmdb.get_watch_providers(content_type, tmdb_data['id'])
                if watch_providers_data:
                    us_providers = watch_providers_data.get('results', {}).get('US', {})
                    new_providers = []
                    
                    # Extract providers from flatrate, free, and ads categories
                    for category in ['flatrate', 'free', 'ads']:
                        providers = us_providers.get(category, [])
                        for provider in providers:
                            provider_name = provider.get('provider_name', 'Unknown')
                            # Normalize provider name to condense similar services
                            normalized_name = self.tmdb.normalize_provider_name(provider_name)
                            new_providers.append(normalized_name)
                    
                    # Remove duplicates and limit to top 10
                    new_providers = list(dict.fromkeys(new_providers))[:10]
                    
                    # Set watch providers if we found any
                    if new_providers:
                        property_key = self._get_property_key(self.property_mapping['watch_providers_property_id'])
                        if property_key:
                            provider_options = build_multi_select_options(new_providers, context='watch_providers')
                            properties[property_key] = {'multi_select': provider_options}
            
        except Exception as e:
            logger.error(f"Error formatting extended properties: {e}")
    
    def _get_property_key(self, property_id: str) -> Optional[str]:
        """Get the property key for a given property ID."""
        return self.property_id_to_key.get(property_id)
    
    def sync_page(self, page: Dict, force_icons: bool = False, force_all: bool = False, force_update: bool = False) -> Optional[bool]:
        """Sync a single page with TMDb data.
        Returns:
            True: Successfully processed (with or without changes)
            False: Failed to process due to error
            None: Skipped (missing required data)
        """
        try:
            page_id = page['id']
            current_data = self.extract_current_data(page)
            type_missing = not bool(current_data.get('content_type_property_id'))
            title_and_type = self.extract_title_and_type(page, current_data)
            
            if not title_and_type:
                logger.warning(f"Missing title or content type for page {page_id}")
                logger.warning(f"Skipping page {page_id} - missing title or content type")
                return None  # Indicate skipped, not failed
            
            title, content_type = title_and_type
            
            # Skip TMDb sync for YouTube content
            if content_type and content_type.lower() == 'youtube':
                logger.info(f"Skipping TMDb sync for YouTube content: {title}")
                return True  # Successfully processed, no sync needed
            
            logger.info(f"Processing: {title} ({content_type})")
            
            # Extract current data for comparison (already pulled before inference)
            
            # Check if we already have TMDb ID and can skip search
            current_tmdb_id = current_data.get('tmdb_id_property_id')
            details = None  # Initialize details variable
            
            if current_tmdb_id:
                # Try to get details directly using existing TMDb ID
                if content_type == 'movie':
                    details = self.tmdb.get_movie_details(current_tmdb_id)
                else:
                    details = self.tmdb.get_tv_details(current_tmdb_id)
                
                if details:
                    logger.info(f"Using existing TMDb ID {current_tmdb_id} for {title}")
                else:
                    # TMDb ID might be invalid, fall back to search
                    details = None
            
            # If no direct lookup, search TMDb
            if not details:
                if content_type == 'movie':
                    search_result = self.tmdb.search_movie(title)
                    if search_result:
                        details = self.tmdb.get_movie_details(search_result['id'])
                else:
                    search_result = self.tmdb.search_tv(title)
                    if search_result:
                        details = self.tmdb.get_tv_details(search_result['id'])
            
            if not details:
                logger.warning(f"Could not get details for: {title}")
                return False
            
            # Check if content is completed and should be skipped (unless force_all or force_update is enabled)
            # Only skip if the page has been updated at least once (has TMDb data in Notion)
            if not force_all and not force_update:
                status = details.get('status', '').lower()
                has_tmdb_data = bool(current_data.get('tmdb_id_property_id'))
                
                if content_type == 'tv' and status in ['ended', 'canceled'] and has_tmdb_data:
                    logger.info(f"Skipping completed TV show: {title} (status: {status}, already synced)")
                    if type_missing:
                        self._ensure_content_type_property(page_id, content_type)
                    return True  # Skip but don't count as failed
                elif content_type == 'movie' and status in ['released'] and has_tmdb_data:
                    logger.info(f"Skipping released movie: {title} (status: {status}, already synced)")
                    if type_missing:
                        self._ensure_content_type_property(page_id, content_type)
                    return True  # Skip but don't count as failed
            
            # Compare current data with TMDb data and get only changed properties
            new_properties, has_changes = self.compare_and_format_properties(current_data, details, content_type)
            
            # #region agent log
            import json, time, os as _os
            try:
                _log_dir = _os.path.join(_os.getcwd(), '.cursor')
                _os.makedirs(_log_dir, exist_ok=True)
                with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                    f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_a","timestamp":int(time.time()*1000),"location":"sync.py:1293","message":"TMDb details received","data":{"title":title,"tmdb_id":details.get('id'),"has_backdrop_path":bool(details.get('backdrop_path')),"backdrop_path_value":details.get('backdrop_path'),"poster_path":details.get('poster_path')},"runId":"cover_debug","hypothesisId":"A,D,E"})+'\n')
            except: pass
            # #endregion
            
            # Check if cover needs updating (only if no cover exists)
            new_cover_url = None
            current_cover_url = current_data.get('_cover_url')
            
            # #region agent log
            try:
                with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                    f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_b","timestamp":int(time.time()*1000),"location":"sync.py:1297","message":"Current cover status","data":{"title":title,"current_cover_url":current_cover_url,"has_existing_cover":bool(current_cover_url)},"runId":"cover_debug","hypothesisId":"B"})+'\n')
            except: pass
            # #endregion
            
            # Only set cover if there's no existing cover
            if details.get('backdrop_path') and not current_cover_url:
                new_cover_url = f"https://image.tmdb.org/t/p/original{details['backdrop_path']}"
                logger.info(f"Setting cover image for {title} (no existing cover)")
                # #region agent log
                try:
                    with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                        f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_c","timestamp":int(time.time()*1000),"location":"sync.py:1300","message":"Cover condition TRUE - will set cover","data":{"title":title,"new_cover_url":new_cover_url},"runId":"cover_debug","hypothesisId":"C"})+'\n')
                except: pass
                # #endregion
            elif current_cover_url:
                logger.info(f"Skipping cover update for {title} (cover already exists)")
                # #region agent log
                try:
                    with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                        f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_d","timestamp":int(time.time()*1000),"location":"sync.py:1303","message":"Cover condition FALSE - cover exists","data":{"title":title,"reason":"cover_already_exists"},"runId":"cover_debug","hypothesisId":"B,C"})+'\n')
                except: pass
                # #endregion
            else:
                # #region agent log
                try:
                    with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                        f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_e","timestamp":int(time.time()*1000),"location":"sync.py:1305","message":"Cover condition FALSE - no backdrop_path","data":{"title":title,"reason":"no_backdrop_path_in_tmdb"},"runId":"cover_debug","hypothesisId":"A,E"})+'\n')
                except: pass
                # #endregion
            
            cover_changed = new_cover_url is not None
            
            # #region agent log
            try:
                with open(_os.path.join(_log_dir, 'debug.log'), 'a') as f:
                    f.write(json.dumps({"id":f"log_{int(time.time()*1000)}_f","timestamp":int(time.time()*1000),"location":"sync.py:1306","message":"Update decision values","data":{"title":title,"has_changes":has_changes,"cover_changed":cover_changed,"force_icons":force_icons,"will_update":has_changes or cover_changed or force_icons},"runId":"cover_debug","hypothesisId":"C"})+'\n')
            except: pass
            # #endregion
            
            # Only update if there are changes (or if forcing icon updates)
            if not has_changes and not cover_changed and not force_icons:
                logger.info(f"No changes detected for: {title}")
                if type_missing:
                    self._ensure_content_type_property(page_id, content_type)
                return True
            
            # Determine icon based on content type (default to emojis)
            icon = None
            if content_type == 'movie':
                icon = '🎬'  # Movie camera emoji
            elif content_type == 'tv':
                icon = '📺'  # Television emoji
            
            # Update the page with only changed properties (or force icon update)
            if self.notion.update_page(page_id, new_properties, new_cover_url if cover_changed else None, icon):
                change_count = len(new_properties)
                change_text = f"{change_count} properties" if change_count > 1 else f"{change_count} property"
                cover_text = " + cover" if cover_changed else ""
                
                # Format icon text for logging
                icon_text = ""
                if icon:
                    icon_text = f" + icon ({icon})"
                
                # Special message for force icons mode
                if force_icons and not has_changes and not cover_changed:
                    logger.info(f"Forced icon update: {title}{icon_text}")
                else:
                    logger.info(f"Successfully updated: {title} ({change_text}{cover_text}{icon_text})")
                return True
            else:
                logger.error(f"Failed to update: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing page {page.get('id')}: {e}")
            return False
    
    def get_last_edited_page(self) -> Optional[Dict]:
        """Get the most recently edited page from the Notion database."""
        try:
            logger.info("Fetching last edited page from database")
            
            # First, try to find a last_edited_time field in the database schema
            database = self.notion.get_database(self.database_id)
            if not database:
                logger.error("Could not retrieve database schema")
                return None
            
            properties = database.get('properties', {})
            last_edited_field = None
            
            # Look for common variations of last edited time field names
            possible_names = ['last edit', 'Last Edit', 'last_edited_time', 'Last Edited Time', 'last_edited', 'Last Edited', 'edited_time', 'Edited Time']
            
            for prop_key, prop_data in properties.items():
                prop_name = prop_data.get('name', '').lower()
                if any(name.lower() in prop_name for name in possible_names):
                    last_edited_field = prop_key
                    logger.info(f"Found last edited time field: {prop_key} -> {prop_data.get('name')}")
                    break
            
            if last_edited_field:
                # Use the database field for sorting (most efficient)
                response = self.notion.client.databases.query(
                    self.database_id,
                    sorts=[{
                        "property": last_edited_field,
                        "direction": "descending"
                    }],
                    page_size=1
                )
            else:
                # Fall back to sorting by Notion's built-in last_edited_time
                logger.info("No last_edited_time field found, using Notion's built-in sorting")
                response = self.notion.client.databases.query(
                    self.database_id,
                    page_size=100  # Get more pages to sort locally
                )
                
                if response.get('results'):
                    # Sort pages by last_edited_time (most recent first)
                    pages = response['results']
                    pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
                    response['results'] = pages[:1]  # Keep only the most recent
            
            if response.get('results') and len(response['results']) > 0:
                page = response['results'][0]
                logger.info(f"Found last edited page: {page.get('id')} (edited: {page.get('last_edited_time')})")
                return page
            else:
                logger.warning("No pages found in database")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching last edited page: {e}")
            return None

    def run_sync_last_page(self, force_icons: bool = False, force_all: bool = False, force_update: bool = False) -> Dict:
        """Run synchronization for only the last edited page."""
        logger.info("Starting Notion-TMDb synchronization (LAST PAGE MODE)")
        if force_update:
            logger.info("Force update mode enabled - will update pages even if they already have TMDb information")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        if not self.property_mapping['content_type_property_id']:
            logger.error("Cannot proceed: No content type property found in database")
            return {'success': False, 'message': 'No content type property found'}
        
        start_time = time.time()
        
        # Get the last edited page
        page = self.get_last_edited_page()
        if not page:
            logger.warning("No pages found in database")
            return {'success': False, 'message': 'No pages found'}
        
        logger.info(f"Processing last edited page: {page.get('id')}")
        
        # Sync the single page
        result = self.sync_page(page, force_icons=force_icons, force_all=force_all, force_update=force_update)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result is True:
            logger.info(f"Last page sync completed successfully in {duration:.2f} seconds")
            return {
                'success': True,
                'total_pages': 1,
                'successful_updates': 1,
                'failed_updates': 0,
                'skipped_updates': 0,
                'duration': duration
            }
        elif result is False:
            logger.error("Last page sync failed")
            return {
                'success': False,
                'total_pages': 1,
                'successful_updates': 0,
                'failed_updates': 1,
                'skipped_updates': 0,
                'duration': duration
            }
        else:  # result is None (skipped)
            logger.info(f"Last page sync skipped in {duration:.2f} seconds")
            return {
                'success': True,
                'total_pages': 1,
                'successful_updates': 0,
                'failed_updates': 0,
                'skipped_updates': 1,
                'duration': duration
            }

    def run_page_sync(
        self,
        page_id: str,
        *,
        force_icons: bool = False,
        force_all: bool = False,
        force_update: bool = False,
    ) -> Dict:
        """Run synchronization for a specific page."""
        logger.info("Starting Notion-TMDb synchronization (PAGE MODE)")
        if force_update:
            logger.info("Force update mode enabled - will update even completed entries")

        page = self.notion.get_page(page_id)
        if not page:
            logger.error("Unable to retrieve Notion page %s", page_id)
            return {'success': False, 'message': f'Page {page_id} could not be retrieved from Notion'}

        parent_db_id = page.get('parent', {}).get('database_id')
        if parent_db_id:
            normalized_parent = normalize_id(parent_db_id)
            normalized_configured = normalize_id(self.database_id)
            if normalized_parent != normalized_configured:
                logger.error(
                    "Page %s belongs to database %s, but configured database is %s",
                    page_id,
                    parent_db_id,
                    self.database_id,
                )
                return {
                    'success': False,
                    'message': f'Page {page_id} does not belong to the configured database',
                }

        result_flag = self.sync_page(page, force_icons=force_icons, force_all=force_all, force_update=force_update)

        results = {
            'success': True,
            'total_pages': 1,
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 0,
            'duration': 0,
        }

        if result_flag is True:
            results['successful_updates'] = 1
        elif result_flag is False:
            results['failed_updates'] = 1
            results['success'] = False
        else:
            results['skipped_updates'] = 1

        return results

    def run_sync(self, force_icons: bool = False, force_all: bool = False, max_workers: int = 3, force_update: bool = False) -> Dict:
        """Run the complete synchronization process."""
        if force_icons and force_all and force_update:
            logger.info("Starting Notion-TMDb synchronization (FORCE ICONS + FORCE ALL + FORCE UPDATE MODE)")
        elif force_icons and force_all:
            logger.info("Starting Notion-TMDb synchronization (FORCE ICONS + FORCE ALL MODE)")
        elif force_icons and force_update:
            logger.info("Starting Notion-TMDb synchronization (FORCE ICONS + FORCE UPDATE MODE)")
        elif force_all and force_update:
            logger.info("Starting Notion-TMDb synchronization (FORCE ALL + FORCE UPDATE MODE)")
        elif force_icons:
            logger.info("Starting Notion-TMDb synchronization (FORCE ICONS MODE)")
        elif force_all:
            logger.info("Starting Notion-TMDb synchronization (FORCE ALL MODE)")
        elif force_update:
            logger.info("Starting Notion-TMDb synchronization (FORCE UPDATE MODE)")
        else:
            logger.info("Starting Notion-TMDb synchronization")
        
        logger.info(f"Using {max_workers} parallel workers for processing")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        if not self.property_mapping['content_type_property_id']:
            logger.error("Cannot proceed: No content type property found in database")
            return {'success': False, 'message': 'No content type property found'}
        
        start_time = time.time()
        pages = self.get_notion_pages()
        
        if not pages:
            logger.warning("No pages found in database")
            return {'success': False, 'message': 'No pages found'}
        
        logger.info(f"Found {len(pages)} pages to process")
        
        successful_updates = 0
        failed_updates = 0
        skipped_updates = 0
        
        # Process pages in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_page = {
                executor.submit(self.sync_page, page, force_icons, force_all, force_update): page 
                for page in pages
            }
            
            # Process completed tasks as they finish
            for i, future in enumerate(concurrent.futures.as_completed(future_to_page), 1):
                page = future_to_page[future]
                try:
                    result = future.result()
                    if result is True:
                        successful_updates += 1
                    elif result is False:
                        failed_updates += 1
                    else:  # result is None (skipped)
                        skipped_updates += 1
                    
                    logger.info(f"Completed page {i}/{len(pages)}")
                    
                except Exception as e:
                    logger.error(f"Error processing page {page.get('id')}: {e}")
                    failed_updates += 1
                
                # Small delay to respect rate limits
                time.sleep(self.request_delay / max_workers)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Sync completed in {duration:.2f} seconds")
        logger.info(f"Successful updates: {successful_updates}")
        logger.info(f"Failed updates: {failed_updates}")
        if skipped_updates > 0:
            logger.info(f"Skipped updates: {skipped_updates}")
        
        return {
            'success': True,
            'total_pages': len(pages),
            'successful_updates': successful_updates,
            'failed_updates': failed_updates,
            'skipped_updates': skipped_updates,
            'duration': duration
        }

def validate_environment():
    """Validate environment variables and configuration."""
    errors = []
    
    # Check required environment variables
    notion_token = get_notion_token()
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    database_id = get_database_id('NOTION_MOVIETV_DATABASE_ID', 'NOTION_DATABASE_ID')
    
    if not notion_token:
        errors.append("NOTION_INTERNAL_INTEGRATION_SECRET (or legacy NOTION_TOKEN)")
    if not tmdb_api_key:
        errors.append("TMDB_API_KEY: Your TMDb API key")
    if not database_id:
        errors.append("NOTION_MOVIETV_DATABASE_ID (or NOTION_DATABASE_ID) for your Notion database ID")
    
    if errors:
        logger.error("Missing required environment variables:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease check your .env file or environment variables.")
        return False
    
    # Validate API keys format
    if notion_token and not notion_token.startswith(('secret_', 'ntn_')):
        logger.warning("Notion token should start with 'secret_' or 'ntn_'")
    
    if len(tmdb_api_key) < 20:
        logger.warning("TMDB_API_KEY seems too short")
    
    # Validate database ID format (should be 32 characters)
    if database_id and len(database_id.replace('-', '')) != 32:
        logger.warning("Notion database ID format seems incorrect")
    
    return True


def _build_sync_instance() -> NotionTMDbSync:
    notion_token = get_notion_token()
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    database_id = get_database_id('NOTION_MOVIETV_DATABASE_ID', 'NOTION_DATABASE_ID')
    return NotionTMDbSync(notion_token, tmdb_api_key, database_id)


def enforce_worker_limits(workers: int) -> int:
    if workers < 1:
        raise ValueError("Number of workers must be at least 1")
    if workers > 10:
        logger.warning("Using %s workers may overwhelm APIs. Consider reducing to 5 or fewer.", workers)
    return workers


def run_sync(
    *,
    force_icons: bool = False,
    force_all: bool = False,
    workers: int = 3,
    last_page: bool = False,
    page_id: Optional[str] = None,
    force_update: bool = False,
) -> Dict:
    """Run the Movies/TV sync with the provided options."""
    enforce_worker_limits(workers)

    if page_id and last_page:
        raise RuntimeError("page-id mode cannot be combined with last-page mode")

    sync = _build_sync_instance()

    if page_id:
        return sync.run_page_sync(
            page_id,
            force_icons=force_icons,
            force_all=force_all,
            force_update=force_update,
        )

    if last_page:
        return sync.run_sync_last_page(
            force_icons=force_icons,
            force_all=force_all,
            force_update=force_update,
        )

    return sync.run_sync(
        force_icons=force_icons,
        force_all=force_all,
        max_workers=workers,
        force_update=force_update,
    )


def get_database_ids() -> List[str]:
    """Return normalized database IDs served by this sync."""
    database_id = get_database_id('NOTION_MOVIETV_DATABASE_ID', 'NOTION_DATABASE_ID')
    normalized = normalize_id(database_id) if database_id else None
    return [normalized] if normalized else []

def main():
    """Main function to run the sync script."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Synchronize Notion database with TMDb data')
        parser.add_argument('--force-icons', action='store_true', 
                           help='Force update all page icons (one-time operation)')
        parser.add_argument('--force-all', action='store_true',
                           help='Process all pages including completed content (overrides skip optimization)')
        parser.add_argument('--workers', type=int, default=3, metavar='N',
                           help='Number of parallel workers (default: 3, max recommended: 5)')
        parser.add_argument('--last-page', action='store_true',
                           help='Sync only the most recently edited page (useful for iOS shortcuts)')
        parser.add_argument('--force-update', action='store_true',
                           help='Force update pages even if they already have TMDb information (overrides skip optimization)')
        args = parser.parse_args()
        
        # Validate workers parameter
        if args.workers < 1:
            logger.error("Number of workers must be at least 1")
            sys.exit(1)
        if args.workers > 10:
            logger.warning(f"Using {args.workers} workers may overwhelm APIs. Consider reducing to 5 or fewer.")
        
        logger.info("Starting Notion TMDb Sync")
        
        # Validate environment
        if not validate_environment():
            sys.exit(1)
        
        # Get environment variables
        notion_token = get_notion_token()
        tmdb_api_key = os.getenv('TMDB_API_KEY')
        database_id = get_database_id('NOTION_MOVIETV_DATABASE_ID', 'NOTION_DATABASE_ID')
        
        # Create sync instance and run
        sync = NotionTMDbSync(notion_token, tmdb_api_key, database_id)
        
        # Choose sync mode based on arguments
        if args.last_page:
            result = sync.run_sync_last_page(force_icons=args.force_icons, force_all=args.force_all, force_update=args.force_update)
        else:
            result = sync.run_sync(force_icons=args.force_icons, force_all=args.force_all, max_workers=args.workers, force_update=args.force_update)
        
        if result['success']:
            logger.info("Synchronization completed successfully")
            logger.info(f"Updated: {result['successful_updates']} pages")
            logger.info(f"Failed: {result['failed_updates']} pages")
            if args.last_page:
                logger.info("Last page mode completed - only the most recently edited page was processed")
            if args.force_icons:
                logger.info("Force icons mode completed - all page icons have been updated")
            if args.force_all:
                logger.info("Force all mode completed - all pages processed including completed content")
            elif not args.force_icons and not args.last_page:
                logger.info("Optimization mode completed - completed content was skipped for efficiency")
            sys.exit(0)
        else:
            logger.error("Synchronization failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Synchronization interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()
