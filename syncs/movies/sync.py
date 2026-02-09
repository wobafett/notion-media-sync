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
from shared.notion_api import NotionAPI
from shared.utils import build_multi_select_options, build_created_after_filter, get_database_id, get_notion_token, normalize_id
from shared.change_detection import has_property_changes

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
        COLLECTION_PROPERTY_ID, DNS_PROPERTY_ID, FIELD_BEHAVIOR,
        SEASON_EPISODES_PROPERTY_ID, LATEST_EPISODE_DISPLAY_PROPERTY_ID,
        MY_SEASON_PROPERTY_ID, MY_EPISODE_PROPERTY_ID
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
            'Shout! Factory TV': 'Shout! Factory',
            'MUBI Amazon Channel': 'MUBI',
            'Cinemax Amazon Channel': 'Cinemax',
            'Cinemax Apple TV Channel': 'Cinemax',
            'Showtime Amazon Channel': 'Showtime',
            'Showtime Roku Premium Channel': 'Showtime',
            'Starz Amazon Channel': 'Starz',
            'Starz Roku Premium Channel': 'Starz',
            # ALLBLK consolidation
            'ALLBLK Apple TV Channel': 'ALLBLK',
            'ALLBLK Amazon Channel': 'ALLBLK',
            # Paramount consolidation
            'Paramount+ Essential': 'Paramount+',
            # Case-insensitive Adult Swim
            'Adultswim': 'Adult Swim',
        }
        
        # Apply specific service mappings
        normalized = service_mappings.get(normalized, normalized)
        
        # Pattern 4: Additional case-insensitive mappings for common variations
        # Check lowercase version for case-insensitive matches
        lower_normalized = normalized.lower()
        case_insensitive_mappings = {
            'adultswim': 'Adult Swim',
            'hbo max': 'HBO Max',  # Ensures consistent spacing
        }
        if lower_normalized in case_insensitive_mappings:
            normalized = case_insensitive_mappings[lower_normalized]
        
        # Pattern 5: Handle "with Showtime" and similar combinations
        if ' with Showtime' in normalized:
            normalized = normalized.replace(' with Showtime', '')
        
        # Final cleanup: trim any remaining whitespace
        normalized = normalized.strip()
        
        return normalized
    

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
                'dns_property_id': DNS_PROPERTY_ID,
                
                # Season/Episode tracking properties
                'season_episodes_property_id': SEASON_EPISODES_PROPERTY_ID,
                'latest_episode_display_property_id': LATEST_EPISODE_DISPLAY_PROPERTY_ID,
                'my_season_property_id': MY_SEASON_PROPERTY_ID,
                'my_episode_property_id': MY_EPISODE_PROPERTY_ID,
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
    
    def _parse_tmdb_url(self, url: str) -> Optional[Dict[str, str]]:
        """
        Parse TMDB URL and extract type + ID.
        
        Supports formats:
        - https://www.themoviedb.org/movie/550
        - https://www.themoviedb.org/tv/1399
        - https://www.themoviedb.org/movie/550-fight-club (with slug)
        
        Returns: {"type": "movie"|"tv", "id": "550"} or None
        """
        import re
        
        if not url:
            return None
        
        # Pattern: themoviedb.org/(movie|tv)/(\d+)
        pattern = r'themoviedb\.org/(movie|tv)/(\d+)'
        match = re.search(pattern, url)
        
        if match:
            content_type = match.group(1)  # "movie" or "tv"
            tmdb_id = match.group(2)  # numeric ID
            return {"type": content_type, "id": tmdb_id}
        
        logger.warning(f"Unable to parse TMDB URL: {url}")
        return None
    
    def get_notion_pages(self, status_filter: Optional[str] = None, created_after: Optional[str] = None) -> List[Dict]:
        """Get all pages from the Notion database, optionally filtered by status and/or creation date."""
        filter_params = None
        filters_to_combine = []
        
        if status_filter:
            # Build Notion filter for status property
            status_property_id = self.property_mapping.get('status_property_id')
            if status_property_id:
                # Handle multiple statuses (e.g., "Released,Ended")
                statuses = [s.strip() for s in status_filter.split(',')]
                
                if len(statuses) == 1:
                    filters_to_combine.append({
                        "property": status_property_id,
                        "status": {"equals": statuses[0]}
                    })
                    logger.info(f"Filtering pages by status: {statuses[0]}")
                else:
                    # OR filter for multiple statuses
                    filters_to_combine.append({
                        "or": [
                            {"property": status_property_id, "status": {"equals": s}}
                            for s in statuses
                        ]
                    })
                    logger.info(f"Filtering pages by statuses: {', '.join(statuses)}")
        
        if created_after:
            created_filter = build_created_after_filter(created_after)
            if created_filter:
                filters_to_combine.append(created_filter)
                logger.info(f"Filtering pages created on/after {created_after}")
        
        # Combine filters with AND logic if multiple
        if len(filters_to_combine) == 1:
            filter_params = filters_to_combine[0]
        elif len(filters_to_combine) > 1:
            filter_params = {"and": filters_to_combine}
        
        logger.info(f"Fetching pages from database {self.database_id}")
        return self.notion.query_database(self.database_id, filter_params)
    
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
    
    def format_all_properties(self, tmdb_data: Dict, content_type: str) -> Dict:
        """Format all TMDb properties for Notion (core + extended)."""
        properties = {}
        
        try:
            # --- CORE PROPERTIES ---
            
            # Title
            if tmdb_data.get('title') or tmdb_data.get('name'):
                title = tmdb_data.get('title') or tmdb_data.get('name')
                if self.property_mapping['title_property_id']:
                    property_key = self._get_property_key(self.property_mapping['title_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'title': [{'text': {'content': title}}]
                        }
            
            # Description
            if tmdb_data.get('overview') and self.property_mapping['description_property_id']:
                property_key = self._get_property_key(self.property_mapping['description_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{'text': {'content': tmdb_data['overview']}}]
                    }
            
            # Release Date
            if (tmdb_data.get('release_date') or tmdb_data.get('first_air_date')) and self.property_mapping['release_date_property_id']:
                release_date = tmdb_data.get('release_date') or tmdb_data.get('first_air_date')
                property_key = self._get_property_key(self.property_mapping['release_date_property_id'])
                if property_key:
                    properties[property_key] = {
                        'date': {'start': release_date}
                    }
            
            # Genres
            if tmdb_data.get('genres') and self.property_mapping['genres_property_id']:
                genre_names = [genre['name'] for genre in tmdb_data['genres']]
                property_key = self._get_property_key(self.property_mapping['genres_property_id'])
                if property_key:
                    genre_options = build_multi_select_options(genre_names, context='genres')
                    properties[property_key] = {'multi_select': genre_options}
            
            # Status
            if tmdb_data.get('status') and self.property_mapping['status_property_id']:
                property_key = self._get_property_key(self.property_mapping['status_property_id'])
                if property_key:
                    properties[property_key] = {
                        'status': {'name': tmdb_data['status']}
                    }
            
            # Rating (vote average)
            if tmdb_data.get('vote_average') is not None and self.property_mapping['rating_property_id']:
                property_key = self._get_property_key(self.property_mapping['rating_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['vote_average']
                    }
            
            # TMDB ID
            if tmdb_data.get('id') and self.property_mapping['tmdb_id_property_id']:
                property_key = self._get_property_key(self.property_mapping['tmdb_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['id']
                    }
            
            # Runtime/Seasons
            if content_type == 'movie' and tmdb_data.get('runtime') and self.property_mapping['runtime_minutes_property_id']:
                property_key = self._get_property_key(self.property_mapping['runtime_minutes_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['runtime']
                    }
            elif content_type == 'tv' and tmdb_data.get('number_of_seasons') and self.property_mapping['seasons_property_id']:
                property_key = self._get_property_key(self.property_mapping['seasons_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['number_of_seasons']
                    }
            
            # Content Type
            select_value = self._normalize_content_type_value(content_type)
            if select_value and self.property_mapping['content_type_property_id']:
                property_key = self._get_property_key(self.property_mapping['content_type_property_id'])
                if property_key:
                    properties[property_key] = {
                        'select': {'name': select_value}
                    }
            
            # --- EXTENDED PROPERTIES ---
            
            # Episodes (TV shows) - Total planned episodes
            if content_type == 'tv' and tmdb_data.get('number_of_episodes') and self.property_mapping['episodes_property_id']:
                property_key = self._get_property_key(self.property_mapping['episodes_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': tmdb_data['number_of_episodes']
                    }
            
            # Season Episode Counts JSON (TV shows) - Store per-season episode counts
            if content_type == 'tv' and tmdb_data.get('seasons') and self.property_mapping.get('season_episodes_property_id'):
                import json
                seasons = tmdb_data.get('seasons', [])
                season_ep_map = {}
                
                for season in seasons:
                    season_num = season.get('season_number', 0)
                    ep_count = season.get('episode_count', 0)
                    # Include season 0 (specials) if it has episodes
                    if season_num >= 0 and ep_count > 0:
                        season_ep_map[str(season_num)] = ep_count
                
                if season_ep_map:
                    property_key = self._get_property_key(self.property_mapping['season_episodes_property_id'])
                    if property_key:
                        # Sort the dict by numeric key order (not alphabetic)
                        sorted_season_map = {str(k): season_ep_map[str(k)] for k in sorted([int(x) for x in season_ep_map.keys()])}
                        properties[property_key] = {
                            'rich_text': [{'text': {'content': json.dumps(sorted_season_map)}}]
                        }
            
            # Latest Episode Display (TV shows) - Human-readable "S3E5" format
            if content_type == 'tv' and self.property_mapping.get('latest_episode_display_property_id'):
                last_episode = tmdb_data.get('last_episode_to_air')
                if last_episode:
                    season_num = last_episode.get('season_number')
                    episode_num = last_episode.get('episode_number')
                    
                    if season_num is not None and episode_num is not None:
                        # Format as "S3E5"
                        display_text = f"S{season_num}E{episode_num}"
                        property_key = self._get_property_key(self.property_mapping['latest_episode_display_property_id'])
                        if property_key:
                            properties[property_key] = {
                                'rich_text': [{'text': {'content': display_text}}]
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
                cast = tmdb_data['credits']['cast'][:5]
                cast_names = [person['name'] for person in cast]
                property_key = self._get_property_key(self.property_mapping['cast_property_id'])
                if property_key:
                    cast_options = build_multi_select_options(cast_names, context='cast')
                    properties[property_key] = {'multi_select': cast_options}
            
            # Director(s)
            if tmdb_data.get('credits', {}).get('crew') and self.property_mapping['director_property_id']:
                directors = [person for person in tmdb_data['credits']['crew'] if person['job'] == 'Director']
                director_names = [person['name'] for person in directors[:3]]
                if director_names:
                    property_key = self._get_property_key(self.property_mapping['director_property_id'])
                    if property_key:
                        director_options = build_multi_select_options(director_names, context='directors')
                        properties[property_key] = {'multi_select': director_options}
            
            # Creator(s) (TV shows)
            if content_type == 'tv' and tmdb_data.get('created_by') and self.property_mapping['creator_property_id']:
                creators = tmdb_data['created_by']
                creator_names = [creator['name'] for creator in creators[:5]]
                if creator_names:
                    property_key = self._get_property_key(self.property_mapping['creator_property_id'])
                    if property_key:
                        creator_options = build_multi_select_options(creator_names, context='creators')
                        properties[property_key] = {'multi_select': creator_options}
            
            # Production Companies
            if tmdb_data.get('production_companies') and self.property_mapping['production_companies_property_id']:
                companies = [company['name'] for company in tmdb_data['production_companies'][:5]]
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
                countries = [country['name'] for country in tmdb_data['production_countries'][:5]]
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
            
            # Watch Providers
            if self.property_mapping['watch_providers_property_id'] and tmdb_data.get('id'):
                watch_providers_data = self.tmdb.get_watch_providers(content_type, tmdb_data['id'])
                if watch_providers_data:
                    us_providers = watch_providers_data.get('results', {}).get('US', {})
                    new_providers = []
                    for category in ['flatrate', 'free', 'ads']:
                        providers = us_providers.get(category, [])
                        for provider in providers:
                            provider_name = provider.get('provider_name', 'Unknown')
                            normalized_name = self.tmdb.normalize_provider_name(provider_name)
                            new_providers.append(normalized_name)
                    new_providers = list(dict.fromkeys(new_providers))[:10]
                    if new_providers:
                        property_key = self._get_property_key(self.property_mapping['watch_providers_property_id'])
                        if property_key:
                            provider_options = build_multi_select_options(new_providers, context='watch_providers')
                            properties[property_key] = {'multi_select': provider_options}
            
        except Exception as e:
            logger.error(f"Error formatting properties: {e}")
        
        return properties
    
    def _filter_properties_by_update_only(self, properties: Dict, update_only: List[str]) -> Dict:
        """Filter properties to only those specified in update_only list."""
        # Map friendly names to property IDs
        name_to_id_map = {
            'title': self.property_mapping.get('title_property_id'),
            'description': self.property_mapping.get('description_property_id'),
            'release_date': self.property_mapping.get('release_date_property_id'),
            'genres': self.property_mapping.get('genres_property_id'),
            'status': self.property_mapping.get('status_property_id'),
            'rating': self.property_mapping.get('rating_property_id'),
            'tmdb_id': self.property_mapping.get('tmdb_id_property_id'),
            'runtime': self.property_mapping.get('runtime_minutes_property_id'),
            'seasons': self.property_mapping.get('seasons_property_id'),
            'content_type': self.property_mapping.get('content_type_property_id'),
            'episodes': self.property_mapping.get('episodes_property_id'),
            'released_episodes': self.property_mapping.get('released_episodes_property_id'),
            'next_episode': self.property_mapping.get('next_episode_property_id'),
            'website': self.property_mapping.get('website_property_id'),
            'homepage': self.property_mapping.get('homepage_property_id'),
            'cast': self.property_mapping.get('cast_property_id'),
            'director': self.property_mapping.get('director_property_id'),
            'creator': self.property_mapping.get('creator_property_id'),
            'production_companies': self.property_mapping.get('production_companies_property_id'),
            'budget': self.property_mapping.get('budget_property_id'),
            'revenue': self.property_mapping.get('revenue_property_id'),
            'original_language': self.property_mapping.get('original_language_property_id'),
            'production_countries': self.property_mapping.get('production_countries_property_id'),
            'tagline': self.property_mapping.get('tagline_property_id'),
            'popularity': self.property_mapping.get('popularity_property_id'),
            'adult_content': self.property_mapping.get('adult_content_property_id'),
            'collection': self.property_mapping.get('collection_property_id'),
            'watch_providers': self.property_mapping.get('watch_providers_property_id'),
            'season_episodes': self.property_mapping.get('season_episodes_property_id'),
            'latest_episode': self.property_mapping.get('latest_episode_display_property_id'),
            'my_season': self.property_mapping.get('my_season_property_id'),
            'my_episode': self.property_mapping.get('my_episode_property_id'),
        }
        
        # Get property IDs for the specified update_only fields
        allowed_property_ids = []
        for field_name in update_only:
            prop_id = name_to_id_map.get(field_name)
            if prop_id:
                allowed_property_ids.append(prop_id)
        
        # Filter properties to only those in allowed list
        filtered = {}
        for prop_key, prop_value in properties.items():
            # Check if this property matches any allowed property ID
            for allowed_id in allowed_property_ids:
                allowed_key = self._get_property_key(allowed_id)
                if allowed_key and prop_key == allowed_key:
                    filtered[prop_key] = prop_value
                    break
        
        return filtered
    
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
    
    def _find_existing_page_by_tmdb_id(self, tmdb_id: int) -> Optional[str]:
        """Search for existing page by TMDB ID in the Notion database."""
        try:
            tmdb_id_property_id = self.property_mapping.get('tmdb_id_property_id')
            if not tmdb_id_property_id:
                logger.warning("TMDB ID property not configured")
                return None
            
            # Query database for page with matching TMDB ID
            filter_params = {
                "property": tmdb_id_property_id,
                "number": {
                    "equals": tmdb_id
                }
            }
            
            pages = self.notion.query_database(self.database_id, filter_params)
            if pages and len(pages) > 0:
                return pages[0]['id']
            
            return None
            
        except Exception as e:
            logger.debug(f"Error searching for page by TMDB ID {tmdb_id}: {e}")
            return None
    
    def _create_movie_from_tmdb(self, tmdb_data: Dict, tmdb_id: int) -> Dict:
        """Create a new movie page from TMDB data."""
        try:
            # Get movie details with credits and images
            details = self.tmdb.get_movie_details(tmdb_id)
            if not details:
                return {
                    'success': False,
                    'message': f'Could not fetch movie details for ID {tmdb_id}'
                }
            
            # Format all properties for initial page creation
            properties = self.format_all_properties(details, 'movie')
            
            # Create the page with DNS=True to prevent automation cascade
            title = details.get('title', 'Untitled Movie')
            
            # Add DNS checkbox if configured
            dns_property_id = self.property_mapping.get('dns_property_id')
            if dns_property_id:
                property_key = self._get_property_key(dns_property_id)
                if property_key:
                    properties[property_key] = {'checkbox': True}
            
            # Get cover image URL
            cover_url = None
            if details.get('backdrop_path'):
                cover_url = f"https://image.tmdb.org/t/p/original{details['backdrop_path']}"
            
            # Create the page
            page_id = self.notion.create_page(
                self.database_id,
                properties,
                cover_url,
                '🎬'  # Movie icon
            )
            
            if not page_id:
                return {
                    'success': False,
                    'message': f'Failed to create page for movie: {title}'
                }
            
            logger.info(f"Created movie page: {title} (ID: {page_id})")
            
            return {
                'success': True,
                'page_id': page_id,
                'entity_type': 'movie',
                'created': True,
                'message': f'Successfully created movie: {title}'
            }
            
        except Exception as e:
            logger.error(f"Error creating movie from TMDB: {e}")
            return {
                'success': False,
                'message': f'Error creating movie: {str(e)}'
            }
    
    def _create_tv_from_tmdb(self, tmdb_data: Dict, tmdb_id: int) -> Dict:
        """Create a new TV show page from TMDB data."""
        try:
            # Get TV details with credits and images
            details = self.tmdb.get_tv_details(tmdb_id)
            if not details:
                return {
                    'success': False,
                    'message': f'Could not fetch TV show details for ID {tmdb_id}'
                }
            
            # Format all properties for initial page creation
            properties = self.format_all_properties(details, 'tv')
            
            # Create the page with DNS=True to prevent automation cascade
            title = details.get('name', 'Untitled TV Show')
            
            # Add DNS checkbox if configured
            dns_property_id = self.property_mapping.get('dns_property_id')
            if dns_property_id:
                property_key = self._get_property_key(dns_property_id)
                if property_key:
                    properties[property_key] = {'checkbox': True}
            
            # Get cover image URL
            cover_url = None
            if details.get('backdrop_path'):
                cover_url = f"https://image.tmdb.org/t/p/original{details['backdrop_path']}"
            
            # Create the page
            page_id = self.notion.create_page(
                self.database_id,
                properties,
                cover_url,
                '📺'  # TV icon
            )
            
            if not page_id:
                return {
                    'success': False,
                    'message': f'Failed to create page for TV show: {title}'
                }
            
            logger.info(f"Created TV show page: {title} (ID: {page_id})")
            
            return {
                'success': True,
                'page_id': page_id,
                'entity_type': 'tv',
                'created': True,
                'message': f'Successfully created TV show: {title}'
            }
            
        except Exception as e:
            logger.error(f"Error creating TV show from TMDB: {e}")
            return {
                'success': False,
                'message': f'Error creating TV show: {str(e)}'
            }
    
    def create_from_tmdb_url(self, tmdb_url: str) -> Dict:
        """
        Create a new Notion page from a TMDB URL.
        
        Args:
            tmdb_url: TMDB URL for a movie or TV show
            
        Returns:
            dict with 'success', 'page_id', 'entity_type', 'created', 'message'
        """
        logger.info(f"Creating page from TMDB URL: {tmdb_url}")
        
        # Parse TMDB URL
        parsed = self._parse_tmdb_url(tmdb_url)
        if not parsed:
            return {
                'success': False,
                'message': f'Invalid TMDB URL format: {tmdb_url}'
            }
        
        content_type = parsed['type']  # 'movie' or 'tv'
        tmdb_id = int(parsed['id'])
        
        # Check for existing page by TMDB ID
        existing_page_id = self._find_existing_page_by_tmdb_id(tmdb_id)
        if existing_page_id:
            logger.info(f"{content_type.title()} already exists in Notion (by TMDB ID): {existing_page_id}")
            return {
                'success': True,
                'page_id': existing_page_id,
                'entity_type': content_type,
                'created': False,
                'message': f'{content_type.title()} already exists in database'
            }
        
        # Fetch data from TMDB
        if content_type == 'movie':
            tmdb_data = self.tmdb.get_movie_details(tmdb_id)
            if not tmdb_data:
                return {
                    'success': False,
                    'message': f'Could not fetch movie data from TMDB for ID {tmdb_id}'
                }
            return self._create_movie_from_tmdb(tmdb_data, tmdb_id)
        
        elif content_type == 'tv':
            tmdb_data = self.tmdb.get_tv_details(tmdb_id)
            if not tmdb_data:
                return {
                    'success': False,
                    'message': f'Could not fetch TV show data from TMDB for ID {tmdb_id}'
                }
            return self._create_tv_from_tmdb(tmdb_data, tmdb_id)
        
        else:
            return {
                'success': False,
                'message': f'Unsupported content type: {content_type}'
            }
    
    def sync_page(
        self, 
        page: Dict, 
        force_icons: bool = False, 
        force_update: bool = False,
        update_only: Optional[List[str]] = None,
    ) -> Optional[bool]:
        """Sync a single page with TMDb data.
        
        Args:
            page: Notion page object
            force_icons: Force update page icons
            force_update: Force update even if page is already synced
            update_only: List of property names to update (e.g., ['rating', 'watch_providers'])
        
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
            
            # Check if content is completed and should be skipped (unless force_update is enabled)
            # Only skip if the page has been updated at least once (has TMDb data in Notion)
            if not force_update:
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
            
            # Format all properties
            properties = self.format_all_properties(details, content_type)
            
            # Filter by update_only if specified
            if update_only:
                properties = self._filter_properties_by_update_only(properties, update_only)
            
            # Get page properties for comparison
            page_properties = page.get('properties', {})
            
            # Check for changes using shared utility
            changes_detected, change_details = has_property_changes(
                page_properties,
                properties,
                self._get_property_key(self.property_mapping.get('last_updated_property_id'))
            )
            
            # Check if cover needs updating (only if no cover exists)
            new_cover_url = None
            current_cover_url = current_data.get('_cover_url')
            
            # Only set cover if there's no existing cover
            if details.get('backdrop_path') and not current_cover_url:
                new_cover_url = f"https://image.tmdb.org/t/p/original{details['backdrop_path']}"
                logger.info(f"Setting cover image for {title} (no existing cover)")
            elif current_cover_url:
                logger.info(f"Skipping cover update for {title} (cover already exists)")
            
            cover_changed = new_cover_url is not None
            
            # Only update if there are changes (or if forcing icon updates)
            if not changes_detected and not cover_changed and not force_icons:
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
            if self.notion.update_page(page_id, properties, new_cover_url if cover_changed else None, icon):
                change_count = len(properties)
                change_text = f"{change_count} properties" if change_count > 1 else f"{change_count} property"
                cover_text = " + cover" if cover_changed else ""
                
                # Format icon text for logging
                icon_text = ""
                if icon:
                    icon_text = f" + icon ({icon})"
                
                # Special message for force icons mode
                if force_icons and not changes_detected and not cover_changed:
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

    def run_sync_last_page(self, force_icons: bool = False, force_update: bool = False) -> Dict:
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
        result = self.sync_page(page, force_icons=force_icons, force_update=force_update)
        
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

        result_flag = self.sync_page(page, force_icons=force_icons, force_update=force_update)

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

    def run_sync(
        self, 
        force_icons: bool = False, 
        max_workers: int = 3, 
        force_update: bool = False,
        status_filter: Optional[str] = None,
        update_only: Optional[List[str]] = None,
        created_after: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict:
        """Run the complete synchronization process."""
        if dry_run:
            logger.warning("dry_run parameter not yet fully implemented for movies sync - proceeding with normal sync")
        mode_parts = []
        if force_icons:
            mode_parts.append("FORCE ICONS")
        if force_update:
            mode_parts.append("FORCE UPDATE")
        if status_filter:
            mode_parts.append(f"STATUS={status_filter}")
        if update_only:
            mode_parts.append(f"UPDATE={','.join(update_only)}")
        if created_after:
            mode_parts.append(f"CREATED_AFTER={created_after[:10]}")
        
        mode_str = " + ".join(mode_parts) if mode_parts else "STANDARD"
        logger.info(f"Starting Notion-TMDb synchronization ({mode_str} MODE)")
        logger.info(f"Using {max_workers} parallel workers for processing")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        if not self.property_mapping['content_type_property_id']:
            logger.error("Cannot proceed: No content type property found in database")
            return {'success': False, 'message': 'No content type property found'}
        
        start_time = time.time()
        pages = self.get_notion_pages(status_filter=status_filter, created_after=created_after)
        
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
                executor.submit(self.sync_page, page, force_icons, force_update, update_only): page 
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
    workers: int = 3,
    last_page: bool = False,
    page_id: Optional[str] = None,
    force_update: bool = False,
    status_filter: Optional[str] = None,
    update_only: Optional[str] = None,
    created_after: Optional[str] = None,
    dry_run: bool = False,
    tmdb_url: Optional[str] = None,
) -> Dict:
    """Run the Movies/TV sync with the provided options."""
    enforce_worker_limits(workers)

    # Handle TMDB URL creation mode (no page_id required)
    if tmdb_url and not page_id:
        logger.info(f"TMDB URL creation mode: {tmdb_url}")
        sync = _build_sync_instance()
        return sync.create_from_tmdb_url(tmdb_url)

    if page_id and last_page:
        raise RuntimeError("page-id mode cannot be combined with last-page mode")
    
    if page_id and created_after:
        logger.warning("--created-after is ignored when --page-id is provided")

    sync = _build_sync_instance()

    if page_id:
        return sync.run_page_sync(
            page_id,
            force_icons=force_icons,
            force_update=force_update,
        )

    if last_page:
        return sync.run_sync_last_page(
            force_icons=force_icons,
            force_update=force_update,
        )

    return sync.run_sync(
        force_icons=force_icons,
        max_workers=workers,
        force_update=force_update,
        status_filter=status_filter,
        update_only=update_only,
        created_after=created_after,
        dry_run=dry_run,
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
        parser.add_argument('--workers', type=int, default=3, metavar='N',
                           help='Number of parallel workers (default: 3, max recommended: 5)')
        parser.add_argument('--last-page', action='store_true',
                           help='Sync only the most recently edited page (useful for iOS shortcuts)')
        parser.add_argument('--force-update', action='store_true',
                           help='Force update pages even if already synced')
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
            result = sync.run_sync_last_page(force_icons=args.force_icons, force_update=args.force_update)
        else:
            result = sync.run_sync(force_icons=args.force_icons, max_workers=args.workers, force_update=args.force_update)
        
        if result['success']:
            logger.info("Synchronization completed successfully")
            logger.info(f"Updated: {result['successful_updates']} pages")
            logger.info(f"Failed: {result['failed_updates']} pages")
            if args.last_page:
                logger.info("Last page mode completed - only the most recently edited page was processed")
            if args.force_icons:
                logger.info("Force icons mode completed - all page icons have been updated")
            if args.force_update:
                logger.info("Force update mode completed - all pages processed including completed content")
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
