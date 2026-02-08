#!/usr/bin/env python3
"""
Notion IGDb Sync Script
Synchronizes video game information from IGDb to Notion database pages.
"""

import os
import sys
import logging
import time
import concurrent.futures
from typing import Dict, List, Optional, Union
from datetime import datetime
import requests

from shared.change_detection import has_property_changes
from shared.logging_config import get_logger, setup_logging
from shared.notion_api import NotionAPI
from shared.utils import (
    build_multi_select_options,
    get_database_id,
    get_notion_token,
    normalize_id,
)

setup_logging('notion_igdb_sync.log')
logger = get_logger(__name__)

# Try to import custom property configuration
try:
    from syncs.games.property_config import (
        TITLE_PROPERTY_ID, DESCRIPTION_PROPERTY_ID,
        RELEASE_DATE_PROPERTY_ID, RATING_PROPERTY_ID, RATING_COUNT_PROPERTY_ID,
        PLAYTIME_PROPERTY_ID, GENRES_PROPERTY_ID, PLATFORMS_PROPERTY_ID,
        PLATFORM_FAMILY_PROPERTY_ID, PLATFORM_TYPE_PROPERTY_ID,
        STATUS_PROPERTY_ID, IGDB_ID_PROPERTY_ID, COVER_IMAGE_PROPERTY_ID,
        LAST_UPDATED_PROPERTY_ID, DEVELOPERS_PROPERTY_ID, PUBLISHERS_PROPERTY_ID,
        FRANCHISE_PROPERTY_ID, COLLECTIONS_PROPERTY_ID, GAME_MODES_PROPERTY_ID, GAME_STATUS_PROPERTY_ID, 
        GAME_TYPE_PROPERTY_ID, MULTIPLAYER_MODES_PROPERTY_ID, THEMES_PROPERTY_ID, 
        WEBSITE_PROPERTY_ID, HOMEPAGE_PROPERTY_ID, FIELD_BEHAVIOR,
        OFFLINE_PLAYERS_PROPERTY_ID, ONLINE_PLAYERS_PROPERTY_ID, OFFLINE_COOP_PLAYERS_PROPERTY_ID, 
        ONLINE_COOP_PLAYERS_PROPERTY_ID
    )
except ImportError:
    logger.error("property_config.py not found. Please create this file with your property IDs.")
    logger.error("Copy property_config.example.py to property_config.py and update with your property IDs.")
    sys.exit(1)

class IGDbAPI:
    """IGDb API client for fetching video game data."""
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.igdb.com/v4"
        self.session = requests.Session()
        self.access_token = None
        self.token_expires_at = 0
        
        # Optimized rate limiting - adaptive based on API response
        self.request_delay = 0.8  # Aggressive but safe starting delay
        self.last_request_time = 0
        self.consecutive_successes = 0
        self.adaptive_delay = 0.8
        
        # Comprehensive caching to reduce API calls
        self._cache = {
            'genres': {},
            'platforms': {},
            'franchises': {},
            'collections': {},
            'game_modes': {},
            'themes': {},
            'companies': {},
            'multiplayer_modes': {},
            'game_details': {},  # Cache full game details
            'playtime': {},      # Cache playtime data
            'covers': {}         # Cache cover URLs
        }
        
        # Request deduplication
        self._pending_requests = set()
        
        # Get initial access token
        self._get_access_token()
    
    def _rate_limit(self):
        """Adaptive rate limiting that adjusts based on API response."""
        import time
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        # Use adaptive delay
        delay = self.adaptive_delay
        
        if time_since_last_request < delay:
            sleep_time = delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _adjust_rate_limit(self, success: bool):
        """Adjust rate limiting based on API response for maximum speed."""
        if success:
            self.consecutive_successes += 1
            # More aggressive delay reduction for faster processing
            if self.consecutive_successes > 3 and self.adaptive_delay > 0.5:
                self.adaptive_delay = max(0.5, self.adaptive_delay - 0.1)
                logger.debug(f"Reduced adaptive delay to {self.adaptive_delay:.1f}s")
            elif self.consecutive_successes > 10 and self.adaptive_delay > 0.3:
                self.adaptive_delay = max(0.3, self.adaptive_delay - 0.05)
                logger.debug(f"Reduced adaptive delay to {self.adaptive_delay:.1f}s")
        else:
            self.consecutive_successes = 0
            # Faster recovery from failures
            self.adaptive_delay = min(2.0, self.adaptive_delay + 0.3)
            logger.debug(f"Increased adaptive delay to {self.adaptive_delay:.1f}s")
    
    def _make_api_request(self, url: str, data: str, max_retries: int = 3) -> requests.Response:
        """Make an API request with comprehensive rate limiting and retry logic."""
        import time
        
        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                # Apply rate limiting before each request
                self._rate_limit()
                
                # Make the request
                response = self.session.post(url, data=data, headers={'Content-Type': 'text/plain'})
                
                # Check for rate limiting
                if response.status_code == 429:
                    if attempt < max_retries:
                        # Exponential backoff with jitter
                        wait_time = (2 ** attempt) + (attempt * 0.1) + (time.time() % 1)  # Add jitter
                        logger.warning(f"Rate limited (429). Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        response.raise_for_status()
                
                # Check for other HTTP errors
                if response.status_code >= 400:
                    if attempt < max_retries:
                        wait_time = 1 + (attempt * 0.5)  # Shorter wait for other errors
                        logger.warning(f"HTTP {response.status_code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
                
                # Success
                self._adjust_rate_limit(True)
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = 1 + (attempt * 0.5)
                    logger.warning(f"Request failed: {e}. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed after {max_retries} retries: {e}")
                    self._adjust_rate_limit(False)
                    raise
        
        # This should never be reached, but just in case
        raise Exception("Max retries exceeded")
    
    def _get_access_token(self):
        """Get OAuth2 access token from Twitch."""
        try:
            response = requests.post(
                "https://id.twitch.tv/oauth2/token",
                params={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'grant_type': 'client_credentials'
                }
            )
            response.raise_for_status()
            data = response.json()
            
            self.access_token = data['access_token']
            self.token_expires_at = time.time() + data['expires_in'] - 60  # 60 second buffer
            
            # Update session headers
            self.session.headers.update({
                'Client-ID': self.client_id,
                'Authorization': f'Bearer {self.access_token}'
            })
            
            logger.info("Successfully obtained IGDb access token")
            
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            raise
    
    def _ensure_valid_token(self):
        """Ensure we have a valid access token."""
        if time.time() >= self.token_expires_at:
            logger.info("Access token expired, refreshing...")
            self._get_access_token()
    
    def search_games(self, title: str) -> Optional[Dict]:
        """Search for games by title with smart selection and fuzzy matching fallback."""
        try:
            self._ensure_valid_token()
            self._rate_limit()  # Rate limiting
            
            # First try exact search
            result = self._search_games_exact(title)
            if result:
                return result
            
            # If no results, try fuzzy matching
            logger.info(f"No exact results for '{title}', trying fuzzy matching...")
            result = self._search_games_fuzzy(title)
            if result:
                logger.info(f"Fuzzy match found: {result['name']} (ID: {result['id']})")
                return result
            
            logger.warning(f"No results found for '{title}' (exact or fuzzy)")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for game '{title}': {e}")
            return None
    
    def _search_games_exact(self, title: str) -> Optional[Dict]:
        """Search for games with exact title matching."""
        search_query = f"""
        search "{title}";
        fields id,name,summary,first_release_date,aggregated_rating,rating_count,
               genres,platforms,game_status,cover,franchises,collections,game_modes,
               category,multiplayer_modes,themes,url,involved_companies,version_parent;
        limit 20;
        """
        
        response = self._make_api_request(f"{self.base_url}/games", search_query)
        results = response.json()
        
        if not results:
            return None
        
        # Smart game selection algorithm
        best_game = self._select_best_game(results, title)
        if best_game:
            logger.info(f"Selected best match: {best_game['name']} (ID: {best_game['id']})")
            return best_game
        
        # Fallback to first result if no good match found
        logger.info(f"Using fallback result: {results[0]['name']} (ID: {results[0]['id']})")
        return results[0]
    
    def _search_games_fuzzy(self, title: str) -> Optional[Dict]:
        """Search for games using fuzzy matching with common title variations."""
        # Generate fuzzy search variations
        fuzzy_variations = self._generate_fuzzy_variations(title)
        
        for variation in fuzzy_variations:
            logger.debug(f"Trying fuzzy variation: '{variation}'")
            
            search_query = f"""
            search "{variation}";
            fields id,name,summary,first_release_date,aggregated_rating,rating_count,
                   genres,platforms,game_status,cover,franchises,collections,game_modes,
                   category,multiplayer_modes,themes,url,involved_companies,version_parent;
            limit 10;
            """
            
            try:
                response = self._make_api_request(f"{self.base_url}/games", search_query)
                results = response.json()
                
                if results:
                    # Use smart selection for fuzzy results too
                    best_game = self._select_best_game(results, title)
                    if best_game:
                        return best_game
                        
            except Exception as e:
                logger.debug(f"Fuzzy search failed for '{variation}': {e}")
                continue
        
        return None
    
    def _generate_fuzzy_variations(self, title: str) -> List[str]:
        """Generate common title variations for fuzzy matching."""
        variations = []
        
        # Original title
        variations.append(title)
        
        # Common character replacements
        replacements = {
            'Spiderman': 'Spider-Man',
            'Spiderman': 'Spider Man',
            'Spider-Man': 'Spiderman',
            'Spider Man': 'Spiderman',
            'Marvel\'s': 'Marvel',
            'Marvel': 'Marvel\'s',
            'The ': '',
            ': ': ' ',
            ' - ': ' ',
            '&': 'and',
            'and': '&'
        }
        
        # Apply replacements
        for old, new in replacements.items():
            if old in title:
                variations.append(title.replace(old, new))
        
        # Remove common words and try partial matches
        words_to_remove = ['The', 'A', 'An', 'Marvel\'s', 'Marvel']
        for word in words_to_remove:
            if title.startswith(word + ' '):
                variations.append(title[len(word) + 1:])
        
        # Try without colons and special characters
        import re
        clean_title = re.sub(r'[:;,\-]', ' ', title)
        variations.append(clean_title)
        
        # Try with different punctuation
        variations.append(title.replace(':', ''))
        variations.append(title.replace('-', ' '))
        
        # Remove duplicates and empty strings
        variations = list(dict.fromkeys([v.strip() for v in variations if v.strip()]))
        
        logger.debug(f"Generated {len(variations)} fuzzy variations for '{title}'")
        return variations
    
    def _select_best_game(self, games: List[Dict], search_title: str) -> Optional[Dict]:
        """Select the best game from search results using smart ranking."""
        if not games:
            return None
        
        # Filter to main game entries (not DLC, expansions, etc.)
        main_games = []
        for game in games:
            category = game.get('category')
            # Keep main games (0) and remakes (8), filter out DLC (1), expansions (2), etc.
            if category in [0, 8] or category is None:
                main_games.append(game)
        
        # If no main games found, use all games
        if not main_games:
            main_games = games
        
        # Score each game based on multiple criteria
        scored_games = []
        for game in main_games:
            score = self._calculate_game_score(game, search_title)
            scored_games.append((score, game))
        
        # Sort by score (highest first)
        scored_games.sort(key=lambda x: x[0], reverse=True)
        
        # Return the highest scoring game
        if scored_games:
            return scored_games[0][1]
        
        return None
    
    def _calculate_game_score(self, game: Dict, search_title: str) -> float:
        """Calculate a score for game selection based on multiple criteria."""
        score = 0.0
        
        # 1. Exact title match (highest priority)
        game_name = game.get('name', '').lower()
        search_title_lower = search_title.lower()
        if game_name == search_title_lower:
            score += 1000.0
        elif game_name.startswith(search_title_lower):
            score += 500.0
        elif search_title_lower in game_name:
            score += 200.0
        
        # 2. Rating count (popularity/engagement)
        rating_count = game.get('rating_count', 0) or 0
        score += min(rating_count / 10.0, 100.0)  # Cap at 100 points
        
        # 3. Rating quality
        rating = game.get('aggregated_rating', 0) or 0
        if rating > 0:
            score += min(rating / 2.0, 50.0)  # Cap at 50 points
        
        # 4. Penalty for bundle/collection games
        bundle_keywords = ['bundle', 'pack', 'collection', 'double pack', '+', '&', 'edition']
        game_name = game.get('name', '').lower()
        if any(keyword in game_name for keyword in bundle_keywords):
            score -= 100.0
        
        # 5. Bonus for main game category
        category = game.get('category')
        if category == 0:  # Main game
            score += 50.0
        elif category == 8:  # Remake
            score += 25.0
        
        # 6. Penalty for very old games (likely not the main version)
        release_date = game.get('first_release_date')
        if release_date:
            import time
            current_time = time.time()
            game_age_years = (current_time - release_date) / (365.25 * 24 * 3600)
            if game_age_years > 20:  # Very old games get penalty
                score -= 20.0
        
        return score
    
    def get_game_details(self, game_id: int) -> Optional[Dict]:
        """Get detailed information for a game with caching."""
        try:
            # Check cache first
            if game_id in self._cache['game_details']:
                logger.debug(f"Using cached game details for ID {game_id}")
                return self._cache['game_details'][game_id]
            
            self._ensure_valid_token()
            self._rate_limit()  # Rate limiting
            
            # IGDb detailed query
            query = f"""
            fields id,name,summary,first_release_date,aggregated_rating,rating_count,
                   genres,platforms,game_status,cover,franchises,collections,game_modes,
                   category,multiplayer_modes,themes,url,involved_companies;
            where id = {game_id};
            """
            
            response = self._make_api_request(f"{self.base_url}/games", query)
            results = response.json()
            
            if results:
                game_data = results[0]
                # Cache the result
                self._cache['game_details'][game_id] = game_data
                return game_data
            return None
            
        except Exception as e:
            logger.error(f"Error getting game details for ID {game_id}: {e}")
            return None
    
    def get_cover_url(self, cover_id: int) -> Optional[str]:
        """Get cover image URL from cover ID with caching."""
        try:
            # Check cache first
            if cover_id in self._cache['covers']:
                logger.debug(f"Using cached cover URL for ID {cover_id}")
                return self._cache['covers'][cover_id]
            
            self._ensure_valid_token()
            
            query = f"""
            fields url;
            where id = {cover_id};
            """
            
            response = self._make_api_request(f"{self.base_url}/covers", query)
            results = response.json()
            
            if results and results[0].get('url'):
                # IGDb returns relative URLs, need to prepend https://images.igdb.com/igdb/image/
                url = results[0]['url']
                
                # Convert to high-quality original size instead of thumbnail
                if '/t_thumb/' in url:
                    url = url.replace('/t_thumb/', '/t_original/')
                elif '/t_cover_big/' in url:
                    url = url.replace('/t_cover_big/', '/t_original/')
                elif '/t_cover_big_2x/' in url:
                    url = url.replace('/t_cover_big_2x/', '/t_original/')
                
                if url.startswith('//'):
                    url = f"https:{url}"
                elif url.startswith('/'):
                    url = f"https://images.igdb.com/igdb/image{url}"
                else:
                    url = f"https://images.igdb.com/igdb/image/{url}"
                
                # Cache the result
                self._cache['covers'][cover_id] = url
                return url
            return None
            
        except Exception as e:
            logger.error(f"Error getting cover URL for ID {cover_id}: {e}")
            return None
    
    def get_genre_names(self, genre_ids: List[int]) -> List[str]:
        """Get genre names from genre IDs."""
        if not genre_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            # Check cache first
            cache_key = tuple(sorted(genre_ids))
            if cache_key in self._cache['genres']:
                return self._cache['genres'][cache_key]
            
            ids_str = ','.join(map(str, genre_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/genres", query)
            results = response.json()
            
            genre_names = [genre['name'] for genre in results]
            
            # Cache the result
            self._cache['genres'][cache_key] = genre_names
            
            return genre_names
            
        except Exception as e:
            logger.error(f"Error getting genre names: {e}")
            return []
    
    def get_platform_names(self, platform_ids: List[int]) -> List[str]:
        """Get platform names from platform IDs."""
        if not platform_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, platform_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/platforms", query)
            results = response.json()
            
            return [platform['name'] for platform in results]
            
        except Exception as e:
            logger.error(f"Error getting platform names: {e}")
            return []
    
    def get_platform_family_names(self, platform_ids: List[int]) -> List[str]:
        """Get platform family names from platform IDs."""
        if not platform_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, platform_ids))
            query = f"""
            fields platform_family;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/platforms", query)
            platforms = response.json()
            
            # Extract unique platform family IDs
            family_ids = set()
            for platform in platforms:
                if platform.get('platform_family'):
                    family_ids.add(platform['platform_family'])
            
            if not family_ids:
                return []
            
            # Get family names
            family_ids_str = ','.join(map(str, family_ids))
            family_query = f"""
            fields name;
            where id = ({family_ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/platform_families", family_query)
            families = response.json()
            
            return [family['name'] for family in families]
            
        except Exception as e:
            logger.error(f"Error getting platform family names: {e}")
            return []
    
    def get_platform_type_names(self, platform_ids: List[int]) -> List[str]:
        """Get platform type names from platform IDs."""
        if not platform_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, platform_ids))
            query = f"""
            fields platform_type;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/platforms", query)
            platforms = response.json()
            
            # Extract unique platform type IDs
            type_ids = set()
            for platform in platforms:
                if platform.get('platform_type'):
                    type_ids.add(platform['platform_type'])
            
            if not type_ids:
                return []
            
            # Map platform type IDs to names (since platform_types endpoint seems to have issues)
            type_mapping = {
                1: "Console",
                2: "Arcade", 
                3: "Platform",
                4: "Computer",
                5: "Operating System",
                6: "Portable Console"
            }
            
            type_names = []
            for type_id in type_ids:
                if type_id in type_mapping:
                    type_names.append(type_mapping[type_id])
            
            return type_names
            
        except Exception as e:
            logger.error(f"Error getting platform type names: {e}")
            return []
    
    
    def get_franchise_names(self, franchise_ids: List[int]) -> List[str]:
        """Get franchise names from franchise IDs."""
        if not franchise_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, franchise_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/franchises", query)
            results = response.json()
            
            return [franchise['name'] for franchise in results]
            
        except Exception as e:
            logger.error(f"Error getting franchise names: {e}")
            return []
    
    def get_collection_names(self, collection_ids: List[int]) -> List[str]:
        """Get collection names from collection IDs."""
        if not collection_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, collection_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/collections", query)
            results = response.json()
            
            return [collection['name'] for collection in results]
            
        except Exception as e:
            logger.error(f"Error getting collection names: {e}")
            return []
    
    def get_game_mode_names(self, mode_ids: List[int]) -> List[str]:
        """Get game mode names from mode IDs."""
        if not mode_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, mode_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/game_modes", query)
            results = response.json()
            
            return [mode['name'] for mode in results]
            
        except Exception as e:
            logger.error(f"Error getting game mode names: {e}")
            return []
    
    def get_theme_names(self, theme_ids: List[int]) -> List[str]:
        """Get theme names from theme IDs."""
        if not theme_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, theme_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/themes", query)
            results = response.json()
            
            return [theme['name'] for theme in results]
            
        except Exception as e:
            logger.error(f"Error getting theme names: {e}")
            return []
    
    def get_involved_companies_details(self, involved_company_ids: List[int]) -> Dict[str, List[str]]:
        """Get involved companies details including developer and publisher information."""
        if not involved_company_ids:
            return {'developers': [], 'publishers': []}
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, involved_company_ids))
            query = f"""
            fields company,developer,publisher;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/involved_companies", query)
            results = response.json()
            
            # Get company names for the involved companies
            company_ids = [result['company'] for result in results if result.get('company')]
            company_names = self.get_company_names(company_ids)
            
            # Create mapping from company ID to name
            company_id_to_name = {}
            for i, company_id in enumerate(company_ids):
                if i < len(company_names):
                    company_id_to_name[company_id] = company_names[i]
            
            # Separate developers and publishers
            developers = []
            publishers = []
            
            for result in results:
                company_id = result.get('company')
                if company_id and company_id in company_id_to_name:
                    company_name = company_id_to_name[company_id]
                    
                    if result.get('developer', False):
                        developers.append(company_name)
                    if result.get('publisher', False):
                        publishers.append(company_name)
            
            return {
                'developers': list(set(developers)),  # Remove duplicates
                'publishers': list(set(publishers))   # Remove duplicates
            }
            
        except Exception as e:
            logger.error(f"Error getting involved companies details: {e}")
            return {'developers': [], 'publishers': []}
    
    def get_multiplayer_mode_names(self, mode_ids: List[int]) -> List[str]:
        """Get multiplayer mode names from mode IDs."""
        if not mode_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, mode_ids))
            query = f"""
            fields *;
            where id = ({ids_str});
            """
            response = self._make_api_request(f"{self.base_url}/multiplayer_modes", query)
            results = response.json()
            
            # Convert boolean flags and numeric values to readable names dynamically
            multiplayer_features = []
            
            # Define field mappings for better readability
            field_mappings = {
                'campaigncoop': 'Campaign Co-op',
                'dropin': 'Drop-in',
                'lancoop': 'LAN Co-op',
                'offlinecoop': 'Offline Co-op',
                'onlinecoop': 'Online Co-op',
                'splitscreen': 'Split Screen'
            }
            
            # Define max player field mappings
            max_player_mappings = {
                'offlinemax': 'Offline Max',
                'offlinecoopmax': 'Offline Co-op Max',
                'onlinemax': 'Online Max',
                'onlinecoopmax': 'Online Co-op Max'
            }
            
            for mode in results:
                features = []
                
                # Process boolean fields dynamically
                for field, display_name in field_mappings.items():
                    if mode.get(field, False):
                        features.append(display_name)
                
                # Remove the extra co-op detection logic - trust IGDb's boolean flags
                
                # Skip max player fields - these are now handled separately
                # for field, display_name in max_player_mappings.items():
                #     value = mode.get(field, 0)
                #     if value and value > 0:
                #         features.append(f'{display_name}: {value} players')
                
                # Handle any unknown boolean fields dynamically
                for field, value in mode.items():
                    if (field not in field_mappings and 
                        field not in max_player_mappings and 
                        field not in ['id', 'game', 'platform', 'checksum'] and
                        isinstance(value, bool) and value):
                        # Convert field name to readable format
                        readable_name = field.replace('_', ' ').replace('coop', 'Co-op').title()
                        features.append(readable_name)
                
                # Skip unknown numeric fields with 'max' - these are now handled separately
                # for field, value in mode.items():
                #     if (field not in field_mappings and 
                #         field not in max_player_mappings and 
                #         field not in ['id', 'game', 'platform', 'checksum'] and
                #         isinstance(value, int) and value > 0 and 'max' in field.lower()):
                #         # Convert field name to readable format
                #         readable_name = field.replace('_', ' ').replace('coop', 'Co-op').title()
                #         features.append(f'{readable_name}: {value} players')
                
                if features:
                    multiplayer_features.extend(features)
            
            # Remove duplicates and return
            return list(set(multiplayer_features))
            
        except Exception as e:
            logger.error(f"Error getting multiplayer mode names: {e}")
            return []
    
    def get_multiplayer_player_counts(self, mode_ids: List[int]) -> Dict[str, int]:
        """Get player counts from multiplayer mode IDs."""
        if not mode_ids:
            return {}
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, mode_ids))
            query = f"""
            fields *;
            where id = ({ids_str});
            """
            response = self._make_api_request(f"{self.base_url}/multiplayer_modes", query)
            results = response.json()
            
            # Extract max values for each player count type
            player_counts = {
                'offline_max': 0,
                'online_max': 0,
                'offline_coop_max': 0,
                'online_coop_max': 0
            }
            
            for mode in results:
                player_counts['offline_max'] = max(player_counts['offline_max'], mode.get('offlinemax', 0))
                player_counts['online_max'] = max(player_counts['online_max'], mode.get('onlinemax', 0))
                player_counts['offline_coop_max'] = max(player_counts['offline_coop_max'], mode.get('offlinecoopmax', 0))
                player_counts['online_coop_max'] = max(player_counts['online_coop_max'], mode.get('onlinecoopmax', 0))
            
            # Return only non-zero values
            return {k: v for k, v in player_counts.items() if v > 0}
            
        except Exception as e:
            logger.error(f"Error getting multiplayer player counts: {e}")
            return {}
    
    def get_game_playtime(self, game_id: int) -> Optional[int]:
        """Get normal playtime in hours from game_time_to_beats endpoint with caching."""
        try:
            # Check cache first
            if game_id in self._cache['playtime']:
                logger.debug(f"Using cached playtime for game ID {game_id}")
                return self._cache['playtime'][game_id]
            
            self._ensure_valid_token()
            self._rate_limit()  # Rate limiting
            
            query = f"""
            fields normally;
            where game_id = {game_id};
            """
            
            response = self._make_api_request(f"{self.base_url}/game_time_to_beats", query)
            results = response.json()
            
            playtime_hours = None
            if results and results[0].get('normally'):
                # Convert seconds to hours
                playtime_seconds = results[0]['normally']
                playtime_hours = round(playtime_seconds / 3600, 1)
            
            # Cache the result (even if None)
            self._cache['playtime'][game_id] = playtime_hours
            return playtime_hours
            
        except Exception as e:
            logger.error(f"Error getting playtime for game {game_id}: {e}")
            return None
    
    def get_company_names(self, company_ids: List[int]) -> List[str]:
        """Get company names from company IDs."""
        if not company_ids:
            return []
        
        try:
            self._ensure_valid_token()
            
            ids_str = ','.join(map(str, company_ids))
            query = f"""
            fields name;
            where id = ({ids_str});
            """
            
            response = self._make_api_request(f"{self.base_url}/companies", query)
            results = response.json()
            
            return [company['name'] for company in results]
            
        except Exception as e:
            logger.error(f"Error getting company names: {e}")
            return []


class NotionIGDbSync:
    """Main class for synchronizing Notion database with IGDb data."""
    
    def __init__(self, notion_token: str, igdb_client_id: str, igdb_client_secret: str, database_id: str):
        self.notion = NotionAPI(notion_token)
        self.igdb = IGDbAPI(igdb_client_id, igdb_client_secret)
        self.database_id = database_id
        
        # Rate limiting - optimized for maximum speed
        self.request_delay = 0.8  # Aggressive delay for maximum performance
        
        # Property mapping - will be populated from database schema
        self.property_mapping = {}
        self.property_id_to_key: Dict[str, str] = {}
        self.last_updated_property_key: Optional[str] = None
        
        # Field behavior configuration
        self.field_behavior = FIELD_BEHAVIOR
        
        self._load_database_schema()
    
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
                
                # Core properties
                'description_property_id': DESCRIPTION_PROPERTY_ID,
                'release_date_property_id': RELEASE_DATE_PROPERTY_ID,
                'rating_property_id': RATING_PROPERTY_ID,
                'rating_count_property_id': RATING_COUNT_PROPERTY_ID,
                'playtime_property_id': PLAYTIME_PROPERTY_ID,
                'genres_property_id': GENRES_PROPERTY_ID,
                'platforms_property_id': PLATFORMS_PROPERTY_ID,
                'platform_family_property_id': PLATFORM_FAMILY_PROPERTY_ID,
                'platform_type_property_id': PLATFORM_TYPE_PROPERTY_ID,
                'status_property_id': STATUS_PROPERTY_ID,
                'igdb_id_property_id': IGDB_ID_PROPERTY_ID,
                'cover_image_property_id': COVER_IMAGE_PROPERTY_ID,
                'last_updated_property_id': LAST_UPDATED_PROPERTY_ID,
                
                # Extended properties
                'developers_property_id': DEVELOPERS_PROPERTY_ID,
                'publishers_property_id': PUBLISHERS_PROPERTY_ID,
                'franchise_property_id': FRANCHISE_PROPERTY_ID,
                'collections_property_id': COLLECTIONS_PROPERTY_ID,
                'game_modes_property_id': GAME_MODES_PROPERTY_ID,
                'game_status_property_id': GAME_STATUS_PROPERTY_ID,
                'game_type_property_id': GAME_TYPE_PROPERTY_ID,
                'multiplayer_modes_property_id': MULTIPLAYER_MODES_PROPERTY_ID,
                'themes_property_id': THEMES_PROPERTY_ID,
                'website_property_id': WEBSITE_PROPERTY_ID,
                'homepage_property_id': HOMEPAGE_PROPERTY_ID,
                
                # Player count properties
                'offline_players_property_id': OFFLINE_PLAYERS_PROPERTY_ID,
                'online_players_property_id': ONLINE_PLAYERS_PROPERTY_ID,
                'offline_coop_players_property_id': OFFLINE_COOP_PLAYERS_PROPERTY_ID,
                'online_coop_players_property_id': ONLINE_COOP_PLAYERS_PROPERTY_ID,
            }
            
            # Log the property mapping
            for prop_key, prop_id in self.property_mapping.items():
                if prop_id is not None:
                    property_key = self.property_id_to_key.get(prop_id, "NOT_FOUND")
                    logger.info(f"✓ {prop_key}: {prop_id} -> {property_key}")
                else:
                    logger.info(f"⏭️  {prop_key}: NOT CONFIGURED (skipped)")

            self.last_updated_property_key = self._get_property_key(
                self.property_mapping.get('last_updated_property_id')
            )
            
            # Validate required properties
            if not self.property_mapping['title_property_id']:
                logger.error("❌ No title property found - this is required!")
                
        except Exception as e:
            logger.error(f"Error loading database schema: {e}")
            self.property_mapping = {}
    
    def get_notion_pages(self) -> List[Dict]:
        """Get all pages from the Notion database."""
        logger.info(f"Fetching pages from database {self.database_id}")
        return self.notion.query_database(self.database_id)
    
    def extract_title(self, page: Dict) -> Optional[str]:
        """Extract title from a Notion page."""
        try:
            properties = page.get('properties', {})
            
            # Get title using mapped property ID
            title = None
            if self.property_mapping['title_property_id']:
                property_key = self.property_id_to_key.get(self.property_mapping['title_property_id'])
                if property_key:
                    title_prop = properties.get(property_key)
                    if title_prop and title_prop.get('type') == 'title' and title_prop.get('title'):
                        title = title_prop['title'][0]['plain_text']
            
            if title:
                return title
            else:
                logger.warning(f"Missing title for page {page.get('id')}")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting data from page {page.get('id')}: {e}")
            return None
    
    def sync_page(self, page: Dict, force_icons: bool = False, force_update: bool = False) -> Optional[bool]:
        """Sync a single page with IGDb data with intelligent skip logic."""
        try:
            page_id = page['id']
            title = self.extract_title(page)
            
            if not title:
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            logger.info(f"Processing: {title}")
            
            # Check if page already has an IGDb ID
            existing_igdb_id = None
            if self.property_mapping['igdb_id_property_id']:
                # Use property key (dynamically mapped from property ID - robust against renames)
                property_key = self._get_property_key(self.property_mapping['igdb_id_property_id'])
                if property_key:
                    igdb_property = page['properties'].get(property_key)
                    if igdb_property and igdb_property.get('number'):
                        existing_igdb_id = igdb_property['number']
                        logger.info(f"Found existing IGDb ID: {existing_igdb_id}")
            
            # Intelligent skip logic - check if page is already up to date
            if existing_igdb_id and not force_update:
                logger.info(f"🔍 Checking if {title} (IGDb ID: {existing_igdb_id}) needs updating...")
                if self._is_page_up_to_date(page, existing_igdb_id):
                    logger.info(f"⏭️  Skipping {title} - already up to date")
                    return None
                else:
                    logger.info(f"📝 {title} needs updating - proceeding with sync")
            
            # Use existing IGDb ID if available, otherwise search by title
            if existing_igdb_id:
                game_data = self.igdb.get_game_details(existing_igdb_id)
                if not game_data:
                    logger.warning(f"Could not find game with IGDb ID: {existing_igdb_id}, falling back to title search")
                    game_data = self.igdb.search_games(title)
            else:
                # Search IGDb for the game by title
                game_data = self.igdb.search_games(title)
            
            if not game_data:
                logger.warning(f"Could not find game: {title}")
                return False
            
            # Get additional details if we have an ID (for title searches)
            if game_data.get('id') and not existing_igdb_id:
                detailed_data = self.igdb.get_game_details(game_data['id'])
                if detailed_data:
                    game_data = detailed_data
            
            # Format properties for Notion
            properties = self.format_notion_properties(game_data)
            
            page_properties = page.get('properties', {})
            has_changes, change_details = has_property_changes(
                page_properties,
                properties,
                self.last_updated_property_key,
            )
            
            if not has_changes:
                logger.info(f"No changes detected for: {title} - skipping update")
                return None  # Return None to indicate skipped
            
            if change_details:
                logger.info(f"Changes detected: {', '.join(change_details)}")
            
            # Add last_updated timestamp since we have changes
            if self.property_mapping['last_updated_property_id']:
                property_key = self._get_property_key(self.property_mapping['last_updated_property_id'])
                if property_key:
                    properties[property_key] = {
                        'date': {'start': datetime.now().isoformat()}
                    }
            
            # Get cover URL if available
            cover_url = None
            if game_data.get('cover'):
                cover_url = self.igdb.get_cover_url(game_data['cover'])
            
            # Set game controller icon
            icon = '🎮'  # Game controller emoji
            
            # Update the page
            if self.notion.update_page(page_id, properties, cover_url, icon):
                logger.info(f"Successfully updated: {title}")
                return True
            else:
                logger.error(f"Failed to update: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing page {page.get('id')}: {e}")
            return False
    
    def format_notion_properties(self, game_data: Dict) -> Dict:
        """Format IGDb data for Notion properties."""
        properties = {}
        
        try:
            # Title (skip updating - use existing title from Notion)
            # if game_data.get('name') and self.property_mapping['title_property_id']:
            #     property_key = self._get_property_key(self.property_mapping['title_property_id'])
            #     if property_key:
            #         properties[property_key] = {
            #             'title': [{'text': {'content': game_data['name']}}]
            #         }
            
            # Description/Summary
            if game_data.get('summary') and self.property_mapping['description_property_id']:
                property_key = self._get_property_key(self.property_mapping['description_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': game_data['summary']}
                        }]
                    }
            
            # Release Date
            if game_data.get('first_release_date') and self.property_mapping['release_date_property_id']:
                # Convert Unix timestamp to ISO date
                release_date = datetime.fromtimestamp(game_data['first_release_date']).strftime('%Y-%m-%d')
                property_key = self._get_property_key(self.property_mapping['release_date_property_id'])
                if property_key:
                    properties[property_key] = {
                        'date': {
                            'start': release_date,
                            'end': None,
                            'time_zone': None
                        }
                    }
            
            # Rating (using aggregated_rating from IGDb)
            if game_data.get('aggregated_rating') and self.property_mapping['rating_property_id']:
                property_key = self._get_property_key(self.property_mapping['rating_property_id'])
                if property_key:
                    properties[property_key] = {
                        'id': self.property_mapping['rating_property_id'],
                        'number': game_data['aggregated_rating'] / 100.0  # Convert from 0-100 to 0-1 scale
                    }
            
            # Rating Count
            if game_data.get('rating_count') and self.property_mapping['rating_count_property_id']:
                property_key = self._get_property_key(self.property_mapping['rating_count_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': game_data['rating_count']
                    }
            
            # IGDb ID
            if game_data.get('id') and self.property_mapping['igdb_id_property_id']:
                property_key = self._get_property_key(self.property_mapping['igdb_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'id': self.property_mapping['igdb_id_property_id'],
                        'number': game_data['id']
                    }
            
            # Playtime (using game_time_to_beats.normally)
            if game_data.get('id') and self.property_mapping['playtime_property_id']:
                playtime_hours = self.igdb.get_game_playtime(game_data['id'])
                if playtime_hours:
                    property_key = self._get_property_key(self.property_mapping['playtime_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'id': self.property_mapping['playtime_property_id'],
                            'number': playtime_hours
                        }
            
            
            # Extended properties
            self._format_extended_properties(game_data, properties)
            
        except Exception as e:
            logger.error(f"Error formatting properties: {e}")
        
        return properties
    
    def _format_extended_properties(self, game_data: Dict, properties: Dict):
        """Format extended IGDb properties."""
        try:
            # Genres
            if game_data.get('genres') and self.property_mapping['genres_property_id']:
                genre_names = self.igdb.get_genre_names(game_data['genres'])
                if genre_names:
                    property_key = self._get_property_key(self.property_mapping['genres_property_id'])
                    if property_key:
                        genre_options = build_multi_select_options(genre_names, context='genres')
                        properties[property_key] = {'multi_select': genre_options}
            
            # Platforms
            if game_data.get('platforms') and self.property_mapping['platforms_property_id']:
                platform_names = self.igdb.get_platform_names(game_data['platforms'])
                if platform_names:
                    property_key = self._get_property_key(self.property_mapping['platforms_property_id'])
                    if property_key:
                        platform_options = build_multi_select_options(platform_names, context='platforms')
                        properties[property_key] = {'multi_select': platform_options}
            
            # Platform Family
            if game_data.get('platforms') and self.property_mapping['platform_family_property_id']:
                platform_family_names = self.igdb.get_platform_family_names(game_data['platforms'])
                if platform_family_names:
                    property_key = self._get_property_key(self.property_mapping['platform_family_property_id'])
                    if property_key:
                        family_options = build_multi_select_options(platform_family_names, context='platform_family')
                        properties[property_key] = {'multi_select': family_options}
            
            # Platform Type
            if game_data.get('platforms') and self.property_mapping['platform_type_property_id']:
                platform_type_names = self.igdb.get_platform_type_names(game_data['platforms'])
                if platform_type_names:
                    property_key = self._get_property_key(self.property_mapping['platform_type_property_id'])
                    if property_key:
                        type_options = build_multi_select_options(platform_type_names, context='platform_type')
                        properties[property_key] = {'multi_select': type_options}
            
            # Developers and Publishers (using involved_companies)
            if game_data.get('involved_companies') and (self.property_mapping['developers_property_id'] or self.property_mapping['publishers_property_id']):
                companies_data = self.igdb.get_involved_companies_details(game_data['involved_companies'])
                
                # Developers
                if companies_data['developers'] and self.property_mapping['developers_property_id']:
                    property_key = self._get_property_key(self.property_mapping['developers_property_id'])
                    if property_key:
                        developer_options = build_multi_select_options(companies_data['developers'], context='developers')
                        properties[property_key] = {'multi_select': developer_options}
                
                # Publishers
                if companies_data['publishers'] and self.property_mapping['publishers_property_id']:
                    property_key = self._get_property_key(self.property_mapping['publishers_property_id'])
                    if property_key:
                        publisher_options = build_multi_select_options(companies_data['publishers'], context='publishers')
                        properties[property_key] = {'multi_select': publisher_options}
            
            # Franchise (multi_select field)
            if game_data.get('franchises') and self.property_mapping['franchise_property_id']:
                franchise_names = self.igdb.get_franchise_names(game_data['franchises'])
                if franchise_names:
                    property_key = self._get_property_key(self.property_mapping['franchise_property_id'])
                    if property_key:
                        franchise_options = build_multi_select_options(franchise_names, context='franchises')
                        properties[property_key] = {'multi_select': franchise_options}
            
            # Collections/Series (multi_select field)
            if game_data.get('collections') and self.property_mapping['collections_property_id']:
                collection_names = self.igdb.get_collection_names(game_data['collections'])
                if collection_names:
                    property_key = self._get_property_key(self.property_mapping['collections_property_id'])
                    if property_key:
                        collection_options = build_multi_select_options(collection_names, context='collections')
                        properties[property_key] = {'multi_select': collection_options}
            
            # Game Modes
            if game_data.get('game_modes') and self.property_mapping['game_modes_property_id']:
                game_mode_names = self.igdb.get_game_mode_names(game_data['game_modes'])
                if game_mode_names:
                    property_key = self._get_property_key(self.property_mapping['game_modes_property_id'])
                    if property_key:
                        mode_options = build_multi_select_options(game_mode_names, context='game_modes')
                        properties[property_key] = {'multi_select': mode_options}
            
            # Game Status
            if self.property_mapping['game_status_property_id']:
                status_value = None
                
                # Check if game has a formal status
                if game_data.get('game_status') is not None:
                    # Map IGDb status ID to name
                    status_map = {
                        0: 'Released',
                        2: 'Alpha',
                        3: 'Beta',
                        4: 'Early Access',
                        5: 'Offline',
                        6: 'Cancelled',
                        7: 'Rumored',
                        8: 'Delisted'
                    }
                    status_value = status_map.get(game_data['game_status'], f'Unknown ({game_data["game_status"]})')
                else:
                    # No formal status - check release date to determine status
                    release_date = game_data.get('first_release_date')
                    if release_date:
                        from datetime import datetime
                        current_time = datetime.now().timestamp()
                        if release_date > current_time:
                            status_value = 'Announced'
                        else:
                            # Past release date with no formal status = likely Released
                            status_value = 'Released'
                
                if status_value:
                    property_key = self._get_property_key(self.property_mapping['game_status_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'status': {'name': status_value}
                        }
            
            # Game Type/Category (multi-select field)
            if game_data.get('category') is not None and self.property_mapping['game_type_property_id']:
                # Map category ID to readable name
                category_map = {
                    0: 'Main Game',
                    1: 'DLC Add-on',
                    2: 'Expansion',
                    3: 'Bundle',
                    4: 'Standalone Expansion',
                    5: 'Mod',
                    6: 'Episode',
                    7: 'Season',
                    8: 'Remake',
                    9: 'Remaster',
                    10: 'Expanded Game',
                    11: 'Port',
                    12: 'Fork',
                    13: 'Pack',
                    14: 'Update'
                }
                
                category_id = game_data['category']
                category_name = category_map.get(category_id, f'Unknown ({category_id})')
                
                property_key = self._get_property_key(self.property_mapping['game_type_property_id'])
                if property_key:
                    category_options = build_multi_select_options([category_name], context='category')
                    properties[property_key] = {'multi_select': category_options}
            
            # Multiplayer Modes (clean feature list only)
            if game_data.get('multiplayer_modes') and self.property_mapping['multiplayer_modes_property_id']:
                multiplayer_mode_names = self.igdb.get_multiplayer_mode_names(game_data['multiplayer_modes'])
                if multiplayer_mode_names:
                    property_key = self._get_property_key(self.property_mapping['multiplayer_modes_property_id'])
                    if property_key:
                        multiplayer_options = build_multi_select_options(multiplayer_mode_names, context='multiplayer_modes')
                        properties[property_key] = {'multi_select': multiplayer_options}
            
            # Player Counts (separate number fields)
            if game_data.get('multiplayer_modes'):
                player_counts = self.igdb.get_multiplayer_player_counts(game_data['multiplayer_modes'])
                
                # Offline Players
                if player_counts.get('offline_max') and self.property_mapping['offline_players_property_id']:
                    property_key = self._get_property_key(self.property_mapping['offline_players_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'id': self.property_mapping['offline_players_property_id'],
                            'number': player_counts['offline_max']
                        }
                
                # Online Players
                if player_counts.get('online_max') and self.property_mapping['online_players_property_id']:
                    property_key = self._get_property_key(self.property_mapping['online_players_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'id': self.property_mapping['online_players_property_id'],
                            'number': player_counts['online_max']
                        }
                
                # Offline Co-op Players
                if player_counts.get('offline_coop_max') and self.property_mapping['offline_coop_players_property_id']:
                    property_key = self._get_property_key(self.property_mapping['offline_coop_players_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'id': self.property_mapping['offline_coop_players_property_id'],
                            'number': player_counts['offline_coop_max']
                        }
                
                # Online Co-op Players
                if player_counts.get('online_coop_max') and self.property_mapping['online_coop_players_property_id']:
                    property_key = self._get_property_key(self.property_mapping['online_coop_players_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'id': self.property_mapping['online_coop_players_property_id'],
                            'number': player_counts['online_coop_max']
                        }
            
            # Themes
            if game_data.get('themes') and self.property_mapping['themes_property_id']:
                theme_names = self.igdb.get_theme_names(game_data['themes'])
                if theme_names:
                    property_key = self._get_property_key(self.property_mapping['themes_property_id'])
                    if property_key:
                        theme_options = build_multi_select_options(theme_names, context='themes')
                        properties[property_key] = {'multi_select': theme_options}
            
            # Website URL
            if game_data.get('url') and self.property_mapping['website_property_id']:
                property_key = self._get_property_key(self.property_mapping['website_property_id'])
                if property_key:
                    properties[property_key] = {
                        'url': game_data['url']
                    }
            
            # IGDb Homepage
            if game_data.get('id') and self.property_mapping['homepage_property_id']:
                igdb_url = f"https://www.igdb.com/games/{game_data['id']}"
                property_key = self._get_property_key(self.property_mapping['homepage_property_id'])
                if property_key:
                    properties[property_key] = {
                        'url': igdb_url
                    }
            
        except Exception as e:
            logger.error(f"Error formatting extended properties: {e}")
    
    def _get_property_key(self, property_id: str) -> Optional[str]:
        """Get the property key for a given property ID."""
        return self.property_id_to_key.get(property_id)
    
    def _is_page_up_to_date(self, page: Dict, igdb_id: int) -> bool:
        """Check if a page is already up to date by comparing IGDb data with Notion data."""
        try:
            # Get current game data from IGDb
            game_data = self.igdb.get_game_details(igdb_id)
            if not game_data:
                logger.info(f"⚠️  Cannot verify if page is up to date - IGDb ID {igdb_id} not found")
                return False
            
            properties = page.get('properties', {})
            differences = []
            
            # Compare essential fields between IGDb and Notion
            essential_fields = [
                ('Release Date', 'first_release_date', 'date'),
                ('Public Rating', 'aggregated_rating', 'number'),
                ('Platforms', 'platforms', 'multi_select'),
                ('Platform Family', 'platform_family', 'multi_select'),
                ('Platform Type', 'platform_type', 'multi_select')
            ]
            
            for field_name, igdb_field, notion_type in essential_fields:
                # Convert field name to property mapping key
                field_key = field_name.lower().replace(" ", "_").replace("_", "_") + "_property_id"
                property_key = self._get_property_key(self.property_mapping.get(field_key, ''))
                if not property_key:
                    continue
                    
                field_prop = properties.get(property_key, {})
                igdb_value = game_data.get(igdb_field)
                
                # Check if field is missing or empty
                if notion_type == 'rich_text' and not field_prop.get('rich_text'):
                    if igdb_value:  # IGDb has data but Notion doesn't
                        differences.append(f"{field_name} (missing)")
                    continue
                elif notion_type == 'date' and not field_prop.get('date'):
                    if igdb_value:  # IGDb has data but Notion doesn't
                        differences.append(f"{field_name} (missing)")
                    continue
                elif notion_type == 'number' and not field_prop.get('number'):
                    if igdb_value:  # IGDb has data but Notion doesn't
                        differences.append(f"{field_name} (missing)")
                    continue
                elif notion_type == 'multi_select' and not field_prop.get('multi_select'):
                    if igdb_value:  # IGDb has data but Notion doesn't
                        differences.append(f"{field_name} (missing)")
                    continue
                
                # Compare values if both exist
                if igdb_value:
                    if notion_type == 'rich_text':
                        notion_value = field_prop.get('rich_text', [{}])[0].get('text', {}).get('content', '')
                        if notion_value != igdb_value:
                            differences.append(f"{field_name} (content differs)")
                    elif notion_type == 'date':
                        notion_date = field_prop.get('date', {}).get('start', '')
                        if notion_date:
                            # Convert IGDb timestamp to date string for comparison
                            from datetime import datetime
                            igdb_date = datetime.fromtimestamp(igdb_value).strftime('%Y-%m-%d')
                            if notion_date != igdb_date:
                                differences.append(f"{field_name} (date differs)")
                    elif notion_type == 'number':
                        notion_value = field_prop.get('number')
                        if notion_value != igdb_value:
                            differences.append(f"{field_name} (rating differs)")
                    elif notion_type == 'multi_select':
                        # For multi-select fields, check if IGDb has data but Notion doesn't
                        notion_values = field_prop.get('multi_select', [])
                        if not notion_values and igdb_value:
                            differences.append(f"{field_name} (missing IGDb data)")
                        # For platform family/type, we need to get the actual values from IGDb
                        elif field_name in ['Platform Family', 'Platform Type']:
                            # Get the actual platform family/type names from IGDb
                            if field_name == 'Platform Family':
                                igdb_names = self.igdb.get_platform_family_names(game_data.get('platforms', []))
                            else:  # Platform Type
                                igdb_names = self.igdb.get_platform_type_names(game_data.get('platforms', []))
                            
                            if igdb_names:
                                notion_names = [item.get('name', '') for item in notion_values]
                                if set(notion_names) != set(igdb_names):
                                    differences.append(f"{field_name} (values differ)")
            
            # Check if IGDb ID in Notion matches the current IGDb ID
            igdb_id_property_key = self._get_property_key(self.property_mapping.get('igdb_id_property_id', ''))
            if igdb_id_property_key:
                notion_igdb_id = properties.get(igdb_id_property_key, {}).get('number')
                if notion_igdb_id != igdb_id:
                    differences.append(f"IGDb ID (Notion: {notion_igdb_id}, Expected: {igdb_id})")
            
            if differences:
                logger.info(f"📝 Will update - differences found: {', '.join(differences)}")
                return False
            
            logger.info(f"✅ All fields match IGDb data - no updates needed")
            return True
            
        except Exception as e:
            logger.debug(f"Error checking if page is up to date: {e}")
            return False
    
    def _run_page_specific_sync(self, page_id: str, force_update: bool = False) -> Dict:
        """Run synchronization for a single explicit page."""
        logger.info(f"Page-specific mode enabled for Notion page {page_id}")
        start_time = time.time()
        
        page = self.notion.get_page(page_id)
        if not page:
            logger.error(f"Unable to retrieve Notion page {page_id}")
            return {
                'success': False,
                'message': f'Page {page_id} could not be retrieved from Notion'
            }
        
        # Validate the page belongs to the configured database
        parent = page.get('parent', {})
        parent_db_id = parent.get('database_id')
        
        # Normalize both IDs before comparison (handles dashes differences)
        normalized_parent_id = normalize_id(parent_db_id)
        normalized_configured_id = normalize_id(self.database_id)
        
        if normalized_parent_id != normalized_configured_id:
            logger.error(
                "Page %s belongs to database %s, but configured database is %s",
                page_id,
                parent_db_id,
                self.database_id
            )
            return {
                'success': False,
                'message': f'Page {page_id} does not belong to the configured database'
            }
        
        # Sync the single page
        result = self.sync_page(page, force_icons=False, force_update=force_update)
        
        # Build results dict
        results = {
            'success': True,
            'total_pages': 1,
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 0,
            'duration': time.time() - start_time
        }
        
        if result is True:
            results['successful_updates'] = 1
        elif result is False:
            results['failed_updates'] = 1
            results['success'] = False
        else:  # result is None (skipped)
            results['skipped_updates'] = 1
        
        logger.info(f"Finished page-specific sync for page {page_id}")
        return results
    
    def run_sync(self, force_icons: bool = False, force_update: bool = False, max_workers: int = 3, last_page: bool = False, page_id: Optional[str] = None) -> Dict:
        """Run the complete synchronization process."""
        # Handle page-specific sync
        if page_id:
            if last_page:
                logger.error("Cannot combine page-specific sync with last-page mode")
                return {
                    'success': False,
                    'message': 'page-id mode cannot be combined with last-page mode'
                }
            return self._run_page_specific_sync(page_id, force_update)
        
        logger.info("Starting Notion-IGDb synchronization")
        logger.info(f"Using {max_workers} parallel workers for processing")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        start_time = time.time()
        pages = self.get_notion_pages()
        
        if not pages:
            logger.warning("No pages found in database")
            return {'success': False, 'message': 'No pages found'}
        
        # Handle last-page mode
        if last_page:
            logger.info("🎮 Last-page mode: Processing only the most recently edited page")
            # Sort by last_edited_time (most recent first)
            pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
            pages = pages[:1]  # Take only the first (most recent) page
            logger.info(f"Selected page: {pages[0].get('id')}")
        
        logger.info(f"Found {len(pages)} pages to process")
        
        successful_updates = 0
        failed_updates = 0
        skipped_updates = 0
        
        # Process pages in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_page = {
                executor.submit(self.sync_page, page, force_icons, force_update): page 
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
                
                # Minimal delay to respect rate limits - optimized for speed
                time.sleep(self.request_delay / (max_workers * 2))
        
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
    igdb_client_id = os.getenv('IGDB_CLIENT_ID')
    igdb_client_secret = os.getenv('IGDB_CLIENT_SECRET')
    database_id = get_database_id('NOTION_GAMES_DATABASE_ID', 'NOTION_DATABASE_ID')
    
    if not notion_token:
        errors.append("NOTION_INTERNAL_INTEGRATION_SECRET (or legacy NOTION_TOKEN)")
    if not igdb_client_id:
        errors.append("IGDB_CLIENT_ID: Your IGDb client ID")
    if not igdb_client_secret:
        errors.append("IGDB_CLIENT_SECRET: Your IGDb client secret")
    if not database_id:
        errors.append("NOTION_GAMES_DATABASE_ID (or NOTION_DATABASE_ID) for your Notion database ID")
    
    if errors:
        logger.error("Missing required environment variables:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease check your .env file or environment variables.")
        return False
    
    # Validate API keys format
    if notion_token and not notion_token.startswith(('secret_', 'ntn_')):
        logger.warning("Notion token should start with 'secret_' or 'ntn_'")
    
    # Validate database ID format (should be 32 characters)
    if database_id and len(database_id.replace('-', '')) != 32:
        logger.warning("Notion database ID format seems incorrect")
    
    return True


def _build_sync_instance() -> NotionIGDbSync:
    notion_token = get_notion_token()
    igdb_client_id = os.getenv('IGDB_CLIENT_ID')
    igdb_client_secret = os.getenv('IGDB_CLIENT_SECRET')
    database_id = get_database_id('NOTION_GAMES_DATABASE_ID', 'NOTION_DATABASE_ID')
    return NotionIGDbSync(notion_token, igdb_client_id, igdb_client_secret, database_id)


def enforce_worker_limits(workers: int) -> int:
    if workers < 1:
        raise ValueError("Number of workers must be at least 1")
    if workers > 4:
        logger.warning("Using %s workers may cause rate limiting issues", workers)
    return workers


def run_sync(
    *,
    force_icons: bool = False,
    force_update: bool = False,
    workers: int = 3,
    last_page: bool = False,
    page_id: Optional[str] = None,
) -> Dict:
    """Run the Games sync with the provided options."""
    enforce_worker_limits(workers)

    if os.getenv('GITHUB_EVENT_NAME') == 'repository_dispatch' and not page_id:
        raise RuntimeError("Repository dispatch triggered without page-id; aborting to avoid full sync")

    sync = _build_sync_instance()
    return sync.run_sync(
        force_icons=force_icons,
        force_update=force_update,
        max_workers=workers,
        last_page=last_page,
        page_id=page_id,
    )


def get_database_ids() -> List[str]:
    """Return normalized database IDs served by this sync."""
    database_id = get_database_id('NOTION_GAMES_DATABASE_ID', 'NOTION_DATABASE_ID')
    normalized = normalize_id(database_id) if database_id else None
    return [normalized] if normalized else []

