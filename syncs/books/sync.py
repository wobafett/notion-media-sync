#!/usr/bin/env python3
"""
Notion Google Books Sync Script
Synchronizes book information from Google Books API to Notion database pages.
"""

import concurrent.futures
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Union

import requests

from .hybrid_api import JikanAPI, ComicVineAPI, HybridBookAPI, StarWarsFandomAPI
from shared.logging_config import get_logger, setup_logging
from shared.notion_api import NotionAPI
from shared.utils import (
    build_created_after_filter,
    build_multi_select_options,
    find_page_by_property,
    get_database_id,
    get_notion_token,
    merge_multi_select_properties,
    normalize_id,
)

setup_logging('notion_books_sync.log')
logger = get_logger(__name__)

# Try to import custom property configuration
try:
    from syncs.books.property_config import (
        TITLE_PROPERTY_ID, AUTHORS_PROPERTY_ID, ARTISTS_PROPERTY_ID, COVER_ARTISTS_PROPERTY_ID,
        DESCRIPTION_PROPERTY_ID, PUBLICATION_DATE_PROPERTY_ID, PUBLISHER_PROPERTY_ID, 
        PAGE_COUNT_PROPERTY_ID, LANGUAGE_PROPERTY_ID, SW_TIMELINE_PROPERTY_ID, SERIES_PROPERTY_ID,
        ISBN_PROPERTY_ID, RATING_PROPERTY_ID, RATING_COUNT_PROPERTY_ID,
        CATEGORIES_PROPERTY_ID, CONTENT_RATING_PROPERTY_ID, BOOK_TYPE_PROPERTY_ID,
        SUBTITLE_PROPERTY_ID, COVER_IMAGE_PROPERTY_ID, GOOGLE_BOOKS_URL_PROPERTY_ID,
        LAST_UPDATED_PROPERTY_ID, GOOGLE_BOOKS_ID_PROPERTY_ID, JIKAN_ID_PROPERTY_ID, 
        COMICVINE_ID_PROPERTY_ID, WOOKIEEPEDIA_ID_PROPERTY_ID, CHAPTERS_PROPERTY_ID, 
        VOLUMES_PROPERTY_ID, STATUS_PROPERTY_ID, TYPE_PROPERTY_ID, COMIC_FORMAT_PROPERTY_ID,
        FOLLOWED_BY_PROPERTY_ID, DNS_PROPERTY_ID, FIELD_BEHAVIOR
    )
except ImportError as exc:
    raise RuntimeError(
        "syncs/books/property_config.py not found. Copy the example file and update it with your property IDs."
    ) from exc

class GoogleBooksAPI:
    """Google Books API client for fetching book data."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/books/v1"
        self.session = requests.Session()
        
        # Rate limiting - Google Books API allows 1000 requests per day
        self.request_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
    
    
    def _rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_api_request(self, url: str, params: Dict = None, max_retries: int = 3) -> requests.Response:
        """Make an API request with rate limiting and retry logic."""
        if params is None:
            params = {}
        
        # Add API key if available
        if self.api_key:
            params['key'] = self.api_key
        
        for attempt in range(max_retries + 1):
            try:
                # Apply rate limiting before each request
                self._rate_limit()
                
                # Make the request
                response = self.session.get(url, params=params)
                
                # Check for rate limiting
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + (attempt * 0.1)
                        logger.warning(f"Rate limited (429). Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        response.raise_for_status()
                
                # Check for other HTTP errors
                if response.status_code >= 400:
                    if attempt < max_retries:
                        wait_time = 1 + (attempt * 0.5)
                        logger.warning(f"HTTP {response.status_code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
                
                # Success
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = 1 + (attempt * 0.5)
                    logger.warning(f"Request failed: {e}. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed after {max_retries} retries: {e}")
                    raise
        
        # This should never be reached, but just in case
        raise Exception("Max retries exceeded")
    
    def search_books(self, title: str) -> Optional[Dict]:
        """Search for books by title with smart selection."""
        try:
            
            # Search Google Books
            params = {
                'q': title,
                'maxResults': 10,
                'printType': 'books'
            }
            
            response = self._make_api_request(f"{self.base_url}/volumes", params)
            data = response.json()
            
            if data.get('items'):
                # Log all search results for debugging
                logger.info(f"Search results for '{title}':")
                for i, book in enumerate(data['items'][:5]):  # Show top 5 results
                    book_title = book.get('volumeInfo', {}).get('title', 'Unknown')
                    book_id = book.get('id', 'Unknown')
                    logger.info(f"  {i+1}. {book_title} (ID: {book_id})")
                
                # Select the best match
                best_book = self._select_best_book(data['items'], title)
                if best_book:
                    logger.info(f"Selected: {best_book['volumeInfo']['title']} (ID: {best_book['id']})")
                    return best_book
            
            logger.warning(f"No results found for '{title}'")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for book '{title}': {e}")
            return None
    
    def _select_best_book(self, books: List[Dict], search_title: str) -> Optional[Dict]:
        """Select the best book from search results using smart ranking."""
        if not books:
            return None
        
        # Score each book based on multiple criteria
        scored_books = []
        for book in books:
            score = self._calculate_book_score(book, search_title)
            title = book.get('volumeInfo', {}).get('title', 'Unknown')
            scored_books.append((score, book, title))
        
        # Sort by score (highest first)
        scored_books.sort(key=lambda x: x[0], reverse=True)
        
        # Log scores for debugging
        logger.info("Book scores:")
        for i, (score, book, title) in enumerate(scored_books[:3]):
            logger.info(f"  {i+1}. {title}: {score:.1f} points")
        
        # Return the highest scoring book
        if scored_books:
            return scored_books[0][1]
        
        return None
    
    def _calculate_book_score(self, book: Dict, search_title: str) -> float:
        """Calculate a score for a book based on relevance to search title."""
        volume_info = book.get('volumeInfo', {})
        score = 0.0
        
        # 1. Title match closeness (highest priority)
        book_title = volume_info.get('title', '').lower()
        book_subtitle = volume_info.get('subtitle', '').lower()
        search_title_lower = search_title.lower()
        
        # Combine title and subtitle for better matching
        combined_book_title = book_title
        if book_subtitle:
            combined_book_title = f"{book_title}: {book_subtitle}"
        
        # Extract core title words (remove common words and punctuation)
        import re
        def extract_core_words(title):
            # Remove common words and punctuation, split into words
            words = re.findall(r'\b\w+\b', title.lower())
            # Remove common words
            common_words = {'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'novel', 'book', 'story', 'tale'}
            return [word for word in words if word not in common_words and len(word) > 2]
        
        search_words = extract_core_words(search_title)
        book_words = extract_core_words(combined_book_title)
        
        # Calculate word overlap score
        if search_words and book_words:
            overlap = len(set(search_words) & set(book_words))
            total_words = len(set(search_words) | set(book_words))
            if total_words > 0:
                word_overlap_score = (overlap / total_words) * 500.0  # Scale to 0-500 points
                score += word_overlap_score
        
        # Exact matching with combined title (highest priority)
        if combined_book_title == search_title_lower:
            score += 1000.0
        elif combined_book_title.startswith(search_title_lower):
            score += 500.0
        elif search_title_lower in combined_book_title:
            score += 200.0
        
        # Also check individual title for backwards compatibility
        elif book_title == search_title_lower:
            score += 1000.0
        elif book_title.startswith(search_title_lower):
            score += 500.0
        elif search_title_lower in book_title:
            score += 200.0
        
        # 2. Rating count (popularity/engagement)
        rating_count = volume_info.get('ratingsCount', 0) or 0
        score += min(rating_count / 10.0, 100.0)  # Cap at 100 points
        
        # 3. Rating quality
        rating = volume_info.get('averageRating', 0) or 0
        if rating > 0:
            score += min(rating * 10.0, 50.0)  # Cap at 50 points
        
        # 4. ISBN presence (indicates official publication)
        if volume_info.get('industryIdentifiers'):
            score += 25.0
        
        # 5. Small penalty for adaptations/derivatives
        title_lower = volume_info.get('title', '').lower()
        adaptation_indicators = ['playtext', 'adaptation', 'screenplay', 'script', 'stage', 'theater', 'theatre']
        if any(indicator in title_lower for indicator in adaptation_indicators):
            score -= 10.0  # Small penalty for adaptations
        
        return score
    
    def _extract_series_info(self, volume_info: Dict) -> Optional[str]:
        """Extract series information from Google Books data."""
        title = volume_info.get('title', '')
        subtitle = volume_info.get('subtitle', '')
        description = volume_info.get('description', '')
        
        # Look for series patterns in title
        series_patterns = [
            r'\(([^)]*[Ss]eries[^)]*)\)',  # (Series Name)
            r'\(([^)]*[Bb]ook \d+[^)]*)\)',  # (Book 1)
            r'\(([^)]*[Vv]olume \d+[^)]*)\)',  # (Volume 1)
            r'\(([^)]*[Pp]art \d+[^)]*)\)',  # (Part 1)
            r'\(([^)]*[Ss]aga[^)]*)\)',  # (Saga Name)
            r'\(([^)]*[Cc]ycle[^)]*)\)',  # (Cycle Name)
            r'\(([^)]*[Tt]rilogy[^)]*)\)',  # (Trilogy Name)
            r'\(([^)]*[Qq]uartet[^)]*)\)',  # (Quartet Name)
            r'\(([^)]*[Ss]equel[^)]*)\)',  # (Sequel Name)
            r'\(([^)]*[Cc]ollection[^)]*)\)',  # (Collection Name)
            r'\(([^)]*[Bb]oxed [Ss]et[^)]*)\)',  # (Boxed Set Name)
        ]
        
        # Check title first
        for pattern in series_patterns:
            match = re.search(pattern, title)
            if match:
                series_name = match.group(1).strip()
                # Clean up the series name
                series_name = re.sub(r'\s+', ' ', series_name)  # Normalize whitespace
                return series_name
        
        # Check subtitle
        for pattern in series_patterns:
            match = re.search(pattern, subtitle)
            if match:
                series_name = match.group(1).strip()
                series_name = re.sub(r'\s+', ' ', series_name)
                return series_name
        
        # Look for common series patterns in description
        if description:
            desc_patterns = [
                r'([A-Z][a-z]+ [A-Z][a-z]+ [Ss]eries)',
                r'([A-Z][a-z]+ [Ss]aga)',
                r'([A-Z][a-z]+ [Cc]ycle)',
                r'([A-Z][a-z]+ [Tt]rilogy)',
                r'([Dd]iscworld)',
                r'([Hh]arry [Pp]otter)',
                r'([Ll]ord [Oo]f [Tt]he [Rr]ings)',
                r'([Aa] [Ss]ong [Oo]f [Ii]ce [Aa]nd [Ff]ire)',
                r'([Dd]une [Ss]aga)',
                r'([Tt]he [Ww]heel [Oo]f [Tt]ime)',
                r'([Tt]he [Ss]tormlight [Aa]rchive)',
                r'([Tt]he [Mm]istborn [Ss]eries)',
            ]
            
            for pattern in desc_patterns:
                match = re.search(pattern, description)
                if match:
                    series_name = match.group(1).strip()
                    return series_name
        
        return None
    
    def get_book_details(self, book_id: str) -> Optional[Dict]:
        """Get detailed information for a book by ID."""
        try:
            
            response = self._make_api_request(f"{self.base_url}/volumes/{book_id}")
            book_data = response.json()
            
            if book_data:
                return book_data
            return None
            
        except Exception as e:
            logger.error(f"Error getting book details for ID {book_id}: {e}")
            return None
    
    def get_cover_url(self, book_data: Dict) -> Optional[str]:
        """Get high-quality cover image URL from book data."""
        try:
            volume_info = book_data.get('volumeInfo', {})
            image_links = volume_info.get('imageLinks', {})
            
            # Try different image sizes in order of preference (largest first)
            # Google Books API provides these sizes:
            # - smallThumbnail: ~80x120px
            # - thumbnail: ~128x193px  
            # - small: ~300x450px
            # - medium: ~600x900px
            # - large: ~900x1350px
            # - extraLarge: ~1200x1800px (if available)
            
            preferred_sizes = ['extraLarge', 'large', 'medium', 'small', 'thumbnail', 'smallThumbnail']
            
            for size in preferred_sizes:
                if image_links.get(size):
                    url = image_links[size]
                    
                    # Convert http to https for better security
                    if url.startswith('http://'):
                        url = url.replace('http://', 'https://')
                    
                    # For Google Books images, we can also try to get even higher quality
                    # by modifying the URL parameters
                    if 'books.google.com' in url or 'googleusercontent.com' in url:
                        # Try to get the highest quality version
                        # Remove any existing zoom parameters and add zoom=0 for original size
                        if 'zoom=' in url:
                            url = url.split('zoom=')[0] + 'zoom=0'
                        elif '&' in url:
                            url = url + '&zoom=0'
                        else:
                            url = url + '?zoom=0'
                    
                    logger.info(f"Using {size} cover image: {url}")
                    return url
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting cover URL: {e}")
            return None


class NotionGoogleBooksSync:
    """Main class for synchronizing Notion database with Google Books data."""
    
    def __init__(self, notion_token: str, google_books_api_key: Optional[str], database_id: str, comicvine_api_key: Optional[str] = None, comicvine_scrape: bool = False):
        self.notion = NotionAPI(notion_token)
        
        # Initialize APIs
        google_books_api = GoogleBooksAPI(google_books_api_key)
        jikan_api = JikanAPI()
        comicvine_api = ComicVineAPI(comicvine_api_key, comicvine_scrape=comicvine_scrape) if comicvine_api_key else None
        star_wars_fandom_api = StarWarsFandomAPI()
        
        # Use hybrid API
        self.google_books = HybridBookAPI(google_books_api, jikan_api, comicvine_api, star_wars_fandom_api)
        
        self.database_id = database_id
        
        # Rate limiting
        self.request_delay = 0.25  # 250ms between requests
        
        # Property mapping - will be populated from database schema
        self.property_mapping = {}
        
        # Field behavior configuration - set to always overwrite for testing
        self.field_behavior = {
            'authors': 'overwrite',
            'description': 'overwrite', 
            'publication_date': 'overwrite',
            'publisher': 'overwrite',
            'page_count': 'overwrite',
            'language': 'overwrite',
            'main_category': 'overwrite',
            'isbn': 'overwrite',
            'rating': 'overwrite',
            'rating_count': 'overwrite',
            'categories': 'overwrite',
            'content_rating': 'overwrite',
            'book_type': 'overwrite',
            'subtitle': 'overwrite',
            'google_books_url': 'overwrite',
            'google_books_id': 'overwrite',
            'jikan_id': 'overwrite',
            'comicvine_id': 'overwrite',
            'chapters': 'overwrite',
            'volumes': 'overwrite',
            'status': 'overwrite'
        }
        
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
                'authors_property_id': AUTHORS_PROPERTY_ID,
                'artists_property_id': ARTISTS_PROPERTY_ID,
                'cover_artists_property_id': COVER_ARTISTS_PROPERTY_ID,
                'description_property_id': DESCRIPTION_PROPERTY_ID,
                'publication_date_property_id': PUBLICATION_DATE_PROPERTY_ID,
                'publisher_property_id': PUBLISHER_PROPERTY_ID,
                'page_count_property_id': PAGE_COUNT_PROPERTY_ID,
                'language_property_id': LANGUAGE_PROPERTY_ID,
                'sw_timeline_property_id': SW_TIMELINE_PROPERTY_ID,
                'isbn_property_id': ISBN_PROPERTY_ID,
                'rating_property_id': RATING_PROPERTY_ID,
                'rating_count_property_id': RATING_COUNT_PROPERTY_ID,
                'categories_property_id': CATEGORIES_PROPERTY_ID,
                'content_rating_property_id': CONTENT_RATING_PROPERTY_ID,
                'book_type_property_id': BOOK_TYPE_PROPERTY_ID,
                'subtitle_property_id': SUBTITLE_PROPERTY_ID,
                'cover_image_property_id': COVER_IMAGE_PROPERTY_ID,
                'google_books_url_property_id': GOOGLE_BOOKS_URL_PROPERTY_ID,
                'last_updated_property_id': LAST_UPDATED_PROPERTY_ID,
                'google_books_id_property_id': GOOGLE_BOOKS_ID_PROPERTY_ID,
                'jikan_id_property_id': JIKAN_ID_PROPERTY_ID,
                'comicvine_id_property_id': COMICVINE_ID_PROPERTY_ID,
                'wookieepedia_id_property_id': WOOKIEEPEDIA_ID_PROPERTY_ID,
                'chapters_property_id': CHAPTERS_PROPERTY_ID,
                'volumes_property_id': VOLUMES_PROPERTY_ID,
                'status_property_id': STATUS_PROPERTY_ID,
                'type_property_id': TYPE_PROPERTY_ID,
                'series_property_id': SERIES_PROPERTY_ID,
                'comic_format_property_id': COMIC_FORMAT_PROPERTY_ID,
                'followed_by_property_id': FOLLOWED_BY_PROPERTY_ID,
                'dns_property_id': DNS_PROPERTY_ID,
            }
            
            # Dynamically find DNS property if not configured
            if not self.property_mapping['dns_property_id']:
                for prop_key, prop_data in properties.items():
                    if prop_data.get('name') == 'DNS' and prop_data.get('type') == 'checkbox':
                        self.property_mapping['dns_property_id'] = prop_data.get('id')
                        logger.info(f"Found DNS checkbox property dynamically: {prop_data.get('id')}")
                        break
            
            # Log the property mapping
            for prop_key, prop_id in self.property_mapping.items():
                if prop_id is not None:
                    property_key = self.property_id_to_key.get(prop_id, "NOT_FOUND")
                    logger.info(f"✓ {prop_key}: {prop_id} -> {property_key}")
                else:
                    logger.info(f"⏭️  {prop_key}: NOT CONFIGURED (skipped)")
            
            # Validate required properties
            if not self.property_mapping['title_property_id']:
                logger.error("❌ No title property found - this is required!")
                
        except Exception as e:
            logger.error(f"Error loading database schema: {e}")
            self.property_mapping = {}
    
    def get_notion_pages(self, created_after: Optional[str] = None) -> List[Dict]:
        """Get all pages from the Notion database, optionally filtered by creation date."""
        filter_params = build_created_after_filter(created_after)
        if filter_params:
            logger.info(f"Filtering pages created on/after {created_after}")
        logger.info(f"Fetching pages from database {self.database_id}")
        return self.notion.query_database(self.database_id, filter_params)
    
    def get_last_edited_page(self) -> Optional[Dict]:
        """Get the most recently edited page from the Notion database."""
        logger.info(f"Fetching last edited page from database {self.database_id}")
        
        try:
            # Get all pages and sort by last_edited_time in Python
            # This is more reliable than trying to sort by property name
            response = self.notion.client.databases.query(
                database_id=self.database_id,
                page_size=100  # Get more pages to find the most recent one
            )
            
            pages = response.get('results', [])
            if not pages:
                logger.warning("No pages found in database")
                return None
            
            # Sort by last_edited_time (this is always available on pages)
            pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
            
            # Get the most recently edited page
            most_recent_page = pages[0]
            logger.info(f"Found last edited page: {self.extract_title(most_recent_page) or 'Unknown Title'}")
            logger.info(f"Last edited: {most_recent_page.get('last_edited_time', 'Unknown time')}")
            return most_recent_page
                
        except Exception as e:
            logger.error(f"Error fetching last edited page: {e}")
            return None
    
    def sync_last_page(self, force_icons: bool = False, force_update: bool = False, dry_run: bool = False) -> Dict:
        """Sync only the most recently edited page."""
        logger.info("Starting sync for last edited page only")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        start_time = time.time()
        
        # Get the last edited page
        page = self.get_last_edited_page()
        
        if not page:
            logger.warning("No pages found in database")
            return {'success': False, 'message': 'No pages found'}
        
        logger.info(f"Processing last edited page: {self.extract_title(page) or 'Unknown Title'}")
        
        # Sync the single page
        try:
            result = self.sync_page(page, force_icons, force_update, dry_run)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if result is True:
                logger.info(f"✅ Successfully synced last edited page in {duration:.2f} seconds")
                return {
                    'success': True, 
                    'message': 'Last edited page synced successfully',
                    'pages_processed': 1,
                    'successful_updates': 1,
                    'failed_updates': 0,
                    'skipped_updates': 0,
                    'duration': duration
                }
            elif result is False:
                logger.info(f"⏭️ Last edited page skipped (no updates needed) in {duration:.2f} seconds")
                return {
                    'success': True, 
                    'message': 'Last edited page skipped (no updates needed)',
                    'pages_processed': 1,
                    'successful_updates': 0,
                    'failed_updates': 0,
                    'skipped_updates': 1,
                    'duration': duration
                }
            else:
                logger.error(f"❌ Failed to sync last edited page in {duration:.2f} seconds")
                return {
                    'success': False, 
                    'message': 'Failed to sync last edited page',
                    'pages_processed': 1,
                    'successful_updates': 0,
                    'failed_updates': 1,
                    'skipped_updates': 0,
                    'duration': duration
                }
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"Error syncing last edited page: {e}")
            return {
                'success': False, 
                'message': f'Error syncing last edited page: {e}',
                'pages_processed': 1,
                'successful_updates': 0,
                'failed_updates': 1,
                'skipped_updates': 0,
                'duration': duration
            }
    
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
    
    def extract_existing_ids(self, page: Dict) -> Dict[str, Optional[str]]:
        """Extract existing API IDs from a Notion page."""
        ids = {
            'google_books_id': None,
            'jikan_id': None,
            'comicvine_id': None,
            'wookieepedia_id': None
        }
        
        # Extract Google Books ID
        if self.property_mapping['google_books_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['google_books_id_property_id'])
            if property_key:
                google_books_property = page['properties'].get(property_key)
                if google_books_property and google_books_property.get('rich_text'):
                    ids['google_books_id'] = google_books_property['rich_text'][0]['plain_text']
        
        # Extract Jikan ID
        if self.property_mapping['jikan_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['jikan_id_property_id'])
            if property_key:
                jikan_property = page['properties'].get(property_key)
                if jikan_property and jikan_property.get('rich_text'):
                    ids['jikan_id'] = jikan_property['rich_text'][0]['plain_text']
        
        # Extract ComicVine ID
        if self.property_mapping['comicvine_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['comicvine_id_property_id'])
            if property_key:
                comicvine_property = page['properties'].get(property_key)
                if comicvine_property and comicvine_property.get('rich_text'):
                    ids['comicvine_id'] = comicvine_property['rich_text'][0]['plain_text']
        
        # Extract Wookieepedia ID
        if self.property_mapping['wookieepedia_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['wookieepedia_id_property_id'])
            if property_key:
                wookieepedia_property = page['properties'].get(property_key)
                if wookieepedia_property and wookieepedia_property.get('rich_text'):
                    ids['wookieepedia_id'] = wookieepedia_property['rich_text'][0]['plain_text']
        
        return ids
    
    def extract_existing_type(self, page: Dict) -> Optional[str]:
        """Extract existing Type property from a Notion page."""
        try:
            properties = page.get('properties', {})
            
            # Extract Type property
            if self.property_mapping['type_property_id']:
                property_key = self._get_property_key(self.property_mapping['type_property_id'])
                if property_key:
                    type_prop = properties.get(property_key)
                    if type_prop and type_prop.get('select'):
                        type_value = type_prop['select']['name']
                        logger.info(f"Found existing Type property: {type_value}")
                        return type_value
            
            logger.info("No existing Type property found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting Type from page {page.get('id')}: {e}")
            return None
    
    def get_cover_url(self, book_data: Dict) -> Optional[str]:
        """Extract the best available cover URL from book data."""
        volume_info = book_data.get('volumeInfo', {})
        
        # Try Google Books cover first
        if volume_info.get('imageLinks'):
            return self.google_books.get_cover_url(book_data)
        
        # Try Jikan cover for manga
        if volume_info.get('jikan_images'):
            jikan_images = volume_info['jikan_images']
            # Prioritize WebP for better quality
            if jikan_images.get('webp', {}).get('large_image_url'):
                logger.info(f"Using Jikan WebP large cover image: {jikan_images['webp']['large_image_url']}")
                return jikan_images['webp']['large_image_url']
            elif jikan_images.get('webp', {}).get('image_url'):
                logger.info(f"Using Jikan WebP cover image: {jikan_images['webp']['image_url']}")
                return jikan_images['webp']['image_url']
            elif jikan_images.get('jpg', {}).get('large_image_url'):
                logger.info(f"Using Jikan JPG large cover image: {jikan_images['jpg']['large_image_url']}")
                return jikan_images['jpg']['large_image_url']
            elif jikan_images.get('jpg', {}).get('image_url'):
                logger.info(f"Using Jikan JPG cover image: {jikan_images['jpg']['image_url']}")
                return jikan_images['jpg']['image_url']
        
        # Try ComicVine cover for comics
        if volume_info.get('comicvine_images'):
            comicvine_image = volume_info['comicvine_images']
            if comicvine_image.get('super_url'):
                logger.info(f"Using ComicVine super cover image: {comicvine_image['super_url']}")
                return comicvine_image['super_url']
            elif comicvine_image.get('medium_url'):
                logger.info(f"Using ComicVine medium cover image: {comicvine_image['medium_url']}")
                return comicvine_image['medium_url']
        
        # Try Wookieepedia cover for Star Wars comics
        if volume_info.get('wookieepedia_images'):
            wookieepedia_image = volume_info['wookieepedia_images']
            if wookieepedia_image.get('cover_url'):
                logger.info(f"Using Wookieepedia cover image: {wookieepedia_image['cover_url']}")
                return wookieepedia_image['cover_url']
        
        return None
    
    def _format_basic_properties(self, volume_info: Dict) -> Dict:
        """Format basic book properties (authors, description, etc.)."""
        properties = {}
        
        # Authors
        if volume_info.get('authors') and self.property_mapping['authors_property_id']:
            property_key = self._get_property_key(self.property_mapping['authors_property_id'])
            if property_key:
                author_options = build_multi_select_options(volume_info['authors'], context='authors')
                properties[property_key] = {'multi_select': author_options}
        
        # Description
        if volume_info.get('description') and self.property_mapping['description_property_id']:
            property_key = self._get_property_key(self.property_mapping['description_property_id'])
            if property_key:
                description = volume_info['description']
                # Truncate description to fit Notion's 2000 character limit
                if len(description) > 2000:
                    description = description[:1997] + "..."
                    logger.info(f"Truncated description from {len(volume_info['description'])} to {len(description)} characters")
                
                logger.info(f"Setting description: {description[:100]}...")
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': description}
                    }]
                }
        
        
        # Page Count
        if volume_info.get('pageCount') and self.property_mapping['page_count_property_id']:
            property_key = self._get_property_key(self.property_mapping['page_count_property_id'])
            if property_key:
                properties[property_key] = {
                    'number': volume_info['pageCount']
                }
        
        # Language
        if volume_info.get('language') and self.property_mapping['language_property_id']:
            property_key = self._get_property_key(self.property_mapping['language_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': volume_info['language']}
                    }]
                }
        
        # ISBN
        if volume_info.get('industryIdentifiers') and self.property_mapping['isbn_property_id']:
            property_key = self._get_property_key(self.property_mapping['isbn_property_id'])
            if property_key:
                isbn = None
                for identifier in volume_info['industryIdentifiers']:
                    if identifier.get('type') == 'ISBN_13':
                        isbn = identifier.get('identifier')
                        break
                    elif identifier.get('type') == 'ISBN_10' and not isbn:
                        isbn = identifier.get('identifier')
                
                if isbn:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': isbn}
                        }]
                    }
        
        # Subtitle
        if volume_info.get('subtitle') and self.property_mapping['subtitle_property_id']:
            property_key = self._get_property_key(self.property_mapping['subtitle_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': volume_info['subtitle']}
                    }]
                }
        
        return properties
    
    def _format_rating_properties(self, volume_info: Dict) -> Dict:
        """Format rating-related properties."""
        properties = {}
        
        # Rating (normalized to 5-point scale)
        if volume_info.get('averageRating') and self.property_mapping['rating_property_id']:
            property_key = self._get_property_key(self.property_mapping['rating_property_id'])
            if property_key:
                # Google Books uses 5-point scale, Jikan uses 10-point scale
                rating = volume_info['averageRating']
                if rating > 5:  # Jikan rating (10-point scale)
                    rating = rating / 2  # Normalize to 5-point scale
                
                properties[property_key] = {
                    'number': round(rating, 2)
                }
        
        return properties
    
    def _format_publication_properties(self, volume_info: Dict) -> Dict:
        """Format publication-related properties."""
        properties = {}
        
        # Publication Date (handle date ranges)
        if volume_info.get('publishedDate') and self.property_mapping['publication_date_property_id']:
            property_key = self._get_property_key(self.property_mapping['publication_date_property_id'])
            if property_key:
                published_date = volume_info['publishedDate']
                
                # Handle date range for series
                if volume_info.get('publishedEndDate'):
                    end_date = volume_info['publishedEndDate']
                    properties[property_key] = {
                        'date': {
                            'start': published_date,
                            'end': end_date
                        }
                    }
                    logger.info(f"Setting publication date range: {published_date} to {end_date}")
                else:
                    properties[property_key] = {
                        'date': {'start': published_date}
                    }
                    logger.info(f"Setting publication date: {published_date}")
        
        # Publication Status
        if volume_info.get('status') and self.property_mapping['status_property_id']:
            property_key = self._get_property_key(self.property_mapping['status_property_id'])
            if property_key:
                properties[property_key] = {
                    'select': {'name': volume_info['status']}
                }
                logger.info(f"Setting status: {volume_info['status']}")
        
        # Chapters/Issues
        if volume_info.get('chapters') and self.property_mapping['chapters_property_id']:
            property_key = self._get_property_key(self.property_mapping['chapters_property_id'])
            if property_key:
                properties[property_key] = {
                    'number': volume_info['chapters']
                }
                logger.info(f"Setting chapters: {volume_info['chapters']}")
        
        # Volumes
        if volume_info.get('volumes') and self.property_mapping['volumes_property_id']:
            property_key = self._get_property_key(self.property_mapping['volumes_property_id'])
            if property_key:
                properties[property_key] = {
                    'number': volume_info['volumes']
                }
        
        return properties
    
    def _format_categorization_properties(self, volume_info: Dict) -> Dict:
        """Format categorization properties (categories, maturity rating, etc.)."""
        properties = {}
        
        # Categories
        if volume_info.get('categories') and self.property_mapping['categories_property_id']:
            property_key = self._get_property_key(self.property_mapping['categories_property_id'])
            if property_key:
                # Filter out "Mature" from categories if present
                categories = volume_info['categories']
                if 'Mature' in categories:
                    categories = [cat for cat in categories if cat != 'Mature']
                    logger.info("Found 'Mature' theme, using for maturity rating")
                
                if categories:  # Only add if there are categories left
                    category_options = build_multi_select_options(categories, context='categories')
                    properties[property_key] = {'multi_select': category_options}
        
        # Maturity Rating
        maturity_rating = None
        
        # Priority: Jikan demographic > ComicVine "Mature" > Google Books maturity
        if volume_info.get('jikan_demographic'):
            maturity_rating = volume_info['jikan_demographic']
            logger.info(f"Using Jikan demographic for maturity rating: {maturity_rating}")
        elif volume_info.get('categories') and 'Mature' in volume_info['categories']:
            maturity_rating = 'Mature'
            logger.info("Using ComicVine maturity rating: Mature")
        elif volume_info.get('maturityRating') and self.property_mapping['content_rating_property_id']:
            maturity_rating = volume_info['maturityRating']
            logger.info(f"Using Google Books maturity rating: {maturity_rating}")
        
        if maturity_rating and self.property_mapping['content_rating_property_id']:
            property_key = self._get_property_key(self.property_mapping['content_rating_property_id'])
            if property_key:
                maturity_options = build_multi_select_options([maturity_rating], context='maturity_rating')
                properties[property_key] = {'multi_select': maturity_options}
        
        # Print Type
        if volume_info.get('printType') and self.property_mapping['book_type_property_id']:
            property_key = self._get_property_key(self.property_mapping['book_type_property_id'])
            if property_key:
                print_type_options = build_multi_select_options([volume_info['printType']], context='print_type')
                properties[property_key] = {'multi_select': print_type_options}
        
        return properties
    
    def _format_api_id_properties(self, book_data: Dict) -> Dict:
        """Format API ID properties."""
        properties = {}
        
        # Google Books ID
        if book_data.get('id') and self.property_mapping['google_books_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['google_books_id_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': book_data['id']}
                    }]
                }
        
        # Jikan ID
        if book_data.get('jikan_id') and self.property_mapping['jikan_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['jikan_id_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': str(book_data['jikan_id'])}
                    }]
                }
        
        # ComicVine ID
        if book_data.get('comicvine_id') and self.property_mapping['comicvine_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['comicvine_id_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': str(book_data['comicvine_id'])}
                    }]
                }
        
        # Wookieepedia ID
        if book_data.get('wookieepedia_id') and self.property_mapping['wookieepedia_id_property_id']:
            property_key = self._get_property_key(self.property_mapping['wookieepedia_id_property_id'])
            if property_key:
                properties[property_key] = {
                    'rich_text': [{
                        'type': 'text',
                        'text': {'content': str(book_data['wookieepedia_id'])}
                    }]
                }
        
        return properties
    
    def _format_url_properties(self, volume_info: Dict) -> Dict:
        """Format URL properties."""
        properties = {}
        
        # Info URL (prioritize Wookieepedia > Jikan > ComicVine > Google Books)
        info_url = None
        if volume_info.get('wookieepedia_url'):
            info_url = volume_info['wookieepedia_url']
            logger.info(f"Using Wookieepedia URL for Info: {info_url}")
        elif volume_info.get('jikan_url'):
            info_url = volume_info['jikan_url']
            logger.info(f"Using Jikan URL for Info: {info_url}")
        elif volume_info.get('comicvine_url'):
            info_url = volume_info['comicvine_url']
            logger.info(f"Using ComicVine URL for Info: {info_url}")
        elif volume_info.get('infoLink'):
            info_url = volume_info['infoLink']
            logger.info(f"Using Google Books URL for Info: {info_url}")
        
        if info_url and self.property_mapping['google_books_url_property_id']:
            property_key = self._get_property_key(self.property_mapping['google_books_url_property_id'])
            if property_key:
                properties[property_key] = {
                    'url': info_url
                }
        
        return properties
    
    def sync_page(self, page: Dict, force_icons: bool = False, force_update: bool = False, dry_run: bool = False) -> Optional[bool]:
        """Sync a single page with Google Books data."""
        try:
            page_id = page['id']
            title = self.extract_title(page)
            
            if not title:
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            logger.info(f"Processing: {title}")
            
            # Check DNS checkbox - skip if checked (prevents automation cascade)
            # Allow force_update to override DNS check
            dns_prop_id = self.property_mapping.get('dns_property_id')
            if dns_prop_id and not force_update:
                dns_key = self._get_property_key(dns_prop_id)
                if dns_key:
                    dns_prop = page.get('properties', {}).get(dns_key, {})
                    if dns_prop.get('checkbox'):
                        logger.info(f"Skipping '{title}' - DNS checkbox is checked")
                        return None
            
            # Extract existing API IDs
            existing_ids = self.extract_existing_ids(page)
            existing_google_books_id = existing_ids['google_books_id']
            existing_jikan_id = existing_ids['jikan_id']
            existing_comicvine_id = existing_ids['comicvine_id']
            existing_wookieepedia_id = existing_ids['wookieepedia_id']
            
            # Skip processing if any ID exists and not forcing all updates
            if not force_update and (existing_google_books_id or existing_jikan_id or existing_comicvine_id or existing_wookieepedia_id):
                logger.info(f"⏭️ Skipping '{title}' - already has API ID(s): "
                           f"Google Books: {existing_google_books_id or 'None'}, "
                           f"Jikan: {existing_jikan_id or 'None'}, "
                           f"ComicVine: {existing_comicvine_id or 'None'}, "
                           f"Wookieepedia: {existing_wookieepedia_id or 'None'}")
                return None  # Return None to indicate skipped
            
            # Extract existing Type property to determine API preference
            existing_type = self.extract_existing_type(page)
            
            # Log found IDs
            if existing_google_books_id:
                logger.info(f"Found existing Google Books ID: {existing_google_books_id}")
            if existing_jikan_id:
                logger.info(f"Found existing Jikan ID: {existing_jikan_id}")
            if existing_comicvine_id:
                logger.info(f"Found existing ComicVine ID: {existing_comicvine_id}")
            if existing_wookieepedia_id:
                logger.info(f"Found existing Wookieepedia ID: {existing_wookieepedia_id}")
            
            # Determine content type preference based on existing Type property
            content_type_preference = None
            if existing_type:
                if existing_type == "Comic":
                    content_type_preference = "comic"
                elif existing_type == "Manga":
                    content_type_preference = "manga"
                elif existing_type == "Book":
                    content_type_preference = "book"
                logger.info(f"Using Type property '{existing_type}' to set content type preference: {content_type_preference}")
            
            # Use existing IDs for direct API calls if available
            if (existing_google_books_id or existing_jikan_id or existing_comicvine_id or existing_wookieepedia_id):
                # Priority order: Jikan (manga) > ComicVine (comics) > Google Books (general books)
                # This ensures we use the most specific API for the content type
                
                if existing_jikan_id:
                    logger.info(f"Using existing Jikan ID: {existing_jikan_id}")
                    # Get manga details directly from Jikan
                    detailed_manga = self.google_books.jikan.get_manga_details(int(existing_jikan_id))
                    if detailed_manga:
                        book_data = self.google_books._create_manga_data_from_jikan(detailed_manga)
                        logger.info(f"Retrieved Jikan data for MAL ID: {existing_jikan_id}")
                    else:
                        logger.warning(f"Could not retrieve Jikan data for MAL ID: {existing_jikan_id}, falling back to hybrid search")
                        if content_type_preference:
                            original_content_type = self.google_books.content_type
                            self.google_books.content_type = content_type_preference
                            book_data = self.google_books.search_books(title)
                            self.google_books.content_type = original_content_type
                        else:
                            book_data = self.google_books.search_books(title)
                
                elif existing_comicvine_id:
                    logger.info(f"Using existing ComicVine ID: {existing_comicvine_id}")
                    # Get comics details directly from ComicVine
                    if self.google_books.comicvine:
                        try:
                            # Get volume details directly using the ComicVine ID
                            volume_details = self.google_books.comicvine.get_volume_details(int(existing_comicvine_id))
                            if volume_details:
                                # Check if we should force scraping even with existing ID
                                if self.google_books.comicvine.comicvine_scrape:
                                    # Force scraping to refresh creator/theme data
                                    themes = self.google_books.comicvine.scrape_volume_themes(int(existing_comicvine_id))
                                    if themes:
                                        volume_details['scraped_themes'] = themes
                                        logger.info(f"Force scraped {len(themes)} themes for volume {existing_comicvine_id}")
                                    
                                    creators = self.google_books.comicvine.scrape_volume_creators(int(existing_comicvine_id))
                                    if creators:
                                        volume_details['scraped_creators'] = creators
                                        logger.info(f"Force scraped {len(creators)} creators for volume {existing_comicvine_id}")
                                else:
                                    # Since we have an existing ComicVine ID, we already have the data
                                    # Skip scraping to avoid redundant work
                                    volume_details['scraped_themes'] = []
                                    volume_details['scraped_creators'] = []
                                    logger.info("Skipping web scraping - ComicVine ID already exists")
                                
                                book_data = self.google_books._create_comics_data_from_comicvine(volume_details)
                                logger.info(f"Retrieved ComicVine data for volume ID: {existing_comicvine_id}")
                            else:
                                logger.warning(f"Could not retrieve ComicVine data for volume ID: {existing_comicvine_id}, falling back to hybrid search")
                                if content_type_preference:
                                    original_content_type = self.google_books.content_type
                                    self.google_books.content_type = content_type_preference
                                    book_data = self.google_books.search_books(title)
                                    self.google_books.content_type = original_content_type
                                else:
                                    book_data = self.google_books.search_books(title)
                        except Exception as e:
                            logger.error(f"Error retrieving ComicVine data for ID {existing_comicvine_id}: {e}")
                            logger.info("Falling back to hybrid search")
                            if content_type_preference:
                                original_content_type = self.google_books.content_type
                                self.google_books.content_type = content_type_preference
                                book_data = self.google_books.search_books(title)
                                self.google_books.content_type = original_content_type
                            else:
                                book_data = self.google_books.search_books(title)
                    else:
                        logger.warning(f"ComicVine API not configured, falling back to hybrid search")
                        if content_type_preference:
                            original_content_type = self.google_books.content_type
                            self.google_books.content_type = content_type_preference
                            book_data = self.google_books.search_books(title)
                            self.google_books.content_type = original_content_type
                        else:
                            book_data = self.google_books.search_books(title)
                
                elif existing_google_books_id and not existing_google_books_id.isdigit():
                    logger.info(f"Using existing Google Books ID: {existing_google_books_id}")
                    book_data = self.google_books.get_book_details(existing_google_books_id)
                    if not book_data:
                        logger.warning(f"Could not find book with Google Books ID: {existing_google_books_id}, falling back to hybrid search")
                        if content_type_preference:
                            original_content_type = self.google_books.content_type
                            self.google_books.content_type = content_type_preference
                            book_data = self.google_books.search_books(title)
                            self.google_books.content_type = original_content_type
                        else:
                            book_data = self.google_books.search_books(title)
                
                else:
                    # Fallback to hybrid search if no valid existing ID
                    logger.info(f"No valid existing ID found, using hybrid search")
                    if content_type_preference:
                        original_content_type = self.google_books.content_type
                        self.google_books.content_type = content_type_preference
                        book_data = self.google_books.search_books(title)
                        self.google_books.content_type = original_content_type
                    else:
                        book_data = self.google_books.search_books(title)
            else:
                # Use hybrid API for all searches to properly detect manga/comics
                # Set content type preference based on existing Type property
                if content_type_preference:
                    logger.info(f"Using content type preference '{content_type_preference}' from Type property")
                    # Temporarily set the content type on the hybrid API
                    original_content_type = self.google_books.content_type
                    self.google_books.content_type = content_type_preference
                    book_data = self.google_books.search_books(title)
                    # Restore original content type
                    self.google_books.content_type = original_content_type
                else:
                    book_data = self.google_books.search_books(title)
            
            if not book_data:
                logger.warning(f"Could not find book: {title}")
                return False
            
            # Format properties for Notion
            properties = self.format_notion_properties(book_data)
            
            # Merge multi-select properties based on FIELD_BEHAVIOR config
            property_mappings = {}
            field_mapping = [
                ('authors_property_id', 'authors_property_id'),
                ('categories_property_id', 'categories_property_id'),
                ('publisher_property_id', 'publisher_property_id'),
                ('artists_property_id', 'artists_property_id'),
                ('cover_artists_property_id', 'cover_artists_property_id'),
                ('language_property_id', 'language_property_id'),
                ('series_property_id', 'series_property_id'),
                ('content_rating_property_id', 'content_rating_property_id'),
                ('book_type_property_id', 'book_type_property_id'),
            ]
            for prop_id_key, config_key in field_mapping:
                if self.property_mapping.get(prop_id_key):
                    prop_key = self._get_property_key(self.property_mapping[prop_id_key])
                    if prop_key:
                        property_mappings[config_key] = prop_key
            properties = merge_multi_select_properties(page, properties, FIELD_BEHAVIOR, property_mappings)
            
            # Get cover URL if available
            cover_url = self.get_cover_url(book_data)
            
            # Set appropriate icon based on content type
            icon = '📚'  # Default book emoji
            volume_info = book_data.get('volumeInfo', {})
            
            # Check for Jikan data (manga)
            if volume_info.get('jikan_data'):
                icon = '🗯️'  # Manga emoji
            # Check for Wookieepedia data (Star Wars comics)
            elif volume_info.get('wookieepedia_data'):
                icon = '💥'  # Comic emoji
            # Check for ComicVine data (Western comics)
            elif volume_info.get('comicvine_data'):
                icon = '💥'  # Comic emoji
            # Check for Google Books with comics content
            elif volume_info.get('comicsContent') is True:
                icon = '💥'  # Comic emoji
            
            # Update the page
            if dry_run:
                logger.info(f"🔍 DRY RUN: Would update page '{title}' with {len(properties)} properties")
                if cover_url:
                    logger.info(f"🔍 DRY RUN: Would set cover to: {cover_url}")
                logger.info(f"🔍 DRY RUN: Would set icon to: {icon}")
                logger.info("🔍 DRY RUN: No actual changes made to Notion")
                return True
            elif self.notion.update_page(page_id, properties, cover_url, icon):
                logger.info(f"Successfully updated: {title}")
                return True
            else:
                logger.error(f"Failed to update: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing page {page.get('id')}: {e}")
            return False
    
    def format_notion_properties(self, book_data: Dict) -> Dict:
        """Format book data for Notion properties (handles Google Books, Jikan, ComicVine, and Wookieepedia data)."""
        properties = {}
        
        try:
            # Handle different data structures
            # Google Books: data is in 'volumeInfo'
            # Jikan/ComicVine/Wookieepedia: data is directly in root
            volume_info = book_data.get('volumeInfo', book_data)
            
            # Title (skip updating - use existing title from Notion)
            # if volume_info.get('title') and self.property_mapping['title_property_id']:
            #     property_key = self._get_property_key(self.property_mapping['title_property_id'])
            #     if property_key:
            #         properties[property_key] = {
            #             'title': [{'text': {'content': volume_info['title']}}]
            #         }
            
            # Authors
            if volume_info.get('authors') and self.property_mapping['authors_property_id']:
                property_key = self._get_property_key(self.property_mapping['authors_property_id'])
                logger.info(f"Authors found: {volume_info.get('authors')}, property_key: {property_key}")
                if property_key:
                    author_options = build_multi_select_options(volume_info['authors'], context='authors')
                    properties[property_key] = {'multi_select': author_options}
                    logger.info(f"Added authors property: {properties[property_key]}")
                else:
                    logger.warning(f"Could not get property key for authors_property_id: {self.property_mapping['authors_property_id']}")
            else:
                pass
            
            # Description - Jikan synopsis is already merged into description field
            if volume_info.get('description') and self.property_mapping['description_property_id']:
                property_key = self._get_property_key(self.property_mapping['description_property_id'])
                if property_key:
                    description = volume_info['description']
                    
                    # Clean HTML from Google Books descriptions
                    if '<' in description and '>' in description:
                        import re
                        # Remove HTML tags
                        description = re.sub(r'<[^>]+>', '', description)
                        # Clean up extra whitespace
                        description = re.sub(r'\s+', ' ', description)
                        # Remove HTML entities
                        description = description.replace('&nbsp;', ' ')
                        description = description.replace('&amp;', '&')
                        description = description.replace('&lt;', '<')
                        description = description.replace('&gt;', '>')
                        description = description.replace('&quot;', '"')
                        description = description.strip()
                        logger.info(f"Cleaned HTML from description, reduced length by {len(volume_info['description']) - len(description)} characters")
                    
                    # Truncate description to fit Notion's 2000 character limit
                    if len(description) > 2000:
                        description = description[:1997] + "..."
                        logger.info(f"Truncated description from {len(volume_info['description'])} to {len(description)} characters")
                    
                    logger.info(f"Setting description: {description[:100]}...")
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': description}
                        }]
                    }
            
            # Publisher
            if volume_info.get('publisher') and self.property_mapping['publisher_property_id']:
                property_key = self._get_property_key(self.property_mapping['publisher_property_id'])
                if property_key:
                    publisher_options = build_multi_select_options([volume_info['publisher']], context='publisher')
                    properties[property_key] = {'multi_select': publisher_options}
                    logger.info(f"Added publisher property: {volume_info['publisher']}")
            
            # Publication Date (with optional end date for date ranges)
            if volume_info.get('publishedDate') and self.property_mapping['publication_date_property_id']:
                # Parse and format the start date
                published_date = volume_info['publishedDate']
                published_end_date = volume_info.get('publishedEndDate')
                
                # Check for ComicVine last_issue_date as end date
                comicvine_data = volume_info.get('comicvine_data', {})
                last_issue_date = comicvine_data.get('last_issue_date')
                
                try:
                    # Handle different date formats for start date
                    if len(published_date) == 4:  # Year only
                        formatted_start_date = f"{published_date}-01-01"
                    elif len(published_date) == 7:  # Year-Month
                        formatted_start_date = f"{published_date}-01"
                    else:  # Full date
                        formatted_start_date = published_date
                    
                    # Handle end date if available (prioritize last_issue_date over publishedEndDate)
                    formatted_end_date = None
                    if last_issue_date:
                        # Use ComicVine's last issue date
                        formatted_end_date = last_issue_date
                        logger.info(f"Setting publication date range: {formatted_start_date} to {formatted_end_date} (from last issue)")
                    elif published_end_date:
                        # Fallback to publishedEndDate
                        if len(published_end_date) == 4:  # Year only
                            formatted_end_date = f"{published_end_date}-01-01"
                        elif len(published_end_date) == 7:  # Year-Month
                            formatted_end_date = f"{published_end_date}-01"
                        else:  # Full date
                            formatted_end_date = published_end_date
                        logger.info(f"Setting publication date range: {formatted_start_date} to {formatted_end_date}")
                    else:
                        logger.info(f"Setting publication date: {formatted_start_date}")
                    
                    property_key = self._get_property_key(self.property_mapping['publication_date_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'date': {
                                'start': formatted_start_date,
                                'end': formatted_end_date,
                                'time_zone': None
                            }
                        }
                except Exception as e:
                    logger.warning(f"Error parsing publication date '{published_date}': {e}")
            
            # Status
            status = volume_info.get('status')
            if not status and published_date and self.property_mapping['status_property_id']:
                # Auto-set status based on publication date
                try:
                    from datetime import datetime
                    
                    # Handle different date formats
                    date_str = published_date.split('T')[0]  # Remove time part if present
                    
                    # Try different date formats
                    pub_date = None
                    if len(date_str) == 10:  # YYYY-MM-DD
                        pub_date = datetime.strptime(date_str, '%Y-%m-%d')
                    elif len(date_str) == 7:  # YYYY-MM
                        pub_date = datetime.strptime(date_str, '%Y-%m')
                    elif len(date_str) == 4:  # YYYY
                        pub_date = datetime.strptime(date_str, '%Y')
                    
                    if pub_date:
                        current_date = datetime.now()
                        
                        if pub_date <= current_date:
                            status = 'Published'
                            logger.info(f"Auto-setting status to 'Published' based on publication date: {published_date}")
                        else:
                            status = 'Upcoming'
                            logger.info(f"Auto-setting status to 'Upcoming' based on future publication date: {published_date}")
                    else:
                        logger.warning(f"Could not parse publication date format: {published_date}")
                        
                except Exception as e:
                    logger.warning(f"Error parsing publication date for status: {e}")
            
            if status and self.property_mapping['status_property_id']:
                property_key = self._get_property_key(self.property_mapping['status_property_id'])
                if property_key:
                    logger.info(f"Setting status: {status}")
                    properties[property_key] = {
                        'select': {'name': status}
                    }
            
            # Artists
            if volume_info.get('artists') and self.property_mapping['artists_property_id']:
                property_key = self._get_property_key(self.property_mapping['artists_property_id'])
                if property_key:
                    artist_options = build_multi_select_options(volume_info['artists'], context='artists')
                    properties[property_key] = {'multi_select': artist_options}
            
            # Cover Artists
            if volume_info.get('cover_artists') and self.property_mapping['cover_artists_property_id']:
                property_key = self._get_property_key(self.property_mapping['cover_artists_property_id'])
                if property_key:
                    cover_artist_options = build_multi_select_options(volume_info['cover_artists'], context='cover_artists')
                    properties[property_key] = {'multi_select': cover_artist_options}
            
            # SW Timeline
            if volume_info.get('sw_timeline') and self.property_mapping['sw_timeline_property_id']:
                property_key = self._get_property_key(self.property_mapping['sw_timeline_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': volume_info['sw_timeline']}
                        }]
                    }
            
            # Series
            series_name = volume_info.get('series')
            if not series_name and 'volumeInfo' in book_data:
                # Try to extract series info from Google Books data
                series_name = self.google_books.google_books._extract_series_info(volume_info)
            
            if series_name and self.property_mapping['series_property_id']:
                property_key = self._get_property_key(self.property_mapping['series_property_id'])
                if property_key:
                    series_options = build_multi_select_options([series_name], context='series')
                    properties[property_key] = {'multi_select': series_options}
            
            # Comic Format (from Wookieepedia)
            if volume_info.get('format') and self.property_mapping['comic_format_property_id']:
                property_key = self._get_property_key(self.property_mapping['comic_format_property_id'])
                if property_key:
                    properties[property_key] = {
                        'select': {'name': volume_info['format']}
                    }
            
            # Followed By (from Wookieepedia)
            if volume_info.get('followed_by') and self.property_mapping['followed_by_property_id']:
                property_key = self._get_property_key(self.property_mapping['followed_by_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': volume_info['followed_by']}
                        }]
                    }
            
            # Page Count
            if volume_info.get('pageCount') and self.property_mapping['page_count_property_id']:
                property_key = self._get_property_key(self.property_mapping['page_count_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': volume_info['pageCount']
                    }
            
            # Chapters
            if volume_info.get('chapters') is not None and self.property_mapping['chapters_property_id']:
                property_key = self._get_property_key(self.property_mapping['chapters_property_id'])
                if property_key:
                    logger.info(f"Setting chapters: {volume_info['chapters']}")
                    properties[property_key] = {
                        'number': volume_info['chapters']
                    }
            
            # Volumes
            if volume_info.get('volumes') is not None and self.property_mapping['volumes_property_id']:
                property_key = self._get_property_key(self.property_mapping['volumes_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': volume_info['volumes']
                    }
            
            # Language
            if volume_info.get('language') and self.property_mapping['language_property_id']:
                property_key = self._get_property_key(self.property_mapping['language_property_id'])
                if property_key:
                    # Convert language code to full language name
                    language_code = volume_info['language'].lower()
                    language_names = {
                        'en': 'English',
                        'es': 'Spanish', 
                        'fr': 'French',
                        'de': 'German',
                        'it': 'Italian',
                        'pt': 'Portuguese',
                        'ru': 'Russian',
                        'ja': 'Japanese',
                        'ko': 'Korean',
                        'zh': 'Chinese',
                        'ar': 'Arabic',
                        'hi': 'Hindi',
                        'nl': 'Dutch',
                        'sv': 'Swedish',
                        'da': 'Danish',
                        'no': 'Norwegian',
                        'fi': 'Finnish',
                        'pl': 'Polish',
                        'tr': 'Turkish',
                        'he': 'Hebrew',
                        'th': 'Thai',
                        'vi': 'Vietnamese',
                        'cs': 'Czech',
                        'hu': 'Hungarian',
                        'ro': 'Romanian',
                        'bg': 'Bulgarian',
                        'hr': 'Croatian',
                        'sk': 'Slovak',
                        'sl': 'Slovenian',
                        'et': 'Estonian',
                        'lv': 'Latvian',
                        'lt': 'Lithuanian',
                        'uk': 'Ukrainian',
                        'be': 'Belarusian',
                        'mk': 'Macedonian',
                        'sq': 'Albanian',
                        'sr': 'Serbian',
                        'bs': 'Bosnian',
                        'mt': 'Maltese',
                        'is': 'Icelandic',
                        'ga': 'Irish',
                        'cy': 'Welsh',
                        'eu': 'Basque',
                        'ca': 'Catalan',
                        'gl': 'Galician'
                    }
                    
                    language_name = language_names.get(language_code, language_code.upper())
                    language_options = build_multi_select_options([language_name], context='language')
                    properties[property_key] = {'multi_select': language_options}
            
            # ISBN
            if volume_info.get('industryIdentifiers') and self.property_mapping['isbn_property_id']:
                # Find ISBN-13 or ISBN-10
                isbn = None
                for identifier in volume_info['industryIdentifiers']:
                    if identifier.get('type') == 'ISBN_13':
                        isbn = identifier.get('identifier')
                        break
                    elif identifier.get('type') == 'ISBN_10' and not isbn:
                        isbn = identifier.get('identifier')
                
                if isbn:
                    property_key = self._get_property_key(self.property_mapping['isbn_property_id'])
                    if property_key:
                        properties[property_key] = {
                            'number': int(isbn.replace('-', ''))
                        }
            
            # Rating - normalize to 5-point scale (Google Books standard)
            rating = volume_info.get('averageRating')
            rating_source = "Google Books"
            
            # Check for Jikan score and normalize from 10-point to 5-point scale
            if volume_info.get('jikan_data', {}).get('score'):
                jikan_score = volume_info['jikan_data']['score']
                # Convert from 10-point scale to 5-point scale
                rating = jikan_score / 2.0
                rating_source = "Jikan (normalized)"
                logger.info(f"Using Jikan score: {jikan_score} -> normalized to: {rating}")
            
            if rating and self.property_mapping['rating_property_id']:
                property_key = self._get_property_key(self.property_mapping['rating_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': round(rating, 2)  # Round to 2 decimal places
                    }
            
            # Rating Count
            if volume_info.get('ratingsCount') and self.property_mapping['rating_count_property_id']:
                property_key = self._get_property_key(self.property_mapping['rating_count_property_id'])
                if property_key:
                    properties[property_key] = {
                        'number': volume_info['ratingsCount']
                    }
            
            # Categories - use appropriate source based on data origin
            all_categories = []
            
            # Process categories based on source format
            if volume_info.get('categories'):
                # Google Books format - categories may contain " / " separators
                # Jikan/ComicVine format - categories are already individual items
                if not volume_info.get('jikan_data') and not volume_info.get('comicvine_data'):
                    # Google Books format - split by " / " to separate individual categories
                    for category_string in volume_info['categories']:
                        individual_categories = [cat.strip() for cat in category_string.split(' / ')]
                        all_categories.extend(individual_categories)
                else:
                    # Jikan/ComicVine format - already individual items, use directly
                    all_categories.extend(volume_info['categories'])
            
            # Add ComicVine themes for comics (scraped from website)
            comicvine_data = volume_info.get('comicvine_data', {})
            comicvine_maturity_rating = None
            if comicvine_data.get('scraped_themes'):
                # Filter out "Mature" from themes and use it for maturity rating
                filtered_themes = []
                for theme in comicvine_data['scraped_themes']:
                    if theme.lower() == 'mature':
                        comicvine_maturity_rating = 'Mature'
                        logger.info(f"Found 'Mature' theme, using for maturity rating")
                    else:
                        filtered_themes.append(theme)
                all_categories.extend(filtered_themes)
            
            if all_categories and self.property_mapping['categories_property_id']:
                property_key = self._get_property_key(self.property_mapping['categories_property_id'])
                if property_key:
                    # Remove duplicates while preserving order
                    unique_categories = list(dict.fromkeys(all_categories))
                    
                    category_options = build_multi_select_options(unique_categories, context='categories')
                    properties[property_key] = {'multi_select': category_options}
            
            
            # Content Rating (Maturity Rating) - prioritize Jikan demographics, then ComicVine "Mature"
            maturity_rating = None
            if volume_info.get('jikan_data', {}).get('demographics'):
                # Use Jikan demographics for manga
                demographics = volume_info['jikan_data']['demographics']
                if demographics:
                    maturity_rating = demographics[0]  # Use first demographic
                    logger.info(f"Using Jikan demographic for maturity rating: {maturity_rating}")
            elif comicvine_maturity_rating:
                # Use ComicVine "Mature" theme for comics
                maturity_rating = comicvine_maturity_rating
                logger.info(f"Using ComicVine maturity rating: {maturity_rating}")
            elif volume_info.get('maturityRating'):
                # Fallback to Google Books maturity rating
                maturity_rating = volume_info['maturityRating']
                logger.info(f"Using Google Books maturity rating: {maturity_rating}")
            
            if maturity_rating and self.property_mapping['content_rating_property_id']:
                property_key = self._get_property_key(self.property_mapping['content_rating_property_id'])
                if property_key:
                    maturity_options = build_multi_select_options([maturity_rating], context='content_rating')
                    properties[property_key] = {'multi_select': maturity_options}
            
            # Book Type (Print Type)
            if volume_info.get('printType') and self.property_mapping['book_type_property_id']:
                property_key = self._get_property_key(self.property_mapping['book_type_property_id'])
                if property_key:
                    print_type_options = build_multi_select_options([volume_info['printType']], context='book_type')
                    properties[property_key] = {'multi_select': print_type_options}
            
            # Subtitle
            if volume_info.get('subtitle') and self.property_mapping['subtitle_property_id']:
                property_key = self._get_property_key(self.property_mapping['subtitle_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': volume_info['subtitle']}
                        }]
                    }
            
            # API-specific IDs - set the appropriate ID based on data source
            book_id = book_data.get('id', '')
            wookieepedia_id = book_data.get('wookieepedia_id', '')
            volume_info = book_data.get('volumeInfo', {})
            
            # Wookieepedia ID (for Star Wars comics)
            if wookieepedia_id and self.property_mapping['wookieepedia_id_property_id']:
                property_key = self._get_property_key(self.property_mapping['wookieepedia_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': wookieepedia_id}
                        }]
                    }
            
            # Google Books ID (only for data that actually comes from Google Books)
            if (book_id and not book_id.isdigit() and 
                not volume_info.get('jikan_data') and 
                not volume_info.get('comicvine_data') and 
                not wookieepedia_id and  # Don't set Google Books ID if we have Wookieepedia ID
                self.property_mapping['google_books_id_property_id']):
                property_key = self._get_property_key(self.property_mapping['google_books_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': book_id}
                        }]
                    }
            
            # Jikan ID (for manga)
            if book_id and book_id.isdigit() and volume_info.get('jikan_data') and self.property_mapping['jikan_id_property_id']:
                property_key = self._get_property_key(self.property_mapping['jikan_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': book_id}
                        }]
                    }
            
            # ComicVine ID (for Western comics)
            if book_id and book_id.isdigit() and volume_info.get('comicvine_data') and self.property_mapping['comicvine_id_property_id']:
                property_key = self._get_property_key(self.property_mapping['comicvine_id_property_id'])
                if property_key:
                    properties[property_key] = {
                        'rich_text': [{
                            'type': 'text',
                            'text': {'content': book_id}
                        }]
                    }
            
            # Info URL - prioritize Wookieepedia > Jikan URL for manga, ComicVine URL for comics, fallback to Google Books
            info_url = None
            if volume_info.get('wookieepedia_url'):
                info_url = volume_info['wookieepedia_url']
                logger.info(f"Using Wookieepedia URL for Info: {info_url}")
            elif volume_info.get('jikan_url'):
                info_url = volume_info['jikan_url']
                logger.info(f"Using Jikan URL for Info: {info_url}")
            elif volume_info.get('comicvine_url'):
                info_url = volume_info['comicvine_url']
                logger.info(f"Using ComicVine URL for Info: {info_url}")
            elif volume_info.get('infoLink'):
                info_url = volume_info['infoLink']
                logger.info(f"Using Google Books URL for Info: {info_url}")
            
            if info_url and self.property_mapping['google_books_url_property_id']:
                property_key = self._get_property_key(self.property_mapping['google_books_url_property_id'])
                if property_key:
                    properties[property_key] = {
                        'url': info_url
                    }
            
            # Last Updated
            if self.property_mapping['last_updated_property_id']:
                property_key = self._get_property_key(self.property_mapping['last_updated_property_id'])
                if property_key:
                    properties[property_key] = {
                        'date': {'start': datetime.now().isoformat()}
                    }
            
            # Type property - set based on which API was used
            if self.property_mapping['type_property_id']:
                property_key = self._get_property_key(self.property_mapping['type_property_id'])
                if property_key:
                    # Determine type based on data source
                    type_value = "Book"  # Default
                    
                    # Check for Jikan data
                    if volume_info.get('jikan_data'):
                        type_value = "Manga"
                    # Check for Wookieepedia data
                    elif volume_info.get('wookieepedia_data'):
                        type_value = "Comic"
                    # Check for ComicVine data
                    elif volume_info.get('comicvine_data'):
                        type_value = "Comic"
                    # Check for Google Books with comics content
                    elif volume_info.get('comicsContent') is True:
                        type_value = "Comic"
                    
                    properties[property_key] = {
                        'select': {'name': type_value}
                    }
            
        except Exception as e:
            logger.error(f"Error formatting properties: {e}")
        
        return properties
    
    def _get_property_key(self, property_id: str) -> Optional[str]:
        """Get the property key for a given property ID."""
        return self.property_id_to_key.get(property_id)
    
    def _parse_google_books_url(self, url: str) -> Optional[str]:
        """
        Parse Google Books URL and extract Volume ID.
        
        Supports formats:
        - https://www.google.com/books/edition/TITLE/VOLUME_ID
        - https://books.google.com/books?id=VOLUME_ID
        - https://play.google.com/store/books/details?id=VOLUME_ID
        
        Returns: volume_id or None
        """
        if not url:
            return None
        
        # Pattern for /edition/Title/VolumeID
        edition_pattern = r'/books/edition/[^/]+/([a-zA-Z0-9_-]+)'
        edition_match = re.search(edition_pattern, url)
        if edition_match:
            return edition_match.group(1)
        
        # Pattern for ?id=VolumeID or &id=VolumeID
        id_pattern = r'[?&]id=([a-zA-Z0-9_-]+)'
        id_match = re.search(id_pattern, url)
        if id_match:
            return id_match.group(1)
        
        logger.warning(f"Unable to parse Google Books URL: {url}")
        return None
    
    def _parse_mal_url(self, url: str) -> Optional[int]:
        """
        Parse MyAnimeList URL and extract MAL ID.
        
        Supports formats:
        - https://myanimelist.net/manga/104/Yotsuba_to
        - https://myanimelist.net/manga/104
        
        Returns: mal_id (int) or None
        """
        if not url:
            return None
        
        # Pattern for /manga/{mal_id}/ or /manga/{mal_id}
        mal_pattern = r'/manga/(\d+)'
        mal_match = re.search(mal_pattern, url)
        if mal_match:
            return int(mal_match.group(1))
        
        logger.warning(f"Unable to parse MyAnimeList URL: {url}")
        return None
    
    def create_from_google_books_url(self, google_books_url: str) -> Dict:
        """
        Create a new Notion page from a Google Books URL.
        
        Args:
            google_books_url: Google Books URL
        
        Returns:
            Dict with keys: success, message, page_id, created
        """
        logger.info(f"Creating page from Google Books URL: {google_books_url}")
        
        # Parse URL to extract Volume ID
        volume_id = self._parse_google_books_url(google_books_url)
        if not volume_id:
            return {
                'success': False,
                'message': f'Invalid Google Books URL format: {google_books_url}'
            }
        
        logger.info(f"Extracted Volume ID: {volume_id}")
        
        # Fetch book data from Google Books API
        book_data = self.google_books.get_book_details(volume_id)
        if not book_data:
            return {
                'success': False,
                'message': f'Could not fetch book data for Volume ID: {volume_id}'
            }
        
        volume_info = book_data.get('volumeInfo', {})
        book_title = volume_info.get('title', '')
        
        if not book_title:
            return {
                'success': False,
                'message': f'No title found in Google Books data for Volume ID: {volume_id}'
            }
        
        logger.info(f"Found book: {book_title}")
        
        # Check for duplicates by Google Books ID
        google_books_id_prop_id = self.property_mapping.get('google_books_id_property_id')
        if google_books_id_prop_id:
            google_books_id_prop_key = self._get_property_key(google_books_id_prop_id)
            if google_books_id_prop_key:
                existing_page_id = find_page_by_property(
                    self.notion,
                    self.database_id,
                    google_books_id_prop_key,
                    'rich_text',
                    volume_id
                )
                
                if existing_page_id:
                    # Validate that the existing page's title matches
                    page = self.notion.get_page(existing_page_id)
                    if page:
                        title_prop_id = self.property_mapping.get('title_property_id')
                        title_key = self._get_property_key(title_prop_id)
                        if title_key:
                            existing_page_title_prop = page.get('properties', {}).get(title_key, {})
                            if existing_page_title_prop.get('title') and existing_page_title_prop['title']:
                                existing_title = existing_page_title_prop['title'][0]['plain_text']
                                # Check if titles match (case-insensitive)
                                if existing_title.lower() == book_title.lower():
                                    logger.info(f"Book already exists in Notion: {existing_page_id}")
                                    # Update the existing page
                                    self.sync_page(page, force_update=True)
                                    
                                    # Explicitly set Google Books URL
                                    url_prop_id = self.property_mapping.get('google_books_url_property_id')
                                    if url_prop_id:
                                        url_key = self._get_property_key(url_prop_id)
                                        if url_key:
                                            self.notion.update_page(existing_page_id, {url_key: {'url': google_books_url}})
                                    
                                    return {
                                        'success': True,
                                        'message': f'Updated existing book: {book_title}',
                                        'page_id': existing_page_id,
                                        'created': False
                                    }
                                else:
                                    # Titles don't match - bad data
                                    logger.warning(f"Volume ID {volume_id} has title '{existing_title}' but requested title is '{book_title}'. Ignoring bad Volume ID and creating new page.")
        
        # Create new page with full metadata
        properties = self.format_notion_properties(book_data)
        
        # Add title (format_notion_properties skips it for updates, but we need it for creation)
        title_prop_id = self.property_mapping.get('title_property_id')
        if title_prop_id and book_title:
            title_key = self._get_property_key(title_prop_id)
            if title_key:
                properties[title_key] = {
                    'title': [{'text': {'content': book_title}}]
                }
        
        # Add Google Books ID
        if google_books_id_prop_id:
            google_books_id_prop_key = self._get_property_key(google_books_id_prop_id)
            if google_books_id_prop_key:
                properties[google_books_id_prop_key] = {
                    'rich_text': [{'text': {'content': volume_id}}]
                }
        
        # Set DNS checkbox to prevent automation cascade
        dns_prop_id = self.property_mapping.get('dns_property_id')
        if dns_prop_id:
            dns_key = self._get_property_key(dns_prop_id)
            if dns_key:
                properties[dns_key] = {'checkbox': True}
                logger.info("Setting DNS checkbox to prevent automation cascade")
        
        # Get cover image URL
        cover_url = self.google_books.get_cover_url(book_data)
        
        # Set icon (book emoji)
        icon = '📚'
        
        # Create the page
        page_id = self.notion.create_page(self.database_id, properties, cover_url, icon)
        
        if not page_id:
            return {
                'success': False,
                'message': f'Failed to create Notion page for: {book_title}'
            }
        
        logger.info(f"Successfully created page: {page_id}")
        
        # Explicitly set Google Books URL (separate update call)
        url_prop_id = self.property_mapping.get('google_books_url_property_id')
        if url_prop_id:
            url_key = self._get_property_key(url_prop_id)
            if url_key:
                self.notion.update_page(page_id, {url_key: {'url': google_books_url}})
                logger.info(f"Set Google Books URL: {google_books_url}")
        
        return {
            'success': True,
            'message': f'Created book page: {book_title}',
            'page_id': page_id,
            'created': True
        }
    
    def create_from_mal_url(self, mal_url: str) -> Dict:
        """
        Create a new Notion page from a MyAnimeList URL.
        
        Args:
            mal_url: MyAnimeList URL
        
        Returns:
            Dict with keys: success, message, page_id, created
        """
        logger.info(f"Creating page from MyAnimeList URL: {mal_url}")
        
        # Parse URL to extract MAL ID
        mal_id = self._parse_mal_url(mal_url)
        if not mal_id:
            return {
                'success': False,
                'message': f'Invalid MyAnimeList URL format: {mal_url}'
            }
        
        logger.info(f"Extracted MAL ID: {mal_id}")
        
        # Fetch manga data from Jikan API
        jikan_data = self.google_books.jikan.get_manga_details(mal_id)
        if not jikan_data:
            return {
                'success': False,
                'message': f'Could not fetch manga data for MAL ID: {mal_id}'
            }
        
        manga_title = jikan_data.get('title', '')
        
        if not manga_title:
            return {
                'success': False,
                'message': f'No title found in Jikan data for MAL ID: {mal_id}'
            }
        
        logger.info(f"Found manga: {manga_title}")
        
        # Check for duplicates by Jikan ID (MAL ID)
        jikan_id_prop_id = self.property_mapping.get('jikan_id_property_id')
        if jikan_id_prop_id:
            jikan_id_prop_key = self._get_property_key(jikan_id_prop_id)
            if jikan_id_prop_key:
                existing_page_id = find_page_by_property(
                    self.notion,
                    self.database_id,
                    jikan_id_prop_key,
                    'rich_text',
                    str(mal_id)
                )
                
                if existing_page_id:
                    # Validate that the existing page's title matches
                    page = self.notion.get_page(existing_page_id)
                    if page:
                        title_prop_id = self.property_mapping.get('title_property_id')
                        title_key = self._get_property_key(title_prop_id)
                        if title_key:
                            existing_page_title_prop = page.get('properties', {}).get(title_key, {})
                            if existing_page_title_prop.get('title') and existing_page_title_prop['title']:
                                existing_title = existing_page_title_prop['title'][0]['plain_text']
                                # Check if titles match (case-insensitive)
                                if existing_title.lower() == manga_title.lower():
                                    logger.info(f"Manga already exists in Notion: {existing_page_id}")
                                    # Update the existing page
                                    self.sync_page(page, force_update=True)
                                    
                                    # Explicitly set MAL URL
                                    url_prop_id = self.property_mapping.get('google_books_url_property_id')
                                    if url_prop_id:
                                        url_key = self._get_property_key(url_prop_id)
                                        if url_key:
                                            self.notion.update_page(existing_page_id, {url_key: {'url': mal_url}})
                                    
                                    return {
                                        'success': True,
                                        'message': f'Updated existing manga: {manga_title}',
                                        'page_id': existing_page_id,
                                        'created': False
                                    }
                                else:
                                    # Titles don't match - bad data
                                    logger.warning(f"MAL ID {mal_id} has title '{existing_title}' but requested title is '{manga_title}'. Ignoring bad MAL ID and creating new page.")
        
        # Create manga data structure from Jikan data
        book_data = self.google_books._create_manga_data_from_jikan(jikan_data)
        
        # Create new page with full metadata
        properties = self.format_notion_properties(book_data)
        
        # Add title (format_notion_properties skips it for updates, but we need it for creation)
        title_prop_id = self.property_mapping.get('title_property_id')
        if title_prop_id and manga_title:
            title_key = self._get_property_key(title_prop_id)
            if title_key:
                properties[title_key] = {
                    'title': [{'text': {'content': manga_title}}]
                }
        
        # Add Jikan ID (MAL ID)
        if jikan_id_prop_id:
            jikan_id_prop_key = self._get_property_key(jikan_id_prop_id)
            if jikan_id_prop_key:
                properties[jikan_id_prop_key] = {
                    'rich_text': [{'text': {'content': str(mal_id)}}]
                }
        
        # Set Type to "Manga"
        type_prop_id = self.property_mapping.get('type_property_id')
        if type_prop_id:
            type_key = self._get_property_key(type_prop_id)
            if type_key:
                properties[type_key] = {'select': {'name': 'Manga'}}
        
        # Set DNS checkbox to prevent automation cascade
        dns_prop_id = self.property_mapping.get('dns_property_id')
        if dns_prop_id:
            dns_key = self._get_property_key(dns_prop_id)
            if dns_key:
                properties[dns_key] = {'checkbox': True}
                logger.info("Setting DNS checkbox to prevent automation cascade")
        
        # Get cover image URL
        cover_url = None
        jikan_images = book_data.get('volumeInfo', {}).get('jikan_images', {})
        if jikan_images:
            # Try to get the best quality image
            if 'jpg' in jikan_images and 'large_image_url' in jikan_images['jpg']:
                cover_url = jikan_images['jpg']['large_image_url']
            elif 'jpg' in jikan_images and 'image_url' in jikan_images['jpg']:
                cover_url = jikan_images['jpg']['image_url']
        
        # Set icon (manga emoji)
        icon = '🗯️'
        
        # Create the page
        page_id = self.notion.create_page(self.database_id, properties, cover_url, icon)
        
        if not page_id:
            return {
                'success': False,
                'message': f'Failed to create Notion page for: {manga_title}'
            }
        
        logger.info(f"Successfully created page: {page_id}")
        
        # Explicitly set MAL URL (separate update call)
        url_prop_id = self.property_mapping.get('google_books_url_property_id')
        if url_prop_id:
            url_key = self._get_property_key(url_prop_id)
            if url_key:
                self.notion.update_page(page_id, {url_key: {'url': mal_url}})
                logger.info(f"Set MyAnimeList URL: {mal_url}")
        
        return {
            'success': True,
            'message': f'Created manga page: {manga_title}',
            'page_id': page_id,
            'created': True
        }
    
    def run_sync(self, force_icons: bool = False, force_update: bool = False, max_workers: int = 3, dry_run: bool = False, google_books_url: Optional[str] = None, mal_url: Optional[str] = None, created_after: Optional[str] = None) -> Dict:
        """Run the complete synchronization process."""
        # MAL URL creation mode takes precedence
        if mal_url:
            return self.create_from_mal_url(mal_url)
        
        # Google Books URL creation mode
        if google_books_url:
            return self.create_from_google_books_url(google_books_url)
        
        logger.info("Starting Notion-Google Books synchronization")
        logger.info(f"Using {max_workers} parallel workers for processing")
        
        # Validate that required properties are found
        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}
        
        start_time = time.time()
        pages = self.get_notion_pages(created_after=created_after)
        
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
                executor.submit(self.sync_page, page, force_icons, force_update, dry_run): page 
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

    def run_page_sync(
        self,
        page_id: str,
        *,
        force_icons: bool = False,
        force_update: bool = False,
        dry_run: bool = False,
    ) -> Dict:
        """Run synchronization for a single explicit page."""
        logger.info("Starting sync for page %s", page_id)

        if not self.property_mapping['title_property_id']:
            logger.error("Cannot proceed: No title property found in database")
            return {'success': False, 'message': 'No title property found'}

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

        start_time = time.time()
        result = self.sync_page(page, force_icons, force_update, dry_run)
        duration = time.time() - start_time

        if result is True:
            logger.info("Page %s synced successfully in %.2f seconds", page_id, duration)
            return {
                'success': True,
                'total_pages': 1,
                'successful_updates': 1,
                'failed_updates': 0,
                'skipped_updates': 0,
                'duration': duration,
            }
        if result is False:
            logger.error("Page %s failed to sync", page_id)
            return {
                'success': False,
                'total_pages': 1,
                'successful_updates': 0,
                'failed_updates': 1,
                'skipped_updates': 0,
                'duration': duration,
            }

        logger.info("Page %s was skipped (no changes)", page_id)
        return {
            'success': True,
            'total_pages': 1,
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 1,
            'duration': duration,
        }


def validate_environment():
    """Validate environment variables and configuration."""
    errors = []
    
    # Check required environment variables
    notion_token = get_notion_token()
    database_id = get_database_id('NOTION_BOOKS_DATABASE_ID', 'NOTION_DATABASE_ID')
    
    if not notion_token:
        errors.append("NOTION_INTERNAL_INTEGRATION_SECRET (or legacy NOTION_TOKEN)")
    if not database_id:
        errors.append("NOTION_BOOKS_DATABASE_ID (or NOTION_DATABASE_ID)")
    
    if errors:
        logger.error("Missing required environment variables:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease check your .env file or environment variables.")
        return False
    
    # Validate database ID format (should be 32 characters)
    if database_id and len(database_id.replace('-', '')) != 32:
        logger.warning("Notion database ID format seems incorrect")
    
    return True


def _build_sync_instance(comicvine_scrape: bool = False) -> NotionGoogleBooksSync:
    notion_token = get_notion_token()
    database_id = get_database_id('NOTION_BOOKS_DATABASE_ID', 'NOTION_DATABASE_ID')
    google_books_api_key = os.getenv('GOOGLE_BOOKS_API_KEY')
    comicvine_api_key = os.getenv('COMICVINE_API_KEY')
    return NotionGoogleBooksSync(
        notion_token,
        google_books_api_key,
        database_id,
        comicvine_api_key,
        comicvine_scrape=comicvine_scrape,
    )


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
    comicvine_scrape: bool = False,
    dry_run: bool = False,
    google_books_url: Optional[str] = None,
    mal_url: Optional[str] = None,
    created_after: Optional[str] = None,
) -> Dict:
    """Run the Books sync with the provided options."""
    # MAL URL creation mode takes precedence
    if mal_url and not page_id:
        sync = _build_sync_instance(comicvine_scrape=comicvine_scrape)
        return sync.create_from_mal_url(mal_url)
    
    # Google Books URL creation mode
    if google_books_url and not page_id:
        sync = _build_sync_instance(comicvine_scrape=comicvine_scrape)
        return sync.create_from_google_books_url(google_books_url)
    
    enforce_worker_limits(workers)

    if page_id and last_page:
        raise RuntimeError("page-id mode cannot be combined with last-page mode")
    
    if page_id and created_after:
        logger.warning("--created-after is ignored when --page-id is provided")

    sync = _build_sync_instance(comicvine_scrape=comicvine_scrape)

    if page_id:
        return sync.run_page_sync(
            page_id,
            force_icons=force_icons,
            force_update=force_update,
            dry_run=dry_run,
        )

    if last_page:
        return sync.sync_last_page(
            force_icons=force_icons,
            force_update=force_update,
            dry_run=dry_run,
        )

    return sync.run_sync(
        force_icons=force_icons,
        force_update=force_update,
        max_workers=workers,
        dry_run=dry_run,
        created_after=created_after,
    )


def get_database_ids() -> List[str]:
    """Return normalized database IDs served by this sync."""
    database_id = get_database_id('NOTION_BOOKS_DATABASE_ID', 'NOTION_DATABASE_ID')
    normalized = normalize_id(database_id) if database_id else None
    return [normalized] if normalized else []
