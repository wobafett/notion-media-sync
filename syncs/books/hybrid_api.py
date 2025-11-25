"""
Hybrid API client that combines Google Books with specialized APIs for comics and manga.
"""

import requests
import time
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BaseAPI:
    """Base API class with common functionality."""
    
    def __init__(self, request_delay: float = 1.0):
        self.request_delay = request_delay
        self.last_request_time = 0.0
    
    def _rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()


class JikanAPI(BaseAPI):
    """Jikan API client for manga/anime data (MyAnimeList wrapper)."""
    
    def __init__(self):
        super().__init__(request_delay=0.5)
        self.base_url = "https://api.jikan.moe/v4"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NotionBooksSync/1.0'
        })
        
    def search_manga(self, title: str) -> Optional[Dict]:
        """Search for manga by title with retry logic."""
        max_retries = 3
        
        for attempt in range(max_retries + 1):
            try:
                
                # Apply rate limiting
                self._rate_limit()
                
                # Search Jikan API
                params = {'q': title, 'type': 'manga'}
                response = self.session.get(f"{self.base_url}/manga", params=params)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + 1  # Exponential backoff: 2s, 3s, 5s
                        logger.warning(f"Jikan search rate limited (429). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Jikan search rate limit exceeded after {max_retries} retries for '{title}'")
                        return None
                
                # Handle other HTTP errors
                if response.status_code >= 400:
                    if attempt < max_retries:
                        wait_time = 1 + (attempt * 0.5)
                        logger.warning(f"Jikan search HTTP {response.status_code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Jikan search HTTP {response.status_code} error after {max_retries} retries for '{title}'")
                        return None
                
                # Success - parse response
                response.raise_for_status()
                data = response.json()
                
                if data.get('data'):
                    # Log search results
                    logger.info(f"Jikan search results for '{title}':")
                    for i, manga in enumerate(data['data'][:3]):
                        manga_title = manga.get('title', 'Unknown')
                        mal_id = manga.get('mal_id', 'Unknown')
                        logger.info(f"  {i+1}. {manga_title} (MAL ID: {mal_id})")
                    
                    # Select the best match (first result is usually best)
                    best_manga = data['data'][0] if data['data'] else None
                    
                    if best_manga:
                        logger.info(f"Selected Jikan result: {best_manga['title']} (MAL ID: {best_manga['mal_id']})")
                        return best_manga
                
                logger.warning(f"No Jikan results found for '{title}'")
                return None
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 1 + (attempt * 0.5)
                    logger.warning(f"Jikan search error for '{title}': {e}. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error searching Jikan for '{title}': {e}")
                    return None
        
        return None
    
    def get_manga_details(self, mal_id: int) -> Optional[Dict]:
        """Get detailed information for a manga by MAL ID with retry logic."""
        max_retries = 3
        
        for attempt in range(max_retries + 1):
            try:
                
                # Apply rate limiting
                self._rate_limit()
                
                # Get manga details
                response = self.session.get(f"{self.base_url}/manga/{mal_id}")
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + 1  # Exponential backoff: 2s, 3s, 5s
                        logger.warning(f"Jikan rate limited (429). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Jikan rate limit exceeded after {max_retries} retries for MAL ID {mal_id}")
                        return None
                
                # Handle other HTTP errors
                if response.status_code >= 400:
                    if attempt < max_retries:
                        wait_time = 1 + (attempt * 0.5)
                        logger.warning(f"Jikan HTTP {response.status_code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Jikan HTTP {response.status_code} error after {max_retries} retries for MAL ID {mal_id}")
                        return None
                
                # Success - parse response
                response.raise_for_status()
                data = response.json()
                
                if data.get('data'):
                    return data['data']
                
                return None
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 1 + (attempt * 0.5)
                    logger.warning(f"Jikan error for MAL ID {mal_id}: {e}. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error getting Jikan manga details for MAL ID {mal_id}: {e}")
                    return None
        
        return None


class ComicVineAPI(BaseAPI):
    """ComicVine API client for comics data."""
    
    def __init__(self, api_key: str, force_scraping: bool = False):
        super().__init__(request_delay=3.0)
        self.api_key = api_key
        self.force_scraping = force_scraping
        self.base_url = "https://comicvine.gamespot.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NotionBooksSync/1.0'
        })
        
    def search_volumes(self, title: str) -> Optional[Dict]:
        """Search for comic volumes by title."""
        try:
            
            # Apply rate limiting
            self._rate_limit()
            
            # Search ComicVine API
            params = {
                'api_key': self.api_key,
                'format': 'json',
                'query': title,
                'resources': 'volume',
                'limit': 10
            }
            
            response = self.session.get(f"{self.base_url}/search/", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('results'):
                # Log search results with more details
                logger.info(f"ComicVine search results for '{title}':")
                for i, volume in enumerate(data['results'][:3]):
                    volume_name = volume.get('name', 'Unknown')
                    volume_id = volume.get('id', 'Unknown')
                    start_year = volume.get('start_year', 'Unknown')
                    publisher = volume.get('publisher', {}).get('name', 'Unknown') if volume.get('publisher') else 'Unknown'
                    count_issues = volume.get('count_of_issues', 'Unknown')
                    logger.info(f"  {i+1}. {volume_name} (ID: {volume_id}, Year: {start_year}, Publisher: {publisher}, Issues: {count_issues})")
                
                # Select the best match using scoring
                best_volume = self._select_best_comicvine_result(title, data['results'])
                
                if best_volume:
                    # Get detailed volume information including concepts
                    volume_id = best_volume.get('id')
                    if volume_id:
                        detailed_volume = self.get_volume_details(volume_id)
                        if detailed_volume:
                            # Always scrape when doing hybrid search (ComicVine ID is empty)
                            # This ensures we get complete data for new entries
                            # Note: force_scraping flag doesn't apply here since this is already automatic scraping
                            themes = self.scrape_volume_themes(volume_id)
                            if themes:
                                detailed_volume['scraped_themes'] = themes
                                logger.info(f"Scraped {len(themes)} themes for volume {volume_id}")
                            
                            creators = self.scrape_volume_creators(volume_id)
                            if creators:
                                detailed_volume['scraped_creators'] = creators
                                logger.info(f"Scraped {len(creators)} creators for volume {volume_id}")
                            
                            logger.info(f"Selected ComicVine result: {detailed_volume['name']} (ID: {detailed_volume['id']})")
                            return detailed_volume
                    
                    # Fallback to basic search result if detailed call fails
                    logger.info(f"Selected ComicVine result: {best_volume['name']} (ID: {best_volume['id']})")
                    return best_volume
            
            logger.warning(f"No ComicVine results found for '{title}'")
            return None
            
        except Exception as e:
            logger.error(f"Error searching ComicVine for '{title}': {e}")
            return None
    
    def _select_best_comicvine_result(self, search_title: str, results: List[Dict]) -> Optional[Dict]:
        """Select the best ComicVine result based on relevance scoring."""
        if not results:
            return None
        
        if len(results) == 1:
            return results[0]
        
        best_result = None
        best_score = -1
        
        logger.info(f"ComicVine scoring for '{search_title}':")
        
        for result in results:
            score = self._calculate_comicvine_score(search_title, result)
            logger.info(f"  {result.get('name', 'Unknown')} (ID: {result.get('id', 'Unknown')}): {score:.1f} points")
            
            if score > best_score:
                best_score = score
                best_result = result
        
        logger.info(f"Best ComicVine result: {best_result.get('name', 'Unknown')} with {best_score:.1f} points")
        return best_result
    
    def _calculate_comicvine_score(self, search_title: str, result: Dict) -> float:
        """Calculate relevance score for ComicVine result."""
        try:
            score = 0.0
            
            # Title relevance (most important)
            result_title = result.get('name', '').lower()
            search_title_lower = search_title.lower()
            
            if search_title_lower == result_title:
                score += 100.0  # Exact match
            elif search_title_lower in result_title:
                score += 80.0   # Search term contained in result
            elif result_title in search_title_lower:
                score += 60.0   # Result contained in search term
            
            # Word overlap bonus
            search_words = set(search_title_lower.split())
            result_words = set(result_title.split())
            if search_words and result_words:
                overlap_ratio = len(search_words.intersection(result_words)) / len(search_words.union(result_words))
                score += overlap_ratio * 40.0
            
            # Publisher bonus (prefer major publishers)
            publisher = result.get('publisher', {}).get('name', '').lower() if result.get('publisher') else ''
            major_publishers = ['dc comics', 'marvel', 'image', 'dark horse', 'vertigo', 'boom! studios']
            if any(major in publisher for major in major_publishers):
                score += 20.0
            
            # Issue count bonus (prefer longer series)
            count_issues = result.get('count_of_issues', 0)
            if count_issues and count_issues > 0:
                score += min(count_issues * 0.5, 25.0)  # Cap at 25 points
            
            # Start year bonus (prefer more recent series for similar titles)
            start_year = result.get('start_year')
            if start_year and isinstance(start_year, (int, str)):
                try:
                    year = int(start_year)
                    # For very similar titles, prefer more recent series
                    if year >= 2020:  # Prefer recent series (2020+)
                        score += 15.0
                    elif year >= 2010:  # Moderate bonus for 2010-2019
                        score += 5.0
                    # No bonus for very old series (pre-2010)
                except ValueError:
                    pass
            
            return score
            
        except Exception as e:
            logger.error(f"Error calculating ComicVine score: {e}")
            return 0.0
    
    def _strip_html(self, html_text: str) -> str:
        """Strip HTML tags and clean up text for plain text display."""
        if not html_text:
            return ''
        
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', html_text)
        
        # Clean up extra whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text)
        
        # Remove any remaining HTML entities
        clean_text = clean_text.replace('&nbsp;', ' ')
        clean_text = clean_text.replace('&amp;', '&')
        clean_text = clean_text.replace('&lt;', '<')
        clean_text = clean_text.replace('&gt;', '>')
        clean_text = clean_text.replace('&quot;', '"')
        
        return clean_text.strip()
    
    def _truncate_description(self, description: str, max_length: int = 2000) -> str:
        """Truncate description to fit Notion's character limit."""
        if not description:
            return ''
        
        if len(description) <= max_length:
            return description
        
        # Truncate and add ellipsis
        truncated = description[:max_length - 3] + "..."
        logger.info(f"Truncated description from {len(description)} to {len(truncated)} characters")
        return truncated
    
    def get_issue_date(self, issue_id: int) -> Optional[str]:
        """Get the publication date of a specific issue."""
        try:
            self._rate_limit()
            
            params = {
                'api_key': self.api_key,
                'format': 'json',
                'field_list': 'id,name,cover_date,store_date'
            }
            
            response = self.session.get(f"{self.base_url}/issue/4000-{issue_id}/", params=params)
            response.raise_for_status()
            
            data = response.json()
            result = data.get('results')
            
            if result:
                # Try cover_date first, then store_date as fallback
                cover_date = result.get('cover_date')
                store_date = result.get('store_date')
                
                if cover_date:
                    return cover_date
                elif store_date:
                    return store_date
                else:
                    logger.warning(f"No date found for issue {issue_id}")
                    return None
            else:
                logger.warning(f"No results found for issue {issue_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting issue date for {issue_id}: {e}")
            return None
    
    def get_volume_details(self, volume_id: int) -> Optional[Dict]:
        """Get detailed volume information including concepts."""
        try:
            
            # Apply rate limiting
            self._rate_limit()
            
            # Get detailed volume information
            params = {
                'api_key': self.api_key,
                'format': 'json',
                'field_list': 'id,name,description,start_year,end_date,last_issue,publisher,count_of_issues,person_credits,concepts,image,site_detail_url,api_detail_url'
            }
            
            response = self.session.get(f"{self.base_url}/volume/4050-{volume_id}/", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('results'):
                volume_data = data['results']
                logger.info(f"Retrieved detailed ComicVine data for volume ID {volume_id}")
                return volume_data
            
            logger.warning(f"No detailed ComicVine data found for volume ID {volume_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting ComicVine volume details for ID {volume_id}: {e}")
            return None
    
    def scrape_volume_themes(self, volume_id: int) -> List[str]:
        """Scrape themes from ComicVine website page."""
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Get the volume page
            url = f"https://comicvine.gamespot.com/the-sandman/4050-{volume_id}/"
            response = self.session.get(url)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for themes in the volume details section
            themes = []
            
            # Debug: Look for any table with volume details
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables on ComicVine page")
            
            # Try different approaches to find themes
            # Method 1: Look for "Themes" text and find nearby links
            theme_elements = soup.find_all(text=re.compile(r'Themes?', re.I))
            for element in theme_elements:
                parent = element.parent
                if parent:
                    # Look for links in the same row or nearby
                    links = parent.find_all('a')
                    for link in links:
                        theme_name = link.get_text(strip=True)
                        if theme_name and len(theme_name) > 1:
                            themes.append(theme_name)
            
            # Method 2: Look for volume details table
            volume_details = soup.find('table')
            if volume_details:
                rows = volume_details.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        first_cell = cells[0].get_text(strip=True).lower()
                        if 'themes' in first_cell:
                            # Extract theme links from second cell
                            theme_links = cells[1].find_all('a')
                            for link in theme_links:
                                theme_name = link.get_text(strip=True)
                                if theme_name:
                                    themes.append(theme_name)
            
            # Remove duplicates while preserving order
            unique_themes = list(dict.fromkeys(themes))
            
            logger.info(f"Scraped {len(unique_themes)} themes from ComicVine page: {unique_themes}")
            return unique_themes
            
        except Exception as e:
            logger.error(f"Error scraping ComicVine themes for volume {volume_id}: {e}")
            return []
    
    def scrape_volume_creators(self, volume_id: int) -> List[Dict[str, Any]]:
        """Scrape creator credits from ComicVine dedicated credits page."""
        try:
            import re
            # Apply rate limiting
            self._rate_limit()
            
            # Get the total issue count from volume details first
            total_issues = None
            try:
                volume_details = self.get_volume_details(volume_id)
                if volume_details:
                    total_issues = volume_details.get('count_of_issues')
                    logger.info(f"Using issue count from volume details: {total_issues}")
            except Exception as e:
                logger.warning(f"Could not get issue count from volume details: {e}")
            
            # Try the dedicated credits page first
            creators = self._scrape_credits_page(volume_id, total_issues)
            
            # If no creators found on credits page, fall back to main volume page
            if not creators:
                logger.info("No creators found on credits page, trying main volume page")
                creators = self._scrape_main_page_creators(volume_id, total_issues)
            
            logger.info(f"Scraped {len(creators)} creators from ComicVine: {creators}")
            return creators
            
        except Exception as e:
            logger.error(f"Error scraping ComicVine creators for volume {volume_id}: {e}")
            return []
    
    def _scrape_credits_page(self, volume_id: int, total_issues: Optional[int]) -> List[Dict[str, Any]]:
        """Scrape creators from the dedicated credits page."""
        try:
            import re
            # Apply rate limiting
            self._rate_limit()
            
            # Get the dedicated credits page
            # First, get the volume name from the API to construct the correct URL
            volume_details = self.get_volume_details(volume_id)
            if not volume_details:
                logger.warning(f"Could not get volume details for {volume_id}, skipping credits page")
                return []
            
            volume_name = volume_details.get('name', '').lower()
            # Convert to URL-friendly format (replace spaces with hyphens, remove special chars)
            import re
            url_name = re.sub(r'[^a-z0-9\s-]', '', volume_name)
            url_name = re.sub(r'\s+', '-', url_name.strip())
            
            url = f"https://comicvine.gamespot.com/{url_name}/4050-{volume_id}/credits/"
            response = self.session.get(url)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            creators = []
            
            # Look for the credits section - it should contain creator names with credit counts
            # Based on the Star Wars page structure, creators are listed with their credit counts
            
            # Method 1: Look for creator entries with credit counts
            # Based on the Star Wars page structure: <a><h3>Name</h3>Number Description</a>
            creator_links = soup.find_all('a')
            
            for link in creator_links:
                # Look for h3 tags within the link (creator names)
                h3_tags = link.find_all('h3')
                if h3_tags:
                    for h3 in h3_tags:
                        name = h3.get_text(strip=True)
                        if not name or len(name) < 2:
                            continue
                            
                        # Look for a number immediately after the h3 tag
                        # The structure is: <h3>Name</h3>Number Description
                        h3_text = h3.get_text()
                        link_text = link.get_text()
                        
                        # Find the position of the name in the link text
                        name_pos = link_text.find(name)
                        if name_pos != -1:
                            # Get text after the name
                            after_name = link_text[name_pos + len(name):].strip()
                            
                            # Extract the first number from the text after the name
                            number_match = re.match(r'^(\d+)', after_name)
                            if number_match:
                                try:
                                    credit_count = int(number_match.group(1))
                                    
                                    # Filter for reasonable names and credit counts
                                    if (len(name) > 2 and 
                                        credit_count > 0 and 
                                        credit_count <= 1000 and  # Reasonable upper bound
                                        not name.lower() in ['credits', 'results', 'volume', 'details', 'navigation', 'summary']):
                                        
                                        creators.append({
                                            'name': name,
                                            'credit_count': credit_count
                                        })
                                        logger.info(f"Found creator on credits page: {name} ({credit_count})")
                                        
                                except ValueError:
                                    continue
            
            # Method 2: Look for specific HTML structure with creator names and counts
            # Look for elements that might contain creator information
            potential_creator_elements = soup.find_all(['div', 'span', 'p'], string=re.compile(r'\w+.*\d+'))
            
            for element in potential_creator_elements:
                text = element.get_text(strip=True)
                if not text:
                    continue
                    
                # Parse format like "Name Number"
                parts = text.split()
                if len(parts) >= 2:
                    try:
                        credit_count = int(parts[-1])
                        name = ' '.join(parts[:-1])
                        
                        # Additional validation for creator names
                        if (len(name) > 2 and 
                            credit_count > 0 and 
                            credit_count <= 1000 and
                            not name.lower() in ['credits', 'results', 'volume', 'details', 'navigation', 'summary']):
                            
                            # Check if this creator is already in our list
                            if not any(creator['name'] == name for creator in creators):
                                creators.append({
                                    'name': name,
                                    'credit_count': credit_count
                                })
                                logger.info(f"Found additional creator: {name} ({credit_count})")
                                
                    except ValueError:
                        continue
            
            # Filter creators based on total issues if available
            filtered_creators = []
            if total_issues:
                for creator in creators:
                    if creator['credit_count'] == total_issues:
                        filtered_creators.append(creator)
                        logger.info(f"Filtered creator with {total_issues} credits: {creator['name']}")
            else:
                # If we can't determine total issues, include creators with reasonable credit counts
                for creator in creators:
                    if 1 <= creator['credit_count'] <= 100:  # Reasonable range
                        filtered_creators.append(creator)
                        logger.info(f"Including creator with {creator['credit_count']} credits: {creator['name']}")
            
            logger.info(f"Found {len(filtered_creators)} creators on credits page")
            return filtered_creators
            
        except Exception as e:
            logger.error(f"Error scraping credits page for volume {volume_id}: {e}")
            return []
    
    def _scrape_main_page_creators(self, volume_id: int, total_issues: Optional[int]) -> List[Dict[str, Any]]:
        """Fallback method to scrape creators from the main volume page."""
        try:
            import re
            # Apply rate limiting
            self._rate_limit()
            
            # Get the main volume page
            # First, get the volume name from the API to construct the correct URL
            volume_details = self.get_volume_details(volume_id)
            if not volume_details:
                logger.warning(f"Could not get volume details for {volume_id}, skipping main page")
                return []
            
            volume_name = volume_details.get('name', '').lower()
            # Convert to URL-friendly format (replace spaces with hyphens, remove special chars)
            import re
            url_name = re.sub(r'[^a-z0-9\s-]', '', volume_name)
            url_name = re.sub(r'\s+', '-', url_name.strip())
            
            url = f"https://comicvine.gamespot.com/{url_name}/4050-{volume_id}/"
            response = self.session.get(url)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            creators = []
            
            # Look for "Most issue credits" section
            credits_section = soup.find(text=re.compile(r'Most issue credits', re.I))
            if credits_section:
                logger.info("Found 'Most issue credits' text on main page")
                
                # Find the parent element containing the credits list
                parent = credits_section.parent
                while parent and not parent.find('ul'):
                    parent = parent.parent
                
                if parent:
                    # Look for the unordered list with credits
                    credits_list = parent.find('ul')
                    if credits_list:
                        logger.info("Found credits list on main page")
                        # Extract creator names and credit counts
                        for item in credits_list.find_all('li'):
                            text = item.get_text(strip=True)
                            
                            # Skip the header text "Most issue credits"
                            if text.lower() == 'most issue credits':
                                continue
                                
                            # Parse format like "Neil Gaiman 75"
                            parts = text.split()
                            if len(parts) >= 2:
                                try:
                                    credit_count = int(parts[-1])
                                    name = ' '.join(parts[:-1])
                                    
                                    creators.append({
                                        'name': name,
                                        'credit_count': credit_count
                                    })
                                    logger.info(f"Found creator on main page: {name} ({credit_count})")
                                except ValueError:
                                    continue
            
            # Filter creators based on total issues if available
            filtered_creators = []
            if total_issues:
                for creator in creators:
                    if creator['credit_count'] == total_issues:
                        filtered_creators.append(creator)
                        logger.info(f"Filtered creator with {total_issues} credits: {creator['name']}")
            else:
                # If we can't determine total issues, include creators with reasonable credit counts
                for creator in creators:
                    if 5 <= creator['credit_count'] <= 100:  # Reasonable range
                        filtered_creators.append(creator)
                        logger.info(f"Including creator with {creator['credit_count']} credits: {creator['name']}")
            
            logger.info(f"Found {len(filtered_creators)} creators on main page")
            return filtered_creators
            
        except Exception as e:
            logger.error(f"Error scraping main page creators for volume {volume_id}: {e}")
            return []


class StarWarsFandomAPI:
    """Star Wars Fandom (Wookieepedia) API client for Star Wars comics data."""
    
    def __init__(self):
        self.base_url = "https://starwars.fandom.com/api.php"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NotionBooksSync/1.0'
        })
        
    
    def search_comic(self, title: str) -> Optional[Dict]:
        """Search for Star Wars comics by title."""
        try:
            logger.debug(f"Starting Wookieepedia search for '{title}'")
            
            
            # Try multiple search variations to find the best match
            search_variations = [
                title,  # Original title
                f"Star Wars: {title}",  # Add "Star Wars:" prefix
                title.replace('Star Wars: ', ''),  # Remove "Star Wars:" prefix
                title.replace(' (', ' ').replace(')', ''),  # Remove parentheses
                title.split(' (')[0],  # Remove year in parentheses
            ]
            
            # Remove duplicates while preserving order
            search_variations = list(dict.fromkeys(search_variations))
            
            best_data = None
            best_match_score = 0
            
            for search_term in search_variations:
                logger.debug(f"Trying Wookieepedia search: '{search_term}'")
                
                # Search Wookieepedia API
                params = {
                    'action': 'query',
                    'format': 'json',
                    'list': 'search',
                    'srsearch': search_term,
                    'srlimit': 5
                }
                
                logger.debug(f"Making API request to {self.base_url}")
                response = self.session.get(self.base_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('query', {}).get('search'):
                    search_results = data['query']['search']
                    logger.info(f"Wookieepedia search results for '{search_term}':")
                    for i, result in enumerate(search_results[:3]):
                        result_title = result.get('title', 'Unknown')
                        page_id = result.get('pageid', 'Unknown')
                        logger.info(f"  {i+1}. {result_title} (Page ID: {page_id})")
                    
                    # Check if this search found a better match
                    if search_results:
                        # Find the best result from all search results, not just the first one
                        best_result = None
                        best_score = 0
                        search_title_lower = title.lower()
                        
                        for result in search_results:
                            result_title = result.get('title', '').lower()
                            
                            # Calculate score for this result
                            if search_title_lower == result_title:
                                score = 100  # Exact match
                            elif search_title_lower in result_title:
                                score = 80   # Search term contained in result
                            elif result_title in search_title_lower:
                                score = 60   # Result contained in search term
                            else:
                                # Calculate word overlap
                                search_words = set(search_title_lower.split())
                                result_words = set(result_title.split())
                                if search_words and result_words:
                                    overlap_ratio = len(search_words.intersection(result_words)) / len(search_words.union(result_words))
                                    score = overlap_ratio * 50
                                else:
                                    score = 0
                            
                            # Apply bonus for series pages (prioritize series over issues)
                            # Check if this looks like an issue number (standalone number at end)
                            title_words = result_title.split()
                            is_issue = False
                            if title_words:
                                last_word = title_words[-1]
                                # Check if last word is just a number (like "5", "1", "12")
                                if last_word.isdigit():
                                    # Only consider it an issue if it's a small number (1-50)
                                    # Years would be much larger
                                    if int(last_word) <= 50:
                                        is_issue = True
                                # Check if last word is a number with parentheses (like "(5)", "(1)")
                                elif last_word.startswith('(') and last_word.endswith(')') and last_word[1:-1].isdigit():
                                    # Only consider it an issue if it's a small number (1-50)
                                    # Years would be much larger
                                    if int(last_word[1:-1]) <= 50:
                                        is_issue = True
                            
                            if is_issue:
                                # This looks like an issue number - no penalty
                                pass
                            else:
                                # Apply strong bonus for series pages (no issue numbers)
                                score += 50  # Strong bonus for series pages
                            
                            if score > best_score:
                                best_score = score
                                best_result = result
                        
                        
                        # Keep the best match
                        if best_score > best_match_score:
                            best_match_score = best_score
                            best_data = data
                            
                        # If we found an exact match, use it immediately
                        if best_score == 100:
                            logger.info(f"Found exact match with '{search_term}', using it")
                            # Return the data structure with only the best result
                            modified_data = data.copy()
                            modified_data['query']['search'] = [best_result]
                            return modified_data
                else:
                    pass
            
            # Return the best match found
            if best_data:
                logger.info(f"Using best match with score {best_match_score}")
                # Extract the best result from the search data
                search_results = best_data.get('query', {}).get('search', [])
                if search_results:
                    # Find the best result again (since we need to return the actual result object)
                    best_result = None
                    best_score = 0
                    search_title_lower = title.lower()
                    
                    for result in search_results:
                        result_title = result.get('title', '').lower()
                        
                        # Calculate score for this result (same logic as above)
                        if search_title_lower == result_title:
                            score = 100
                        elif search_title_lower in result_title:
                            score = 80
                        elif result_title in search_title_lower:
                            score = 60
                        else:
                            search_words = set(search_title_lower.split())
                            result_words = set(result_title.split())
                            if search_words and result_words:
                                overlap_ratio = len(search_words.intersection(result_words)) / len(search_words.union(result_words))
                                score = overlap_ratio * 50
                            else:
                                score = 0
                        
                        # Apply bonus for series pages (prioritize series over issues)
                        # Check if this looks like an issue number (standalone number at end)
                        title_words = result_title.split()
                        is_issue = False
                        if title_words:
                            last_word = title_words[-1]
                            # Check if last word is just a number (like "5", "1", "12")
                            if last_word.isdigit():
                                # Only consider it an issue if it's a small number (1-50)
                                # Years would be much larger
                                if int(last_word) <= 50:
                                    is_issue = True
                            # Check if last word is a number with parentheses (like "(5)", "(1)")
                            elif last_word.startswith('(') and last_word.endswith(')') and last_word[1:-1].isdigit():
                                # Only consider it an issue if it's a small number (1-50)
                                # Years would be much larger
                                if int(last_word[1:-1]) <= 50:
                                    is_issue = True
                        
                        if is_issue:
                            # This looks like an issue number - no penalty
                            pass
                        else:
                            # Apply strong bonus for series pages (no issue numbers)
                            score += 50  # Strong bonus for series pages
                        
                        if score > best_score:
                            best_score = score
                            best_result = result
                    
                    if best_result:
                        logger.info(f"Selected best result: '{best_result.get('title', 'Unknown')}' with score {best_score}")
                        # Return the data structure with only the best result
                        modified_data = best_data.copy()
                        modified_data['query']['search'] = [best_result]
                        return modified_data
                
                # Fallback to original data if something goes wrong
                return best_data
            
            logger.warning(f"No Wookieepedia results found for any variation of '{title}'")
            return None
            
        except Exception as e:
            logger.error(f"Error searching Wookieepedia for '{title}': {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def get_page_content(self, page_id: int) -> Optional[Dict]:
        """Get page content from Wookieepedia."""
        try:
            
            # Get page content using parse action
            params = {
                'action': 'parse',
                'format': 'json',
                'pageid': page_id,
                'prop': 'text',
                'section': 0
            }
            
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('parse'):
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting Wookieepedia page content for ID {page_id}: {e}")
            return None


class HybridBookAPI:
    """Hybrid API that combines Google Books with specialized APIs."""
    
    def __init__(self, google_books_api, jikan_api: JikanAPI, comicvine_api: ComicVineAPI, star_wars_fandom_api: StarWarsFandomAPI = None, content_type: str = None):
        self.google_books = google_books_api
        self.jikan = jikan_api
        self.comicvine = comicvine_api
        self.star_wars_fandom = star_wars_fandom_api or StarWarsFandomAPI()
        self.content_type = content_type  # 'book', 'comic', 'manga', or None
    
    def _should_handoff_google_books(self, google_data: Dict) -> bool:
        """Determine if Google Books should hand off to next API."""
        
        # Case 1: No data found at all (title not found)
        if not google_data:
            logger.info("Google Books: Title not found, handing off")
            return True
        
        # Case 2: comicsContent field is explicitly true
        comics_content = google_data.get('comicsContent', False)
        if comics_content:
            logger.info("Google Books: comicsContent=true, handing off")
            return True
        
        # Case 3: comicsContent field not present, check categories
        volume_info = google_data.get('volumeInfo', {})
        categories = volume_info.get('categories', [])
        
        if categories:
            # Check if any category contains comic-related terms
            comic_keywords = [
                "comics & graphic novels",
                "comic",
                "graphic novel",
                "graphic novels",
                "adult graphic novels",
                "superhero",
                "manga"
            ]
            
            for category in categories:
                # Split category by "/" to handle subcategories like "Comics & Graphic Novels / Superheroes"
                category_parts = [part.strip().lower() for part in category.split('/')]
                
                for category_part in category_parts:
                    for keyword in comic_keywords:
                        if keyword in category_part:
                            logger.info(f"Google Books: Found comic-related category '{category}' (matched '{category_part}'), handing off")
                            return True
        
        # Case 4: Check publisher for known comic publishers
        publisher = volume_info.get('publisher', '').lower()
        if publisher:
            comic_publishers = [
                'marvel',
                'dark horse comics',
                'idw',
                'dc'
            ]
            
            for comic_publisher in comic_publishers:
                if comic_publisher in publisher:
                    logger.info(f"Google Books: Found comic publisher '{volume_info.get('publisher', '')}', handing off")
                    return True
        
        # Case 5: Check series information for comic series
        series_info = volume_info.get('seriesInfo', {})
        if series_info:
            volume_series = series_info.get('volumeSeries', [])
            for series in volume_series:
                series_type = series.get('seriesBookType', '')
                if series_type == 'ORDERED_COMICS_SERIES':
                    logger.info(f"Google Books: Found comic series type '{series_type}', handing off")
                    return True
        
        # Case 6: Check structural indicators for comics (when no categories)
        if not categories:
            # Check reading modes - comics typically have text: false, image: true
            reading_modes = volume_info.get('readingModes', {})
            if (reading_modes.get('text') == False and 
                reading_modes.get('image') == True):
                logger.info("Google Books: Found comic reading mode (text: false, image: true), handing off")
                return True
            
            # Check panelization summary - comics have image bubbles
            panelization = volume_info.get('panelizationSummary', {})
            if panelization.get('containsImageBubbles') == True:
                logger.info("Google Books: Found image bubbles in panelization, handing off")
                return True
        
        # Case 7: None of the above - it's a regular book, keep Google Books data
        logger.info("Google Books: Regular book content, keeping data")
        return False
    
    def _should_handoff_wookieepedia(self, wookiee_data: Dict, search_title: str) -> bool:
        """Determine if Wookieepedia should hand off to next API."""
        
        # Case 1: No data found at all
        if not wookiee_data:
            logger.info("Wookieepedia: No results found, handing off")
            return True
        
        # Case 2: Check if the best result is relevant enough
        search_results = wookiee_data.get('query', {}).get('search', [])
        if not search_results:
            logger.info("Wookieepedia: No search results, handing off")
            return True
        
        # Filter for comic-specific content first
        comic_results = []
        for result in search_results:
            title = result.get('title', '').lower()
            page_id = result.get('pageid', '')
            
            # Look for comic-specific keywords
            comic_keywords = ['comic', 'issue', 'volume', 'series', 'adventures']
            has_comic_keyword = any(keyword in title for keyword in comic_keywords)
            
            # Also check for known comic series page IDs
            known_comic_pages = ['621368', '761547']  # Main series pages
            is_known_comic = str(page_id) in known_comic_pages
            
            # Include if it has comic keywords OR is a known comic page
            if has_comic_keyword or is_known_comic:
                comic_results.append(result)
                if is_known_comic:
                    logger.debug(f"Including known comic page: {result.get('title')} (ID: {page_id})")
        
        # If we found comic-specific results, use those instead
        if comic_results:
            search_results = comic_results
            logger.info(f"Wookieepedia: Found {len(comic_results)} comic-specific results, using those")
        
        # Get the best match (first result)
        best_match = search_results[0]
        best_title = best_match.get('title', '')
        
        # Calculate relevance score using the complete Notion title
        relevance_score = self._calculate_wookieepedia_relevance(search_title, best_title)
        
        # Hand off if relevance is too low
        if relevance_score < 0.7:  # Threshold for relevance
            logger.info(f"Wookieepedia: Relevance too low ({relevance_score:.2f}) for '{search_title}' vs '{best_title}', handing off")
            return True
        
        logger.info(f"Wookieepedia: Good relevance ({relevance_score:.2f}) for '{search_title}' vs '{best_title}', keeping data")
        return False
    
    def _should_handoff_jikan(self, jikan_data: Dict, search_title: str) -> bool:
        """Determine if Jikan should hand off to next API."""
        
        # Case 1: No data found at all
        if not jikan_data:
            logger.info("Jikan: No results found, handing off")
            return True
        
        # Case 2: Check if the result is relevant enough
        jikan_title = jikan_data.get('title', '')
        relevance_score = self._calculate_relevance_score(search_title, jikan_title)
        
        # Hand off if relevance is too low
        if relevance_score < 0.3:  # Threshold for relevance
            logger.info(f"Jikan: Relevance too low ({relevance_score:.2f}) for '{search_title}' vs '{jikan_title}', handing off")
            return True
        
        logger.info(f"Jikan: Good relevance ({relevance_score:.2f}) for '{search_title}' vs '{jikan_title}', keeping data")
        return False
    
    def _should_handoff_comicvine(self, comicvine_data: Dict, search_title: str) -> bool:
        """Determine if ComicVine should hand off to next API."""
        
        # Case 1: No data found at all
        if not comicvine_data:
            logger.info("ComicVine: No results found, handing off")
            return True
        
        # Case 2: Check if the result is relevant enough
        comicvine_title = comicvine_data.get('name', '')
        relevance_score = self._calculate_relevance_score(search_title, comicvine_title)
        
        # Hand off if relevance is too low
        if relevance_score < 0.3:  # Threshold for relevance
            logger.info(f"ComicVine: Relevance too low ({relevance_score:.2f}) for '{search_title}' vs '{comicvine_title}', handing off")
            return True
        
        logger.info(f"ComicVine: Good relevance ({relevance_score:.2f}) for '{search_title}' vs '{comicvine_title}', keeping data")
        return False
    
    def _is_star_wars_content(self, title: str) -> bool:
        """Check if the title appears to be Star Wars related."""
        star_wars_keywords = ['star wars', 'darth vader', 'luke skywalker', 'princess leia', 'han solo', 'yoda', 'obi-wan', 'anakin', 'padme', 'clone wars', 'rebels', 'mandalorian', 'boba fett', 'jedi', 'sith', 'force', 'empire', 'rebellion', 'high republic']
        
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in star_wars_keywords)
    
    def _calculate_wookieepedia_relevance(self, notion_title: str, wookiee_title: str) -> float:
        """Calculate relevance score for Wookieepedia results using complete Notion title."""
        
        # Normalize titles for comparison
        notion_normalized = notion_title.lower().strip()
        wookiee_normalized = wookiee_title.lower().strip()
        
        # Exact match gets perfect score
        if notion_normalized == wookiee_normalized:
            return 1.0
        
        # Check if Notion title is contained in Wookieepedia title
        if notion_normalized in wookiee_normalized:
            # Calculate containment ratio
            containment_ratio = len(notion_normalized) / len(wookiee_normalized)
            return min(0.9, containment_ratio + 0.3)  # Bonus for containment
        
        # Check if Wookieepedia title is contained in Notion title
        if wookiee_normalized in notion_normalized:
            containment_ratio = len(wookiee_normalized) / len(notion_normalized)
            return min(0.8, containment_ratio + 0.2)
        
        # Use fuzzy matching for partial matches
        from difflib import SequenceMatcher
        similarity = SequenceMatcher(None, notion_normalized, wookiee_normalized).ratio()
        
        # Apply Star Wars specific bonuses
        star_wars_bonus = 0.0
        if 'star wars' in wookiee_normalized and 'star wars' in notion_normalized:
            star_wars_bonus = 0.1
        
        return min(1.0, similarity + star_wars_bonus)
    
    def _create_star_wars_data_from_wookieepedia(self, wookiee_data: Dict, title: str) -> Dict:
        """Create book data structure from Wookieepedia data."""
        try:
            # Get search results
            search_results = wookiee_data.get('query', {}).get('search', [])
            if not search_results:
                return self._create_sync_failed_data(title)
            
            # Filter for comic-specific content first (same logic as _should_handoff_wookieepedia)
            comic_results = []
            for result in search_results:
                result_title = result.get('title', '').lower()
                page_id = result.get('pageid', '')
                
                # Look for comic-specific keywords
                comic_keywords = ['comic', 'issue', 'volume', 'series', 'adventures']
                has_comic_keyword = any(keyword in result_title for keyword in comic_keywords)
                
                # Also check for known comic series page IDs
                known_comic_pages = ['621368', '761547']  # Main series pages
                is_known_comic = str(page_id) in known_comic_pages
                
                # Include if it has comic keywords OR is a known comic page
                if has_comic_keyword or is_known_comic:
                    comic_results.append(result)
                    if is_known_comic:
                        logger.debug(f"Including known comic page: {result.get('title')} (ID: {page_id})")
            
            # If we found comic-specific results, use those instead
            if comic_results:
                search_results = comic_results
                logger.info(f"Wookieepedia: Found {len(comic_results)} comic-specific results, using those")
            
            # Get the best match (first result)
            best_match = search_results[0]
            page_id = best_match.get('pageid')
            page_title = best_match.get('title', title)
            
            # Get page content for detailed information
            page_content = self.star_wars_fandom.get_page_content(page_id)
            
            # Extract structured data from HTML
            extracted_data = self._extract_wookieepedia_data(page_content, page_id)
            
            volume_info = {
                'title': page_title,
                'authors': extracted_data.get('authors', []),
                'artists': extracted_data.get('artists', []),
                'cover_artists': extracted_data.get('cover_artists', []),
                'description': extracted_data.get('description', ''),
                'publishedDate': extracted_data.get('start_date') or extracted_data.get('release_date') or extracted_data.get('publication_date', ''),
                'publishedEndDate': extracted_data.get('end_date', ''),
                'publisher': extracted_data.get('publisher', ''),
                'pageCount': extracted_data.get('pages'),
                'language': 'en',
                'categories': ['Comics & Graphic Novels', 'Star Wars'],
                'averageRating': None,
                'ratingsCount': None,
                'maturityRating': 'All Ages',
                'printType': 'BOOK',
                'subtitle': '',
                'imageLinks': {},
                'chapters': extracted_data.get('issues'),
                'status': self._determine_status(extracted_data.get('end_date')),
                'format': extracted_data.get('format'),
                'sw_timeline': extracted_data.get('timeline'),
                'series': extracted_data.get('series'),
                'preceded_by': extracted_data.get('preceded_by'),
                'followed_by': extracted_data.get('followed_by'),
                'wookieepedia_url': f"https://starwars.fandom.com/wiki/{page_title.replace(' ', '_')}",
                'wookieepedia_data': {
                    'page_id': page_id,
                    'page_title': page_title,
                    'url': f"https://starwars.fandom.com/wiki/{page_title.replace(' ', '_')}",
                    'extracted_data': extracted_data
                },
                'wookieepedia_images': {
                    'cover_url': extracted_data.get('cover_image')
                }
            }
            
            return {
                'id': None,  # Don't set Google Books ID for Wookieepedia data
                'wookieepedia_id': str(page_id),  # Raw page ID without prefix
                'volumeInfo': volume_info
            }
            
        except Exception as e:
            logger.error(f"Error creating Star Wars data from Wookieepedia: {e}")
            return self._create_sync_failed_data(title)
    
    def _extract_wookieepedia_data(self, page_content: Dict, page_id: int) -> Dict:
        """Extract structured data from Wookieepedia page content."""
        try:
            if not page_content or not page_content.get('parse', {}).get('text', {}).get('*'):
                return {}
            
            html_content = page_content['parse']['text']['*']
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            extracted_data = {}
            
            # Extract authors (writers, editors)
            authors = []
            author_fields = ['writer', 'editor', 'writers', 'editors']
            for field in author_fields:
                elem = soup.find('div', {'data-source': field})
                if elem:
                    author_data = elem.find('div', class_='pi-data-value')
                    if author_data:
                        authors.extend(self._parse_creator_list(author_data.get_text(strip=True)))
            
            extracted_data['authors'] = authors
            
            # Extract artists (pencillers, inkers, line artists, colorists, letterers)
            artists = []
            artist_fields = ['penciller', 'inker', 'line artist', 'colorist', 'letterer']
            for field in artist_fields:
                elem = soup.find('div', {'data-source': field})
                if elem:
                    artist_data = elem.find('div', class_='pi-data-value')
                    if artist_data:
                        artists.extend(self._parse_creator_list_from_html(artist_data))
            
            # Also check for plural forms (e.g., "letterers")
            artist_fields_plural = ['pencillers', 'inkers', 'line artists', 'colorists', 'letterers']
            for field in artist_fields_plural:
                elem = soup.find('div', {'data-source': field})
                if elem:
                    artist_data = elem.find('div', class_='pi-data-value')
                    if artist_data:
                        artists.extend(self._parse_creator_list_from_html(artist_data))
            
            extracted_data['artists'] = artists
            
            # Extract cover artists
            cover_artists = []
            cover_fields = ['cover artist', 'cover artists']
            for field in cover_fields:
                elem = soup.find('div', {'data-source': field})
                if elem:
                    cover_data = elem.find('div', class_='pi-data-value')
                    if cover_data:
                        cover_artists.extend(self._parse_creator_list_from_html(cover_data))
            
            extracted_data['cover_artists'] = cover_artists
            
            # Extract format
            format_elem = soup.find('div', {'data-source': 'format'})
            if format_elem:
                format_data = format_elem.find('div', class_='pi-data-value')
                if format_data:
                    extracted_data['format'] = self._clean_reference_numbers(format_data.get_text(strip=True))
            
            # Extract publisher
            publisher_elem = soup.find('div', {'data-source': 'publisher'})
            if publisher_elem:
                publisher_data = publisher_elem.find('div', class_='pi-data-value')
                if publisher_data:
                    publisher_text = publisher_data.get_text(strip=True)
                    # Clean up reference numbers
                    publisher_text = self._clean_reference_numbers(publisher_text)
                    extracted_data['publisher'] = publisher_text
            
            # Extract timeline (capture full range like "19 BBY-12 BBY")
            timeline_elem = soup.find('div', {'data-source': 'timeline'})
            if timeline_elem:
                timeline_data = timeline_elem.find('div', class_='pi-data-value')
                if timeline_data:
                    timeline_text = timeline_data.get_text(strip=True)
                    # Clean up the timeline text to capture full ranges
                    timeline_text = self._clean_reference_numbers(timeline_text)
                    # Fix BBY formatting - ensure it's "BBY" not "B BY"
                    import re
                    timeline_text = re.sub(r'(\d+)\s+B\s+BY', r'\1 BBY', timeline_text)
                    extracted_data['timeline'] = timeline_text
            
            # Extract pages
            pages_elem = soup.find('div', {'data-source': 'pages'})
            if pages_elem:
                pages_data = pages_elem.find('div', class_='pi-data-value')
                if pages_data:
                    pages_text = pages_data.get_text(strip=True)
                    # Extract number from text like "200" or "200 pages"
                    import re
                    pages_match = re.search(r'(\d+)', pages_text)
                    if pages_match:
                        extracted_data['pages'] = int(pages_match.group(1))
            
            # Extract series
            series_elem = soup.find('div', {'data-source': 'series'})
            if series_elem:
                series_data = series_elem.find('div', class_='pi-data-value')
                if series_data:
                    extracted_data['series'] = self._clean_reference_numbers(series_data.get_text(strip=True))
            else:
                # For Star Wars comics, extract series from title (remove year)
                title = page_content.get('parse', {}).get('title', '')
                if title and '(' in title and ')' in title:
                    # Extract series name by removing year in parentheses
                    series_name = title.split('(')[0].strip()
                    if series_name and series_name != title:
                        extracted_data['series'] = series_name
            
            # Extract followed by
            followed_elem = soup.find('div', {'data-source': 'followed by'})
            if followed_elem:
                followed_data = followed_elem.find('div', class_='pi-data-value')
                if followed_data:
                    followed_text = followed_data.get_text(strip=True)
                    # Clean up reference numbers
                    followed_text = self._clean_reference_numbers(followed_text)
                    extracted_data['followed_by'] = followed_text
            
            # Extract preceded by
            preceded_elem = soup.find('div', {'data-source': 'preceded by'})
            if preceded_elem:
                preceded_data = preceded_elem.find('div', class_='pi-data-value')
                if preceded_data:
                    extracted_data['preceded_by'] = self._clean_reference_numbers(preceded_data.get_text(strip=True))
            
            # Extract followed by
            followed_elem = soup.find('div', {'data-source': 'followed by'})
            if followed_elem:
                followed_data = followed_elem.find('div', class_='pi-data-value')
                if followed_data:
                    extracted_data['followed_by'] = self._clean_reference_numbers(followed_data.get_text(strip=True))
            
            # Extract publisher
            publisher_elem = soup.find('div', {'data-source': 'publisher'})
            if publisher_elem:
                publisher_data = publisher_elem.find('div', class_='pi-data-value')
                if publisher_data:
                    extracted_data['publisher'] = self._clean_reference_numbers(publisher_data.get_text(strip=True))
            
            # Extract start date
            start_elem = soup.find('div', {'data-source': 'start date'})
            if start_elem:
                start_data = start_elem.find('div', class_='pi-data-value')
                if start_data:
                    extracted_data['start_date'] = self._parse_date(start_data.get_text(strip=True))
            
            # Extract end date
            end_elem = soup.find('div', {'data-source': 'end date'})
            if end_elem:
                end_data = end_elem.find('div', class_='pi-data-value')
                if end_data:
                    extracted_data['end_date'] = self._parse_date(end_data.get_text(strip=True))
            
            # Extract release date (for one-shots and single issues)
            release_elem = soup.find('div', {'data-source': 'release date'})
            if release_elem:
                release_data = release_elem.find('div', class_='pi-data-value')
                if release_data:
                    extracted_data['release_date'] = self._parse_date(release_data.get_text(strip=True))
            
            # Extract publication date (alternative field name)
            pub_elem = soup.find('div', {'data-source': 'publication date'})
            if pub_elem:
                pub_data = pub_elem.find('div', class_='pi-data-value')
                if pub_data:
                    extracted_data['publication_date'] = self._parse_date(pub_data.get_text(strip=True))
            
            # Extract issues
            issues_elem = soup.find('div', {'data-source': 'issues'})
            if issues_elem:
                issues_data = issues_elem.find('div', class_='pi-data-value')
                if issues_data:
                    issues_text = issues_data.get_text(strip=True)
                    import re
                    issues_match = re.search(r'(\d+)', issues_text)
                    if issues_match:
                        extracted_data['issues'] = int(issues_match.group(1))
            
            # Extract description from main content area after aside
            # Find the main content div
            main_content = soup.find('div', class_='mw-content-ltr mw-parser-output')
            if main_content:
                # Find the aside tag within the main content
                aside = main_content.find('aside')
                if aside:
                    # Look for the actual description paragraph after the aside
                    # Skip over individual elements and find the first substantial paragraph
                    current = aside.find_next_sibling()
                    description_found = False
                    
                    while current and not description_found:
                        if current.name == 'p':
                            # Found a paragraph, check if it looks like a description
                            text = current.get_text(strip=True)
                            if text and len(text) > 50:  # Substantial content
                                # Check if it looks like a description (contains common description words)
                                if any(word in text.lower() for word in ['is a', 'is an', 'is the', 'are a', 'are an', 'are the', 'by ', 'featuring ', 'anthology', 'miniseries', 'series', 'comic', 'story', 'tells']):
                                    description_text = text
                                    description_text = self._clean_reference_numbers(description_text)
                                    import re
                                    description_text = re.sub(r'\s+', ' ', description_text)  # Normalize all whitespace
                                    description_text = description_text.strip()
                                    extracted_data['description'] = description_text[:2000]  # Truncate to 2000 chars
                                    description_found = True
                                    break
                        current = current.find_next_sibling()
                    
                    # If no proper paragraph found, try regex patterns on the main content
                    if not description_found:
                        main_text = main_content.get_text()
                        import re
                        description_patterns = [
                            r'is a canon one-shot comic.*?\.',  # Clean one-shot description
                            r'is a canon.*?comic book.*?\.',  # Clean comic book description
                            r'is a canon.*?comic.*?series.*?\.',  # Clean comic series description
                            r'is an anthology.*?\.',  # Clean anthology description
                            r'collects.*?\.',  # Clean collects description
                            r'is a.*?story.*?\.',  # Clean story description
                            r'tells.*?story.*?\.',  # Clean tells story description
                        ]
                        
                        for pattern in description_patterns:
                            match = re.search(pattern, main_text, re.IGNORECASE | re.DOTALL)
                            if match:
                                description_text = match.group(0)
                                description_text = self._clean_reference_numbers(description_text)
                                extracted_data['description'] = description_text[:2000]  # Truncate to 2000 chars
                                break
            
            # Fallback: use first paragraph if no description found yet
            if 'description' not in extracted_data:
                paragraphs = soup.find_all('p')
                if paragraphs:
                    description_text = paragraphs[0].get_text(separator=' ', strip=True)
                    description_text = self._clean_reference_numbers(description_text)
                    extracted_data['description'] = description_text[:2000]  # Truncate to 2000 chars
                else:
                    # Last resort: search full text for description patterns
                    all_text = soup.get_text()
                    import re
                    
                    # Look for common description patterns
                    description_patterns = [
                        r'is a canon one-shot comic.*?\.',  # Clean one-shot description
                        r'is a canon.*?comic book.*?\.',  # Clean comic book description
                        r'is a canon.*?comic.*?series.*?\.',  # Clean comic series description
                        r'is an anthology.*?\.',  # Clean anthology description
                        r'collects.*?\.',  # Clean collects description
                        r'is a.*?story.*?\.',  # Clean story description
                        r'tells.*?story.*?\.',  # Clean tells story description
                    ]
                    
                    for pattern in description_patterns:
                        match = re.search(pattern, all_text, re.IGNORECASE | re.DOTALL)
                        if match:
                            description_text = match.group(0)
                            description_text = self._clean_reference_numbers(description_text)
                            extracted_data['description'] = description_text[:2000]  # Truncate to 2000 chars
                            break
            
            # Extract cover image - prioritize comic covers over logos
            cover_image_url = self._get_comic_cover_from_api(page_id)
            if cover_image_url:
                extracted_data['cover_image'] = cover_image_url
            else:
                # Fallback to infobox image
                img_elem = soup.find('img', class_='pi-image-thumbnail')
                if img_elem:
                    img_src = img_elem.get('src')
                    if img_src:
                        # Convert to full resolution image
                        # Remove scale-to-width-down parameter and keep the original image
                        if 'scale-to-width-down' in img_src:
                            # Extract the base URL and cache buster
                            base_url = img_src.split('scale-to-width-down')[0]
                            if 'cb=' in img_src:
                                cache_buster = img_src.split('cb=')[1]
                                img_src = base_url + '?cb=' + cache_buster
                            else:
                                img_src = base_url
                        
                        # Ensure it's a full URL
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = 'https://starwars.fandom.com' + img_src
                        
                        extracted_data['cover_image'] = img_src
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extracting Wookieepedia data: {e}")
            return {}
    
    def _get_comic_cover_from_api(self, page_id: int) -> Optional[str]:
        """Get comic cover image URL from Wookieepedia API, prioritizing actual covers over logos."""
        try:
            import requests
            
            # Get all images for the page
            images_url = f'https://starwars.fandom.com/api.php?action=query&format=json&prop=images&pageids={page_id}&imlimit=500'
            response = requests.get(images_url, timeout=10)
            
            if response.status_code != 200:
                return None
                
            data = response.json()
            pages = data.get('query', {}).get('pages', {})
            if str(page_id) not in pages:
                return None
                
            images = pages[str(page_id)].get('images', [])
            
            # Look for cover-related images, prioritizing actual comic covers
            cover_candidates = []
            for img in images:
                title = img.get('title', '')
                title_lower = title.lower()
                
                # Prioritize actual comic covers
                if any(keyword in title_lower for keyword in ['final-cover', 'cover.jpg', 'cover.png']):
                    cover_candidates.insert(0, title)  # High priority
                elif any(keyword in title_lower for keyword in ['cover', 'issue']):
                    cover_candidates.append(title)  # Medium priority
            
            # Get the URL for the best cover candidate
            for cover_title in cover_candidates:
                image_info_url = f'https://starwars.fandom.com/api.php?action=query&format=json&prop=imageinfo&titles={cover_title}&iiprop=url'
                img_response = requests.get(image_info_url, timeout=10)
                
                if img_response.status_code == 200:
                    img_data = img_response.json()
                    pages = img_data.get('query', {}).get('pages', {})
                    for img_page_id, page_data in pages.items():
                        if 'imageinfo' in page_data:
                            url = page_data['imageinfo'][0].get('url', '')
                            if url:
                                # Test if URL is accessible
                                test_response = requests.head(url, timeout=5)
                                if test_response.status_code == 200:
                                    logger.info(f"Found comic cover: {cover_title} -> {url}")
                                    return url
                                else:
                                    logger.warning(f"Comic cover URL not accessible: {url}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting comic cover from API: {e}")
            return None

    def _parse_creator_list(self, creator_text: str) -> List[str]:
        """Parse comma-separated creator list and clean up names."""
        if not creator_text:
            return []
        
        # Clean up reference numbers first
        creator_text = re.sub(r'\[\d+\]', '', creator_text)
        
        # Check if this looks like concatenated names with parenthetical info
        # Pattern: "Name1(role1)Name2(role2)Name3(role3)"
        if '(' in creator_text and ')' in creator_text and ',' not in creator_text:
            # Split on parenthetical patterns to separate names
            # Look for patterns like "Name(role)" and split before each name
            creators = []
            # Use regex to find all name(role) patterns
            pattern = r'([A-Za-z][A-Za-z\s\.&]+?)\([^)]+\)'
            matches = re.findall(pattern, creator_text)
            
            if matches:
                # Clean up each match
                for match in matches:
                    name = match.strip()
                    if name and len(name) > 1:
                        creators.append(name)
                
                # Also check for any remaining text after the last parenthetical
                last_paren = creator_text.rfind(')')
                if last_paren != -1 and last_paren < len(creator_text) - 1:
                    remaining = creator_text[last_paren + 1:].strip()
                    if remaining and len(remaining) > 1:
                        creators.append(remaining)
                
                # Clean up each creator name
                cleaned_creators = []
                for creator in creators:
                    # Remove extra whitespace and normalize
                    creator = re.sub(r'\s+', ' ', creator).strip()
                    
                    # Split on & if present (for cases like "Jim Cheung&Matthew Wilson")
                    if '&' in creator:
                        # Split on & and clean each part
                        parts = creator.split('&')
                        for part in parts:
                            part = part.strip()
                            if part and len(part) > 1:
                                cleaned_creators.append(part)
                    else:
                        if creator and len(creator) > 1:
                            cleaned_creators.append(creator)
                
                return cleaned_creators
        
        # Original comma-based parsing logic
        creators = []
        current_creator = ""
        paren_count = 0
        
        for char in creator_text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                # Only split on comma if we're not inside parentheses
                creator = current_creator.strip()
                if creator:
                    creators.append(creator)
                current_creator = ""
                continue
            
            current_creator += char
        
        # Add the last creator
        creator = current_creator.strip()
        if creator:
            creators.append(creator)
        
        # Clean up each creator name
        cleaned_creators = []
        for creator in creators:
            # Remove issue numbers and extra whitespace
            creator = re.sub(r'\(\d+[^)]*\)', '', creator).strip()
            creator = re.sub(r'\s+', ' ', creator)  # Normalize whitespace
            
            # Remove parenthetical information like "(editor)", "(assistant editor)", etc.
            creator = re.sub(r'\([^)]*\)', '', creator).strip()
            
            # Split on & if present (for cases like "Jim Cheung&Matthew Wilson")
            if '&' in creator:
                # Split on & and clean each part
                parts = creator.split('&')
                for part in parts:
                    part = part.strip()
                    if part and len(part) > 1:
                        cleaned_creators.append(part)
            else:
                if creator and len(creator) > 1:  # Avoid single characters
                    cleaned_creators.append(creator)
        
        return cleaned_creators

    def _parse_creator_list_from_html(self, html_element) -> List[str]:
        """Parse creator list from HTML element (handles both lists and plain text)."""
        if not html_element:
            return []
        
        creators = []
        
        # Check if it's a list structure
        ul_elem = html_element.find('ul')
        if ul_elem:
            # Handle list structure
            li_elements = ul_elem.find_all('li')
            for li in li_elements:
                creator_text = li.get_text(strip=True)
                creators.extend(self._parse_creator_list(creator_text))
        else:
            # Handle plain text in HTML element
            creator_text = html_element.get_text(strip=True)
            creators.extend(self._parse_creator_list(creator_text))
        
        return creators

    def _clean_reference_numbers(self, text: str) -> str:
        """Remove reference numbers from text and normalize spacing."""
        if not text:
            return text
        
        # Remove reference numbers like [1], [2], etc. and [ 6 ], [ 7 ], etc.
        text = re.sub(r'\[\s*\d+\s*\]', '', text)
        # Normalize whitespace - replace multiple spaces/newlines with single space
        text = re.sub(r'\s+', ' ', text)
        # Fix spacing around punctuation
        text = re.sub(r'\s+([,\.])', r'\1', text)  # Remove space before comma/period
        text = re.sub(r'([a-zA-Z])([A-Z])', r'\1 \2', text)  # Add space between camelCase
        return text.strip()
    
    def _parse_date(self, date_text: str) -> str:
        """Parse date text and return in YYYY-MM-DD format."""
        if not date_text:
            return ''
        
        try:
            # Look for patterns like "June 7, 2017" or "2017-06-07"
            date_match = re.search(r'(\w+)\s+(\d+),\s*(\d{4})', date_text)
            if date_match:
                month_name, day, year = date_match.groups()
                month_map = {
                    'January': '01', 'February': '02', 'March': '03', 'April': '04',
                    'May': '05', 'June': '06', 'July': '07', 'August': '08',
                    'September': '09', 'October': '10', 'November': '11', 'December': '12'
                }
                month_num = month_map.get(month_name, '01')
                return f"{year}-{month_num}-{day.zfill(2)}"
            
            # Look for YYYY-MM-DD format
            iso_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_text)
            if iso_match:
                return iso_match.group(0)
            
            # Look for just year
            year_match = re.search(r'(\d{4})', date_text)
            if year_match:
                return f"{year_match.group(1)}-01-01"
            
            return date_text
            
        except Exception as e:
            logger.error(f"Error parsing date '{date_text}': {e}")
            return date_text
    
    def _determine_status(self, end_date: str) -> str:
        """Determine publication status based on end date."""
        if not end_date:
            return 'Publishing'
        
        try:
            # Parse the date
            if '-' in end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                return 'Publishing'
            
            # If end date is in the past, it's published
            if end_dt < datetime.now():
                return 'Published'
            else:
                return 'Publishing'
                
        except Exception as e:
            logger.error(f"Error determining status from end_date '{end_date}': {e}")
            return 'Publishing'
    
    def _map_jikan_status(self, jikan_status: str) -> str:
        """Map Jikan status values to standardized status values."""
        if not jikan_status:
            return 'Publishing'
        
        # Jikan status mapping
        status_mapping = {
            'Publishing': 'Publishing',
            'Complete': 'Published',
            'Finished': 'Published',
            'Completed': 'Published',
            'Hiatus': 'Publishing',
            'Discontinued': 'Published',
            'Not yet published': 'Upcoming',
            'Upcoming': 'Upcoming'
        }
        
        mapped_status = status_mapping.get(jikan_status, 'Publishing')
        logger.info(f"Mapped Jikan status '{jikan_status}' to '{mapped_status}'")
        return mapped_status
    
    def _create_sync_failed_data(self, title: str) -> Dict:
        """Create data structure for when sync fails."""
        return {
            'id': f"sync_failed_{hash(title)}",
            'volumeInfo': {
                'title': title,
                'authors': [],
                'description': '',
                'publishedDate': '',
                'publisher': '',
                'pageCount': None,
                'language': '',
                'categories': ['Sync Failed'],
                'averageRating': None,
                'ratingsCount': None,
                'maturityRating': None,
                'printType': 'BOOK',
                'subtitle': '',
                'imageLinks': {}
            }
        }
    
    def search_books(self, title: str) -> Optional[Dict]:
        """Content-type aware progressive fallback logic."""
        try:
            # If content type is specified, use that API directly
            if self.content_type == 'book':
                logger.info(f"Content type 'book' specified: Using Google Books for '{title}'")
                google_data = self.google_books.search_books(title)
                if google_data:
                    logger.info("✅ Using Google Books data")
                    return google_data
                else:
                    logger.warning("❌ Google Books failed for forced book content type")
                    return self._create_sync_failed_data(title)
            
            elif self.content_type == 'manga':
                logger.info(f"Content type 'manga' specified: Using Jikan for '{title}'")
                jikan_data = self.jikan.search_manga(title)
                if jikan_data and not self._should_handoff_jikan(jikan_data, title):
                    logger.info("✅ Using Jikan data")
                    # Get detailed manga information
                    mal_id = jikan_data.get('mal_id')
                    if mal_id:
                        detailed_manga = self.jikan.get_manga_details(mal_id)
                        if detailed_manga:
                            manga_data = self._create_manga_data_from_jikan(detailed_manga)
                            logger.info(f"Using Jikan detailed data for '{title}'")
                            return manga_data
                    
                    # Fallback to search result if details fail
                    manga_data = self._create_manga_data_from_jikan(jikan_data)
                    logger.info(f"Using Jikan search data for '{title}'")
                    return manga_data
                else:
                    logger.warning("❌ Jikan failed for forced manga content type")
                    return self._create_sync_failed_data(title)
            
            elif self.content_type == 'comic':
                logger.info(f"Content type 'comic' specified: Using Wookieepedia→ComicVine for '{title}'")
                
                # Try Wookieepedia first for comics
                logger.info(f"Step 1: Searching Wookieepedia for comic '{title}'")
                wookiee_data = self.star_wars_fandom.search_comic(title)
                if wookiee_data and not self._should_handoff_wookieepedia(wookiee_data, title):
                    logger.info("✅ Using Wookieepedia data")
                    return self._create_star_wars_data_from_wookieepedia(wookiee_data, title)
                
                # Fallback to ComicVine
                logger.info(f"Step 2: Searching ComicVine for comic '{title}'")
                if self.comicvine:
                    comicvine_data = self.comicvine.search_volumes(title)
                    if comicvine_data and not self._should_handoff_comicvine(comicvine_data, title):
                        logger.info("✅ Using ComicVine data")
                        comics_data = self._create_comics_data_from_comicvine(comicvine_data)
                        logger.info(f"Using ComicVine data for '{title}'")
                        return comics_data
                
                logger.warning("❌ Both Wookieepedia and ComicVine failed for forced comic content type")
                return self._create_sync_failed_data(title)
            
            # Default behavior: Progressive fallback (Google Books → Wookieepedia → Jikan → ComicVine)
            logger.info(f"No content type specified: Using progressive fallback for '{title}'")
            
            # Step 1: Google Books (broadest coverage)
            logger.info(f"Step 1: Searching Google Books for '{title}'")
            google_data = self.google_books.search_books(title)
            
            if google_data and not self._should_handoff_google_books(google_data):
                logger.info("✅ Using Google Books data")
                return google_data
            
            # Step 2: Wookieepedia (comics specialized)
            logger.info(f"Step 2: Searching Wookieepedia for comic content '{title}'")
            wookiee_data = self.star_wars_fandom.search_comic(title)
            if wookiee_data and not self._should_handoff_wookieepedia(wookiee_data, title):
                logger.info("✅ Using Wookieepedia data")
                return self._create_star_wars_data_from_wookieepedia(wookiee_data, title)
            
            # Step 3: Jikan (manga/anime specialized)
            logger.info(f"Step 3: Searching Jikan for manga/anime content '{title}'")
            jikan_data = self.jikan.search_manga(title)
            if jikan_data and not self._should_handoff_jikan(jikan_data, title):
                logger.info("✅ Using Jikan data")
                # Get detailed manga information
                mal_id = jikan_data.get('mal_id')
                if mal_id:
                    detailed_manga = self.jikan.get_manga_details(mal_id)
                    if detailed_manga:
                        manga_data = self._create_manga_data_from_jikan(detailed_manga)
                        logger.info(f"Using Jikan detailed data for '{title}'")
                        return manga_data
                
                # Fallback to search result if details fail
                manga_data = self._create_manga_data_from_jikan(jikan_data)
                logger.info(f"Using Jikan search data for '{title}'")
                return manga_data
            
            # Step 4: ComicVine (comics specialized)
            logger.info(f"Step 4: Searching ComicVine for comics content '{title}'")
            if self.comicvine:
                comicvine_data = self.comicvine.search_volumes(title)
                if comicvine_data and not self._should_handoff_comicvine(comicvine_data, title):
                    logger.info("✅ Using ComicVine data")
                    comics_data = self._create_comics_data_from_comicvine(comicvine_data)
                    logger.info(f"Using ComicVine data for '{title}'")
                    return comics_data
            
            # Step 5: Sync Failed
            logger.warning(f"❌ Sync failed: No suitable data source found for '{title}'")
            return self._create_sync_failed_data(title)
            
        except Exception as e:
            logger.error(f"Error in hybrid search for '{title}': {e}")
            return self._create_sync_failed_data(title)
    
    def _create_manga_data_from_jikan(self, jikan_data: Dict) -> Dict:
        """Create book data structure from Jikan manga data."""
        try:
            # Create a structure that matches Google Books format
            volume_info = {
                'title': jikan_data.get('title', ''),
                'authors': [author.get('name', '').replace(',', '') for author in jikan_data.get('authors', [])],
                'description': jikan_data.get('synopsis', ''),
                'publishedDate': jikan_data.get('published', {}).get('from', '').split('T')[0] if jikan_data.get('published', {}).get('from') else '',
                'publishedEndDate': jikan_data.get('published', {}).get('to', '').split('T')[0] if jikan_data.get('published', {}).get('to') else '',
                'status': self._map_jikan_status(jikan_data.get('status', '')),
                'publisher': jikan_data.get('serializations', [{}])[0].get('name', '') if jikan_data.get('serializations') else '',
                'pageCount': None,  # Jikan doesn't have page count
                'language': 'ja',  # Default to Japanese for manga
                'categories': [genre.get('name', '') for genre in jikan_data.get('genres', [])],
                'chapters': jikan_data.get('chapters'),
                'volumes': jikan_data.get('volumes'),
                'averageRating': jikan_data.get('score'),
                'ratingsCount': jikan_data.get('scored_by'),
                'maturityRating': None,
                'printType': 'BOOK',
                'subtitle': '',
                'imageLinks': {},
                'jikan_data': {
                    'mal_id': jikan_data.get('mal_id'),
                    'score': jikan_data.get('score'),
                    'scored_by': jikan_data.get('scored_by'),
                    'rank': jikan_data.get('rank'),
                    'popularity': jikan_data.get('popularity'),
                    'members': jikan_data.get('members'),
                    'favorites': jikan_data.get('favorites'),
                    'status': self._map_jikan_status(jikan_data.get('status')),
                    'publishing': jikan_data.get('publishing'),
                    'demographics': [demo.get('name') for demo in jikan_data.get('demographics', [])],
                    'explicit_genres': [genre.get('name') for genre in jikan_data.get('explicit_genres', [])],
                    'themes': [theme.get('name') for theme in jikan_data.get('themes', [])]
                },
                'jikan_images': jikan_data.get('images', {}),
                'jikan_url': jikan_data.get('url')
            }
            
            return {
                'id': str(jikan_data.get('mal_id', 'unknown')),
                'volumeInfo': volume_info
            }
            
        except Exception as e:
            logger.error(f"Error creating manga data from Jikan: {e}")
            return self._create_empty_data(jikan_data.get('title', 'Unknown'))
    
    def _create_comics_data_from_comicvine(self, comicvine_data: Dict) -> Dict:
        """Create book data structure from ComicVine data."""
        try:
            # Log ComicVine data for debugging
            logger.info(f"ComicVine data for '{comicvine_data.get('name', 'Unknown')}':")
            logger.info(f"  - count_of_issues: {comicvine_data.get('count_of_issues')}")
            logger.info(f"  - start_year: {comicvine_data.get('start_year')}")
            logger.info(f"  - publisher: {comicvine_data.get('publisher', {}).get('name', 'None')}")
            logger.info(f"  - person_credits: {comicvine_data.get('person_credits', [])}")
            logger.info(f"  - description length: {len(comicvine_data.get('description', ''))}")
            logger.info(f"  - description preview: {comicvine_data.get('description', '')[:100]}...")
            logger.info(f"  - concepts: {comicvine_data.get('concepts', [])}")
            
            # Create a structure that matches Google Books format
            # Extract authors from scraped creators (prefer scraped data over API)
            authors = []
            scraped_creators = comicvine_data.get('scraped_creators', [])
            if scraped_creators:
                # Use scraped creators who have credits equal to total issues
                total_issues = comicvine_data.get('count_of_issues', 0)
                for creator in scraped_creators:
                    if creator.get('credit_count') == total_issues:
                        authors.append(creator['name'])
                logger.info(f"Using scraped creators for authors: {authors}")
            else:
                # Fallback to API person_credits
                person_credits = comicvine_data.get('person_credits', [])
                for person in person_credits:
                    if person.get('name'):
                        authors.append(person['name'])
                logger.info(f"Using API person_credits for authors: {authors}")
            
            # Use description field (strip HTML and truncate for length)
            description = comicvine_data.get('description', '')
            clean_description = self.comicvine._strip_html(description)
            
            # Determine publication status based on last_issue field
            last_issue = comicvine_data.get('last_issue')
            logger.info(f"ComicVine last_issue: {last_issue}")
            
            last_issue_date = None
            if last_issue and isinstance(last_issue, dict) and last_issue.get('id'):
                # Try to get the publication date of the last issue
                try:
                    last_issue_date = self.comicvine.get_issue_date(last_issue['id'])
                    logger.info(f"Last issue publication date: {last_issue_date}")
                except Exception as e:
                    logger.warning(f"Could not get last issue date: {e}")
            
            if last_issue is None or last_issue == '':
                publication_status = 'Publishing'
                logger.info("Setting status to 'Publishing' (no last_issue)")
            else:
                publication_status = 'Published'
                logger.info(f"Setting status to 'Published' (last_issue: {last_issue})")
            
            volume_info = {
                'title': comicvine_data.get('name', ''),
                'authors': authors,  # Extract from person_credits
                'description': self.comicvine._truncate_description(clean_description),
                'publishedDate': comicvine_data.get('start_year', ''),
                'publisher': comicvine_data.get('publisher', {}).get('name', '') if comicvine_data.get('publisher') else '',
                'pageCount': None,
                'language': 'en',  # Default to English for comics
                'categories': ['Comics & Graphic Novels'],
                'averageRating': None,
                'ratingsCount': None,
                'maturityRating': None,
                'printType': 'BOOK',
                'subtitle': '',
                'imageLinks': {},
                'chapters': comicvine_data.get('count_of_issues'),  # Map issues to chapters
                'status': publication_status,  # Add publication status
                'comicvine_url': comicvine_data.get('site_detail_url'),  # ComicVine page URL
                'comicvine_data': {
                    'comicvine_id': comicvine_data.get('id'),
                    'comicvine_url': comicvine_data.get('api_detail_url'),
                    'site_detail_url': comicvine_data.get('site_detail_url'),
                    'count_of_issues': comicvine_data.get('count_of_issues'),
                    'start_year': comicvine_data.get('start_year'),
                    'end_date': comicvine_data.get('end_date'),
                    'last_issue': comicvine_data.get('last_issue'),
                    'last_issue_date': last_issue_date,
                    'publisher': comicvine_data.get('publisher', {}).get('name') if comicvine_data.get('publisher') else None,
                    'concepts': comicvine_data.get('concepts', []),
                    'scraped_themes': comicvine_data.get('scraped_themes', []),
                    'scraped_creators': comicvine_data.get('scraped_creators', [])
                },
                'comicvine_images': comicvine_data.get('image', {})
            }
            
            return {
                'id': str(comicvine_data.get('id', 'unknown')),
                'volumeInfo': volume_info
            }
            
        except Exception as e:
            logger.error(f"Error creating comics data from ComicVine: {e}")
            return self._create_empty_data(comicvine_data.get('name', 'Unknown'))
    
    def _calculate_relevance_score(self, search_title: str, result_title: str) -> float:
        """Calculate relevance score between search title and result title."""
        try:
            # Normalize titles for comparison
            search_normalized = search_title.lower().strip()
            result_normalized = result_title.lower().strip()
            
            # Exact match gets highest score
            if search_normalized == result_normalized:
                return 1.0
            
            # Check if search title is contained in result title
            if search_normalized in result_normalized:
                # Calculate containment ratio
                containment_ratio = len(search_normalized) / len(result_normalized)
                return 0.8 + (containment_ratio * 0.2)  # 0.8 to 1.0 range
            
            # Check if result title is contained in search title
            if result_normalized in search_normalized:
                containment_ratio = len(result_normalized) / len(search_normalized)
                return 0.6 + (containment_ratio * 0.2)  # 0.6 to 0.8 range
            
            # Check for word overlap
            search_words = set(search_normalized.split())
            result_words = set(result_normalized.split())
            
            if search_words and result_words:
                overlap_ratio = len(search_words.intersection(result_words)) / len(search_words.union(result_words))
                return overlap_ratio * 0.6  # 0.0 to 0.6 range
            
            # No similarity
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating relevance score: {e}")
            return 0.0
    
    def _is_relevant_result(self, search_title: str, result_title: str, min_score: float = 0.3) -> bool:
        """Check if a search result is relevant enough to use."""
        score = self._calculate_relevance_score(search_title, result_title)
        logger.debug(f"Relevance score for '{search_title}' vs '{result_title}': {score:.2f}")
        return score >= min_score

    def _create_empty_data(self, title: str) -> Dict:
        """Create empty data structure for when no API data is available."""
        return {
            'id': f"empty_{hash(title)}",
            'volumeInfo': {
                'title': title,
                'authors': [],
                'description': '',
                'publishedDate': '',
                'publisher': '',
                'pageCount': None,
                'language': '',
                'categories': [],
                'averageRating': None,
                'ratingsCount': None,
                'maturityRating': None,
                'printType': 'BOOK',
                'subtitle': '',
                'imageLinks': {}
            }
        }
    
    def _merge_manga_data(self, google_data: Dict, jikan_data: Dict) -> Dict:
        """Merge Google Books and Jikan data for manga."""
        try:
            # Start with Google Books data
            merged = google_data.copy()
            
            # Enhance with Jikan data
            volume_info = merged.get('volumeInfo', {})
            
            # Add Jikan-specific fields
            volume_info['jikan_data'] = {
                'mal_id': jikan_data.get('mal_id'),
                'score': jikan_data.get('score'),
                'scored_by': jikan_data.get('scored_by'),
                'rank': jikan_data.get('rank'),
                'popularity': jikan_data.get('popularity'),
                'members': jikan_data.get('members'),
                'favorites': jikan_data.get('favorites'),
                'status': self._map_jikan_status(jikan_data.get('status')),
                'publishing': jikan_data.get('publishing'),
                'demographics': [demo.get('name') for demo in jikan_data.get('demographics', [])],
                'explicit_genres': [genre.get('name') for genre in jikan_data.get('explicit_genres', [])],
                'themes': [theme.get('name') for theme in jikan_data.get('themes', [])]
            }
            
            # Use Jikan description if available and better
            if jikan_data.get('synopsis') and len(jikan_data['synopsis']) > len(volume_info.get('description', '')):
                volume_info['description'] = jikan_data['synopsis']
            
            # Use Jikan background info if available
            if jikan_data.get('background'):
                volume_info['background'] = jikan_data['background']
            
            # Use Jikan images if available (higher quality)
            if jikan_data.get('images'):
                volume_info['jikan_images'] = jikan_data['images']
            
            # Add Jikan URL
            volume_info['jikan_url'] = jikan_data.get('url')
            
            return merged
            
        except Exception as e:
            logger.error(f"Error merging manga data: {e}")
            return google_data
    
    def _merge_comics_data(self, google_data: Dict, comicvine_data: Dict) -> Dict:
        """Merge Google Books and ComicVine data for comics."""
        try:
            # Start with Google Books data
            merged = google_data.copy()
            
            # Enhance with ComicVine data
            volume_info = merged.get('volumeInfo', {})
            
            # Add ComicVine-specific fields
            volume_info['comicvine_data'] = {
                'comicvine_id': comicvine_data.get('id'),
                'comicvine_url': comicvine_data.get('api_detail_url'),
                'site_detail_url': comicvine_data.get('site_detail_url'),
                'count_of_issues': comicvine_data.get('count_of_issues'),
                'start_year': comicvine_data.get('start_year'),
                'publisher': comicvine_data.get('publisher', {}).get('name') if comicvine_data.get('publisher') else None
            }
            
            # Use ComicVine description if available and better
            if comicvine_data.get('description') and len(comicvine_data['description']) > len(volume_info.get('description', '')):
                volume_info['description'] = comicvine_data['description']
            
            # Use ComicVine images if available
            if comicvine_data.get('image'):
                volume_info['comicvine_images'] = comicvine_data['image']
            
            return merged
            
        except Exception as e:
            logger.error(f"Error merging comics data: {e}")
            return google_data
    
    def get_cover_url(self, book_data: Dict) -> Optional[str]:
        """Get high-quality cover image URL from book data."""
        try:
            volume_info = book_data.get('volumeInfo', {})
            
            # Check for Jikan images first (higher quality for manga)
            if volume_info.get('jikan_images'):
                jikan_images = volume_info['jikan_images']
                # Prefer large image, fallback to regular
                if jikan_images.get('jpg', {}).get('large_image_url'):
                    url = jikan_images['jpg']['large_image_url']
                    logger.info(f"Using Jikan large cover image: {url}")
                    return url
                elif jikan_images.get('jpg', {}).get('image_url'):
                    url = jikan_images['jpg']['image_url']
                    logger.info(f"Using Jikan cover image: {url}")
                    return url
            
            # Check for ComicVine images
            if volume_info.get('comicvine_images'):
                comicvine_image = volume_info['comicvine_images']
                if comicvine_image.get('super_url'):
                    url = comicvine_image['super_url']
                    logger.info(f"Using ComicVine super cover image: {url}")
                    return url
                elif comicvine_image.get('medium_url'):
                    url = comicvine_image['medium_url']
                    logger.info(f"Using ComicVine medium cover image: {url}")
                    return url
            
            # Fallback to Google Books images
            image_links = volume_info.get('imageLinks', {})
            preferred_sizes = ['extraLarge', 'large', 'medium', 'small', 'thumbnail', 'smallThumbnail']

            for size in preferred_sizes:
                if image_links.get(size):
                    url = image_links[size]

                    if url.startswith('http://'):
                        url = url.replace('http://', 'https://')

                    if 'books.google.com' in url or 'googleusercontent.com' in url:
                        if 'zoom=' in url:
                            url = url.split('zoom=')[0] + 'zoom=0'
                        elif '&' in url:
                            url = url + '&zoom=0'
                        else:
                            url = url + '?zoom=0'

                    logger.info(f"Using Google Books {size} cover image: {url}")
                    return url

            return None

        except Exception as e:
            logger.error(f"Error getting cover URL: {e}")
            return None
    
    def get_book_details(self, book_id: str) -> Optional[Dict]:
        """Get detailed information for a book by ID."""
        # Delegate to Google Books API
        return self.google_books.get_book_details(book_id)
