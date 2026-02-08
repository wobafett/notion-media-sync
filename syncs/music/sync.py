#!/usr/bin/env python3
"""
Notion MusicBrainz Sync Script
Synchronizes music information from MusicBrainz API to Notion database pages.
Supports three databases: Artists, Albums, Songs, and Labels.
"""

import os
import logging
import time
import re
from urllib.parse import urlparse
from typing import Dict, List, Optional, Union
from datetime import datetime, timezone
import requests

from shared.change_detection import has_property_changes
from shared.logging_config import get_logger
from shared.notion_api import NotionAPI
from shared.utils import build_multi_select_options, get_notion_token, normalize_id

logger = get_logger(__name__)

# Try to import custom property configuration
try:
    from syncs.music.property_config import (
        # Artists
        ARTISTS_TITLE_PROPERTY_ID,
        ARTISTS_MUSICBRAINZ_ID_PROPERTY_ID,
        ARTISTS_SORT_NAME_PROPERTY_ID,
        ARTISTS_TYPE_PROPERTY_ID,
        ARTISTS_GENDER_PROPERTY_ID,
        ARTISTS_AREA_PROPERTY_ID,
        ARTISTS_BORN_IN_PROPERTY_ID,
        ARTISTS_IG_LINK_PROPERTY_ID,
        ARTISTS_WEBSITE_LINK_PROPERTY_ID,
        ARTISTS_YOUTUBE_LINK_PROPERTY_ID,
        ARTISTS_BANDCAMP_LINK_PROPERTY_ID,
        ARTISTS_STREAMING_LINK_PROPERTY_ID,
        ARTISTS_COUNTRY_PROPERTY_ID,
        ARTISTS_BEGIN_DATE_PROPERTY_ID,
        ARTISTS_END_DATE_PROPERTY_ID,
        ARTISTS_DISAMBIGUATION_PROPERTY_ID,
        ARTISTS_DESCRIPTION_PROPERTY_ID,
        ARTISTS_GENRES_PROPERTY_ID,
        ARTISTS_TAGS_PROPERTY_ID,
        ARTISTS_RATING_PROPERTY_ID,
        ARTISTS_LAST_UPDATED_PROPERTY_ID,
        ARTISTS_MUSICBRAINZ_URL_PROPERTY_ID,
        ARTISTS_ALBUMS_PROPERTY_ID,
        ARTISTS_SONGS_PROPERTY_ID,
        # Albums
        ALBUMS_TITLE_PROPERTY_ID,
        ALBUMS_MUSICBRAINZ_ID_PROPERTY_ID,
        ALBUMS_ARTIST_PROPERTY_ID,
        ALBUMS_RELEASE_DATE_PROPERTY_ID,
        ALBUMS_COUNTRY_PROPERTY_ID,
        ALBUMS_LABEL_PROPERTY_ID,
        ALBUMS_TYPE_PROPERTY_ID,
        ALBUMS_LISTEN_PROPERTY_ID,
        ALBUMS_STATUS_PROPERTY_ID,
        ALBUMS_PACKAGING_PROPERTY_ID,
        ALBUMS_BARCODE_PROPERTY_ID,
        ALBUMS_FORMAT_PROPERTY_ID,
        ALBUMS_TRACK_COUNT_PROPERTY_ID,
        ALBUMS_DESCRIPTION_PROPERTY_ID,
        ALBUMS_GENRES_PROPERTY_ID,
        ALBUMS_TAGS_PROPERTY_ID,
        ALBUMS_RATING_PROPERTY_ID,
        ALBUMS_COVER_IMAGE_PROPERTY_ID,
        ALBUMS_MUSICBRAINZ_URL_PROPERTY_ID,
        ALBUMS_LAST_UPDATED_PROPERTY_ID,
        ALBUMS_DISCS_PROPERTY_ID,
        ALBUMS_SONGS_PROPERTY_ID,
        # Songs
        SONGS_TITLE_PROPERTY_ID,
        SONGS_MUSICBRAINZ_ID_PROPERTY_ID,
        SONGS_ARTIST_PROPERTY_ID,
        SONGS_ALBUM_PROPERTY_ID,
        SONGS_TRACK_NUMBER_PROPERTY_ID,
        SONGS_LENGTH_PROPERTY_ID,
        SONGS_ISRC_PROPERTY_ID,
        SONGS_DISAMBIGUATION_PROPERTY_ID,
        SONGS_DESCRIPTION_PROPERTY_ID,
        SONGS_GENRES_PROPERTY_ID,
        SONGS_TAGS_PROPERTY_ID,
        SONGS_LISTEN_PROPERTY_ID,
        SONGS_RATING_PROPERTY_ID,
        SONGS_MUSICBRAINZ_URL_PROPERTY_ID,
        SONGS_LAST_UPDATED_PROPERTY_ID,
        SONGS_DISC_PROPERTY_ID,
        # Labels
        LABELS_TITLE_PROPERTY_ID,
        LABELS_MUSICBRAINZ_ID_PROPERTY_ID,
        LABELS_TYPE_PROPERTY_ID,
        LABELS_COUNTRY_PROPERTY_ID,
        LABELS_BEGIN_DATE_PROPERTY_ID,
        LABELS_END_DATE_PROPERTY_ID,
        LABELS_DISAMBIGUATION_PROPERTY_ID,
        LABELS_DESCRIPTION_PROPERTY_ID,
        LABELS_GENRES_PROPERTY_ID,
        LABELS_TAGS_PROPERTY_ID,
        LABELS_RATING_PROPERTY_ID,
        LABELS_MUSICBRAINZ_URL_PROPERTY_ID,
        LABELS_OFFICIAL_WEBSITE_PROPERTY_ID,
        LABELS_IG_PROPERTY_ID,
        LABELS_BANDCAMP_PROPERTY_ID,
        LABELS_FOUNDED_PROPERTY_ID,
        LABELS_LAST_UPDATED_PROPERTY_ID,
        LABELS_ALBUMS_PROPERTY_ID,
        LABELS_AREA_PROPERTY_ID,
    )
except ImportError as exc:
    raise RuntimeError(
        "syncs/music/property_config.py not found. Copy the example file and set your property IDs."
    ) from exc


class MusicBrainzAPI:
    """MusicBrainz API client for fetching music data."""
    
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.base_url = "https://musicbrainz.org/ws/2"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/json'
        })
        
        # Rate limiting - MusicBrainz allows 1 request per second
        self.request_delay = 1.0
        self.last_request_time = 0
        
        # Caching to reduce API calls
        self._cache = {
            'artists': {},
            'releases': {},
            'recordings': {},
            'labels': {},
            'cover_art': {},
            'release_groups': {},
            'artist_release_groups': {},
            'artist_recordings': {}
        }
    
    def _rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_api_request(self, url: str, params: Dict = None, headers: Dict = None, max_retries: int = 3) -> requests.Response:
        """Make an API request with rate limiting and retry logic."""
        if params is None:
            params = {}
        
        for attempt in range(max_retries + 1):
            try:
                # Apply rate limiting before each request
                self._rate_limit()
                
                # Make the request
                response = self.session.get(url, params=params, headers=headers)
                
                # Check for rate limiting (429)
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + 1  # Exponential backoff
                        logger.warning(f"Rate limited (429). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
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
        
        raise Exception("Max retries exceeded")
    
    def search_artists(self, name: str, limit: int = 5) -> List[Dict]:
        """Search for artists by name."""
        try:
            url = f"{self.base_url}/artist"
            params = {
                'query': name,
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('artists', [])
            
        except Exception as e:
            logger.error(f"Error searching for artist '{name}': {e}")
            return []
    
    def get_artist(self, mbid: str) -> Optional[Dict]:
        """Get detailed artist information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['artists']:
                logger.debug(f"Using cached artist data for MBID {mbid}")
                return self._cache['artists'][mbid]
            
            url = f"{self.base_url}/artist/{mbid}"
            params = {
                'inc': 'aliases+tags+ratings+release-groups+genres+url-rels+area-rels',
                'fmt': 'json'
            }
            # Note: 'genres' in inc will include genres on both artist and release-groups
            
            response = self._make_api_request(url, params)
            artist = response.json()
            
            # Cache the result
            self._cache['artists'][mbid] = artist
            return artist
            
        except Exception as e:
            logger.error(f"Error getting artist {mbid}: {e}")
            return None
    
    def search_releases(self, title: str, artist: str = None, limit: int = 5) -> List[Dict]:
        """Search for releases (albums) by title and optionally artist."""
        try:
            url = f"{self.base_url}/release"
            
            # Build query
            query_parts = [f'release:"{title}"']
            if artist:
                query_parts.append(f'artist:"{artist}"')
            
            params = {
                'query': ' AND '.join(query_parts),
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('releases', [])
            
        except Exception as e:
            logger.error(f"Error searching for release '{title}': {e}")
            return []
    
    def search_releases_by_recording(self, recording_id: str, limit: int = 50) -> List[Dict]:
        """Search for releases that contain a specific recording."""
        try:
            url = f"{self.base_url}/release"
            
            # Search for releases containing this recording
            params = {
                'query': f'recordingid:{recording_id}',
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('releases', [])
            
        except Exception as e:
            logger.debug(f"Error searching for releases by recording {recording_id}: {e}")
            return []
    
    def get_release(self, mbid: str) -> Optional[Dict]:
        """Get detailed release information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['releases']:
                logger.debug(f"Using cached release data for MBID {mbid}")
                return self._cache['releases'][mbid]
            
            url = f"{self.base_url}/release/{mbid}"
            params = {
                'inc': 'artists+labels+recordings+release-groups+tags+ratings+genres+url-rels',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            release = response.json()
            
            # Cache the result
            self._cache['releases'][mbid] = release
            return release
            
        except Exception as e:
            logger.error(f"Error getting release {mbid}: {e}")
            return None
    
    def get_release_group(self, mbid: str) -> Optional[Dict]:
        """Get release-group details (including releases) by MBID."""
        try:
            if mbid in self._cache['release_groups']:
                logger.debug(f"Using cached release-group data for MBID {mbid}")
                return self._cache['release_groups'][mbid]
            
            url = f"{self.base_url}/release-group/{mbid}"
            params = {
                'inc': 'releases+tags+ratings',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            release_group = response.json()
            
            self._cache['release_groups'][mbid] = release_group
            return release_group
        except Exception as e:
            logger.error(f"Error getting release-group {mbid}: {e}")
            return None
    
    def get_artist_release_groups(self, artist_mbid: str, primary_type: str = 'album') -> List[Dict]:
        """Get all release-groups for an artist, optionally filtered by primary type."""
        cache_key = f"{artist_mbid}:{primary_type or 'any'}"
        if cache_key in self._cache['artist_release_groups']:
            return self._cache['artist_release_groups'][cache_key]
        
        release_groups = []
        offset = 0
        limit = 100
        
        try:
            while True:
                params = {
                    'artist': artist_mbid,
                    'limit': limit,
                    'offset': offset,
                    'fmt': 'json'
                }
                if primary_type:
                    params['type'] = primary_type
                
                url = f"{self.base_url}/release-group"
                response = self._make_api_request(url, params)
                data = response.json()
                
                batch = data.get('release-groups', [])
                if not batch:
                    break
                
                release_groups.extend(batch)
                
                count = data.get('release-group-count')
                offset += len(batch)
                if count is None or offset >= count:
                    break
            
            self._cache['artist_release_groups'][cache_key] = release_groups
            return release_groups
        except Exception as e:
            logger.error(f"Error fetching release-groups for artist {artist_mbid}: {e}")
            return release_groups
    
    def search_recordings(self, title: str, artist: str = None, album: str = None, artist_mbid: str = None, limit: int = 5) -> List[Dict]:
        """Search for recordings (songs) by title and optionally artist and album.
        
        Args:
            title: Recording title
            artist: Artist name (used if artist_mbid not provided)
            album: Album/release name
            artist_mbid: Artist MBID (preferred over artist name for accuracy)
            limit: Maximum number of results
        """
        try:
            url = f"{self.base_url}/recording"
            
            # Build query - use less strict title matching when we have artist MBID
            # This helps find recordings that might have slight title variations
            # Note: When querying /recording endpoint, we don't need 'type:recording'
            if artist_mbid:
                # When we have artist MBID, use a more flexible title search
                # This will match "New Genesis" even if the recording has additional text
                query_parts = [f'recording:{title}']  # No quotes = partial match
                query_parts.append(f'arid:{artist_mbid}')
            else:
                # Use exact match when we don't have artist MBID
                query_parts = [f'recording:"{title}"']
                if artist:
                    query_parts.append(f'artist:"{artist}"')
            if album:
                query_parts.append(f'release:"{album}"')
            
            params = {
                'query': ' AND '.join(query_parts),
                'limit': limit,
                'fmt': 'json',
                'inc': 'artist-credits+aliases+releases'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('recordings', [])
            
        except Exception as e:
            logger.error(f"Error searching for recording '{title}': {e}")
            return []

    def get_artist_recordings(self, artist_mbid: str, limit: int = 100) -> List[Dict]:
        """Return cached recordings for an artist, fetching once when needed."""
        if not artist_mbid:
            return []
        cache_key = f"{artist_mbid}:{limit}"
        if cache_key in self._cache['artist_recordings']:
            return self._cache['artist_recordings'][cache_key]
        try:
            url = f"{self.base_url}/recording"
            params = {
                'query': f'arid:{artist_mbid}',
                'limit': limit,
                'fmt': 'json',
                'inc': 'artist-credits+aliases+releases'
            }
            response = self._make_api_request(url, params)
            data = response.json()
            recordings = data.get('recordings', [])
            self._cache['artist_recordings'][cache_key] = recordings
            return recordings
        except Exception as e:
            logger.error(f"Error fetching recordings for artist {artist_mbid}: {e}")
            return []
    
    def get_recording(self, mbid: str) -> Optional[Dict]:
        """Get detailed recording information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['recordings']:
                logger.debug(f"Using cached recording data for MBID {mbid}")
                return self._cache['recordings'][mbid]
            
            url = f"{self.base_url}/recording/{mbid}"
            params = {
                'inc': 'artists+releases+release-groups+tags+ratings+isrcs+url-rels+genres+aliases',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            recording = response.json()
            
            # Cache the result
            self._cache['recordings'][mbid] = recording
            return recording
            
        except Exception as e:
            logger.error(f"Error getting recording {mbid}: {e}")
            return None
    
    def search_recording_by_isrc(self, isrc: str) -> Optional[Dict]:
        """
        Search for a recording by ISRC code.
        
        ISRC (International Standard Recording Code) uniquely identifies a specific recording.
        This provides highly accurate matching compared to name-based searches.
        """
        try:
            if not isrc:
                return None
            
            url = f"{self.base_url}/isrc/{isrc}"
            params = {
                'inc': 'artists+releases+release-groups+artist-credits+aliases+genres+tags',
                'fmt': 'json'
            }
            
            # Use max_retries=0 since 404 is a valid response (ISRC doesn't exist)
            response = self._make_api_request(url, params, max_retries=0)
            data = response.json()
            
            # ISRC lookup returns a recording directly (not a list)
            if data and 'id' in data:
                logger.info(f"Found recording via ISRC {isrc}: {data.get('title')} by {data.get('artist-credit', [{}])[0].get('name')}")
                return data
            
            logger.debug(f"No recording found for ISRC {isrc}")
            return None
            
        except Exception as e:
            logger.debug(f"Error searching for ISRC {isrc}: {e}")
            return None
    
    def search_release_by_barcode(self, barcode: str) -> Optional[Dict]:
        """
        Search for a release by barcode (UPC/EAN).
        
        Barcode uniquely identifies a specific release (album).
        This provides accurate matching compared to name-based searches.
        """
        try:
            if not barcode:
                return None
            
            url = f"{self.base_url}/release"
            params = {
                'query': f'barcode:{barcode}',
                'fmt': 'json',
                'limit': 5
            }
            
            # Use max_retries=0 since 404 is a valid response (barcode doesn't exist)
            response = self._make_api_request(url, params, max_retries=0)
            data = response.json()
            
            releases = data.get('releases', [])
            if releases:
                # Return the first (best) match
                release = releases[0]
                logger.info(f"Found release via barcode {barcode}: {release.get('title')} by {release.get('artist-credit', [{}])[0].get('name')}")
                return release
            
            logger.debug(f"No release found for barcode {barcode}")
            return None
            
        except Exception as e:
            logger.debug(f"Error searching for barcode {barcode}: {e}")
            return None
    
    def get_artist_by_spotify_id(self, spotify_id: str) -> Optional[Dict]:
        """
        Find MusicBrainz artist by Spotify ID in relationships.
        
        Searches for artists that have a Spotify URL relationship matching the given ID.
        """
        try:
            if not spotify_id:
                return None
            
            spotify_url = f"https://open.spotify.com/artist/{spotify_id}"
            url = f"{self.base_url}/artist"
            params = {
                'query': f'url:"{spotify_url}"',
                'fmt': 'json',
                'limit': 5
            }
            
            # Use max_retries=0 since no match is a valid response
            response = self._make_api_request(url, params, max_retries=0)
            data = response.json()
            
            artists = data.get('artists', [])
            if artists:
                # Return the first (best) match
                artist = artists[0]
                logger.info(f"Found artist via Spotify ID {spotify_id}: {artist.get('name')}")
                return artist
            
            logger.debug(f"No artist found for Spotify ID {spotify_id}")
            return None
            
        except Exception as e:
            logger.debug(f"Error searching for Spotify ID {spotify_id}: {e}")
            return None
    
    def get_cover_art_url(self, release_mbid: str) -> Optional[str]:
        """Get cover art URL from Cover Art Archive."""
        try:
            # Check cache first
            if release_mbid in self._cache['cover_art']:
                logger.debug(f"Using cached cover art URL for release {release_mbid}")
                return self._cache['cover_art'][release_mbid]
            
            # Cover Art Archive API
            url = f"https://coverartarchive.org/release/{release_mbid}"
            
            response = self._make_api_request(url, max_retries=0)
            data = response.json()
            
            # Get front cover image
            images = data.get('images', [])
            front_cover = next((img for img in images if img.get('front', False)), None)
            
            if front_cover:
                cover_url = front_cover.get('image')
                # Cache the result
                self._cache['cover_art'][release_mbid] = cover_url
                return cover_url
            
            # Cache negative result to avoid repeated 404s
            self._cache['cover_art'][release_mbid] = None
            return None
            
        except Exception as e:
            logger.debug(f"No cover art found for release {release_mbid}: {e}")
            self._cache['cover_art'][release_mbid] = None
            return None
    
    def _get_spotify_access_token(self) -> Optional[str]:
        """Get Spotify access token using client credentials flow."""
        try:
            client_id = os.getenv('SPOTIFY_CLIENT_ID')
            client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
            
            if not client_id or not client_secret:
                logger.debug("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set")
                return None
            
            # Spotify token endpoint
            url = "https://accounts.spotify.com/api/token"
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'client_credentials'
            }
            
            response = requests.post(
                url, 
                headers=headers, 
                data=data,
                auth=(client_id, client_secret),
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                return token_data.get('access_token')
            else:
                logger.debug(f"Spotify token request failed with status {response.status_code}")
                return None
                
        except Exception as e:
            logger.debug(f"Error getting Spotify access token: {e}")
            return None
    
    def _get_spotify_album_url(self, album_title: str, artist_name: str = None) -> Optional[str]:
        """Get Spotify album URL by searching Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for album
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: album title and optionally artist name
            if artist_name:
                query = f'album:"{album_title}" artist:"{artist_name}"'
            else:
                query = f'album:"{album_title}"'
            
            params = {
                'q': query,
                'type': 'album',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('albums') and 
                    data['albums'].get('items') and 
                    len(data['albums']['items']) > 0):
                    album = data['albums']['items'][0]
                    
                    # Get the Spotify external URL
                    if album.get('external_urls') and album['external_urls'].get('spotify'):
                        return album['external_urls']['spotify']
                    else:
                        logger.debug(f"Spotify album {album_title} has no external URL")
                else:
                    logger.debug(f"Spotify search returned no results for album {album_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for album {album_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify album URL: {e}")
            return None
    
    def _get_spotify_album_image(self, album_title: str, artist_name: str = None) -> Optional[str]:
        """Get album cover image URL from Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for album
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: album title and optionally artist name
            if artist_name:
                query = f'album:"{album_title}" artist:"{artist_name}"'
            else:
                query = f'album:"{album_title}"'
            
            params = {
                'q': query,
                'type': 'album',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('albums') and 
                    data['albums'].get('items') and 
                    len(data['albums']['items']) > 0):
                    album = data['albums']['items'][0]
                    
                    # Spotify returns images in an array, sorted by size (largest first)
                    if album.get('images') and len(album['images']) > 0:
                        # Get the first (largest) image
                        image = album['images'][0]
                        if image.get('url'):
                            return image['url']
                    else:
                        logger.debug(f"Spotify album {album_title} has no images")
                else:
                    logger.debug(f"Spotify search returned no results for album {album_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for album {album_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify album image: {e}")
            return None
    
    def _get_spotify_track_url(self, track_title: str, artist_name: str = None) -> Optional[str]:
        """Get Spotify track URL by searching Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for track
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: track title and optionally artist name
            if artist_name:
                query = f'track:"{track_title}" artist:"{artist_name}"'
            else:
                query = f'track:"{track_title}"'
            
            params = {
                'q': query,
                'type': 'track',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('tracks') and 
                    data['tracks'].get('items') and 
                    len(data['tracks']['items']) > 0):
                    track = data['tracks']['items'][0]
                    
                    # Get the Spotify external URL
                    if track.get('external_urls') and track['external_urls'].get('spotify'):
                        return track['external_urls']['spotify']
                    else:
                        logger.debug(f"Spotify track {track_title} has no external URL")
                else:
                    logger.debug(f"Spotify search returned no results for track {track_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for track {track_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify track URL: {e}")
            return None
    
    def _extract_spotify_artist_id(self, artist_data: Optional[Dict]) -> Optional[str]:
        """Extract Spotify artist ID from MusicBrainz relations."""
        if not artist_data:
            return None
        
        relations = artist_data.get('relations') or []
        for relation in relations:
            if not isinstance(relation, dict):
                continue
            url = (relation.get('url') or {}).get('resource') or ''
            if not url:
                continue
            lowercase = url.lower()
            if 'open.spotify.com/artist/' in lowercase:
                parsed = urlparse(url)
                path = parsed.path or ''
                segments = [segment for segment in path.split('/') if segment]
                if len(segments) >= 2 and segments[0] == 'artist':
                    spotify_id = segments[1]
                    return spotify_id
            elif lowercase.startswith('spotify:artist:'):
                return url.split(':')[-1]
        return None
    
    def _get_spotify_artist_image(self, artist_name: str, artist_mbid: str = None, spotify_artist_id: Optional[str] = None) -> Optional[str]:
        """Get artist image URL from Spotify API using a known Spotify artist ID."""
        if not spotify_artist_id:
            logger.debug(f"No Spotify artist ID available for {artist_name}; skipping Spotify image fetch")
            return None
        
        try:
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            time.sleep(0.1)  # stay polite
            
            url = f"https://api.spotify.com/v1/artists/{spotify_artist_id}"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                images = data.get('images') or []
                if images:
                    image = images[0]
                    if image.get('url'):
                        logger.info(f"Using Spotify image for %s via artist ID %s", artist_name, spotify_artist_id)
                        return image['url']
                    logger.debug(f"Spotify artist {artist_name} returned image payload without URL")
                else:
                    logger.debug(f"Spotify artist {artist_name} (ID {spotify_artist_id}) has no images")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid while fetching artist %s", artist_name)
            else:
                logger.debug(
                    "Spotify API returned status %s for artist %s (ID %s)",
                    response.status_code,
                    artist_name,
                    spotify_artist_id
                )
            return None
        except Exception as e:
            logger.debug(f"Error fetching Spotify image for {artist_name}: {e}")
            return None
    
    def get_artist_image_url(self, artist_mbid: str, artist_name: str = None, artist_data: Dict = None) -> Optional[str]:
        """Get artist image URL from Spotify API."""
        try:
            if not artist_name:
                return None
            
            if not artist_data and artist_mbid:
                artist_data = self.get_artist(artist_mbid)
            
            spotify_artist_id = self._extract_spotify_artist_id(artist_data)
            if not spotify_artist_id:
                logger.info(f"No Spotify relation found for {artist_name}; cannot fetch Spotify image")
                return None
            
            spotify_image = self._get_spotify_artist_image(artist_name, artist_mbid, spotify_artist_id)
            if spotify_image:
                logger.info(f"Found Spotify image for {artist_name} via relation ID {spotify_artist_id}")
                return spotify_image
            
            return None
            
        except Exception as e:
            logger.debug(f"No artist image found for {artist_mbid}: {e}")
            return None
    
    def _parse_spotify_url(self, url: str) -> Optional[Dict[str, str]]:
        """
        Parse Spotify URL and extract type + ID.
        
        Supports formats:
        - https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6
        - https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy
        - https://open.spotify.com/artist/0OdUWJ0sBjDrqHygGUXeCF
        - spotify:track:6rqhFgbbKwnb9MLmUQDhG6
        
        Returns: {"type": "track"|"album"|"artist", "id": "spotify_id"} or None
        """
        import re
        
        if not url:
            return None
        
        # Pattern for https://open.spotify.com/{type}/{id}
        web_pattern = r'https?://open\.spotify\.com/(track|album|artist)/([a-zA-Z0-9]+)'
        web_match = re.search(web_pattern, url)
        if web_match:
            return {"type": web_match.group(1), "id": web_match.group(2)}
        
        # Pattern for spotify:{type}:{id}
        uri_pattern = r'spotify:(track|album|artist):([a-zA-Z0-9]+)'
        uri_match = re.search(uri_pattern, url)
        if uri_match:
            return {"type": uri_match.group(1), "id": uri_match.group(2)}
        
        logger.warning(f"Unable to parse Spotify URL: {url}")
        return None
    
    def _get_spotify_track_by_id(self, track_id: str) -> Optional[Dict]:
        """Fetch full track metadata from Spotify API by ID."""
        access_token = self._get_spotify_access_token()
        if not access_token:
            return None
        
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            response = self._make_api_request(url, headers=headers, max_retries=1)
            data = response.json()
            if data and data.get("id"):
                logger.info(f"Fetched Spotify track: {data.get('name')} by {data.get('artists', [{}])[0].get('name')}")
                return data
        except Exception as e:
            logger.warning(f"Error fetching Spotify track {track_id}: {e}")
        
        return None
    
    def _get_spotify_album_by_id(self, album_id: str) -> Optional[Dict]:
        """Fetch full album metadata from Spotify API by ID."""
        access_token = self._get_spotify_access_token()
        if not access_token:
            return None
        
        url = f"https://api.spotify.com/v1/albums/{album_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            response = self._make_api_request(url, headers=headers, max_retries=1)
            data = response.json()
            if data and data.get("id"):
                logger.info(f"Fetched Spotify album: {data.get('name')} by {data.get('artists', [{}])[0].get('name')}")
                return data
        except Exception as e:
            logger.warning(f"Error fetching Spotify album {album_id}: {e}")
        
        return None
    
    def _get_spotify_artist_by_id(self, artist_id: str) -> Optional[Dict]:
        """Fetch full artist metadata from Spotify API by ID."""
        access_token = self._get_spotify_access_token()
        if not access_token:
            return None
        
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            response = self._make_api_request(url, headers=headers, max_retries=1)
            data = response.json()
            if data and data.get("id"):
                logger.info(f"Fetched Spotify artist: {data.get('name')}")
                return data
        except Exception as e:
            logger.warning(f"Error fetching Spotify artist {artist_id}: {e}")
        
        return None
    
    def _extract_external_ids(self, spotify_data: Dict) -> Dict[str, Optional[str]]:
        """
        Extract external IDs (ISRC, UPC, EAN) from Spotify response.
        
        Returns: {"isrc": "...", "upc": "...", "ean": "..."}
        """
        external_ids = {}
        
        # For tracks, external_ids is directly available
        if "external_ids" in spotify_data:
            ext = spotify_data["external_ids"]
            external_ids["isrc"] = ext.get("isrc")
            external_ids["upc"] = ext.get("upc")
            external_ids["ean"] = ext.get("ean")
        
        # For albums, external_ids is also directly available
        elif "type" in spotify_data and spotify_data["type"] == "album":
            ext = spotify_data.get("external_ids", {})
            external_ids["isrc"] = ext.get("isrc")
            external_ids["upc"] = ext.get("upc")
            external_ids["ean"] = ext.get("ean")
        
        return external_ids
    
    def _extract_spotify_genres(self, spotify_data: Dict, entity_type: str) -> List[str]:
        """
        Extract genres from Spotify album or artist data.
        
        Args:
            spotify_data: Spotify API response for album or artist
            entity_type: 'album', 'artist', or 'track'
        
        Returns:
            List of genre strings
        """
        genres = []
        
        if entity_type == 'album' and spotify_data.get('genres'):
            genres.extend(spotify_data['genres'])
        elif entity_type == 'artist' and spotify_data.get('genres'):
            genres.extend(spotify_data['genres'])
        elif entity_type == 'track':
            # Tracks don't have genres directly; get from album and artist
            album_data = spotify_data.get('album', {})
            if album_data.get('id'):
                full_album = self._get_spotify_album_by_id(album_data['id'])
                if full_album and full_album.get('genres'):
                    genres.extend(full_album['genres'])
            
            # Also get from first artist
            artists = spotify_data.get('artists', [])
            if artists and artists[0].get('id'):
                artist_data = self._get_spotify_artist_by_id(artists[0]['id'])
                if artist_data and artist_data.get('genres'):
                    genres.extend(artist_data['genres'])
        
        return genres
    
    def search_labels(self, name: str, limit: int = 5) -> List[Dict]:
        """Search for labels by name."""
        try:
            url = f"{self.base_url}/label"
            params = {
                'query': name,
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('labels', [])
            
        except Exception as e:
            logger.error(f"Error searching for label '{name}': {e}")
            return []
    
    def get_label(self, mbid: str) -> Optional[Dict]:
        """Get detailed label information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['labels']:
                logger.debug(f"Using cached label data for MBID {mbid}")
                return self._cache['labels'][mbid]
            
            url = f"{self.base_url}/label/{mbid}"
            params = {
                'inc': 'aliases+tags+ratings+url-rels+area-rels+genres',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            label = response.json()
            
            # Cache the result
            self._cache['labels'][mbid] = label
            return label
            
        except Exception as e:
            logger.error(f"Error getting label {mbid}: {e}")
            return None


class NotionMusicBrainzSync:
    """Main class for synchronizing Notion databases with MusicBrainz data."""
    
    def __init__(self, notion_token: str, musicbrainz_user_agent: str,
                 artists_db_id: Optional[str] = None,
                 albums_db_id: Optional[str] = None,
                 songs_db_id: Optional[str] = None,
                 labels_db_id: Optional[str] = None):
        self.notion = NotionAPI(notion_token)
        self.mb = MusicBrainzAPI(musicbrainz_user_agent)
        
        self.artists_db_id = artists_db_id
        self.albums_db_id = albums_db_id
        self.songs_db_id = songs_db_id
        self.labels_db_id = labels_db_id
        self.locations_db_id = os.getenv('NOTION_LOCATIONS_DATABASE_ID')
        
        # Property mappings for each database
        self.artists_properties = {}
        self.albums_properties = {}
        self.songs_properties = {}
        self.labels_properties = {}
        
        # Property ID to key mappings
        self.artists_property_id_to_key = {}
        self.albums_property_id_to_key = {}
        self.songs_property_id_to_key = {}
        self.labels_property_id_to_key = {}
        
        # Caches for performance optimization
        self._location_cache = None  # Cache location name -> page_id (None = not loaded, {} = loaded empty)
        self._locations_title_key = None  # Cache title property key for locations
        self._database_pages_cache = {}  # Cache full database queries
        self._artist_mbid_map = {}
        self._album_mbid_map = {}
        self._song_mbid_map = {}
        self._label_mbid_map = {}
        
        # Load database schemas
        if self.artists_db_id:
            self._load_artists_schema()
            self._artist_mbid_map = self._build_mbid_cache(
                self.artists_db_id,
                self.artists_properties.get('musicbrainz_id'),
                self.artists_property_id_to_key,
                'artist',
            )
        if self.albums_db_id:
            self._load_albums_schema()
            self._album_mbid_map = self._build_mbid_cache(
                self.albums_db_id,
                self.albums_properties.get('musicbrainz_id'),
                self.albums_property_id_to_key,
                'album',
            )
        if self.songs_db_id:
            self._load_songs_schema()
            self._song_mbid_map = self._build_mbid_cache(
                self.songs_db_id,
                self.songs_properties.get('musicbrainz_id'),
                self.songs_property_id_to_key,
                'song',
            )
        if self.labels_db_id:
            self._load_labels_schema()
            self._label_mbid_map = self._build_mbid_cache(
                self.labels_db_id,
                self.labels_properties.get('musicbrainz_id'),
                self.labels_property_id_to_key,
                'label',
            )
    
    def _load_artists_schema(self):
        """Load and analyze the Artists database schema."""
        try:
            database = self.notion.get_database(self.artists_db_id)
            if not database:
                logger.error("Could not retrieve Artists database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.artists_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.artists_properties = {
                'title': ARTISTS_TITLE_PROPERTY_ID,
                'musicbrainz_id': ARTISTS_MUSICBRAINZ_ID_PROPERTY_ID,
                'sort_name': ARTISTS_SORT_NAME_PROPERTY_ID,
                'type': ARTISTS_TYPE_PROPERTY_ID,
                'gender': ARTISTS_GENDER_PROPERTY_ID,
                'area': ARTISTS_AREA_PROPERTY_ID,
                'born_in': ARTISTS_BORN_IN_PROPERTY_ID,
                'ig_link': ARTISTS_IG_LINK_PROPERTY_ID,
                'website_link': ARTISTS_WEBSITE_LINK_PROPERTY_ID,
                'youtube_link': ARTISTS_YOUTUBE_LINK_PROPERTY_ID,
                'bandcamp_link': ARTISTS_BANDCAMP_LINK_PROPERTY_ID,
                'streaming_link': ARTISTS_STREAMING_LINK_PROPERTY_ID,
                'country': ARTISTS_COUNTRY_PROPERTY_ID,
                'begin_date': ARTISTS_BEGIN_DATE_PROPERTY_ID,
                'end_date': ARTISTS_END_DATE_PROPERTY_ID,
                'disambiguation': ARTISTS_DISAMBIGUATION_PROPERTY_ID,
                'description': ARTISTS_DESCRIPTION_PROPERTY_ID,
                'genres': ARTISTS_GENRES_PROPERTY_ID,
                'tags': ARTISTS_TAGS_PROPERTY_ID,
                'rating': ARTISTS_RATING_PROPERTY_ID,
                'last_updated': ARTISTS_LAST_UPDATED_PROPERTY_ID,
                'musicbrainz_url': ARTISTS_MUSICBRAINZ_URL_PROPERTY_ID,
                'dns': properties.get('DNS', {}).get('id'),
            }
            
            logger.info("✓ Artists database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Artists database schema: {e}")
    
    def _load_albums_schema(self):
        """Load and analyze the Albums database schema."""
        try:
            database = self.notion.get_database(self.albums_db_id)
            if not database:
                logger.error("Could not retrieve Albums database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.albums_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.albums_properties = {
                'title': ALBUMS_TITLE_PROPERTY_ID,
                'musicbrainz_id': ALBUMS_MUSICBRAINZ_ID_PROPERTY_ID,
                'artist': ALBUMS_ARTIST_PROPERTY_ID,
                'release_date': ALBUMS_RELEASE_DATE_PROPERTY_ID,
                'country': ALBUMS_COUNTRY_PROPERTY_ID,
                'label': ALBUMS_LABEL_PROPERTY_ID,
                'type': ALBUMS_TYPE_PROPERTY_ID,
                'listen': ALBUMS_LISTEN_PROPERTY_ID,
                'status': ALBUMS_STATUS_PROPERTY_ID,
                'packaging': ALBUMS_PACKAGING_PROPERTY_ID,
                'barcode': ALBUMS_BARCODE_PROPERTY_ID,
                'format': ALBUMS_FORMAT_PROPERTY_ID,
                'track_count': ALBUMS_TRACK_COUNT_PROPERTY_ID,
                'description': ALBUMS_DESCRIPTION_PROPERTY_ID,
                'genres': ALBUMS_GENRES_PROPERTY_ID,
                'tags': ALBUMS_TAGS_PROPERTY_ID,
                'rating': ALBUMS_RATING_PROPERTY_ID,
                'cover_image': ALBUMS_COVER_IMAGE_PROPERTY_ID,
                'musicbrainz_url': ALBUMS_MUSICBRAINZ_URL_PROPERTY_ID,
                'last_updated': ALBUMS_LAST_UPDATED_PROPERTY_ID,
                'discs': ALBUMS_DISCS_PROPERTY_ID,
                'songs': ALBUMS_SONGS_PROPERTY_ID,
                'dns': properties.get('DNS', {}).get('id'),
            }
            
            logger.info("✓ Albums database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Albums database schema: {e}")
    
    def _load_songs_schema(self):
        """Load and analyze the Songs database schema."""
        try:
            database = self.notion.get_database(self.songs_db_id)
            if not database:
                logger.error("Could not retrieve Songs database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.songs_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.songs_properties = {
                'title': SONGS_TITLE_PROPERTY_ID,
                'musicbrainz_id': SONGS_MUSICBRAINZ_ID_PROPERTY_ID,
                'artist': SONGS_ARTIST_PROPERTY_ID,
                'album': SONGS_ALBUM_PROPERTY_ID,
                'track_number': SONGS_TRACK_NUMBER_PROPERTY_ID,
                'length': SONGS_LENGTH_PROPERTY_ID,
                'isrc': SONGS_ISRC_PROPERTY_ID,
                'disambiguation': SONGS_DISAMBIGUATION_PROPERTY_ID,
                'description': SONGS_DESCRIPTION_PROPERTY_ID,
                'genres': SONGS_GENRES_PROPERTY_ID,
                'tags': SONGS_TAGS_PROPERTY_ID,
                'listen': SONGS_LISTEN_PROPERTY_ID,
                'rating': SONGS_RATING_PROPERTY_ID,
                'musicbrainz_url': SONGS_MUSICBRAINZ_URL_PROPERTY_ID,
                'last_updated': SONGS_LAST_UPDATED_PROPERTY_ID,
                'disc': SONGS_DISC_PROPERTY_ID,
                'dns': properties.get('DNS', {}).get('id'),
            }
            
            logger.info("✓ Songs database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Songs database schema: {e}")
    
    def _load_labels_schema(self):
        """Load and analyze the Labels database schema."""
        try:
            database = self.notion.get_database(self.labels_db_id)
            if not database:
                logger.error("Could not retrieve Labels database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.labels_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.labels_properties = {
                'title': LABELS_TITLE_PROPERTY_ID,
                'musicbrainz_id': LABELS_MUSICBRAINZ_ID_PROPERTY_ID,
                'type': LABELS_TYPE_PROPERTY_ID,
                'country': LABELS_COUNTRY_PROPERTY_ID,
                'begin_date': LABELS_BEGIN_DATE_PROPERTY_ID,
                'end_date': LABELS_END_DATE_PROPERTY_ID,
                'disambiguation': LABELS_DISAMBIGUATION_PROPERTY_ID,
                'description': LABELS_DESCRIPTION_PROPERTY_ID,
                'genres': LABELS_GENRES_PROPERTY_ID,
                'tags': LABELS_TAGS_PROPERTY_ID,
                'rating': LABELS_RATING_PROPERTY_ID,
                'last_updated': LABELS_LAST_UPDATED_PROPERTY_ID,
                'musicbrainz_url': LABELS_MUSICBRAINZ_URL_PROPERTY_ID,
                'official_website': LABELS_OFFICIAL_WEBSITE_PROPERTY_ID,
                'ig': LABELS_IG_PROPERTY_ID,
                'bandcamp': LABELS_BANDCAMP_PROPERTY_ID,
                'founded': LABELS_FOUNDED_PROPERTY_ID,
                'albums': LABELS_ALBUMS_PROPERTY_ID,
                'area': LABELS_AREA_PROPERTY_ID,
                'dns': properties.get('DNS', {}).get('id'),
            }
            
            logger.info("✓ Labels database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Labels database schema: {e}")
    
    def _get_property_key(self, property_id: Optional[str], database: str) -> Optional[str]:
        """Get the property key for a given property ID in a specific database."""
        if not property_id:
            return None
        
        if database == 'artists':
            return self.artists_property_id_to_key.get(property_id)
        elif database == 'albums':
            return self.albums_property_id_to_key.get(property_id)
        elif database == 'songs':
            return self.songs_property_id_to_key.get(property_id)
        elif database == 'labels':
            return self.labels_property_id_to_key.get(property_id)
        return None
    
    def _fetch_artist_data_by_mbid_or_name(self, artist_name: str, artist_mbid: Optional[str]) -> Optional[Dict]:
        """Fetch full artist data from MusicBrainz by MBID or name search."""
        if artist_mbid:
            artist_data = self.mb.get_artist(artist_mbid)
            if artist_data:
                return artist_data
            logger.warning(f"Could not find artist with MBID {artist_mbid}, falling back to name search")
        
        # Fallback to name search
        search_results = self.mb.search_artists(artist_name, limit=5)
        if search_results:
            best_match = search_results[0]
            return self.mb.get_artist(best_match['id'])
        
        logger.warning(f"Could not find artist data for: {artist_name}")
        return None
    
    def _fetch_album_data_by_mbid_or_name(self, album_title: str, album_mbid: Optional[str], artist_name: Optional[str] = None) -> Optional[Dict]:
        """Fetch full album data from MusicBrainz by MBID or name search."""
        if album_mbid:
            album_data = self.mb.get_release(album_mbid)
            if album_data:
                return album_data
            logger.warning(f"Could not find album with MBID {album_mbid}, falling back to name search")
        
        # Fallback to name search (optionally with artist for better matching)
        query = album_title
        if artist_name:
            query = f"{album_title} AND artist:{artist_name}"
        
        search_results = self.mb.search_releases(query, limit=5)
        if search_results:
            best_match = search_results[0]
            return self.mb.get_release(best_match['id'])
        
        logger.warning(f"Could not find album data for: {album_title}")
        return None
    
    def _fetch_label_data_by_mbid_or_name(self, label_name: str, label_mbid: Optional[str]) -> Optional[Dict]:
        """Fetch full label data from MusicBrainz by MBID or name search."""
        if label_mbid:
            label_data = self.mb.get_label(label_mbid)
            if label_data:
                return label_data
            logger.warning(f"Could not find label with MBID {label_mbid}, falling back to name search")
        
        # Fallback to name search
        search_results = self.mb.search_labels(label_name, limit=5)
        if search_results:
            best_match = search_results[0]
            return self.mb.get_label(best_match['id'])
        
        logger.warning(f"Could not find label data for: {label_name}")
        return None
    
    def _get_database_pages(self, database_id: str) -> List[Dict]:
        """Return cached pages for a database (querying Notion once per run)."""
        if database_id in self._database_pages_cache:
            return self._database_pages_cache[database_id]
        pages = self.notion.query_database(database_id)
        self._database_pages_cache[database_id] = pages
        return pages

    @staticmethod
    def _normalize_mbid(mbid: Optional[str]) -> Optional[str]:
        if not mbid:
            return None
        return mbid.strip().lower()

    @staticmethod
    def _extract_rich_text_plain(prop: Optional[Dict]) -> Optional[str]:
        if not prop:
            return None
        rich_text = prop.get('rich_text')
        if not rich_text:
            return None
        first = rich_text[0]
        return first.get('plain_text') or first.get('text', {}).get('content')

    def _build_mbid_cache(
        self,
        database_id: Optional[str],
        mbid_property_id: Optional[str],
        property_id_to_key: Dict[str, str],
        entity_name: str,
    ) -> Dict[str, str]:
        """Create a {mbid -> page_id} cache for the specified database."""
        if not database_id or not mbid_property_id:
            return {}
        property_key = property_id_to_key.get(mbid_property_id)
        if not property_key:
            logger.warning("Skipping %s MBID cache; property %s missing in schema", entity_name, mbid_property_id)
            return {}
        pages = self._get_database_pages(database_id)
        cache: Dict[str, str] = {}
        for page in pages:
            page_id = page.get('id')
            mbid_prop = page.get('properties', {}).get(property_key)
            mbid = self._normalize_mbid(self._extract_rich_text_plain(mbid_prop))
            if mbid and page_id:
                cache[mbid] = page_id
        if cache:
            logger.debug("Cached %d %s MBIDs", len(cache), entity_name)
        return cache

    def _register_mbid(self, cache: Dict[str, str], mbid: Optional[str], page_id: Optional[str]):
        normalized = self._normalize_mbid(mbid)
        if normalized and page_id:
            cache[normalized] = page_id

    def _persist_mbid_on_page(
        self,
        database: str,
        page: Optional[Dict],
        page_id: str,
        mbid: Optional[str],
        property_id: Optional[str],
        cache: Dict[str, str],
    ):
        normalized = self._normalize_mbid(mbid)
        if not normalized or not property_id:
            return
        prop_key = self._get_property_key(property_id, database)
        if not prop_key:
            return
        existing_prop = page.get('properties', {}).get(prop_key) if page else None
        current_value = self._normalize_mbid(self._extract_rich_text_plain(existing_prop))
        if current_value == normalized:
            cache[normalized] = page_id
            return
        update_payload = {
            prop_key: {
                'rich_text': [{'text': {'content': mbid}}],
            }
        }
        if self.notion.update_page(page_id, update_payload):
            cache[normalized] = page_id
    
    def sync_artist_page(self, page: Dict, force_update: bool = False, spotify_url: str = None) -> Optional[bool]:
        """Sync a single artist page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.artists_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Artists database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'artists')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing artist: {title}")
            
            # Check for existing MBID
            mb_id_prop_id = self.artists_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'artists')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Check for Spotify URL (dual-purpose: input and output)
            spotify_url_from_notion = None
            spotify_prop_id = self.artists_properties.get('streaming_link')  # Spotify property
            if spotify_prop_id:
                spotify_key = self._get_property_key(spotify_prop_id, 'artists')
                if spotify_key:
                    spotify_prop = properties.get(spotify_key, {})
                    if spotify_prop.get('url'):
                        spotify_url_from_notion = spotify_prop['url']
            
            # Determine which Spotify URL to use
            active_spotify_url = spotify_url or spotify_url_from_notion
            spotify_provided_via_input = bool(active_spotify_url)
            
            # Search or get artist data
            artist_data = None
            spotify_artist_data = None
            
            # Try Spotify URL approach if provided
            if active_spotify_url and not existing_mbid:
                logger.info(f"Spotify URL provided: {active_spotify_url}")
                parsed = self.mb._parse_spotify_url(active_spotify_url)
                
                if parsed and parsed['type'] == 'artist':
                    spotify_artist_data = self.mb._get_spotify_artist_by_id(parsed['id'])
                    
                    if spotify_artist_data:
                        spotify_artist_id = spotify_artist_data.get('id')
                        # Search MusicBrainz by Spotify ID relationship
                        artist_data = self.mb.get_artist_by_spotify_id(spotify_artist_id)
                        
                        if artist_data:
                            logger.info(f"Successfully matched artist via Spotify ID: {spotify_artist_id}")
                        else:
                            logger.info(f"No MusicBrainz artist found with Spotify ID {spotify_artist_id}, falling back to name search")
                    else:
                        logger.warning(f"Could not fetch Spotify artist data, falling back to name search")
                elif parsed:
                    logger.warning(f"Spotify URL is not an artist (type: {parsed['type']}), ignoring")
            
            if existing_mbid:
                artist_data = self.mb.get_artist(existing_mbid)
                if not artist_data:
                    logger.warning(f"Could not find artist with MBID {existing_mbid}, searching by name")
                    existing_mbid = None
                elif not force_update:
                    # Skip pages with existing MBIDs unless force_update is True
                    logger.info(f"Skipping artist '{title}' - already has MBID {existing_mbid} (use --force-update to update)")
                    return None
            
            if not artist_data:
                search_results = self.mb.search_artists(title, limit=5)
                if not search_results:
                    logger.warning(f"Could not find artist: {title}")
                    return False
                
                # Select best match (first result for now)
                best_match = search_results[0]
                artist_data = self.mb.get_artist(best_match['id'])
            
            if not artist_data:
                logger.warning(f"Could not get artist data for: {title}")
                return False
            
            # Format properties
            # Skip writing Spotify URL if it was provided as input
            notion_props = self._format_artist_properties(
                artist_data,
                skip_spotify_url=spotify_provided_via_input
            )
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'artists')
            
            # Get artist image from Spotify
            artist_image_url = None
            if artist_data.get('id'):
                artist_name = artist_data.get('name')
                artist_image_url = self.mb.get_artist_image_url(artist_data['id'], artist_name, artist_data)
                if artist_image_url:
                    logger.info(f"Found artist image for {title} from Spotify: {artist_image_url[:50]}...")
                else:
                    logger.debug(f"No artist image found for {title}")
            
            # Set icon (use emoji if no image, otherwise image will be cover)
            icon = '🎤'  # Microphone emoji for artists
            
            # Update the page (use artist image as cover if available)
            if self.notion.update_page(page_id, notion_props, artist_image_url, icon):
                logger.info(f"Successfully updated artist: {title}")
                return True
            else:
                logger.error(f"Failed to update artist: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing artist page {page.get('id')}: {e}")
            return False
    
    def _format_artist_properties(self, artist_data: Dict, skip_spotify_url: bool = False) -> Dict:
        """Format MusicBrainz artist data for Notion properties."""
        properties = {}
        primary_artist_mbid = None
        
        try:
            # Title (name)
            if artist_data.get('name') and self.artists_properties.get('title'):
                prop_key = self._get_property_key(self.artists_properties['title'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': artist_data['name']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if artist_data.get('id') and self.artists_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.artists_properties['musicbrainz_id'], 'artists')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['id']}}]
                    }
            
            # Sort name
            if artist_data.get('sort-name') and self.artists_properties.get('sort_name'):
                prop_key = self._get_property_key(self.artists_properties['sort_name'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['sort-name']}}]
                    }
            
            # Type
            if artist_data.get('type') and self.artists_properties.get('type'):
                prop_key = self._get_property_key(self.artists_properties['type'], 'artists')
                if prop_key:
                    properties[prop_key] = {'select': {'name': artist_data['type']}}
            
            # Gender
            if artist_data.get('gender') and self.artists_properties.get('gender'):
                prop_key = self._get_property_key(self.artists_properties['gender'], 'artists')
                if prop_key:
                    properties[prop_key] = {'select': {'name': artist_data['gender']}}
            
            # Area (relation to Locations database)
            if artist_data.get('area') and artist_data['area'].get('name') and self.artists_properties.get('area') and self.locations_db_id:
                area_name = artist_data['area']['name']
                location_page_id = self._find_or_create_location_page(area_name)
                if location_page_id:
                    prop_key = self._get_property_key(self.artists_properties['area'], 'artists')
                    if prop_key:
                        properties[prop_key] = {
                            'relation': [{'id': location_page_id}]
                        }
            
            # Born In (relation to Locations database)
            if self.artists_properties.get('born_in') and self.locations_db_id:
                born_in_location = None
                # Try to get from begin-area
                if artist_data.get('begin-area') and artist_data['begin-area'].get('name'):
                    born_in_location = artist_data['begin-area']['name']
                
                prop_key = self._get_property_key(self.artists_properties['born_in'], 'artists')
                if prop_key:
                    if born_in_location:
                        # Only set relation if we have data from MusicBrainz
                        location_page_id = self._find_or_create_location_page(born_in_location)
                        if location_page_id:
                            properties[prop_key] = {
                                'relation': [{'id': location_page_id}]
                            }
                    # If no data from MusicBrainz, explicitly clear the relation
                    else:
                        properties[prop_key] = {
                            'relation': []
                        }
            
            # Extract URLs from relationships
            ig_url = None
            website_url = None
            youtube_url = None
            bandcamp_url = None
            spotify_url = None
            
            if artist_data.get('relations'):
                for relation in artist_data['relations']:
                    relation_type = relation.get('type', '').lower()
                    url_resource = relation.get('url', {}).get('resource', '').lower()
                    
                    # Instagram
                    if relation_type == 'instagram' or (relation_type == 'social network' and 'instagram' in url_resource):
                        ig_url = relation.get('url', {}).get('resource')
                    # Official homepage/website
                    elif relation_type == 'official homepage' or relation_type == 'official website':
                        website_url = relation.get('url', {}).get('resource')
                    # YouTube (exclude YouTube Music)
                    elif ('youtube' in url_resource or 'youtu.be' in url_resource) and 'music.youtube.com' not in url_resource:
                        youtube_url = relation.get('url', {}).get('resource')
                    # Bandcamp
                    elif 'bandcamp' in url_resource:
                        bandcamp_url = relation.get('url', {}).get('resource')
                    # Spotify (only extract if we're allowed to write it)
                    elif not skip_spotify_url and 'spotify' in url_resource:
                        spotify_url = relation.get('url', {}).get('resource')
            
            # IG Link
            if ig_url and self.artists_properties.get('ig_link'):
                prop_key = self._get_property_key(self.artists_properties['ig_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': ig_url}
            
            # Official Website Link
            if website_url and self.artists_properties.get('website_link'):
                prop_key = self._get_property_key(self.artists_properties['website_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': website_url}
            
            # YouTube Link
            if youtube_url and self.artists_properties.get('youtube_link'):
                prop_key = self._get_property_key(self.artists_properties['youtube_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': youtube_url}
            
            # Bandcamp Link
            if bandcamp_url and self.artists_properties.get('bandcamp_link'):
                prop_key = self._get_property_key(self.artists_properties['bandcamp_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': bandcamp_url}
            
            # Streaming Link (Spotify) - only write if it wasn't provided as input
            if not skip_spotify_url and spotify_url and self.artists_properties.get('streaming_link'):
                prop_key = self._get_property_key(self.artists_properties['streaming_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': spotify_url}
            
            # Country
            if artist_data.get('area') and artist_data['area'].get('iso-3166-1-code-list'):
                country_code = artist_data['area']['iso-3166-1-code-list'][0]
                if self.artists_properties.get('country'):
                    prop_key = self._get_property_key(self.artists_properties['country'], 'artists')
                    if prop_key:
                        properties[prop_key] = {'select': {'name': country_code}}
            
            # Begin date and End date - based on first and latest release dates
            # Using a single date property with start (first release) and end (latest release)
            if artist_data.get('id'):
                # Fetch releases for this artist to get release dates
                release_dates = self._get_artist_release_dates(artist_data['id'])
                
                if release_dates:
                    # Begin date = earliest release date (start of range)
                    earliest_date = min(release_dates)
                    # End date = latest release date (end of range)
                    latest_date = max(release_dates)
                    
                    if self.artists_properties.get('begin_date'):
                        prop_key = self._get_property_key(self.artists_properties['begin_date'], 'artists')
                        if prop_key:
                            # Set both start and end dates in the same date property
                            properties[prop_key] = {
                                'date': {
                                    'start': earliest_date[:10],  # First release date
                                    'end': latest_date[:10]       # Latest release date
                                }
                            }
            
            # Disambiguation
            if artist_data.get('disambiguation') and self.artists_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.artists_properties['disambiguation'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['disambiguation']}}]
                    }
            
            # Genres + tags - consolidate everything into the Genres property for artists
            if self.artists_properties.get('genres'):
                prop_key = self._get_property_key(self.artists_properties['genres'], 'artists')
                if prop_key:
                    genre_candidates = []
                    if artist_data.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in artist_data['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if artist_data.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in artist_data['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    genre_options = build_multi_select_options(
                        genre_candidates,
                        limit=10,
                        context='artist genres',
                    )
                    if genre_options:
                        properties[prop_key] = {'multi_select': genre_options}
            
            # MusicBrainz URL
            if artist_data.get('id') and self.artists_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/artist/{artist_data['id']}"
                prop_key = self._get_property_key(self.artists_properties['musicbrainz_url'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.artists_properties.get('last_updated'):
                prop_key = self._get_property_key(self.artists_properties['last_updated'], 'artists')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting artist properties: {e}")
        
        return properties
    
    def _get_artist_release_dates(self, artist_mbid: str) -> List[str]:
        """Get all release dates for an artist from MusicBrainz."""
        release_dates = []
        
        try:
            # Search for releases by this artist
            url = f"{self.mb.base_url}/release"
            params = {
                'query': f'arid:{artist_mbid}',
                'limit': 100,  # Get up to 100 releases
                'fmt': 'json'
            }
            
            response = self.mb._make_api_request(url, params)
            data = response.json()
            
            releases = data.get('releases', [])
            
            for release in releases:
                if release.get('date'):
                    release_date = release['date']
                    # Only add valid dates (YYYY-MM-DD format or partial)
                    # Normalize partial dates: YYYY -> YYYY-01-01, YYYY-MM -> YYYY-MM-01
                    if release_date and len(release_date) >= 4:  # At least YYYY
                        # Normalize to YYYY-MM-DD format for proper comparison
                        normalized_date = self._normalize_date(release_date)
                        if normalized_date:
                            release_dates.append(normalized_date)
            
            logger.debug(f"Found {len(release_dates)} release dates for artist {artist_mbid}")
            
        except Exception as e:
            logger.warning(f"Error fetching release dates for artist {artist_mbid}: {e}")
        
        return release_dates
    
    def _get_mbid_from_related_page(self, page_id: str, database_type: str) -> Optional[str]:
        """Get MusicBrainz ID from a related page.
        
        Args:
            page_id: The Notion page ID
            database_type: 'artists', 'albums', 'songs', or 'labels'
            
        Returns:
            The MusicBrainz ID if found, None otherwise
        """
        try:
            page = self.notion.get_page(page_id)
            if not page:
                return None
            
            properties = page.get('properties', {})
            
            # Get the MBID property ID based on database type
            mb_id_prop_id = None
            if database_type == 'artists':
                mb_id_prop_id = self.artists_properties.get('musicbrainz_id')
            elif database_type == 'albums':
                mb_id_prop_id = self.albums_properties.get('musicbrainz_id')
            elif database_type == 'songs':
                mb_id_prop_id = self.songs_properties.get('musicbrainz_id')
            elif database_type == 'labels':
                mb_id_prop_id = self.labels_properties.get('musicbrainz_id')
            
            if not mb_id_prop_id:
                return None
            
            # Get the property key
            prop_key = self._get_property_key(mb_id_prop_id, database_type)
            if not prop_key:
                return None
            
            # Extract MBID from rich_text
            mb_id_prop = properties.get(prop_key, {})
            if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                return mb_id_prop['rich_text'][0]['plain_text']
            
            return None
        except Exception as e:
            logger.debug(f"Error getting MBID from related page {page_id}: {e}")
            return None
    
    def _recording_appears_on_album(self, recording_id: str, album_mbid: str) -> bool:
        """Check if a recording appears on a specific album.
        
        Args:
            recording_id: The recording MBID
            album_mbid: The album (release) MBID
            
        Returns:
            True if the recording appears on the album, False otherwise
        """
        try:
            # Get the album/release data
            release_data = self.mb.get_release(album_mbid)
            if not release_data:
                return False
            
            # Check if any medium contains this recording
            for medium in release_data.get('media', []):
                for track in medium.get('tracks', []):
                    if track.get('recording') and track['recording'].get('id') == recording_id:
                        return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking if recording {recording_id} appears on album {album_mbid}: {e}")
            return False
    
    def _prioritize_release_groups(self, release_groups: List[Dict], preferred_title: Optional[str] = None) -> List[Dict]:
        """Sort release-groups so likely matches (by title) are checked first."""
        if not release_groups:
            return []
        
        def group_score(group: Dict) -> tuple:
            score = 0
            title = group.get('title', '') or ''
            if preferred_title:
                if self._titles_match_exactly(preferred_title, title):
                    score += 1000
                elif preferred_title.lower() in title.lower():
                    score += 100
            release_date = group.get('first-release-date') or '9999-12-31'
            return (-score, release_date, title.lower())
        
        return sorted(release_groups, key=group_score)
    
    def _iter_release_candidates(
        self,
        releases: List[Dict],
        max_candidates: int = 10,
        group_meta: Optional[Dict] = None
    ):
        """Yield full release data ordered by desirability."""
        if not releases:
            return
        
        disallowed_secondary = {'live', 'compilation', 'soundtrack', 'remix', 'dj-mix'}
        candidate_releases = []
        for release in releases:
            status = (release.get('status') or '').lower()
            if status != 'official':
                continue
            
            release_group = release.get('release-group') or group_meta or {}
            primary_type = (release_group.get('primary-type') or '').lower()
            if primary_type and primary_type != 'album':
                continue
            
            secondary_types = [t.lower() for t in release_group.get('secondary-types', [])]
            if any(t in disallowed_secondary for t in secondary_types):
                continue
            
            candidate_releases.append(release)
        
        if not candidate_releases:
            return
        
        scored_releases = []
        for release in candidate_releases:
            score, date = self._score_release_for_song(release)
            scored_releases.append((score, date, release))
        
        scored_releases.sort(key=lambda x: (-x[0], x[1]))
        seen = set()
        
        limit = min(max_candidates, len(scored_releases))
        for i in range(limit):
            _, _, release = scored_releases[i]
            release_mbid = release.get('id')
            if not release_mbid or release_mbid in seen:
                continue
            seen.add(release_mbid)
            full_release = self.mb.get_release(release_mbid)
            if full_release:
                yield full_release
    
    def _match_track_in_release(self, release_data: Dict, search_title: str) -> Optional[Dict]:
        """Check if a release contains the target track, returning detailed match info."""
        if not release_data:
            return None
        
        for medium in release_data.get('media', []):
            for track in medium.get('tracks', []):
                track_title = track.get('title', '')
                recording = track.get('recording', {}) or {}
                recording_id = recording.get('id')
                match_reason = None
                recording_data = None
                
                if self._titles_match_exactly(search_title, track_title):
                    if not recording_id:
                        continue
                    match_reason = 'title'
                    recording_data = self.mb.get_recording(recording_id)
                    if not recording_data and recording:
                        # Build a minimal payload from the track so callers can skip the slower
                        # recording search fallback even if the detailed API call fails.
                        recording_data = {
                            'id': recording_id,
                            'title': track_title,
                            'artist-credit': recording.get('artist-credit') or release_data.get('artist-credit') or [],
                            'length': recording.get('length'),
                            'releases': [release_data],
                        }
                    if not recording_data:
                        continue
                elif recording_id:
                    recording_data = self.mb.get_recording(recording_id)
                    if recording_data and self._recording_title_matches(recording_data, search_title):
                        match_reason = 'alias'
                    else:
                        continue
                else:
                    continue
                
                return {
                    'recording_data': recording_data,
                    'release': release_data,
                    'track': track,
                    'medium': medium,
                    'match_reason': match_reason
                }
        
        return None
    
    def _find_release_via_release_groups(
        self,
        song_title: str,
        artist_mbid: str,
        preferred_album_mbid: Optional[str] = None,
        preferred_album_title: Optional[str] = None
    ) -> Optional[Dict]:
        """Find a release/recording by walking artist release-groups instead of recording search."""
        # Check a preferred album (from Notion) first, if provided
        if preferred_album_mbid:
            release_data = self.mb.get_release(preferred_album_mbid)
            if release_data:
                match = self._match_track_in_release(release_data, song_title)
                if match:
                    logger.info(f"Matched '{song_title}' via preferred album release {preferred_album_mbid}")
                    match['release_group'] = release_data.get('release-group')
                    return match
        
        release_groups = self.mb.get_artist_release_groups(artist_mbid)
        if not release_groups:
            logger.info(f"No release-groups found for artist {artist_mbid}")
            return None
        
        prioritized_groups = self._prioritize_release_groups(release_groups, preferred_album_title)
        max_groups = 20  # avoid walking an entire massive catalog
        checked = 0
        disallowed_secondary = {'live', 'compilation', 'soundtrack', 'remix', 'dj-mix'}
        
        for group in prioritized_groups:
            if checked >= max_groups:
                break
            group_id = group.get('id')
            if not group_id:
                continue
            
            primary_type = (group.get('primary-type') or '').lower()
            if primary_type and primary_type != 'album':
                logger.debug(f"Skipping release-group '{group.get('title')}' (primary type: {primary_type})")
                continue
            
            secondary_types = [t.lower() for t in group.get('secondary-types', [])]
            if any(t in disallowed_secondary for t in secondary_types):
                logger.debug(
                    f"Skipping release-group '{group.get('title')}' due to secondary types: {secondary_types}"
                )
                continue
            
            group_data = self.mb.get_release_group(group_id)
            if not group_data or not group_data.get('releases'):
                continue
            
            if not primary_type:
                fetched_primary = (group_data.get('primary-type') or '').lower()
                if fetched_primary and fetched_primary != 'album':
                    logger.debug(f"Skipping release-group '{group.get('title')}' after fetch (primary type: {fetched_primary})")
                    continue
            fetched_secondary = [t.lower() for t in group_data.get('secondary-types', [])]
            if any(t in disallowed_secondary for t in fetched_secondary):
                logger.debug(
                    f"Skipping release-group '{group.get('title')}' after fetch due to secondary types: {fetched_secondary}"
                )
                continue
            
            checked += 1
            for release in self._iter_release_candidates(
                group_data.get('releases', []),
                max_candidates=1,
                group_meta=group_data
            ):
                match = self._match_track_in_release(release, song_title)
                if match:
                    logger.info(
                        f"Matched '{song_title}' via release '{release.get('title')}' "
                        f"from release-group '{group.get('title')}'"
                    )
                    match['release_group'] = group_data
                    return match
        
        logger.info(f"No release containing '{song_title}' found via release-groups for artist {artist_mbid}")
        return None
    
    def _recording_title_matches(self, recording_data: Dict, search_title: str) -> bool:
        """Check if a recording's title or aliases match the search title.
        
        Args:
            recording_data: The recording data from MusicBrainz
            search_title: The title we're searching for
            
        Returns:
            True if the recording title or any alias matches the search title
        """
        try:
            # Check main title
            recording_title = recording_data.get('title', '')
            if self._titles_match_exactly(search_title, recording_title):
                return True
            
            # Check aliases
            aliases = recording_data.get('aliases', [])
            for alias in aliases:
                alias_name = alias.get('name', '')
                if alias_name and self._titles_match_exactly(search_title, alias_name):
                    return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking if recording title matches '{search_title}': {e}")
            return False
    
    def _recording_is_by_artist(self, recording_data: Dict, artist_mbid: str) -> bool:
        """Check if a recording is by a specific artist.
        
        Args:
            recording_data: The recording data from MusicBrainz (can be from search or full fetch)
            artist_mbid: The artist MBID
            
        Returns:
            True if the recording is by the artist, False otherwise
        """
        try:
            # This helper often drives the longest wall-clock time on a single song because we may
            # need to fetch the full recording payload (rate-limited) whenever search results omit
            # artist-credit info. Keep callers aware so they can cache results.
            recording_id = recording_data.get('id')
            recording_title = recording_data.get('title', 'Unknown')
            
            # Check artist-credit in recording data (may be present in search results)
            if recording_data.get('artist-credit'):
                for ac in recording_data['artist-credit']:
                    if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                        logger.info(f"Recording '{recording_title}' ({recording_id}) is by artist {artist_mbid} (from search result)")
                        return True
                    elif ac.get('artist'):
                        logger.info(f"Recording '{recording_title}' artist: {ac['artist'].get('id')} (expected: {artist_mbid})")
            
            # If artist-credit not in search results, fetch full recording data
            if recording_id:
                logger.info(f"Fetching full recording data for '{recording_title}' ({recording_id}) to verify artist")
                full_recording = self.mb.get_recording(recording_id)
                if full_recording and full_recording.get('artist-credit'):
                    for ac in full_recording['artist-credit']:
                        if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                            logger.info(f"Recording '{recording_title}' ({recording_id}) is by artist {artist_mbid} (from full fetch)")
                            return True
                        elif ac.get('artist'):
                            logger.info(f"Recording '{recording_title}' artist: {ac['artist'].get('id')} (expected: {artist_mbid})")
                elif not full_recording:
                    logger.warning(f"Could not fetch full recording data for {recording_id}")
                elif not full_recording.get('artist-credit'):
                    logger.warning(f"Full recording data for {recording_id} has no artist-credit")
            
            logger.info(f"Recording '{recording_title}' ({recording_id}) is NOT by artist {artist_mbid}")
            return False
        except Exception as e:
            logger.warning(f"Error checking if recording is by artist {artist_mbid}: {e}")
            return False
    
    def _recording_release_rank(
        self,
        recording_data: Dict,
        album_mbid: Optional[str],
        artist_mbid: Optional[str]
    ) -> int:
        """Rank how closely a recording's releases match the desired album.
        
        Returns:
            4 - Exact release MBID match
            3 - Album release by target artist
            2 - Album release (compilation/soundtrack)
            1 - Single release
            0 - No useful release data
        """
        releases = recording_data.get('releases', []) or []
        best_rank = 0
        
        for release in releases:
            release_id = release.get('id')
            release_group = release.get('release-group', {}) or {}
            primary_type = (release_group.get('primary-type') or '').lower()
            secondary_types = [t.lower() for t in release_group.get('secondary-types', [])]
            release_artist_mbids = [
                ac.get('artist', {}).get('id')
                for ac in release.get('artist-credit', [])
                if ac.get('artist')
            ]
            
            if album_mbid and release_id == album_mbid:
                return 4
            
            if primary_type == 'album':
                if artist_mbid and artist_mbid not in release_artist_mbids:
                    continue
                
                if 'compilation' in secondary_types or 'soundtrack' in secondary_types:
                    best_rank = max(best_rank, 2)
                else:
                    return 3
            elif primary_type == 'single':
                best_rank = max(best_rank, 1)
        
        return best_rank
    
    def _release_is_by_artist(self, release_data: Dict, artist_mbid: str) -> bool:
        """Check if a release is by a specific artist.
        
        Args:
            release_data: The release data from MusicBrainz
            artist_mbid: The artist MBID
            
        Returns:
            True if the release is by the artist, False otherwise
        """
        try:
            # Check artist-credit
            if release_data.get('artist-credit'):
                for ac in release_data['artist-credit']:
                    if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                        return True
            
            # Check release-group artist-credit
            if release_data.get('release-group') and release_data['release-group'].get('artist-credit'):
                for ac in release_data['release-group']['artist-credit']:
                    if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                        return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking if release is by artist {artist_mbid}: {e}")
            return False
    
    def _release_contains_recordings(self, release_data: Dict, recording_mbids: List[str], recording_titles: List[str] = None) -> bool:
        """Check if a release contains all specified recordings.
        
        Args:
            release_data: The release data from MusicBrainz
            recording_mbids: List of recording MBIDs that must appear on the release
            recording_titles: Optional list of recording titles to check if MBIDs aren't available
            
        Returns:
            True if the release contains all recordings, False otherwise
        """
        if not recording_mbids and not recording_titles:
            return True
        
        try:
            # Collect all recording IDs and titles from the release
            release_recording_ids = set()
            release_recording_titles = set()
            for medium in release_data.get('media', []):
                for track in medium.get('tracks', []):
                    recording = track.get('recording', {})
                    if recording.get('id'):
                        release_recording_ids.add(recording['id'])
                    if recording.get('title'):
                        # Normalize title for comparison
                        normalized_title = ' '.join(self._normalize_title_for_matching(recording['title']))
                        release_recording_titles.add(normalized_title)
            
            # Check by MBID first (most reliable)
            if recording_mbids:
                required_set = set(recording_mbids)
                if not required_set.issubset(release_recording_ids):
                    return False
            
            # Check by title if MBIDs weren't available or as additional verification
            if recording_titles:
                required_titles = {' '.join(self._normalize_title_for_matching(title)) for title in recording_titles}
                if not required_titles.issubset(release_recording_titles):
                    return False
            
            return True
        except Exception as e:
            logger.debug(f"Error checking if release contains recordings: {e}")
            return False
    
    def _merge_relations(self, page: Dict, new_properties: Dict, database_type: str) -> Dict:
        """Merge new relation properties with existing relations to preserve user-added connections.
        
        Args:
            page: The existing Notion page
            new_properties: New properties to be set
            database_type: 'artists', 'albums', or 'songs'
            
        Returns:
            Updated properties dict with merged relations
        """
        try:
            existing_properties = page.get('properties', {})
            merged_properties = new_properties.copy()
            
            # Get relation property IDs for this database type
            relation_property_ids = []
            if database_type == 'artists':
                # Artists don't typically have relations to other artists/albums/songs in our schema
                # But we should preserve any relations they might have
                pass
            elif database_type == 'albums':
                relation_property_ids = [
                    ('artist', self.albums_properties.get('artist')),
                    ('songs', self.albums_properties.get('songs')),
                    ('label', self.albums_properties.get('label')),
                ]
            elif database_type == 'songs':
                relation_property_ids = [
                    ('artist', self.songs_properties.get('artist')),
                    ('album', self.songs_properties.get('album')),
                ]
            
            # Merge each relation property
            for relation_name, relation_prop_id in relation_property_ids:
                if not relation_prop_id:
                    continue
                
                # Get property keys
                new_prop_key = self._get_property_key(relation_prop_id, database_type)
                if not new_prop_key:
                    continue
                
                # Get existing relations
                existing_relation_prop = existing_properties.get(new_prop_key, {})
                existing_relations = existing_relation_prop.get('relation', [])
                existing_relation_ids = {rel.get('id') for rel in existing_relations if rel.get('id')}
                
                # Get new relations (if the property exists in new_properties)
                new_relation_prop = new_properties.get(new_prop_key, {})
                new_relations = new_relation_prop.get('relation', [])
                new_relation_ids = {rel.get('id') for rel in new_relations if rel.get('id')}
                
                # If new_properties has this relation property, merge with existing
                # If it doesn't have it, preserve existing relations by not updating
                if new_prop_key in new_properties:
                    # Merge: combine existing and new, avoiding duplicates
                    merged_relation_ids = existing_relation_ids | new_relation_ids
                    merged_relations = [{'id': rel_id} for rel_id in merged_relation_ids]
                    
                    # Always set the merged relations (even if empty, to preserve existing if new is empty)
                    merged_properties[new_prop_key] = {'relation': merged_relations}
                    logger.debug(f"Merged {relation_name} relations: {len(existing_relations)} existing + {len(new_relations)} new = {len(merged_relations)} total")
                elif existing_relations:
                    # If new_properties doesn't have this relation, preserve existing by not updating
                    # (existing relations will remain unchanged)
                    logger.debug(f"Preserving existing {relation_name} relations: {len(existing_relations)} (not in new properties)")
            
            return merged_properties
            
        except Exception as e:
            logger.warning(f"Error merging relations: {e}")
            # Return new properties if merge fails
            return new_properties
    
    def _normalize_title_for_matching(self, title: str) -> List[str]:
        """Normalize a title for exact word matching.
        
        Removes special characters, converts to lowercase, and splits into words.
        Used to compare titles word-for-word (not fuzzy matching).
        
        Args:
            title: The title to normalize
            
        Returns:
            List of normalized words
        """
        if not title:
            return []
        
        # Remove special characters, keep only alphanumeric and spaces
        normalized = re.sub(r'[^a-zA-Z0-9\s]', ' ', title)
        # Convert to lowercase and split into words
        words = [word for word in normalized.lower().split() if word]
        return words
    
    def _titles_match_exactly(self, title1: str, title2: str) -> bool:
        """Check if two titles match exactly (word-for-word, case-insensitive, ignoring special chars).
        
        Args:
            title1: First title
            title2: Second title
            
        Returns:
            True if titles match word-for-word, False otherwise
        """
        words1 = self._normalize_title_for_matching(title1)
        words2 = self._normalize_title_for_matching(title2)
        return words1 == words2
    
    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize a date string to YYYY-MM-DD format for comparison.
        
        Handles partial dates:
        - YYYY -> YYYY-01-01
        - YYYY-MM -> YYYY-MM-01
        - YYYY-MM-DD -> YYYY-MM-DD (unchanged)
        """
        if not date_str or len(date_str) < 4:
            return None
        
        try:
            parts = date_str.split('-')
            year = parts[0]
            
            if len(parts) == 1:
                # Just YYYY
                return f"{year}-01-01"
            elif len(parts) == 2:
                # YYYY-MM
                month = parts[1]
                return f"{year}-{month}-01"
            else:
                # YYYY-MM-DD (or more)
                return date_str[:10]  # Take first 10 chars (YYYY-MM-DD)
        except Exception:
            return None
    
    def sync_album_page(self, page: Dict, force_update: bool = False, spotify_url: str = None) -> Optional[bool]:
        """Sync a single album page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.albums_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Albums database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'albums')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing album: {title}")
            
            # Try to extract artist name and MBID from relation
            artist_name = None
            artist_mbid = None
            artist_prop_id = self.albums_properties.get('artist')
            if artist_prop_id:
                artist_key = self._get_property_key(artist_prop_id, 'albums')
                if artist_key:
                    artist_prop = properties.get(artist_key, {})
                    if artist_prop.get('relation'):
                        # Get first related artist
                        relation = artist_prop['relation']
                        if relation:
                            # Fetch the artist page to get the name and MBID
                            artist_page_id = relation[0]['id']
                            artist_page = self.notion.get_page(artist_page_id)
                            if artist_page:
                                artist_props = artist_page.get('properties', {})
                                artist_title_key = self._get_property_key(self.artists_properties.get('title'), 'artists')
                                if artist_title_key and artist_props.get(artist_title_key):
                                    artist_title_prop = artist_props[artist_title_key]
                                    if artist_title_prop.get('title') and artist_title_prop['title']:
                                        artist_name = artist_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found artist from relation: {artist_name}")
                                
                                # Get artist MBID for verification
                                artist_mbid = self._get_mbid_from_related_page(artist_page_id, 'artists')
                                if artist_mbid:
                                    logger.debug(f"Found artist MBID from relation: {artist_mbid}")
            
            # Try to extract related song MBIDs and titles from relation
            song_mbids = []
            song_titles = []
            songs_prop_id = self.albums_properties.get('songs')
            if songs_prop_id:
                songs_key = self._get_property_key(songs_prop_id, 'albums')
                if songs_key:
                    songs_prop = properties.get(songs_key, {})
                    if songs_prop.get('relation'):
                        logger.info(f"Found {len(songs_prop['relation'])} related song(s) for album")
                        # Get MBIDs and titles from all related song pages
                        for song_relation in songs_prop['relation']:
                            song_page_id = song_relation.get('id')
                            if song_page_id:
                                song_mbid = self._get_mbid_from_related_page(song_page_id, 'songs')
                                if song_mbid:
                                    song_mbids.append(song_mbid)
                                    logger.debug(f"Found song MBID from relation: {song_mbid}")
                                
                                # Also get song title as fallback
                                song_page = self.notion.get_page(song_page_id)
                                if song_page:
                                    song_props = song_page.get('properties', {})
                                    song_title_key = self._get_property_key(self.songs_properties.get('title'), 'songs')
                                    if song_title_key and song_props.get(song_title_key):
                                        song_title_prop = song_props[song_title_key]
                                        if song_title_prop.get('title') and song_title_prop['title']:
                                            song_title = song_title_prop['title'][0]['plain_text']
                                            song_titles.append(song_title)
                                            logger.info(f"Found song title from relation: {song_title}")
                                else:
                                    logger.warning(f"Could not fetch song page {song_page_id} to get title")
            
            # Check for existing MBID
            mb_id_prop_id = self.albums_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'albums')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Check for Spotify URL (dual-purpose: input and output)
            spotify_url_from_notion = None
            spotify_prop_id = self.albums_properties.get('listen')  # Spotify property
            if spotify_prop_id:
                spotify_key = self._get_property_key(spotify_prop_id, 'albums')
                if spotify_key:
                    spotify_prop = properties.get(spotify_key, {})
                    if spotify_prop.get('url'):
                        spotify_url_from_notion = spotify_prop['url']
            
            # Determine which Spotify URL to use
            active_spotify_url = spotify_url or spotify_url_from_notion
            spotify_provided_via_input = bool(active_spotify_url)
            
            # Search or get release data
            release_data = None
            if existing_mbid:
                release_data = self.mb.get_release(existing_mbid)
                if not release_data:
                    logger.warning(f"Could not find release with MBID {existing_mbid}, searching by title")
                    existing_mbid = None
                else:
                    # Verify the existing release contains all related songs
                    if song_mbids or song_titles:
                        logger.info(f"Verifying existing release {existing_mbid} contains {len(song_mbids)} song MBIDs and {len(song_titles)} song titles")
                        if not self._release_contains_recordings(release_data, song_mbids, song_titles):
                            logger.warning(f"Existing release {existing_mbid} does not contain all related songs, searching for a new match")
                            release_data = None
                            existing_mbid = None
                            # Keep artist filter - we'll use it to narrow search and verify songs match
                            # Only clear if we're absolutely sure the artist is wrong (which we can't know yet)
                        else:
                            logger.info(f"Existing release {existing_mbid} contains all related songs, using it")
                            # Skip pages with existing MBIDs unless force_update is True
                            if not force_update:
                                logger.info(f"Skipping album '{title}' - already has MBID {existing_mbid} (use --force-update to update)")
                                return None
                    elif not force_update:
                        # No related songs to verify, skip if force_update is False
                        logger.info(f"Skipping album '{title}' - already has MBID {existing_mbid} (use --force-update to update)")
                        return None
            
            spotify_album_data = None
            # Try Spotify URL approach if provided
            if active_spotify_url and not existing_mbid:
                logger.info(f"Spotify URL provided: {active_spotify_url}")
                parsed = self.mb._parse_spotify_url(active_spotify_url)
                
                if parsed and parsed['type'] == 'album':
                    spotify_album_data = self.mb._get_spotify_album_by_id(parsed['id'])
                    
                    if spotify_album_data:
                        # Extract UPC/EAN from Spotify data
                        external_ids = self.mb._extract_external_ids(spotify_album_data)
                        barcode = external_ids.get('upc') or external_ids.get('ean')
                        
                        if barcode:
                            logger.info(f"Found barcode from Spotify: {barcode}")
                            # Search MusicBrainz by barcode
                            release_data = self.mb.search_release_by_barcode(barcode)
                            
                            if release_data:
                                logger.info(f"Successfully matched release via Spotify barcode: {barcode}")
                            else:
                                logger.info(f"No MusicBrainz match for barcode {barcode}, falling back to name search")
                        else:
                            logger.info("Spotify album has no barcode, falling back to name search")
                    else:
                        logger.warning(f"Could not fetch Spotify album data, falling back to name search")
                elif parsed:
                    logger.warning(f"Spotify URL is not an album (type: {parsed['type']}), ignoring")
            
            if not release_data:
                # New approach: Use related artist or song MBID to get candidate releases
                # Priority: artist MBID > song MBID > song title
                search_results = []
                
                if artist_mbid:
                    # Get all releases by this artist
                    logger.info(f"Searching for releases by artist MBID: {artist_mbid}")
                    url = f"{self.mb.base_url}/release"
                    params = {
                        'query': f'arid:{artist_mbid}',
                        'limit': 100,  # Get up to 100 releases
                        'fmt': 'json'
                    }
                    response = self.mb._make_api_request(url, params)
                    data = response.json()
                    search_results = data.get('releases', [])
                    logger.info(f"Found {len(search_results)} releases by artist")
                
                elif song_mbids:
                    # Get all releases containing this song
                    logger.info(f"Searching for releases containing song MBID: {song_mbids[0]}")
                    search_results = self.mb.search_releases_by_recording(song_mbids[0], limit=100)
                    logger.info(f"Found {len(search_results)} releases containing song")
                
                elif song_titles:
                    # Find the song MBID first, then get releases
                    logger.info(f"Searching for song by title: {song_titles[0]}")
                    recording_search = self.mb.search_recordings(song_titles[0], limit=5)
                    if recording_search:
                        # Take the first exact match
                        for rec in recording_search:
                            if self._titles_match_exactly(song_titles[0], rec.get('title', '')):
                                recording_id = rec.get('id')
                                logger.info(f"Found song MBID: {recording_id}, searching for releases")
                                search_results = self.mb.search_releases_by_recording(recording_id, limit=100)
                                logger.info(f"Found {len(search_results)} releases containing song")
                                break
                
                # If we still don't have results, fall back to regular search
                if not search_results:
                    logger.info(f"Falling back to regular search for: {title}")
                    search_results = self.mb.search_releases(title, artist_name, limit=50)
                
                if not search_results:
                    logger.warning(f"Could not find album: {title}")
                    return False
                
                # Filter releases by exact title match
                matching_releases = []
                for result in search_results:
                    result_title = result.get('title', '')
                    if self._titles_match_exactly(title, result_title):
                        matching_releases.append(result)
                
                if not matching_releases:
                    logger.warning(f"No releases found with exact title match for '{title}'")
                    return False
                
                logger.info(f"Found {len(matching_releases)} releases with matching title")
                
                # Use existing scoring logic to find the best release
                # Optimization: Score with available data first, then fetch full data for top candidates
                scored_releases = []
                for release in matching_releases:
                    # Use the same scoring logic as songs (US country, album type, earliest date)
                    score, date = self._score_release_for_song(release)
                    
                    # Boost score if release contains all related songs (check with available data first)
                    contains_songs = False
                    if song_mbids or song_titles:
                        contains_songs = self._release_contains_recordings(release, song_mbids, song_titles)
                        if contains_songs:
                            score += 1000  # Large boost for containing required songs
                    
                    scored_releases.append((score, date, release, contains_songs))
                
                # Sort by score (descending), then by date (ascending - earlier is better)
                scored_releases.sort(key=lambda x: (-x[0], x[1]))
                
                # Only fetch full release data for top 10 candidates (or all if < 10)
                top_candidates = min(10, len(scored_releases))
                top_releases = []
                
                for i in range(top_candidates):
                    score, date, release, contains_songs = scored_releases[i]
                    release_mbid = release.get('id')
                    
                    # Fetch full release data for accurate final scoring
                    if release_mbid:
                        full_release = self.mb.get_release(release_mbid)
                        if full_release:
                            # Re-score with full data
                            score, date = self._score_release_for_song(full_release)
                            
                            # Re-check if release contains all related songs with full data
                            if song_mbids or song_titles:
                                contains_songs = self._release_contains_recordings(full_release, song_mbids, song_titles)
                                if contains_songs:
                                    score += 1000
                            
                            release = full_release
                    
                    top_releases.append((score, date, release, contains_songs))
                
                # Re-sort top candidates with full data
                top_releases.sort(key=lambda x: (-x[0], x[1]))
                
                if not top_releases:
                    logger.warning(f"Could not fetch release data for scoring")
                    return False
                
                # If we have required songs, filter to only releases that contain them
                if song_mbids or song_titles:
                    releases_with_songs = [r for r in top_releases if r[3]]  # r[3] is contains_songs
                    if releases_with_songs:
                        # Only consider releases that contain all required songs
                        top_releases = releases_with_songs
                        logger.info(f"Filtered to {len(top_releases)} releases that contain all required songs")
                    else:
                        logger.warning(f"No releases found that contain all required songs, but continuing anyway")
                
                # Get the best match
                best_release = top_releases[0][2]
                contains_songs = top_releases[0][3]
                
                if contains_songs:
                    logger.info(f"Best release contains all related songs")
                elif song_mbids or song_titles:
                    logger.warning(f"Best release does not contain all related songs")
                
                release_data = best_release
                logger.info(f"Selected best release: {best_release.get('title')} (ID: {best_release.get('id')})")
            
            if not release_data:
                logger.warning(f"Could not get album data for: {title}")
                return False
            
            # Format properties
            # Skip writing Spotify URL if it was provided as input
            notion_props = self._format_album_properties(
                release_data,
                skip_spotify_url=spotify_provided_via_input
            )
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'albums')
            
            # Get cover art - try Cover Art Archive first, then Spotify as fallback
            cover_url = None
            if release_data.get('id'):
                cover_url = self.mb.get_cover_art_url(release_data['id'])
                if not cover_url:
                    # Fallback to Spotify if Cover Art Archive doesn't have it
                    album_title = release_data.get('title', title)
                    artist_name = None
                    # Get artist name from artist-credit
                    if release_data.get('artist-credit') and release_data['artist-credit']:
                        first_artist = release_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    if album_title:
                        cover_url = self.mb._get_spotify_album_image(album_title, artist_name)
                        if cover_url:
                            logger.info(f"Found album cover image from Spotify for {title}")
            
            # Set icon
            icon = '💿'  # CD emoji for albums
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, cover_url, icon):
                logger.info(f"Successfully updated album: {title}")
                return True
            else:
                logger.error(f"Failed to update album: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing album page {page.get('id')}: {e}")
            return False
    
    def _format_album_properties(self, release_data: Dict, skip_spotify_url: bool = False, set_dns_on_labels: bool = False) -> Dict:
        """Format MusicBrainz release data for Notion properties."""
        properties = {}
        
        try:
            # Title
            if release_data.get('title') and self.albums_properties.get('title'):
                prop_key = self._get_property_key(self.albums_properties['title'], 'albums')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': release_data['title']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if release_data.get('id') and self.albums_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.albums_properties['musicbrainz_id'], 'albums')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': release_data['id']}}]
                    }
            
            # Release date
            if release_data.get('date') and self.albums_properties.get('release_date'):
                release_date = release_data['date']
                prop_key = self._get_property_key(self.albums_properties['release_date'], 'albums')
                if prop_key:
                    properties[prop_key] = {'date': {'start': release_date[:10]}}
            
            # Artists (as relations)
            if release_data.get('artist-credit') and self.albums_properties.get('artist') and self.artists_db_id:
                # Extract artist names and MBIDs from artist-credit
                artist_names = []
                artist_mbids = []
                
                for ac in release_data.get('artist-credit', []):
                    if ac.get('artist'):
                        artist = ac['artist']
                        artist_name = artist.get('name')
                        artist_mbid = artist.get('id')
                        if artist_name:
                            artist_names.append(artist_name)
                            if artist_mbid:
                                artist_mbids.append(artist_mbid)
                            else:
                                artist_mbids.append(None)
                
                if artist_names:
                    # Find or create artist pages and get their IDs
                    artist_page_ids = []
                    for i, artist_name in enumerate(artist_names[:5]):  # Limit to 5 artists
                        artist_mbid = artist_mbids[i] if i < len(artist_mbids) else None
                        artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid)
                        if artist_page_id:
                            artist_page_ids.append(artist_page_id)
                    
                    if artist_page_ids:
                        prop_key = self._get_property_key(self.albums_properties['artist'], 'albums')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in artist_page_ids]
                            }
            
            # Country
            if release_data.get('country') and self.albums_properties.get('country'):
                prop_key = self._get_property_key(self.albums_properties['country'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['country']}}
            
            # Labels (as relations)
            if release_data.get('label-info') and self.albums_properties.get('label') and self.labels_db_id:
                label_names = [li['label']['name'] for li in release_data['label-info'] if li.get('label', {}).get('name')]
                label_mbids = [li['label']['id'] for li in release_data['label-info'] if li.get('label', {}).get('id')]
                
                if label_names:
                    # Find or create label pages and get their IDs
                    label_page_ids = []
                    for i, label_name in enumerate(label_names[:5]):  # Limit to 5 labels
                        label_mbid = label_mbids[i] if i < len(label_mbids) else None
                        # Set DNS=True if this is part of Spotify URL flow
                        label_page_id = self._find_or_create_label_page(label_name, label_mbid, set_dns=set_dns_on_labels)
                        if label_page_id:
                            label_page_ids.append(label_page_id)
                    
                    if label_page_ids:
                        prop_key = self._get_property_key(self.albums_properties['label'], 'albums')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in label_page_ids]
                            }
            
            # Status
            if release_data.get('status') and self.albums_properties.get('status'):
                prop_key = self._get_property_key(self.albums_properties['status'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['status']}}
            
            # Packaging
            if release_data.get('packaging') and self.albums_properties.get('packaging'):
                prop_key = self._get_property_key(self.albums_properties['packaging'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['packaging']}}
            
            # Barcode
            if release_data.get('barcode') and self.albums_properties.get('barcode'):
                prop_key = self._get_property_key(self.albums_properties['barcode'], 'albums')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': release_data['barcode']}}]
                    }
            
            # Format
            if release_data.get('media') and self.albums_properties.get('format'):
                formats = []
                for medium in release_data['media']:
                    if medium.get('format'):
                        formats.append(medium['format'])
                if formats:
                    prop_key = self._get_property_key(self.albums_properties['format'], 'albums')
                    if prop_key:
                        format_options = build_multi_select_options(
                            formats,
                            context='album formats',
                        )
                        if format_options:
                            properties[prop_key] = {'multi_select': format_options}
            
            # Track count
            if release_data.get('media') and self.albums_properties.get('track_count'):
                total_tracks = sum(medium.get('track-count', 0) for medium in release_data['media'])
                if total_tracks > 0:
                    prop_key = self._get_property_key(self.albums_properties['track_count'], 'albums')
                    if prop_key:
                        properties[prop_key] = {'number': total_tracks}
            
            # Disc count (only if > 1)
            if release_data.get('media') and self.albums_properties.get('discs'):
                disc_count = len(release_data['media'])
                if disc_count > 1:
                    prop_key = self._get_property_key(self.albums_properties['discs'], 'albums')
                    if prop_key:
                        properties[prop_key] = {'number': disc_count}
            
            # Genres/Tags combination for albums
            release_group = release_data.get('release-group') or {}
            if self.albums_properties.get('genres'):
                prop_key = self._get_property_key(self.albums_properties['genres'], 'albums')
                if prop_key:
                    genre_candidates = []
                    if release_group.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in release_group['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if release_data.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in release_data['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if release_group.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in release_group['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    if release_data.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in release_data['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    
                    # Add Spotify album genres if available
                    if release_data.get('relations'):
                        for relation in release_data.get('relations', []):
                            if relation.get('type', '').lower() in ['streaming', 'free streaming']:
                                url_resource = relation.get('url', {})
                                if isinstance(url_resource, dict):
                                    url_str = url_resource.get('resource', '')
                                else:
                                    url_str = str(url_resource)
                                
                                if url_str and 'spotify.com/album/' in url_str:
                                    spotify_id = url_str.split('/')[-1].split('?')[0]
                                    spotify_album = self.mb._get_spotify_album_by_id(spotify_id)
                                    if spotify_album and spotify_album.get('genres'):
                                        genre_candidates.extend(spotify_album['genres'])
                                        logger.debug(f"Added {len(spotify_album['genres'])} genres from Spotify album")
                                    break
                    
                    # Add Spotify artist genres
                    if release_data.get('artist-credit') and release_data['artist-credit']:
                        artist = release_data['artist-credit'][0].get('artist', {})
                        artist_mbid = artist.get('id')
                        if artist_mbid:
                            artist_data = self.mb.get_artist(artist_mbid)
                            if artist_data and artist_data.get('relations'):
                                for relation in artist_data.get('relations', []):
                                    url_resource = relation.get('url', {})
                                    if isinstance(url_resource, dict):
                                        url_str = url_resource.get('resource', '')
                                    else:
                                        url_str = str(url_resource)
                                    
                                    if url_str and 'spotify' in url_str.lower() and '/artist/' in url_str:
                                        spotify_id = url_str.split('/')[-1].split('?')[0]
                                        spotify_artist = self.mb._get_spotify_artist_by_id(spotify_id)
                                        if spotify_artist and spotify_artist.get('genres'):
                                            genre_candidates.extend(spotify_artist['genres'])
                                            logger.debug(f"Added {len(spotify_artist['genres'])} genres from Spotify artist")
                                        break
                    
                    genre_options = build_multi_select_options(
                        genre_candidates,
                        limit=10,
                        context='album genres',
                    )
                    if genre_options:
                        properties[prop_key] = {'multi_select': genre_options}
            
            # Album Type (from release-group primary-type)
            if release_data.get('release-group') and release_data['release-group'].get('primary-type') and self.albums_properties.get('type'):
                album_type = release_data['release-group']['primary-type']
                prop_key = self._get_property_key(self.albums_properties['type'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': album_type}}
            
            # Spotify link (from url-rels) - check for both "streaming" and "free streaming"
            # Only write Spotify URL if it wasn't provided as input
            if not skip_spotify_url:
                spotify_url = None
                if release_data.get('relations') and self.albums_properties.get('listen'):
                    for relation in release_data.get('relations', []):
                        relation_type = relation.get('type', '').lower()
                        # Check for both "streaming" and "free streaming" relation types
                        if relation_type in ['streaming', 'free streaming']:
                            url_resource = relation.get('url', {})
                            if isinstance(url_resource, dict):
                                url_str = url_resource.get('resource', '')
                            else:
                                url_str = str(url_resource)
                            
                            # Check if it's a Spotify URL
                            if url_str and 'spotify' in url_str.lower() and 'spotify.com' in url_str.lower():
                                spotify_url = url_str
                                break
                
                # If no Spotify link found in MusicBrainz, try searching Spotify directly
                if not spotify_url and self.albums_properties.get('listen'):
                    album_title = release_data.get('title', '')
                    artist_name = None
                    # Get artist name from artist-credit
                    if release_data.get('artist-credit') and release_data['artist-credit']:
                        first_artist = release_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    if album_title:
                        spotify_url = self.mb._get_spotify_album_url(album_title, artist_name)
                        if spotify_url:
                            logger.debug(f"Found Spotify URL via API search: {spotify_url}")
                
                if spotify_url:
                    prop_key = self._get_property_key(self.albums_properties['listen'], 'albums')
                    if prop_key:
                        properties[prop_key] = {'url': spotify_url}
            
            # MusicBrainz URL
            if release_data.get('id') and self.albums_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/release/{release_data['id']}"
                prop_key = self._get_property_key(self.albums_properties['musicbrainz_url'], 'albums')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.albums_properties.get('last_updated'):
                prop_key = self._get_property_key(self.albums_properties['last_updated'], 'albums')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting album properties: {e}", exc_info=True)
        
        return properties
    
    def _find_existing_page_by_mbid(self, database_id: str, mbid: str, mbid_prop_key: str) -> Optional[str]:
        """Search for an existing page by MusicBrainz ID."""
        if not database_id or not mbid or not mbid_prop_key:
            return None
        
        try:
            filter_params = {
                'property': mbid_prop_key,
                'rich_text': {
                    'equals': mbid
                }
            }
            existing_pages = self.notion.query_database(database_id, filter_params)
            if existing_pages:
                return existing_pages[0]['id']
        except Exception as e:
            logger.debug(f"Error searching for page by MBID: {e}")
        
        return None
    
    def _find_existing_page_by_spotify_url(self, database_id: str, spotify_url: str, spotify_prop_key: str) -> Optional[str]:
        """Search for an existing page by Spotify URL."""
        if not database_id or not spotify_url or not spotify_prop_key:
            return None
        
        try:
            filter_params = {
                'property': spotify_prop_key,
                'url': {
                    'equals': spotify_url
                }
            }
            existing_pages = self.notion.query_database(database_id, filter_params)
            if existing_pages:
                return existing_pages[0]['id']
        except Exception as e:
            logger.debug(f"Error searching for page by Spotify URL: {e}")
        
        return None
    
    def _find_or_create_artist_page(self, artist_name: str, artist_mbid: Optional[str] = None, set_dns: bool = False) -> Optional[str]:
        """Find or create an artist page in the Artists database and return its page ID."""
        if not self.artists_db_id:
            return None
        
        try:
            normalized_mbid = self._normalize_mbid(artist_mbid)
            if normalized_mbid:
                cached_page_id = self._artist_mbid_map.get(normalized_mbid)
                if cached_page_id:
                    # Validate that the cached page's name matches the requested name
                    # This prevents linking to wrong artists when MusicBrainz returns bad data
                    try:
                        cached_page = self.notion.get_page(cached_page_id)
                        if cached_page:
                            title_prop_id = self.artists_properties.get('title')
                            title_key = self._get_property_key(title_prop_id, 'artists')
                            if title_key:
                                cached_page_title_prop = cached_page.get('properties', {}).get(title_key, {})
                                if cached_page_title_prop.get('title') and cached_page_title_prop['title']:
                                    cached_name = cached_page_title_prop['title'][0]['plain_text']
                                    # Check if names match (case-insensitive)
                                    if cached_name.lower() == artist_name.lower():
                                        return cached_page_id
                                    else:
                                        # Names don't match - MusicBrainz likely returned wrong artist for Spotify ID
                                        # Clear the bad MBID and search by name instead
                                        logger.warning(f"Cached artist MBID {normalized_mbid} has name '{cached_name}' but requested name is '{artist_name}'. Ignoring bad MBID and searching by name.")
                                        artist_mbid = None
                                        normalized_mbid = None
                    except Exception as e:
                        logger.warning(f"Error validating cached artist page: {e}. Proceeding with name search.")
            
            title_prop_id = self.artists_properties.get('title')
            mbid_prop_id = self.artists_properties.get('musicbrainz_id')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'artists')
            if not title_key:
                return None
            
            filter_params = {
                'property': title_key,
                'title': {'equals': artist_name},
            }
            existing_pages = self.notion.query_database(self.artists_db_id, filter_params)
            
            if existing_pages:
                page = existing_pages[0]
                page_id = page['id']
                
                if normalized_mbid:
                    self._persist_mbid_on_page(
                        'artists',
                        page,
                        page_id,
                        artist_mbid,
                        mbid_prop_id,
                        self._artist_mbid_map,
                    )
                else:
                    mbid_prop_key = self._get_property_key(mbid_prop_id, 'artists')
                    if mbid_prop_key:
                        existing_mbid = self._extract_rich_text_plain(page['properties'].get(mbid_prop_key))
                        self._register_mbid(self._artist_mbid_map, existing_mbid, page_id)
                return page_id
            
            # Case-insensitive fallback
            all_pages = self._get_database_pages(self.artists_db_id)
            
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == artist_name.lower():
                        page_id = page['id']
                        
                        if normalized_mbid:
                            self._persist_mbid_on_page(
                                'artists',
                                page,
                                page_id,
                                artist_mbid,
                                mbid_prop_id,
                                self._artist_mbid_map,
                            )
                        else:
                            mbid_prop_key = self._get_property_key(mbid_prop_id, 'artists')
                            if mbid_prop_key:
                                existing_mbid = self._extract_rich_text_plain(page_props.get(mbid_prop_key))
                                self._register_mbid(self._artist_mbid_map, existing_mbid, page_id)
                        return page_id
            
            # Artist doesn't exist - create it
            logger.info(f"Creating new artist page: {artist_name}")
            
            # Fetch full artist data before creating
            artist_data = self._fetch_artist_data_by_mbid_or_name(artist_name, artist_mbid)
            
            # Format ALL properties including DNS
            artist_props = {}
            if artist_data:
                artist_props = self._format_artist_properties(artist_data)
            else:
                # Minimal fallback if no MusicBrainz data found
                artist_props[title_key] = {'title': [{'text': {'content': artist_name}}]}
            
            # Set DNS checkbox if requested (Spotify URL flow sets this to prevent automation cascade)
            if set_dns:
                dns_key = self._get_property_key(self.artists_properties.get('dns'), 'artists')
                if dns_key:
                    artist_props[dns_key] = {'checkbox': True}
            
            # Create page with everything in one call
            artist_page_id = self.notion.create_page(
                self.artists_db_id,
                artist_props,
                None,
                '🎤',
            )
            
            if artist_page_id:
                logger.info(f"Created artist page: {artist_name} (ID: {artist_page_id})")
                # Register in cache
                if artist_data and artist_data.get('id'):
                    self._register_mbid(self._artist_mbid_map, artist_data['id'], artist_page_id)
            
            return artist_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating artist page for '{artist_name}': {e}")
            return None
    
    def _score_release_for_song(self, release: Dict) -> tuple:
        """Score a release for song-to-album matching.
        
        Returns a tuple (score, date) where:
        - Higher score = better match
        - Date is used for tie-breaking (earlier is better)
        
        Criteria (in priority order):
        1. US country (prefer US)
        2. Album type (prefer "Album" over other types)
        3. Earliest release date
        """
        score = 0
        release_date = None
        
        # Get release-group for album type
        release_group = release.get('release-group', {})
        release_group_type = release_group.get('type', '').lower() if release_group else ''
        
        # Get country - check release-events if not in release directly
        country = release.get('country') or ''
        country = country.upper() if country else ''
        if not country and release.get('release-events'):
            # Get country from first release event
            first_event = release['release-events'][0]
            country = first_event.get('area', {}).get('iso-3166-1-codes', [''])[0] if first_event.get('area') else ''
            if country:
                country = country.upper()
        
        # Get release date - check multiple sources
        date_str = release.get('date', '')
        
        # If no date in release, check release-events (first event date)
        if not date_str and release.get('release-events'):
            first_event = release['release-events'][0]
            date_str = first_event.get('date', '')
        
        # If still no date, check release-group first-release-date
        if not date_str:
            release_group = release.get('release-group', {})
            if release_group:
                date_str = release_group.get('first-release-date', '')
        
        if date_str:
            # Normalize date to YYYY-MM-DD for comparison
            # Prefer full dates over partial dates (year-only or year-month)
            try:
                parts = date_str.split('-')
                if len(parts) >= 1:
                    year = parts[0]
                    if len(parts) == 1:
                        # Just year - set to end of year so it sorts after all full dates in that year
                        month = '12'
                        day = '31'
                    elif len(parts) == 2:
                        # Year and month - set to end of month so it sorts after all full dates in that month
                        month = parts[1]
                        # Get last day of month (approximate - use 28 to be safe, or 31 for most months)
                        if month in ['01', '03', '05', '07', '08', '10', '12']:
                            day = '31'
                        elif month in ['04', '06', '09', '11']:
                            day = '30'
                        else:
                            day = '28'  # February
                    else:
                        # Full date - use as is
                        month = parts[1]
                        day = parts[2]
                    release_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except Exception:
                pass
        
        # Scoring:
        # 1. Country priority: US > XW > others
        if country == 'US':
            score += 200
        elif country == 'XW':
            score += 100
        
        # 2. Album type: Album = 50 points, others = 0
        if release_group_type == 'album':
            score += 50
        
        # Date will be used for sorting (earlier is better)
        # We'll return it as a string for comparison
        
        return (score, release_date or '9999-12-31')  # Use far future date if no date
    
    def _find_best_release_for_song(self, releases: List[Dict]) -> Optional[Dict]:
        """Find the best release for a song with explicit country priority.
        
        Priority:
        1. Official releases only (fallback to any release if none official)
        2. Country preference: US > XW > all others
        3. Earliest release date within the selected country bucket
        """
        if not releases:
            return None
        
        # Separate official releases from the rest so we only fall back if needed
        official_releases = [
            release for release in releases
            if (release.get('status') or '').lower() == 'official'
        ]
        candidate_releases = official_releases or releases
        
        if not candidate_releases:
            return None
        
        # Score and sort candidates so we can limit full release fetches
        scored_releases = []
        for release in candidate_releases:
            score, date = self._score_release_for_song(release)
            scored_releases.append((score, date, release))
        
        scored_releases.sort(key=lambda x: (-x[0], x[1]))
        
        top_candidates = min(10, len(scored_releases))
        top_releases = []
        
        for i in range(top_candidates):
            score, date, release = scored_releases[i]
            release_mbid = release.get('id')
            
            # Fetch full release data to ensure we have track + event info
            if release_mbid:
                full_release = self.mb.get_release(release_mbid)
                if full_release:
                    release = {**release, **full_release}
                    score, date = self._score_release_for_song(release)
            
            top_releases.append((score, date, release))
        
        top_releases.sort(key=lambda x: (-x[0], x[1]))
        
        return top_releases[0][2] if top_releases else None
    
    def _get_album_cover_url(self, album_data: Optional[Dict]) -> Optional[str]:
        """Get an album cover URL from Cover Art Archive or Spotify."""
        if not album_data:
            return None
        
        cover_url = None
        release_id = album_data.get('id')
        if release_id:
            cover_url = self.mb.get_cover_art_url(release_id)
        
        if cover_url:
            return cover_url
        
        album_title = album_data.get('title')
        artist_name = None
        if album_data.get('artist-credit'):
            first_artist = album_data['artist-credit'][0].get('artist', {})
            artist_name = first_artist.get('name')
        
        if album_title:
            cover_url = self.mb._get_spotify_album_image(album_title, artist_name)
        
        return cover_url
    
    def _find_or_create_album_page(self, album_title: str, album_mbid: Optional[str] = None, artist_name: Optional[str] = None, set_dns: bool = False) -> Optional[str]:
        """Find or create an album page in the Albums database and return its page ID."""
        if not self.albums_db_id:
            return None
        
        try:
            normalized_mbid = self._normalize_mbid(album_mbid)
            if normalized_mbid:
                cached_page_id = self._album_mbid_map.get(normalized_mbid)
                if cached_page_id:
                    # Validate that the cached page's title matches the requested title
                    try:
                        cached_page = self.notion.get_page(cached_page_id)
                        if cached_page:
                            title_prop_id = self.albums_properties.get('title')
                            title_key = self._get_property_key(title_prop_id, 'albums')
                            if title_key:
                                cached_page_title_prop = cached_page.get('properties', {}).get(title_key, {})
                                if cached_page_title_prop.get('title') and cached_page_title_prop['title']:
                                    cached_title = cached_page_title_prop['title'][0]['plain_text']
                                    # Check if titles match (case-insensitive)
                                    if cached_title.lower() == album_title.lower():
                                        return cached_page_id
                                    else:
                                        # Titles don't match - MusicBrainz likely returned wrong album
                                        logger.warning(f"Cached album MBID {normalized_mbid} has title '{cached_title}' but requested title is '{album_title}'. Ignoring bad MBID and searching by title.")
                                        album_mbid = None
                                        normalized_mbid = None
                    except Exception as e:
                        logger.warning(f"Error validating cached album page: {e}. Proceeding with title search.")
            
            # First, try to find existing album by title
            title_prop_id = self.albums_properties.get('title')
            mbid_prop_id = self.albums_properties.get('musicbrainz_id')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'albums')
            if not title_key:
                return None
            
            # Search for existing album page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': album_title
                }
            }
            
            existing_pages = self.notion.query_database(self.albums_db_id, filter_params)
            
            if existing_pages:
                page = existing_pages[0]
                album_page_id = page['id']
                
                if album_mbid:
                    album_data = self.mb.get_release(album_mbid)
                    if album_data:
                        # Format full properties and update the page with all metadata
                        full_props = self._format_album_properties(album_data)
                        cover_url = self._get_album_cover_url(album_data)
                        logger.info(f"Updating existing album page with full metadata: '{album_title}'")
                        self.notion.update_page(album_page_id, full_props, cover_url)
                if normalized_mbid:
                    self._persist_mbid_on_page(
                        'albums',
                        page,
                        album_page_id,
                        album_mbid,
                        mbid_prop_id,
                        self._album_mbid_map,
                    )
                return album_page_id
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self._get_database_pages(self.albums_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == album_title.lower():
                        page_id = page['id']
                        if normalized_mbid:
                            self._persist_mbid_on_page(
                                'albums',
                                page,
                                page_id,
                                album_mbid,
                                mbid_prop_id,
                                self._album_mbid_map,
                            )
                        else:
                            mbid_prop_key = self._get_property_key(mbid_prop_id, 'albums')
                            if mbid_prop_key:
                                existing_mbid = self._extract_rich_text_plain(page_props.get(mbid_prop_key))
                                self._register_mbid(self._album_mbid_map, existing_mbid, page_id)
                        return page['id']
            
            # Album doesn't exist - create it
            logger.info(f"Creating new album page: {album_title}")
            
            # Fetch full album data before creating
            album_data = self._fetch_album_data_by_mbid_or_name(album_title, album_mbid, artist_name)
            
            # Format ALL properties including DNS
            album_props = {}
            cover_url = None
            if album_data:
                album_props = self._format_album_properties(album_data, skip_spotify_url=False, set_dns_on_labels=set_dns)
                cover_url = self._get_album_cover_url(album_data)
            else:
                # Minimal fallback if no MusicBrainz data found
                album_props[title_key] = {'title': [{'text': {'content': album_title}}]}
            
            # Set DNS checkbox if requested (Spotify URL flow sets this to prevent automation cascade)
            if set_dns:
                dns_key = self._get_property_key(self.albums_properties.get('dns'), 'albums')
                if dns_key:
                    album_props[dns_key] = {'checkbox': True}
            
            # Create the album page with everything in one call
            album_page_id = self.notion.create_page(
                self.albums_db_id,
                album_props,
                cover_url,
                '💿'
            )
            
            if album_page_id:
                logger.info(f"Created album page: {album_title} (ID: {album_page_id})")
                # Register in cache
                if album_data and album_data.get('id'):
                    self._register_mbid(self._album_mbid_map, album_data['id'], album_page_id)
            
            return album_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating album page for '{album_title}': {e}")
            return None
    
    def _find_or_create_label_page(self, label_name: str, label_mbid: Optional[str] = None, set_dns: bool = False) -> Optional[str]:
        """Find or create a label page in the Labels database and return its page ID."""
        if not self.labels_db_id:
            return None
        
        try:
            normalized_mbid = self._normalize_mbid(label_mbid)
            if normalized_mbid:
                cached_page_id = self._label_mbid_map.get(normalized_mbid)
                if cached_page_id:
                    # Validate that the cached page's name matches the requested name
                    # This prevents linking to wrong labels when MusicBrainz returns bad data
                    try:
                        cached_page = self.notion.get_page(cached_page_id)
                        if cached_page:
                            title_prop_id = self.labels_properties.get('title')
                            title_key = self._get_property_key(title_prop_id, 'labels')
                            if title_key:
                                cached_page_title_prop = cached_page.get('properties', {}).get(title_key, {})
                                if cached_page_title_prop.get('title') and cached_page_title_prop['title']:
                                    cached_name = cached_page_title_prop['title'][0]['plain_text']
                                    # Check if names match (case-insensitive)
                                    if cached_name.lower() == label_name.lower():
                                        return cached_page_id
                                    else:
                                        # Names don't match - MusicBrainz likely returned wrong label for ID
                                        # Clear the bad MBID and search by name instead
                                        logger.warning(f"Cached label MBID {normalized_mbid} has name '{cached_name}' but requested name is '{label_name}'. Ignoring bad MBID and searching by name.")
                                        label_mbid = None
                                        normalized_mbid = None
                    except Exception as e:
                        logger.warning(f"Error validating cached label page: {e}. Proceeding with name search.")
            
            # First, try to find existing label by name
            title_prop_id = self.labels_properties.get('title')
            mbid_prop_id = self.labels_properties.get('musicbrainz_id')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'labels')
            if not title_key:
                return None
            
            # Search for existing label page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': label_name
                }
            }
            
            existing_pages = self.notion.query_database(self.labels_db_id, filter_params)
            
            if existing_pages:
                page = existing_pages[0]
                page_id = page['id']
                if normalized_mbid:
                    self._persist_mbid_on_page(
                        'labels',
                        page,
                        page_id,
                        label_mbid,
                        mbid_prop_id,
                        self._label_mbid_map,
                    )
                else:
                    mbid_prop_key = self._get_property_key(mbid_prop_id, 'labels')
                    if mbid_prop_key:
                        existing_mbid = self._extract_rich_text_plain(page['properties'].get(mbid_prop_key))
                        self._register_mbid(self._label_mbid_map, existing_mbid, page_id)
                return page_id
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self._get_database_pages(self.labels_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == label_name.lower():
                        page_id = page['id']
                        if normalized_mbid:
                            self._persist_mbid_on_page(
                                'labels',
                                page,
                                page_id,
                                label_mbid,
                                mbid_prop_id,
                                self._label_mbid_map,
                            )
                        else:
                            mbid_prop_key = self._get_property_key(mbid_prop_id, 'labels')
                            if mbid_prop_key:
                                existing_mbid = self._extract_rich_text_plain(page_props.get(mbid_prop_key))
                                self._register_mbid(self._label_mbid_map, existing_mbid, page_id)
                        return page['id']
            
            # Label doesn't exist - create it
            logger.info(f"Creating new label page: {label_name}")
            
            # Fetch full label data before creating
            label_data = self._fetch_label_data_by_mbid_or_name(label_name, label_mbid)
            
            # Format ALL properties including DNS
            label_props = {}
            if label_data:
                label_props = self._format_label_properties(label_data)
            else:
                # Minimal fallback if no MusicBrainz data found
                label_props[title_key] = {'title': [{'text': {'content': label_name}}]}
            
            # Set DNS checkbox if requested (Spotify URL flow sets this to prevent automation cascade)
            if set_dns:
                dns_key = self._get_property_key(self.labels_properties.get('dns'), 'labels')
                if dns_key:
                    label_props[dns_key] = {'checkbox': True}
            
            # Create the label page with everything in one call
            label_page_id = self.notion.create_page(
                self.labels_db_id,
                label_props,
                None,
                '🏷️'
            )
            
            if label_page_id:
                logger.info(f"Created label page: {label_name} (ID: {label_page_id})")
                # Register in cache
                if label_data and label_data.get('id'):
                    self._register_mbid(self._label_mbid_map, label_data['id'], label_page_id)
            
            return label_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating label page for '{label_name}': {e}")
            return None
    
    def _load_locations_cache(self):
        """Load all locations into cache to avoid repeated database queries."""
        if not self.locations_db_id or self._location_cache is not None:
            return
        
        try:
            # Query all location pages once
            all_pages = self.notion.query_database(self.locations_db_id)
            
            # Find the title property key
            if all_pages:
                first_page_props = all_pages[0].get('properties', {})
                for prop_key, prop_data in first_page_props.items():
                    if prop_data.get('type') == 'title':
                        self._locations_title_key = prop_key
                        break
            
            if not self._locations_title_key:
                logger.warning("Could not find title property in Locations database")
                self._location_cache = {}  # Mark as loaded (empty)
                return
            
            # Build cache: location name (lowercase) -> page_id
            self._location_cache = {}
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(self._locations_title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    self._location_cache[page_title.lower()] = page['id']
            
            logger.debug(f"Loaded {len(self._location_cache)} locations into cache")
            
        except Exception as e:
            logger.error(f"Error loading locations cache: {e}")
            self._location_cache = {}  # Mark as loaded (empty)
    
    def _find_or_create_location_page(self, location_name: str) -> Optional[str]:
        """Find or create a location page in the Locations database and return its page ID."""
        if not self.locations_db_id:
            return None
        
        try:
            # Load cache if not already loaded
            if self._location_cache is None:
                self._load_locations_cache()
            
            # Check cache first
            location_name_lower = location_name.lower()
            if location_name_lower in self._location_cache:
                return self._location_cache[location_name_lower]
            
            # Location doesn't exist - create it
            if not self._locations_title_key:
                # Need to get title key if we don't have it
                all_pages = self.notion.query_database(self.locations_db_id)
                if all_pages:
                    first_page_props = all_pages[0].get('properties', {})
                    for prop_key, prop_data in first_page_props.items():
                        if prop_data.get('type') == 'title':
                            self._locations_title_key = prop_key
                            break
            
            if not self._locations_title_key:
                logger.warning("Could not find title property in Locations database")
                return None
            
            logger.info(f"Creating new location page: {location_name}")
            
            # Format properties for new location
            location_props = {}
            location_props[self._locations_title_key] = {
                'title': [{'text': {'content': location_name}}]
            }
            
            # Create the location page
            location_page_id = self.notion.create_page(
                self.locations_db_id,
                location_props,
                None,
                '📍'  # Location pin emoji
            )
            
            if location_page_id:
                logger.info(f"Created location page: {location_name} (ID: {location_page_id})")
                # Add to cache
                self._location_cache[location_name_lower] = location_page_id
            
            return location_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating location page for '{location_name}': {e}")
            return None
    
    def _find_or_create_song_page(self, song_title: str, song_mbid: Optional[str] = None, 
                                   album_page_id: Optional[str] = None, 
                                   artist_page_id: Optional[str] = None,
                                   set_dns: bool = False) -> Optional[str]:
        """Find or create a song page in the Songs database and return its page ID."""
        if not self.songs_db_id:
            return None
        
        try:
            normalized_mbid = self._normalize_mbid(song_mbid)
            if normalized_mbid:
                cached_page = self._song_mbid_map.get(normalized_mbid)
                if cached_page:
                    return cached_page
            
            # First, try to find existing song by title
            title_prop_id = self.songs_properties.get('title')
            mbid_prop_id = self.songs_properties.get('musicbrainz_id')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'songs')
            if not title_key:
                return None
            
            # Search for existing song page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': song_title
                }
            }
            
            existing_pages = self.notion.query_database(self.songs_db_id, filter_params)
            
            if existing_pages:
                page = existing_pages[0]
                song_page_id = page['id']
                if normalized_mbid:
                    self._persist_mbid_on_page(
                        'songs',
                        page,
                        song_page_id,
                        song_mbid,
                        mbid_prop_id,
                        self._song_mbid_map,
                    )
                return song_page_id
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self._get_database_pages(self.songs_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == song_title.lower():
                        page_id = page['id']
                        if normalized_mbid:
                            self._persist_mbid_on_page(
                                'songs',
                                page,
                                page_id,
                                song_mbid,
                                mbid_prop_id,
                                self._song_mbid_map,
                            )
                        else:
                            mbid_prop_key = self._get_property_key(mbid_prop_id, 'songs')
                            if mbid_prop_key:
                                existing_mbid = self._extract_rich_text_plain(page_props.get(mbid_prop_key))
                                self._register_mbid(self._song_mbid_map, existing_mbid, page_id)
                        return page['id']
            
            # Song doesn't exist - create it
            logger.info(f"Creating new song page: {song_title}")
            
            # Get song data from MusicBrainz if we have MBID
            song_data = None
            if song_mbid:
                song_data = self.mb.get_recording(song_mbid)
            
            # Format properties for new song
            song_props = {}
            song_props[title_key] = {
                'title': [{'text': {'content': song_title}}]
            }
            
            mbid_to_store = None
            if song_data and song_data.get('id'):
                mbid_to_store = song_data['id']
            elif song_mbid:
                mbid_to_store = song_mbid
            
            if mbid_to_store and self.songs_properties.get('musicbrainz_id'):
                mb_id_key = self._get_property_key(self.songs_properties['musicbrainz_id'], 'songs')
                if mb_id_key:
                    song_props[mb_id_key] = {
                        'rich_text': [{'text': {'content': mbid_to_store}}]
                    }
            
            # Add relations if provided
            if album_page_id and self.songs_properties.get('album'):
                album_key = self._get_property_key(self.songs_properties['album'], 'songs')
                if album_key:
                    song_props[album_key] = {
                        'relation': [{'id': album_page_id}]
                    }
            
            if artist_page_id and self.songs_properties.get('artist'):
                artist_key = self._get_property_key(self.songs_properties['artist'], 'songs')
                if artist_key:
                    song_props[artist_key] = {
                        'relation': [{'id': artist_page_id}]
                    }
            
            # Set DNS checkbox if requested (Spotify URL flow sets this to prevent automation cascade)
            if set_dns:
                dns_key = self._get_property_key(self.songs_properties.get('dns'), 'songs')
                if dns_key:
                    song_props[dns_key] = {'checkbox': True}
            
            # Create the song page
            song_page_id = self.notion.create_page(
                self.songs_db_id,
                song_props,
                None,
                '🎵'  # Music note emoji
            )
            
            if song_page_id:
                logger.info(f"Created song page: {song_title} (ID: {song_page_id})")
                # If we have full song data, update the page with it
                if song_data:
                    full_props = self._format_song_properties(song_data)
                    self.notion.update_page(song_page_id, full_props)
                self._register_mbid(self._song_mbid_map, mbid_to_store, song_page_id)
            
            return song_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating song page for '{song_title}': {e}")
            return None
    
    def sync_song_page(self, page: Dict, force_update: bool = False, spotify_url: str = None) -> Optional[bool]:
        """Sync a single song page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.songs_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Songs database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'songs')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing song: {title}")
            
            # Try to extract artist name and MBID from relation
            artist_name = None
            artist_mbid = None
            artist_prop_id = self.songs_properties.get('artist')
            if artist_prop_id:
                artist_key = self._get_property_key(artist_prop_id, 'songs')
                if artist_key:
                    artist_prop = properties.get(artist_key, {})
                    if artist_prop.get('relation') and len(artist_prop['relation']) > 0:
                        # Fetch the artist page to get the name and MBID
                        artist_page_id = artist_prop['relation'][0]['id']
                        if artist_page_id:
                            artist_page = self.notion.get_page(artist_page_id)
                            if artist_page:
                                artist_props = artist_page.get('properties', {})
                                artist_title_key = self._get_property_key(self.artists_properties.get('title'), 'artists')
                                if artist_title_key and artist_props.get(artist_title_key):
                                    artist_title_prop = artist_props[artist_title_key]
                                    if artist_title_prop.get('title') and artist_title_prop['title']:
                                        artist_name = artist_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found artist from relation: {artist_name}")
                                
                                # Get artist MBID for more accurate searching
                                artist_mbid = self._get_mbid_from_related_page(artist_page_id, 'artists')
                                if artist_mbid:
                                    logger.debug(f"Found artist MBID from relation: {artist_mbid}")
            
            # Try to extract album name and MBID from relation
            album_name = None
            album_mbid = None
            album_prop_id = self.songs_properties.get('album')
            if album_prop_id:
                album_key = self._get_property_key(album_prop_id, 'songs')
                if album_key:
                    album_prop = properties.get(album_key, {})
                    if album_prop.get('relation') and len(album_prop['relation']) > 0:
                        # Fetch the album page to get the name and MBID
                        album_page_id = album_prop['relation'][0]['id']
                        if album_page_id:
                            album_page = self.notion.get_page(album_page_id)
                            if album_page:
                                album_props = album_page.get('properties', {})
                                album_title_key = self._get_property_key(self.albums_properties.get('title'), 'albums')
                                if album_title_key and album_props.get(album_title_key):
                                    album_title_prop = album_props[album_title_key]
                                    if album_title_prop.get('title') and album_title_prop['title']:
                                        album_name = album_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found album from relation: {album_name}")
                                
                                # Get album MBID for verification
                                album_mbid = self._get_mbid_from_related_page(album_page_id, 'albums')
                                if album_mbid:
                                    logger.debug(f"Found album MBID from relation: {album_mbid}")
            
            # Check for existing MBID
            mb_id_prop_id = self.songs_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'songs')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Check for Spotify URL (dual-purpose: input and output)
            # Priority: CLI parameter > Notion property
            spotify_url_from_notion = None
            spotify_prop_id = self.songs_properties.get('listen')  # Spotify property
            if spotify_prop_id:
                spotify_key = self._get_property_key(spotify_prop_id, 'songs')
                if spotify_key:
                    spotify_prop = properties.get(spotify_key, {})
                    if spotify_prop.get('url'):
                        spotify_url_from_notion = spotify_prop['url']
            
            # Determine which Spotify URL to use (parameter takes priority)
            active_spotify_url = spotify_url or spotify_url_from_notion
            spotify_provided_via_input = bool(active_spotify_url)
            
            # Search or get recording data
            recording_data = None
            matched_release = None
            matched_track = None
            release_group_used = False
            search_limit = 50
            spotify_track_data = None
            
            # Try Spotify URL approach if provided
            if active_spotify_url and not existing_mbid:
                logger.info(f"Spotify URL provided: {active_spotify_url}")
                parsed = self.mb._parse_spotify_url(active_spotify_url)
                
                if parsed and parsed['type'] == 'track':
                    spotify_track_data = self.mb._get_spotify_track_by_id(parsed['id'])
                    
                    if spotify_track_data:
                        # Extract ISRC from Spotify data
                        external_ids = self.mb._extract_external_ids(spotify_track_data)
                        isrc = external_ids.get('isrc')
                        
                        if isrc:
                            logger.info(f"Found ISRC from Spotify: {isrc}")
                            # Search MusicBrainz by ISRC
                            recording_data = self.mb.search_recording_by_isrc(isrc)
                            
                            if recording_data:
                                logger.info(f"Successfully matched recording via Spotify ISRC: {isrc}")
                            else:
                                logger.info(f"No MusicBrainz match for ISRC {isrc}, falling back to name search")
                        else:
                            logger.info("Spotify track has no ISRC, falling back to name search")
                    else:
                        logger.warning(f"Could not fetch Spotify track data, falling back to name search")
                elif parsed:
                    logger.warning(f"Spotify URL is not a track (type: {parsed['type']}), ignoring")
            
            if existing_mbid:
                recording_data = self.mb.get_recording(existing_mbid)
                if not recording_data:
                    logger.warning(f"Could not find recording with MBID {existing_mbid}, searching by title")
                    existing_mbid = None
                elif not force_update:
                    # Skip pages with existing MBIDs unless force_update is True
                    logger.info(f"Skipping song '{title}' - already has MBID {existing_mbid} (use --force-update to update)")
                    return None
            
            if not recording_data:
                # Hot path: walking release-groups is more expensive than recording search but
                # dramatically more accurate, so we do it once before falling back to the
                # brute-force recording search below.
                # Try album-first approach using release-groups before falling back to recording search
                if artist_mbid:
                    logger.info("Attempting release-group driven search for '%s'", title)
                    release_match = self._find_release_via_release_groups(
                        title,
                        artist_mbid,
                        preferred_album_mbid=album_mbid,
                        preferred_album_title=album_name
                    )
                    if release_match:
                        recording_data = release_match.get('recording_data')
                        if recording_data:
                            matched_release = release_match.get('release')
                            matched_track = release_match.get('track')
                            release_group_used = True
                            logger.info("Release-group search succeeded for '%s'", title)
                        else:
                            logger.info("Release-group search found release but missing recording data for '%s'", title)
                
                if not recording_data:
                    # The fallback recording search can fan out into dozens of MusicBrainz calls,
                    # so try to keep the result set tight and reuse cached payloads wherever possible.
                    # Try search with artist MBID first (most accurate), then artist name, then without artist
                    search_results = None
                    search_limit = 20 if release_group_used else 50
                    if artist_mbid:
                        logger.info(f"Searching with artist MBID: {artist_mbid}")
                    # First try with title in query
                    search_results = self.mb.search_recordings(title, None, album_name, artist_mbid=artist_mbid, limit=search_limit)
                    logger.info(f"Search with title returned {len(search_results)} results")
                    
                    # If we didn't find exact matches, try searching all recordings by this artist
                    # and filter by title in code (more reliable)
                    if not search_results or not any(self._titles_match_exactly(title, r.get('title', '')) for r in search_results):
                        logger.info(f"No exact title matches found, searching all recordings by artist {artist_mbid}")
                        all_artist_recordings = self.mb.get_artist_recordings(artist_mbid, limit=120)
                        logger.info(f"Found {len(all_artist_recordings)} total recordings by artist")
                        
                        # Filter by exact title match (including aliases)
                        # First check titles, then check aliases for all recordings
                        title_matches = [r for r in all_artist_recordings if self._titles_match_exactly(title, r.get('title', ''))]
                        logger.info(f"Found {len(title_matches)} recordings with exact title match")
                        
                        # Also check aliases for all recordings (not just when no title matches)
                        # This is important for songs like "New Genesis" which has Japanese primary title
                        # First check if aliases are already in search results
                        sample_recording = all_artist_recordings[0] if all_artist_recordings else None
                        has_aliases_in_results = sample_recording and 'aliases' in sample_recording
                        
                        if has_aliases_in_results:
                            logger.info("Aliases already in search results, checking them...")
                            for recording in all_artist_recordings:
                                # Skip if already matched by title
                                if any(m.get('id') == recording.get('id') for m in title_matches):
                                    continue
                                
                                if self._recording_title_matches(recording, title):
                                    title_matches.append(recording)
                                    logger.info(f"Found match via alias: {recording.get('title')} (aliases: {[a.get('name') for a in recording.get('aliases', [])]})")
                        else:
                            logger.info("Aliases not in search results, fetching full data for recordings...")
                            for recording in all_artist_recordings:
                                # Skip if already matched by title
                                if any(m.get('id') == recording.get('id') for m in title_matches):
                                    continue
                                
                                recording_id = recording.get('id')
                                if recording_id:
                                    # Fetch full recording data to check aliases (uses cache if available)
                                    full_recording = self.mb.get_recording(recording_id)
                                    if full_recording and self._recording_title_matches(full_recording, title):
                                        title_matches.append(full_recording)
                                        logger.info(f"Found match via alias: {full_recording.get('title')} (aliases: {[a.get('name') for a in full_recording.get('aliases', [])]})")
                        
                        search_results = title_matches
                        logger.info(f"Filtered to {len(search_results)} recordings with exact title/alias match")
                elif artist_name:
                    logger.debug(f"Searching with artist name: {artist_name}")
                    search_results = self.mb.search_recordings(title, artist_name, album_name, limit=search_limit)
                
                if not search_results:
                    # If search with artist failed completely, try without artist
                    logger.debug(f"No results with artist filter, trying without artist filter")
                    search_results = self.mb.search_recordings(title, None, album_name, limit=search_limit)
                
                if not search_results:
                    logger.warning(f"Could not find song: {title}")
                    return False
                
                logger.info(f"Search returned {len(search_results)} results for '{title}'")
                if artist_mbid:
                    logger.info(f"Looking for recordings by artist MBID: {artist_mbid}")
                
                # Find exact match (word-for-word, case-insensitive, ignoring special characters)
                # Check ALL results, not just the first one, since search results may be ordered incorrectly
                # Also verify it appears on the related album if album_mbid is provided (preferred but not required)
                # And verify it's by the correct artist if artist_mbid is provided (required)
                best_match = None
                best_match_on_album = None  # Preferred: match that appears on the album
                exact_matches = []
                candidate_matches = []
                artist_check_cache: Dict[str, bool] = {}
                artist_mismatch_budget = 20
                for result in search_results:
                    result_title = result.get('title', '')
                    # Check if title matches (including aliases)
                    # First try with available data, then fetch full data if needed for alias check
                    title_matches = False
                    if self._titles_match_exactly(title, result_title):
                        title_matches = True
                    elif not result.get('aliases'):
                        # If no aliases in search result, fetch full data to check aliases
                        recording_id = result.get('id')
                        if recording_id:
                            full_recording = self.mb.get_recording(recording_id)
                            if full_recording and self._recording_title_matches(full_recording, title):
                                title_matches = True
                                # Update result with full data for later use
                                result = full_recording
                    else:
                        # Aliases are present, use the full check
                        title_matches = self._recording_title_matches(result, title)
                    
                    if not title_matches:
                        continue
                    
                    exact_matches.append(result_title)
                    
                    # If we have an artist MBID, verify the recording is by that artist (required)
                    if artist_mbid:
                        recording_id = result.get('id')
                        cached_match = artist_check_cache.get(recording_id) if recording_id else None
                        matches_artist = cached_match if cached_match is not None else self._recording_is_by_artist(result, artist_mbid)
                        if recording_id:
                            artist_check_cache[recording_id] = matches_artist
                        if not matches_artist:
                            logger.info(f"Recording '{result_title}' is not by artist {artist_mbid}, skipping")
                            artist_mismatch_budget -= 1
                            if artist_mismatch_budget <= 0:
                                logger.info("Artist mismatch budget exhausted; stopping search iteration early")
                                break
                            continue
                    
                    # If we have an album MBID, check if the recording appears on that album (preferred)
                    on_album = False
                    if album_mbid:
                        recording_id = result.get('id')
                        if recording_id:
                            on_album = self._recording_appears_on_album(recording_id, album_mbid)
                            if on_album:
                                best_match_on_album = result
                                logger.debug(f"Found exact match on album: '{result_title}' for '{title}'")
                            else:
                                logger.debug(f"Recording '{result_title}' does not appear on album {album_mbid}, but keeping as candidate")
                    
                    release_rank = self._recording_release_rank(result, album_mbid, artist_mbid)
                    candidate_matches.append((release_rank, len(candidate_matches), result))
                    
                    # If we found a match on the album, prefer that and break
                    if best_match_on_album:
                        best_match = best_match_on_album
                        break
                
                # If no exact match found and we searched with artist, try without artist filter
                if not best_match and (artist_mbid or artist_name) and search_results:
                    logger.debug(f"No exact match found with artist filter, trying search without artist filter")
                    search_results_no_artist = self.mb.search_recordings(title, None, album_name, limit=search_limit)
                    if search_results_no_artist:
                        logger.debug(f"Search without artist returned {len(search_results_no_artist)} results")
                        best_match_on_album = None
                        candidate_matches_no_artist = []
                        for result in search_results_no_artist:
                            result_title = result.get('title', '')
                            # Check if title matches (including aliases)
                            title_matches = False
                            if self._titles_match_exactly(title, result_title):
                                title_matches = True
                            elif not result.get('aliases'):
                                # If no aliases in search result, fetch full data to check aliases
                                recording_id = result.get('id')
                                if recording_id:
                                    full_recording = self.mb.get_recording(recording_id)
                                    if full_recording and self._recording_title_matches(full_recording, title):
                                        title_matches = True
                                        result = full_recording
                            else:
                                # Aliases are present, use the full check
                                title_matches = self._recording_title_matches(result, title)
                            
                            if not title_matches:
                                continue
                            
                            exact_matches.append(result_title)
                            
                            # If we have an artist MBID, verify the recording is by that artist (required)
                            if artist_mbid:
                                recording_id = result.get('id')
                                cached_match = artist_check_cache.get(recording_id) if recording_id else None
                                matches_artist = cached_match if cached_match is not None else self._recording_is_by_artist(result, artist_mbid)
                                if recording_id:
                                    artist_check_cache[recording_id] = matches_artist
                                if not matches_artist:
                                    logger.info(f"Recording '{result_title}' is not by artist {artist_mbid}, skipping")
                                    continue
                            
                            # If we have an album MBID, check if the recording appears on that album (preferred)
                            on_album = False
                            if album_mbid:
                                recording_id = result.get('id')
                                if recording_id:
                                    on_album = self._recording_appears_on_album(recording_id, album_mbid)
                                    if on_album:
                                        best_match_on_album = result
                                        logger.debug(f"Found exact match on album: '{result_title}' for '{title}'")
                            
                            release_rank = self._recording_release_rank(result, album_mbid, artist_mbid)
                            candidate_matches_no_artist.append((release_rank, len(candidate_matches_no_artist), result))
                            
                            # If we found a match on the album, prefer that and break
                            if best_match_on_album:
                                best_match = best_match_on_album
                                break
                
                if not best_match and best_match_on_album:
                    best_match = best_match_on_album
                
                if not best_match and candidate_matches:
                    candidate_matches.sort(key=lambda x: (-x[0], x[1]))
                    best_match = candidate_matches[0][2]
                
                # If still no exact match found, warn and skip
                if not best_match:
                    # Log all result titles for debugging
                    result_titles = [r.get('title', 'Unknown') for r in search_results[:10]]
                    if exact_matches:
                        if album_mbid:
                            logger.warning(f"No exact match found for '{title}' by correct artist. Found {len(exact_matches)} exact title matches: {exact_matches[:5]}. Top search results: {result_titles}")
                        else:
                            logger.warning(f"No exact match found for '{title}' by correct artist. Found {len(exact_matches)} exact title matches: {exact_matches[:5]}. Top search results: {result_titles}")
                    else:
                        logger.warning(f"No exact match found for '{title}' after checking {len(search_results)} results. Top results: {result_titles}. Skipping to avoid incorrect match.")
                    return False
                
                recording_data = self.mb.get_recording(best_match['id'])
            
            if not recording_data:
                logger.warning(f"Could not get song data for: {title}")
                return False
            
            # Format properties
            # Skip writing Spotify URL if it was provided as input (preserve user's input)
            notion_props = self._format_song_properties(
                recording_data,
                matched_release,
                matched_track,
                skip_spotify_url=spotify_provided_via_input
            )
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'songs')
            
            # Set icon
            icon = '🎵'  # Musical note emoji for songs
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, None, icon):
                logger.info(f"Successfully updated song: {title}")
                return True
            else:
                logger.error(f"Failed to update song: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing song page {page.get('id')}: {e}")
            return False
    
    def _format_song_properties(
        self,
        recording_data: Dict,
        preferred_release: Optional[Dict] = None,
        preferred_track: Optional[Dict] = None,
        skip_spotify_url: bool = False,
        spotify_context: Optional[Dict] = None
    ) -> Dict:
        """Format MusicBrainz recording data for Notion properties.
        
        Args:
            recording_data: Recording metadata from MusicBrainz.
            preferred_release: Release data selected via release-group search (if any).
            preferred_track: Track entry from the preferred release (if any).
            skip_spotify_url: If True, skip writing Spotify URL (because it was user-provided).
            spotify_context: Optional dict with 'album' and 'artist' Spotify data to avoid redundant API calls.
        """
        properties = {}
        
        try:
            # Title
            if recording_data.get('title') and self.songs_properties.get('title'):
                prop_key = self._get_property_key(self.songs_properties['title'], 'songs')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': recording_data['title']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if recording_data.get('id') and self.songs_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.songs_properties['musicbrainz_id'], 'songs')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': recording_data['id']}}]
                    }
            
            # Artists (as relations)
            if recording_data.get('artist-credit') and self.songs_properties.get('artist') and self.artists_db_id:
                # Extract artist names and MBIDs from artist-credit
                artist_names = []
                artist_mbids = []
                
                for ac in recording_data.get('artist-credit', []):
                    if ac.get('artist'):
                        artist = ac['artist']
                        artist_name = artist.get('name')
                        artist_mbid = artist.get('id')
                        if artist_name:
                            artist_names.append(artist_name)
                            if artist_mbid:
                                artist_mbids.append(artist_mbid)
                            else:
                                artist_mbids.append(None)
                
                if artist_mbids:
                    primary_artist_mbid = artist_mbids[0]
                
                if artist_names:
                    # Find or create artist pages and get their IDs
                    artist_page_ids = []
                    for i, artist_name in enumerate(artist_names[:5]):  # Limit to 5 artists
                        artist_mbid = artist_mbids[i] if i < len(artist_mbids) else None
                        artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid)
                        if artist_page_id:
                            artist_page_ids.append(artist_page_id)
                    
                    if artist_page_ids:
                        prop_key = self._get_property_key(self.songs_properties['artist'], 'songs')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in artist_page_ids]
                            }
            
            # Album (as relation) - get best release based on criteria
            best_release = preferred_release  # Initialize for use in genres extraction
            if recording_data.get('id') and self.songs_properties.get('album') and self.albums_db_id:
                # Get releases from recording data
                releases = recording_data.get('releases', [])
                
                # If we have an artist MBID, get the artist's release-groups (albums only) and filter releases
                # to only those that belong to those release-groups
                artist_release_group_ids = set()
                if primary_artist_mbid:
                    logger.info(f"Getting artist's release-groups to filter releases")
                    artist_data = self.mb.get_artist(primary_artist_mbid)
                    if artist_data and artist_data.get('release-groups'):
                        # Get only "Album" type release-group IDs for this artist (exclude singles, EPs, etc.)
                        for rg in artist_data['release-groups']:
                            if not rg or not isinstance(rg, dict):
                                logger.debug("Skipping invalid release-group entry on artist payload")
                                continue
                            rg_type = (rg.get('primary-type') or '').lower()
                            # Only include Album type release-groups
                            if rg_type == 'album':
                                rg_id = rg.get('id')
                                if rg_id:
                                    artist_release_group_ids.add(rg_id)
                                    logger.debug(f"Including release-group: {rg.get('title')} (ID: {rg_id}, type: {rg_type})")
                            else:
                                logger.debug(f"Excluding release-group: {rg.get('title')} (type: {rg_type})")
                        logger.info(f"Found {len(artist_release_group_ids)} Album release-groups for artist {primary_artist_mbid}")
                
                # If we have few releases or they don't have complete data, search for more releases
                # by searching for releases containing this recording
                recording_id = recording_data.get('id')
                if recording_id:
                    # Search for all releases containing this recording (more reliable than title search)
                    releases_by_recording = self.mb.search_releases_by_recording(recording_id, limit=100)
                    if releases_by_recording:
                        # Merge with existing releases (avoid duplicates)
                        existing_ids = {r.get('id') for r in releases if r.get('id')}
                        for result in releases_by_recording:
                            if result.get('id') and result['id'] not in existing_ids:
                                releases.append(result)
                
                # If still no releases or incomplete data, try searching by title as fallback
                if not releases or not any(r.get('country') or r.get('release-group') for r in releases):
                    # Get artist name for search
                    artist_name = None
                    if recording_data.get('artist-credit') and recording_data['artist-credit']:
                        first_artist = recording_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    # Search for releases with the same title as the song
                    song_title = recording_data.get('title')
                    if song_title and artist_name:
                        search_results = self.mb.search_releases(song_title, artist_name, limit=50)
                        if search_results:
                            # Merge with existing releases (avoid duplicates)
                            existing_ids = {r.get('id') for r in releases if r.get('id')}
                            for result in search_results:
                                if result.get('id') and result['id'] not in existing_ids:
                                    releases.append(result)
                
                # Filter releases to only those from the artist's release-groups (albums)
                if releases and artist_release_group_ids:
                    filtered_releases = []
                    for release in releases:
                        release_group = release.get('release-group', {})
                        if not isinstance(release_group, dict):
                            logger.debug(f"Release '{release.get('title')}' has invalid release-group data")
                            continue
                        
                        release_group_id = release_group.get('id')
                        release_group_title = release_group.get('title', 'Unknown')
                        
                        if release_group_id and release_group_id in artist_release_group_ids:
                            filtered_releases.append(release)
                            logger.debug(f"Including release '{release.get('title')}' from release-group '{release_group_title}' (ID: {release_group_id})")
                        else:
                            logger.debug(f"Filtering out release '{release.get('title')}' - release-group ID {release_group_id} not in artist's albums")
                    
                    if filtered_releases:
                        logger.info(f"Filtered to {len(filtered_releases)} releases from artist's albums (out of {len(releases)} total)")
                        releases = filtered_releases
                    else:
                        logger.warning(f"No releases found from artist's albums. Artist has {len(artist_release_group_ids)} albums, but none of the {len(releases)} releases match. Using all releases.")
                        # Log the release-group IDs we're looking for vs what we found
                        found_rg_ids = {r.get('release-group', {}).get('id') for r in releases if isinstance(r.get('release-group'), dict)}
                        logger.warning(f"Looking for release-groups: {list(artist_release_group_ids)[:5]}")
                        logger.warning(f"Found release-groups: {list(found_rg_ids)[:5]}")
                
                if not best_release and releases:
                    # Find the best release based on criteria:
                    # - US country (prefer US)
                    # - Album type (prefer "Album" over "Album + Compilation" etc.)
                    # - Earliest release date
                    try:
                        best_release = self._find_best_release_for_song(releases)
                    except Exception as e:
                        logger.warning(f"Error finding best release for song: {e}")
                        best_release = None
                    
                if best_release:
                    release_title = best_release.get('title')
                    release_mbid = best_release.get('id')
                    
                    if release_title:
                        # Find or create album page
                        album_page_id = self._find_or_create_album_page(release_title, release_mbid)
                        if album_page_id:
                            prop_key = self._get_property_key(self.songs_properties['album'], 'songs')
                            if prop_key:
                                properties[prop_key] = {
                                    'relation': [{'id': album_page_id}]
                                }
                    
                    # Extract track number and disc number from preferred track or release media
                    track_number = None
                    disc_number = None
                    if preferred_track:
                        track_number = preferred_track.get('position')
                    
                    if track_number is None and recording_data.get('id') and best_release.get('media'):
                        recording_id = recording_data.get('id')
                        for medium in best_release.get('media', []):
                            for track in medium.get('tracks', []):
                                if track.get('recording') and track['recording'].get('id') == recording_id:
                                    track_number = track.get('position')
                                    disc_number = medium.get('position')  # Get disc position
                                    if track_number:
                                        break
                            if track_number:
                                break
                    
                    if track_number and self.songs_properties.get('track_number'):
                        prop_key = self._get_property_key(self.songs_properties['track_number'], 'songs')
                        if prop_key:
                            properties[prop_key] = {'number': int(track_number)}
                    
                    # Set disc number (only if > 1 disc)
                    if disc_number and len(best_release.get('media', [])) > 1 and self.songs_properties.get('disc'):
                        prop_key = self._get_property_key(self.songs_properties['disc'], 'songs')
                        if prop_key:
                            properties[prop_key] = {'number': disc_number}
            
            # Length
            if recording_data.get('length') and self.songs_properties.get('length'):
                length_seconds = recording_data['length'] / 1000  # Convert from milliseconds
                prop_key = self._get_property_key(self.songs_properties['length'], 'songs')
                if prop_key:
                    properties[prop_key] = {'number': int(length_seconds)}
            
            # ISRC
            if recording_data.get('isrc-list') and self.songs_properties.get('isrc'):
                isrc = recording_data['isrc-list'][0] if recording_data['isrc-list'] else None
                if isrc:
                    prop_key = self._get_property_key(self.songs_properties['isrc'], 'songs')
                    if prop_key:
                        properties[prop_key] = {
                            'rich_text': [{'text': {'content': isrc}}]
                        }
            
            # Disambiguation
            if recording_data.get('disambiguation') and self.songs_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.songs_properties['disambiguation'], 'songs')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': recording_data['disambiguation']}}]
                    }
            
            # Genres + tags for songs
            if self.songs_properties.get('genres'):
                prop_key = self._get_property_key(self.songs_properties['genres'], 'songs')
                if prop_key:
                    genre_candidates = []
                    release_group = None
                    if best_release and best_release.get('release-group'):
                        release_group = best_release['release-group']
                    if release_group and release_group.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in release_group['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if release_group and release_group.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in release_group['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    if recording_data.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in recording_data['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if recording_data.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in recording_data['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    
                    # Add Spotify genres from album if available
                    # Use spotify_context if provided (from Spotify URL creation), otherwise look up via MusicBrainz relations
                    if spotify_context and spotify_context.get('album'):
                        spotify_album = spotify_context['album']
                        if spotify_album.get('genres'):
                            genre_candidates.extend(spotify_album['genres'])
                            logger.debug(f"Added {len(spotify_album['genres'])} genres from Spotify album (via context)")
                    elif best_release and best_release.get('relations'):
                        for relation in best_release.get('relations', []):
                            if relation.get('type', '').lower() in ['streaming', 'free streaming']:
                                url_resource = relation.get('url', {})
                                if isinstance(url_resource, dict):
                                    url_str = url_resource.get('resource', '')
                                else:
                                    url_str = str(url_resource)
                                
                                if url_str and 'spotify.com/album/' in url_str:
                                    spotify_id = url_str.split('/')[-1].split('?')[0]
                                    spotify_album = self.mb._get_spotify_album_by_id(spotify_id)
                                    if spotify_album and spotify_album.get('genres'):
                                        genre_candidates.extend(spotify_album['genres'])
                                        logger.debug(f"Added {len(spotify_album['genres'])} genres from Spotify album")
                                    break
                    
                    # Add Spotify artist genres if we have artist data
                    # Use spotify_context if provided (from Spotify URL creation), otherwise look up via MusicBrainz relations
                    if spotify_context and spotify_context.get('artist'):
                        spotify_artist = spotify_context['artist']
                        if spotify_artist.get('genres'):
                            genre_candidates.extend(spotify_artist['genres'])
                            logger.debug(f"Added {len(spotify_artist['genres'])} genres from Spotify artist (via context)")
                    elif recording_data.get('artist-credit') and recording_data['artist-credit']:
                        artist = recording_data['artist-credit'][0].get('artist', {})
                        artist_mbid = artist.get('id')
                        if artist_mbid:
                            # Check if artist has Spotify relationship
                            artist_data = self.mb.get_artist(artist_mbid)
                            if artist_data and artist_data.get('relations'):
                                for relation in artist_data.get('relations', []):
                                    url_resource = relation.get('url', {})
                                    if isinstance(url_resource, dict):
                                        url_str = url_resource.get('resource', '')
                                    else:
                                        url_str = str(url_resource)
                                    
                                    if url_str and 'spotify' in url_str.lower() and '/artist/' in url_str:
                                        spotify_id = url_str.split('/')[-1].split('?')[0]
                                        spotify_artist = self.mb._get_spotify_artist_by_id(spotify_id)
                                        if spotify_artist and spotify_artist.get('genres'):
                                            genre_candidates.extend(spotify_artist['genres'])
                                            logger.debug(f"Added {len(spotify_artist['genres'])} genres from Spotify artist")
                                        break
                    
                    genre_options = build_multi_select_options(
                        genre_candidates,
                        limit=10,
                        context='song genres',
                    )
                    if genre_options:
                        properties[prop_key] = {'multi_select': genre_options}
            
            # Spotify link (from url-rels) - check for both "streaming" and "free streaming"
            # Only write Spotify URL if it wasn't provided as input
            if not skip_spotify_url:
                spotify_url = None
                if recording_data.get('relations') and self.songs_properties.get('listen'):
                    for relation in recording_data.get('relations', []):
                        relation_type = relation.get('type', '').lower()
                        # Check for both "streaming" and "free streaming" relation types
                        if relation_type in ['streaming', 'free streaming']:
                            url_resource = relation.get('url', {})
                            if isinstance(url_resource, dict):
                                url_str = url_resource.get('resource', '')
                            else:
                                url_str = str(url_resource)
                            
                            # Check if it's a Spotify URL
                            if url_str and 'spotify' in url_str.lower() and 'spotify.com' in url_str.lower():
                                spotify_url = url_str
                                break
                
                # If no Spotify link found in MusicBrainz, try searching Spotify directly
                if not spotify_url and self.songs_properties.get('listen'):
                    song_title = recording_data.get('title', '')
                    artist_name = None
                    # Get artist name from artist-credit
                    if recording_data.get('artist-credit') and recording_data['artist-credit']:
                        first_artist = recording_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    if song_title:
                        spotify_url = self.mb._get_spotify_track_url(song_title, artist_name)
                        if spotify_url:
                            logger.debug(f"Found Spotify URL via API search: {spotify_url}")
                
                if spotify_url:
                    prop_key = self._get_property_key(self.songs_properties['listen'], 'songs')
                    if prop_key:
                        properties[prop_key] = {'url': spotify_url}
            
            # MusicBrainz URL
            if recording_data.get('id') and self.songs_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/recording/{recording_data['id']}"
                prop_key = self._get_property_key(self.songs_properties['musicbrainz_url'], 'songs')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.songs_properties.get('last_updated'):
                prop_key = self._get_property_key(self.songs_properties['last_updated'], 'songs')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting song properties: {e}")
        
        return properties
    
    def sync_label_page(self, page: Dict, force_update: bool = False) -> Optional[bool]:
        """Sync a single label page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.labels_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Labels database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'labels')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing label: {title}")
            
            # Check for existing MBID
            mb_id_prop_id = self.labels_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'labels')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Search or get label data
            label_data = None
            if existing_mbid:
                label_data = self.mb.get_label(existing_mbid)
                if not label_data:
                    logger.warning(f"Could not find label with MBID {existing_mbid}, searching by name")
                    existing_mbid = None
                elif not force_update:
                    # Skip pages with existing MBIDs unless force_update is True
                    logger.info(f"Skipping label '{title}' - already has MBID {existing_mbid} (use --force-update to update)")
                    return None
            
            if not label_data:
                search_results = self.mb.search_labels(title, limit=5)
                if not search_results:
                    logger.warning(f"Could not find label: {title}")
                    return False
                
                # Select best match (first result for now)
                best_match = search_results[0]
                label_data = self.mb.get_label(best_match['id'])
            
            if not label_data:
                logger.warning(f"Could not get label data for: {title}")
                return False
            
            # Format properties
            notion_props = self._format_label_properties(label_data)
            
            # Set icon
            icon = '🏷️'  # Label emoji for labels
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, None, icon):
                logger.info(f"Successfully updated label: {title}")
                return True
            else:
                logger.error(f"Failed to update label: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing label page {page.get('id')}: {e}")
            return False
    
    def _format_label_properties(self, label_data: Dict) -> Dict:
        """Format MusicBrainz label data for Notion properties."""
        properties = {}
        
        try:
            # Title (name)
            if label_data.get('name') and self.labels_properties.get('title'):
                prop_key = self._get_property_key(self.labels_properties['title'], 'labels')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': label_data['name']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if label_data.get('id') and self.labels_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.labels_properties['musicbrainz_id'], 'labels')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': label_data['id']}}]
                    }
            
            # Type
            if label_data.get('type') and self.labels_properties.get('type'):
                prop_key = self._get_property_key(self.labels_properties['type'], 'labels')
                if prop_key:
                    properties[prop_key] = {'select': {'name': label_data['type']}}
            
            # Country
            if label_data.get('area') and label_data['area'].get('iso-3166-1-code-list'):
                country_code = label_data['area']['iso-3166-1-code-list'][0]
                if self.labels_properties.get('country'):
                    prop_key = self._get_property_key(self.labels_properties['country'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'select': {'name': country_code}}
            
            # Begin date
            if label_data.get('life-span') and label_data['life-span'].get('begin'):
                begin_date = label_data['life-span']['begin']
                if self.labels_properties.get('begin_date'):
                    prop_key = self._get_property_key(self.labels_properties['begin_date'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'date': {'start': begin_date[:10]}}  # YYYY-MM-DD
            
            # End date
            if label_data.get('life-span') and label_data['life-span'].get('end'):
                end_date = label_data['life-span']['end']
                if self.labels_properties.get('end_date'):
                    prop_key = self._get_property_key(self.labels_properties['end_date'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'date': {'start': end_date[:10]}}
            
            # Disambiguation
            if label_data.get('disambiguation') and self.labels_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.labels_properties['disambiguation'], 'labels')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': label_data['disambiguation']}}]
                    }
            
            # Genres + tags for labels
            if self.labels_properties.get('genres'):
                prop_key = self._get_property_key(self.labels_properties['genres'], 'labels')
                if prop_key:
                    genre_candidates = []
                    if label_data.get('genres'):
                        genre_candidates.extend(
                            genre['name']
                            for genre in label_data['genres']
                            if isinstance(genre, dict) and genre.get('name')
                        )
                    if label_data.get('tags'):
                        genre_candidates.extend(
                            tag['name']
                            for tag in label_data['tags']
                            if isinstance(tag, dict) and tag.get('name')
                        )
                    genre_options = build_multi_select_options(
                        genre_candidates,
                        limit=10,
                        context='label genres',
                    )
                    if genre_options:
                        properties[prop_key] = {'multi_select': genre_options}
            
            # MusicBrainz URL
            if label_data.get('id') and self.labels_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/label/{label_data['id']}"
                prop_key = self._get_property_key(self.labels_properties['musicbrainz_url'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Extract URLs from url-rels
            ig_url = None
            website_url = None
            bandcamp_url = None
            
            if label_data.get('relations'):
                for relation in label_data['relations']:
                    relation_type = relation.get('type', '').lower()
                    url_resource = relation.get('url', {}).get('resource', '').lower()
                    
                    # Instagram
                    if relation_type == 'instagram' or (relation_type == 'social network' and 'instagram' in url_resource):
                        ig_url = relation.get('url', {}).get('resource')
                    # Official homepage/website/site
                    elif (relation_type == 'official homepage' or 
                          relation_type == 'official website' or 
                          relation_type == 'official site'):
                        website_url = relation.get('url', {}).get('resource')
                    # Bandcamp
                    elif 'bandcamp' in url_resource:
                        bandcamp_url = relation.get('url', {}).get('resource')
            
            # Official Website Link
            if website_url and self.labels_properties.get('official_website'):
                prop_key = self._get_property_key(self.labels_properties['official_website'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': website_url}
            
            # IG Link
            if ig_url and self.labels_properties.get('ig'):
                prop_key = self._get_property_key(self.labels_properties['ig'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': ig_url}
            
            # Bandcamp Link
            if bandcamp_url and self.labels_properties.get('bandcamp'):
                prop_key = self._get_property_key(self.labels_properties['bandcamp'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': bandcamp_url}
            
            # Founded (date from begin date)
            if label_data.get('life-span') and label_data['life-span'].get('begin') and self.labels_properties.get('founded'):
                begin_date = label_data['life-span']['begin']
                # Format as date (YYYY-MM-DD, truncate to 10 chars if longer)
                prop_key = self._get_property_key(self.labels_properties['founded'], 'labels')
                if prop_key:
                    properties[prop_key] = {'date': {'start': begin_date[:10]}}  # YYYY-MM-DD
            
            # Area (relation to Locations database)
            if label_data.get('area') and label_data['area'].get('name') and self.labels_properties.get('area') and self.locations_db_id:
                area_name = label_data['area']['name']
                location_page_id = self._find_or_create_location_page(area_name)
                if location_page_id:
                    prop_key = self._get_property_key(self.labels_properties['area'], 'labels')
                    if prop_key:
                        properties[prop_key] = {
                            'relation': [{'id': location_page_id}]
                        }
            
            # Last updated
            if self.labels_properties.get('last_updated'):
                prop_key = self._get_property_key(self.labels_properties['last_updated'], 'labels')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting label properties: {e}")
        
        return properties
    
    def _init_results(self) -> Dict:
        """Return a fresh results dictionary."""
        return {
            'success': True,
            'total_pages': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 0,
            'duration': 0
        }
    
    def _database_name_from_page(self, page: Dict) -> Optional[str]:
        """Infer which configured database a page belongs to."""
        parent = page.get('parent') or {}
        database_id = normalize_id(parent.get('database_id'))
        configured = {
            'artists': normalize_id(self.artists_db_id),
            'albums': normalize_id(self.albums_db_id),
            'songs': normalize_id(self.songs_db_id),
            'labels': normalize_id(self.labels_db_id)
        }
        for name, cfg_id in configured.items():
            if cfg_id and database_id == cfg_id:
                return name
        return None
    
    def _process_page_by_db_name(self, db_name: str, page: Dict, force_update: bool) -> Optional[bool]:
        """Dispatch page processing based on database name."""
        if db_name == 'artists':
            return self.sync_artist_page(page, force_update)
        if db_name == 'albums':
            return self.sync_album_page(page, force_update)
        if db_name == 'songs':
            return self.sync_song_page(page, force_update)
        if db_name == 'labels':
            return self.sync_label_page(page, force_update)
        
        logger.error(f"Unsupported database '{db_name}' for page {page.get('id')}")
        return None
    
    def _run_page_specific_sync(self, page_id: str, force_update: bool, expected_database: str) -> Dict:
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
        parent = page.get('parent') or {}
        parent_db_id = parent.get('database_id')
        
        db_name = self._database_name_from_page(page)
        if not db_name:
            configured_ids = {
                'artists': self.artists_db_id,
                'albums': self.albums_db_id,
                'songs': self.songs_db_id,
                'labels': self.labels_db_id
            }
            configured_ids = {k: v for k, v in configured_ids.items() if v}
            logger.error(
                "Page %s belongs to database %s, which is not among configured IDs: %s",
                page_id,
                parent_db_id,
                configured_ids
            )
            return {
                'success': False,
                'message': f'Page {page_id} parent {parent_db_id} is not part of a configured database'
            }
        
        if expected_database != 'all' and expected_database != db_name:
            logger.warning(
                "Requested database '%s' does not match page %s database '%s'; continuing anyway",
                expected_database,
                page_id,
                db_name
            )
        
        if db_name in ['artists', 'labels'] and self.locations_db_id:
            self._load_locations_cache()
        
        result_flag = self._process_page_by_db_name(db_name, page, force_update)
        results = self._init_results()
        results['total_pages'] = 1
        if result_flag is True:
            results['successful_updates'] = 1
        elif result_flag is False:
            results['failed_updates'] = 1
        else:
            results['skipped_updates'] = 1
        
        results['duration'] = time.time() - start_time
        logger.info(f"Finished page-specific sync for {db_name} page {page_id}")
        return results
    
    def create_from_spotify_url(self, spotify_url: str) -> Dict:
        """Create a new Notion page from a Spotify URL.
        
        Args:
            spotify_url: Spotify URL for a track, album, or artist
            
        Returns:
            Dict with keys: success, message, page_id, entity_type, created
        """
        logger.info(f"Creating page from Spotify URL: {spotify_url}")
        
        # Parse Spotify URL
        parsed = self.mb._parse_spotify_url(spotify_url)
        if not parsed:
            return {
                'success': False,
                'message': f'Invalid Spotify URL format: {spotify_url}'
            }
        
        entity_type = parsed['type']
        spotify_id = parsed['id']
        
        logger.info(f"Detected Spotify {entity_type}: {spotify_id}")
        
        # Fetch Spotify data based on type
        spotify_data = None
        if entity_type == 'track':
            spotify_data = self.mb._get_spotify_track_by_id(spotify_id)
        elif entity_type == 'album':
            spotify_data = self.mb._get_spotify_album_by_id(spotify_id)
        elif entity_type == 'artist':
            spotify_data = self.mb._get_spotify_artist_by_id(spotify_id)
        
        if not spotify_data:
            return {
                'success': False,
                'message': f'Could not fetch {entity_type} data from Spotify'
            }
        
        # Extract basic info
        name = spotify_data.get('name', '')
        if not name:
            return {
                'success': False,
                'message': f'No name found in Spotify {entity_type} data'
            }
        
        # Handle creation based on entity type
        if entity_type == 'track':
            return self._create_track_from_spotify(spotify_data, spotify_url)
        elif entity_type == 'album':
            return self._create_album_from_spotify(spotify_data, spotify_url)
        elif entity_type == 'artist':
            return self._create_artist_from_spotify(spotify_data, spotify_url)
        
        return {
            'success': False,
            'message': f'Unsupported entity type: {entity_type}'
        }
    
    def _create_track_from_spotify(self, spotify_data: Dict, spotify_url: str) -> Dict:
        """Create a track page from Spotify data."""
        track_name = spotify_data.get('name', '')
        
        # Extract external IDs
        external_ids = self.mb._extract_external_ids(spotify_data)
        isrc = external_ids.get('isrc')
        
        # Search MusicBrainz by ISRC
        mb_recording = None
        song_mbid = None
        if isrc:
            logger.info(f"Searching MusicBrainz by ISRC: {isrc}")
            mb_recording = self.mb.search_recording_by_isrc(isrc)
            if mb_recording:
                song_mbid = mb_recording.get('id')
                logger.info(f"Found MusicBrainz recording: {song_mbid}")
        
        # Check if song already exists in Notion
        if song_mbid:
            mbid_key = self._get_property_key(self.songs_properties.get('musicbrainz_id'), 'songs')
            if mbid_key:
                existing_page_id = self._find_existing_page_by_mbid(self.songs_db_id, song_mbid, mbid_key)
                if existing_page_id:
                    logger.info(f"Song already exists in Notion: {existing_page_id}")
                    # Update the existing page
                    page = self.notion.get_page(existing_page_id)
                    if page:
                        self.sync_song_page(page, force_update=True, spotify_url=spotify_url)
                    return {
                        'success': True,
                        'message': f'Updated existing song: {track_name}',
                        'page_id': existing_page_id,
                        'entity_type': 'song',
                        'created': False
                    }
        
        # Check by Spotify URL
        spotify_key = self._get_property_key(self.songs_properties.get('listen'), 'songs')
        if spotify_key:
            existing_page_id = self._find_existing_page_by_spotify_url(self.songs_db_id, spotify_url, spotify_key)
            if existing_page_id:
                logger.info(f"Song already exists in Notion (by Spotify URL): {existing_page_id}")
                return {
                    'success': True,
                    'message': f'Song already exists: {track_name}',
                    'page_id': existing_page_id,
                    'entity_type': 'song',
                    'created': False
                }
        
        # Extract album and artist info
        album_data = spotify_data.get('album', {})
        album_name = album_data.get('name', '')
        artists_data = spotify_data.get('artists', [])
        
        # Create/find album
        album_page_id = None
        album_mbid = None
        album_spotify_id = album_data.get('id', '') if album_data else ''
        if album_name:
            # Search for album by UPC if available
            album_external_ids = self.mb._extract_external_ids(album_data)
            upc = album_external_ids.get('upc') or album_external_ids.get('ean')
            mb_release = None
            if upc:
                logger.info(f"Searching MusicBrainz for album by barcode: {upc}")
                mb_release = self.mb.search_release_by_barcode(upc)
                if mb_release:
                    album_mbid = mb_release.get('id')
                    logger.info(f"Found MusicBrainz release: {album_mbid}")
            
            # Pass artist name for better album matching, and set_dns=True for Spotify URL flow
            album_page_id = self._find_or_create_album_page(album_name, album_mbid, artist_name=artists_data[0].get('name') if artists_data else None, set_dns=True)
            
            # Set Spotify URL on the album page
            if album_page_id and album_spotify_id:
                album_spotify_url = f"https://open.spotify.com/album/{album_spotify_id}"
                spotify_prop_id = self.albums_properties.get('listen')
                if spotify_prop_id:
                    spotify_key = self._get_property_key(spotify_prop_id, 'albums')
                    if spotify_key:
                        self.notion.update_page(album_page_id, {
                            spotify_key: {'url': album_spotify_url}
                        })
        
        # Create/find artist
        artist_page_id = None
        if artists_data:
            artist_name = artists_data[0].get('name', '')
            artist_spotify_id = artists_data[0].get('id', '')
            artist_mbid = None
            
            # Try to find artist in MusicBrainz by Spotify ID
            if artist_spotify_id:
                mb_artist = self.mb.get_artist_by_spotify_id(artist_spotify_id)
                if mb_artist:
                    artist_mbid = mb_artist.get('id')
                    logger.info(f"Found MusicBrainz artist: {artist_mbid}")
            
            # Set DNS=True for Spotify URL flow to prevent automation cascade
            artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid, set_dns=True)
            
            # Set artist cover image from Spotify
            if artist_page_id and artist_spotify_id:
                artist_cover_url = self.mb._get_spotify_artist_image(artist_name, artist_mbid, artist_spotify_id)
                if artist_cover_url:
                    self.notion.update_page(artist_page_id, {}, artist_cover_url)
        
        # Create the song page with DNS=True for Spotify URL flow
        song_page_id = self._find_or_create_song_page(
            track_name,
            song_mbid,
            album_page_id,
            artist_page_id,
            set_dns=True
        )
        
        # Populate Spotify URL on the song page
        if song_page_id and spotify_url:
            spotify_prop_id = self.songs_properties.get('listen')
            if spotify_prop_id:
                spotify_key = self._get_property_key(spotify_prop_id, 'songs')
                if spotify_key:
                    self.notion.update_page(song_page_id, {
                        spotify_key: {'url': spotify_url}
                    })
        
        if not song_page_id:
            return {
                'success': False,
                'message': f'Failed to create song page: {track_name}'
            }
        
        # Now sync the page to populate all fields
        page = self.notion.get_page(song_page_id)
        if page:
            sync_result = self.sync_song_page(page, force_update=True, spotify_url=spotify_url)
            
            # If MusicBrainz sync failed, populate with Spotify data as fallback
            if not sync_result and spotify_data:
                logger.info(f"MusicBrainz sync failed, populating with Spotify data for '{track_name}'")
                spotify_props = self._format_spotify_song_properties(spotify_data, spotify_url)
                if spotify_props:
                    self.notion.update_page(song_page_id, spotify_props)
                    logger.info(f"Updated song with Spotify data: {track_name}")
        
        return {
            'success': True,
            'message': f'Created song: {track_name}',
            'page_id': song_page_id,
            'entity_type': 'song',
            'created': True
        }
    
    def _format_spotify_song_properties(self, spotify_data: Dict, spotify_url: str) -> Dict:
        """Format Spotify track data for Notion properties (fallback when MusicBrainz unavailable)."""
        properties = {}
        
        try:
            # Spotify URL (Listen property)
            if spotify_url and self.songs_properties.get('listen'):
                prop_key = self._get_property_key(self.songs_properties['listen'], 'songs')
                if prop_key:
                    properties[prop_key] = {'url': spotify_url}
            
            # Track length/duration (convert from milliseconds to seconds)
            if spotify_data.get('duration_ms') and self.songs_properties.get('length'):
                prop_key = self._get_property_key(self.songs_properties['length'], 'songs')
                if prop_key:
                    duration_seconds = spotify_data['duration_ms'] / 1000
                    # Format as MM:SS
                    minutes = int(duration_seconds // 60)
                    seconds = int(duration_seconds % 60)
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': f"{minutes}:{seconds:02d}"}}]
                    }
            
            # Track number (disc_number and track_number)
            if spotify_data.get('track_number') and self.songs_properties.get('track_number'):
                prop_key = self._get_property_key(self.songs_properties['track_number'], 'songs')
                if prop_key:
                    track_num = spotify_data['track_number']
                    if spotify_data.get('disc_number', 1) > 1:
                        track_num = f"{spotify_data['disc_number']}-{track_num}"
                    properties[prop_key] = {'number': spotify_data['track_number']}
            
            # Store disc number if > 1
            if spotify_data.get('disc_number', 1) > 1 and self.songs_properties.get('disc'):
                disc_prop_key = self._get_property_key(self.songs_properties['disc'], 'songs')
                if disc_prop_key:
                    properties[disc_prop_key] = {'number': spotify_data['disc_number']}
            
            # ISRC (if available)
            if spotify_data.get('external_ids', {}).get('isrc') and self.songs_properties.get('isrc'):
                prop_key = self._get_property_key(self.songs_properties['isrc'], 'songs')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': spotify_data['external_ids']['isrc']}}]
                    }
            
            # Genres (fetch from artist since tracks don't have genres in Spotify)
            if spotify_data.get('artists') and self.songs_properties.get('genres'):
                artist_spotify_id = spotify_data['artists'][0].get('id')
                if artist_spotify_id:
                    # Fetch full artist data to get genres
                    artist_data = self.mb._get_spotify_artist_by_id(artist_spotify_id)
                    if artist_data and artist_data.get('genres'):
                        genres = artist_data['genres'][:10]  # Limit to top 10 genres
                        if genres:
                            from shared.utils import build_multi_select_options
                            genre_options = build_multi_select_options(
                                genres,
                                limit=10,
                                context="genres"
                            )
                            if genre_options:
                                prop_key = self._get_property_key(self.songs_properties['genres'], 'songs')
                                if prop_key:
                                    properties[prop_key] = {'multi_select': genre_options}
                                    logger.info(f"Added {len(genre_options)} genres from Spotify artist")
            
            logger.info(f"Formatted {len(properties)} Spotify properties for song")
            return properties
            
        except Exception as e:
            logger.error(f"Error formatting Spotify song properties: {e}")
            return {}
    
    def _create_album_from_spotify(self, spotify_data: Dict, spotify_url: str) -> Dict:
        """Create an album page from Spotify data."""
        album_name = spotify_data.get('name', '')
        
        # Extract external IDs
        external_ids = self.mb._extract_external_ids(spotify_data)
        upc = external_ids.get('upc') or external_ids.get('ean')
        
        # Search MusicBrainz by barcode
        mb_release = None
        album_mbid = None
        if upc:
            logger.info(f"Searching MusicBrainz by barcode: {upc}")
            mb_release = self.mb.search_release_by_barcode(upc)
            if mb_release:
                album_mbid = mb_release.get('id')
                logger.info(f"Found MusicBrainz release: {album_mbid}")
        
        # Check if album already exists in Notion
        if album_mbid:
            mbid_key = self._get_property_key(self.albums_properties.get('musicbrainz_id'), 'albums')
            if mbid_key:
                existing_page_id = self._find_existing_page_by_mbid(self.albums_db_id, album_mbid, mbid_key)
                if existing_page_id:
                    # Validate that the existing page's title matches the requested title
                    page = self.notion.get_page(existing_page_id)
                    if page:
                        title_prop_id = self.albums_properties.get('title')
                        title_key = self._get_property_key(title_prop_id, 'albums')
                        if title_key:
                            existing_page_title_prop = page.get('properties', {}).get(title_key, {})
                            if existing_page_title_prop.get('title') and existing_page_title_prop['title']:
                                existing_title = existing_page_title_prop['title'][0]['plain_text']
                                # Check if titles match (case-insensitive)
                                if existing_title.lower() == album_name.lower():
                                    logger.info(f"Album already exists in Notion: {existing_page_id}")
                                    self.sync_album_page(page, force_update=True, spotify_url=spotify_url)
                                    return {
                                        'success': True,
                                        'message': f'Updated existing album: {album_name}',
                                        'page_id': existing_page_id,
                                        'entity_type': 'album',
                                        'created': False
                                    }
                                else:
                                    # Titles don't match - MusicBrainz returned wrong album
                                    logger.warning(f"MBID {album_mbid} has title '{existing_title}' but requested title is '{album_name}'. Ignoring bad MBID and creating new page.")
                                    album_mbid = None  # Clear the bad MBID
        
        # Check by Spotify URL
        spotify_key = self._get_property_key(self.albums_properties.get('listen'), 'albums')
        if spotify_key:
            existing_page_id = self._find_existing_page_by_spotify_url(self.albums_db_id, spotify_url, spotify_key)
            if existing_page_id:
                logger.info(f"Album already exists in Notion (by Spotify URL): {existing_page_id}")
                return {
                    'success': True,
                    'message': f'Album already exists: {album_name}',
                    'page_id': existing_page_id,
                    'entity_type': 'album',
                    'created': False
                }
        
        # Create the album page with DNS=True for Spotify URL flow
        album_page_id = self._find_or_create_album_page(album_name, album_mbid, set_dns=True)
        
        if not album_page_id:
            return {
                'success': False,
                'message': f'Failed to create album page: {album_name}'
            }
        
        # Now sync the page to populate all fields
        page = self.notion.get_page(album_page_id)
        if page:
            self.sync_album_page(page, force_update=True, spotify_url=spotify_url)
        
        return {
            'success': True,
            'message': f'Created album: {album_name}',
            'page_id': album_page_id,
            'entity_type': 'album',
            'created': True
        }
    
    def _create_artist_from_spotify(self, spotify_data: Dict, spotify_url: str) -> Dict:
        """Create an artist page from Spotify data."""
        artist_name = spotify_data.get('name', '')
        artist_spotify_id = spotify_data.get('id', '')
        
        # Search MusicBrainz by Spotify ID
        mb_artist = None
        artist_mbid = None
        if artist_spotify_id:
            logger.info(f"Searching MusicBrainz by Spotify ID: {artist_spotify_id}")
            mb_artist = self.mb.get_artist_by_spotify_id(artist_spotify_id)
            if mb_artist:
                artist_mbid = mb_artist.get('id')
                logger.info(f"Found MusicBrainz artist: {artist_mbid}")
        
        # Check if artist already exists in Notion
        if artist_mbid:
            mbid_key = self._get_property_key(self.artists_properties.get('musicbrainz_id'), 'artists')
            if mbid_key:
                existing_page_id = self._find_existing_page_by_mbid(self.artists_db_id, artist_mbid, mbid_key)
                if existing_page_id:
                    # Validate that the existing page's name matches the requested name
                    # This prevents linking to wrong artists when MusicBrainz returns bad data
                    page = self.notion.get_page(existing_page_id)
                    if page:
                        title_prop_id = self.artists_properties.get('title')
                        title_key = self._get_property_key(title_prop_id, 'artists')
                        if title_key:
                            existing_page_title_prop = page.get('properties', {}).get(title_key, {})
                            if existing_page_title_prop.get('title') and existing_page_title_prop['title']:
                                existing_name = existing_page_title_prop['title'][0]['plain_text']
                                # Check if names match (case-insensitive)
                                if existing_name.lower() == artist_name.lower():
                                    logger.info(f"Artist already exists in Notion: {existing_page_id}")
                                    self.sync_artist_page(page, force_update=True, spotify_url=spotify_url)
                                    return {
                                        'success': True,
                                        'message': f'Updated existing artist: {artist_name}',
                                        'page_id': existing_page_id,
                                        'entity_type': 'artist',
                                        'created': False
                                    }
                                else:
                                    # Names don't match - MusicBrainz returned wrong artist for Spotify ID
                                    logger.warning(f"MBID {artist_mbid} has name '{existing_name}' but requested name is '{artist_name}'. Ignoring bad MBID and creating new page.")
                                    artist_mbid = None  # Clear the bad MBID
        
        # Check by Spotify URL
        spotify_key = self._get_property_key(self.artists_properties.get('streaming_link'), 'artists')
        if spotify_key:
            existing_page_id = self._find_existing_page_by_spotify_url(self.artists_db_id, spotify_url, spotify_key)
            if existing_page_id:
                logger.info(f"Artist already exists in Notion (by Spotify URL): {existing_page_id}")
                return {
                    'success': True,
                    'message': f'Artist already exists: {artist_name}',
                    'page_id': existing_page_id,
                    'entity_type': 'artist',
                    'created': False
                }
        
        # Create the artist page with DNS=True for Spotify URL flow
        artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid, set_dns=True)
        
        if not artist_page_id:
            return {
                'success': False,
                'message': f'Failed to create artist page: {artist_name}'
            }
        
        # Set artist cover image from Spotify (before syncing)
        if artist_spotify_id:
            artist_cover_url = self.mb._get_spotify_artist_image(artist_name, artist_mbid, artist_spotify_id)
            if artist_cover_url:
                self.notion.update_page(artist_page_id, {}, artist_cover_url)
        
        # Now sync the page to populate all fields
        page = self.notion.get_page(artist_page_id)
        if page:
            self.sync_artist_page(page, force_update=True, spotify_url=spotify_url)
        
        return {
            'success': True,
            'message': f'Created artist: {artist_name}',
            'page_id': artist_page_id,
            'entity_type': 'artist',
            'created': True
        }
    
    def run_sync(
        self,
        database: str = 'all',
        force_update: bool = False,
        last_page: bool = False,
        created_after: Optional[str] = None,
        page_id: Optional[str] = None,
        spotify_url: Optional[str] = None
    ) -> Dict:
        """Run the synchronization process for specified database(s).
        
        Args:
            database: Which database to sync or 'all'
            force_update: Update pages even if they already have MBIDs
            last_page: Only process the most recently edited page
            created_after: ISO timestamp string to filter pages by creation date
            page_id: Explicit Notion page ID to sync
            spotify_url: Spotify URL to create new page from (track, album, or artist)
        """
        # Handle Spotify URL creation mode (no page_id required)
        if spotify_url and not page_id:
            logger.info(f"Spotify URL creation mode: {spotify_url}")
            return self.create_from_spotify_url(spotify_url)
        
        logger.info(f"Starting Notion-MusicBrainz synchronization (database: {database})")
        
        if page_id:
            if last_page:
                logger.error("Cannot combine page-specific sync with last-page mode")
                return {
                    'success': False,
                    'message': 'last-page mode cannot be combined with page-specific sync'
                }
            if created_after:
                logger.warning("--created-after is ignored when --page-id is provided")
            return self._run_page_specific_sync(page_id, force_update, database)
        
        if database not in ['artists', 'albums', 'songs', 'labels', 'all']:
            logger.error(f"Invalid database: {database}. Must be 'artists', 'albums', 'songs', 'labels', or 'all'")
            return {'success': False, 'message': f'Invalid database: {database}'}
        if last_page and database == 'all':
            logger.error("Last-page mode requires a specific database (not 'all')")
            return {
                'success': False,
                'message': 'last-page mode requires a specific database'
            }
        
        start_time = time.time()
        results = self._init_results()
        
        databases_to_sync = []
        if database == 'all':
            databases_to_sync = ['artists', 'albums', 'songs', 'labels']
        else:
            databases_to_sync = [database]
        
        for db_name in databases_to_sync:
            if db_name == 'artists' and not self.artists_db_id:
                logger.warning("Artists database ID not configured, skipping")
                continue
            elif db_name == 'albums' and not self.albums_db_id:
                logger.warning("Albums database ID not configured, skipping")
                continue
            elif db_name == 'songs' and not self.songs_db_id:
                logger.warning("Songs database ID not configured, skipping")
                continue
            elif db_name == 'labels' and not self.labels_db_id:
                logger.warning("Labels database ID not configured, skipping")
                continue
            
            logger.info(f"Syncing {db_name} database...")
            
            # Initialize location cache if needed (for artists and labels)
            if db_name in ['artists', 'labels'] and self.locations_db_id:
                self._load_locations_cache()
            
            db_id = getattr(self, f'{db_name}_db_id')
            filter_params = None
            if created_after:
                logger.info(f"Filtering {db_name} pages created on/after {created_after}")
                filter_params = {
                    'timestamp': 'created_time',
                    'created_time': {'on_or_after': created_after}
                }
            pages = self.notion.query_database(db_id, filter_params)
            
            if not pages:
                logger.warning(f"No pages found in {db_name} database")
                continue
            
            # Handle last-page mode
            if last_page:
                logger.info(f"Last-page mode: Processing only the most recently edited page in {db_name}")
                pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
                pages = pages[:1]
            
            logger.info(f"Found {len(pages)} pages to process in {db_name}")
            results['total_pages'] += len(pages)
            
            successful = 0
            failed = 0
            skipped = 0
            
            # Process pages (single-threaded due to rate limiting)
            for i, page in enumerate(pages, 1):
                try:
                    result = self._process_page_by_db_name(db_name, page, force_update)
                    if result is True:
                        successful += 1
                    elif result is False:
                        failed += 1
                    else:
                        skipped += 1
                    
                    logger.info(f"Completed {db_name} page {i}/{len(pages)}")
                    
                except Exception as e:
                    logger.error(f"Error processing {db_name} page {page.get('id')}: {e}")
                    failed += 1
            
            results['successful_updates'] += successful
            results['failed_updates'] += failed
            results['skipped_updates'] += skipped
        
        end_time = time.time()
        results['duration'] = end_time - start_time
        
        logger.info(f"Sync completed in {results['duration']:.2f} seconds")
        logger.info(f"Successful updates: {results['successful_updates']}")
        logger.info(f"Failed updates: {results['failed_updates']}")
        if results['skipped_updates'] > 0:
            logger.info(f"Skipped updates: {results['skipped_updates']}")
        
        return results


def validate_environment():
    """Validate environment variables and configuration."""
    errors = []
    
    notion_token = get_notion_token()
    musicbrainz_user_agent = os.getenv('MUSICBRAINZ_USER_AGENT')
    artists_db_id = os.getenv('NOTION_ARTISTS_DATABASE_ID')
    albums_db_id = os.getenv('NOTION_ALBUMS_DATABASE_ID')
    songs_db_id = os.getenv('NOTION_SONGS_DATABASE_ID')
    
    if not notion_token:
        errors.append("NOTION_INTERNAL_INTEGRATION_SECRET (or legacy NOTION_TOKEN)")
    if not musicbrainz_user_agent:
        errors.append("MUSICBRAINZ_USER_AGENT: Your app name and contact email (e.g., 'MyApp/1.0 (email@example.com)')")
    
    labels_db_id = os.getenv('NOTION_LABELS_DATABASE_ID')
    
    if not artists_db_id and not albums_db_id and not songs_db_id and not labels_db_id:
        errors.append("At least one database ID must be configured (NOTION_ARTISTS_DATABASE_ID, NOTION_ALBUMS_DATABASE_ID, NOTION_SONGS_DATABASE_ID, or NOTION_LABELS_DATABASE_ID)")
    
    if errors:
        logger.error("Missing required environment variables:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease check your .env file or environment variables.")
        return False
    
    if notion_token and not notion_token.startswith(('secret_', 'ntn_')):
        logger.warning("Notion token should start with 'secret_' or 'ntn_'")
    
    return True


def _build_sync_instance() -> NotionMusicBrainzSync:
    notion_token = get_notion_token()
    musicbrainz_user_agent = os.getenv('MUSICBRAINZ_USER_AGENT')
    artists_db_id = os.getenv('NOTION_ARTISTS_DATABASE_ID')
    albums_db_id = os.getenv('NOTION_ALBUMS_DATABASE_ID')
    songs_db_id = os.getenv('NOTION_SONGS_DATABASE_ID')
    labels_db_id = os.getenv('NOTION_LABELS_DATABASE_ID')
    
    return NotionMusicBrainzSync(
        notion_token,
        musicbrainz_user_agent,
        artists_db_id=artists_db_id,
        albums_db_id=albums_db_id,
        songs_db_id=songs_db_id,
        labels_db_id=labels_db_id
    )


def run_sync(
    *,
    database: str = 'all',
    force_update: bool = False,
    last_page: bool = False,
    created_after: Optional[str] = None,
    page_id: Optional[str] = None,
    spotify_url: Optional[str] = None
) -> Dict:
    """Run the MusicBrainz sync with the provided options."""
    # Spotify URL creation mode takes precedence
    if spotify_url and not page_id:
        return _build_sync_instance().run_sync(spotify_url=spotify_url)
    
    is_repo_dispatch = os.getenv('GITHUB_EVENT_NAME') == 'repository_dispatch'
    if is_repo_dispatch and not page_id:
        if database == 'all':
            raise RuntimeError(
                "Repository dispatch triggered without page-id or specific database; aborting to avoid full sync"
            )
        if not last_page:
            logger.info(
                "Repository dispatch without page-id detected; defaulting to last-page mode for %s",
                database,
            )
            last_page = True
    if last_page and database == 'all':
        raise RuntimeError("--last-page requires a specific database when page-id is absent")

    normalized_created_after = None
    if created_after:
        input_str = created_after.strip()
        if input_str.lower() == 'today':
            today = datetime.now(timezone.utc).date()
            normalized_created_after = f"{today.isoformat()}T00:00:00Z"
        else:
            try:
                parsed_date = datetime.strptime(input_str, '%Y-%m-%d').date()
                normalized_created_after = f"{parsed_date.isoformat()}T00:00:00Z"
            except ValueError:
                raise ValueError("--created-after must be in YYYY-MM-DD format or 'today'")
    return _build_sync_instance().run_sync(
        database=database,
        force_update=force_update,
        last_page=last_page,
        created_after=normalized_created_after,
        page_id=page_id,
        spotify_url=spotify_url
    )


def get_database_ids() -> List[str]:
    """Return normalized database IDs served by this sync."""
    env_vars = [
        'NOTION_ARTISTS_DATABASE_ID',
        'NOTION_ALBUMS_DATABASE_ID',
        'NOTION_SONGS_DATABASE_ID',
        'NOTION_LABELS_DATABASE_ID',
        'NOTION_LOCATIONS_DATABASE_ID'
    ]
    normalized_ids = []
    for key in env_vars:
        value = os.getenv(key)
        normalized = normalize_id(value) if value else None
        if normalized:
            normalized_ids.append(normalized)
    return normalized_ids

