# Property Mapping Configuration for MusicBrainz Sync
# This file allows you to specify exact property IDs for maximum stability
# Property IDs never change when you rename properties in Notion

# ============================================================================
# ARTISTS DATABASE PROPERTIES
# ============================================================================

# Required Properties (must be present in your Artists database)
ARTISTS_TITLE_PROPERTY_ID = "title"  # Name (title)

# Core Properties
ARTISTS_MUSICBRAINZ_ID_PROPERTY_ID = "W%5BxR"  # Artist ID (rich_text)
ARTISTS_SORT_NAME_PROPERTY_ID = None  # Sort name (rich text) - not in database
ARTISTS_TYPE_PROPERTY_ID = "GUXe"  # Type: Person, Group, etc. (select)
ARTISTS_GENDER_PROPERTY_ID = "RLTz"  # Gender (select)
ARTISTS_AREA_PROPERTY_ID = "gZQw"  # Area (place)
ARTISTS_BORN_IN_PROPERTY_ID = "dYj%3F"  # Born In (place)
ARTISTS_IG_LINK_PROPERTY_ID = "Rkyv"  # IG link (url)
ARTISTS_WEBSITE_LINK_PROPERTY_ID = "B%3EiA"  # Official website link (url)
ARTISTS_YOUTUBE_LINK_PROPERTY_ID = "pEKL"  # YouTube link (url)
ARTISTS_BANDCAMP_LINK_PROPERTY_ID = "Gj%5BM"  # Bandcamp link (url)
ARTISTS_STREAMING_LINK_PROPERTY_ID = "Cd~%3B"  # Streaming link (url) - Spotify
ARTISTS_COUNTRY_PROPERTY_ID = None  # Country (select) - not in database
ARTISTS_BEGIN_DATE_PROPERTY_ID = "E%3E%7Be"  # Active (date) - using as begin date
ARTISTS_END_DATE_PROPERTY_ID = None  # End date (date) - not in database
ARTISTS_DISAMBIGUATION_PROPERTY_ID = None  # Disambiguation (rich text) - not in database
ARTISTS_DESCRIPTION_PROPERTY_ID = None  # Description/biography (rich text) - not in database
ARTISTS_GENRES_PROPERTY_ID = "Y%5CLv"  # Genres (multi-select)
ARTISTS_TAGS_PROPERTY_ID = "mos%3C"  # M-Tags (multi-select)
ARTISTS_RATING_PROPERTY_ID = "cs_k"  # Rating (number)
ARTISTS_LAST_UPDATED_PROPERTY_ID = None  # Last updated (date) - not in database
ARTISTS_MUSICBRAINZ_URL_PROPERTY_ID = "W%40zN"  # Website (url) - using for MusicBrainz URL

# Relations
ARTISTS_ALBUMS_PROPERTY_ID = "r%60k%7C"  # Albums (relation)
ARTISTS_SONGS_PROPERTY_ID = "_W%3Fj"  # Songs (relation)

# ============================================================================
# ALBUMS DATABASE PROPERTIES
# ============================================================================

# Required Properties (must be present in your Albums database)
ALBUMS_TITLE_PROPERTY_ID = "title"  # Name (title)

# Core Properties
ALBUMS_MUSICBRAINZ_ID_PROPERTY_ID = "UH%5EX"  # Album ID (rich_text)
ALBUMS_ARTIST_PROPERTY_ID = "oqh%5C"  # Artist(s) (relation)
ALBUMS_RELEASE_DATE_PROPERTY_ID = "vvEz"  # Released (date)
ALBUMS_COUNTRY_PROPERTY_ID = None  # Country (select) - not in database
ALBUMS_LABEL_PROPERTY_ID = "Q%3B%5Ev"  # Label (relation)
ALBUMS_TYPE_PROPERTY_ID = "tVNK"  # Album Type (select)
ALBUMS_STATUS_PROPERTY_ID = None  # Status: Official, Promotion, etc. (select) - not in database
ALBUMS_LISTEN_PROPERTY_ID = "GaOL"  # Listen (url) - Spotify link
ALBUMS_PACKAGING_PROPERTY_ID = None  # Packaging (select) - not in database
ALBUMS_BARCODE_PROPERTY_ID = None  # Barcode (rich text) - not in database
ALBUMS_FORMAT_PROPERTY_ID = None  # Format: CD, Digital, etc. (multi-select) - not in database
ALBUMS_TRACK_COUNT_PROPERTY_ID = None  # Track count (number) - not in database
ALBUMS_DESCRIPTION_PROPERTY_ID = None  # Description (rich text) - not in database
ALBUMS_GENRES_PROPERTY_ID = "BDUf"  # Genres (multi-select)
ALBUMS_TAGS_PROPERTY_ID = "gylL"  # M-Tags (multi-select)
ALBUMS_RATING_PROPERTY_ID = "%7BSBN"  # Rating (number)
ALBUMS_COVER_IMAGE_PROPERTY_ID = None  # Cover image URL (url) - not in database
ALBUMS_MUSICBRAINZ_URL_PROPERTY_ID = "UMvx"  # MusicBrainz URL (url)
ALBUMS_LAST_UPDATED_PROPERTY_ID = None  # Last updated (date) - not in database
ALBUMS_DISCS_PROPERTY_ID = "%3F%5D_P"  # Discs (number) - total disc count for multi-disc albums

# Relations
ALBUMS_SONGS_PROPERTY_ID = "%7BCY%3F"  # Songs (relation)

# ============================================================================
# SONGS DATABASE PROPERTIES
# ============================================================================

# Required Properties (must be present in your Songs database)
SONGS_TITLE_PROPERTY_ID = "title"  # Name (title)

# Core Properties
SONGS_MUSICBRAINZ_ID_PROPERTY_ID = "qNi%5B"  # Song ID (rich_text)
SONGS_ARTIST_PROPERTY_ID = "%3FYRk"  # Artist (relation)
SONGS_ALBUM_PROPERTY_ID = "H%5D%5Dk"  # Album (relation)
SONGS_TRACK_NUMBER_PROPERTY_ID = "pX%7CI"  # Track No. (number)
SONGS_LENGTH_PROPERTY_ID = None  # Length in seconds (number) - not in database
SONGS_ISRC_PROPERTY_ID = None  # ISRC (rich text) - not in database
SONGS_DISAMBIGUATION_PROPERTY_ID = None  # Disambiguation (rich text) - not in database
SONGS_DESCRIPTION_PROPERTY_ID = None  # Description (rich text) - not in database
SONGS_GENRES_PROPERTY_ID = "zuFC"  # Genre (multi-select)
SONGS_TAGS_PROPERTY_ID = "PF%60e"  # Tags (multi-select)
SONGS_LISTEN_PROPERTY_ID = "ZSU_"  # Listen (url) - Spotify link
SONGS_RATING_PROPERTY_ID = "HGkO"  # Rating (number)
SONGS_MUSICBRAINZ_URL_PROPERTY_ID = "daMa"  # MusicBrainz URL (url)
SONGS_LAST_UPDATED_PROPERTY_ID = None  # Last updated (date) - not in database
SONGS_DISC_PROPERTY_ID = "tkZF"  # Disc (number) - which disc this track is on (for multi-disc albums)

# ============================================================================
# LABELS DATABASE PROPERTIES
# ============================================================================

# Required Properties (must be present in your Labels database)
LABELS_TITLE_PROPERTY_ID = "title"  # Name (title)

# Core Properties
LABELS_MUSICBRAINZ_ID_PROPERTY_ID = "AVbv"  # MusicBrainz ID (number - but MBIDs are UUIDs, may need to change to rich_text)
LABELS_TYPE_PROPERTY_ID = None  # Type: Label, Distributor, etc. (select) - not in database
LABELS_COUNTRY_PROPERTY_ID = None  # Country (select) - not in database
LABELS_BEGIN_DATE_PROPERTY_ID = None  # Begin date (date) - not in database
LABELS_END_DATE_PROPERTY_ID = None  # End date (date) - not in database
LABELS_DISAMBIGUATION_PROPERTY_ID = None  # Disambiguation (rich text) - not in database
LABELS_DESCRIPTION_PROPERTY_ID = None  # Description (rich text) - not in database
LABELS_GENRES_PROPERTY_ID = "J%5Ev%3B"  # Genre (multi-select)
LABELS_TAGS_PROPERTY_ID = "uRPH"  # Tags (multi-select)
LABELS_RATING_PROPERTY_ID = None  # Rating (number) - not in database
LABELS_MUSICBRAINZ_URL_PROPERTY_ID = "HM%3DA"  # MusicBrainz (url)
LABELS_OFFICIAL_WEBSITE_PROPERTY_ID = "Gym%3F"  # Official Website (url)
LABELS_IG_PROPERTY_ID = "C_Uh"  # IG (url)
LABELS_BANDCAMP_PROPERTY_ID = "%40vep"  # Bandcamp (url)
LABELS_FOUNDED_PROPERTY_ID = "pmui"  # Founded (number - year)
LABELS_LAST_UPDATED_PROPERTY_ID = None  # Last updated (date) - not in database

# Relations
LABELS_ALBUMS_PROPERTY_ID = "pnZJ"  # Albums (relation)
LABELS_AREA_PROPERTY_ID = "%3D%40Hc"  # Area (relation)

# ============================================================================
# FIELD BEHAVIOR CONFIGURATION
# ============================================================================
# Controls how each field is handled during sync
# Options:
#   'default' - Always overwrite with MusicBrainz data (even if empty)
#   'merge'   - Merge MusicBrainz data with existing data (remove duplicates)
#   'preserve' - Only update if MusicBrainz has data (preserve existing if empty)
#   'skip'    - Never update this field

FIELD_BEHAVIOR = {
    # Fields that merge MusicBrainz data with existing data
    'artists_genres_property_id': 'merge',
    'albums_genres_property_id': 'merge',
    'songs_genres_property_id': 'merge',
    'labels_genres_property_id': 'merge',
    
    # Note: Labels are now relations, not multi-select, so they don't need merge behavior
    # Relations are always replaced with the new relation values
    
    # All other fields use default behavior (replace with MusicBrainz data)
}

