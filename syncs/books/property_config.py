# Property Configuration for Notion Google Books Sync
# Update these property IDs with the actual IDs from your Notion database
# Run find_property_ids.py to get your property IDs

# Core properties (required)
TITLE_PROPERTY_ID = "title"  # Name (title property)

# API-specific ID fields
GOOGLE_BOOKS_ID_PROPERTY_ID = "M%60Tt"  # Google Books ID (rich_text property)
JIKAN_ID_PROPERTY_ID = "hqg%5E"  # Jikan ID (rich_text property)
COMICVINE_ID_PROPERTY_ID = "L%7CON"  # ComicVine ID (rich_text property)
WOOKIEEPEDIA_ID_PROPERTY_ID = "lPt~"  # Wookieepedia ID (rich_text property)

# Basic book information
AUTHORS_PROPERTY_ID = "hN~X"  # Author (multi_select property)
ARTISTS_PROPERTY_ID = "%3ENY%5B"  # Artists (multi_select property)
COVER_ARTISTS_PROPERTY_ID = "gxQu"  # Cover Artist(s) (multi_select property)
DESCRIPTION_PROPERTY_ID = "vfSJ"  # Description (rich_text property)
PUBLICATION_DATE_PROPERTY_ID = "~%5Cs%3C"  # Publish Start (date property)
PUBLICATION_END_DATE_PROPERTY_ID = "bQMA"  # Publish End (date property)
PUBLISHER_PROPERTY_ID = "OIIv"  # Publisher (multi_select property)
PAGE_COUNT_PROPERTY_ID = "%3B%3EaC"  # Page Count (number property)
LANGUAGE_PROPERTY_ID = "WZMD"  # Language (multi_select property)
CHAPTERS_PROPERTY_ID = "G_Yd"  # Chapters (number property)
VOLUMES_PROPERTY_ID = "P%3C%5Ef"  # Volumes (number property)
STATUS_PROPERTY_ID = "xJRW"  # Status (select property)
SW_TIMELINE_PROPERTY_ID = "LwF%5C"  # SW Timeline (rich_text property)
SERIES_PROPERTY_ID = "s%7C%7Bf"  # Series (multi_select property)
COMIC_FORMAT_PROPERTY_ID = "AqgX"  # Comic Format (select property)
FOLLOWED_BY_PROPERTY_ID = "%7CDF%5C"  # Followed by (rich_text property)

# Identifiers
ISBN_PROPERTY_ID = "gYZ%3B"  # ISBN (number property)

# Ratings and reviews
RATING_PROPERTY_ID = "M_Qj"  # Rating (number property)
RATING_COUNT_PROPERTY_ID = None  # Rating Count property not found in your database

# Categories and classification
CATEGORIES_PROPERTY_ID = "Lmtv"  # Categories (multi_select property)
CONTENT_RATING_PROPERTY_ID = "C_Z%5E"  # Maturity Rating (multi_select property)
BOOK_TYPE_PROPERTY_ID = "IfLR"  # Print Type (multi_select property)
TYPE_PROPERTY_ID = "ERZ%3E"  # Type (select property) - Book, Manga, Comic

# Additional information
SUBTITLE_PROPERTY_ID = "rYiG"  # Subtitle (rich_text property)
COVER_IMAGE_PROPERTY_ID = None  # Cover Image property not found in your database
GOOGLE_BOOKS_URL_PROPERTY_ID = "KbZ%5D"  # Info (url property)
LAST_UPDATED_PROPERTY_ID = None  # Last Updated property not found in your database

# Field behavior configuration
# Controls how multi-select fields are handled during sync
FIELD_BEHAVIOR = {
    # Fields that merge Google Books data with existing data
    #'authors_property_id': 'merge',                    # Merge author data
    #'categories_property_id': 'merge',                 # Merge category data
    #'publisher_property_id': 'merge',                  # Merge publisher data
    
    # Fields that only update if Google Books has data
    #'subtitle_property_id': 'preserve',               # Only update if Google Books has subtitle
    #'content_rating_property_id': 'preserve',         # Only update if Google Books has content rating
    
    # All other fields use default behavior (replace with Google Books data)
}

# Behavior options:
# 'default': Always overwrite with Google Books data (even if empty)
# 'merge': Merge Google Books data with existing data (remove duplicates)
# 'preserve': Only update if Google Books has data (preserve existing if Google Books empty)
# 'skip': Never update this field
