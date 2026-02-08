# Property Mapping Configuration
# This file allows you to specify exact property IDs for maximum stability
# Property IDs never change when you rename properties in Notion

# Required Properties (must be present in your database)
# These are the stable property IDs from your Notion database
TITLE_PROPERTY_ID = "title"  # e.g., "Qjhc" - the actual property ID from Notion
CONTENT_TYPE_PROPERTY_ID = "%40i%7Cq"  # e.g., "Abcd" - the actual property ID from Notion

# Additional Properties (will be populated if found)
DESCRIPTION_PROPERTY_ID = "d%60vn"  # e.g., "Efgh" - the actual property ID from Notion
RELEASE_DATE_PROPERTY_ID = "S%3Bu%3A"  # e.g., "Ijkl" - the actual property ID from Notion
RATING_PROPERTY_ID = "vfDu"  # e.g., "Mnop" - the actual property ID from Notion
VOTE_COUNT_PROPERTY_ID = None  # e.g., "Qrst" - the actual property ID from Notion
RUNTIME_PROPERTY_ID = None  # e.g., "Uvwx" - the actual property ID from Notion
SEASONS_PROPERTY_ID = "Z~qp"  # e.g., "Yzab" - the actual property ID from Notion
GENRES_PROPERTY_ID = "Qjhc"  # e.g., "Cdef" - the actual property ID from Notion
STATUS_PROPERTY_ID = "F%3EH%60"  # e.g., "Ghij" - the actual property ID from Notion
TMDB_ID_PROPERTY_ID = "KEtB"  # e.g., "Klmn" - the actual property ID from Notion
LAST_UPDATED_PROPERTY_ID = None  # e.g., "Stuv" - the actual property ID from Notion
EPISODES_PROPERTY_ID = "%7DaY%5B"  # Number of episodes (TV shows)
WEBSITE_PROPERTY_ID = None  # Official website URL
HOMEPAGE_PROPERTY_ID = "wISg"  # TMDb homepage URL
CAST_PROPERTY_ID = "HJZH"  # Main cast members (multi-select)
DIRECTOR_PROPERTY_ID = "BAMb"  # Director(s) (Movies, multi-select)
CREATOR_PROPERTY_ID = "BAMb"  # Creator(s) (TV, multi-select)
PRODUCTION_COMPANIES_PROPERTY_ID = "AHPi"  # Production companies (multi-select)
BUDGET_PROPERTY_ID = None  # Production budget (number)
REVENUE_PROPERTY_ID = None  # Box office revenue (number)
ORIGINAL_LANGUAGE_PROPERTY_ID = "dd%7Cp"  # Original language (select)
PRODUCTION_COUNTRIES_PROPERTY_ID = "LU%3E%5C"  # Production countries (multi-select)
TAGLINE_PROPERTY_ID = "S~w%40"  # Movie tagline (rich text)
POPULARITY_PROPERTY_ID = None  # TMDb popularity score (number)
RUNTIME_MINUTES_PROPERTY_ID = "mIn~"  # Runtime in minutes (number)
ADULT_CONTENT_PROPERTY_ID = None  # Adult content flag (checkbox)
WATCH_PROVIDERS_PROPERTY_ID = "wQIc"  # Where to watch (multi-select)
RELEASED_EPISODES_PROPERTY_ID = "v%3F%7Ci"  # Last released episode number (number)
NEXT_EPISODE_PROPERTY_ID = "Szt%5B"  # Next episode air date (date)
COLLECTION_PROPERTY_ID = "TW%5DE"  # Collection (multi-select)

# Field Behavior Configuration
# Controls how each field is handled during sync
# Options:
#   'default' - Always overwrite with TMDb data (even if empty)
#   'merge'   - Merge TMDb data with existing data (remove duplicates)
#   'preserve' - Only update if TMDb has data (preserve existing if TMDb empty)
#   'skip'    - Never update this field
FIELD_BEHAVIOR = {
    # Fields that merge TMDb data with existing data
#    'cast_property_id': 'merge',                    # Merge cast data
    'genres_property_id': 'merge',                  # Merge genre data
 #   'production_companies_property_id': 'merge',    # Merge company data
    'collection_property_id': 'merge',              # Merge collection data
    
    # Fields that only update if TMDb has data
 #   'director_property_id': 'preserve',             # Only update if TMDb has directors
 #   'creator_property_id': 'preserve',             # Only update if TMDb has creators
    
    # Fields that always overwrite (default behavior)
    # 'watch_providers_property_id': 'default',     # Always overwrite watch providers
    
    # All other fields use default behavior (replace with TMDb data)
    # Add more fields here as needed
}
