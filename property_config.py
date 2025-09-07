# Property Mapping Configuration for IGDb
# This file allows you to specify exact property IDs for maximum stability
# Property IDs never change when you rename properties in Notion

# Required Properties (must be present in your database)
# These are the stable property IDs from your Notion database
TITLE_PROPERTY_ID = "title"  # e.g., "Qjhc" - the actual property ID from Notion

# Additional Properties (will be populated if found)
DESCRIPTION_PROPERTY_ID = "%7D%3D%3Ac"  # e.g., "Efgh" - the actual property ID from Notion
RELEASE_DATE_PROPERTY_ID = "emZj"  # e.g., "Ijkl" - the actual property ID from Notion
RATING_PROPERTY_ID = "L~RS"  # e.g., "Mnop" - the actual property ID from Notion
RATING_COUNT_PROPERTY_ID = None  # e.g., "Qrst" - the actual property ID from Notion
PLAYTIME_PROPERTY_ID = "aZKN"  # e.g., "Uvwx" - the actual property ID from Notion
GENRES_PROPERTY_ID = "%3CYQ%3D"  # e.g., "Cdef" - the actual property ID from Notion
PLATFORMS_PROPERTY_ID = "JiDn"  # e.g., "Ghij" - the actual property ID from Notion
PLATFORM_FAMILY_PROPERTY_ID = "wXCg"  # Platform family (multi-select)
PLATFORM_TYPE_PROPERTY_ID = "SlOc"  # Platform type (multi-select)
STATUS_PROPERTY_ID = None  # e.g., "Klmn" - the actual property ID from Notion
IGDB_ID_PROPERTY_ID = "Nek%7B"  # e.g., "Opqr" - the actual property ID from Notion
COVER_IMAGE_PROPERTY_ID = None  # e.g., "Stuv" - the actual property ID from Notion (for cover images)
LAST_UPDATED_PROPERTY_ID = None  # e.g., "Wxyz" - the actual property ID from Notion

# Extended Properties
DEVELOPERS_PROPERTY_ID = "%40Lyo"  # Game developers (multi-select)
PUBLISHERS_PROPERTY_ID = "Su%5BA"  # Game publishers (multi-select)
FRANCHISE_PROPERTY_ID = "fi_D"  # Game franchise (multi-select)
COLLECTIONS_PROPERTY_ID = "r%7DI%7D"  # Game series/collections (multi-select)
GAME_MODES_PROPERTY_ID = "T%3C%3FM"  # Game modes (multi-select)
GAME_STATUS_PROPERTY_ID = "%3BfF%3C"  # Game status (status)
GAME_TYPE_PROPERTY_ID = "%3A~Wi"  # Game type (multi-select)
MULTIPLAYER_MODES_PROPERTY_ID = "X%3Ao~"  # Multiplayer modes (multi-select)
THEMES_PROPERTY_ID = "%5BT%5Cq"  # Game themes (multi-select)
WEBSITE_PROPERTY_ID = "J%3BP%3D"  # Official website URL
HOMEPAGE_PROPERTY_ID = None  # IGDb homepage URL

# Player Count Properties
OFFLINE_PLAYERS_PROPERTY_ID = "~OxG"  # Max offline players (number)
ONLINE_PLAYERS_PROPERTY_ID = "fzUK"  # Max online players (number)
OFFLINE_COOP_PLAYERS_PROPERTY_ID = "~laL"  # Max offline co-op players (number)
ONLINE_COOP_PLAYERS_PROPERTY_ID = "HUAX"  # Max online co-op players (number)

# Field Behavior Configuration
# Controls how each field is handled during sync
# Options:
#   'default' - Always overwrite with IGDb data (even if empty)
#   'merge'   - Merge IGDb data with existing data (remove duplicates)
#   'preserve' - Only update if IGDb has data (preserve existing if IGDb empty)
#   'skip'    - Never update this field
FIELD_BEHAVIOR = {
    # Fields that merge IGDb data with existing data
    'genres_property_id': 'merge',                    # Merge genre data
    'platforms_property_id': 'merge',                 # Merge platform data
    'platform_family_property_id': 'merge',           # Merge platform family data
    'platform_type_property_id': 'merge',             # Merge platform type data
    'franchise_property_id': 'merge',                 # Merge franchise data
    'collections_property_id': 'merge',               # Merge collections/series data
    'game_modes_property_id': 'merge',               # Merge game modes data
    'multiplayer_modes_property_id': 'merge',        # Merge multiplayer modes data
    'themes_property_id': 'merge',                    # Merge themes data
    
    # Fields that only update if IGDb has data ('preserve')

    
    # All other fields use default behavior (replace with IGDb data)
    # Add more fields here as needed
}
