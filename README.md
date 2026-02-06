# Notion IGDb Sync

A Python script that synchronizes video game data from the Internet Game Database (IGDb) to your Notion database. Perfect for maintaining an up-to-date gaming collection with rich metadata, platform information, and developer details.

## ✨ Features

- **Automatic Data Sync**: Keeps your Notion database updated with latest IGDb information
- **Rich Metadata**: Includes genres, platforms, developers, publishers, ratings, and more
- **Cover Images**: Automatically updates page covers with game artwork
- **Platform Support**: Tracks which platforms each game is available on
- **Developer/Publisher Info**: Shows who made and published each game
- **Efficient Updates**: Only updates changed data to minimize API calls
- **Cloud Ready**: Easy deployment to cloud platforms

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- IGDb API credentials ([Get them here](https://api.igdb.com/))
- Notion integration token ([Create one here](https://www.notion.so/my-integrations))

### 🎯 Deployment Options

**Option 1: GitHub Actions (Recommended)**
- Automated syncing with GitHub Actions
- Scheduled runs every 6 hours (full sync) and 30 minutes (single page)
- Manual triggers with custom options
- See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed setup

**Option 2: Local Development**
- Run scripts locally for testing and development
- Perfect for custom configurations
- Notion API token ([Get one here](https://www.notion.so/my-integrations))
- Notion database with proper properties

### Installation

1. **Clone or download the project**
   ```bash
   cd "Notion IGDb"
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your API keys and database ID
   ```

4. **Configure your Notion database**
   - Run `python find_property_ids.py` to get your property IDs
   - Update `property_config.py` with your property IDs

5. **Run the sync**
   ```bash
   python notion_igdb_sync.py
   ```

## 🎛 Unified Entry Points

This repository now hosts the combined sync logic for **Games**, **Music**, **Movies/TV**, and **Books**. All targets share the same CLI and webhook router:

- `python3 main.py --target games|music|movies|books [options]`
- `python3 webhook.py --page-id <notion_page_id> [options]` (auto-routes the page to the correct target)
- Backwards-compatible shims (`notion_igdb_sync.py`, `notion_musicbrainz_sync.py`, etc.) simply call `main.py` with their default targets.

### GitHub Actions workflow

The repo includes `.github/workflows/notion-sync.yml`, which exposes a manual `workflow_dispatch` form:

- Required input: `target` (games/music/movies/books)
- Optional inputs: `page_id` (single page mode via webhook), `workers`, `database`, `created_after`, and the standard flags `force_icons`, `force_all`, `force_update`, `force_research`, `force_scraping`, `dry_run`
- Secrets: set `NOTION_INTERNAL_INTEGRATION_SECRET`, the relevant `NOTION_*_DATABASE_ID` values, and API keys (IGDB, TMDb, MusicBrainz, Google Books, ComicVine, etc.) in the repository settings
- A placeholder cron entry is commented out inside the workflow—uncomment or duplicate it when you’re ready for scheduled runs.

Manual dispatch lets you test the router/webhook end-to-end in the cloud without needing four separate workflows or repos.

### Spotify URL Input (Music Syncs Only)

For music syncs, you can now provide Spotify URLs for two purposes:
1. **Create new pages** directly from Spotify URLs (no manual page creation needed)
2. **Improve matching accuracy** for existing pages

**🆕 Create New Pages from Spotify URLs:**
```bash
# Create a new song page (and related album, artist, label pages if needed)
python3 webhook.py --spotify-url "https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6"

# Create a new album page (and related artist, label pages if needed)
python3 webhook.py --spotify-url "https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy"

# Create a new artist page
python3 webhook.py --spotify-url "https://open.spotify.com/artist/0OdUWJ0sBjDrqHygGUXeCF"
```

**How Creation Works:**
1. Parses Spotify URL to identify type (track/album/artist)
2. Fetches metadata from Spotify API
3. Extracts external IDs (ISRC/UPC) and searches MusicBrainz
4. Checks for duplicates in Notion (by MBID or Spotify URL)
5. Creates new page or updates existing one
6. Automatically creates related entities:
   - **Track** → creates/links album, artist, label
   - **Album** → creates/links artist, label
   - **Artist** → standalone creation

**Enhance Existing Pages:**
```bash
# Update existing page with Spotify URL
python3 webhook.py --page-id <page_id> --spotify-url "https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6"
```

**Matching Process:**
- **Songs**: Extracts ISRC from Spotify → searches MusicBrainz by ISRC
- **Albums**: Extracts UPC/EAN barcode → searches MusicBrainz by barcode
- **Artists**: Extracts Spotify ID → searches MusicBrainz by Spotify relationship

**Dual-Purpose Property:**
- If "Spotify" field is filled → reads and uses for identification
- If "Spotify" field is empty → writes the found URL back after syncing
- If URL was provided, it's preserved (not overwritten)

**Fallback Behavior:**
- If ISRC/UPC not found → falls back to name-based search
- Works seamlessly with existing sync logic

## 📋 Setup Guide

### 1. Environment Variables

Create a `.env` file with the following variables:

```env
# IGDb API Configuration
IGDB_CLIENT_ID=your_igdb_client_id_here
IGDB_CLIENT_SECRET=your_igdb_client_secret_here

# Notion API Configuration
NOTION_INTERNAL_INTEGRATION_SECRET=your_notion_integration_token_here
NOTION_GAMES_DATABASE_ID=your_games_database_id_here
NOTION_MOVIETV_DATABASE_ID=your_movies_tv_database_id_here
NOTION_BOOKS_DATABASE_ID=your_books_database_id_here

# Optional: Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### 2. IGDb API Setup

1. Go to [IGDb API](https://api.igdb.com/) and create an account
2. Create a new application to get your Client ID and Client Secret
3. Add these credentials to your `.env` file

### 3. Notion Database Setup

Your Notion database should have these properties (you can customize the names):

| Property Name | Type | Description |
|---------------|------|-------------|
| Name | Title | Game title |
| Description | Rich Text | Game summary |
| Release Date | Date | Release date |
| Rating | Number | IGDb rating (0-1 scale) |
| Rating Count | Number | Number of ratings |
| Playtime | Number | Time to beat in hours |
| Genres | Multi-select | Genre tags |
| Platforms | Multi-select | Available platforms |
| Status | Status | Current status |
| IGDb ID | Number | IGDb identifier |
| Developers | Multi-select | Game developers |
| Publishers | Multi-select | Game publishers |
| IGDb | URL | IGDb page link |

### 4. Property Configuration

The repository includes a working `property_config.py` file with example property IDs. You need to update these with your own database's property IDs.

**Step 1: Get your property IDs**
```bash
python find_property_ids.py
```

**Step 2: Update the configuration**
Edit `property_config.py` and replace the example property IDs with your actual ones from the script output.

**Note:** Property IDs are not sensitive data - they're just identifiers for your database fields. You can safely commit your `property_config.py` file to your repository.

#### Field Behavior Configuration

The script supports four different behaviors for how multi-select fields are handled during sync:

```python
# Field Behavior Configuration
FIELD_BEHAVIOR = {
    # Fields that merge IGDb data with existing data
    'genres_property_id': 'merge',                    # Merge genre data
    'platforms_property_id': 'merge',                 # Merge platform data
    'developers_property_id': 'merge',                # Merge developer data
    'publishers_property_id': 'merge',                # Merge publisher data
    
    # Fields that only update if IGDb has data
    'game_modes_property_id': 'preserve',            # Only update if IGDb has game modes
    'themes_property_id': 'preserve',                 # Only update if IGDb has themes
    
    # All other fields use default behavior (replace with IGDb data)
}
```

**Behavior Options:**
- **`'default'`**: Always overwrite with IGDb data (even if empty)
- **`'merge'`**: Merge IGDb data with existing data (remove duplicates)
- **`'preserve'`**: Only update if IGDb has data (preserve existing if IGDb empty)
- **`'skip'`**: Never update this field

## ⚙️ Configuration

### Property Mapping

The script uses property IDs for robust mapping. Edit `property_config.py` to match your database:

```python
# Core properties (required)
TITLE_PROPERTY_ID = "title"
IGDB_ID_PROPERTY_ID = "KEtB"

# Optional properties (set to None to skip)
RATING_PROPERTY_ID = None
PLAYTIME_PROPERTY_ID = None
# ... etc
```

### Customization

- **Add new fields**: Add property IDs to `property_config.py` and implement the logic in `notion_igdb_sync.py`
- **Change update frequency**: Modify the script to run on your preferred schedule
- **Filter content**: Add filters to only sync specific types of games

## 📊 Usage Examples

### Basic Sync
```bash
python notion_igdb_sync.py
```

### With Custom Workers
```bash
python notion_igdb_sync.py --workers 3
```

### Force Icon Updates
```bash
python notion_igdb_sync.py --force-icons
```

### Scheduled Runs
```bash
# Add to crontab for daily updates
0 2 * * * cd /path/to/notion-igdb-sync && python notion_igdb_sync.py
```

## 🔧 Troubleshooting

### Common Issues

1. **401 Unauthorized**: Check your API keys in `.env`
2. **Property not found**: Run `find_property_ids.py` to get correct IDs
3. **Rate limiting**: The script includes built-in rate limiting
4. **Missing data**: Some games may not have complete IGDb data

### Debug Mode

Enable debug logging by modifying the script:

```python
logging.basicConfig(level=logging.DEBUG)
```

Or set in your `.env` file:
```env
LOG_LEVEL=DEBUG
```

## 📈 Performance

- **Efficient**: Only updates changed data
- **Rate Limited**: Intelligent adaptive rate limiting (0.3s - 2.0s delays)
- **Batch Processing**: Handles large databases
- **Error Handling**: Robust error recovery
- **Parallel Processing**: Configurable workers (1-4) for faster sync
- **Caching**: Comprehensive caching reduces redundant API calls

## 🤖 GitHub Actions

This project includes two automated workflows:

### Notion IGDb Sync Workflow
- **Schedule**: Every 6 hours
- **Manual**: On-demand with custom options
- **Features**: Syncs all pages, configurable workers, force modes

### Sync Last Page Workflow  
- **Schedule**: Every 30 minutes
- **Manual**: On-demand for quick updates
- **Features**: Ultra-fast single page processing, perfect for iOS shortcuts
- **iOS Integration**: See [iOS_SHORTCUT_SETUP.md](iOS_SHORTCUT_SETUP.md) for iPhone/iPad setup

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete setup instructions.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [IGDb](https://www.igdb.com/) for the comprehensive video game database
- [Notion](https://www.notion.so/) for the flexible database platform
- The Python community for excellent libraries

## 📞 Support

If you encounter issues:

1. Check the troubleshooting section
2. Review the configuration guide
3. Open an issue on GitHub
4. Check the logs for error details

---

**Happy gaming! 🎮**
# Notion-IGDB-Sync
