# Notion IGDb Sync - Deployment Guide

## 🚀 GitHub Actions Deployment

This project includes a single automated GitHub Actions workflow for the unified Games / Music / Movies / Books sync.

### 📋 Prerequisites

1. **GitHub Repository**: Push this code to a GitHub repository
2. **API Credentials**: Obtain the following API keys:
   - Notion Integration Token
   - IGDb Client ID and Secret
   - Notion Database ID

### 🔐 Required GitHub Secrets

Add these secrets to your GitHub repository (`Settings > Secrets and variables > Actions`):

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `NOTION_INTERNAL_INTEGRATION_SECRET` | Notion integration token | `ntn_abc123...` |
| `NOTION_GAMES_DATABASE_ID` | Games DB ID | `2661404745bb80538779c22c298a186b` |
| `NOTION_MOVIETV_DATABASE_ID` | Movies/TV DB ID | `2ab1404745bb81e9bb20e4b1930b6d0f` |
| `NOTION_BOOKS_DATABASE_ID` | Books DB ID | `feedfacebeefdeadbead12345678` |
| `NOTION_ARTISTS_DATABASE_ID` | Music Artists DB ID | `aaaabbbbccccddddeeeeffffffffffff` |
| `NOTION_ALBUMS_DATABASE_ID` | Music Albums DB ID | `11112222333344445555666677778888` |
| `NOTION_SONGS_DATABASE_ID` | Music Songs DB ID | `99990000aaaabbbbccccddddeeeeffff` |
| `NOTION_LABELS_DATABASE_ID` | Music Labels DB ID | `abcd1234abcd1234abcd1234abcd1234` |
| `IGDB_CLIENT_ID` / `IGDB_CLIENT_SECRET` | IGDb API credentials | |
| `TMDB_API_KEY` | TMDb API key | |
| `MUSICBRAINZ_USER_AGENT` | MusicBrainz app name + contact | `MyApp/1.0 (me@example.com)` |
| `GOOGLE_BOOKS_API_KEY` | Google Books API key (optional) | |
| `COMICVINE_API_KEY` | ComicVine API key (optional) | |
| `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` | Optional Spotify enrichments | |

### ⚙️ Workflow Overview

- **File**: `.github/workflows/notion-sync.yml`
- **Dispatch inputs**: `target`, `page_id` (single-page webhook mode), `workers`, `database`, `created_after`, and booleans for `force_icons`, `force_update`, `force_research`, `force_scraping`, `dry_run`
- **Schedule**: No active cron by default; a commented placeholder (`0 0 * * *`) lives inside the workflow so you can uncomment / duplicate when you’re ready for timed runs.
- **Modes**:
  - If `page_id` is provided, the job calls `python3 webhook.py` to auto-route the page’s database.
  - Otherwise it runs `python3 main.py --target <games|music|movies|books>` with the provided flags.

#### Spotify URL Input (Music Syncs)

The `spotify_url` workflow input enables two powerful capabilities:

##### 🆕 1. Create New Pages from Spotify URLs

**No manual page creation needed** - just provide the Spotify URL and the system will:
- Parse the URL to identify type (track/album/artist)
- Fetch metadata from Spotify API
- Search MusicBrainz for additional metadata
- Check for duplicates in Notion
- Create new page with full metadata
- Automatically create related entities:
  - **Track** → creates/links album, artist, label
  - **Album** → creates/links artist, label
  - **Artist** → standalone creation

**Example workflow runs (creation mode):**
```yaml
# Create new track page
page_id: (leave empty)
spotify_url: "https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6"

# Create new album page
page_id: (leave empty)
spotify_url: "https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy"

# Create new artist page
page_id: (leave empty)
spotify_url: "https://open.spotify.com/artist/0OdUWJ0sBjDrqHygGUXeCF"
```

##### 2. Enhance Existing Page Matching

**Improve accuracy** for pages you've already created:
- Uses Spotify API to extract ISRC/UPC/EAN identifiers
- Matches them to MusicBrainz entries (more accurate than name search)
- Falls back to name-based search if external ID not found

**Example workflow runs (update mode):**
```yaml
# Update existing page with Spotify data
page_id: "abc123xyz"
spotify_url: "https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6"
```

**Property behavior:** The "Spotify" property serves dual purpose:
- **Input**: If filled, reads and uses for identification
- **Output**: If empty, writes found URL after sync
- User-provided URLs are preserved (not overwritten)

### 🛠️ Setup Instructions

1. **Create/Clone Repository**
   ```bash
   git clone https://github.com/wobafett/notion-media-sync.git
   cd notion-media-sync
   ```

2. **Configure Secrets**
   - Go to your GitHub repository
   - Navigate to `Settings > Secrets and variables > Actions`
   - Add all required secrets listed above

3. **Enable Workflow**
- Go to the `Actions` tab and enable workflows for the repository

4. **Test Manual Run**
- Click on "Notion Media Sync"
- Choose a `target` (or provide `page_id`) and run the workflow

### 📊 Monitoring

**Workflow Status:**
- Check the `Actions` tab for run history
- View logs for each run
- Download log artifacts for debugging

**Performance Metrics:**
- Full sync: ~4-5 minutes (18 games)
- Single page sync: ~8-10 seconds
- Success rate: 99%+ with optimized rate limiting

### 🔧 Customization

**Schedule Changes:**
Uncomment or duplicate the placeholder cron inside `.github/workflows/notion-sync.yml`:
```yaml
schedule:
  - cron: '0 0 * * *' # Example: run nightly at midnight UTC
```

**Environment Variables:**
Add custom environment variables to workflows:
```yaml
env:
  LOG_LEVEL: DEBUG
  CUSTOM_SETTING: value
```

### 🚨 Troubleshooting

**Common Issues:**

1. **Authentication Errors**
   - Verify all secrets are correctly set
   - Check token permissions in Notion
   - Ensure IGDb credentials are valid

2. **Rate Limiting**
   - Workflows use optimized rate limiting
   - If issues persist, reduce worker count
   - Check IGDb API status

3. **Database Not Found**
   - Verify `NOTION_GAMES_DATABASE_ID` / `NOTION_MOVIETV_DATABASE_ID` is correct
   - Ensure Notion integration has database access
   - Check database permissions

**Debug Steps:**
1. Check workflow logs in GitHub Actions
2. Download log artifacts for detailed analysis
3. Test locally with same credentials
4. Verify API endpoints are accessible

### 📈 Performance Optimization

**Current Optimizations:**
- Adaptive rate limiting (0.3s - 2.0s delays)
- Parallel processing (3 workers default)
- Intelligent caching
- Smart skip logic

**Scaling:**
- Increase workers for larger databases (max 4)
- Adjust schedule frequency based on needs
- Monitor API usage limits

### 🔄 Updates

**Keeping Up to Date:**
- Pull latest changes from repository
- Secrets persist across updates
- Workflows auto-update with code changes

**Version Control:**
- All changes are tracked in Git
- Rollback capability for issues
- Tagged releases for stability

---

## 🎯 Quick Start Checklist

- [ ] Repository created and code pushed
- [ ] Core GitHub secrets configured (`NOTION_INTERNAL_INTEGRATION_SECRET`, relevant `NOTION_*_DATABASE_ID`, API keys)
- [ ] First manual workflow run successful
- [ ] (Optional) Schedule enabled
- [ ] Monitoring plan in place

**Ready to sync!** 🎮✨
