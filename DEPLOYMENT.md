# Notion IGDb Sync - Deployment Guide

## 🚀 GitHub Actions Deployment

This project includes automated GitHub Actions workflows for seamless Notion-IGDb synchronization.

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
| `NOTION_TOKEN` | Notion integration token | `secret_abc123...` |
| `IGDB_CLIENT_ID` | IGDb API client ID | `abc123def456` |
| `IGDB_CLIENT_SECRET` | IGDb API client secret | `xyz789uvw012` |
| `NOTION_DATABASE_ID` | Notion database ID | `2661404745bb80538779c22c298a186b` |

### ⚙️ Workflows

#### 1. Notion IGDb Sync (`notion_igdb_sync.yml`)

**Triggers:**
- **Scheduled**: Every 6 hours automatically
- **Manual**: On-demand via GitHub Actions tab

**Features:**
- Syncs all pages in your Notion database
- Configurable parallel workers (1-4)
- Optional force update modes
- Comprehensive logging

**Manual Run Options:**
- `force_all`: Update all pages including completed content
- `force_icons`: Force update all page icons
- `workers`: Number of parallel workers (default: 3)

#### 2. Sync Last Page (`sync_last_page.yml`)

**Triggers:**
- **Scheduled**: Every 30 minutes automatically
- **Manual**: On-demand via GitHub Actions tab

**Features:**
- Syncs only the most recently edited page
- Ultra-fast processing (optimized for single page)
- Perfect for iOS shortcuts integration
- Minimal resource usage

### 🛠️ Setup Instructions

1. **Fork/Clone Repository**
   ```bash
   git clone https://github.com/yourusername/notion-igdb-sync.git
   cd notion-igdb-sync
   ```

2. **Configure Secrets**
   - Go to your GitHub repository
   - Navigate to `Settings > Secrets and variables > Actions`
   - Add all required secrets listed above

3. **Enable Workflows**
   - Go to `Actions` tab in your repository
   - Both workflows should be visible and ready to run

4. **Test Manual Run**
   - Click on "Sync Last Page"
   - Click "Run workflow"
   - Verify it completes successfully

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
Edit the `cron` expressions in the workflow files:
```yaml
schedule:
  - cron: '0 */6 * * *'  # Every 6 hours
  - cron: '*/30 * * * *' # Every 30 minutes
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
   - Verify `NOTION_DATABASE_ID` is correct
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
- [ ] All 4 GitHub secrets configured
- [ ] First manual run successful
- [ ] Scheduled runs enabled
- [ ] Monitoring setup complete

**Ready to sync!** 🎮✨
