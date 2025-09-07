#!/bin/bash
# GitHub Repository Setup Script for Notion IGDb Sync

echo "🚀 Notion IGDb Sync - GitHub Repository Setup"
echo "=============================================="

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "❌ Not a git repository. Please run 'git init' first."
    exit 1
fi

echo "📋 Next Steps:"
echo ""
echo "1. Create a new GitHub repository:"
echo "   - Go to https://github.com/new"
echo "   - Name: notion-igdb-sync (or your preferred name)"
echo "   - Description: Automated Notion-IGDb synchronization with GitHub Actions"
echo "   - Make it Private (recommended for API keys)"
echo ""
echo "2. Add the remote origin:"
echo "   git remote add origin https://github.com/YOUR_USERNAME/notion-igdb-sync.git"
echo ""
echo "3. Push the code:"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo "4. Configure GitHub Secrets:"
echo "   Go to Settings > Secrets and variables > Actions"
echo "   Add these secrets:"
echo "   - NOTION_TOKEN"
echo "   - IGDB_CLIENT_ID" 
echo "   - IGDB_CLIENT_SECRET"
echo "   - NOTION_DATABASE_ID"
echo ""
echo "5. Enable GitHub Actions:"
echo "   Go to Actions tab and enable workflows"
echo ""
echo "6. Test the setup:"
echo "   Run 'Notion IGDb Single Page Sync' workflow manually"
echo ""
echo "📚 For detailed instructions, see DEPLOYMENT.md"
echo ""
echo "🎉 Ready to deploy!"
