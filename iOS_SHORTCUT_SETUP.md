# iOS Shortcut Setup for Sync Last Page

## 🍎 **iOS Shortcut Configuration**

### **Method 1: GitHub API (Recommended)**

1. **Create New Shortcut:**
   - Open Shortcuts app on iOS
   - Tap "+" to create new shortcut
   - Name it "Sync Last Page"

2. **Add GitHub API Action:**
   - Search for "Get Contents of URL"
   - Add this action

3. **Configure the Action:**
   ```
   URL: https://api.github.com/repos/wobafett/Notion-IGDB-Sync/actions/workflows/sync_last_page.yml/dispatches
   Method: POST
   Headers:
     - Accept: application/vnd.github+json
     - Authorization: Bearer YOUR_GITHUB_TOKEN
     - X-GitHub-Api-Version: 2022-11-28
   Body: {"ref":"main"}
   ```

4. **Get GitHub Token:**
   - Go to https://github.com/settings/tokens
   - Generate new token (classic)
   - Select scopes: `repo` and `workflow`
   - Copy the token

### **Method 2: Webhook (Alternative)**

1. **Create Webhook in GitHub:**
   - Go to your repository Settings
   - Webhooks → Add webhook
   - Payload URL: Your webhook service URL
   - Content type: application/json
   - Events: Workflow runs

2. **Use Webhook Service:**
   - Services like Zapier, IFTTT, or custom webhook
   - Trigger GitHub workflow via webhook

### **Method 3: Simple GitHub URL (Easiest)**

1. **Create Shortcut:**
   - Open Shortcuts app
   - Add "Open URLs" action
   - URL: `https://github.com/wobafett/Notion-IGDB-Sync/actions/workflows/sync_last_page.yml`

2. **Add to Home Screen:**
   - Tap share button in Shortcuts
   - "Add to Home Screen"
   - This opens GitHub Actions page for manual trigger

## 🔧 **Detailed Setup (Method 1)**

### **Step 1: Get GitHub Token**
1. Go to https://github.com/settings/tokens
2. Click "Generate new token" → "Generate new token (classic)"
3. Name: "iOS Shortcut Sync"
4. Expiration: 90 days (or your preference)
5. Select scopes:
   - ✅ `repo` (Full control of private repositories)
   - ✅ `workflow` (Update GitHub Action workflows)
6. Click "Generate token"
7. **Copy the token immediately** (you won't see it again)

### **Step 2: Create Shortcut**
1. Open Shortcuts app
2. Tap "+" → "Add Action"
3. Search for "Get Contents of URL"
4. Configure:
   - **URL:** `https://api.github.com/repos/wobafett/Notion-IGDB-Sync/actions/workflows/sync_last_page.yml/dispatches`
   - **Method:** POST
   - **Headers:** 
     - `Accept`: `application/vnd.github+json`
     - `Authorization`: `Bearer YOUR_TOKEN_HERE`
     - `X-GitHub-Api-Version`: `2022-11-28`
   - **Request Body:** `{"ref":"main"}`

### **Step 3: Add Success Feedback**
1. Add "Show Result" action
2. Set text: "Sync Last Page triggered! Check GitHub Actions for status."

### **Step 4: Add to Home Screen**
1. Tap share button in Shortcuts
2. "Add to Home Screen"
3. Choose icon and name
4. Tap "Add"

## 🎯 **Usage**

### **From Home Screen:**
- Tap the shortcut icon
- Confirmation dialog appears
- Tap "Run"
- Success message shows

### **From Shortcuts App:**
- Open Shortcuts app
- Tap "Sync Last Page"
- Run the shortcut

### **From Control Center:**
- Add shortcut to Control Center
- Access from anywhere on iOS

## 🔍 **Troubleshooting**

### **Common Issues:**

1. **"Repository not found"**
   - Check repository name in URL
   - Ensure token has `repo` scope

2. **"Workflow not found"**
   - Verify workflow file exists: `sync_last_page.yml`
   - Check workflow is enabled in GitHub Actions

3. **"Authentication failed"**
   - Verify token is correct
   - Check token hasn't expired
   - Ensure token has `workflow` scope

4. **"Shortcut not working"**
   - Test URL in Safari first
   - Check internet connection
   - Verify GitHub API status

### **Testing:**

1. **Test in Safari:**
   ```
   https://api.github.com/repos/wobafett/Notion-IGDB-Sync/actions/workflows/sync_last_page.yml/dispatches
   ```

2. **Check GitHub Actions:**
   - Go to Actions tab
   - Look for "Sync Last Page" workflow runs
   - Verify it triggers successfully

## 🚀 **Advanced Features**

### **Add Confirmation Dialog:**
1. Add "Ask for Input" action
2. Question: "Sync the last edited game?"
3. Input Type: Text
4. Default Answer: "Yes"

### **Add Status Check:**
1. After triggering, check workflow status
2. Show success/failure message
3. Display last sync time

### **Add to Siri:**
1. In Shortcuts app, tap shortcut
2. Tap "Add to Siri"
3. Record phrase: "Sync last page"
4. Say "Hey Siri, sync last page"

## 📱 **iOS Requirements**

- **iOS Version:** 13.0 or later
- **Shortcuts App:** Pre-installed or download from App Store
- **Internet Connection:** Required for GitHub API calls
- **GitHub Account:** With repository access

---

## 🎮 **Quick Start Summary**

1. **Get GitHub token** with `repo` and `workflow` scopes
2. **Create shortcut** with "Get Contents of URL" action
3. **Configure URL** to trigger GitHub workflow
4. **Add headers** with authorization token
5. **Test and add to home screen**

**Perfect for quick game updates on the go!** 🎮✨
