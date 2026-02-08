# Make.com Setup Guide - Notion Webhook to GitHub Actions

This guide shows you how to automatically trigger GitHub Actions when pages are created or updated in Notion using Make.com (formerly Integromat).

## Overview

1. **Notion Integration** → Creates webhook subscription
2. **Make.com Scenario** → Receives webhooks and triggers GitHub Actions
3. **GitHub Actions** → Runs the sync workflow for the specific page

## Prerequisites

1. **Make.com Account** - Sign up at https://www.make.com (free tier available)
2. **GitHub Personal Access Token** with `repo` scope
   - Go to: https://github.com/settings/tokens/new
   - Name: "Make.com - Notion Webhook"
   - Scope: `repo` (full control of private repositories)
   - Generate and copy the token

3. **Notion Integration** - Already set up (you're using it for the sync script)

## Step 1: Create Notion Webhook Subscription

1. **Go to Notion Integrations**
   - Visit: https://www.notion.so/my-integrations
   - Select your integration

2. **Create Webhook Subscription**
   - Click the **"Webhooks"** tab
   - Click **"+ Create a subscription"**

3. **Configure the Subscription**
   - **Webhook URL**: We'll get this from Make.com in Step 2
   - **Events**: 
     - ✅ `page.created` (optional - see note below)
     - ✅ `page.updated` (recommended - fires when title is added)
   - **Databases**: Select your games database
   - Click **"Create subscription"** (you can update the URL later)
   
   **Note on Events:**
   - `page.created` fires immediately when a page is created (title might be empty)
   - `page.updated` fires when properties change (including when title is added)
   - **Recommended**: Use only `page.updated` - it will fire when the user adds a title
   - **Alternative**: Use both, but add a filter in Make.com to only process if title exists

## Step 2: Create Make.com Scenario

1. **Create New Scenario**
   - Go to: https://www.make.com
   - Click **"Create a new scenario"**

2. **Add Webhook Module**
   - Click **"+"** to add a module
   - Search for **"Webhooks"** or **"HTTP"**
   - Select **"Webhooks"** → **"Custom webhook"** (or **"HTTP"** → **"Custom webhook"**)
   - Click **"Add"** to create a new webhook
   - **Webhook name**: `notion-games-webhook`
   - Click **"Save"**

3. **Get the Webhook URL**
   - The webhook module is waiting for incoming requests (that's normal)
   - **To find the URL**:
     - Click on the webhook module to open its settings
     - Look for a section showing the webhook details
     - The URL should be displayed somewhere in the module configuration
     - It will look like: `https://hook.make.com/xxxxxxxxxxxxxxxxxxxxxxxxxxxxx`
   - **If you still don't see it**:
     - Try clicking "Show webhook address" or similar button in the module
     - Check if there's a "Copy webhook URL" button
     - The URL might be in a collapsed section - expand all sections
   - **Alternative**: Use the webhook's unique identifier
     - Some Make.com webhooks show an ID that you can construct the URL from
     - Format: `https://hook.make.com/[webhook-id]`
   - **Copy this URL** - you'll need it for Notion

4. **Handle Notion Webhook Verification**
   - Notion will send a verification request to your webhook
   - You need to return the challenge token
   - **Add a Router after the webhook module**:
     - Click **"+"** after the webhook module
     - Search for **"Router"** and add it
   - **Add a filter for verification requests**:
     - In the router, add a filter
     - Condition: `{{1.type}}` equals `subscription.verification`
     - If true, add an **"HTTP Response"** module:
       - Search for **"HTTP"** → **"Return a response"**
       - **Status**: `200`
       - **Response Body**: `{{1.challenge}}`
       - This returns the challenge token to Notion
   - **Add another filter for regular webhooks**:
     - Condition: `{{1.type}}` does not equal `subscription.verification`
     - This is where your normal webhook processing goes (HTTP request to GitHub)

5. **Update Notion Webhook URL**
   - Go back to Notion webhook settings (from Step 1)
   - Paste the Make.com webhook URL into the **"Webhook URL"** field
   - Save the subscription
   - Notion will send a verification request
   - Make.com should automatically return the challenge (if configured above)

6. **Add Router to Filter by Title and IGDb ID (Save Credits)**
   - Add a Router right after the webhook module (in the regular webhooks route)
   - **Filter 1: Title exists AND IGDb ID is empty**
     - Check if page has a title (to avoid processing empty pages)
     - Check if IGDb ID field is empty (to avoid syncing pages that already have IGDb ID)
     - **If webhook includes properties**: Check `{{1.data.properties.[TITLE_FIELD].title}}` exists and `{{1.data.properties.[IGDB_ID_FIELD].number}}` is empty
     - **If webhook doesn't include properties**: You'll need "Get a page" first (see step 7)
   - **Filter 2: Title missing OR IGDb ID exists**
     - Stop/End - don't process (saves credits and GitHub Actions runs)
   
   **Why this helps:**
   - If you use `page.created`, this filters out pages without titles
   - If you use `page.updated`, this ensures you only sync pages that need syncing
   - Saves Make.com credits by filtering early

7. **Check Webhook Payload for IGDb ID (Save Credits)**
   - First, check if the webhook payload includes property data
   - Click the webhook module and check the output structure
   - Look for: `{{1.data.properties}}` or similar
   - **If IGDb ID is in webhook payload**: You can filter in the router above without API calls
   - **If IGDb ID is NOT in webhook payload**: You'll need to fetch the page (see below)

8. **Add Notion "Get a Page" Module (Only if needed)**
   - **Only add this if webhook doesn't include IGDb ID data**
   - Click **"+"** after the webhook/router module
   - Search for **"Notion"**
   - Select **"Get a page"** or **"Retrieve a page"**
   - Configure:
     - **Page ID**: `{{1.data.id}}` (the page ID from the webhook)
   - This fetches the full page data including properties (uses 1 credit)

9. **Add Router to Check IGDb ID (If using "Get a page")**
   - Click **"+"** after the "Get a page" module
   - Search for **"Router"** and add it
   - **Add a filter for "IGDb ID is empty"**:
     - Condition: Check if the IGDb ID field is empty
     - The IGDb ID field path depends on your property name:
       - Example: `{{2.properties.IGDb ID.number}}` (or similar)
     - Condition: Field is empty OR value is null
   - **Route 1 (IGDb ID empty)**: Continue to HTTP request
   - **Route 2 (IGDb ID exists)**: Stop/End (don't trigger sync - saves GitHub Actions runs)

10. **Add HTTP Module (GitHub API)**
    - Click **"+"** after the router (in the "IGDb ID empty" route)
    - Search for **"HTTP"**
    - Select **"Make an HTTP Request"**
    - Configure:
      - **Method**: `POST`
      - **URL**: `https://api.github.com/repos/wobafett/notion-media-sync/actions/workflows/notion-sync.yml/dispatches`
      - **Headers** (add each header separately):
        - `Authorization`: `Bearer YOUR_GITHUB_TOKEN`
          - Replace `YOUR_GITHUB_TOKEN` with your actual GitHub Personal Access Token
        - `Accept`: `application/vnd.github+json`
        - `X-GitHub-Api-Version`: `2022-11-28`
        - `Content-Type`: `application/json`
      - **Body Type**: `Raw` (make sure Content-Type header is set to `application/json`)
     - **Request Content**: 
       ```
       {"event_type":"sync","client_payload":{"page_id":"{{1.data.id}}","force_update":false}}
       ```
      - **Important**: 
       - Use a single-line JSON (no line breaks)
       - `page_id`: `{{1.data.id}}` (the Notion page that triggered the webhook)
       - **Note**: You can also pass a full Notion page URL instead of just the page ID - the system automatically extracts the ID from URLs
       - Make sure there are no extra spaces or formatting
      - **Alternative (if using variables)**: If Make.com supports JSON body builder:
        - `event_type`: `sync`
        - `client_payload`: (object)
         - `page_id`: `{{1.data.id}}` (the Notion page that triggered the webhook)
         - `force_update`: `false`

> ⚠️ **Important:** Repository-dispatch runs without a `page_id` will abort immediately to avoid accidental full-database syncs. Always include the page ID when available.

## Step 3: Test the Scenario

### Testing Steps:

1. **Turn on the scenario** in Make.com
   - Make sure the scenario is **"On"** (toggle switch at the top)

2. **Create or update a page** in your Notion games database
   - Go to your games database
   - Create a new page or edit an existing one
   - Make a small change (add text, change a property, etc.)

3. **Check Make.com execution logs**
   - In Make.com, go to your scenario
   - Click on **"Executions"** or **"History"** tab
   - You should see a new execution appear (may take a few seconds)
   - Click on the execution to see details:
     - **Webhook module**: Should show data received from Notion
     - **HTTP module**: Should show the request to GitHub
     - Look for any errors (red indicators)

4. **Check GitHub Actions**
   - Go to: https://github.com/wobafett/notion-media-sync/actions
   - You should see a new workflow run triggered by `repository_dispatch`
   - It should appear within 10-30 seconds of the Notion update

### Troubleshooting:

**If you don't see an execution in Make.com:**
- Check that the scenario is turned **"On"**
- Verify the webhook URL in Notion matches the Make.com webhook URL
- Check Notion webhook subscription is **"Active"**
- Try updating a page again

**If Make.com execution shows an error:**
- Click on the execution to see error details
- Check the HTTP module - verify the GitHub token is correct
- Check the request body - verify the JSON is formatted correctly
- Verify the repository name: `wobafett/notion-media-sync`
- Verify the workflow file name: `notion-sync.yml`

**If GitHub Actions doesn't trigger:**
- Check Make.com execution logs - did the HTTP request succeed?
- Verify the GitHub token has `repo` scope
- Check the repository name: `wobafett/notion-media-sync`
- Look at the HTTP response in Make.com - should be status 204 (No Content) for success
- Verify the workflow file name matches: `notion-sync.yml`

**If GitHub Actions triggers but fails:**
- Check GitHub Actions logs for error messages
- Verify the page_id is being passed correctly
- Check that the page belongs to the configured database

## Step 4: Error Handling (Optional)

Add error handling in Make.com:
- Add an **"Error Handler"** module
- Configure it to retry on failures
- Set up notifications if needed

## Make.com Scenario Structure

```
Notion Webhook
    ↓
Router (verification vs. regular webhooks)
    ↓
Router/Filter (by title and IGDb ID)
    ↓
[Optional: Notion "Get a page" if properties not in webhook]
    ↓
[Optional: Router to check IGDb ID]
    ↓
HTTP Request (GitHub API)
    ↓
Error Handler (optional)
```

## Configuration Details

### Notion Webhook Payload

When Notion sends a webhook, it includes:
```json
{
  "type": "page.updated",
  "data": {
    "id": "page-id",
    "parent": {
      "database_id": "your-database-id"
    }
  }
}
```

### GitHub API Request

The HTTP request to GitHub should be:
```json
{
  "event_type": "sync",
  "client_payload": {
    "page_id": "{{trigger_page_id}}",
    "force_update": false
  }
}
```

## Cost Estimate

Make.com free tier includes:
- **1,000 operations/month**
- Each webhook + API call = 2 operations
- Estimated usage: ~50-200 operations/month (depending on how often you update Notion)
- **Cost**: $0 (well within free tier)

## Advantages of Make.com

✅ **No server deployment** - Everything runs in Make.com's cloud  
✅ **Visual workflow builder** - Easy to see and modify the flow  
✅ **Built-in error handling** - Automatic retries and notifications  
✅ **Free tier** - More than enough for this use case  
✅ **No maintenance** - Make.com handles infrastructure  

## Next Steps

1. Create Make.com account
2. Set up the scenario as described above
3. Test with a page update
4. Monitor executions in Make.com dashboard

---

That's it! Your Notion webhooks will now automatically trigger GitHub Actions to sync specific pages via Make.com. 🎉



