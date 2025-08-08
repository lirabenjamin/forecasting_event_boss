#!/usr/bin/env python3
"""
Test version of your Kalshi scraper - runs once and uploads to GitHub
"""

from forecasting import KalshiAPI
import random
import datetime
import os
import json
import requests
import logging
import base64
import dotenv

dotenv.load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_kalshi_events():
    """Your existing scraping logic"""
    logger.info("Starting Kalshi event scraping...")
    
    kalshi = KalshiAPI()
    today = datetime.date.today().strftime("%y%m%d")
    events_files = [f"events_{today}.txt", 'todays_events.txt']
    
    # Your original code
    REFRESH_EVENTS = True
    if REFRESH_EVENTS:
        events = kalshi.fetch_all_events(status='open')
        events = [event['event_ticker'] for event in events if len(event['markets']) < 6]
        random.shuffle(events)
        
        # Save events to files
        for file in events_files:
            with open(file, 'w') as f:
                for event in events:
                    f.write(f"{event}\n")
            logger.info(f"Saved {len(events)} events to {file}")
    
    # Load events from file
    with open(events_files[1], 'r') as f:
        events = [line.strip() for line in f if line.strip()]
    
    return events_files, events

def push_to_github_repo(filepath, github_token, repo_owner, repo_name, branch='main'):
    """Push file directly to GitHub repository"""
    filename = os.path.basename(filepath)
    
    # Read file content
    with open(filepath, 'r') as f:
        content = f.read()
    
    # GitHub API URL
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{filename}"
    
    # Check if file exists to get its SHA (required for updates)
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(url, headers=headers)
    sha = None
    if response.status_code == 200:
        sha = response.json()['sha']
        logger.info(f"File {filename} exists, will update it")
    else:
        logger.info(f"File {filename} doesn't exist, will create it")
    
    # Prepare the data
    content_encoded = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    
    data = {
        "message": f"Update {filename} - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "content": content_encoded,
        "branch": branch
    }
    
    if sha:  # File exists, include SHA for update
        data["sha"] = sha
    
    # Push to GitHub
    response = requests.put(url, json=data, headers=headers)
    
    if response.status_code in [200, 201]:
        download_url = response.json()['content']['download_url']
        logger.info(f"✅ Pushed {filename} to GitHub")
        logger.info(f"🌐 Public URL: {download_url}")
        return download_url
    else:
        logger.error(f"❌ Failed to push {filename}: {response.text}")
        return None

def main():
    """Test run - scrape and upload once"""
    logger.info("=== TEST RUN - Kalshi Scraper ===")

    dotenv.load_dotenv()
    
    # Get environment variables
    github_token = os.getenv('TOKEN')
    repo_owner = os.getenv('REPO_OWNER')  
    repo_name = os.getenv('REPO_NAME')    

    print(github_token)
    print(repo_owner)
    print(repo_name)
    
    if not github_token:
        logger.error("GITHUB_TOKEN not found in environment variables")
        logger.info("Make sure you've set: export GITHUB_TOKEN='your_token_here'")
        return
    
    if not repo_owner:
        logger.error("GITHUB_REPO_OWNER not set")
        logger.info("Set it with: export GITHUB_REPO_OWNER='your_github_username'")
        return
        
    if not repo_name:
        logger.warning("GITHUB_REPO_NAME not set, using default 'kalshi-events'")
        repo_name = 'kalshi-events'
    
    logger.info(f"Will upload to: https://github.com/{repo_owner}/{repo_name}")
    
    # Scrape events
    try:
        files, events = scrape_kalshi_events()
        logger.info(f"✅ Scraped {len(events)} events successfully")
    except Exception as e:
        logger.error(f"❌ Failed to scrape events: {e}")
        return
    
    if not files:
        logger.error("No files to upload")
        return
    
    # Push files to GitHub
    urls = {}
    for filepath in files:
        if os.path.exists(filepath):
            logger.info(f"Uploading {filepath}...")
            url = push_to_github_repo(filepath, github_token, repo_owner, repo_name)
            if url:
                urls[filepath] = url
    
    # Save URLs for reference
    with open('github_urls.json', 'w') as f:
        json.dump(urls, f, indent=2)
    
    # Summary
    logger.info("=" * 50)
    logger.info("🎉 TEST COMPLETED SUCCESSFULLY!")
    logger.info(f"📊 Processed {len(events)} events")
    logger.info("📁 Your files are now public at:")
    
    for filepath, url in urls.items():
        logger.info(f"   {os.path.basename(filepath)}: {url}")
    
    logger.info("💾 URLs saved to github_urls.json")

if __name__ == "__main__":
    main()