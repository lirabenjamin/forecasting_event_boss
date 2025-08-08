#!/usr/bin/env python3
"""
Simple Kalshi Event Scraper - Pushes files directly to GitHub
"""

from forecasting import KalshiAPI
import random
import datetime
import os
import json
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
REFRESH_EVENTS = True
EXPERIMENTID = 'daily_scrape'

def scrape_kalshi_events():
    """Your existing scraping logic"""
    logger.info("Starting Kalshi event scraping...")
    
    kalshi = KalshiAPI()
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    events_files = [f"events_{today}.txt", 'todays_events.txt']
    
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
    """
    Push file directly to GitHub repository
    Simple method using GitHub API
    """
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
    
    # Prepare the data
    import base64
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
        logger.info(f"✅ Pushed {filename} to GitHub: {download_url}")
        return download_url
    else:
        logger.error(f"❌ Failed to push {filename}: {response.text}")
        return None

def main():
    """Main function - runs the scraping and GitHub push"""
    # Get environment variables
    github_token = os.getenv('TOKEN')
    repo_owner = os.getenv('REPO_OWNER')  # Your GitHub username
    repo_name = os.getenv('REPO_NAME')    # Repository name
    
    if not all([github_token, repo_owner, repo_name]):
        logger.error("Missing required environment variables:")
        logger.error("GITHUB_TOKEN, GITHUB_REPO_OWNER, GITHUB_REPO_NAME")
        return
    
    # Scrape events
    files, events = scrape_kalshi_events()
    
    if not files:
        logger.error("Failed to scrape events")
        return
    
    # Push files to GitHub
    urls = {}
    for filepath in files:
        if os.path.exists(filepath):
            url = push_to_github_repo(filepath, github_token, repo_owner, repo_name)
            if url:
                urls[filepath] = url
    
    # Save URLs for reference
    with open('github_urls.json', 'w') as f:
        json.dump(urls, f, indent=2)
    
    logger.info("=== Summary ===")
    logger.info(f"Processed {len(events)} events")
    for filepath, url in urls.items():
        logger.info(f"{filepath} -> {url}")

if __name__ == "__main__":
    main()
