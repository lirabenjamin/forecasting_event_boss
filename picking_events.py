#!/usr/bin/env python3
import base64
import datetime as dt
import json
import logging
import os
import random
import requests
from forecasting import KalshiAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REFRESH_EVENTS = True
EXPERIMENTID = 'daily_scrape'

def utc_stamp():
    # Safe for paths/URLs
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def scrape_kalshi_events():
    logger.info("Starting Kalshi event scraping...")
    kalshi = KalshiAPI()
    ts = utc_stamp()
    events_files = [f"events_{ts}.txt", "todays_events.txt"]

    if REFRESH_EVENTS:
        events = kalshi.fetch_all_events(status='open')
        events = [e['event_ticker'] for e in events if len(e['markets']) < 6]
        random.shuffle(events)

        for path in events_files:
            with open(path, "w") as f:
                f.write("\n".join(events))
            logger.info(f"Saved {len(events)} events to {path}")

    with open(events_files[1], "r") as f:
        events = [line.strip() for line in f if line.strip()]
    return events_files, events

def push_to_github_repo(filepath, github_token, repo_full, branch='main'):
    owner, repo = repo_full.split("/", 1)
    filename = os.path.basename(filepath)

    with open(filepath, "r") as f:
        content = f.read()
    content_encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    base = f"https://api.github.com/repos/{owner}/{repo}/contents/{filename}"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }

    # Get current SHA if file exists
    r = requests.get(base, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None

    data = {
        "message": f"Update {filename} - {utc_stamp()}",
        "content": content_encoded,
        "branch": branch,
    }
    if sha:
        data["sha"] = sha

    r = requests.put(base, json=data, headers=headers)
    if r.status_code in (200, 201):
        url = r.json()["content"]["html_url"]
        logger.info(f"✅ Pushed {filename} to GitHub: {url}")
        return url
    logger.error(f"❌ Failed to push {filename}: {r.status_code} {r.text}")
    return None

def main():
    github_token = os.getenv("GITHUB_TOKEN")
    repo_full = os.getenv("GITHUB_REPOSITORY")  # e.g., "owner/repo"

    if not github_token:
        logger.error("Missing GITHUB_TOKEN in environment.")
        return
    if not repo_full:
        logger.error("Missing GITHUB_REPOSITORY in environment (owner/repo).")
        return

    files, events = scrape_kalshi_events()
    if not files:
        logger.error("Failed to scrape events")
        return

    urls = {}
    for path in files:
        if os.path.exists(path):
            url = push_to_github_repo(path, github_token, repo_full)
            if url:
                urls[path] = url

    with open("github_urls.json", "w") as f:
        json.dump(urls, f, indent=2)

    logger.info("=== Summary ===")
    logger.info(f"Processed {len(events)} events")
    for path, url in urls.items():
        logger.info(f"{path} -> {url}")

if __name__ == "__main__":
    main()
