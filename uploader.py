#!/usr/bin/env python3
"""
Daily Kalshi Event Scraper - Runs at 1 AM UTC and uploads files online
"""

from forecasting import KalshiAPI
import random
import datetime
import os
import json
from pymongo import MongoClient
import pandas as pd
import logging
import requests
import boto3
from pathlib import Path
import schedule
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kalshi_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
REFRESH_EVENTS = True
EXPERIMENTID = 'daily_scrape'

class FileUploader:
    """Handle different file upload methods"""
    
    @staticmethod
    def upload_to_github_gist(filename, content, gist_token):
        """Upload to GitHub Gist (public, accessible via URL)"""
        url = "https://api.github.com/gists"
        
        data = {
            "description": f"Kalshi Events - {datetime.date.today()}",
            "public": True,
            "files": {
                filename: {
                    "content": content
                }
            }
        }
        
        headers = {
            "Authorization": f"token {gist_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 201:
            gist_url = response.json()['html_url']
            raw_url = response.json()['files'][filename]['raw_url']
            logger.info(f"Uploaded to GitHub Gist: {gist_url}")
            logger.info(f"Raw URL: {raw_url}")
            return gist_url, raw_url
        else:
            logger.error(f"Failed to upload to GitHub Gist: {response.text}")
            return None, None
    
    @staticmethod
    def upload_to_s3(filename, filepath, bucket_name, aws_access_key, aws_secret_key):
        """Upload to AWS S3 (configurable public access)"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            
            s3_client.upload_file(
                filepath, 
                bucket_name, 
                filename,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/plain'}
            )
            
            url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
            logger.info(f"Uploaded to S3: {url}")
            return url
            
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            return None
    
    @staticmethod
    def upload_to_pastebin(content, api_key, paste_name):
        """Upload to Pastebin (public)"""
        url = "https://pastebin.com/api/api_post.php"
        
        data = {
            'api_dev_key': api_key,
            'api_option': 'paste',
            'api_paste_code': content,
            'api_paste_name': paste_name,
            'api_paste_expire_date': 'N',  # Never expire
            'api_paste_private': '0'  # Public
        }
        
        response = requests.post(url, data=data)
        if response.status_code == 200 and response.text.startswith('https://'):
            logger.info(f"Uploaded to Pastebin: {response.text}")
            return response.text
        else:
            logger.error(f"Failed to upload to Pastebin: {response.text}")
            return None

def scrape_kalshi_events():
    """Main scraping function"""
    logger.info("Starting Kalshi event scraping...")
    
    try:
        kalshi = KalshiAPI()
        today = datetime.date.today().strftime("%y%m%d")
        events_files = [f"events_{today}.txt", 'todays_events.txt']
        
        if REFRESH_EVENTS:
            logger.info("Fetching events from Kalshi API...")
            events = kalshi.fetch_all_events(status='open')
            events = [event['event_ticker'] for event in events if len(event['markets']) < 6]
            random.shuffle(events)
            
            logger.info(f"Found {len(events)} events")
            
            # Save events to files
            for file in events_files:
                with open(file, 'w') as f:
                    for event in events:
                        f.write(f"{event}\n")
                logger.info(f"Saved events to {file}")
        
        # Load events from file for verification
        with open(events_files[1], 'r') as f:
            events = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Loaded {len(events)} events from file")
        return events_files, events
        
    except Exception as e:
        logger.error(f"Error in scrape_kalshi_events: {e}")
        return None, None

def upload_files_online(files):
    """Upload files to various online services"""
    
    # Get credentials from environment variables
    github_token = os.getenv('GITHUB_TOKEN')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    pastebin_key = os.getenv('PASTEBIN_API_KEY')
    
    uploader = FileUploader()
    upload_results = {}
    
    for filepath in files:
        if not os.path.exists(filepath):
            continue
            
        filename = os.path.basename(filepath)
        
        # Read file content
        with open(filepath, 'r') as f:
            content = f.read()
        
        upload_results[filename] = {}
        
        # Upload to GitHub Gist
        if github_token:
            gist_url, raw_url = uploader.upload_to_github_gist(
                filename, content, github_token
            )
            upload_results[filename]['github_gist'] = gist_url
            upload_results[filename]['github_raw'] = raw_url
        
        # Upload to S3
        if aws_access_key and aws_secret_key and s3_bucket:
            s3_url = uploader.upload_to_s3(
                filename, filepath, s3_bucket, aws_access_key, aws_secret_key
            )
            upload_results[filename]['s3'] = s3_url
        
        # Upload to Pastebin
        if pastebin_key:
            pastebin_url = uploader.upload_to_pastebin(
                content, pastebin_key, f"Kalshi Events - {filename}"
            )
            upload_results[filename]['pastebin'] = pastebin_url
    
    # Save upload results
    results_file = f"upload_results_{datetime.date.today().strftime('%y%m%d')}.json"
    with open(results_file, 'w') as f:
        json.dump(upload_results, f, indent=2)
    
    logger.info(f"Upload results saved to {results_file}")
    return upload_results

def daily_job():
    """The job that runs daily at 1 AM UTC"""
    logger.info("=== Starting daily Kalshi scraping job ===")
    
    try:
        # Scrape events
        files, events = scrape_kalshi_events()
        
        if files and events:
            # Upload files online
            upload_results = upload_files_online(files)
            
            # Log summary
            logger.info("=== Daily job completed successfully ===")
            logger.info(f"Processed {len(events)} events")
            logger.info(f"Files created: {files}")
            
            for filename, urls in upload_results.items():
                logger.info(f"\n{filename} uploaded to:")
                for service, url in urls.items():
                    if url:
                        logger.info(f"  {service}: {url}")
        else:
            logger.error("Failed to scrape events")
            
    except Exception as e:
        logger.error(f"Error in daily job: {e}")

def setup_scheduler():
    """Setup the daily scheduler"""
    # Schedule job for 1 AM UTC daily
    schedule.every().day.at("01:00").do(daily_job)
    
    logger.info("Scheduler set up - will run daily at 1:00 AM UTC")
    logger.info("Current UTC time: " + datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    
    # Run once immediately for testing (optional)
    if os.getenv('RUN_IMMEDIATELY', 'false').lower() == 'true':
        logger.info("Running job immediately for testing...")
        daily_job()

def main():
    """Main function"""
    logger.info("Starting Kalshi Daily Scraper")
    
    # Check required environment variables
    required_vars = []
    optional_vars = ['GITHUB_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 
                    'S3_BUCKET_NAME', 'PASTEBIN_API_KEY']
    
    available_services = []
    for var in optional_vars:
        if os.getenv(var):
            available_services.append(var)
    
    if not available_services:
        logger.warning("No upload service credentials found. Files will only be saved locally.")
        logger.info("Set environment variables for: " + ", ".join(optional_vars))
    else:
        logger.info(f"Available upload services: {available_services}")
    
    # Setup scheduler
    setup_scheduler()
    
    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Script stopped by user")

if __name__ == "__main__":
    main()