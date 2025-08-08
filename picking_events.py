from forecasting import KalshiAPI
import random
import datetime
import os
import json
from pymongo import MongoClient
import pandas as pd
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REFRESH_EVENTS = True
EXPERIMENTID = 'daily_scrape'

kalshi = KalshiAPI()
today = datetime.date.today().strftime("%y%m%d")

events_file = [f"events_{today}.txt", 'todays_events.txt']
if REFRESH_EVENTS:
    events = kalshi.fetch_all_events(status='open')
    events = [event['event_ticker'] for event in events if len(event['markets']) < 6]
    random.shuffle(events)

    # save events to a file
    for file in events_file:
        with open(file, 'w') as f:
            for event in events:
                f.write(f"{event}\n")


# Load events from file
with open(events_file[1], 'r') as f:
    events = [line.strip() for line in f if line.strip()]

events