# TO-DO
from pymongo import MongoClient
from typing import List, Dict, Optional, Any, Tuple
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from dateutil import parser
from datetime import datetime, date
import numpy as np
import json, json5, re, os, random, logging, requests, backoff, asyncio, time, tqdm, uuid, copy, statistics, openai
from pymongo import DESCENDING
from collections import defaultdict
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle  # use if you prefer round-robin instead of random
from readability import Document
from bs4 import BeautifulSoup

# fake azure
def DefaultAzureCredential():
    pass

def HttpResponseError():
    pass

# ------------------------- SETUP -------------------------
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("azure").setLevel(logging.ERROR)
logging.getLogger("azure.core").setLevel(logging.ERROR)
logging.getLogger("azure.identity").setLevel(logging.ERROR)
logging.getLogger("pymongo").setLevel(logging.ERROR)

# ------------------------- UTILS -------------------------

def to_python_types(doc):
    """
    Convert numpy types in a dictionary to native Python types.
    """
    return {k: (v.item() if isinstance(v, np.generic) else v) for k, v in doc.items()}

@backoff.on_exception(
    backoff.expo,  # exponential backoff: 1s, 2s, 4s, ...
    RuntimeError,
    max_tries=10,
    jitter=backoff.full_jitter,  # optional: adds randomness to prevent thundering herd
)
def create_and_process_run(client, thread_id, agent_id):
    run = client.agents.runs.create_and_process(
        thread_id=thread_id,
        agent_id=agent_id
    )
    if run.status == "failed":
        raise RuntimeError(f"Agent run failed: {run.last_error}")
    return run
# ------------------------- CONFIG -------------------------
BASE_URL_EVENTS = "https://api.kalshi.com/v1/events"
BASE_URL_MARKETS = "https://api.kalshi.com/v1/markets"
HEADERS = {"Content-Type": "application/json"}

AGENT_CLONES = [ "asst_lOPI4FY0Af9iZBcQ3KVSSWpc",'asst_037aOVawn9GWFdXFnCofXLcE', 'asst_JnhaVkfqkJQrypVAQJH5EnoH', 'asst_5S6EJ7hVO61S4EndcRLeT1Ah', 'asst_X9dVuOkICkMNqXMbKqVkFuaE']
AGENT_CONFIG = {
    ("high", False): ["asst_g0BLDbfJHrnIPSAC7dBCI4Mq"],
    ("low",  False): ["asst_eAn9hpGgz68mzwh9XSoaqqn4"],
    ("high", True):  ["asst_lOPI4FY0Af9iZBcQ3KVSSWpc"],
    ("low",  True):  AGENT_CLONES,            # spread load here
}
_ROUNDS = {k: cycle(v) for k, v in AGENT_CONFIG.items()}


CONDITION_MAP = {
    'baseline': {"base_rate": False, "ref_class": False},
    'random': {"base_rate": False, "ref_class": False},
    'reference class': {"base_rate": False, "ref_class": True},
    'base rate': {"base_rate": True, "ref_class": False},
    'reference class + base rate': {"base_rate": True, "ref_class": True},
}

def get_agent_id(temp: str, search: bool, strategy: str = "random") -> str:
    """
    temp      : 'high' or 'low'
    search    : bool  (True = reference class)
    strategy  : 'random' | 'rr'  (round-robin)
    """
    pool = AGENT_CONFIG[(temp.lower(), search)]
    if strategy == "rr":
        return next(_ROUNDS[(temp.lower(), search)])
    return random.choice(pool)

random_personas = [
    ("methodical_bayesian", "Methodical Bayesian", "You are a Bayesian decision scientist trained in probabilistic reasoning and inference. You believe every forecast should be grounded in prior base rates, updated by current data. Apply Bayes’ theorem rigorously as you read. Avoid heuristics and gut feels. Your writing should include priors, likelihoods, and posterior probability reasoning."),
    ("ethnographer", "AI Ethnographer", "You are a sociotechnical analyst who studies how public discourse, media narratives, and expert consensus shape forecasts. Examine online articles, social sentiment, and institutional trust. Highlight divergent views and why they exist."),
    ("contrarian", "Contrarian Strategist", "You are a contrarian investor who looks for underpriced risks and asymmetric information. Your goal is to challenge the consensus and find blindspots others missed. Explicitly list what most people believe, and where you disagree."),
    ("intel_analyst", "Intelligence Analyst", "You are a CIA-trained analyst evaluating geopolitical and economic events. You specialize in threat modeling, uncertainty quantification, and confidence calibration. Use structured analytic techniques."),
    ("historical_analogist", "Historical Analogist", "You are a historian focused on identifying relevant analogies from past events. Your forecast relies on identifying similar events in the past 50 years and reasoning from those outcomes."),
    ("macro_modeler", "Macroeconomic Modeler", "You are a macroeconomist focused on modeling broad economic forces (GDP, inflation, trade policy, interest rates) to make forecasts. Model assumptions should be made explicit."),
    ("regulatory_expert", "Regulatory Expert", "You are a legal analyst who tracks laws, executive orders, regulatory frameworks, and international treaties. You reason based on institutional constraints and legal precedent."),
    ("market_technician", "Technical Trader", "You are a quantitative analyst using prediction markets, options data, and market pricing signals to forecast. Your approach is purely price-driven."),
    ("political_scientist", "Political Scientist", "You are a political scientist using polling data, demographic trends, approval ratings, and historical election results to forecast political events."),
    ("systems_thinker", "Systems Thinker", "You analyze events using systems thinking: feedback loops, second-order effects, unintended consequences. Look for interdependencies and emergent dynamics."),
    ("superforecaster", "Superforecaster", "You are a top-tier superforecaster trained by Good Judgment Inc. You update forecasts regularly, break down complex questions into subcomponents, and give probabilistic estimates."),
    ("behavioral_economist", "Behavioral Economist", "You are a behavioral economist trained in understanding human biases, cognitive heuristics, and market irrationality. Your predictions should consider how voters, policymakers, or investors are likely to behave irrationally in this context."),
]
# Ask the LLM to pick 6, and use those for the final experiment.
top6_personas = [
    ("methodical_bayesian", "Methodical Bayesian", "You are a Bayesian decision scientist trained in probabilistic reasoning and inference. You believe every forecast should be grounded in prior base rates, updated by current data. Apply Bayes’ theorem rigorously as you read. Avoid heuristics and gut feels. Your writing should include priors, likelihoods, and posterior probability reasoning."),
    # ("ethnographer", "AI Ethnographer", "You are a sociotechnical analyst who studies how public discourse, media narratives, and expert consensus shape forecasts. Examine online articles, social sentiment, and institutional trust. Highlight divergent views and why they exist."),
    ("contrarian", "Contrarian Strategist", "You are a contrarian investor who looks for underpriced risks and asymmetric information. Your goal is to challenge the consensus and find blindspots others missed. Explicitly list what most people believe, and where you disagree."),
    # ("intel_analyst", "Intelligence Analyst", "You are a CIA-trained analyst evaluating geopolitical and economic events. You specialize in threat modeling, uncertainty quantification, and confidence calibration. Use structured analytic techniques."),
    # ("historical_analogist", "Historical Analogist", "You are a historian focused on identifying relevant analogies from past events. Your forecast relies on identifying similar events in the past 50 years and reasoning from those outcomes."),
    # ("macro_modeler", "Macroeconomic Modeler", "You are a macroeconomist focused on modeling broad economic forces (GDP, inflation, trade policy, interest rates) to make forecasts. Model assumptions should be made explicit."),
    # ("regulatory_expert", "Regulatory Expert", "You are a legal analyst who tracks laws, executive orders, regulatory frameworks, and international treaties. You reason based on institutional constraints and legal precedent."),
    ("market_technician", "Technical Trader", "You are a quantitative analyst using prediction markets, options data, and market pricing signals to forecast. Your approach is purely price-driven."),
    # ("political_scientist", "Political Scientist", "You are a political scientist using polling data, demographic trends, approval ratings, and historical election results to forecast political events."),
    ("systems_thinker", "Systems Thinker", "You analyze events using systems thinking: feedback loops, second-order effects, unintended consequences. Look for interdependencies and emergent dynamics."),
    ("superforecaster", "Superforecaster", "You are a top-tier superforecaster trained by Good Judgment Inc. You update forecasts regularly, break down complex questions into subcomponents, and give probabilistic estimates."),
    ("behavioral_economist", "Behavioral Economist", "You are a behavioral economist trained in understanding human biases, cognitive heuristics, and market irrationality. Your predictions should consider how voters, policymakers, or investors are likely to behave irrationally in this context."),
]

# ------------------------- DB LOGGING -------------------------
# mongo_client = MongoClient(os.getenv("MONGODB_URI"))
# db = mongo_client["forecasting"]

# def test_db_connection():
#     try:
#         mongo_client.admin.command('ping')
#         logger.info("✅ Successfully connected to MongoDB.")
#     except Exception as e:
#         logger.error(f"❌ Failed to connect to MongoDB: {e}")
#         raise

# test_db_connection()
def write_to_db(collection_name, data):
    """
    Writes a dictionary or list of dictionaries to the specified collection.

    Args:
        collection_name (str): Name of the MongoDB collection.
        data (dict or list of dict): Document(s) to insert.
    """
    collection = db[collection_name]
    now = datetime.utcnow()

    if isinstance(data, dict):
        data["timestamp"] = now
        result = collection.insert_one(data)
        print(f"Inserted document with _id: {result.inserted_id}")
    elif isinstance(data, list):
        for doc in data:
            doc["timestamp"] = now  # Add timestamp to each document
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents.")
    else:
        raise ValueError("Data must be a dict or a list of dicts.")

def log_run_to_db(event_data, market_data=None):
    """
    Logs a forecasting run to the database.

    Args:
        event_data (dict): Data about the event.
        market_data (list, optional): List of market data dictionaries.
    """
    event_data["timestamp"] = datetime.utcnow()
    db["event_predictions"].insert_one(event_data)

    if market_data:
        for market in market_data:
            market["event_ticker"] = event_data["event"]
            market["timestamp"] = datetime.utcnow()
        db['market_predictions'].insert_many(market_data)
 
    logger.debug(f"Logged run for event {event_data['event']} with {len(market_data) if market_data else 0} markets")

# ------------------------- KALSHI API -------------------------
class KalshiAPI:
    def __init__(self):
        self.base_url_events = "https://api.elections.kalshi.com/trade-api/v2/events"
        self.base_url_markets = "https://api.elections.kalshi.com/trade-api/v2/markets"

    def make_forecasting_event(self, event_ticker):
        event_data = self.fetch_event(event_ticker)
        if not event_data:
            logger.error(f"Event {event_ticker} not found")
            return None
        markets = self.fetch_markets_for_event(event_ticker)
        flat_df = flatten_events([event_data])
        return ForecastingEvent(event_data, flat_df, markets)

    def fetch_event(self, event_ticker: str):
        url = f"{self.base_url_events}/{event_ticker}"
        logger.info(f"Fetching event with ticker: {event_ticker}")
        resp = requests.get(url)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch event: {resp.status_code}")
            return None
        return resp.json()
 
    def fetch_all_events(self, status=None, with_markets=True):
        params = {}
        if status:
            params["status"] = status
        if with_markets:
            params["with_nested_markets"] = "true"

        events = []
        cursor = None

        logger.info(f"Fetching all events with status={status} and with_markets={with_markets}")

        while True:
            if cursor:
                params["cursor"] = cursor

            logger.debug(f"Requesting events with params: {params}")
            resp = requests.get(self.base_url_events, params=params)
            if resp.status_code != 200:
                logger.error(f"Failed to fetch events: {resp.status_code}")
                break

            data = resp.json()
            batch_events = data.get("events", [])
            logger.info(f"Fetched {len(batch_events)} events in this batch")
            events.extend(batch_events)

            cursor = data.get("cursor")
            if not cursor:
                break

        logger.info(f"Total events fetched: {len(events)}")
        return events

    def fetch_markets_for_event(self, event_ticker: str):
        markets = []
        cursor = None
        while True:
            params = {"event_ticker": event_ticker}
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(self.base_url_markets, params=params)
            if resp.status_code != 200:
                print(f"Error fetching markets for {event_ticker}: {resp.status_code}")
                break

            data = resp.json()
            markets.extend(data.get("markets", []))
            cursor = data.get("cursor")
            if not cursor:
                break

        return markets

def flatten_events(events):
    flattened_events = []
    for e in events:
        event_info = e.get("event", {})
        flattened_event = {**e, **event_info}
        flattened_event.pop("event", None)
        flattened_events.append(flattened_event)

    # Step 2: Normalize with proper meta keys
    df = pd.json_normalize(
        flattened_events,
        record_path="markets",
        meta=["event_ticker", "title", "category", "sub_title", "series_ticker"],
        meta_prefix="event_",
        errors="ignore"
    )

    # Step 3: Additional calculated columns
    df["mid_price"] = (df["yes_bid"] + df["yes_ask"]) / 2
    df['yes_implied_prob'] = np.where(
    (df['yes_bid'] + df['no_bid']) > 0,
    df['yes_bid'] / (df['yes_bid'] + df['no_bid']),
    df['last_price'] / 100  # fallback
)
    df['expiration_time'] = pd.to_datetime(df['expiration_time'], errors='coerce')
    df['time_to_expiration'] = (df['expiration_time'] - pd.Timestamp.now(tz='UTC')).dt.total_seconds() / 3600 / 24
    df['extremity'] = abs(df['yes_implied_prob'] - 0.5)

    return df

def get_market_descriptions(event, markets):
    # Generate market descriptions
    market_descriptions = ""
   
    if len(markets) == 1:
        m = markets[0]
        market_descriptions = f"""Event title: {event.title}
Title: {m.title}
Subtitle: {m.yes_sub_title}
Possible Outcomes: Yes (0) or No (1)
Rules: {m.rules_primary}"""

        if type(m.rules_secondary) == str and len(m.rules_secondary) > 0:
            market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
        market_descriptions += f"\nScheduled close date: {m.expiration.strftime('%Y-%m-%d')}"
        market_descriptions += f"\n(Note: The market may resolve before this date.)\n"

    elif len(markets) > 1:
        for idx, m in enumerate(markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m.ticker}
Title: {m.title}
Subtitle: {getattr(m, 'yes_sub_title', '')}
Possible Outcomes: Yes (0) or No (1)
Rules: {getattr(m, 'rules_primary', '')}"""

            if isinstance(getattr(m, 'rules_secondary', None), str) and len(getattr(m, 'rules_secondary', '')) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
            market_descriptions += f"\nScheduled close date: {m.expiration.strftime('%Y-%m-%d')}\n\n"
   
    return market_descriptions


# ------------------------- CLASSES -------------------------
class Market:
    def __init__(self, data: dict):
        self.ticker = data.get("ticker")
        self.title = data.get("title")
        self.rules_primary = data.get("rules_primary")
        self.expiration = data.get("expiration_time")
        self.yes_sub_title = data.get("yes_sub_title", "")
        self.rules_secondary = data.get("rules_secondary", "")
        self.resolution = data.get('result')
        self.meta = data

class ForecastingEvent:
    def __init__(self, data: dict, flat_df, markets: List[dict]):
        self.event_ticker = data["event"]['event_ticker']
        self.title = data['event'].get("title", "")
        self.category = data["event"].get("category", "")
        self.flat_df = flat_df
        self.markets = [Market(m) for m in markets]

    def has_too_many_markets(self) -> bool:
        return len(self.markets) >= 6
     
    def is_resolved(self) -> bool:
        """
        Check if the event is resolved by looking for markets with a resolution.
        """
        return any(m.resolution is not None for m in self.markets)

class LLMAgent:
    def __init__(self, temp: str, search: bool, credential: Optional[DefaultAzureCredential] = None, rewrite_prompts: bool = False, use_trapi: bool = False, useo3=False):
        self.temp = temp
        self.search = search
        self.agent_id = get_agent_id(temp, search)
        self.credential = credential or DefaultAzureCredential()
        self.endpoint = "https://t-benjaminl.services.ai.azure.com/api/projects/t-benjaminl"
        self.client = AIProjectClient(credential=self.credential, endpoint=self.endpoint)
        self.rewrite_prompts = rewrite_prompts
        self.use_trapi = use_trapi
        if self.use_trapi:
            logger.info("Using TRAPI")
            self.trapi = TRAPIAgent()


    @backoff.on_exception(
        backoff.expo,
        (RuntimeError, ValueError, TimeoutError, HttpResponseError),
        max_tries=10,
        jitter=backoff.full_jitter
    )
    def _generic_azure_call(self, prompt):
        thread = self.client.agents.threads.create()
        logger.debug(f"📬 Created thread {thread.id}")

        # Step 1: Research
        self.client.agents.messages.create(thread_id=thread.id, role="user", content=prompt)
        run = self.client.agents.runs.create(thread_id=thread.id, agent_id=self.agent_id)
        self._wait_for_run(run, thread.id)
        logger.info("✅ Azure GPT call completed")

        # Messages
        messages = self._get_messages(thread.id)
        assistant_msgs = [m for m in messages if m.role == "assistant"]

        if len(assistant_msgs) < 1:
            raise RuntimeError("Expected at least 1 assistant messages, got fewer.")
        logger.debug(f"Found {len(assistant_msgs)} assistant messages in thread {thread.id}")

        text = assistant_msgs[0].content[0]["text"]["value"]
        return text


    def _wait_for_run(self, run, thread_id, timeout=150):
        for _ in range(timeout):
            rs = self.client.agents.runs.get(thread_id=thread_id, run_id=run.id)
            if rs.status == "completed":
                return rs

            if rs.status in {"failed", "cancelled", "expired"}:
                err = rs.last_error or {}
                code    = err.get("code",    "unknown_code")
                detail  = err.get("message", "no message provided")
                logger.error(f"❌ Run {run.id} failed — code={code} | msg={detail}")
                raise RuntimeError(f"{code}: {detail}")

            time.sleep(1)

        raise TimeoutError("Run polling timed-out")

    def _get_messages(self, thread_id: str):
        return list(self.client.agents.messages.list(thread_id=thread_id, order=ListSortOrder.ASCENDING))

    def _get_urls_from_thread(self, thread_id: str):
        messages = self._get_messages(thread_id)
        urls = []
        for i, msg in enumerate(messages):
            if msg.role == "assistant":
                annotations = msg.content[0]['text'].get('annotations', [])
                for annotation in annotations:
                    if annotation['type'] == 'url_citation':
                        url_citation = annotation['url_citation']
                        urls.append({"response": i, 'url': url_citation['url'], 'title': url_citation['title']})
        return urls

    def _get_texts_from_thread(self, thread_id: str):
        messages = self._get_messages(thread_id)
        return [msg.content[0]['text']['value'] for msg in messages if msg.role == "assistant"]

    def _parse_research(self, text: str) -> Dict[str, str]:
        sections = re.split(r"\n(?=\w+?:)", text)
        output = {}
        for section in sections:
            if "RESEARCH REPORT" in section:
                output["research_report"] = section
            elif "REFERENCE CLASS" in section:
                output["reference_class"] = section
            elif "BASE RATES" in section:
                output["base_rates"] = section
            elif "CRITICISM" in section:
                output["criticism"] = section
        return output

    def _parse_prediction(self, text: str) -> List[Dict[str, Any]]:
        # Remove code block markers if present
        text = re.sub(r"```(?:json)?\n(.*?)\n```", r"\1", text, flags=re.DOTALL).strip()
       
        # Method 1: Use greedy matching instead of non-greedy
        json_match = re.search(r'\[[\s\S]*\]', text)  # Removed the ? for greedy matching
       
        if json_match:
            json_text = json_match.group(0).strip()
        else:
            # Method 2: Find balanced brackets
            json_text = self._extract_balanced_json_array(text)
       
        if not json_text:
            json_text = text.strip()
       
        try:
            return json.loads(json_text)
        except json.JSONDecodeError:
            try:
                return json5.loads(json_text)
            except Exception as e:
                logger.error(f"Still failed to parse prediction JSON: {json_text[:1500]}")
                raise ValueError("Prediction output is not valid JSON")

    def _extract_balanced_json_array(self, text: str) -> str:
        """Extract JSON array with balanced brackets"""
        start_idx = text.find('[')
        if start_idx == -1:
            return ""
       
        bracket_count = 0
        end_idx = start_idx
       
        for i in range(start_idx, len(text)):
            if text[i] == '[':
                bracket_count += 1
            elif text[i] == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end_idx = i + 1
                    break
       
        return text[start_idx:end_idx] if bracket_count == 0 else ""

    def _parse_urls(self, text: str) -> List[str]:
        return re.findall(r'https?://\S+', text)

    @backoff.on_exception(backoff.expo, (RuntimeError, ValueError, TimeoutError, HttpResponseError), max_tries=10, jitter=backoff.full_jitter)
    def research_call_gpt(self, prompt: str) -> Dict[str, Any]:
        if not self.use_trapi:
            try:
                thread = self.client.agents.threads.create()
                logger.debug(f"📬 Created thread {thread.id}")

                # Step 1: Research
                self.client.agents.messages.create(thread_id=thread.id, role="user", content=prompt)
                run = self.client.agents.runs.create(thread_id=thread.id, agent_id=self.agent_id)
                self._wait_for_run(run, thread.id)
                logger.info("✅ Research run complete")

                # Messages
                messages = self._get_messages(thread.id)
                assistant_msgs = [m for m in messages if m.role == "assistant"]

                if len(assistant_msgs) < 1:
                    raise RuntimeError("Expected at least 1 assistant messages, got fewer.")
                logger.debug(f"Found {len(assistant_msgs)} assistant messages in thread {thread.id}")

                research_text = assistant_msgs[0].content[0]["text"]["value"]
                # logger.info(f"Research text length: {len(research_text)} characters")
               
                research_parts = self._parse_research(research_text)
                # logger.info(f"Parsed research report with keys: {list(research_parts.keys())}")
               
                urls = self._get_urls_from_thread(thread.id)
                # logger.info(f"Found {len(urls)} URLs in thread {thread.id}")

                # log things that go into results to understand why it is not working
                logger.debug(f"Research report: {research_parts.get('research_report', '')[:100]}...")
                logger.debug(f"Criticism: {research_parts.get('criticism', '')[:100]}...")
                logger.debug(f"Reference class: {research_parts.get('reference_class', '')[:100]}...")
                logger.debug(f"Base rates: {research_parts.get('base_rates', '')[:100]}...")
           
                if not urls:
                    logger.warning("No URLs found in the response.")
                if not isinstance(urls, list):
                    logger.error(f"URLs are not a list: {type(urls)}")
                    raise ValueError(f"URLs should be a list, got {type(urls)}")
                if not all(isinstance(u, dict) and 'url' in u for u in urls):
                    logger.error("Not all URLs are dictionaries with 'url' key.")
                    raise ValueError("Each URL should be a dictionary with 'url' key.")
                logger.debug(f"URLs: {json.dumps(urls, indent=2)[:200]}...")

                result = {
                    "thread_id": thread.id,
                    "raw_text": research_text,
                    "research_report": research_parts.get("research_report", ""),
                    "criticism": research_parts.get("criticism", ""),
                    "reference_class": research_parts.get("reference_class", ""),
                    "base_rates": research_parts.get("base_rates", ""),
                    "urls": urls
                }
                logger.info(f"Returning result with research summary and {len(result['urls'])} URLs")
                return result
            except Exception as e:
                logger.error(f"❌ GPT call failed: {type(e).__name__} | {e}")
                logger.debug("Full exception info", exc_info=True)
                raise
        elif self.use_trapi:
            try:
                # Step 1: Research
                research = self.trapi._llm_call_generic(prompt)
                logger.info("✅ Research run complete")
                research_parts = self._parse_research(research)
                result = {
                    "raw_text": research,
                    "research_report": research_parts.get("research_report", ""),
                    "criticism": research_parts.get("criticism", ""),
                    "reference_class": research_parts.get("reference_class", ""),
                    "base_rates": research_parts.get("base_rates", ""),
                }
                logger.info(f"Returning result with research components")
                return result
            except Exception as e:
                logger.error(f"❌ GPT call failed: {type(e).__name__} | {e}")
                logger.debug("Full exception info", exc_info=True)
                raise

    @backoff.on_exception(backoff.expo, (RuntimeError, ValueError,TimeoutError, HttpResponseError, openai.RateLimitError), max_tries=10, jitter=backoff.full_jitter)
    def prediction_call_gpt(self, prompt: str, thread_id = None, research_text = None, research_prompt = None) -> Dict[str, Any]:
        if not self.use_trapi:
            try:
                if not thread_id:
                    thread = self.client.agents.threads.create()
                    thread_id = thread.id

                # Step 2: Forecast
                self.client.agents.messages.create(thread_id=thread_id, role="user", content=prompt)
                run = self.client.agents.runs.create(thread_id=thread_id, agent_id=self.agent_id)
                self._wait_for_run(run, thread_id)
                logger.info("✅ Forecast run complete")

                # Messages
                messages = self._get_messages(thread_id)
                assistant_msgs = [m for m in messages if m.role == "assistant"]

                if len(assistant_msgs) < 2:
                    raise RuntimeError("Expected at least 2 assistant messages, got fewer.")
                logger.debug(f"Found {len(assistant_msgs)} assistant messages in thread {thread_id}")

                messages = self._get_messages(thread_id)
                assistant_msgs = [m for m in messages if m.role == "assistant"]
                prediction_text = assistant_msgs[1].content[0]["text"]["value"]
             
           
                predictions = self._parse_prediction(prediction_text)


             
                if not predictions:
                    logger.error("No predictions found in the response.")
                    raise ValueError("No predictions found in the response.")
                if not isinstance(predictions, list):
                    logger.error(f"Predictions are not a list: {type(predictions)}")
                    raise ValueError(f"Predictions should be a list, got {type(predictions)}")
                if not all(isinstance(p, dict) for p in predictions):
                    logger.error("Not all predictions are dictionaries.")
                    raise ValueError("All predictions should be dictionaries.")
                if not all("ticker" in p and "prediction" in p for p in predictions):
                    logger.error("Some predictions are missing 'ticker' or 'prediction' keys.")
                    raise ValueError("Each prediction must have 'ticker' and 'prediction' keys.")
                logger.debug(f"Predictions: {json.dumps(predictions, indent=2)[:200]}...")  # Log first 200 chars for brevity


                result = {
                    "thread_id": thread_id,
                    "predictions": predictions,
                }
                logger.info(f"Returning result with {len(result['predictions'])} predictions")
                return result
            except Exception as e:
                logger.error(f"❌ GPT call failed: {type(e).__name__} | {e}")
                logger.debug("Full exception info", exc_info=True)
                raise
        elif self.use_trapi:
            if not research_text or not research_prompt:
                logger.error("Both research_text and research_prompt must be provided when using TRAPI.")
                raise ValueError("Both research_text and research_prompt must be provided when using TRAPI.")
            try:
                messages = [
                    {"role" : "user" , "content": research_prompt},
                    {"role" : "assistant", "content": research_text},
                    {"role" : "user", "content": prompt}
                ]

                logger.debug("Running TRAPI multi-turn call with messages:")
                for msg in messages:
                    logger.debug(f"{msg['role']}: {msg['content'][:1000]}...")  # Log first 100 chars for brevity

                forecast = self.trapi._llm_call_generic_multiturn(messages)
                logger.info("✅ Forecast run complete")

                predictions = self._parse_prediction(forecast)
           
             
                if not predictions:
                    logger.error("No predictions found in the response.")
                    raise ValueError("No predictions found in the response.")
                if not isinstance(predictions, list):
                    logger.error(f"Predictions are not a list: {type(predictions)}")
                    raise ValueError(f"Predictions should be a list, got {type(predictions)}")
                if not all(isinstance(p, dict) for p in predictions):
                    logger.error("Not all predictions are dictionaries.")
                    raise ValueError("All predictions should be dictionaries.")
                if not all("ticker" in p and "prediction" in p for p in predictions):
                    logger.error("Some predictions are missing 'ticker' or 'prediction' keys.")
                    raise ValueError("Each prediction must have 'ticker' and 'prediction' keys.")
                logger.debug(f"Predictions: {json.dumps(predictions, indent=2)[:200]}...")  # Log first 200 chars for brevity
             

                result = {
                    "predictions": predictions,
                }
                logger.info(f"Returning result with {len(result['predictions'])} predictions")
                return result
            except Exception as e:
                logger.error(f"❌ GPT call failed: {type(e).__name__} | {e}")
                logger.debug("Full exception info", exc_info=True)
                raise

    def two_part_call_gpt(self, prompt: str, response: str) -> Dict[str, Any]:
        try:
            # Step 1: Research
            research_result = self.research_call_gpt(prompt)
           
            # Step 2: Forecast
            if self.use_trapi:
                forecast_result = self.prediction_call_gpt(
                    response,
                    research_text=research_result["raw_text"],
                    research_prompt=prompt
                )
            else:
                forecast_result = self.prediction_call_gpt(
                    response,
                    thread_id=research_result["thread_id"]
                )
           
            # Combine results
            result = {
                "thread_id": research_result.get("thread_id"),
                "research_report": research_result["research_report"],
                "criticism": research_result["criticism"],
                "reference_class": research_result["reference_class"],
                "base_rates": research_result["base_rates"],
                "predictions": forecast_result["predictions"],
                "urls": research_result.get("urls", [])
            }
           
            logger.info(f"Returning result with {len(result['predictions'])} predictions and {len(result.get('urls', []))} URLs")
            return result
           
        except Exception as e:
            logger.error(f"❌ Two-part GPT call failed: {type(e).__name__} | {e}")
            logger.debug("Full exception info", exc_info=True)
            raise

    @backoff.on_exception(backoff.expo, (RuntimeError, ValueError), max_tries=10, jitter=backoff.full_jitter)
    def debate_call_gpt(self, prompt: str) -> Dict[str, Any]:
        try:
            thread_agent1 = self.client.agents.threads.create()
            thread_agent2 = self.client.agents.threads.create()
            thread_agent3 = self.client.agents.threads.create()
            logger.debug(f"📬 Created threads {thread_agent1.id} {thread_agent2.id} {thread_agent3.id}")
            thread_ids = [thread_agent1.id, thread_agent2.id, thread_agent3.id]

            # Step 1: Research
            for index in range(3):
                self.client.agents.messages.create(thread_id=thread_ids[index], role="user", content=prompt)
                run = self.client.agents.runs.create(thread_id=thread_ids[index], agent_id=self.agent_id)
                self._wait_for_run(run, thread_ids[index])
                logger.info(f"✅ Research run for agent {index + 1} complete")

            # Responses
            research_reports = []
            for thread_id in thread_ids:
                messages = self._get_messages(thread_id)
                assistant_msgs = [m for m in messages if m.role == "assistant"]
                if len(assistant_msgs) < 1:
                    raise RuntimeError("Expected at least 1 assistant message, got fewer.")
                logger.info(f"Found {len(assistant_msgs)} assistant message in thread {thread_id}")
                research_reports.append(assistant_msgs[0].content[0]["text"]["value"])

            # Step 2: Show & Update
            next_prompts = []
            for i in range(3):
                prompt = f"""The following are the research reports from two other agents. Using these other reports as additional advice, and provide an updated research report.
The final research report should be purely objective, and not include any reference to the other agents."""
               
                for j in range(3):
                    if i != j:
                        prompt += f"\n\nResearch report from agent {j + 1}:\n{research_reports[j]}"

                next_prompts.append(prompt)

            for index, thread_id in enumerate(thread_ids):
                self.client.agents.messages.create(thread_id=thread_id, role="user", content=next_prompts[index])
                run = self.client.agents.runs.create(thread_id=thread_id, agent_id=self.agent_id)
                self._wait_for_run(run, thread_id)
                logger.info("✅ Update run for agent 1 complete")

            # Responses
            results = []
            for thread_id in thread_ids:
                messages = self._get_messages(thread_id)
                assistant_msgs = [m for m in messages if m.role == "assistant"]
                if len(assistant_msgs) < 2:
                    raise RuntimeError("Expected at least 2 assistant message, got fewer.")
                logger.info(f"Found {len(assistant_msgs)} assistant message in thread {thread_id}")

                # get last item from assistant_msgs
                updated_report = assistant_msgs[-1].content[0]["text"]["value"]

                results.append({
                    "thread_id": thread_id,
                    "research_report": updated_report,
                })

            return results
        except Exception as e:
            logger.error(f"❌ GPT call failed: {type(e).__name__} | {e}")
            logger.debug("Full exception info", exc_info=True)

class TRAPIAgent:
    def __init__(self, temperature: float = 1.0, useo3: bool = False):
        self.temperature = temperature
        self.useo3 = useo3
        logger.info("TRAPI Agent initialized with temperature: %s", self.temperature)

    # generic function to call the LLM
    def _llm_call(self,prompt):
        # Required packages
        from openai import AzureOpenAI
        from azure.identity import ChainedTokenCredential, AzureCliCredential, ManagedIdentityCredential, get_bearer_token_provider

        #Authenticate by trying az login first, then a managed identity, if one exists on the system)
        scope = "api://trapi/.default"
        credential = get_bearer_token_provider(ChainedTokenCredential(
            AzureCliCredential(),
            ManagedIdentityCredential(),
        ), scope)

        api_version = '2024-12-01-preview'  # Ensure this is a valid API version see: https://learn.microsoft.com/en-us/azure/ai-services/openai/api-version-deprecation#latest-ga-api-release
        if not self.useo3:
            deployment_name = 'gpt-4o_2024-11-20'  # Ensure this is a valid deployment name see https://aka.ms/trapi/models for the deployment name
        elif self.useo3:
            deployment_name = 'o3_2025-04-16'
        instance = 'msrne/shared' # See https://aka.ms/trapi/models for the instance name
        endpoint = f'https://trapi.research.microsoft.com/{instance}'

        #Create an AzureOpenAI Client
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=credential,
            api_version=api_version,
        )

        #Do a chat completion and capture the response
        response = client.chat.completions.create(
            temperature=self.temperature,
            model=deployment_name,
            messages=[
                {
                    "role": "system",
                    "content": "You are a prompt rewriter. Your job is to take the user prompt and rewrite it. Do not change any of the details, keep the same market tickers, dates, rules, and other specifics. However do change the wording of the instructions. Do not change the required output format (i.e., JSON keys and section headers like 'RESEARCH REPORT', 'REFERENCE CLASS', 'BASE RATES', 'CRITICISM'). Your output should be a single string that is the rewritten prompt.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ]
        )

        #Parse out the message and print
        response_content = response.choices[0].message.content
        return(response_content)
 
    @backoff.on_exception(
        backoff.expo,
        (openai.RateLimitError, openai.APITimeoutError, openai.APIError, Exception),
        max_tries=10,
        jitter=backoff.full_jitter
    )
    def _llm_call_generic(self,prompt):
        # Required packages
        from openai import AzureOpenAI
        from azure.identity import ChainedTokenCredential, AzureCliCredential, ManagedIdentityCredential, get_bearer_token_provider

        #Authenticate by trying az login first, then a managed identity, if one exists on the system)
        scope = "api://trapi/.default"
        credential = get_bearer_token_provider(ChainedTokenCredential(
            AzureCliCredential(),
            ManagedIdentityCredential(),
        ), scope)

        api_version = '2024-12-01-preview'  # Ensure this is a valid API version see: https://learn.microsoft.com/en-us/azure/ai-services/openai/api-version-deprecation#latest-ga-api-release
        deployment_name = 'gpt-4o_2024-08-06'  # Ensure this is a valid deployment name see https://aka.ms/trapi/models for the deployment name
        instance = 'gcr/shared' # See https://aka.ms/trapi/models for the instance name
        endpoint = f'https://trapi.research.microsoft.com/{instance}'

        #Create an AzureOpenAI Client
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=credential,
            api_version=api_version,
        )

        #Do a chat completion and capture the response
        response = client.chat.completions.create(
            model=deployment_name,
            temperature=self.temperature,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                },
            ]
        )

        #Parse out the message and print
        response_content = response.choices[0].message.content
        return(response_content)
 
    def _llm_call_generic_multiturn(self,messages: List[Dict[str, str]]):
        # Required packages
        from openai import AzureOpenAI
        from azure.identity import ChainedTokenCredential, AzureCliCredential, ManagedIdentityCredential, get_bearer_token_provider


        #Authenticate by trying az login first, then a managed identity, if one exists on the system)
        scope = "api://trapi/.default"
        credential = get_bearer_token_provider(ChainedTokenCredential(
            AzureCliCredential(),
            ManagedIdentityCredential(),
        ), scope)

        api_version = '2024-12-01-preview'  # Ensure this is a valid API version see: https://learn.microsoft.com/en-us/azure/ai-services/openai/api-version-deprecation#latest-ga-api-release
        deployment_name = 'gpt-4o_2024-08-06'  # Ensure this is a valid deployment name see https://aka.ms/trapi/models for the deployment name
        instance = 'gcr/shared' # See https://aka.ms/trapi/models for the instance name
        endpoint = f'https://trapi.research.microsoft.com/{instance}'

        #Create an AzureOpenAI Client
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=credential,
            api_version=api_version,
        )

        #Do a chat completion and capture the response
        response = client.chat.completions.create(
            temperature=self.temperature,
            model=deployment_name,
            max_completion_tokens=2048*4,
            messages=messages
        )

        #Parse out the message and print
        response_content = response.choices[0].message.content
        return(response_content)

    def rewrite_summary(self, summary: str, prompt = None) -> str:
        """
        Rewrite the summary prompt using TRAPI.
        """
        prompt = prompt or f"""You will see a research summary. Reword it, but don't change the content at all.\n\nf{summary}\n\nRespond with the rewritten summary only, no other text."""
        return self._llm_call_generic(prompt)

class ScraperAgent:
    def __init__(self):
        pass

    async def get_text_from_single_url(self,page, url):
        try:
            await page.goto(url, timeout=15000)
            content = await page.content()
            return url, content
        except Exception as e:
            return url, f"[ERROR] {str(e)}"

    async def get_text_from_urls_async(self,urls):
        # to-do check the db and only scrape if not already scraped within the last 24 hours
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            # 🚀 Open a page per URL and scrape them in parallel
            results = await asyncio.gather(
                *[self.scrape_url(context, url) for url in urls]
            )

            await browser.close()
            return dict(results)

    async def scrape_url(self, context, url):
        page = await context.new_page()
        result = await self.get_text_from_single_url(page, url)
        await page.close()
        return result
   
    def parse_with_readability(self,html_content: str) -> Dict[str, str]:
        """Extract title and main content from HTML using Readability."""
        doc = Document(html_content)
       
        title = doc.short_title()
        summary_html = doc.summary()  # This is HTML
       
        # Optional: clean plain text version using BeautifulSoup
        summary_text = BeautifulSoup(summary_html, 'html.parser').get_text(separator="\n", strip=True)
       
        return {
            "title": title,
            "content_html": summary_html,
            "content_text": summary_text
        }
 
    def save_results(self, results: Dict[str, str], event_ticker: str):
        # To-do: save such that each scraped url is one line, remove nesting.
        data = {
            "event_ticker": event_ticker,
            "timestamp": datetime.utcnow().isoformat(),
            "texts": results
        }
        db["news_data"].insert_one(data)
        logger.info(f"Saved scraping results for {event_ticker} to database")

class PromptWriter:
    def __init__(self, base_rate: bool = False, reference_class: bool = False, rewrite_prompts: bool = False):
        self.base_rate = base_rate
        self.reference_class = reference_class
        self.rewrite_prompts = rewrite_prompts

    def _build_static_market_data(self, event) -> str:
        """Build the static market data section that should NOT be rewritten"""
        for m in event.markets:
            if isinstance(m.expiration, str):
                m.expiration = parser.parse(m.expiration)

        market_descriptions = ""
        for idx, m in enumerate(event.markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m.ticker}
Title: {m.title}
Subtitle: {m.yes_sub_title}
Possible Outcomes: Yes (0) or No (1)
Rules: {m.rules_primary}"""

            # ✅ FIXED: Inside the loop
            if isinstance(m.rules_secondary, str) and len(m.rules_secondary) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
            market_descriptions += f"\nScheduled close date: {m.expiration.strftime('%Y-%m-%d')}\n\n"
     
        prompt = f"""EVENT AND MARKET DETAILS:
The following are markets under the event titled "{event.title}". The markets can resolve before the scheduled close date.

{market_descriptions}"""

        return prompt
 
    def build_instruction_block(self) -> str:
        instruction_block = ""
        if self.base_rate and self.reference_class:
            instruction_block = "Include all information needed to create base rates and reference classes."
        elif self.base_rate:
            instruction_block = "Include all information needed to create base rates."
        elif self.reference_class:
            instruction_block = "Include all information needed to create reference classes."
     
        ref_class_block = """REFERENCE CLASS:
Write 1 paragraph on what the reference class should be."""
        base_rates_block = """BASE RATES:
Write 1 paragraph on what the base rates should be."""

        full_rcbr_block = ""
        if self.reference_class and self.base_rate:
            full_rcbr_block = f"""{ref_class_block}\n\n{base_rates_block}"""
        elif self.reference_class:
            full_rcbr_block = ref_class_block
        elif self.base_rate:
            full_rcbr_block = base_rates_block

        prompt = f"""
                You are an expert superforecaster, familiar with the work of Philip Tetlock.

                # Instructions
                Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

                RESEARCH REPORT:
                Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting. {instruction_block}

                {full_rcbr_block}
                """
        return prompt

    def build_research_prompt(self, event) -> str:
        market_descriptions = self._build_static_market_data(event)
        logger.debug(f"Built static market data:\n{market_descriptions[:500]}...")  # Log first 500 chars for brevity
        instructions1 = self.build_instruction_block()
        logger.debug(f"Built instruction block:\n{instructions1[:500]}...")  # Log first 500 chars for brevity

        if self.rewrite_prompts:
            instructions1 = self._rewrite_prompts(instructions1)
            logger.debug(f"Rewritten instruction block for run :\n{instructions1[:500]}...")  # Log first 500 chars for brevity

        prompt1 = f"""
{market_descriptions}

{instructions1}
        """
        logger.debug(f"Generated prompt1 for run :\n{prompt1[:500]}...")  # Log first 500 chars for brevity
        return prompt1

    def build_prediction_prompt(self, n_markets) -> str:
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {n_markets}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
[{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
"reasoning": "A brief explanation of how you arrived at the prediction",
"prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

Your goal is to make the best possible prediction, using all relevant information."""
        return prompt2
   
    def _rewrite_prompts(self,prompt) -> str:
        """
        Rewrites the prompts using the TRAPIAgent.
        """
        self.trapi = TRAPIAgent()
        logger.info(f"Rewriting prompts")
        rewritten_prompt = self.trapi._llm_call(prompt)
        logger.info(f"Prompts rewritten successfully")
        return rewritten_prompt
class ResearcherAgent:
    """writes research reports"""
    def __init__(self, event, temperature = "high", search: bool = True, agent = None, prompt_writer: Optional[PromptWriter] = None, base_rate: bool = False, reference_class: bool = False):
        self.temperature = temperature
        self.search = search
        self.event = event
        self.base_rate = base_rate
        self.reference_class = reference_class
        logger.info("Researcher Agent initialized with temperature: %s", self.temperature)
        if agent is not None:
            logger.info("Using provided agent instance")
            self.agent = agent
        elif agent is None and self.search:
            self.agent = LLMAgent(temp=self.temperature, search=True, rewrite_prompts=False, use_trapi=False)
        elif not self.search:
            self.agent = LLMAgent(temp=self.temperature, search=False, rewrite_prompts=False, use_trapi=True)
        if prompt_writer is not None:
            logger.info("Using provided PromptWriter instance")
            self.prompt_writer = prompt_writer
        elif prompt_writer is None:
            logger.info("Creating new PromptWriter instance")
            self.prompt_writer = PromptWriter(base_rate=self.base_rate, reference_class=self.reference_class)


    def research(self, event: ForecastingEvent, run_id: int, experiment_id: Optional[str] = None, do_scrape: bool = False):
        research_prompt = self.prompt_writer.build_research_prompt(event)
        logger.debug(f"Generated research prompt for run {run_id}:\n{research_prompt[:500]}...")  # Log first 500 chars for brevity
        research_result = self.agent.research_call_gpt(prompt=research_prompt)
        logger.info(f"Research completed for run {run_id} with result: {research_result.get('research_report', '')[:100]}...")  # Log first 100 chars for brevity
        if do_scrape:
            logger.info(f"Scraping URLs for run {run_id}")
            urls = self.agent.scraper_agent.get_text_from_urls_async(research_result.get("urls", []))
            logger.info(f"Scraping completed for run {run_id} with {len(urls)} URLs")
 
class PredictorAgent:
    """takes research reports and makes predictions"""
    def __init__(self, event, temperature: float = 1.0, search: bool = True, agent: Optional[LLMAgent] = None, prompt_writer: Optional[PromptWriter] = None, base_rate: bool = False, reference_class: bool = False):
        self.temperature = temperature
        self.search = search
        self.event = event
        self.base_rate = base_rate
        self.reference_class = reference_class
        logger.info("Predictor Agent initialized with temperature: %s", self.temperature)
        if agent is not None:
            logger.info("Using provided agent instance")
            self.agent = agent
        elif agent is None and self.search:
            self.agent = LLMAgent(temp=self.temperature, search=True, rewrite_prompts=False, use_trapi=False)
        elif not self.search:
            self.agent = LLMAgent(temp=self.temperature, search=False, rewrite_prompts=False, use_trapi=True)
        if prompt_writer is not None:
            logger.info("Using provided PromptWriter instance")
            self.prompt_writer = prompt_writer
        elif prompt_writer is None:
            logger.info("Creating new PromptWriter instance")
            self.prompt_writer = PromptWriter(base_rate=self.base_rate, reference_class=self.reference_class)

    def predict(self, event: ForecastingEvent, run_id: int, experiment_id: Optional[str] = None):
        prediction_prompt = self.prompt_writer.build_prediction_prompt(len(event.markets))
        logger.debug(f"Generated prediction prompt for run {run_id}:\n{prediction_prompt[:500]}...")
        prediction_result = self.agent.prediction_call_gpt(prediction_prompt)
        logger.info(f"Prediction completed for run {run_id} with result: {prediction_result.get('predictions', [])[:5]}...")

   
class ForecastingRun:

    def __init__(self, event: ForecastingEvent, agent: LLMAgent, condition: Dict[str, bool], run_id: int, condition_label: str, persona_id:Optional[str] = None, persona_blurb: Optional[str] = None, rewrite_prompts: bool = False, experiment_id: Optional[str] = None, do_scrape = False):
        self.event = event
        self.agent = agent
        self.experiment_id = experiment_id
        self.condition = condition
        self.condition_label = condition_label
        self.run_id = run_id
        self.result = None
        self.persona_id = persona_id
        self.persona_blurb = persona_blurb
        self.prompt1 = None
        self.prompt2 = None
        self.base_rate = condition.get("base_rate")
        self.reference_class = condition.get("ref_class")
        self.rewrite_prompts = rewrite_prompts
        self.uuid = str(uuid.uuid4())
        self.do_scrape = do_scrape
        if self.rewrite_prompts:
            logger.info("Using TRAPI agent for prompt rewriting")
            self.trapi = TRAPIAgent()
        if do_scrape:
            logger.info("Using ScraperAgent for web scraping")
            self.scraper_agent = ScraperAgent()

    def _build_static_market_data(self) -> str:
        """Build the static market data section that should NOT be rewritten"""
        for m in self.event.markets:
            if isinstance(m.expiration, str):
                m.expiration = parser.parse(m.expiration)

        market_descriptions = ""
        for idx, m in enumerate(self.event.markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m.ticker}
Title: {m.title}
Subtitle: {m.yes_sub_title}
Possible Outcomes: Yes (0) or No (1)
Rules: {m.rules_primary}"""

            # ✅ FIXED: Inside the loop
            if isinstance(m.rules_secondary, str) and len(m.rules_secondary) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
            market_descriptions += f"\nScheduled close date: {m.expiration.strftime('%Y-%m-%d')}\n\n"
     
        prompt = f"""EVENT AND MARKET DETAILS:
The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.

{market_descriptions}"""

        return prompt
 
    def build_instruction_block(self) -> str:
        instruction_block = ""
        if self.base_rate and self.reference_class:
            instruction_block = "Include all information needed to create base rates and reference classes."
        elif self.base_rate:
            instruction_block = "Include all information needed to create base rates."
        elif self.reference_class:
            instruction_block = "Include all information needed to create reference classes."
     
        ref_class_block = """REFERENCE CLASS:
Write 1 paragraph on what the reference class should be."""
        base_rates_block = """BASE RATES:
Write 1 paragraph on what the base rates should be."""

        full_rcbr_block = ""
        if self.reference_class and self.base_rate:
            full_rcbr_block = f"""{ref_class_block}\n\n{base_rates_block}"""
        elif self.reference_class:
            full_rcbr_block = ref_class_block
        elif self.base_rate:
            full_rcbr_block = base_rates_block

        prompt = f"""
                You are an expert superforecaster, familiar with the work of Philip Tetlock.

                # Instructions
                Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

                RESEARCH REPORT:
                Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting. {instruction_block}

                {full_rcbr_block}
                """
        return prompt



    def build_prompt(self) -> str:
        market_descriptions = self._build_static_market_data()
        logger.debug(f"Built static market data for run {self.run_id}:\n{market_descriptions[:500]}...")  # Log first 500 chars for brevity
        instructions1 = self.build_instruction_block()
        logger.debug(f"Built instruction block for run {self.run_id}:\n{instructions1[:500]}...")  # Log first 500 chars for brevity

        if self.rewrite_prompts:
            instructions1 = self._rewrite_prompts(instructions1)
            logger.debug(f"Rewritten instruction block for run {self.run_id}:\n{instructions1[:500]}...")  # Log first 500 chars for brevity

        prompt1 = f"""
{market_descriptions}

{instructions1}
        """
        logger.debug(f"Generated prompt1 for run {self.run_id}:\n{prompt1[:500]}...")  # Log first 500 chars for brevity
             
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
[{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
"reasoning": "A brief explanation of how you arrived at the prediction",
"prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

Your goal is to make the best possible prediction, using all relevant information."""
     
        if self.rewrite_prompts:
            prompt2 = self._rewrite_prompts(prompt2)
        logger.debug(f"Generated prompt2 for run {self.run_id}:\n{prompt2[:500]}...")  # Log first 500 chars for brevity
     
        logger.debug(f"Generated prompt for run {self.run_id}:\n{prompt1}...")
        logger.debug(f"Generated prediction prompt for run {self.run_id}:\n{prompt2}...")
        return prompt1, prompt2

    def build_random_persona_prompt(self) -> str:
        for m in self.event.markets:
            if isinstance(m.expiration, str):
                m.expiration = parser.parse(m.expiration)
           
        market_descriptions = ""

        for idx, m in enumerate(self.event.markets):
            market_descriptions += f"""# Market {idx + 1}
    Ticker: {m.ticker}
    Title: {m.title}
    Subtitle: {m.yes_sub_title}
    Possible Outcomes: Yes (0) or No (1)
    Rules: {m.rules_primary}"""

            if isinstance(m.rules_secondary, str) and len(m.rules_secondary) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
            market_descriptions += f"\nScheduled close date: {m.expiration.strftime('%Y-%m-%d')}\n\n"

        prompt1 = f"""{self.persona_blurb}
    The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.

    {market_descriptions}

    # Instructions
    Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

    RESEARCH REPORT:
    Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting."""
       
        # ALWAYS use array format for consistency, even with single market
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

    Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
    [{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
    "reasoning": "A brief explanation of how you arrived at the prediction",
    "prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

    Your goal is to make the best possible prediction, using all relevant information."""
   
        logger.debug(f"Generated prompt for run {self.run_id}:\n{prompt1}...")
        logger.debug(f"Generated prediction prompt for run {self.run_id}:\n{prompt2}...")
        return prompt1, prompt2

    def _rewrite_prompts(self,prompt) -> str:
        """
        Rewrites the prompts using the TRAPIAgent.
        """
        if self.trapi is None:
            raise RuntimeError("TRAPI agent not initialized but rewrite_prompts is True")

        logger.info(f"Rewriting prompts for run {self.run_id} with condition {self.condition_label}")
        rewritten_prompt = self.trapi._llm_call(prompt)
        logger.info(f"Prompts rewritten successfully for run {self.run_id}")
        return rewritten_prompt

    def execute(self) -> str:
        test_db_connection()
        logger.info(f"Executing run {self.run_id} for event {self.event.event_ticker} with condition {self.condition_label}")
        if self.condition_label == "random" and self.persona_blurb:
            self.prompt1, self.prompt2 = self.build_random_persona_prompt()
        else:
            self.prompt1, self.prompt2 = self.build_prompt()

        logger.debug(f"\n\nPromt1: {self.prompt1}...")  # Log first 100 chars for brevity
        logger.debug(f"Prompt2: {self.prompt2}...")  # Log first 100 chars for brevity
        self.result = self.agent.two_part_call_gpt(self.prompt1, self.prompt2)
        if self.do_scrape:
            logger.info(f"Scraping URLs for run {self.run_id}")
            urls = self.result.get("urls", [])
            urls = [u['url'] for u in urls if isinstance(u, dict) and 'url' in u]
            urls = list(set(urls))  # Remove duplicates
            if not urls:
                logger.warning("No URLs found in the result, skipping scraping.")
            else:
                scraped_content = asyncio.run(self.scraper_agent.get_text_from_urls_async(urls))
                self.result["scraped_content"] = scraped_content
                logger.info(f"Scraped content for {len(scraped_content)} URLs in run {self.run_id}")
                if scraped_content:
                    self.scraper_agent.save_results(scraped_content, self.event.event_ticker)
                    logger.info(f"Saved scraped content for event {self.event.event_ticker} in run {self.run_id}")
        logger.debug(f"Run {self.run_id} completed with result: {str(self.result)[:600000]}...")
        return self.result

    def save(self):
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "criticism": self.result.get("criticism"),
            "research_report": self.result.get("research_report"),
            "reference_class": self.result.get("reference_class"),
            "base_rates": self.result.get("base_rates"),
            "marketday": str(date.today()),
            "timestamp": datetime.utcnow(),
            "condition_desc": self.condition,
            "condition": self.condition_label,
            "persona_id": self.persona_id,
            "persona_blurb": self.persona_blurb,
            "temperature": self.agent.temp,
            "search": self.agent.search,
            "agent_id": self.agent.agent_id,
            "run_type": "forecasting",
            "urls": self.result.get("urls", []),
            "run_id": self.run_id,
            "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None,
            "for_experiment": True,
            "rewrite_prompts": self.rewrite_prompts,
            "uuid": self.uuid,
        }
        logger.debug(f"Event level data prepared for save {self.run_id}")

        market_level_data = []
        for m in self.result.get("predictions", []):
                # Find the corresponding row in flat by ticker
                match = self.event.flat_df[self.event.flat_df["ticker"] == m['ticker']]
             
                if not match.empty:
                    row = match.iloc[0]  # get the first (and likely only) match
                 
                    enriched_market = {
                        **m,
                        "condition": self.condition_label,
                        "event": self.event.event_ticker,
                        "marketday": str(date.today()),
                        "timestamp": datetime.utcnow(),
                        "market_title": row["title"],
                        "mid_price": row["mid_price"],
                        "implied_prob": row["yes_implied_prob"],
                        "volume": row["volume"],
                        'category': row["event_category"],
                        "extremity": row["extremity"],
                        "run_type": "forecasting",
                        "expiration_time": row["expiration_time"],
                        "persona_id": self.persona_id,
                        "persona_blurb": self.persona_blurb,
                        "temperature": self.agent.temp,
                        "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None,
                        "search": self.agent.search,
                        "run_id": self.run_id,
                        "rewrite_prompts": self.rewrite_prompts,
                        "uuid": self.uuid,
                    }
                    market_level_data.append(enriched_market)
                else:
                    logger.warning(f"⚠️ No match found in flat for ticker: {m.ticker}")
        market_level_data = [to_python_types(d) for d in market_level_data]
        logger.debug(f"Market level data prepared for save {self.run_id}: {len(market_level_data)} markets")

        log_run_to_db(event_level_data, market_level_data)
        logger.info(f"💾 Run {self.run_id} saved to database for event {self.event.event_ticker}")

class ForecastingFromSeed:
    def __init__(self, event: ForecastingEvent, rewriter_agent: LLMAgent, forecasting_agent: LLMAgent, run_id: int,seed_summary: str, repetitions: int, rewrite_strategy: str = "sequential", difference_level = "similar", prompt_writer: Optional[PromptWriter] = None, enable_search_rewrite: bool = False, experiment_id: Optional[str] = None):
        if difference_level not in ["similar", "different", "same", 'six_points']:
            raise ValueError("difference_level must be either 'similar', 'different', or 'same'")
        if rewrite_strategy not in ["independent", "all_at_once"] and difference_level != "same":
            raise ValueError("rewrite_strategy must be either 'independent' or 'all_at_once'")
        self.event = event
        self.rewriter_agent = rewriter_agent
        self.run_id = run_id
        self.seed_summary = seed_summary
        self.repetitions = repetitions
        self.rewrite_strategy = rewrite_strategy
        self.difference_level = difference_level
        self.rewritten_seed_summaries = []
        self.prompt_writer = prompt_writer if prompt_writer else PromptWriter(base_rate=False, reference_class=False)
        self.event_desc_block = self.prompt_writer._build_static_market_data(event)
        self.research_prompt = self.prompt_writer.build_research_prompt(event)
        self.rewrite_prompt = None
        self.make_rewrite_prompt()
        self.uuid = str(uuid.uuid4())
        self.result = None
        self.enable_search_rewrite = enable_search_rewrite
        self.forecasting_agent = forecasting_agent
        self.experiment_id = experiment_id


    def make_rewrite_prompt(self, label=None):
        if self.difference_level == "similar" and self.rewrite_strategy == "independent":
            self.rewrite_prompt = f"See the research report below for this event {self.event_desc_block}.\n\nDo more research and create a research report that takes a very similar perspective (in terms of implied probability of the event) from the one provided:\n\n{self.seed_summary}"
        elif self.difference_level == "different" and self.rewrite_strategy == "independent":
            self.rewrite_prompt = f"""See the research report below for this event {self.event_desc_block}.\n\nDo more research and create a research report that takes a very different perspective (in terms of implied probability of the event) from the one provided: \n\n{self.seed_summary}"""
        elif self.difference_level == "six_points" and self.rewrite_strategy == "independent":
            self.rewrite_prompt = f"See the research report below for this event {self.event_desc_block}.\n\nDo more research and create a research report that takes a perspective that is {label} (in terms of implied probability of the event) from the one provided:\n\n{self.seed_summary}"
       
        elif self.difference_level == 'similar' and self.rewrite_strategy == "all_at_once":
            self.rewrite_prompt = f"""See the research report below for this event {self.event_desc_block}.\n\nDo more research and create 6 research reports that take very similar perspectives (in terms of implied probability of the event) from each other. Delimit your summaries with *****\n\n{self.seed_summary}."""
        elif self.difference_level == 'different' and self.rewrite_strategy == "all_at_once":
            self.rewrite_prompt = f"""See the research report below for this event {self.event_desc_block}.\n\nDo more research and create 6 research reports that take very different perspectives (in terms of implied probability of the event) from each other. Delimit your summaries with *****\n\n{self.seed_summary}"""
        elif self.difference_level == 'six_points' and self.rewrite_strategy == "all_at_once":
            self.rewrite_prompt = f"""See the research report below for this event {self.event_desc_block}. \n\nConsider a six-point scale describing research reports that the event is more or less likely to happen compared this one.\n6 - very much more likely\n5 - moderately more likely\n4 - a little bit more likely\n3 - a little bit less likely\n2 - moderately less likely\n1 - very much less likely\n\nDo more research and generate 6 research reports that take each of these perspectives. Delimit your summaries with *****\n\n{self.seed_summary}"""

    def rewrite_summaries(self) -> str:
        """
        Rewrite the seed summary using the TRAPI agent.
        """
        if not hasattr(self, 'trapi') and not self.enable_search_rewrite and self.rewriter_agent.use_trapi:
            self.trapi = TRAPIAgent()
            logger.info(f"Rewriting seed summary for run {self.run_id} with condition {self.difference_level}")
            rewritten_summary = self.trapi.rewrite_summary(self.seed_summary, prompt=self.rewrite_prompt)
            logger.info(f"Seed summary rewritten successfully for run {self.run_id}")
            return rewritten_summary
        elif self.enable_search_rewrite and not self.rewriter_agent.use_trapi:
            logger.info(f"Rewriting seed summary for run {self.run_id} with condition {self.difference_level} using agent")
            rewritten_summary = self.rewriter_agent._generic_azure_call(prompt=self.rewrite_prompt)
            logger.info(f"Seed summary rewritten successfully for run {self.run_id}")
            return rewritten_summary
        else:
            raise RuntimeError("TRAPI agent not initialized but enable_search_rewrite is False and rewriter_agent.use_trapi is True")

    def rewrite_all_at_once(self) -> List[str]:
        """
        Rewrite all seed summaries at once using the TRAPI agent.
        """
        if not hasattr(self, 'trapi') and not self.enable_search_rewrite:
            self.trapi = TRAPIAgent()
       
        if not self.enable_search_rewrite:
            logger.info(f"Rewriting all seed summaries for run {self.run_id} with condition {self.difference_level}")
            rewritten_summaries = self.trapi._llm_call_generic(
            prompt=self.rewrite_prompt,
            messages=[{"role": "user", "content": self.seed_summary}],
            temperature=0.7,  # Adjust temperature as needed
            max_tokens=2048 * 4  # Adjust max tokens as needed
            )
        elif self.enable_search_rewrite:
            logger.info(f"Rewriting all seed summaries for run {self.run_id} with condition {self.difference_level} using agent")
            rewritten_summaries = self.rewriter_agent._generic_azure_call(prompt=self.rewrite_prompt)

        rewritten_summaries = self._summaries_to_list(rewritten_summaries)
        return rewritten_summaries
       
    def _summaries_to_list(self, summaries: str) -> List[str]:
        """
        Convert a string of summaries into a list.
        Assumes summaries are separated by newlines or semicolons.
        """
        if not summaries:
            return []
        return [s.strip() for s in summaries.split('*****') if s.strip()]
   
    def get_rewritten_summaries(self) -> List[str]:
        # rewrite seed research
        if self.rewrite_strategy == "independent" and self.difference_level == "different":
            rewritten_summaries = []
            for _ in range(self.repetitions):
                summary_rewrite = self.rewrite_summaries()
                rewritten_summaries.append(summary_rewrite)
            logger.info(f"Rewritten seed summaries for run {self.run_id}: {rewritten_summaries[:3]}...")  # Log first 3 summaries for brevity
            self.rewritten_seed_summaries = rewritten_summaries

        elif self.rewrite_strategy == "independent" and self.difference_level == "similar":
            rewritten_summaries = []
            for _ in range(self.repetitions):
                summary_rewrite = self.rewrite_summaries()
                rewritten_summaries.append(summary_rewrite)
            logger.info(f"Rewritten seed summaries for run {self.run_id}: {rewritten_summaries[:3]}...")
            self.rewritten_seed_summaries = rewritten_summaries

        elif self.rewrite_strategy == "independent" and self.difference_level == "six_points":
            labels = [
                "6 - a research report that suggests the event is very much more likely",
                "5 - a research report that suggests the event is moderately more likely",
                "4 - a research report that suggests the event is a little bit more likely",
                "3 - a research report that suggests the event is a little bit less likely",
                "2 - a research report that suggests the event is moderately less likely",
                "1 - a research report that suggests the event is very much less likely"
            ]
            rewritten_summaries = []
            for label in labels:
                self.make_rewrite_prompt(label)
                summary_rewrite = self.rewrite_summaries()
                rewritten_summaries.append(summary_rewrite)
            self.rewritten_seed_summaries = rewritten_summaries
            logger.info(f"Rewritten seed summaries for run {self.run_id}: {rewritten_summaries[:3]}...")

        elif self.rewrite_strategy == "all_at_once":
            rewritten_summaries = self.rewrite_all_at_once()
            self.rewritten_seed_summaries = rewritten_summaries

        elif self.difference_level == "same":
            # If the difference level is "same", we use the original seed summary
            self.rewritten_seed_summaries = [self.seed_summary] * self.repetitions
            logger.info(f"Using original seed summary for run {self.run_id}: {self.seed_summary[:100]}...")

    def execute(self):
        logger.info(f"Executing ForecastingFromSeed for run {self.run_id} with seed summary: {self.seed_summary[:100]}...")
        prediction_prompt = self.prompt_writer.build_prediction_prompt(len(self.event.markets))
        self.get_rewritten_summaries()
       
        # make predictions from each rewritten summary
        prediction_results = []
        for i, summary in enumerate(self.rewritten_seed_summaries):
            prediction_result = self.forecasting_agent.prediction_call_gpt(prediction_prompt, research_text = summary, research_prompt=self.research_prompt)
            prediction_result['seed_summary'] = summary
            prediction_result['run_id'] = self.run_id
            prediction_result['seed_index'] = i
            prediction_result['difference_level'] = self.difference_level
            prediction_result['rewrite_strategy'] = self.rewrite_strategy
            prediction_results.append(prediction_result)
        logger.info(f"Prediction results for run {self.run_id}: {prediction_results[:3]}...")  # Log first 3 results for brevity
        self.result = prediction_results
        return prediction_results
   
    def save(self):
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "run_type": "forecasting_from_seed",
            "run_id": self.run_id,
            "seed_summary": self.seed_summary,
            "repetitions": self.repetitions,
            "difference_level": self.difference_level,
            "rewrite_strategy": self.rewrite_strategy,
            # Rewriter agent info
            "rewriter_temperature": self.rewriter_agent.temp,
            "rewriter_search": self.rewriter_agent.search,
            "rewriter_agent_id": self.rewriter_agent.agent_id,
            # Forecaster agent info
            "forecaster_temperature": self.forecasting_agent.temp if hasattr(self, "forecasting_agent") else None,
            "forecaster_search": self.forecasting_agent.search if hasattr(self, "forecasting_agent") else None,
            "forecaster_agent_id": self.forecasting_agent.agent_id if hasattr(self, "forecasting_agent") else None,
            "uuid": self.uuid,
            "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None
        }
        market_level_data = []
        for result in self.result:
            enriched_market = {
                **result,
                "event": self.event.event_ticker,
                "marketday": str(date.today()),
                "timestamp": datetime.utcnow(),
                "run_type": "forecasting_from_seed",
                "difference_level": self.difference_level,
                "rewrite_strategy": self.rewrite_strategy,
                "uuid": self.uuid,
                "implied_prob": self.event.flat_df[['ticker', 'yes_implied_prob']].to_json(orient = 'records'),
                "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None,
            }
            market_level_data.append(enriched_market)
       
        log_run_to_db(event_level_data, market_level_data)
        logger.info(f"💾 ForecastingFromSeed run {self.run_id} saved to database for event {self.event.event_ticker}")

class EnsembleFromSeed:
    """This should be similar to ensemble run, but it will take the output of 5 forecasting from seed with same + one forecasting run (for condition same).,
    or 6 forecastingfromseed runs (for similar, different, six_points) and make an ensemble prediction.
    I shold be able to save all relevant info
    i want to turn on or off search. default is no search and use trapi (ensemble without RAG)
    options for passing predictions or not, passing scraped content or not, passing summaries or not
    """
    def __init__(self, seed_uuid, event: ForecastingEvent, agent: LLMAgent, run_id: int, experiment_id: Optional[str] = None, include_median = False,   include_market_preds: bool = False, include_scraped_content: bool = False, include_summaries = True, produce_weights: bool = False):
        self.event = event
        self.seed_uuid = seed_uuid
        self.markets = event.markets
        self.agent = agent
        self.experiment_id = experiment_id if experiment_id else None
        self.run_id = run_id
        self.result = None
        self.uuid = str(uuid.uuid4())
        self.include_market_preds = include_market_preds
        self.include_median = include_median
        self.include_scraped_content = include_scraped_content
        self.include_summaries = include_summaries
        self.rewrite_strategy = None
        self.difference_level = None
        self.produce_weights = produce_weights
        logger.info(f"EnsembleFromSeed initialized for run {self.run_id} with event {self.event.event_ticker}")

    def _get_seed_summaries_from_db(self) -> Dict:
        """
        Pull the rewritten summaries from a previous ForecastingFromSeed run.
        """
        # Query market_predictions for this seed run
        market_collection = db["market_predictions"]
       
        query = {
            "uuid": self.seed_uuid,
            "run_type": "forecasting_from_seed",
            "event": self.event.event_ticker
        }
       
        logger.info(f"Querying for seed summaries with: {query}")
       
        # Get all predictions from this seed run
        cursor = market_collection.find(query).sort("seed_index", 1)
        predictions = list(cursor)
       
        # Extract the seed summaries and predictions
        summaries = []
        all_predictions = []
       
        for pred in predictions:
            # print(pred)
            if 'seed_summary' in pred:
                summaries.append(pred['seed_summary'])
                self.rewrite_strategy = pred.get('rewrite_strategy')
                self.difference_level = pred.get('difference_level')
            if 'predictions' in pred:
                # Each 'predictions' is a list containing prediction dictionaries
                all_predictions.extend(pred['predictions'])
       
        # Calculate median prediction for each ticker
        medians = None
        if all_predictions and self.include_median:
            import pandas as pd
           
            # Create DataFrame from all predictions
            df_predictions = pd.DataFrame(all_predictions)
           
            # Group by ticker and calculate median for each
            if 'ticker' in df_predictions.columns and 'prediction' in df_predictions.columns:
                # Convert predictions to numeric, handling any conversion errors
                df_predictions['prediction'] = pd.to_numeric(df_predictions['prediction'], errors='coerce')
               
                # Group by ticker and calculate median
                medians_df = df_predictions.groupby('ticker').agg({
                    'ticker': 'first',
                    'prediction': 'median'
                }).reset_index(drop=True)
               
                medians_df['prediction'] = medians_df['prediction'].round(3)  # Round to 3 decimal places
               
                # Convert to list of dictionaries
                medians = medians_df.to_dict(orient='records')
            else:
                logger.warning("Could not find 'ticker' or 'prediction' columns in predictions data")
       
        # Put everything together
        result = {'summaries': summaries}
        if self.include_market_preds:
            result['predictions'] = all_predictions
        if self.include_median and medians:
            result['medians'] = medians
       
        return result

    def build_ensemble_prompt_from_db_summaries(self, summaries: Dict) -> Tuple[str, str]:
        """Build ensemble prompt from summaries pulled from database"""
        # Market descriptions
        market_descriptions = ""
        for idx, m in enumerate(self.event.markets):
            market_descriptions += f"""# Market {idx + 1}
    Ticker: {m.ticker}
    Title: {m.title}
    Subtitle: {m.yes_sub_title}
    Possible Outcomes: Yes (0) or No (1)
    Rules: {m.rules_primary}"""
            if isinstance(m.rules_secondary, str) and len(m.rules_secondary) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
           
            # Fix the expiration date handling
            if hasattr(m.expiration, 'strftime'):
                expiration_str = m.expiration.strftime('%Y-%m-%d')
            else:
                expiration_str = str(m.expiration)  # Fallback to string conversion
            market_descriptions += f"\nScheduled close date: {expiration_str}\n\n"
       
        # Group predictions by summary index if predictions are included
        predictions_by_summary = {}
        if self.include_market_preds and 'predictions' in summaries:
            # Assuming predictions are in the same order as summaries (6 summaries = 6 predictions)
            for i, prediction in enumerate(summaries['predictions']):
                predictions_by_summary[i] = prediction
       
        # Create combined summary and prediction blocks
        combined_blocks = []
        for i, summary in enumerate(summaries['summaries']):
            block = f"Summary {i+1}:\n{summary}"
           
            # Add corresponding prediction if available
            if i in predictions_by_summary:
                pred = predictions_by_summary[i]
                block += f"\n\nPrediction derived from this summary:\nTicker: {pred['ticker']}\nPrediction: {pred['prediction']:.3f}\nReasoning: {pred['reasoning']}"
           
            combined_blocks.append(block)
       
        combined_text = "\n\n".join(combined_blocks)
       
        # Create median block if requested
        median_text = ""
        if self.include_median and 'medians' in summaries:
            median_blocks = []
            for median in summaries['medians']:
                median_blocks.append(f"Ticker: {median['ticker']}: {median['prediction']:.3f}")
            median_text = f"\n\nThe median prediction for each ticker is:\n" + "\n".join(median_blocks)
       
        intro = f"""# Research Summaries from Seed Rewriting
    Below are {len(summaries['summaries'])} research summaries generated by different agents."""
       
        # Build prompts
        prompt1 = f"""You are an expert superforecaster, familiar with the work of Philip Tetlock.
    The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.

    {market_descriptions}

    {intro}
    Your task is to synthesize the information provided and generate a final, well-reasoned probability forecast for each market.

    {combined_text}{median_text}

    # Instructions
    Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes. We expect you to answer in this format:

    CRITICISM:
    Write a paragraph critically evaluating the research summaries provided. What are the strengths and weaknesses? Are there biases or gaps?

    RESEARCH REPORT:
    Write a *final* research summary integrating your current search with the provided summaries (at least 5 paragraphs). Use plain text without markdown formatting.

    REFERENCE CLASS:
    Write 1 paragraph on what the reference class should be.

    BASE RATES:
    Write 1 paragraph on what the base rates should be."""
       
       
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

    Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
    [{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
    "reasoning": "A brief explanation of how you arrived at the prediction",
    "prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

    Your goal is to make the best possible prediction, using all relevant information."""
   
        prompt2w = f"""Now, make your predictions for each market. Include the relative weight given to each research summary in coming to this prediction. Use strict json format:

    Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
    [{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
    "reasoning": "A brief explanation of how you arrived at the prediction",
    "weights": [{{"summary_index": 0, "weight": 0.2}}, {{"summary_index": 1, "weight": 0.3}}, ...], (weights for each summary, where summary_index is the index of the summary in the original summaries list)
    "prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

    Your goal is to make the best possible prediction, using all relevant information."""
        if self.produce_weights:
            prompt2 = prompt2w
        return prompt1, prompt2
    def execute(self):
        """
        Execute ensemble from seed by:
        1. Pulling rewritten summaries from database
        2. Creating ensemble prediction
        """
        logger.info(f"Executing EnsembleFromSeed for seed run {self.seed_uuid}")
       
        # Step 1: Get summaries from database
        summaries = self._get_seed_summaries_from_db()
       
        # Step 2: Build ensemble prompt
        prompt1, prompt2 = self.build_ensemble_prompt_from_db_summaries(summaries)
       
        # Step 3: Make ensemble prediction
        ensemble_result = self.agent.two_part_call_gpt(prompt1, prompt2)
       
        # Step 4: Structure result
        self.result = {
            "ensemble_prediction": ensemble_result,
            "source_summaries": summaries,
            "num_summaries": len(summaries),
            "seed_uuid": self.seed_uuid,
            "run_id": self.run_id
        }
       
        logger.info(f"EnsembleFromSeed completed with {len(summaries)} summaries")
        return self.result
   
    def save(self):
        """Save results to database"""
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "run_type": "ensemble_from_seed",
            "run_id": self.run_id,
            "seed_uuid": self.seed_uuid,
            'rewrite_strategy': self.rewrite_strategy,
            'difference_level': self.difference_level,
            "num_summaries": len(self.result["source_summaries"]),
           
            # Ensemble results
            "criticism": self.result["ensemble_prediction"].get("criticism"),
            "research_report": self.result["ensemble_prediction"].get("research_report"),
            "reference_class": self.result["ensemble_prediction"].get("reference_class"),
            "base_rates": self.result["ensemble_prediction"].get("base_rates"),
            "urls": self.result["ensemble_prediction"].get("urls", []),
            "include_market_preds": self.include_market_preds,
            "include_median": self.include_median,
            "produce_weights": self.produce_weights,
           
            # Agent info
            "ensemble_temperature": self.agent.temp,
            "ensemble_search": self.agent.search,
            "ensemble_agent_id": self.agent.agent_id,
           
            # Metadata
            "uuid": self.uuid,
            "experiment_id": self.experiment_id,
            "marketday": str(date.today()),
            "timestamp": datetime.utcnow(),
        }
       
        market_level_data = []
        for prediction in self.result["ensemble_prediction"].get("predictions", []):
            enriched_market = {
                **prediction,
                "event": self.event.event_ticker,
                "marketday": str(date.today()),
                "timestamp": datetime.utcnow(),
                "run_type": "ensemble_from_seed",
                "seed_uuid": self.seed_uuid,
                "num_summaries": len(self.result["source_summaries"]),
                "uuid": self.uuid,
                'rewrite_strategy': self.rewrite_strategy,
                'difference_level': self.difference_level,
                'include_market_preds': self.include_market_preds,
                'include_median': self.include_median,
                "experiment_id": self.experiment_id,
                "produce_weights": self.produce_weights,
                "run_id": self.run_id,
            }
            market_level_data.append(enriched_market)
       
        log_run_to_db(event_level_data, market_level_data)
        logger.info(f"💾 EnsembleFromSeed run {self.run_id} saved to database")

class EnsembleRun:
    # To-do abstract away the common functionality of the ForecastingRun and EnsembleRun
    # To-do separate the two part gpt call and use Predictor and Researcher agents
    def __init__(self, event: ForecastingEvent, agent: LLMAgent, run_id: int, condition:str, number_of_summaries: int = 4, experiment_id: Optional[str] = None, use_trapi: bool = False, rewrite_prompts: bool = False, exclusive_summaries = True, include_market_preds: bool = False, include_scraped_content: bool = False, include_summaries = True, ingredient_temp = 'high', rewrite_summaries: bool = False, rewrite_summaries_number: int = 1):
        self.event = event
        self.markets = event.markets
        self.agent = agent
        self.experiment_id = experiment_id if experiment_id else None
        self.run_id = run_id
        self.condition = condition
        self.number_of_summaries = number_of_summaries
        self.condition_label = condition
        self.reference_class = True
        self.base_rate = True
        self.prompt = None
        self.rewrite_prompts = rewrite_prompts
        self.result = None
        self.use_trapi = use_trapi
        self.uuid = str(uuid.uuid4())
        self.exclusive_summaries = exclusive_summaries
        self.include_market_preds = include_market_preds
        self.include_scraped_content = include_scraped_content
        self.ingredient_temp = ingredient_temp
        self.include_summaries = include_summaries
        self.rewrite_summaries = rewrite_summaries
        self.rewrite_summaries_number = rewrite_summaries_number
        self.input_summaries = self._get_summaries(self.condition, number_of_summaries = self.number_of_summaries, shuffle=True, include_market_preds=include_market_preds, ingredient_temp=ingredient_temp,         rewrite_summaries=self.rewrite_summaries,
        rewrite_summaries_number=self.rewrite_summaries_number,)


    def _get_summaries(self, condition_label, number_of_summaries, shuffle=True, include_market_preds: bool = False, ingredient_temp='high', rewrite_summaries = False, rewrite_summaries_number = 1) -> List[Dict[str, Any]]:
        logger.info(f"Downloading {number_of_summaries} summaries for condition '{condition_label}' with ingredient temperature '{ingredient_temp}' and include_market_preds={include_market_preds}")
        collection = db["event_predictions"]
        base_query = {
            "event": self.event.event_ticker,
            'temperature': ingredient_temp,
            "marketday": {"$gte": str(date.today() - pd.Timedelta(days=3))},
            'rewrite_prompts': self.rewrite_prompts,
            'run_type': 'forecasting',
        }
        if self.exclusive_summaries:
            base_query["experiment_id"] = self.experiment_id

        if condition_label == "random":
            query = {**base_query, "condition": condition_label}
        elif condition_label == "vanilla":
            query = {**base_query, "condition": "baseline"}
        else:
            query = None  # Will build per-condition below

        def attach_market_preds(summaries):
            if include_market_preds:
                uuids = [doc.get("uuid") for doc in summaries if doc.get("uuid")]
                market_collection = db["market_predictions"]
                market_docs = market_collection.find({"uuid": {"$in": uuids}})
                uuid_to_markets = {}
                for doc in market_docs:
                    uuid_to_markets.setdefault(doc["uuid"], []).append(doc)
                for s in summaries:
                    s["market_preds"] = uuid_to_markets.get(s.get("uuid"), [])
            return summaries
       
        def rewrite_summaries(summaries):
            trapi = TRAPIAgent(temperature=.7)
            out = []
            if rewrite_summaries and len(summaries) >= number_of_summaries:
                logger.info(f"Rewriting each of the {len(summaries)} summaires, {(rewrite_summaries_number)} times with TRAPI agent")
                logger.info(f"\n\n Original summaries: {summaries[:3]}...")  # Log first 3 summaries for brevity
                for s in summaries:
                    for _ in range(int(rewrite_summaries_number)):
                        new_s = copy.deepcopy(s)
                        if "summary" in new_s:
                            new_s["summary"] = trapi.rewrite_summary(s["summary"])
                            out.append(new_s)
                summaries = out
                logger.info(f"Rewritten summaries: {summaries[:3]}...")  # Log first 3 summaries for brevity
            return summaries

        if query is not None:
            cursor = collection.find(query).sort("timestamp", DESCENDING)
            summaries = [
                {
                    "summary": doc.get("research_report", ""),
                    "urls": doc.get("urls", []),
                    "uuid": doc.get("uuid"),
                    "condition_label": condition_label
                }
                for doc in cursor
            ]
           
            if len(summaries) < number_of_summaries:
                raise ValueError(f"Not enough summaries for condition '{condition_label}' in ensemble run {self.run_id}: needed {number_of_summaries}, got {len(summaries)}")
           
            if shuffle:
                random.shuffle(summaries)
            summaries = summaries[:number_of_summaries]
            summaries = attach_market_preds(summaries)

            all_urls = [url for doc in summaries for url in doc.get("urls", [])]

            if self.rewrite_summaries:
                summaries = rewrite_summaries(summaries)

            if all_urls:
                logger.info(f"Found {len(all_urls)} URLs in summaries for condition '{condition_label}'")
                urls = [u['url'] for u in all_urls if isinstance(u, dict) and 'url' in u]
                unique_urls = list(set(urls))
                if self.include_scraped_content and unique_urls:
                    scraper = ScraperAgent()
                    scraped_content = {}
                    news_collection = db["news_data"]
                    news_doc = news_collection.find_one({"event_ticker": self.event.event_ticker , "urls": {"$in": unique_urls}, "condition": condition_label})
                    if news_doc and "texts" in news_doc:
                        scraped_content = news_doc["texts"]
                    else:
                        logger.warning(f"No scraped content found in DB for event {self.event.event_ticker}")
                    logger.info(f"Downloaded {len(scraped_content)} unique websites for condition '{condition_label}'")
                    # Save the scraped HTML to news_data collection
                    # Parse HTML to plain text for each URL
                    scraped_content_text = {}
                    logger.debug("Parsing scraped HTML content with Readability")
                    for url, html in scraped_content.items():
                        try:
                            parsed = scraper.parse_with_readability(html)
                            logger.debug(f"Parsed content for {url}: {parsed}")
                            scraped_content_text[url] = parsed.get("content_text", "")
                        except Exception as e:
                            # scraped_content_text[url] = f"[ERROR parsing] {str(e)}"
                            logger.warning(f"Failed to parse content for {url}: {e}")
                            scraped_content_text[url] = ""
                    # Attach scraped content to summaries
                    for s in summaries:
                        s["text_content"] = [scraped_content_text.get(u['url'], "") for u in s.get('urls', [])]
                    logger.debug(f"_get_summaries: Attached scraped content to {len(summaries)} summaries for condition '{condition_label}'")
            return summaries

        elif condition_label == "cycle":
            cycle_conditions = ["baseline", "reference class", "base rate", "reference class + base rate"]
            per_condition = number_of_summaries // len(cycle_conditions)
            summaries = []

            for condition in cycle_conditions:
                query = {**base_query, "condition": condition}
                cursor = collection.find(query).sort("timestamp", DESCENDING)
                condition_summaries = [
                    {
                        "summary": doc.get("research_report", ""),
                        "urls": doc.get("urls", []),
                        "uuid": doc.get("uuid"),
                        "condition_label": condition
                    }
                    for doc in cursor
                ]
                if shuffle:
                    random.shuffle(condition_summaries)
                summaries.extend(condition_summaries[:per_condition])
                if len(condition_summaries) < per_condition:
                    raise ValueError(f"Not enough summaries for cycle condition '{condition}' in ensemble run {self.run_id}: "f"needed {per_condition}, got {len(condition_summaries)}")

            if shuffle:
                random.shuffle(summaries)

            return attach_market_preds(summaries)

        else:
            raise ValueError(f"Unknown condition_label: {condition_label}")

    def build_ensemble_prompt(self, shuffle_summaries=True):
        """
        Build the ensemble prompt by aggregating research summaries, market predictions, and optionally scraped content.

        Args:
            shuffle_summaries (bool): Whether to shuffle the order of the input summaries before building the prompt. Default True.
            include_summaries (bool): Whether to include the summary text in each summary block. Default True.
            include_market_preds (bool): Whether to include market predictions for each summary if available. Default False.
            include_scraped_content (bool): Whether to include scraped content for each summary if available. Default False.

        Returns:
            tuple: (prompt1, prompt2) for the ensemble run.
        """
        for m in self.markets:
            if isinstance(m.expiration, str):
                m.expiration = parser.parse(m.expiration)

        # Generate market descriptions
        market_descriptions = get_market_descriptions(self.event, self.markets)

        # Generate instruction block based on conditions
        instruction_block = ""
        if self.base_rate and self.reference_class:
            instruction_block = "Include all information needed to create base rates and reference classes."
        elif self.base_rate:
            instruction_block = "Include all information needed to create base rates."
        elif self.reference_class:
            instruction_block = "Include all information needed to create reference classes."
         
        ref_class_block = """REFERENCE CLASS:
Write 1 paragraph on what the reference class should be."""
        base_rates_block = """BASE RATES:
Write 1 paragraph on what the base rates should be."""

        # Shuffle summaries if requested
        if shuffle_summaries:
            random.shuffle(self.input_summaries)

        # Generate summary text with flexible inclusion of content
        summary_blocks = []
        for i, summary in enumerate(self.input_summaries):
            summary_block = f"Summary {i+1}:\n" if self.include_summaries else ""
            if self.include_summaries:
                summary_block += f"{summary['summary']}\n"
            if summary.get("urls") and self.include_summaries:
                summary_urls = "\n".join([f"- {url}" for url in summary.get("urls", [])])
                summary_block += f"\nURLs consulted for Summary {i+1}:\n{summary_urls}"
            if self.include_market_preds and summary.get("market_preds"):
                preds = "\n".join([
                    f"- {mp['ticker']}: {mp.get('market_title', '')}\n  Prediction: {mp['prediction']:.2f}\n  Reasoning: {mp.get('reasoning', '')}"
                    for mp in summary["market_preds"]
                ])
                summary_block += f"\nMarket Predictions for Summary {i+1}:\n{preds}"
            if self.include_scraped_content:
                scraped = "\n".join([f"---\n{content[:3000]}...\n---" for content in summary["text_content"] if content])
                if self.include_scraped_content:
                    summary_block += f"\nScraped Content for Summary {i+1}:\n{scraped}"
            summary_blocks.append(summary_block)

        summary_text = "\n\n".join(summary_blocks)

        full_rcbr_block = ""
        if self.reference_class and self.base_rate:
            full_rcbr_block = f"""{ref_class_block}\n\n{base_rates_block}"""
        elif self.reference_class:
            full_rcbr_block = ref_class_block
        elif self.base_rate:
            full_rcbr_block = base_rates_block

        if self.include_summaries:
            intro = f"""# Research Summaries
Below are research summaries from {len(self.input_summaries)} distinct sources pertaining to the markets listed above."""
        if not self.include_summaries:
           intro = f"""# Web content
Below is potentially relevant scraped web content pertaining to the markets listed above."""
        # Build the final prompts
        if len(self.markets) == 1:
            prompt1 = f"""You are an expert superforecaster, familiar with the work of Philip Tetlock.
The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.
 
{market_descriptions}

{intro}
Your task is to synthesize the information provided and generate a final, well-reasoned probability forecast for each market associated with this event.

{summary_text}

# Instructions
Given all you know, make the best possible prediction for whether this market will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of this market. We expect you to answer in this format:

CRITICISM:
Write a paragraph critically evaluating the research summaries provided. What are the strengths and weaknesses of each summary? Are there any biases or gaps in the information presented? How do these affect your confidence in the predictions?

RESEARCH REPORT:
Write a *final* research summary integrating your current search with the provided reserach summaries (at least 5 paragraphs). Use plain text without markdown formatting. {instruction_block}

{full_rcbr_block}""".strip()

        elif len(self.markets) > 1:
            prompt1 = f"""You are an expert superforecaster, familiar with the work of Philip Tetlock.
The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.
 
{market_descriptions}

{intro}
Your task is to synthesize the information provided and generate a final, well-reasoned probability forecast for each market associated with this event.

{summary_text}

# Instructions
Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

CRITICISM:
Write a paragraph critically evaluating the research summaries provided. What are the strengths and weaknesses of each summary? Are there any biases or gaps in the information presented? How do these affect your confidence in the predictions?

RESEARCH REPORT:
Write a *final* research summary integrating your current search with the provided reserach summaries (at least 5 paragraphs). Use plain text without markdown formatting. {instruction_block}

{full_rcbr_block}"""
     
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
[{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
"reasoning": "A brief explanation of how you arrived at the prediction",
"prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

Your goal is to make the best possible prediction, using all relevant information."""

        logger.debug(f"Generated prompt for run {self.run_id}:\n{prompt1}...")
        logger.debug(f"Generated prompt2 for run {self.run_id}:\n{prompt2}...")
        return prompt1, prompt2
 
    def execute(self) -> str:
        test_db_connection()
        logger.info(f"Executing run {self.run_id} for event {self.event.event_ticker} with condition {self.condition_label}")
        self.prompt1, self.prompt2 = self.build_ensemble_prompt()
        logger.info(f"Run {self.run_id} prompts built successfully.")
        self.result = self.agent.two_part_call_gpt(self.prompt1, self.prompt2)
        logger.info(f"Run {self.run_id} completed with result: {str(self.result)[:60]}...")
        return self.result

    def save(self):
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "criticism": self.result.get("criticism"),
            "research_report": self.result.get("research_report"),
            "reference_class": self.result.get("reference_class"),
            "base_rates": self.result.get("base_rates"),
            "marketday": str(date.today()),
            "timestamp": datetime.utcnow(),
            "condition_desc": self.condition,
            "condition": self.condition_label,
            "temperature": self.agent.temp,
            "search": self.agent.search,
            "agent_id": self.agent.agent_id,
            "run_type": "ensemble",
            "number_of_summaries": self.number_of_summaries,
            "for_experiment": True,
            "run_id": self.run_id,
            "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None,
            "use_trapi": self.use_trapi,
            "urls": self.result.get("urls", []),
            "rewrite_prompts": self.rewrite_prompts,
            'include_summaries': self.include_summaries,
            'include_market_preds': self.include_market_preds,
            'include_scraped_content': self.include_scraped_content,
            'ingredient_temp': self.ingredient_temp,
            "rewrite_summaries": self.rewrite_summaries,
            "rewrite_summaries_number": self.rewrite_summaries_number,
            "uuid": self.uuid,
        }
        logger.debug(f"Event level data prepared for save {self.run_id}: {event_level_data}")

        market_level_data = []
        for m in self.result.get("predictions", []):
                # Find the corresponding row in flat by ticker
                match = self.event.flat_df[self.event.flat_df["ticker"] == m['ticker']]
             
                if not match.empty:
                    row = match.iloc[0]  # get the first (and likely only) match
                 
                    enriched_market = {
                        **m,
                        "condition": self.condition_label,
                        "temperature": self.agent.temp,
                        "search": self.agent.search,
                        "event": self.event.event_ticker,
                        "marketday": str(date.today()),
                        "timestamp": datetime.utcnow(),
                        "market_title": row["title"],
                        "mid_price": row["mid_price"],
                        "implied_prob": row["yes_implied_prob"],
                        "volume": row["volume"],
                        'category': row["event_category"],
                        "extremity": row["extremity"],
                        "run_type": "ensemble",
                        "number_of_summaries": self.number_of_summaries,
                        "for_experiment": True,
                        "run_id": self.run_id,
                        "experiment_id": self.experiment_id if hasattr(self, 'experiment_id') else None,
                        "use_trapi": self.use_trapi,
                        "rewrite_prompts": self.rewrite_prompts,
                        "include_summaries": self.include_summaries,
                        'include_market_preds': self.include_market_preds,
                        'include_scraped_content': self.include_scraped_content,
                        "ingredient_temp": self.ingredient_temp,
                        "rewrite_summaries": self.rewrite_summaries,
                        "rewrite_summaries_number": self.rewrite_summaries_number,
                        "uuid": self.uuid,
                    }
                    market_level_data.append(enriched_market)
                else:
                    print(f"⚠️ No match found in flat for ticker: {m['ticker']}")
        market_level_data = [to_python_types(d) for d in market_level_data]
        logger.debug(f"Market level data prepared for save {self.run_id}: {len(market_level_data)} markets")

        log_run_to_db(event_level_data, market_level_data)
        logger.info(f"💾 Run {self.run_id} saved to database for event {self.event.event_ticker}")

class EnsembleFromEnsemble:
    """
    Create an ensemble from previous ensemble runs.
    Takes multiple ensemble_from_seed runs and creates a meta-ensemble.
    """
    def __init__(self, ensemble_uuids: List[str], event: ForecastingEvent, agent: LLMAgent, run_id: int,
                 experiment_id: Optional[str] = None, include_median: bool = False,
                 include_market_preds: bool = False, include_summaries: bool = True):
        self.event = event
        self.ensemble_uuids = ensemble_uuids  # List of UUIDs from previous ensemble runs
        self.markets = event.markets
        self.agent = agent
        self.experiment_id = experiment_id if experiment_id else None
        self.run_id = run_id
        self.result = None
        self.uuid = str(uuid.uuid4())
        self.include_market_preds = include_market_preds
        self.include_median = include_median
        self.include_summaries = include_summaries
        logger.info(f"EnsembleFromEnsemble initialized for run {self.run_id} with {len(ensemble_uuids)} ensemble runs")

    def _get_ensemble_data_from_db(self) -> Dict:
        """
        Pull data from previous ensemble runs.
        """
        market_collection = db["market_predictions"]
       
        # Get event-level data from ensemble runs
        query = {
            "uuid": {"$in": self.ensemble_uuids},
            "run_type": "ensemble_from_seed",
            "event": self.event.event_ticker
        }
       
        logger.info(f"Querying for ensemble data with: {query}")
       
        # Get all ensemble results
        cursor = market_collection.find(query)
        ensemble_runs = list(cursor)
       
        # Extract research reports (these become our "summaries")
        research_reports = []
        all_predictions = []
       
        for run in ensemble_runs:
            if 'research_report' in run:
                research_reports.append(run['research_report'])
           
            # Get market-level predictions for this ensemble run
            market_query = {
                "uuid": run['uuid'],
                "run_type": "ensemble_from_seed",
                "event": self.event.event_ticker
            }
            market_cursor = market_collection.find(market_query)
            market_predictions = list(market_cursor)
           
            for pred in market_predictions:
                if 'ticker' in pred and 'prediction' in pred:
                    all_predictions.append({
                        'ticker': pred['ticker'],
                        'prediction': pred['prediction'],
                        'reasoning': pred.get('reasoning', ''),
                        'ensemble_uuid': run['uuid']
                    })
       
        # Calculate median prediction for each ticker
        medians = None
        if all_predictions and self.include_median:
            import pandas as pd
           
            df_predictions = pd.DataFrame(all_predictions)
           
            if 'ticker' in df_predictions.columns and 'prediction' in df_predictions.columns:
                df_predictions['prediction'] = pd.to_numeric(df_predictions['prediction'], errors='coerce')
               
                medians_df = df_predictions.groupby('ticker').agg({
                    'ticker': 'first',
                    'prediction': 'median'
                }).reset_index(drop=True)
               
                medians_df['prediction'] = medians_df['prediction'].round(3)
                medians = medians_df.to_dict(orient='records')
       
        # Put everything together
        result = {'research_reports': research_reports}
        if self.include_market_preds:
            result['predictions'] = all_predictions
        if self.include_median and medians:
            result['medians'] = medians
       
        return result

    def build_ensemble_prompt_from_ensembles(self, data: Dict) -> Tuple[str, str]:
        """Build ensemble prompt from previous ensemble data"""
        # Market descriptions (same as before)
        market_descriptions = ""
        for idx, m in enumerate(self.event.markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m.ticker}
Title: {m.title}
Subtitle: {m.yes_sub_title}
Possible Outcomes: Yes (0) or No (1)
Rules: {m.rules_primary}"""
            if isinstance(m.rules_secondary, str) and len(m.rules_secondary) > 0:
                market_descriptions += f"\nSecondary rules: {m.rules_secondary}"
           
            if hasattr(m.expiration, 'strftime'):
                expiration_str = m.expiration.strftime('%Y-%m-%d')
            else:
                expiration_str = str(m.expiration)
            market_descriptions += f"\nScheduled close date: {expiration_str}\n\n"
       
        # Group predictions by ensemble if predictions are included
        predictions_by_ensemble = {}
        if self.include_market_preds and 'predictions' in data:
            # Group predictions by ensemble_uuid
            for prediction in data['predictions']:
                ensemble_uuid = prediction.get('ensemble_uuid', 'unknown')
                if ensemble_uuid not in predictions_by_ensemble:
                    predictions_by_ensemble[ensemble_uuid] = []
                predictions_by_ensemble[ensemble_uuid].append(prediction)
       
        # Create combined research report and prediction blocks
        combined_blocks = []
        for i, research_report in enumerate(data['research_reports']):
            block = f"Research Report {i+1}:\n{research_report}"
           
            # Add corresponding predictions if available
            if self.include_market_preds and len(predictions_by_ensemble) > i:
                ensemble_uuid = list(predictions_by_ensemble.keys())[i]
                preds = predictions_by_ensemble[ensemble_uuid]
                if preds:
                    block += "\n\nPredictions from this ensemble:"
                    for pred in preds:
                        block += f"\nTicker: {pred['ticker']}\nPrediction: {pred['prediction']:.3f}\nReasoning: {pred['reasoning']}"
           
            combined_blocks.append(block)
       
        combined_text = "\n\n".join(combined_blocks)
       
        # Create median block if requested
        median_text = ""
        if self.include_median and 'medians' in data:
            median_blocks = []
            for median in data['medians']:
                median_blocks.append(f"Ticker: {median['ticker']}: {median['prediction']:.3f}")
            median_text = f"\n\nThe median prediction across all ensemble runs for each ticker is:\n" + "\n".join(median_blocks)
       
        intro = f"""# Research Reports from Previous Ensemble Runs
Below are {len(data['research_reports'])} research reports from previous ensemble runs."""
       
        # Build prompts
        prompt1 = f"""You are an expert superforecaster, familiar with the work of Philip Tetlock.
The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.

{market_descriptions}

{intro}
Your task is to synthesize the information from these ensemble reports and generate a final, well-reasoned probability forecast for each market.

{combined_text}{median_text}

# Instructions
Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes. We expect you to answer in this format:

CRITICISM:
Write a paragraph critically evaluating the research reports provided. What are the strengths and weaknesses? Are there biases or gaps?

RESEARCH REPORT:
Write a *final* research summary integrating your current search with the provided reports (at least 5 paragraphs). Use plain text without markdown formatting.

REFERENCE CLASS:
Write 1 paragraph on what the reference class should be.

BASE RATES:
Write 1 paragraph on what the base rates should be."""
       
        prompt2 = f"""Now, make your predictions for each market. Use strict json format:

Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
[{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
"reasoning": "A brief explanation of how you arrived at the prediction",
"prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

Your goal is to make the best possible prediction, using all relevant information."""
       
        return prompt1, prompt2

    def execute(self):
        """Execute ensemble from ensemble runs"""
        logger.info(f"Executing EnsembleFromEnsemble with {len(self.ensemble_uuids)} ensemble runs")
       
        # Step 1: Get data from previous ensemble runs
        data = self._get_ensemble_data_from_db()
       
        # Step 2: Build ensemble prompt
        prompt1, prompt2 = self.build_ensemble_prompt_from_ensembles(data)
       
        # Step 3: Make ensemble prediction
        ensemble_result = self.agent.two_part_call_gpt(prompt1, prompt2)
       
        # Step 4: Structure result
        self.result = {
            "ensemble_prediction": ensemble_result,
            "source_ensembles": data,
            "num_source_ensembles": len(data['research_reports']),
            "source_ensemble_uuids": self.ensemble_uuids,
            "run_id": self.run_id
        }
       
        logger.info(f"EnsembleFromEnsemble completed with {len(data['research_reports'])} source ensembles")
        return self.result

    def save(self):
        """Save results to database"""
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "run_type": "ensemble_from_ensemble",
            "run_id": self.run_id,
            "source_ensemble_uuids": self.ensemble_uuids,
            "num_source_ensembles": len(self.result["source_ensembles"]["research_reports"]),
           
            # Ensemble results
            "criticism": self.result["ensemble_prediction"].get("criticism"),
            "research_report": self.result["ensemble_prediction"].get("research_report"),
            "reference_class": self.result["ensemble_prediction"].get("reference_class"),
            "base_rates": self.result["ensemble_prediction"].get("base_rates"),
            "urls": self.result["ensemble_prediction"].get("urls", []),
            "include_market_preds": self.include_market_preds,
            "include_median": self.include_median,
           
            # Agent info
            "ensemble_temperature": self.agent.temp,
            "ensemble_search": self.agent.search,
            "ensemble_agent_id": self.agent.agent_id,
           
            # Metadata
            "uuid": self.uuid,
            "experiment_id": self.experiment_id,
            "marketday": str(date.today()),
            "timestamp": datetime.utcnow(),
        }
       
        market_level_data = []
        for prediction in self.result["ensemble_prediction"].get("predictions", []):
            enriched_market = {
                **prediction,
                "event": self.event.event_ticker,
                "marketday": str(date.today()),
                "timestamp": datetime.utcnow(),
                "run_type": "ensemble_from_ensemble",
                "source_ensemble_uuids": self.ensemble_uuids,
                "num_source_ensembles": len(self.result["source_ensembles"]["research_reports"]),
                "uuid": self.uuid,
                "include_market_preds": self.include_market_preds,
                "include_median": self.include_median,
                "experiment_id": self.experiment_id,
                "run_id": self.run_id,
            }
            market_level_data.append(enriched_market)
       
        log_run_to_db(event_level_data, market_level_data)
        logger.info(f"💾 EnsembleFromEnsemble run {self.run_id} saved to database")

class DebateRun:
    def __init__(self, event: ForecastingEvent, agent: LLMAgent, run_id: int, condition_label: str):
        self.event = event
        self.agent = agent
        self.condition_label = condition_label
        self.run_id = run_id
        self.result = None

    def build_prompt(self) -> str:
        for m in self.event.markets:
            if isinstance(m.expiration, str):
                m.expiration = parser.parse(m.expiration)

        market_descriptions = get_market_descriptions(self.event, self.event.markets)

        if len(self.event.markets) == 1:
            prompt = f"""{market_descriptions}

# Instructions
Search the web for reliable and up-to-date information that can help forecast the outcomes of the market above.
Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting."""

        else:
            prompt = f"""The following are markets under the event titled "{self.event.title}". The markets can resolve before the scheduled close date.

{market_descriptions}

# Instructions
Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets.
Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting."""
       
        return prompt.strip()
       
   
    def execute(self):
        # assert requests.get("https://api.ipify.org").text.startswith("173.59")
        logger.info(f"Executing run {self.run_id} for event {self.event.event_ticker} with condition {self.condition_label}")
        self.prompt = self.build_prompt()
        logger.info(f"Run {self.run_id} prompts built successfully.")
        self.results = self.agent.debate_call_gpt(self.prompt)
        logger.info(f"Run {self.run_id} completed with results: {str(self.results)[:60]}...")

        return self.results

    def save(self):
        for result in self.results:
            event_level_data = {
                "event": self.event.event_ticker,
                "title": self.event.title,
                "volume": sum(self.event.flat_df["volume"]),
                "category": self.event.category,
                "research_report": result.get("research_report"),
                "marketday": str(date.today()),
                "timestamp": datetime.utcnow(),
                "condition": self.condition_label,
                "temperature": self.agent.temp,
                "search": self.agent.search,
                "agent_id": self.agent.agent_id,
                "run_type": "debate",
                "for_experiment": True,
            }
            logger.info(f"Event level data prepared for save {self.run_id}")

            market_level_data = []
            for m in self.result.get("predictions", []):
                # Find the corresponding row in flat by ticker
                match = self.event.flat_df[self.event.flat_df["ticker"] == m['ticker']]
               
                if not match.empty:
                    row = match.iloc[0]  # get the first (and likely only) match
                   
                    enriched_market = {
                        **m,
                        "condition": self.condition_label,
                        "event": self.event.event_ticker,
                        "marketday": str(date.today()),
                        "timestamp": datetime.utcnow(),
                        "market_title": row["title"],
                        "mid_price": row["mid_price"],
                        "implied_prob": row["yes_implied_prob"],
                        "volume": row["volume"],
                        'category': row["event_category"],
                        "extremity": row["extremity"],
                        "run_type": "debate",
                        "expiration_time": row["expiration_time"],
                        "temperature": self.agent.temp,
                        "for_experiment": True,
                        "search": self.agent.search,
                    }
                    market_level_data.append(enriched_market)
                else:
                    print(f"⚠️ No match found in flat for ticker: {m.ticker}")
       
            market_level_data = [to_python_types(d) for d in market_level_data]
            logger.info(f"Market level data prepared for save {self.run_id}: {len(market_level_data)} markets")

            log_run_to_db(event_level_data, market_level_data)
            logger.info(f"💾 Run {self.run_id} saved to database for event {self.event.event_ticker}")

class PredictionChain:
    """
    Creates a sequential chain of predictions where each agent builds on the previous one's work.
    Agent 1 gets the event and makes a prediction, Agent 2 sees the event + Agent 1's summary/predictions,
    and so on. The final agent's output goes to a forecaster agent (without search) for the final prediction.
    """
   
    def __init__(self, event: ForecastingEvent, chain_length: int = 6, run_id: int = 0,
                 experiment_id: Optional[str] = None, chain_agent_temp: str = "high",
                 final_agent_temp: str = "high", use_trapi_chain: bool = False,
                 use_trapi_final: bool = True, rewrite_prompts: bool = False):
        self.event = event
        self.chain_length = chain_length
        self.run_id = run_id
        self.experiment_id = experiment_id
        self.chain_agent_temp = chain_agent_temp
        self.final_agent_temp = final_agent_temp
        self.use_trapi_chain = use_trapi_chain
        self.use_trapi_final = use_trapi_final
        self.rewrite_prompts = rewrite_prompts
        self.uuid = str(uuid.uuid4())
        self.result = None
        self.chain_results = []
       
        # Create chain agents (with search)
        self.chain_agents = []
        for i in range(chain_length):
            agent = LLMAgent(
                temp=chain_agent_temp,
                search=True,
                use_trapi=use_trapi_chain,
                rewrite_prompts=rewrite_prompts
            )
            self.chain_agents.append(agent)
       
        # Create final forecaster agent (without search)
        self.final_agent = LLMAgent(
            temp=final_agent_temp,
            search=False,
            use_trapi=use_trapi_final,
            rewrite_prompts=rewrite_prompts
        )
       
        # Initialize prompt writer
        self.prompt_writer = PromptWriter(base_rate=True, reference_class=True)
       
        logger.info(f"PredictionChain initialized with {chain_length} chain agents and 1 final agent")

    def _build_initial_prompt(self) -> str:
        """Build the initial prompt for the first agent in the chain"""
        market_descriptions = self.prompt_writer._build_static_market_data(self.event)
       
        prompt = f"""{market_descriptions}

You are an expert superforecaster, familiar with the work of Philip Tetlock. You are the first agent in a chain of forecasters.

# Instructions
Given all you know, make the best possible prediction for whether each of these markets will resolve to Yes. Search the web for reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

RESEARCH REPORT:
Write a *complete* record of the full search results (at least 5 paragraphs). Use plain text without markdown formatting. Include all information needed to create base rates and reference classes.

REFERENCE CLASS:
Write 1 paragraph on what the reference class should be.

BASE RATES:
Write 1 paragraph on what the base rates should be.

CRITICISM:
Write a paragraph of self-criticism. What are the potential weaknesses in your analysis? What uncertainties remain?"""
       
        return prompt

    def _build_chain_prompt(self, agent_number: int, previous_results: List[Dict]) -> str:
        """Build prompt for agents 2+ in the chain"""
        market_descriptions = self.prompt_writer._build_static_market_data(self.event)
       
        # Get only the immediately previous agent's work (agent n-1)
        prev_result = previous_results[-1]  # Last element is the most recent
        prev_agent_num = len(previous_results)
       
        previous_work = f"=== AGENT {prev_agent_num} ANALYSIS ===\n"
        previous_work += f"Research Report: {prev_result.get('research_report', '')}\n"
        previous_work += f"Reference Class: {prev_result.get('reference_class', '')}\n"
        previous_work += f"Base Rates: {prev_result.get('base_rates', '')}\n"
        previous_work += f"Criticism: {prev_result.get('criticism', '')}\n"
       
        # Include predictions if available
        if 'predictions' in prev_result:
            previous_work += "Predictions:\n"
            for pred in prev_result['predictions']:
                previous_work += f"- {pred.get('ticker', '')}: {pred.get('prediction', 0):.3f} ({pred.get('reasoning', '')})\n"
       
        prompt = f"""{market_descriptions}

You are an expert superforecaster, familiar with the work of Philip Tetlock. You are Agent {agent_number} in a chain of forecasters.

# Previous Agent's Work
Below you can see the analysis from Agent {prev_agent_num} (the agent immediately before you):

{previous_work}

# Instructions
Review the previous agent's work and build upon it. Search the web for additional reliable and up-to-date information that can help forecast the outcomes of these markets. We expect you to answer in this format:

RESEARCH REPORT:
Write a *complete* research report that builds on the previous analysis (at least 5 paragraphs). Include your own web searches and integrate them with the previous findings. Use plain text without markdown formatting.

REFERENCE CLASS:
Write 1 paragraph on what the reference class should be, considering the previous agent's input.

BASE RATES:
Write 1 paragraph on what the base rates should be, considering the previous agent's input.

CRITICISM:
Write a paragraph critically evaluating both your analysis and the previous agent's work. What are the strengths and weaknesses? What gaps remain?"""
       
        return prompt

    def _build_final_prompt(self, chain_results: List[Dict]) -> str:
        """Build the final forecasting prompt (without search)"""
        market_descriptions = self.prompt_writer._build_static_market_data(self.event)
       
        # Compile all chain agents' work
        chain_summary = ""
        for i, result in enumerate(chain_results):
            agent_num = i + 1
            chain_summary += f"\n\n=== AGENT {agent_num} ANALYSIS ===\n"
            chain_summary += f"Research Report: {result.get('research_report', '')}\n"
            chain_summary += f"Reference Class: {result.get('reference_class', '')}\n"
            chain_summary += f"Base Rates: {result.get('base_rates', '')}\n"
            chain_summary += f"Criticism: {result.get('criticism', '')}\n"
           
            # Include final predictions from last agent
            if i == len(chain_results) - 1 and 'predictions' in result:
                chain_summary += "Final Chain Predictions:\n"
                for pred in result['predictions']:
                    chain_summary += f"- {pred.get('ticker', '')}: {pred.get('prediction', 0):.3f}\n"
       
        prompt1 = f"""{market_descriptions}

You are an expert superforecaster making the final prediction in a prediction chain. Below is the complete analysis from {len(chain_results)} previous agents:

{chain_summary}

# Instructions
Based on all the analysis above, create your final assessment. Do NOT search the web - use only the information provided by the chain agents. We expect you to answer in this format:

FINAL CRITICISM:
Write a paragraph critically evaluating the chain agents' collective work. What are the overall strengths and weaknesses of their analysis?

FINAL RESEARCH REPORT:
Write a final integrated summary of the key findings (at least 3 paragraphs). Synthesize the most important insights from the chain.

FINAL REFERENCE CLASS:
Write 1 paragraph on your final assessment of the reference class.

FINAL BASE RATES:
Write 1 paragraph on your final assessment of the base rates."""

        prompt2 = f"""Now, make your final predictions for each market. Use strict json format:

Make a prediction for each of the markets in the event. Format your predictions as a strict json object. Don't include comments inside the json. The length of your array must be {len(self.event.markets)}. Include ALL markets, even if you think they will resolve to No. Use the following structure:
[{{"ticker": "KXWTAMATCH-25JUN30KALSTO-STO", (Market ticker copied exactly from the market metadata)
"reasoning": "A brief explanation of how you arrived at the prediction, referencing the chain analysis",
"prediction": 0.00  (a probability between 0 and 1, inclusive)}}]

Your goal is to make the best possible prediction, using all information from the prediction chain."""

        return prompt1, prompt2

    def execute(self) -> Dict[str, Any]:
        """Execute the complete prediction chain"""
        test_db_connection()
        logger.info(f"Executing PredictionChain {self.run_id} for event {self.event.event_ticker}")
       
        self.chain_results = []
       
        # Step 1: First agent gets initial prompt
        logger.info("Step 1: First agent analysis")
        initial_prompt = self._build_initial_prompt()
        prediction_prompt = self.prompt_writer.build_prediction_prompt(len(self.event.markets))
       
        first_result = self.chain_agents[0].two_part_call_gpt(initial_prompt, prediction_prompt)
        self.chain_results.append(first_result)
        logger.info(f"Agent 1 completed with {len(first_result.get('predictions', []))} predictions")
       
        # Step 2: Chain agents 2 through N
        for i in range(1, self.chain_length):
            agent_number = i + 1
            logger.info(f"Step {i+1}: Agent {agent_number} analysis")
           
            chain_prompt = self._build_chain_prompt(agent_number, self.chain_results)
            chain_result = self.chain_agents[i].two_part_call_gpt(chain_prompt, prediction_prompt)
            self.chain_results.append(chain_result)
            logger.info(f"Agent {agent_number} completed with {len(chain_result.get('predictions', []))} predictions")
       
        # Step 3: Final agent (without search) makes final prediction
        logger.info(f"Step {self.chain_length + 1}: Final forecaster analysis (no search)")
        final_prompt1, final_prompt2 = self._build_final_prompt(self.chain_results)
       
        final_result = self.final_agent.two_part_call_gpt(final_prompt1, final_prompt2)
       
        # Structure the complete result
        self.result = {
            "chain_results": self.chain_results,
            "final_result": final_result,
            "chain_length": self.chain_length,
            "run_id": self.run_id,
            "uuid": self.uuid
        }
       
        logger.info(f"PredictionChain {self.run_id} completed successfully")
        return self.result

    def save(self):
        """Save the prediction chain results to database"""
        if not self.result:
            raise ValueError("No results to save. Execute the chain first.")
       
        # Event-level data
        event_level_data = {
            "event": self.event.event_ticker,
            "title": self.event.title,
            "volume": sum(self.event.flat_df["volume"]),
            "category": self.event.category,
            "run_type": "prediction_chain",
            "run_id": self.run_id,
            "chain_length": self.chain_length,
           
            # Final results
            "criticism": self.result["final_result"].get("criticism"),
            "research_report": self.result["final_result"].get("research_report"),
            "reference_class": self.result["final_result"].get("reference_class"),
            "base_rates": self.result["final_result"].get("base_rates"),
            "urls": self.result["final_result"].get("urls", []),
           
            # Chain agent info
            "chain_agent_temp": self.chain_agent_temp,
            "final_agent_temp": self.final_agent_temp,
            "use_trapi_chain": self.use_trapi_chain,
            "use_trapi_final": self.use_trapi_final,
           
            # Metadata
            "uuid": self.uuid,
            "experiment_id": self.experiment_id,
            "rewrite_prompts": self.rewrite_prompts,
            "marketday": str(date.today()),
            "timestamp": datetime.utcnow(),
        }
       
        # Market-level data (from final predictions)
        market_level_data = []
        final_predictions = self.result["final_result"].get("predictions", [])
       
        for prediction in final_predictions:
            # Find corresponding market data
            match = self.event.flat_df[self.event.flat_df["ticker"] == prediction['ticker']]
           
            if not match.empty:
                row = match.iloc[0]
               
                enriched_market = {
                    **prediction,
                    "event": self.event.event_ticker,
                    "marketday": str(date.today()),
                    "timestamp": datetime.utcnow(),
                    "run_type": "prediction_chain",
                    "chain_length": self.chain_length,
                    "market_title": row["title"],
                    "mid_price": row["mid_price"],
                    "implied_prob": row["yes_implied_prob"],
                    "volume": row["volume"],
                    "category": row["event_category"],
                    "extremity": row["extremity"],
                    "expiration_time": row["expiration_time"],
                    "uuid": self.uuid,
                    "experiment_id": self.experiment_id,
                    "run_id": self.run_id,
                }
                market_level_data.append(enriched_market)
            else:
                logger.warning(f"⚠️ No match found in flat_df for ticker: {prediction['ticker']}")
       
        # Convert to Python types and save
        market_level_data = [to_python_types(d) for d in market_level_data]
        log_run_to_db(event_level_data, market_level_data)
       
        logger.info(f"💾 PredictionChain {self.run_id} saved to database for event {self.event.event_ticker}")

    def get_chain_summary(self) -> str:
        """Get a readable summary of the prediction chain results"""
        if not self.result:
            return "No results available. Execute the chain first."
       
        summary = f"=== PREDICTION CHAIN SUMMARY ===\n"
        summary += f"Event: {self.event.event_ticker}\n"
        summary += f"Chain Length: {self.chain_length}\n"
        summary += f"Markets: {len(self.event.markets)}\n\n"
       
        # Show evolution of predictions across the chain
        if self.event.markets:
            first_ticker = self.event.markets[0].ticker
            summary += f"Prediction Evolution for {first_ticker}:\n"
           
            for i, chain_result in enumerate(self.result["chain_results"]):
                agent_num = i + 1
                for pred in chain_result.get("predictions", []):
                    if pred["ticker"] == first_ticker:
                        summary += f"  Agent {agent_num}: {pred['prediction']:.3f}\n"
                        break
           
            # Final prediction
            for pred in self.result["final_result"].get("predictions", []):
                if pred["ticker"] == first_ticker:
                    summary += f"  Final: {pred['prediction']:.3f}\n"
                    break
       
        return summary


class DatabaseQQ():
    def __init__(self):
        self.db = db
        self.events = self.db["event_predictions"]
        self.markets = self.db["market_predictions"]
        self.articles = self.db["news_data"]
        self.df = pd.DataFrame(self.query_recent_market_predictions())
        self.unique_events = self.df.event.unique().tolist()
        self.missing_runs = self.get_all_missing_runs()

    def query_recent_market_predictions(self):
        # Define the cutoff timestamp: July 7, 2025, 13:56
        cutoff = datetime(2025, 7, 7, 13, 56)
        # cutoff2 = datetime(2025, 8,8, 17, 0, 59)
        # Query the market_predictions table for entries after the cutoff
        results = db.market_predictions.find({
            "timestamp": {
                "$gt": cutoff,
                # "$lt": cutoff2
            }
        })
        predictions = list(results)
        logger.debug(f"🔎 Found {len(predictions)} market_predictions after {cutoff}.")
        return predictions
 
    def normalize_row(self,row):
        if row["condition"] == "random":
            return ("high", "random", row.get("persona_blurb", None), "ensemble", None, 1)
        elif pd.notna(row.get("number_of_summaries")):
            n = row.get("number_of_summaries")
            n = int(n) if pd.notna(n) else 0
            return (
                "high",
                row["condition"],
                None,
                "ensemble",
                row.get("search", False),
                n
            )
        else:
            temp = "low" if "low" in str(row.get("reasoning", "")).lower() else "high"
            return (temp, row["condition"], None, "forecasting", None, None)

    def validate_event_runs_dataframe(self, event_ticker):
        df = self.df.copy()
        event_df = df[df['event'] == event_ticker].copy()

        # Define expected runs
        expected_runs = []

        # 2x2 Low Temp
        expected_runs.extend([
            ("low", "baseline", None, "forecasting", None, None),
            ("low", "reference class", None, "forecasting", None, None),
            ("low", "base rate", None, "forecasting", None, None),
            ("low", "reference class + base rate", None, "forecasting", None, None)
        ])

        # 2x2 High Temp × 3 reps
        for _ in range(3):
            expected_runs.extend([
                ("high", "baseline", None, "forecasting", None, None),
                ("high", "reference class", None, "forecasting", None, None),
                ("high", "base rate", None, "forecasting", None, None),
                ("high", "reference class + base rate", None, "forecasting", None, None)
            ])

        # 9 baseline single runs
        expected_runs.extend([("high", "baseline", None, "forecasting", None, None)] * 9)

        # Persona runs
        persona_blurbs = self.df[self.df['persona_blurb'].notnull() & (self.df['persona_blurb'] != "")].persona_blurb.unique()
        for pid in persona_blurbs:
            expected_runs.append(("high", "random", pid, "forecasting", None, None))  # Use dummy value for summaries

        # Ensemble runs
        search_options = [False, True]
        summary_counts = [4, 8, 12]
        for cond in ["vanilla", "cycle", "random"]:
            for search in search_options:
                for n in summary_counts:
                    expected_runs.append(("high", cond, None, "ensemble", search, n))

        # Count actual and expected
        actual_counts = Counter(self.normalize_row(row) for _, row in event_df.iterrows())
        expected_counts = Counter(expected_runs)

        # Build DataFrame rows
        records = []

        all_keys = set(actual_counts.keys()).union(expected_counts.keys())
        for key in sorted(all_keys, key=lambda x: (x[0], x[1], str(x[2]) if x[2] is not None else "", str(x[3]), str(x[4]), str(x[5]))):
            temp, cond, persona, rtype, search, nsummaries = key
            actual = actual_counts.get(key, 0)
            expected = expected_counts.get(key, 0)
            diff = actual - expected

            if expected > 0 or actual > 0:
                status = "found" if actual == expected else ("missing" if actual < expected else "extra")
                records.append({
                    "event_ticker": event_ticker,
                    "status": status,
                    "temperature": temp,
                    "condition": cond,
                    "persona": persona,
                    "run_type": rtype,
                    "search": search,
                    "number_of_summaries": nsummaries,
                    "actual": actual,
                    "expected": expected,
                    "difference": diff
                })

        result_df = pd.DataFrame(records).sort_values(by=["status", "run_type", "condition", "number_of_summaries", "search"])
        return result_df

    def run_missing(self):
        results_df = self.get_all_missing_runs()
     
        # Prepare all task arguments first (no execution)
        task_args = []
        for row in tqdm.tqdm(results_df.itertuples(), total=len(results_df), desc="Preparing missing runs"):
            if row.status == "found":
                continue
             
            if row.run_type not in ["ensemble", "forecasting"]:
                continue
             
            if pd.isna(row.temperature) or pd.isna(row.condition):
                logger.warning(f"Skipping run {row.event_ticker} due to missing temperature or condition.")
                continue
             
            if row.run_type == "ensemble" and pd.isna(row.number_of_summaries):
                logger.warning(f"Skipping ensemble run {row.event_ticker} due to missing number_of_summaries.")
                continue
             
            # Build keyword args for run (just data preparation, no execution)
            kwargs = {
                "event_ticker": row.event_ticker,
                "temperature": row.temperature,
                "condition": row.condition,
                "persona_blurb": row.persona,
                "run_type": row.run_type,
                "search": row.search,
                "number_of_summaries": int(row.number_of_summaries) if pd.notna(row.number_of_summaries) else None,
                "n": row.difference * -1
            }
            task_args.append(kwargs)
     
        logger.info(f"Prepared {len(task_args)} tasks for execution")
     
        # Now execute all tasks in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all tasks at once
            futures = []
            for kwargs in task_args:
                future = executor.submit(self._execute_single_run, **kwargs)
                futures.append(future)
                time.sleep(0.1)  # Small delay to avoid overwhelming the executor
         
            # Wait for completion and handle results
            for i, future in enumerate(tqdm.tqdm(as_completed(futures), total=len(futures), desc="Executing runs")):
                try:
                    future.result()
                    logger.info(f"✅ Completed run {i+1}/{len(futures)}")
                except Exception as e:
                    logger.error(f"❌ Run {i+1}/{len(futures)} failed: {type(e).__name__} | {e}", exc_info=True)

    def _execute_single_run(self, **kwargs):
        """
        This method contains ALL the actual execution logic.
        It should only be called by the thread pool executor.
        """
        event_ticker = kwargs.get('event_ticker', 'unknown')
        logger.info(f"Starting execution for {event_ticker}")
     
        try:
            # Call your original run_from_metadata2 logic
            result = self.run_from_metadata2(**kwargs)
            logger.info(f"Completed execution for {event_ticker}")
            return result
        except Exception as e:
            logger.error(f"Failed execution for {event_ticker}: {e}")
            raise

    def run_from_metadata2(self,temperature, condition, persona_blurb, run_type, search, number_of_summaries, event_ticker, n, rewrite_prompts=True, experiment_id=None, run_id=0, use_trapi = False, include_market_preds = False, include_scraped_text = False, ingredient_temp = 'high'):
        kalshi = KalshiAPI()
        # logger.info(f"Fetching event data for {event_ticker}")
        # logger.info(f"Running with temperature: {temperature}, condition: {condition}, run_type: {run_type}, search: {search}, number_of_summaries: {number_of_summaries}, n: {n}")
        event = kalshi.make_forecasting_event(event_ticker)
        if event.has_too_many_markets():
            logger.info(f"Skipping {event.event_ticker} due to too many markets")
            return
     
        logger.info(f"Starting run for event {event.event_ticker} with condition {condition} and temperature {temperature}")

        if run_type == "forecasting":
            for _ in range(n):
                run_one(event, temperature, condition, persona_blurb = persona_blurb, experiment_id=experiment_id, rewrite_prompts=rewrite_prompts, run_id=run_id, use_trapi=use_trapi)
     
        elif run_type == "ensemble":
            if(search):
                agent = LLMAgent(temp=temperature, search=True)
            else:
                agent = LLMAgent(temp=temperature, search=False)
            run = EnsembleRun(event, agent, run_id=run_id, condition=condition, number_of_summaries=number_of_summaries, experiment_id=experiment_id, use_trapi=use_trapi, rewrite_prompts=rewrite_prompts, include_market_preds=include_market_preds, include_scraped_content=include_scraped_text, ingredient_temp=ingredient_temp)
            run.execute()
            run.save()

    def detect_invalid_ensemble_runs(self, val_df):
        """
        Returns a DataFrame of ensemble runs that were marked as 'found'
        but are invalid due to not having enough ingredients.
        """
        invalid = []
        val_df = val_df.copy()
        # Get counts of high-temp forecasting runs (excluding ensembles)
        forecast_counts = (
            val_df[(val_df.run_type == "forecasting") & (val_df.status != "missing") & (val_df.temperature == "high")]
            .groupby("condition")["actual"].sum()
            .to_dict()
        )

        # Count number of persona runs
        persona_count = (
            val_df[(val_df.run_type == "ensemble") & (val_df.condition == "random") & (val_df["persona"].notna())]['persona'].nunique()
        )

        # Loop through found ensemble runs
        found_ensembles = val_df[(val_df.run_type == "ensemble") & (val_df.status == "found") & (val_df.persona.isna())]

        for row in found_ensembles.itertuples():
            condition = row.condition
            n = int(row.number_of_summaries)
            needed_per_condition = n // 4 if condition == "cycle" else n

            if condition == "vanilla":
                if forecast_counts.get("baseline", 0) < n:
                    invalid.append(row._asdict() | {"reason": f"vanilla: need {n} baseline, found {forecast_counts.get('baseline', 0)}"})

            elif condition == "cycle":
                missing_conds = []
                for c in ["baseline", "reference class", "base rate", "reference class + base rate"]:
                    if forecast_counts.get(c, 0) < needed_per_condition:
                        missing_conds.append((c, forecast_counts.get(c, 0)))
                if missing_conds:
                    reasons = "; ".join(f"{c}: {count}" for c, count in missing_conds)
                    invalid.append(row._asdict() | {"reason": f"cycle: need {needed_per_condition} per condition. Found - {reasons}"})

            elif condition == "random":
                if persona_count < 12:
                    invalid.append(row._asdict() | {"reason": f"random: need {n} personas, found {persona_count}"})

        return pd.DataFrame(invalid)

    def get_all_missing_runs(self):
        """
        Returns a DataFrame of all missing runs across all events.
        """
        all_missing = pd.DataFrame()

        for event in self.unique_events:
            val_df = self.validate_event_runs_dataframe(event)
            missing_runs = val_df[val_df.status == "missing"].copy()
            if not missing_runs.empty:
                missing_runs["event"] = event
                all_missing = pd.concat([all_missing, missing_runs], ignore_index=True)
        all_missing = all_missing.sort_values(by=["run_type"], ascending=False)
        return all_missing

    def print_invalid_ensemble_runs(self):
        for event in self.unique_events:
            val_df = self.validate_event_runs_dataframe(event)
            invalid_runs = self.detect_invalid_ensemble_runs(val_df)
            if not invalid_runs.empty:
                logger.info(f"Invalid ensemble runs for event {event}:\n{invalid_runs}")
            else:
                logger.info(f"All ensemble runs for event {event} are valid.")

def run_from_metadata2(temperature, condition, persona_blurb, run_type, search, number_of_summaries, event_ticker, n, experiment_id=None, run_id=0, use_trapi = False, rewrite_prompts = False, include_summaries = True, include_market_preds=False, include_scraped_content=False, ingredient_temp='high', rewrite_summaries=False, rewrite_summaries_number=None):
    kalshi = KalshiAPI()
    # logger.info(f"Fetching event data for {event_ticker}")
    # logger.info(f"Running with temperature: {temperature}, condition: {condition}, run_type: {run_type}, search: {search}, number_of_summaries: {number_of_summaries}, n: {n}")
    event = kalshi.make_forecasting_event(event_ticker)
    if event.has_too_many_markets():
        logger.info(f"Skipping {event.event_ticker} due to too many markets")
        return
 
    logger.info(f"Starting run for event {event.event_ticker} with condition {condition} and temperature {temperature}")

    if run_type == "forecasting":
        for _ in range(n):
            run_one(event, temperature, condition, run_id=run_id, persona_blurb = persona_blurb, experiment_id=experiment_id, rewrite_prompts=rewrite_prompts, use_trapi=use_trapi)
 
    elif run_type == "ensemble":
        if(search):
            agent = LLMAgent(temp=temperature, search=True)
        else:
            agent = LLMAgent(temp=temperature, search=False, use_trapi=use_trapi)
        run = EnsembleRun(event, agent, condition=condition, number_of_summaries=number_of_summaries, experiment_id=experiment_id, run_id=run_id, use_trapi=use_trapi,rewrite_prompts=rewrite_prompts, include_summaries =include_summaries, include_market_preds=include_market_preds, include_scraped_content=include_scraped_content, ingredient_temp=ingredient_temp, rewrite_summaries=rewrite_summaries, rewrite_summaries_number=rewrite_summaries_number)
        run.execute()
        run.save()
# ------------------------- PROMPTS -------------------------


# ------------------------- ENTRYPOINT -------------------------

def run_2by2(event: ForecastingEvent, temp: str, run_id = 0):
    results = []
    for label in ["baseline", "reference class", "base rate", "reference class + base rate"]:
        condition = CONDITION_MAP[label]
        agent = LLMAgent(temp=temp, search=True)
        run = ForecastingRun(event, agent, condition=condition, run_id=run_id, condition_label=label)
        result = run.execute()
        run.save()
        results.append(result)
        run_id += 1
    logger.debug(f"Completed 2x2 analysis for event {event.event_ticker} with results: {results}")
    return results

def run_one(event: ForecastingEvent, temp: str, condition: str, run_id=0, persona_blurb: Optional[str] = None, experiment_id: Optional[str] = None, rewrite_prompts: bool = False, use_trapi: bool = False):
    condition_dict = CONDITION_MAP[condition]
    agent = LLMAgent(temp=temp, search=True, use_trapi=use_trapi, rewrite_prompts=rewrite_prompts)
    run = ForecastingRun(event, agent, condition=condition_dict, run_id=run_id, condition_label=condition, persona_blurb=persona_blurb, experiment_id=experiment_id, rewrite_prompts=rewrite_prompts)
    result = run.execute()
    run.save()
    return result

def run_debate(event: ForecastingEvent, temp: str, run_id=0):
    agent = LLMAgent(temp=temp, search=True)
    run = DebateRun(event, agent, run_id=run_id, condition_label="debate")
    results = run.execute()
    run.save()
    return results

def get_metadata_from_row(row):
    """
    Extracts metadata from a pandas Series (row) or dict.
    Supports both DataFrame row access and dict input.
    """
    try:
        return (
            row.temperature,
            row.condition,
            row.persona,
            row.run_type,
            row.search,
            row.number_of_summaries,
            row.difference * -1
        )
    except AttributeError:
        raise TypeError("Input must be a pandas Series or have attribute access")

def run_single_event(event_ticker, expert_conditions = random_personas):
    kalshi = KalshiAPI()
    logger.info(f"Fetching event data for {event_ticker}")
    event = kalshi.make_forecasting_event(event_ticker)
    if event.has_too_many_markets():
        logger.info(f"Skipping {event.event_ticker} due to too many markets")
        return

    logger.info("Starting INGREDIENTS analysis")
    run_id = 0

    run_2by2(event, temp="low", run_id=run_id)
    run_id += 4  # 4 runs for 2x2 analysis
    for _ in range(3):
        run_2by2(event, temp="high", run_id=run_id)
        run_id += 4      
    for _ in range(9):
        run_one(event, temp="high", condition='baseline', run_id=run_id)
        run_id += 1
    agent = LLMAgent(temp="high", search=True)

    for i, (persona_id,_, persona_blurb) in enumerate(expert_conditions):
        run = ForecastingRun(
            event=event,
            agent=agent,
            run_id=run_id + i,
            condition=CONDITION_MAP["random"],
            condition_label="random",
            persona_id=persona_id,
            persona_blurb=persona_blurb
            )
        run.execute()
        run.save()

    logger.info(f"Completed ingredients analysis for event {event.event_ticker}")

    run_id += len(expert_conditions)  # Increment run_id by number of expert conditions
    for num_summaries in [4, 8, 12]:
        for search in [False, True]:
            agent = LLMAgent(temp="high", search=search)


            run = EnsembleRun(event, agent, run_id=run_id, condition="vanilla", number_of_summaries=num_summaries)
            run.execute()
            run.save()
            logger.info(f"✅ Ensemble {run_id} | Source: baseline | Search: {search} | N={num_summaries}")
            run_id += 1

            run = EnsembleRun(event, agent, run_id=run_id, condition="cycle", number_of_summaries=num_summaries)
            run.execute()
            run.save()
            logger.info(f"✅ Ensemble {run_id} | Source: 2x2 | Search: {search} | N={num_summaries}")
            run_id += 1

            run = EnsembleRun(event, agent, run_id=run_id, condition="random", number_of_summaries=num_summaries)
            run.execute()
            run.save()
            logger.info(f"✅ Ensemble {run_id} | Source: random | Search: {search} | N={num_summaries}")
            run_id += 1


# specs are: runid, run_type, temperature, condition, persona_blurb, search, number_of_summaries
def create_specs55():
    df = pd.DataFrame(columns=[
        "run_id", "run_type", "temperature", "condition", "person_id","persona_blurb", "search", "number_of_summaries", 'experiment_id'])
 
    # 2x2 Low Temp
    for label in ["baseline", "reference class", "base rate", "reference class + base rate"]:
        df = pd.concat([df, pd.DataFrame({
            "run_id": [0],
            "run_type": ["forecasting"],
            "temperature": ["low"],
            "condition": [label],
            "persona_blurb": [None],
            "search": [None],
            "number_of_summaries": [None]
        })], ignore_index=True)

    # 2x2 High Temp × 3 reps
    for _ in range(3):
        for label in ["baseline", "reference class", "base rate", "reference class + base rate"]:
            df = pd.concat([df, pd.DataFrame({
                "run_id": [0],
                "run_type": ["forecasting"],
                "temperature": ["high"],
                "condition": [label],
                "persona_blurb": [None],
                "search": [None],
                "number_of_summaries": [None]
            })], ignore_index=True)
    # 9 baseline single runs
    for _ in range(9):
        df = pd.concat([df, pd.DataFrame({
            "run_id": [0],
            "run_type": ["forecasting"],
            "temperature": ["high"],
            "condition": ["baseline"],
            "persona_blurb": [None],
            "search": [None],
            "number_of_summaries": [None]
        })], ignore_index=True)

    # Persona runs
    for persona_id, _, persona_blurb in random_personas:
        df = pd.concat([df, pd.DataFrame({
            "run_id": [0],
            "run_type": ["forecasting"],
            "temperature": ["high"],
            "condition": ["random"],
            "person_id": [persona_id],
            "persona_blurb": [persona_blurb],
            "search": [None],
            "number_of_summaries": [None]
        })], ignore_index=True)

    # Ensemble runs
    search_options = [False, True]
    summary_counts = [4, 8, 12]
    for cond in ["vanilla", "cycle", "random"]:
        for search in search_options:
            for n in summary_counts:
                df = pd.concat([df, pd.DataFrame({
                    "run_id": [0],
                    "run_type": ["ensemble"],
                    "temperature": ["high"],
                    "condition": [cond],
                    "persona_blurb": [None],
                    "search": [search],
                    "number_of_summaries": [n]
                })], ignore_index=True)
    return df

def create_specsF(experiment_id=None):
    """
    Create a DataFrame of run specifications for baseline forecasting runs,
    both with and without prompt rewriting.
    """
    def make_rows(start_id, n, temp_flag):
        return pd.DataFrame({
            "run_id": list(range(start_id, start_id + n)),
            "run_type": ["forecasting"] * n,
            "temperature": [temp_flag] * n,
            "condition": ["baseline"] * n,
            "person_id": [None] * n,
            "persona_blurb": [None] * n,
            "search": [True] * n,
            "number_of_summaries": [None] * n,
            "rewrite": [False] * n
        })

    df = pd.DataFrame(columns=[
        "run_id", "run_type", "temperature", "condition", "person_id",
        "persona_blurb", "search", "number_of_summaries", "rewrite"
    ])

    # Baseline runs without rewrite
    df = pd.concat([df, make_rows(0, 6, 'low')], ignore_index=True)
    # Baseline runs with rewrite
    df = pd.concat([df, make_rows(6, 6, 'high')], ignore_index=True)

    # Add experiment_id if provided
    if experiment_id is not None:
        df['experiment_id'] = experiment_id
    else:
        df['experiment_id'] = None

    return df

# for ensmebles
def create_specsE(experiment_id=None, use_trapi=True):
    df = pd.DataFrame({
        "run_id": [0, 1, 2, 3],
        "run_type": ["ensemble"] * 4,
        "temperature": ["high"] * 4,
        "condition": ["vanilla"] * 4,
        "persona_blurb": [None] * 4,
        "search": [False] * 4,
        "number_of_summaries": [6] * 4,
        'experiment_id': [experiment_id] * 4 if experiment_id else [None] * 4,
        'use_trapi': [use_trapi] * 4,
        'rewrite': [False] * 4,
        'include_summaries': [True, True, True, False],
        'include_market_preds': [False, False, True, False],
        'include_scraped_content': [False, False, False, True],
        'ingredient_temp': ['high', 'low', 'high', 'high']
    }, index=[0,1,2,3])
    return df

def create_specsF14(experiment_id=None ):
    df = pd.DataFrame({
        "run_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        "run_type": ["forecasting"] * 12,
        "temperature": ["high"] * 12,
        "condition": ["baseline"] * 12,
        "persona_blurb": [None] * 12,
        "search": [True] * 12,
        "number_of_summaries": [None] * 12,
        'experiment_id': [experiment_id] * 12 if experiment_id else [None] * 12,
        'use_trapi': [False] * 12,
        'rewrite': [False] * 12,
        'include_summaries': [None] * 12,
        'include_market_preds': [False] * 12,
        'include_scraped_content': [False] * 12,
        'ingredient_temp': [None] * 12
    }, index=[0,1,2,3, 4, 5, 6, 7, 8, 9, 10, 11])
    return df

def create_specsE14(experiment_id=None ):
    df = pd.DataFrame({
        "run_id": [0, 1, 2, 3],
        "run_type": ["ensemble"] * 4,
        "temperature": ["high"] * 4,
        "condition": ["vanilla"] * 4,
        "persona_blurb": [None] * 4,
        "search": [False] * 4,
        "number_of_summaries": [1,1,6,12],
        'experiment_id': [experiment_id] * 4 if experiment_id else [None] * 4,
        'use_trapi': [True] * 4,
        'rewrite': [False] * 4,
        'include_summaries': [True] * 4,
        'include_market_preds': [False] * 4,
        'include_scraped_content': [False] * 4,
        'ingredient_temp': ['high'] * 4,
        "rewrite_summaries": [True, True, False, False],
        "rewrite_summaries_number": [6, 12, None, None]
    }, index=[0,1,2,3, ])
    return df

def execute_one_task(task_data):
    """Execute a single task (forecasting or ensemble)"""
    try:
        run_from_metadata2(
            temperature=task_data['temperature'],
            condition=task_data['condition'],
            persona_blurb=task_data['persona_blurb'],
            run_type=task_data['run_type'],
            search=task_data['search'],
            number_of_summaries=task_data['number_of_summaries'],
            event_ticker=task_data['event_ticker'],
            experiment_id=task_data.get('experiment_id', None),
            run_id=task_data['run_id'],
            use_trapi=task_data.get('use_trapi', False),
            n=1,
            rewrite_prompts=task_data.get('rewrite'),
            include_market_preds=task_data.get('include_market_preds'),
            include_scraped_content=task_data.get('include_scraped_content'),
            include_summaries=task_data.get('include_summaries', True),
            ingredient_temp=task_data.get('ingredient_temp', 'high'),
            rewrite_summaries= task_data.get('rewrite_summaries'),
            rewrite_summaries_number=task_data.get('rewrite_summaries_number')
        )
        return {"success": True, "index": task_data['index']}
    except Exception as e:
        logger.error(f"❌ Task failed: {e}")
        return {"success": False, "index": task_data['index']}


def run_with_executor(stage_specs, event_ticker, stage_name, max_workers=3):
    """Run incomplete tasks in a stage with thread pool executor"""
    # Get remaining incomplete tasks
    remaining = stage_specs[~stage_specs['complete']].copy()
 
    if remaining.empty:
        logger.info(f"✅ All {stage_name} tasks already complete")
        return True
 
    logger.info(f"🔄 Running {len(remaining)} {stage_name} tasks")
 
    # Prepare tasks
    tasks = []
    for idx, row in remaining.iterrows():
        task_data = row.to_dict()
        task_data['index'] = idx
        task_data['event_ticker'] = event_ticker
        tasks.append(task_data)
 
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(execute_one_task, task) for task in tasks]
     
        for future in tqdm.tqdm(as_completed(futures), total=len(futures),
                             desc=f"{stage_name}"):
            result = future.result()
            if result['success']:
                stage_specs.loc[result['index'], 'complete'] = True
 
    completed = stage_specs['complete'].sum()
    total = len(stage_specs)
    logger.info(f"📊 {stage_name}: {completed}/{total} completed")
 
    return completed == total


def run_single_event_safe_df(specs, event_ticker, max_workers=3, max_retries=3):
    """
    Modular staged execution:
    1. While forecasting incomplete: run_with_executor
    2. While ensemble incomplete: run_with_executor
    """
    specs = specs.copy()
    specs['complete'] = False
 
    logger.info(f"🚀 Starting {event_ticker}")
 
    forecasting_specs = specs[specs['run_type'] == 'forecasting'].copy()
    ensemble_specs = specs[specs['run_type'] == 'ensemble'].copy()
 
    # Stage 1: While forecasting incomplete, run executor
    retry_count = 0
    while retry_count < max_retries and not forecasting_specs['complete'].all():
        logger.info(f"🔄 Forecasting attempt {retry_count + 1}/{max_retries}")
     
        forecasting_complete = run_with_executor(
            forecasting_specs, event_ticker, "Forecasting", max_workers
        )
     
        if forecasting_complete:
            logger.info(f"✅ Forecasting complete!")
            break
         
        retry_count += 1
     
        if retry_count < max_retries:
            wait_time = 30 * retry_count
            logger.info(f"⏳ Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
 
    # Check forecasting completion
    if not forecasting_specs['complete'].all():
        failed_count = len(forecasting_specs) - forecasting_specs['complete'].sum()
        logger.error(f"❌ Forecasting incomplete: {failed_count} tasks failed after {max_retries} attempts")
        return forecasting_specs, ensemble_specs
 
    # Stage 2: While ensemble incomplete, run executor
    retry_count = 0
    while retry_count < max_retries and not ensemble_specs['complete'].all():
        logger.info(f"🔄 Ensemble attempt {retry_count + 1}/{max_retries}")
     
        ensemble_complete = run_with_executor(
            ensemble_specs, event_ticker, "Ensemble", max_workers
        )
     
        if ensemble_complete:
            logger.info(f"✅ Ensemble complete!")
            break
         
        retry_count += 1
     
        if retry_count < max_retries:
            wait_time = 30 * retry_count
            logger.info(f"⏳ Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
 
    # Final status
    if forecasting_specs['complete'].all() and ensemble_specs['complete'].all():
        logger.info(f"🎉 {event_ticker} FULLY COMPLETE!")
    else:
        failed_ensemble = len(ensemble_specs) - ensemble_specs['complete'].sum()
        logger.warning(f"⚠️ {event_ticker} incomplete: {failed_ensemble} ensemble tasks failed")
 
    return forecasting_specs, ensemble_specs


def run_multiple_events_safe(event_tickers, specs_template, max_workers=3, max_retries=3):
    """Run multiple events with user confirmation"""
    for i, event_ticker in enumerate(event_tickers):
        logger.info(f"\n🎯 EVENT {i+1}/{len(event_tickers)}: {event_ticker}")
     
        try:
            run_single_event_safe_df(specs_template, event_ticker,max_workers=max_workers, max_retries=max_retries)
         
            # if i < len(event_tickers) - 1:
            #     user_input = input(f"❓ Continue to next event? (Y/n): ")
            #     if user_input.lower() == 'n':
            #         break
                 
        except KeyboardInterrupt:
            logger.info("🛑 User interrupted")
            break



def run_ensemble(event: ForecastingEvent, search_option: bool, condition, number_of_summaries: int):
    agent = LLMAgent(temp="high", search=search_option)
    run = EnsembleRun(event, agent, run_id=0, condition=condition, number_of_summaries=number_of_summaries)
    run.execute()
    run.save()




def combine_seed_results_multi_market(seed_result, same_result, similar_result, different_result, six_result=None):
    """
    Combine all seed forecasting results for multiple markets into a structured format.
   
    Args:
        seed_result: Original forecasting result with research_report and predictions
        same_result: Tuple/list of results using same seed summary
        similar_result: Tuple/list of results using similar rewrites of seed summary  
        different_result: Tuple/list of results using different rewrites of seed summary
        six_result: Tuple/list of results using six-point scale rewrites (optional)
   
    Returns:
        dict: Combined structured data organized by market ticker
    """
   
    def extract_predictions_by_market(result_data):
        """Extract predictions organized by market ticker"""
        market_predictions = defaultdict(list)
       
        # Handle tuple format like sim_run.result
        if isinstance(result_data, tuple) and len(result_data) > 0:
            result_list = result_data[0] if isinstance(result_data[0], list) else [result_data[0]]
        elif isinstance(result_data, list):
            result_list = result_data
        else:
            result_list = [result_data] if result_data else []
       
        for item in result_list:
            if 'predictions' in item and item['predictions']:
                for pred in item['predictions']:
                    ticker = pred.get('ticker', '')
                    market_predictions[ticker].append({
                        'prediction': pred.get('prediction', 0),
                        'reasoning': pred.get('reasoning', ''),
                        'seed_summary': item.get('seed_summary', ''),
                        'seed_index': item.get('seed_index', 0)
                    })
       
        return market_predictions
   
    # Extract original predictions by market
    original_by_market = {}
    if seed_result and 'predictions' in seed_result:
        for pred in seed_result['predictions']:
            ticker = pred.get('ticker', '')
            original_by_market[ticker] = {
                'prediction': pred.get('prediction', 0),
                'reasoning': pred.get('reasoning', ''),
                'research_report': seed_result.get('research_report', '')
            }
   
    # Extract predictions by type and market
    same_by_market = extract_predictions_by_market(same_result)
    similar_by_market = extract_predictions_by_market(similar_result)
    different_by_market = extract_predictions_by_market(different_result)
    six_by_market = extract_predictions_by_market(six_result) if six_result else defaultdict(list)
   
    # Get all unique tickers
    all_tickers = set()
    all_tickers.update(original_by_market.keys())
    all_tickers.update(same_by_market.keys())
    all_tickers.update(similar_by_market.keys())
    all_tickers.update(different_by_market.keys())
    all_tickers.update(six_by_market.keys())
   
    # Organize data by market
    markets_data = {}
   
    for ticker in sorted(all_tickers):
        markets_data[ticker] = {
            'original': original_by_market.get(ticker, {}),
            'same': {
                'predictions': same_by_market.get(ticker, []),
                'count': len(same_by_market.get(ticker, []))
            },
            'similar': {
                'predictions': similar_by_market.get(ticker, []),
                'count': len(similar_by_market.get(ticker, []))
            },
            'different': {
                'predictions': different_by_market.get(ticker, []),
                'count': len(different_by_market.get(ticker, []))
            },
            'six_points': {
                'predictions': six_by_market.get(ticker, []),
                'count': len(six_by_market.get(ticker, []))
            }
        }
   
    # Calculate overall metadata
    total_predictions_count = 0
    for ticker in all_tickers:
        total_predictions_count += (
            (1 if ticker in original_by_market else 0) +
            len(same_by_market.get(ticker, [])) +
            len(similar_by_market.get(ticker, [])) +
            len(different_by_market.get(ticker, [])) +
            len(six_by_market.get(ticker, []))
        )
   
    combined_data = {
        'markets': markets_data,
        'metadata': {
            'total_markets': len(all_tickers),
            'total_predictions': total_predictions_count,
            'market_tickers': sorted(list(all_tickers)),
            'has_original': len(original_by_market) > 0,
            'has_same': len(same_by_market) > 0,
            'has_similar': len(similar_by_market) > 0,
            'has_different': len(different_by_market) > 0,
            'has_six_points': len(six_by_market) > 0
        }
    }
   
    return combined_data


def calculate_per_market_statistics(combined_data):
    """Calculate comprehensive statistics for each market"""
    market_stats = {}
   
    for ticker, market_data in combined_data['markets'].items():
        # Collect all predictions for this market
        all_predictions = []
        predictions_by_type = {
            'original': [],
            'same': [],
            'similar': [],
            'different': [],
            'six_points': []
        }
       
        # Original prediction
        if market_data['original']:
            orig_pred = market_data['original'].get('prediction', 0)
            all_predictions.append(orig_pred)
            predictions_by_type['original'].append(orig_pred)
       
        # Same predictions
        for pred in market_data['same']['predictions']:
            val = pred['prediction']
            all_predictions.append(val)
            predictions_by_type['same'].append(val)
       
        # Similar predictions
        for pred in market_data['similar']['predictions']:
            val = pred['prediction']
            all_predictions.append(val)
            predictions_by_type['similar'].append(val)
       
        # Different predictions
        for pred in market_data['different']['predictions']:
            val = pred['prediction']
            all_predictions.append(val)
            predictions_by_type['different'].append(val)
       
        # Six-point predictions
        for pred in market_data['six_points']['predictions']:
            val = pred['prediction']
            all_predictions.append(val)
            predictions_by_type['six_points'].append(val)
       
        # Calculate overall statistics for this market
        if all_predictions:
            market_stats[ticker] = {
                'overall': {
                    'count': len(all_predictions),
                    'mean': statistics.mean(all_predictions),
                    'median': statistics.median(all_predictions),
                    'std_dev': statistics.stdev(all_predictions) if len(all_predictions) > 1 else 0,
                    'min': min(all_predictions),
                    'max': max(all_predictions),
                    'range': max(all_predictions) - min(all_predictions)
                },
                'by_type': {}
            }
           
            # Calculate statistics by prediction type
            for pred_type, values in predictions_by_type.items():
                if values:
                    market_stats[ticker]['by_type'][pred_type] = {
                        'count': len(values),
                        'mean': statistics.mean(values),
                        'median': statistics.median(values),
                        'std_dev': statistics.stdev(values) if len(values) > 1 else 0,
                        'min': min(values),
                        'max': max(values),
                        'values': values  # Include raw values for reference
                    }
                else:
                    market_stats[ticker]['by_type'][pred_type] = {
                        'count': 0,
                        'mean': None,
                        'median': None,
                        'std_dev': None,
                        'min': None,
                        'max': None,
                        'values': []
                    }
   
    return market_stats


def save_multi_market_analysis(combined_data, market_stats, filename="multi_market_seed_analysis.json"):
    """Save the complete multi-market analysis to JSON"""
   
    # Combine data and statistics
    output_data = {
        'combined_data': combined_data,
        'market_statistics': market_stats,
        'summary': {
            'markets_analyzed': len(combined_data['markets']),
            'total_predictions': combined_data['metadata']['total_predictions']
        }
    }
   
    # Save to file
    with open(filename, 'w') as f:
        json.dump(output_data, f, indent=2)
   
    print(f"Multi-market analysis saved to {filename}")
   
    # Print summary
    print(f"\n=== MULTI-MARKET SEED ANALYSIS SUMMARY ===")
    print(f"Total Markets: {len(combined_data['markets'])}")
    print(f"Total Predictions: {combined_data['metadata']['total_predictions']}")
    print(f"\nMarkets: {', '.join(combined_data['metadata']['market_tickers'])}")
   
    print(f"\n=== PER-MARKET STATISTICS ===")
    for ticker in sorted(market_stats.keys()):
        stats = market_stats[ticker]
        print(f"\n{ticker}:")
        print(f"  Overall: {stats['overall']['count']} predictions")
        print(f"  Mean: {stats['overall']['mean']:.3f}")
        print(f"  Std Dev: {stats['overall']['std_dev']:.3f}")
        print(f"  Range: {stats['overall']['min']:.3f} - {stats['overall']['max']:.3f}")
       
        # Show means by type
        for pred_type in ['original', 'same', 'similar', 'different', 'six_points']:
            type_stats = stats['by_type'][pred_type]
            if type_stats['count'] > 0:
                print(f"  {pred_type.title()}: {type_stats['count']} predictions, mean={type_stats['mean']:.3f}")


def analyze_cross_market_variance(market_stats):
    """Analyze how much variance exists across different markets"""
   
    # Collect means by prediction type across all markets
    means_by_type = defaultdict(list)
   
    for ticker, stats in market_stats.items():
        for pred_type, type_stats in stats['by_type'].items():
            if type_stats['count'] > 0:
                means_by_type[pred_type].append(type_stats['mean'])
   
    cross_market_analysis = {}
    for pred_type, means in means_by_type.items():
        if len(means) > 1:
            cross_market_analysis[pred_type] = {
                'markets_count': len(means),
                'mean_of_means': statistics.mean(means),
                'std_of_means': statistics.stdev(means),
                'min_mean': min(means),
                'max_mean': max(means),
                'range_of_means': max(means) - min(means)
            }
   
    return cross_market_analysis


def run_complete_multi_market_analysis(seed_result, same_result, similar_result, different_result=None, six_result=None):
    """Run complete analysis and save results"""
   
    # Combine the results
    combined = combine_seed_results_multi_market(
        seed_result, same_result, similar_result, different_result, six_result
    )
   
    # Calculate per-market statistics
    market_stats = calculate_per_market_statistics(combined)
   
    # Analyze cross-market variance
    cross_market = analyze_cross_market_variance(market_stats)
   
    # Add cross-market analysis to output
    combined['cross_market_analysis'] = cross_market
   
    # Save everything
    save_multi_market_analysis(combined, market_stats)
   
    return combined, market_stats, cross_market