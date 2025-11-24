import os
import json
import time
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
import feedparser
import pandas as pd
from confluent_kafka import Producer

# --- Kafka Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "finance-news")
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(f"✅ Delivered {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# --- Load tickers (inside container) ---
TICKERS_CSV = os.getenv("TICKERS_CSV", "/data/tickers.csv")
TICKERS_DF = pd.read_csv(TICKERS_CSV)
TICKERS = set(TICKERS_DF['Ticker'].tolist())
print(f"📈 Loaded {len(TICKERS)} tickers")

# --- Utility Functions ---
def extract_tickers(text):
    words = set(re.findall(r'\b[A-Z]{1,5}\b', text))
    return [w for w in words if w in TICKERS]

def normalize_item(source, title, url):
    symbols = extract_tickers(title)
    if not symbols:
        return None
    return {
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": title,
        "symbols": symbols,
        "url": url
    }

# --- News Sources ---
def fetch_finviz():
    url = "https://finviz.com/news.ashx"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.select("a.nn-tab-link"):
        item = normalize_item("Finviz", a.get_text(strip=True), a["href"])
        if item:
            yield item

def fetch_marketwatch():
    url = "https://feeds.marketwatch.com/marketwatch/topstories/"
    feed = feedparser.parse(url)
    for entry in feed.entries:
        item = normalize_item("MarketWatch", entry.title, entry.link)
        if item:
            yield item

def fetch_benzinga():
    try:
        url = "https://www.benzinga.com/feed"
        feed = feedparser.parse(url)
        for entry in feed.entries:
            item = normalize_item("Benzinga", entry.get("title", ""), entry.get("link", ""))
            if item:
                yield item
    except Exception as e:
        print(f"⚠️ Error fetching from Benzinga: {e}")

# --- Intervals & Tracking ---
SOURCE_INTERVALS = {"finviz": 300, "marketwatch": 300, "benzinga": 300}
LAST_FETCH = {source: 0 for source in SOURCE_INTERVALS}
SENT_URLS = {source: set() for source in SOURCE_INTERVALS}

# --- Main Loop ---
if __name__ == "__main__":
    print("🚀 Starting continuous news fetcher with tickers filtering...")
    try:
        while True:
            now = time.time()
            for fetcher in [fetch_finviz, fetch_marketwatch, fetch_benzinga]:
                source_name = fetcher.__name__.replace("fetch_", "")
                interval = SOURCE_INTERVALS[source_name]

                if now - LAST_FETCH[source_name] >= interval:
                    try:
                        items = list(fetcher())
                        new_count = 0
                        for news in items:
                            if news["url"] not in SENT_URLS[source_name]:
                                producer.produce(
                                    KAFKA_TOPIC,
                                    key=news["source"],
                                    value=json.dumps(news).encode("utf-8"),
                                    callback=delivery_report
                                )
                                SENT_URLS[source_name].add(news["url"])
                                new_count += 1
                        producer.flush()
                        LAST_FETCH[source_name] = now
                        print(f"🔹 {source_name} fetched {len(items)} items, {new_count} new")
                    except Exception as e:
                        print(f"⚠️ Error fetching {source_name}: {e}")
                else:
                    remaining = interval - (now - LAST_FETCH[source_name])
                    print(f"⏳ {source_name} will fetch in {int(remaining)}s")
            time.sleep(5)
    except KeyboardInterrupt:
        print("🛑 Stopped by user")
