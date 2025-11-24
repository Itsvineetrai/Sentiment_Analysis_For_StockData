import os
import json
import time
import re
import hashlib
import requests
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime, timedelta, timezone

# --- Kafka Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_NEWS = os.getenv("TOPIC_NEWS", "news")
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

# --- Load tickers + company names + aliases ---
TICKERS_CSV = os.getenv("TICKERS_CSV", "week3-news/tickers.csv")
tickers_df = pd.read_csv(TICKERS_CSV)
tickers_df["Company"] = tickers_df["Company"].str.lower()

# --- Track sent articles to avoid duplicates ---
SENT_URLS_FILE = "sent_urls.json"
if os.path.exists(SENT_URLS_FILE):
    with open(SENT_URLS_FILE, "r") as f:
        SENT_URLS = set(json.load(f))
else:
    SENT_URLS = set()

# --- Kafka callback ---
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered: {msg.value().decode('utf-8')[:100]}...")

# --- Generate hash of an article ---
def get_article_hash(title, text):
    return hashlib.md5(f"{title}{text}".encode("utf-8")).hexdigest()

# --- Extract tickers/company mentions ---
def extract_symbols(title, text):
    symbols = set()
    combined = f"{title} {text}".lower()

    for _, row in tickers_df.iterrows():
        ticker = str(row["Ticker"]).upper()
        company = str(row["Company"]).lower()
        aliases = str(row.get("Aliases", "")).lower().split(";")
        
        # Ticker matching: whole word or (TICKER)
        if re.search(rf"\b{ticker}\b|\({ticker}\)", combined, re.IGNORECASE):
            symbols.add(ticker)

        # Company/alias matching
        for alias in [company] + aliases:
            alias = alias.strip()
            if alias and alias in combined:
                symbols.add(ticker)

    return list(symbols)

# --- Fetch news from NewsAPI ---
def fetch_news(query="finance", from_time=None):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "pageSize": 50,
        "apiKey": os.getenv("NEWSAPI_KEY"),
        "language": "en",
        "sortBy": "publishedAt"
    }
    if from_time:
        params["from"] = from_time.isoformat()
    r = requests.get(url, params=params)
    if r.status_code != 200:
        print(f"⚠️ NewsAPI error: {r.status_code} - {r.text}")
        return []
    return r.json().get("articles", [])

# --- Main loop ---
if __name__ == "__main__":
    last_fetch_time = datetime.now(timezone.utc) - timedelta(minutes=5)

    while True:
        articles = fetch_news("finance", from_time=last_fetch_time)
        new_count = 0

        for art in articles:
            title = art.get("title", "") or ""
            text = art.get("description", "") or ""
            url = art.get("url")
            if not url:
                continue

            article_hash = get_article_hash(title, text)
            if url in SENT_URLS or article_hash in SENT_URLS:
                continue  # skip duplicates

            SENT_URLS.add(url)
            SENT_URLS.add(article_hash)

            symbols = extract_symbols(title, text)

            msg = {
                "source": art.get("source", {}).get("name"),
                "timestamp": art.get("publishedAt"),
                "title": title,
                "text": text,
                "symbols": symbols,
                "url": url,
            }

            # Only produce if article is fetched from API
            producer.produce(
                TOPIC_NEWS,
                json.dumps(msg).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            new_count += 1
 
        producer.flush()
        print(f"⏳ Fetched {len(articles)} articles, {new_count} new sent to Kafka")

        # Persist sent URLs/hashes
        if new_count > 0:
            with open(SENT_URLS_FILE, "w") as f:
                json.dump(list(SENT_URLS), f)

        last_fetch_time = datetime.now(timezone.utc)
        time.sleep(150)  # sleep 5 minutes.