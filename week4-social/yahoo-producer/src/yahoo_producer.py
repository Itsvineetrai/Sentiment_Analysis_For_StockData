import os
import json
import time
from yahoo_fin import news
from confluent_kafka import Producer
from textblob import TextBlob
import pandas as pd

# --- Kafka config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_YAHOO", "yahoo")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# --- Load tickers from CSV ---
TICKERS_CSV = os.getenv("TICKERS_CSV", "/data/tickers.csv")
tickers_df = pd.read_csv(TICKERS_CSV)
TICKERS = tickers_df['Ticker'].tolist()

# --- Deduplication to avoid sending same news multiple times ---
SENT_URLS = set()

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered: {msg.value().decode('utf-8')[:100]}...")

def fetch_and_send_news():
    for ticker in TICKERS:
        try:
            headlines = news.get_yf_rss(ticker)
            if not headlines:
                print(f"⚠️ No news found for {ticker}")
                continue

            for item in headlines:
                url = item.get("link")
                if not url or url in SENT_URLS:
                    continue  # skip duplicates

                title = item.get("title")
                sentiment = TextBlob(title).sentiment.polarity if title else 0

                msg = {
                    "symbol": ticker,
                    "title": title,
                    "link": url,
                    "publisher": item.get("publisher"),
                    "pubDate": item.get("pubDate"),
                    "sentiment": sentiment
                }

                producer.produce(TOPIC, json.dumps(msg).encode("utf-8"), callback=delivery_report)
                SENT_URLS.add(url)
                producer.poll(0)

        except Exception as e:
            print(f"⚠️ Error fetching news for {ticker}: {e}")

if __name__ == "__main__":
    print("🚀 Starting Yahoo news producer for selected tickers...")
    try:
        while True:
            fetch_and_send_news()
            producer.flush()
            print("⏳ Sleeping for 5 minutes before next fetch...")
            time.sleep(300)  # keep your original sleep
    except KeyboardInterrupt:
        print("🛑 Stopped by user")
    finally:
        producer.flush()
