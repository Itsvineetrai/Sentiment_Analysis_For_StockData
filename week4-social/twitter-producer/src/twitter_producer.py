import os
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import tweepy

# --- Kafka config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_TWEETS", "tweets")
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    if err:
        print(f"❌ Kafka error: {err}")
    else:
        print(f"✅ Delivered: {msg.value().decode('utf-8')[:100]}...")

# --- Twitter config ---
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)

# --- Keywords ---
KEYWORDS = ["stocks", "investing", "wallstreetbets", "finance", "AAPL", "TSLA", "GOOG"]
QUERY = " OR ".join(KEYWORDS)

# --- Rate-limit safe interval ---
FETCH_INTERVAL = 15 * 60  # 15 minutes
LAST_FETCH_FILE = "/tmp/last_fetch.txt"

def read_last_fetch():
    if os.path.exists(LAST_FETCH_FILE):
        try:
            return float(open(LAST_FETCH_FILE).read().strip())
        except:
            return 0
    return 0

def write_last_fetch(ts):
    with open(LAST_FETCH_FILE, "w") as f:
        f.write(str(ts))

def push_to_kafka(tweet):
    producer.produce(TOPIC, json.dumps(tweet).encode("utf-8"), callback=delivery_report)
    producer.flush()

def fetch_tweets(since_id=None):
    print("🔹 Fetching tweets...")
    try:
        tweets = client.search_recent_tweets(
            query=QUERY,
            max_results=100,          # Max allowed per request
            since_id=since_id,
            tweet_fields=["created_at", "text", "id", "author_id"]
        )
        new_since_id = since_id
        if tweets.data:
            for t in tweets.data:
                msg = {
                    "source": "twitter",
                    "id": t.id,
                    "author_id": t.author_id,
                    "text": t.text,
                    "keywords": [kw for kw in KEYWORDS if kw.lower() in t.text.lower()],
                    "created_at": str(t.created_at)
                }
                push_to_kafka(msg)
            new_since_id = tweets.data[0].id
        return new_since_id
    except tweepy.TooManyRequests:
        print(f"⚠️ Rate limit exceeded. Waiting {FETCH_INTERVAL} seconds...")
        time.sleep(FETCH_INTERVAL)
        return since_id
    except Exception as e:
        print(f"⚠️ Twitter API error: {e}")
        return since_id

if __name__ == "__main__":
    print("🚀 Starting Twitter fetcher (free-tier safe)...")
    last_fetch_time = read_last_fetch()
    now = time.time()

    # If last fetch was less than 15 min ago, sleep remaining time
    if now - last_fetch_time < FETCH_INTERVAL:
        wait_time = FETCH_INTERVAL - (now - last_fetch_time)
        print(f"⏳ Sleeping {int(wait_time)}s to respect rate limits...")
        time.sleep(wait_time)

    since_id = None
    while True:
        since_id = fetch_tweets(since_id)
        last_fetch_time = time.time()
        write_last_fetch(last_fetch_time)
        print(f"⏳ Sleeping {FETCH_INTERVAL//60} minutes before next fetch...")
        time.sleep(FETCH_INTERVAL)
