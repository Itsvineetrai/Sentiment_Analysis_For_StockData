import os
import json
import re
import time
import praw
import pandas as pd
from confluent_kafka import Producer

# --- Kafka config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_REDDIT", "reddit")
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

# --- Reddit API config ---
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT", "my-reddit-app:v1.0").strip()
)

# --- Subreddits and tickers ---
SUBREDDITS = ["stocks", "investing", "wallstreetbets", "finance"]

TICKERS_CSV = os.getenv("TICKERS_CSV", "/data/tickers.csv")
tickers_df = pd.read_csv(TICKERS_CSV)
TICKERS = tickers_df['Ticker'].tolist()


def delivery_report(err, msg):
    if err:
        print(f"❌ Kafka error: {err}")
    else:
        print(f"✅ Sent: {msg.value().decode('utf-8')[:120]}...")

def push_to_kafka(msg):
    producer.produce(TOPIC, json.dumps(msg).encode("utf-8"), callback=delivery_report)
    producer.poll(0)

# --- Backfill recent posts ---
print("Fetching recent Reddit posts (backfill)...")
for subreddit in SUBREDDITS:
    try:
        for post in reddit.subreddit(subreddit).new(limit=200):  # limit lowered
            text = f"{post.title} {post.selftext}"
            words = set(text.split())
            symbols = [w for w in words if w in TICKERS]
            if symbols:
                msg = {
                    "source": "reddit",
                    "id": post.id,
                    "title": post.title,
                    "selftext": post.selftext,
                    "subreddit": post.subreddit.display_name,
                    "created": post.created_utc,
                    "symbols": symbols
                }
                push_to_kafka(msg)
            time.sleep(1)  # ⏳ 1 request per second
        producer.flush()
        print(f"✅ Backfill done for r/{subreddit}")
    except Exception as e:
        print(f"⚠️ Error fetching r/{subreddit}: {e}")

# --- Live streaming new posts ---
print("Starting live stream for new posts...")
try:
    for post in reddit.subreddit("+".join(SUBREDDITS)).stream.submissions(skip_existing=True):
        try:
            text = f"{post.title} {post.selftext}"
            words = set(text.split())
            symbols = [w for w in words if w in TICKERS]
            if symbols:
                msg = {
                    "source": "reddit",
                    "id": post.id,
                    "title": post.title,
                    "selftext": post.selftext,
                    "subreddit": post.subreddit.display_name,
                    "created": post.created_utc,
                    "symbols": symbols
                }
                push_to_kafka(msg)
            time.sleep(1)  # ⏳ avoid hitting Reddit’s rate limit
        except Exception as e:
            print(f"⚠️ Error processing post: {e}")
except KeyboardInterrupt:
    print("🛑 Stream stopped by user")

producer.flush()
print("All Reddit posts have been sent to Kafka.")
