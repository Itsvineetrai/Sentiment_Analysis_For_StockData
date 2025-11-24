# Week 3 & 4 Project Structure
# Week 3: NewsAPI → Kafka → Spark Streaming

This module ingests news articles from NewsAPI and streams them into Kafka,
then consumes them in Spark Structured Streaming for further processing.

## Components
- **news-producer/** → Python producer that fetches from NewsAPI and writes to Kafka (`news` topic).
- **spark-streaming/** → Spark Structured Streaming job that consumes the `news` topic.
- **tests/** → Smoke tests for quick validation.

## How to run
1. Start infra (Kafka, Zookeeper, Spark):
   ```bash
   cd infra
   docker-compose up -d
   ./scripts/create_topics.sh

2.Run the NewsAPI producer:
cd week3-news/news-producer
docker build -t news-producer .
docker run --env-file config.example.env --network host news-producer
 
3. cd week3-news/spark-streaming
spark-submit src/news_spark_stream.py


# Week 4: Social Data Ingestion (Twitter & Reddit) → Kafka → Spark Analytics

This module ingests social data from Twitter (via snscrape or Tweepy) 
and Reddit (via PRAW), streams them into Kafka, and runs Spark jobs 
to analyze sentiment, trending topics, or symbol mentions.

## Components
- **twitter-producer/** → Python producer for Twitter/X data
- **reddit-producer/** → Python producer for Reddit data
- **spark-analytics/** → Spark Structured Streaming analytics job

<!-- Commands

1.cd infra ->  docker-compose up -d --build

2. scripts/create_topics.sh

3.docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.6.1 --master spark://spark-master:7077 /opt/bitnami/spark/app/news_spark_stream.py

$env:NEWSAPI_KEY="ecef6f736b4a4d6b9972076a03c21f6d"
 -->
 <!-- docker compose -f infra/docker-compose.yml build social-analytics
docker compose -f infra/docker-compose.yml up -d social-analytics
docker compose -f infra/docker-compose.yml logs -f social-analytics
 -->