# Week 3: NewsAPI → Kafka → Spark Streaming

This module ingests news articles from NewsAPI and streams them into Kafka,
then consumes them in Spark Structured Streaming for further processing.

## Components
- **news-producer/** → Python producer that fetches from NewsAPI and writes to Kafka (`news` topic).
- **spark-streaming/** → Spark Structured Streaming job that consumes the `news` topic.
- **tests/** → Smoke tests for quick validation.

## How to run
1. Start infra (Kafka, Zookeeper, Spark):
   ```
   cd infra
   docker-compose up -d
   ./scripts/create_topics.sh

2.Run the NewsAPI producer:
cd week3-news/news-producer
docker build -t news-producer .
docker run --env-file config.example.env --network host news-producer/docker run --env-file config.example.env --network infra_default news-producer
 
3. cd infra
docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.6.1 --master spark://spark-master:7077 /opt/bitnami/spark/app/news_spark_stream.py
