📊 Real-Time Financial Data Streaming & Analytics Platform
Overview

This project is an end-to-end real-time data engineering pipeline designed to ingest, process, clean, analyze, and prepare financial market data from multiple sources for downstream analytics and dashboarding.

It uses Apache Kafka for streaming ingestion and Apache Spark Structured Streaming for real-time processing, analytics, and sentiment analysis.
The final processed data is designed to be consumed by Power BI dashboards.

🎯 Project Objectives

Ingest real-time financial data from news and social platforms
Stream data reliably using Kafka
Process data using Spark Structured Streaming
Clean and normalize noisy text data
Perform sentiment analysis on financial discussions
Track and analyze stock ticker mentions
Prepare analytics-ready datasets for Power BI visualization

🏗️ Architecture Overview
[Producers]
  ├── News APIs
  ├── Twitter/X (scraping)
  ├── Reddit
  ├── Yahoo Finance
  └── Finance News Sites
        ↓
     Apache Kafka
        ↓
  Spark Structured Streaming
        ↓
 ┌───────────────┐
 │ Data Cleaning │  (Week 5)
 └───────────────┘
        ↓
 ┌───────────────────┐
 │ Sentiment Analysis │ (Week 6)
 └───────────────────┘
        ↓
   Storage Layer
 (CSV / Database)
        ↓
     Power BI

🧰 Technology Stack
Streaming & Processing
Apache Kafka
Apache Spark (Structured Streaming)
Docker & Docker Compose
Programming & Libraries
Python 3.11
pyspark
kafka-python / confluent-kafka
vaderSentiment / NLP tools

Data Sources
News APIs
Twitter/X (scraped)
Reddit
Yahoo Finance
Finviz / MarketWatch / Benzinga
Visualization
Power BI (final dashboard)

📂 Project Structure
week3-4-project/
│
├── common/                # Shared Docker & scripts
├── infra/                 # Kafka, Zookeeper, Spark setup
├── jars/                  # Kafka–Spark connector JARs
├── Ticker/                # Stock ticker master CSV
│   └── tickers.csv
│
├── week3-news/             # News ingestion & streaming
│   ├── news-producer
│   └── spark-streaming
│
├── week4-social/           # Social data ingestion & analytics
│   ├── twitter-producer
│   ├── reddit-producer
│   ├── yahoo-producer
│   ├── finance-news-producer
│   └── spark-analytics
│
├── week5-cleaning/         # Data cleaning (Spark)
│   └── spark-cleaning
│
├── week6-sentiment/        # Sentiment analysis (Spark)
│   └── sentiment-analysis
│
└── README.md

📅 Week-Wise Implementation
✅ Week 3 — News Streaming

News API ingestion
Kafka topic creation
Spark consumer validation
End-to-end streaming verification

✅ Week 4 — Social Data Ingestion

Twitter/X, Reddit, Yahoo Finance producers
Unified JSON schema
Multi-topic Spark analytics
Ticker detection using CSV-based reference list

⏳ Week 5 — Data Cleaning

Text normalization
URL & emoji removal
Case normalization
Deduplication
Cleaned stream written to a new Kafka topic

⏳ Week 6 — Sentiment Analysis

Sentiment scoring (Positive / Neutral / Negative)
Aggregation by ticker & time window
Storage to analytics-ready format (CSV / DB)

📈 Key Features

Real-time multi-source ingestion
Fault-tolerant streaming
Schema-driven Spark processing
Dynamic ticker detection via CSV
Modular & scalable architecture
Power BI–ready analytics output

🧪 Testing & Validation
Kafka UI for topic monitoring
Spark console output validation
Smoke tests for producers
Manual inspection of cleaned & enriched data

🚀 Future Enhancements

Elasticsearch + Kibana dashboards
Real-time alerting for sentiment spikes
Machine-learning-based sentiment models
Deployment on cloud (AWS / GCP / Azure)

Author
Vineet Rai GitHub: https://github.com/Itsvineetrai
Deployment on cloud (AWS / GCP / Azure)

Stream scheduling & orchestration (Airflow)
