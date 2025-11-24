import os
import re
import pandas as pd
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, to_json, struct
from pyspark.sql.types import StringType, ArrayType, StructType

# -----------------------------
# Load ticker CSV
# -----------------------------
TICKERS_CSV = os.getenv("TICKERS_CSV", "/data/tickers.csv")
tickers_df = pd.read_csv(TICKERS_CSV)
ALIASES = {}
for _, row in tickers_df.iterrows():
    ticker = str(row["Ticker"]).upper()
    aliases = [a.strip().lower() for a in str(row["Aliases"]).split(";")]
    ALIASES[ticker] = aliases
TICKERS = list(ALIASES.keys())

# -----------------------------
# Load NLP model
# -----------------------------
nlp = spacy.load("en_core_web_sm")

# -----------------------------
# Function to detect symbols
# -----------------------------
def detect_symbols(text):
    if not text:
        return []
    symbols = set()
    combined = text.lower()

    # 1. Regex / exact ticker match
    for ticker in TICKERS:
        if re.search(rf"\b{ticker}\b", text.upper()):
            symbols.add(ticker)

    # 2. Alias matching
    for ticker, aliases in ALIASES.items():
        for alias in aliases:
            if alias in combined:
                symbols.add(ticker)

    # 3. Named Entity Recognition
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ == "ORG":
            for ticker, aliases in ALIASES.items():
                if ent.text.lower() in aliases:
                    symbols.add(ticker)

    return list(symbols)

# UDF for Spark
detect_symbols_udf = udf(detect_symbols, ArrayType(StringType()))

# -----------------------------
# Spark session
# -----------------------------
spark = SparkSession.builder.appName("NewsSymbolEnricher").getOrCreate()
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# -----------------------------
# Read news from Kafka
# -----------------------------
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", "news,yahoo_news,reddit,finance-news")
    .option("startingOffsets", "latest")
    .load()
)

# Schema for incoming messages
schema = StructType() \
    .add("source", StringType()) \
    .add("timestamp", StringType()) \
    .add("title", StringType()) \
    .add("text", StringType()) \
    .add("symbols", ArrayType(StringType())) \
    .add("url", StringType())

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# -----------------------------
# Enrich with detected symbols
# -----------------------------
enriched_df = json_df.withColumn(
    "detected_symbols",
    detect_symbols_udf(col("title") + " " + col("text"))
).filter(col("detected_symbols").isNotNull() & (col("detected_symbols").getItem(0).isNotNull()))

# -----------------------------
# Write enriched messages back to Kafka
# -----------------------------
output_df = enriched_df.select(
    to_json(struct(
        "source", "timestamp", "title", "text", "url", "detected_symbols"
    )).alias("value")
)

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", "news-enriched") \
    .option("checkpointLocation", "/tmp/spark_news_symbols_checkpoint") \
    .start()

query.awaitTermination()
