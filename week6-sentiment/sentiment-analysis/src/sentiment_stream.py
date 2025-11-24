from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from textblob import TextBlob

def get_sentiment(text: str) -> str:
    if not text:
        return "neutral"
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    return "neutral"

sentiment_udf = udf(get_sentiment, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkSentimentJob") \
        .getOrCreate()

    # Option A: Read from Kafka (cleaned topic)
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "cleaned_texts") \
        .load() \
        .selectExpr("CAST(value AS STRING) as cleaned_text")

    # Apply sentiment UDF
    sentiment_df = df.withColumn("sentiment", sentiment_udf(col("cleaned_text")))

    # Write to console (or parquet for persistence)
    query = sentiment_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()
