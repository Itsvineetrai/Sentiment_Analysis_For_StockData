from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, lower
from pyspark.sql.types import StringType
import re
import nltk

# Download stopwords if not already done
nltk.download('stopwords')
from nltk.corpus import stopwords

STOPWORDS = set(stopwords.words("english"))

def clean_text(text: str) -> str:
    if text is None:
        return ""
    # Remove URLs
    text = re.sub(r"http\S+|www\S+", "", text)
    # Lowercase
    text = text.lower()
    # Remove stopwords
    tokens = [word for word in text.split() if word not in STOPWORDS]
    return " ".join(tokens)

clean_text_udf = udf(clean_text, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkCleaningJob") \
        .getOrCreate()

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_texts") \
        .load()

    # Parse value as string
    text_df = raw_df.selectExpr("CAST(value AS STRING) as text")

    # Apply cleaning UDF
    cleaned_df = text_df.withColumn("cleaned_text", clean_text_udf(col("text")))

    # Write to parquet
    query = cleaned_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/tmp/cleaned_data/") \
        .option("checkpointLocation", "/tmp/checkpoints/cleaning") \
        .start()

    query.awaitTermination()
    