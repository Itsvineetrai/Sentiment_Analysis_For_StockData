import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

#
# Configure Spark and Kafka integration
# - Ensure the Kafka source is available by loading the Kafka connector package
# - Allow overriding Kafka bootstrap via env var when running outside Docker
#

SPARK_KAFKA_PACKAGE_VERSION = os.getenv("SPARK_KAFKA_PACKAGE_VERSION", "3.5.1")
SPARK_KAFKA_COORDINATOR = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_KAFKA_PACKAGE_VERSION}"

spark = (
    SparkSession.builder
    .appName("social-analytics")
    .config("spark.jars.packages", SPARK_KAFKA_COORDINATOR)
    .getOrCreate()
)

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", "news,yahoo_news,reddit,finance-news")
    .option("startingOffsets", "latest")
    .load()
)

schema = (
    StructType()
    .add("id", StringType())
    .add("content", StringType())
    .add("title", StringType())
)

json_df = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

query = json_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
