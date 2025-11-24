FROM bitnami/spark:3

WORKDIR /opt/spark/app

# Copy requirements first
COPY requirements.txt .

# Switch to root to install build tools
USER root
RUN apt-get update && \
    apt-get install -y build-essential python3-dev && \
    pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ /opt/spark/app/src
ENV TICKERS_CSV=/data/tickers.csv

# Add Kafka connector packages to Spark
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.6.1", \
     "--master", "spark://spark-master:7077", \
     "/opt/spark/app/src/news_spark_stream.py"]

