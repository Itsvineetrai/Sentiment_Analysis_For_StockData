FROM bitnami/spark:3.5.1

WORKDIR /opt/spark/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /opt/spark/app/src

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/opt/spark/app/src/social_analytics.py"]