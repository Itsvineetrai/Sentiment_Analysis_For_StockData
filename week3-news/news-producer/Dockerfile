FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src
COPY config.example.env /app/
ENV TICKERS_CSV=/week3-news/tickers.csv

CMD [ "python", "src/producer.py" ]
