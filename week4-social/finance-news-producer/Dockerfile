FROM python:3.11-slim
WORKDIR /app 

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src
ENV TICKERS_CSV=/data/tickers.csv

CMD [ "python", "src/finance_news_producer.py" ]
