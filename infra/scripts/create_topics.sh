#!/usr/bin/env bash
# Script to create topics
set -e
echo "Creating Kafka topics..."

docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q|head -n1) \
    kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \\
    --topic news

docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q|head -n1) \
    kafka-topics --create --if-not-exists \ 
    --bootsrap server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \\
    --topic tweets  

docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q|head -n1) \
    kafka-topics --create --if-not-exists \
    --bootsrap server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \\
    --topic reddit

echo "Kafka topics created: news, tweets, reddit"