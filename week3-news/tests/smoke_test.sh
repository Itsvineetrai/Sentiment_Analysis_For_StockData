#!/usr/bin/env bash
# Smoke test script
set -e

echo "✅ Checking Kafka topics..."
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q | head -n1) \
  kafka-topics --list --bootstrap-server kafka:9092

echo "✅ Running console consumer (press Ctrl+C to exit)..."
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q | head -n1) \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic news --from-beginning
