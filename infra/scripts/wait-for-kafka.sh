#!/usr/bin/env bash
# Script to wait for kafka
set -e
echo "Waiting for Kafka to be ready..."
while ! docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q|head -n1) \
    kafka-topics --list --bootstrap-server kafka:9092 &> /dev/null; do
    echo "Kafka is not ready yet. Retrying in 5 seconds..."
    sleep 5
done
echo "Kafka is ready!"  
echo "You can now create topics using the create_topics.sh script."
#!/usr/bin/env bash