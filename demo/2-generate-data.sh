#!/bin/bash
echo "Step 2: Generate Mock CDC Data in Kafka"
echo "========================================================"
# Script to generate mock CDC data and publish to Kafka

# Check if rebuild flag is passed
if [ "$1" = "rebuild" ]; then
  echo "Rebuilding Spark image with debugging tools..."
  cd ../spark && docker build -t cred-spark:latest .
  cd ../demo
  echo "Rebuild complete"
fi

# Install required dependencies and generate Kafka data
echo "Installing dependencies and generating mock CDC events in Kafka..."

# Define the kafka topic name that will be used consistently across all scripts
KAFKA_TOPIC="orders_cdc"
echo "Using Kafka topic: $KAFKA_TOPIC"

podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_TOPIC=$KAFKA_TOPIC \
  --entrypoint "sh -c 'pip install boto3 kafka-python && python /app/generate_kafka_data.py --topic $KAFKA_TOPIC --events 100 --create-buckets'" \
  spark

echo "✅ Data generation complete!"
echo "You can verify this by checking Kafka UI (http://localhost:8081) for messages in the orders_cdc topic"
