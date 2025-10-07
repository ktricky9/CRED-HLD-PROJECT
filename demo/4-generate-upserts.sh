#!/bin/bash
echo "Step 4: Generate and Process Upsert Events"
echo "========================================================"

# Define the kafka topic name that will be used consistently across all scripts
KAFKA_TOPIC="orders_cdc"
echo "Using Kafka topic: $KAFKA_TOPIC"

echo "Step 1/2: Generating upsert data based on existing events..."
# Generate upsert data from existing events
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -v "`pwd`/data:/app/data" \
  --entrypoint "python /app/generate_upsert_data.py --source /app/data/mock_cdc_events.jsonl --output /app/data/mock_upserts.jsonl --count 25" \
  spark

echo -e "\nStep 2/2: Publishing upsert events to Kafka..."
# Send the upsert events to Kafka
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_TOPIC=$KAFKA_TOPIC \
  -v "`pwd`/data:/app/data" \
  --entrypoint "python /app/generate_kafka_data.py --topic $KAFKA_TOPIC --file /app/data/mock_upserts.jsonl" \
  spark

echo -e "\n✅ Upsert events generation and publishing complete!"
echo "Upsert events have been sent to Kafka topic: $KAFKA_TOPIC"
echo "Run the next script to process these upserts with Hudi"
echo "You can verify the raw events by examining: ./data/mock_upserts.jsonl"
