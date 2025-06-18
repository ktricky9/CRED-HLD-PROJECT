#!/bin/bash
echo "Step 2: Generate Mock CDC Data in Kafka"
echo "========================================================"
# Script to generate mock CDC data and publish to Kafka

# Parse arguments
REBUILD="no"
EVENT_COUNT=100

# Process command-line args
for arg in "$@"; do
  case $arg in
    rebuild)
      REBUILD="yes"
      ;;
    --count=*)
      EVENT_COUNT="${arg#*=}"
      ;;
  esac
done

# Handle rebuild if needed
if [ "$REBUILD" = "yes" ]; then
  echo "Rebuilding Spark image with debugging tools..."
  cd ../spark && docker build -t cred-spark:latest .
  cd ../demo
  echo "Rebuild complete"
fi

# Display event count info
echo "Generating $EVENT_COUNT events with sequential IDs"

# Install required dependencies and generate mock CDC events
echo "Installing dependencies and generating mock CDC events..."

# Define the kafka topic name that will be used consistently across all scripts
KAFKA_TOPIC="orders_cdc"
echo "Using Kafka topic: $KAFKA_TOPIC"

# Define the data file path to persist events
DATA_FILE="/app/data/mock_cdc_events.jsonl"
echo "Events will be persisted to: $DATA_FILE"

# Create data directory if needed
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  --entrypoint "mkdir -p /app/data" \
  spark

# First generate events to the data file
echo "Step 1/2: Generating $EVENT_COUNT events to file..."
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -v "`pwd`/data:/app/data" \
  --entrypoint "sh -c 'pip install boto3 kafka-python && python /app/generate_kafka_data.py --file $DATA_FILE --events $EVENT_COUNT --start-id 1 --start-lsn 10000 --create-buckets --generate-only'" \
  spark

# Then send the events to Kafka
echo -e "\nStep 2/2: Sending events from file to Kafka..."
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_TOPIC=$KAFKA_TOPIC \
  -v "`pwd`/data:/app/data" \
  --entrypoint "python /app/generate_kafka_data.py --topic $KAFKA_TOPIC --file $DATA_FILE" \
  spark

echo -e "\n✅ Data generation complete!"
echo "You can verify this by checking Kafka UI (http://localhost:8081) for messages in the orders_cdc topic"
echo "Event data is persisted in: ./data/mock_cdc_events.jsonl"
