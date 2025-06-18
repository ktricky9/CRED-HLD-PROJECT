#!/bin/bash
echo "Step 5: Process Upsert Events with Hudi"
echo "========================================================"

# Check if rebuild flag is passed
if [ "$1" = "rebuild" ]; then
  echo "Rebuilding Spark image with debugging utilities..."
  podman-compose build spark
  echo "Rebuild complete"
fi

echo "Processing upsert events from Kafka to Hudi..."
echo "This demonstrates Hudi's upsert capability - records with the same key will be updated"

# Define the kafka topic name that will be used consistently across all scripts
KAFKA_TOPIC="orders_cdc"
echo "Using Kafka topic: $KAFKA_TOPIC"

# Configure Spark UI access
SPARK_UI_WAIT_SECONDS=300
echo "Spark UI will be available at http://localhost:4040 for $SPARK_UI_WAIT_SECONDS seconds after job completion"

# Run the ingestion job with the same Kafka topic
podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e KAFKA_TOPIC=$KAFKA_TOPIC \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -e SPARK_UI_WAIT_SECONDS=$SPARK_UI_WAIT_SECONDS \
  -p 4040:4040 \
  spark

echo -e "\n✅ Upsert processing complete!"
echo "You can verify the updates by running the next step to compare data before and after"
echo "Notice the updated status and amount values in the sample data above"
