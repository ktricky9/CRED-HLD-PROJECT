#!/bin/bash
echo "Step 3: Load Orders Data from Kafka to Hudi"
echo "========================================================"

# Check if rebuild flag is passed
REBUILD="${1:-no}"
if [ "$REBUILD" = "rebuild" ]; then
  echo "Rebuilding Spark image with debugging utilities..."
  podman-compose build spark
  echo "Rebuild complete"
fi

# Remind user to generate data first if needed
echo "Note: Make sure to run ./2-generate-data.sh first to generate Kafka data"
echo ""

# Define the kafka topic name - must match the one used in step 2
KAFKA_TOPIC="orders_cdc"
echo "Reading from Kafka topic: $KAFKA_TOPIC"

echo "Running Spark job to process data from Kafka to Hudi..."

# Configure Spark for UI access with Bitnami Spark-specific settings
podman-compose run \
  -e SPARK_UI_WAIT_SECONDS=600 \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_TOPIC=$KAFKA_TOPIC \
  -p 4040:4040 \
  --entrypoint "python /app/main.py" \
  spark

echo "✅ Data loaded successfully!"
echo "You can verify this by looking at MinIO UI (http://localhost:9001) for the hudi-data/orders directory"
echo "The data schema, count, and sample records are displayed above."
