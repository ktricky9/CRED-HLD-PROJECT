#!/bin/bash

echo "==================================================="
echo "Step 8: Schema Evolution with Kafka and Hudi"
echo "==================================================="

# Step 1: Generate fresh mock evolved schema data each time (overwriting existing)
echo "Generating fresh evolved schema data for demo..."
podman-compose run \
  -v $(pwd)/demo:/demo \
  -v $(pwd)/data:/app/data \
  -e PYTHONUNBUFFERED=1 \
  --entrypoint "python /demo/evolved_producer.py --file-only --generate-file --mock-file /app/data/mock_evolved_schema.jsonl" \
  spark

# Step 2: Send the mock data to Kafka
echo ""
echo "Sending evolved schema data to Kafka..."
echo "================================================"
podman-compose run \
  -v $(pwd)/demo:/demo \
  -v $(pwd)/data:/app/data \
  -e PYTHONUNBUFFERED=1 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  --entrypoint "python /demo/evolved_producer.py --mock-file /app/data/mock_evolved_schema.jsonl" \
  spark

# Step 3: Process the evolved data with Spark
echo ""
echo "Now processing the evolved data with Spark from Kafka:"
echo "================================================"

# Configure Spark UI wait time and network binding for Bitnami Spark container
echo "Configuring Spark UI for schema evolution job..."
podman-compose run \
  -v $(pwd)/demo:/demo \
  -v $(pwd)/data:/app/data \
  -e PYTHONUNBUFFERED=1 \
  -e SPARK_UI_WAIT_SECONDS=600 \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e KAFKA_TOPIC=orders_cdc \
  -p 4040:4040 \
  --entrypoint "python /demo/process_evolved_schema.py --use-mock-file --push-to-kafka --mock-file /app/data/mock_evolved_schema.jsonl --ui-wait 600" \
  spark

echo "\n✅ Schema evolution demo complete!"
echo "You can verify schema evolution by checking the Hudi table in MinIO UI (http://localhost:9001)"
echo "Look for the new fields (payment_method, shipping_provider, items_count) in the orders table"
