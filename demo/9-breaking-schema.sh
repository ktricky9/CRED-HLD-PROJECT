#!/bin/bash
echo "Step 9: Breaking Schema Changes (Limitations Demo)"
echo "========================================================="

echo "This demo shows schema changes that Hudi DOESN'T handle well"
echo "It's designed to illustrate limitations and best practices"
echo ""

echo "Step 1/2: Producing events with breaking schema changes..."
# Run the breaking schema producer
podman-compose run -v $(pwd)/demo:/demo -e PYTHONUNBUFFERED=1 --entrypoint "python /demo/breaking_schema_producer.py" spark

echo ""
echo "Step 2/2: Processing breaking schema changes and showing Hudi limitations..."
echo "================================================"

# Configure Spark UI wait time and network binding for Bitnami Spark container
podman-compose run \
  -v $(pwd)/demo:/demo \
  -e PYTHONUNBUFFERED=1 \
  -e SPARK_UI_WAIT_SECONDS=600 \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -p 4040:4040 \
  --entrypoint "python /demo/process_breaking_schema.py --ui-wait 600" \
  spark

echo "========================================================="
echo "✅ Breaking Schema Demo Complete!"
echo "This demo illustrates common schema evolution problems that"
echo "would require careful handling in a production environment."
echo "========================================================="
