#!/bin/bash
echo "Step 8: Schema Evolution"
echo "========================================================="

echo "Running producer with evolved schema (new fields)..."
# Copy the evolved_producer.py to the container and run it from there
podman-compose run -v $(pwd)/demo:/demo -e PYTHONUNBUFFERED=1 --entrypoint "python /demo/evolved_producer.py" spark

echo ""
echo "Now let's process the evolved data with Spark:"
echo "================================================"

# Configure Spark UI wait time and network binding for Bitnami Spark container
echo "Configuring Spark UI for schema evolution job..."
podman-compose run \
  -v $(pwd)/demo:/demo \
  -e PYTHONUNBUFFERED=1 \
  -e SPARK_UI_WAIT_SECONDS=600 \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -p 4040:4040 \
  --entrypoint "python /demo/process_evolved_schema.py --ui-wait 600" \
  spark
