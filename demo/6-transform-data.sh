#!/bin/bash
echo "Step 6: Transform Orders Data"
echo "========================================================="

# Create a specific container for the transform job
echo "Running transformation job to flatten JSON fields..."

# Get the Spark container ID
SPARK_CONTAINER=$(podman ps | grep spark | awk '{print $1}' | head -n 1)

if [ -z "$SPARK_CONTAINER" ]; then
  echo "Creating a new container for transformation..."
  # If no Spark container is running, start a new one with transform script
  # Using Bitnami-specific configuration for UI access
  podman-compose run \
    -e PYTHONUNBUFFERED=1 \
    -e SPARK_UI_WAIT_SECONDS=600 \
    -e SPARK_LOCAL_IP=0.0.0.0 \
    -p 4040:4040 \
    --entrypoint "python /app/transform.py" \
    spark
else
  echo "Using existing Spark container for transformation..."
  # If a Spark container is already running, use it
  podman exec -it -e SPARK_UI_WAIT_SECONDS=60 $SPARK_CONTAINER python /app/transform.py
fi

echo "✅ Transformation complete!"
echo "You can verify this in MinIO UI (http://localhost:9001)"
echo "Look for the hudi-data/orders_transformed directory"
echo "Notice the flattened status fields (status_code, status_message, etc.) in the sample data above"
