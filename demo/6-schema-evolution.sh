#!/bin/bash
echo "Step 6: Schema Evolution"
echo "========================================================="

echo "Running producer with evolved schema (new fields)..."
# Copy the evolved_producer.py to the container and run it from there
podman-compose run -v $(pwd)/demo:/demo -e PYTHONUNBUFFERED=1 --entrypoint "python /demo/evolved_producer.py" spark

echo ""
echo "Now let's process the evolved data with Spark:"
echo "================================================"

# Run the transformation script in the Spark container with volume mount
podman-compose run -v $(pwd)/demo:/demo -e PYTHONUNBUFFERED=1 --entrypoint "python /demo/process_evolved_schema.py" spark
