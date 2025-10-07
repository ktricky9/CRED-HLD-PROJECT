#!/bin/bash
echo "Step 7: Compare Source and Transformed Tables"
echo "========================================================="

# Run a Spark shell script to show table comparisons
echo "Starting Spark shell for table comparison..."

# Run the comparison script in a Spark shell using the file we created
podman-compose run -v $(pwd)/demo:/demo -e PYTHONUNBUFFERED=1 --entrypoint "spark-shell --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 -i /demo/compare_tables.scala" spark
