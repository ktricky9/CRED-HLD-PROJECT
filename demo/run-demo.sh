#!/bin/bash
# Master demo script for Kafka to Hudi pipeline

# Make all scripts executable
chmod +x demo/*.sh

echo "========================================================"
echo "KAFKA TO HUDI PIPELINE DEMO"
echo "========================================================"
echo ""
echo "This demo will showcase:"
echo "1. Infrastructure setup (Kafka, MinIO, etc.)"
echo "2. Data generation into Kafka (configurable row count)"
echo "3. Data loading from Kafka to Hudi"
echo "4. Generate upsert events for existing orders"
echo "5. Process upserts with Hudi (demonstrating merge-on-read)"
echo "6. JSON transformation (flattening nested fields)"
echo "7. Table comparisons and schema inspection"
echo "8. Schema evolution with new fields"

echo ""
echo "Usage Notes:"
echo "- To control the number of events generated: ./demo/2-generate-data.sh --count=500"
echo "- Events will be generated with sequential order_id and LSN values"
echo ""
echo "Press ENTER to start each step when prompted."
echo "========================================================"

read -p "Ready to start the demo? [Press ENTER]"

echo -e "\n\n"
./demo/1-start-infra.sh
read -p "Press ENTER to continue to step 2 (Generate Data in Kafka)"

echo -e "\n\n"
./demo/2-generate-data.sh
read -p "Press ENTER to continue to step 3 (Load Data from Kafka to Hudi)"

echo -e "\n\n"
./demo/3-load-data.sh
read -p "Press ENTER to continue to step 4 (Generate Upsert Events)"

echo -e "\n\n"
./demo/4-generate-upserts.sh
read -p "Press ENTER to continue to step 5 (Process Upserts)"

echo -e "\n\n"
./demo/5-process-upserts.sh
read -p "Press ENTER to continue to step 6 (Transform Data)"

echo -e "\n\n"
./demo/6-transform-data.sh
read -p "Press ENTER to continue to step 7 (Compare Tables)"

echo -e "\n\n"
./demo/7-compare-tables.sh
read -p "Press ENTER to continue to step 8 (Schema Evolution)"

echo -e "\n\n"
./demo/8-schema-evolution.sh

echo -e "\n\n"
echo "========================================================"
echo "Demo Complete!"
echo "========================================================"
