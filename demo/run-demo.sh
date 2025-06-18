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
echo "2. Data generation into Kafka"
echo "3. Data loading from Kafka to Hudi"
echo "4. JSON transformation (flattening nested fields)"
echo "5. Table comparisons and schema inspection"
echo "6. Schema evolution with new fields"
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
read -p "Press ENTER to continue to step 4 (Transform Data)"

echo -e "\n\n"
./demo/4-transform-data.sh
read -p "Press ENTER to continue to step 5 (Compare Tables)"

echo -e "\n\n"
./demo/5-compare-tables.sh
read -p "Press ENTER to continue to step 6 (Schema Evolution)"

echo -e "\n\n"
./demo/6-schema-evolution.sh

echo -e "\n\n"
echo "========================================================"
echo "Demo Complete!"
echo "========================================================"
