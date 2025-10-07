#!/bin/bash
echo "Step 1: Starting Infrastructure (Kafka, ZooKeeper, MinIO, Kafka UI)"
echo "========================================================"

# Stop any existing containers
podman-compose down

# Start only infrastructure services
podman-compose up -d zookeeper kafka minio kafka-ui

echo "Waiting 10 seconds for services to fully initialize..."
sleep 10

echo "Services started successfully!"
echo "  - Kafka UI:  http://localhost:8081"
echo "  - MinIO UI:  http://localhost:9001 (login: minioadmin/minioadmin)"
echo ""
echo "NOTE: The Kafka UI and MinIO UI pages should be open in your browser for the demo"
