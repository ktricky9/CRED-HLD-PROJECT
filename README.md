# Kafka to Hudi Local Pipeline (Dockerized)

## Overview
This project sets up a local data ingestion pipeline using Kafka (CDC events), Apache Spark, Apache Hudi, and MinIO (as S3 replacement), following the provided HLD. The system processes Change Data Capture (CDC) events from a Kafka topic, transforms them using Spark, and stores them in Apache Hudi format on MinIO (S3-compatible storage).

## Architecture

- **Kafka** + **Zookeeper**: Event streaming platform with dual listeners (internal/external)
- **MinIO**: S3-compatible storage for Hudi tables 
- **Spark**: Runs PySpark jobs with Hudi integration
- **Kafka UI**: Web interface for monitoring Kafka topics and messages
- **Mock CDC Producer**: Generates sample order events with CDC metadata

## Features

- Automated MinIO bucket creation
- Proper partition handling for timestamps in Hudi
- Configurable Kafka topic and bootstrap servers
- Multiple listener configuration for both container and host access
- Interactive demo script for showcasing pipeline components
- CDC event simulation with configurable event count

## How to Run

### Option 1: Run Everything at Once
```sh
podman-compose up --build
# or with Docker
docker-compose up --build
```

### Option 2: Interactive Demo

1. **Start Infrastructure**
   ```sh
   podman-compose up -d zookeeper kafka minio kafka-ui
   ```

2. **Run Demo Script with Options**
   ```sh
   # Setup environment variables
   export KAFKA_BOOTSTRAP_SERVERS=localhost:39092
   export MINIO_ENDPOINT=http://localhost:9001
   export MINIO_ACCESS_KEY=minioadmin
   export MINIO_SECRET_KEY=minioadmin
   export HUDI_BUCKET=s3a://hudi-data/
   
   # Run demo script
   python demo.py --setup      # Create MinIO buckets
   python demo.py --events 25  # Generate 25 CDC events
   python demo.py --process    # Process events with Spark/Hudi
   
   # Or run everything
   python demo.py --all
   ```

## Web UIs

- **Kafka UI**: [http://localhost:8081](http://localhost:8081)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)
  - Login: `minioadmin` / `minioadmin`

## Configuration

- **Main Configuration**: `spark/app/config.json`
  - Configure table name, Kafka topic, etc.
- **Kafka Configuration**: 
  - Internal access: `kafka:29092` (for containers)
  - External access: `localhost:39092` (from host)
- **MinIO Data**: 
  - Bucket: `hudi-data`
  - Path: `s3a://hudi-data/orders/`
  - Partitioned by date from timestamp

## Troubleshooting

```sh
# View logs
podman-compose logs spark    # Spark logs
podman-compose logs kafka    # Kafka logs
podman-compose logs kafka-ui # Kafka UI logs

# Restart specific service
podman-compose restart spark
```

## Development

- Modify `spark/app/main.py` for custom transformations
- Edit `spark/app/config.json` for table/topic configuration
- `demo.py` provides modular pipeline execution for presentations

---

This project demonstrates a scalable Kafka-to-Hudi ingestion pattern that supports upserts, schema evolution, and efficient time-based partitioning.