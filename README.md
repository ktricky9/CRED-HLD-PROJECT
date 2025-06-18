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
- Modular and sequential demo scripts for pipeline components
- CDC event simulation with configurable event count
- Spark UI accessible for real-time job monitoring and debugging
- JSON field flattening transformations with schema preservation
- Schema evolution with new field support

## How to Run

### Option 1: Run Everything at Once
```sh
podman-compose up --build
# or with Docker
docker-compose up --build
```

### Option 2: Step-by-Step Demo (Recommended)

Either run the all-in-one demo script:
```sh
./demo/run-demo.sh
```

Or execute each step individually:

1. **Start Infrastructure**
   ```sh
   ./demo/1-start-infra.sh
   ```

2. **Generate Mock CDC Data**
   ```sh
   ./demo/2-generate-data.sh
   ```

3. **Load Data from Kafka to Hudi**
   ```sh
   ./demo/3-load-data.sh
   ```

4. **Transform Data (JSON Flattening)**
   ```sh
   ./demo/4-transform-data.sh
   ```

5. **Compare Source and Transformed Tables**
   ```sh
   ./demo/5-compare-tables.sh
   ```

6. **Demonstrate Schema Evolution**
   ```sh
   ./demo/6-schema-evolution.sh
   ```

## Web UIs

- **Kafka UI**: [http://localhost:8081](http://localhost:8081)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)
  - Login: `minioadmin` / `minioadmin`
- **Spark UI**: [http://localhost:4040](http://localhost:4040)
  - Available during and after job execution (configurable wait time)

## Configuration

- **Main Configuration**: 
  - Data ingestion: `spark/app/config.json`
  - Data transformation: `spark/app/transform_config.json`
- **Kafka Configuration**: 
  - Internal access: `kafka:29092` (for containers)
  - External access: `localhost:39092` (from host)
  - Default topic: `orders_cdc`
- **MinIO Data**: 
  - Bucket: `hudi-data`
  - Paths: 
    - Source data: `s3a://hudi-data/orders/`
    - Transformed data: `s3a://hudi-data/orders_transformed/`
  - Partitioned by `partition_date` column
- **Spark UI Configuration**:
  - Network binding: `SPARK_LOCAL_IP=0.0.0.0` (for Bitnami Spark container)
  - UI wait time: `SPARK_UI_WAIT_SECONDS=600` (configurable)
  - Default port: `4040` (mapped to host)

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

- **Data Generation**: Modify `spark/app/generate_kafka_data.py` for custom CDC events
- **Data Ingestion**: Customize `spark/app/main.py` for Kafka to Hudi pipeline
- **Data Transformation**: Adjust `spark/app/transform.py` for new transformations
- **Configuration**:
  - `spark/app/config.json` for ingestion settings
  - `spark/app/transform_config.json` for transformation settings

## Project Structure

- **demo/**: Contains all modular demo scripts for each step of the pipeline
  - `1-start-infra.sh`: Starts Kafka, MinIO, and other services
  - `2-generate-data.sh`: Generates mock CDC events in Kafka
  - `3-load-data.sh`: Processes data from Kafka to Hudi
  - `4-transform-data.sh`: Applies transformations (JSON flattening)
  - `5-compare-tables.sh`: Compares source and transformed data
  - `6-schema-evolution.sh`: Demonstrates schema evolution capability
- **spark/app/**: Core application code
  - `main.py`: Kafka to Hudi ingestion logic
  - `transform.py`: Data transformation logic
  - `generate_kafka_data.py`: Mock CDC data generation

---

This project demonstrates a scalable Kafka-to-Hudi ingestion pattern that supports upserts, schema evolution, and efficient time-based partitioning. The modular design allows for flexible execution of individual pipeline components with real-time monitoring via the Spark UI.