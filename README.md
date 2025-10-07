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
- CDC event simulation with configurable row count and sequential IDs
- Upsert event generation and processing with merge-on-read tables
- Spark UI accessible for real-time job monitoring and debugging
- Data summary integration (schema, count, samples) in pipeline
- JSON field flattening transformations with schema preservation
- Schema evolution with new field support (payment_method, shipping_provider, items_count)
- Interactive Spark shell for ad-hoc queries against Hudi tables

## Prerequisites

- **Podman** or **Docker**: This project requires either Podman (recommended) or Docker to run the containerized services
- **Podman Compose** or **Docker Compose**: For orchestrating the multi-container setup
- **Bash shell**: For running the demo scripts
- Minimum 4GB RAM recommended for running all services

### Using Docker Instead of Podman

This project uses `podman` and `podman-compose` commands in the scripts. If you have Docker installed but not Podman, you can create aliases to use Docker instead:

```bash
# Add these lines to your ~/.bashrc or ~/.zshrc
alias podman='docker'
alias podman-compose='docker compose'

# Then reload your shell configuration
source ~/.bashrc  # or source ~/.zshrc if using zsh
```

With these aliases, all the scripts will run Docker commands instead of Podman, without needing any modifications.

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

2. **Generate Mock CDC Data** (supports configurable row count)
   ```sh
   ./demo/2-generate-data.sh --count=100
   ```

3. **Load Data from Kafka to Hudi**
   ```sh
   ./demo/3-load-data.sh
   ```

4. **Generate Upsert Events** (updates to existing records)
   ```sh
   ./demo/4-generate-upserts.sh
   ```

5. **Process Upserts with Hudi**
   ```sh
   ./demo/5-process-upserts.sh
   ```

6. **Transform Data** (JSON flattening)
   ```sh
   ./demo/6-transform-data.sh
   ```

7. **Compare Source and Transformed Tables**
   ```sh
   ./demo/7-compare-tables.sh
   ```

8. **Demonstrate Schema Evolution** (adding new fields)
   ```sh
   ./demo/8-schema-evolution.sh
   ```

9. **Interactive Spark Shell** (for ad-hoc queries)
   ```sh
   ./demo/spark-shell.sh
   ```

## Web UIs

- **Kafka UI**: [http://localhost:8081](http://localhost:8081)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)
  - Login: `minioadmin` / `minioadmin`
- **Spark UI**: [http://localhost:4040](http://localhost:4040)
  - Available during and after job execution (configurable wait time)

## Interactive Spark Shell

The project includes a handy script `demo/spark-shell.sh` that launches an interactive Spark shell with all necessary MinIO/Hudi configurations pre-loaded.

### Usage Examples:

```scala
// Start the shell
./demo/spark-shell.sh

// Inside the Spark shell (Scala):
// Note: For SQL queries, you must register DataFrames as temp views first!

// 1. Read the original Hudi table
val ordersDF = spark.read.format("hudi").load("s3a://hudi-data/orders")

// 1b. Register as temp view for SQL queries
ordersDF.createOrReplaceTempView("orders")

// 2. Show table schema
ordersDF.printSchema()

// 3. Display sample data
ordersDF.show(5)

// 4. Count records
ordersDF.count()

// 5. Run SQL queries
spark.sql("SELECT order_id, customer_id, amount, status.code FROM orders WHERE amount > 5000").show()

// 6. Check for evolved schema fields
val columns = ordersDF.columns
columns.foreach(println)

// 7. Read transformed table
val transformedDF = spark.read.format("hudi").load("s3a://hudi-data/orders_transformed")

// Register transformed table for SQL queries too
transformedDF.createOrReplaceTempView("orders_transformed")
transformedDF.show(5)

// 8. Query the transformed table with SQL
spark.sql("SELECT * FROM orders_transformed WHERE status_code = 'delivered'").show()
```

These examples let you interactively explore the data and schema at any point during or after running the pipeline.

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
  - `4-generate-upserts.sh`: Generates upsert events for existing records
  - `5-process-upserts.sh`: Processes upserts with Hudi merge-on-read
  - `6-transform-data.sh`: Applies transformations (JSON flattening)
  - `7-compare-tables.sh`: Compares source and transformed data
  - `8-schema-evolution.sh`: Demonstrates schema evolution capability
  - `evolved_producer.py`: Generates and sends evolved schema data to Kafka
  - `process_evolved_schema.py`: Processes evolved schema from Kafka to Hudi
  - `spark-shell.sh`: Launches interactive Spark shell with MinIO access
  - `run-demo.sh`: Master script that runs all demo steps
- **spark/app/**: Core application code
  - `main.py`: Main Spark application for Kafka to Hudi ingestion
  - `transform.py`: Transformation logic for JSON flattening
  - `generate_kafka_data.py`: Generates sample CDC events
  - `generate_upsert_data.py`: Generates upsert events for existing records
  - `config.json`: Configuration for ingestion settings
  - `transform_config.json`: Configuration for transformation settings
- **data/**: Sample data files
  - `mock_cdc_events.jsonl`: Sample CDC events for testing
  - `mock_upserts.jsonl`: Sample upsert events for testing
  - `mock_evolved_schema.jsonl`: Sample events with evolved schema

---

This project demonstrates a scalable Kafka-to-Hudi ingestion pattern that supports upserts, schema evolution, and efficient time-based partitioning. The modular design allows for flexible execution of individual pipeline components with real-time monitoring via the Spark UI.