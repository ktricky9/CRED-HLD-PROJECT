# Kafka to Hudi Local Pipeline (Dockerized)

## Overview
This project sets up a local data ingestion pipeline using Kafka (CDC events), Apache Spark, Apache Hudi, and MinIO (as S3 replacement), following the provided HLD. Includes Kafka UI for topic inspection and mock CDC event generation.

## Services
- **Kafka** + **Zookeeper**: For CDC/OLAP event streaming
- **MinIO**: S3-compatible storage for Hudi output
- **Spark**: Runs ingestion jobs (PySpark + Hudi)
- **Kafka UI**: Inspect/manage Kafka topics
- **Mock CDC Producer**: Generates fake `orders` CDC events

## How to Run
1. **Build and Start All Services**
    ```sh
    docker-compose up --build
    ```
2. **Kafka UI**: [http://localhost:8080](http://localhost:8080)
3. **MinIO UI**: [http://localhost:9001](http://localhost:9001) (user/pass: `minioadmin`/`minioadmin`)
4. **Spark Job**: Automatically runs mock CDC producer and ingestion job on container start.

## Config
- Edit `spark/app/config.json` for table/topic/bucket settings.

## Notes
- Data is written to MinIO at `s3a://hudi-data/orders/`.
- Inspect output using MinIO UI.
- Modify `main.py` for custom logic or schema.

---

For any issues, check logs of the `spark` container:
```sh
docker-compose logs spark
```