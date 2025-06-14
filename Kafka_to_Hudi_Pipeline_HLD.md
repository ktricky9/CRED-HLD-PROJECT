**High-Level Design (HLD): Kafka to Data Lake Ingestion Pipeline**

---

### 1. Overview

This document outlines the architecture for a data ingestion pipeline that consumes ~2TB/day from Kafka (CDC from OLTP and OLAP streams) and loads it into a Data Lake using Apache Hudi. The design enables upserts, time-travel capabilities, schema evolution, and scalable processing using Spark (batch & streaming).

---

### 2. Ingestion Sources

**a. OLTP Data (CDC Events)**

- Source: Kafka topics (pushed via Debezium or equivalent)
- Nature: Append-heavy with upserts
- Format: AVRO
- Criticality: Mixed (business-critical and regular)

**b. OLAP Data**

- Source: Kafka/Kinesis topics
- Nature: Partitioned, append-only
- Format: JSON
- Criticality: Low (batch processing acceptable)

---

### 3. Data Processing Model

#### a. OLTP Pipeline

| Use Case          | Processing Type          | Spark Mode                          | Output Table Format  |
| ----------------- | ------------------------ | ----------------------------------- | -------------------- |
| Append-Only       | Batch (1-hr)             | Spark Batch                         | Hudi (Copy on Write) |
| Upsert/Update     | Batch (1-hr)             | Spark Batch                         | Hudi (Merge on Read) |
| Business-Critical | Near Real-Time (<5 mins) | Hudi continuous / Spark micro batch | Hudi (Merge on Read) |

#### b. OLAP Pipeline

| Use Case           | Processing Type | Spark Mode           | Output Table Format   |
| ------------------ | --------------- | -------------------- | --------------------- |
| Partitioned Append | Near Real-Time  | Structured Streaming | Parquet (Append only) |

Data is segregated at the consumer based on Kafka partitions and topic metadata.

---

### 4. Hudi Data Lake

- Table Type: COW (Copy-on-Write) or MOR (Merge-on-Read) based on latency requirements
- Partitioning:
  - OLTP Data: Partitioning is based on a combination of domain-specific fields such as `created_at`, `region`, or `entity_type`.
  - OLAP Data: For append-only OLAP streams, partitioning should be primarily time-based — using fields like `event_date` or `event_hour` extracted from message payloads or Kafka timestamps.
- Precombine Field: This will be used to upsert data in case of upsert type. Not applicable for append. We will take `__lsn` field provided by CDC ingestion of OLTP.
- Schema Evolution: Enabled using Hudi's schema merging capabilities. Breaking schemas are not handled. Example: string to int conversion.

---

### 5. Spark Job Configuration

**a. Generic Config Parameters**

Ingestion config

```json
{
  "schema": "db_name",
  "table_name": "table1",
  "kafka_topic": "topic_name",
  "batch_size": "100000",
  "criticality": "high/low",
  "bucket_prefix": "s3://datalake/raw/..."
}
```

**b. Spark Runtime Tuning**

Configurable via external YAML/JSON

Predefined default executor memory and dynamic allocation

**Parameters:**

**Spark Core Settings**

```properties
spark.dynamicAllocation.enabled = true
spark.shuffle.service.enabled = true

spark.dynamicAllocation.minExecutors = 4
spark.dynamicAllocation.maxExecutors = 50

spark.executor.cores = 4
spark.executor.memory = 32g
spark.executor.memoryOverhead = 6g

spark.sql.shuffle.partitions = 300
spark.serializer = org.apache.spark.serializer.KryoSerializer
```

**Hudi-Specific Settings**

```properties
hoodie.datasource.write.table.type = MERGE_ON_READ       # or COPY_ON_WRITE
hoodie.datasource.write.recordkey.field = id
hoodie.datasource.write.precombine.field = __lsn

hoodie.insert.shuffle.parallelism = 200
hoodie.upsert.shuffle.parallelism = 200
hoodie.bulkinsert.shuffle.parallelism = 200
```

---

### 6. Transformations

Configurable JSON Explosions:

- Column flattening
- Array-to-String
- Type casting (e.g., string → timestamp)

Transformation config:

```json
{
  "source_schema": "db_name1",
  "souce_table": "table1",
  "target_schema": "db_name2",
  "target_table": "table2",
  "criticality": "high/low",
  "input_bucket": "datalake",
  "output_bucket": "datalake",
  "transformation_column": "col_name",
  "transformation_type": "type"
}
```

---

### 7. Schema Evolution Strategy

- Supported via Hudi
- Source schema tracked via:
  - Schema Registry (for Avro, required only for OLTP)
  - Inferred via Spark on JSON
- Handles:
  - New columns
  - Column renames (manual intervention required)
  - Type widening

---

### 8. Fault Tolerance & Scalability

- Scalable ingestion: Kafka partitioning + Spark parallelism + Hudi partitioning
- Checkpoints: For OLAP jobs (via checkpoint location in S3)
- Retries: Configurable max retries and backoff
- Audit Logs: Log each batch with job metadata and error tracking

---

### 9. Testing Strategy

- Unit Tests: Transformation functions (e.g., flattening, casting)
- Integration Tests:
  - Kafka-to-Hudi pipeline with mock topics
  - Time-travel queries validation
  - Schema evolution test cases
- End-to-End Tests: Trigger full pipeline on test Kafka cluster with data verification in lake

---

### 10. Tech Stack

| Component    | Technology                                  |
| ------------ | ------------------------------------------- |
| Ingestion    | Apache Kafka                                |
| Processing   | Apache Spark (Batch + Structured Streaming) |
| Lake Storage | Apache Hudi (on S3)                         |
| Metadata     | Internal Table Tracking or Spark Catalog    |
| Monitoring   | Spark UI                                    |
| CI/CD        | GitHub Actions + Docker                     |
| Testing      | PyTest/ScalaTest + TestContainers           |

---

### 11. Pipeline Layout (visual)

*(No update made to diagram — let me know if you want it revised to reflect changes.)*
