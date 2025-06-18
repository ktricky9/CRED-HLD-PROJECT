#!/usr/bin/env python3
"""
Process the evolved schema data using Hudi's schema evolution capability
"""
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession with Hudi
spark = SparkSession.builder \
    .appName("HudiSchemaEvolution") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

print("\n===== READING LATEST RECORDS FROM KAFKA =====")

# Define schema to parse JSON payload
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("__lsn", IntegerType(), True),
    # New fields - will be inferred if not specified
    StructField("payment_method", StringType(), True),
    StructField("shipping_provider", StringType(), True),
    StructField("items_count", IntegerType(), True)
])

# Read from Kafka
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders_cdc") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse JSON
from pyspark.sql.functions import col, from_json, to_date, date_format
orders_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Add partition field - same as original
def add_partition_field(df):
    return df.withColumn("partition_date", date_format(to_date(col("created_at")), "yyyy-MM-dd"))

orders_df = add_partition_field(orders_df)

# Show the evolved schema
print("\n===== EVOLVED SCHEMA WITH NEW FIELDS =====")
orders_df.printSchema()

# Show sample data with new fields
print("\n===== SAMPLE RECORDS WITH NEW FIELDS =====")
orders_df.select("order_id", "payment_method", "shipping_provider", "items_count").show(5, truncate=False)

# Write to the same Hudi table (schema evolution)
print("\n===== WRITING TO HUDI WITH SCHEMA EVOLUTION =====")
output_path = "s3a://hudi-data/orders"

orders_df.write.format("hudi") \
    .option("hoodie.table.name", "orders") \
    .option("hoodie.datasource.write.recordkey.field", "order_id") \
    .option("hoodie.datasource.write.partitionpath.field", "partition_date") \
    .option("hoodie.datasource.write.precombine.field", "__lsn") \
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ") \
    .option("hoodie.datasource.hive_sync.enable", "false") \
    .option("hoodie.datasource.write.hive_style_partitioning", "true") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save(output_path)

# Read back the table to confirm schema evolution
print("\n===== READING BACK HUDI TABLE TO VERIFY SCHEMA EVOLUTION =====")
result_df = spark.read.format("hudi").load(output_path)
print("Updated schema of the Hudi table:")
result_df.printSchema()

# Show records with the new fields
print("\n===== VERIFYING NEW FIELDS IN THE HUDI TABLE =====")
result_df.createOrReplaceTempView("orders")

# Count records with new fields vs. total
total_count = result_df.count()
new_schema_count = result_df.filter(col("payment_method").isNotNull()).count()

print(f"Total records in table: {total_count}")
print(f"Records with new fields: {new_schema_count}")

# Show sample of records with new fields
print("\nSample of records with new fields:")
spark.sql("""
  SELECT 
    order_id, 
    customer_id, 
    payment_method, 
    shipping_provider,
    items_count
  FROM orders 
  WHERE payment_method IS NOT NULL
  LIMIT 5
""").show(truncate=False)

print("\n✅ Schema evolution successful! Hudi table now includes the new fields.")
spark.stop()
