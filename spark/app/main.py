import json
import os
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from init_minio import create_buckets

# --- Mock CDC Data Generator ---
def generate_order_event(order_id):
    event = {
        "order_id": str(order_id),
        "customer_id": str(random.randint(1, 100)),
        "amount": random.randint(100, 10000),
        "status": random.choice(["created", "updated", "cancelled"]),
        "created_at": datetime.now().isoformat(),
        "__lsn": random.randint(10000, 99999)
    }
    return event

def produce_mock_cdc_events(topic, bootstrap_servers, num_events=100):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for i in range(num_events):
        event = generate_order_event(i)
        producer.send(topic, event)
        time.sleep(0.1)
    producer.flush()
    producer.close()

# --- Spark Hudi Job ---
def run_spark_hudi_job(config_path):
    with open(config_path) as f:
        config = json.load(f)
    spark = SparkSession.builder \
        .appName("KafkaToHudiPipeline") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    # Read from Kafka
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS")) \
        .option("subscribe", config["kafka_topic"]) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON payload
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", IntegerType()),
        StructField("status", StringType()),
        StructField("created_at", StringType()),
        StructField("__lsn", IntegerType())
    ])
    
    # Add a function to convert ISO timestamp to date format for partitioning
    from pyspark.sql.functions import col, to_date, date_format
    def add_partition_field(df):
        return df.withColumn("partition_date", date_format(to_date(col("created_at")), "yyyy-MM-dd"))
    
    from pyspark.sql.functions import col, from_json
    orders_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Add partition field with file-safe format
    orders_df = add_partition_field(orders_df)
    
    # Write to Hudi on MinIO
    output_path = os.environ.get("HUDI_BUCKET") + config["table_name"]
    orders_df.write.format("hudi") \
        .option("hoodie.table.name", config["table_name"]) \
        .option("hoodie.datasource.write.recordkey.field", "order_id") \
        .option("hoodie.datasource.write.partitionpath.field", "partition_date") \
        .option("hoodie.datasource.write.precombine.field", "__lsn") \
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ") \
        .option("hoodie.datasource.hive_sync.enable", "false") \
        .option("hoodie.datasource.write.hive_style_partitioning", "true") \
        .mode("overwrite") \
        .save(output_path)
    spark.stop()

if __name__ == "__main__":
    # Ensure MinIO bucket exists before starting
    print("Ensuring MinIO bucket exists...")
    create_buckets()
    
    # Produce mock CDC events
    default_config_path = "/app/config.json"
    topic = "orders_cdc"
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    produce_mock_cdc_events(topic, kafka_bootstrap, num_events=100)
    
    # Run Spark Hudi job
    run_spark_hudi_job(default_config_path)
