import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from init_minio import create_buckets

# --- Spark Hudi Job ---
def run_spark_hudi_job(config_path):
    """Run a Spark job to process CDC events from Kafka to a Hudi table
    Returns the SparkSession for further operations
    """
    with open(config_path) as f:
        config = json.load(f)
    
    # Get Kafka topic from environment variable or use default
    kafka_topic = os.environ.get("KAFKA_TOPIC", "orders_cdc")
    print(f"Reading from Kafka topic: {kafka_topic}")
    # Ensure Spark UI is properly enabled and configured for network access
    # For Bitnami Spark, configure UI to be accessible from outside the container
    # The key change: use SPARK_LOCAL_IP for bindAddress (critical for Bitnami's Spark image)
    container_ip = os.environ.get("SPARK_LOCAL_IP", "0.0.0.0")
    
    # Create the unique Spark config for Bitnami container
    print("\n=== CONFIGURING SPARK UI FOR BITNAMI SPARK CONTAINER ===")
    print(f"Container IP/Bind Address: {container_ip}")
    print(f"Enabling UI on port 4040")
    
    # Simplified configuration specifically for Bitnami Spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("KafkaToHudiPipeline") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.driver.bindAddress", container_ip) \
        .config("spark.driver.host", container_ip) \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    # Add visible UI message
    print("\n========================================================")
    print("Spark is running with UI enabled")
    print("Access the Spark UI at: http://localhost:4040")
    print("========================================================\n")
    
    # Create a small dataframe and execute an action to ensure UI is populated
    print("\nRunning a small test job to populate the UI...")
    test_df = spark.createDataFrame([(1, "test"), (2, "sample")], ["id", "value"])
    test_df.show()
    
    # Force some activity to appear in the UI
    count = test_df.count()
    print(f"Test data count: {count}")
    
    # Print SparkContext information
    sc = spark.sparkContext
    print(f"\nSpark running with applicationId: {sc.applicationId}")
    
    # Safe way to get UI URL without using Scala Option methods
    try:
        ui_web_url = sc._jsc.sc().uiWebUrl()
        if ui_web_url is not None and ui_web_url.isDefined():
            print(f"Spark Web UI is at: {ui_web_url.get()}")
        else:
            print("Spark Web UI should be at: http://localhost:4040")
    except Exception as e:
        print("Spark Web UI should be at: http://localhost:4040")
        
    print("\n" + "*" * 60)
    
    # Log information about Spark UI
    print("\n=====================================================\n")
    print(f"Spark UI is available at: http://localhost:4040")
    print("\n=====================================================\n")
    
    # Read from Kafka using topic from environment variable
    kafka_topic = os.environ.get("KAFKA_TOPIC", "orders_cdc")
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS")) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(f"Successfully connected to Kafka topic: {kafka_topic}")
    
    
    # Parse JSON using schema inference
    from pyspark.sql.functions import col, to_date, date_format, from_json, expr, schema_of_json
    
    # First, get a sample of the JSON data to infer schema
    print("Inferring schema from JSON data...")
    sample_json_df = df.select(col("value").cast("string").alias("json_str")).limit(10)
    
    # Check if we have data
    if sample_json_df.count() > 0:
        # Infer schema from the first record
        first_json = sample_json_df.first()[0]
        print(f"Sample JSON for schema inference: {first_json[:200]}..." if len(first_json) > 200 else first_json)
        inferred_schema = spark.read.json(spark.sparkContext.parallelize([first_json])).schema
        print("Inferred schema:")
        for field in inferred_schema.fields:
            print(f"  - {field.name}: {field.dataType} (nullable={field.nullable})")
    else:
        print("No data found in Kafka topic for schema inference!")
        inferred_schema = None
    
    # Add a function to convert ISO timestamp to date format for partitioning
    def add_partition_field(df):
        if "created_at" in [f.name for f in df.schema.fields]:
            return df.withColumn("partition_date", date_format(to_date(col("created_at")), "yyyy-MM-dd"))
        else:
            print("WARNING: created_at field not found in schema, using current date for partition")
            from pyspark.sql.functions import current_date
            return df.withColumn("partition_date", date_format(current_date(), "yyyy-MM-dd"))
    
    # Parse the JSON data with dynamic schema inference
    if inferred_schema:
        orders_df = df.select(from_json(col("value").cast("string"), inferred_schema).alias("data")).select("data.*")
    else:
        # Fallback to direct schema inference if we couldn't get a sample
        orders_df = spark.read.json(df.select(col("value").cast("string")).rdd.map(lambda x: x[0]))
    
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
        .option("hoodie.datasource.write.schema.allow.auto.evolution", "true") \
        .mode("overwrite") \
        .save(output_path)
    
    # Note: We're NOT stopping the Spark session here
    # This allows the UI to remain available during the sleep period
    # spark.stop() is moved to the end of the script
    
    # Return the Spark session for further operations
    return spark

def parse_args():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run CDC data ingestion to Hudi job")
    parser.add_argument("--config", default="/app/config.json", 
                        help="Path to configuration JSON")
    parser.add_argument("--ui-wait", type=int, default=60,
                        help="Time to keep Spark UI available after job completion (seconds)")
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Calculate UI wait time with priority: ENV VAR > Command Line Arg > Default
    ui_wait_time = int(os.environ.get("SPARK_UI_WAIT_SECONDS", str(args.ui_wait)))
    
    # Ensure MinIO bucket exists before starting
    print("Ensuring MinIO bucket exists...")
    create_buckets()
    
    # Print UI information
    print("\n=====================================================\n")
    print(f"Spark UI is available at: http://localhost:4040")
    print(f"UI will remain accessible for {ui_wait_time} seconds after job completion")
    print("\n=====================================================\n")
    
    # Run Spark Hudi job to consume data from Kafka and write to Hudi
    print("Starting Spark job to process data from Kafka to Hudi...")
    spark = run_spark_hudi_job(args.config)
    
    # Display data summary (schema, count, sample data)
    print("\n📊 Data Summary for Initial Orders Table")
    print("=======================================================\n")
    
    try:
        # Get the Hudi table path from config
        with open(args.config) as f:
            config = json.load(f)
        output_path = os.environ.get("HUDI_BUCKET") + config["table_name"]
        
        # Read the table we just wrote
        print(f"Reading table from {output_path}")
        result_df = spark.read.format("hudi").load(output_path)
        
        # Show schema
        print("\n=== SCHEMA ===\n")
        result_df.printSchema()
        
        # Show count
        count = result_df.count()
        print(f"\n=== ROW COUNT: {count} ===\n")
        
        # Show sample data (10 rows)
        print("\n=== SAMPLE DATA (10 ROWS) ===\n")
        result_df.show(10, truncate=False)
    except Exception as e:
        print(f"Error displaying data summary: {str(e)}")
    
    # Keep the application running for a while to allow UI inspection
    print(f"\n✅ Job completed! Keeping Spark UI available for {ui_wait_time} seconds...")
    print(f"Access Spark UI at: http://localhost:4040")
    try:
        time.sleep(ui_wait_time)
    except KeyboardInterrupt:
        print("\nProcess interrupted. Exiting...")
        pass
    
    print("Done.")
    
    # Now stop the Spark session after the sleep period
    if 'spark' in locals() or 'spark' in globals():
        print("Shutting down Spark session...")
        spark.stop()
    
    print("\n✅ Data loaded successfully!")
    print("You can verify this by:")
    print("  1. Looking at MinIO UI (http://localhost:9001) for the hudi-data/orders directory")
