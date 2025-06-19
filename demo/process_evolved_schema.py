#!/usr/bin/env python3
"""
Process the evolved schema data using Hudi's schema evolution capability
"""
import os
import json
import time
import random
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Process schema evolution with Hudi")
    parser.add_argument("--ui-wait", type=int, default=60,
                      help="Time to keep Spark UI available after job completion (seconds)")
    parser.add_argument("--generate-mock", action="store_true",
                      help="Generate and save mock evolved schema data to JSONL file")
    parser.add_argument("--events", type=int, default=20,
                      help="Number of mock events to generate (if --generate-mock is used)")
    parser.add_argument("--use-mock-file", action="store_true",
                      help="Read from mock evolved schema JSONL file instead of Kafka")
    parser.add_argument("--mock-file", default="../data/mock_evolved_schema.jsonl",
                      help="Path to mock evolved schema JSONL file")
    return parser.parse_args()

# Generate evolved schema mock data
def generate_evolved_schema_mock_data(num_events=20, output_file="/app/data/mock_evolved_schema.jsonl"):
    """Generate mock data with evolved schema and save to JSONL file"""
    # Use the same structure as evolved_producer.py but save to file instead
    
    # Adjust path for local environment if needed
    if not os.path.exists(os.path.dirname(output_file)):
        # Try to find data directory in parent directories
        if os.path.exists("/data"):
            output_file = "/data/mock_evolved_schema.jsonl"
        elif os.path.exists("../data"):
            output_file = "../data/mock_evolved_schema.jsonl"
        else:
            output_file = "mock_evolved_schema.jsonl"
    
    print(f"\n===== GENERATING MOCK EVOLVED SCHEMA DATA =====")
    print(f"Generating {num_events} events with evolved schema")
    print(f"Output file: {output_file}")
    
    # Status codes
    order_statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
    
    # Status messages for each status code
    status_messages = {
        "created": ["Order placed successfully", "New order received", "Order initiated"],
        "processing": ["Payment confirmed", "Items being packed", "Processing in warehouse"],
        "shipped": ["Package en route", "Shipped via express", "Carrier picked up"],
        "delivered": ["Package delivered", "Received by customer", "Delivery confirmed"],
        "cancelled": ["Customer requested cancellation", "Payment failed", "Items unavailable"]
    }
    
    # New field values
    payment_methods = ["credit_card", "debit_card", "wallet", "bank_transfer", "cash_on_delivery"]
    shipping_providers = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"]
    
    # Start with high IDs to distinguish from original data
    start_id = 1000
    events = []
    
    for i in range(num_events):
        order_id = start_id + i
        status_code = random.choice(order_statuses)
        
        # Create nested status JSON
        status_json = {
            "code": status_code,
            "message": random.choice(status_messages[status_code]),
            "severity": random.choice(["low", "medium", "high"]),
            "updated_at": datetime.now().isoformat()
        }
        
        # Create event with evolved schema
        event = {
            "order_id": str(order_id),
            "customer_id": str(random.randint(1, 100)),
            "amount": random.randint(100, 10000),
            "status": status_json,  # Keep as nested JSON object
            "created_at": datetime.now().isoformat(),
            "__lsn": random.randint(20000, 30000),  # Higher LSN than original
            
            # NEW FIELDS - schema evolution
            "payment_method": random.choice(payment_methods),
            "shipping_provider": random.choice(shipping_providers) if status_code != "created" else None,
            "items_count": random.randint(1, 10)
        }
        
        events.append(event)
    
    # Write to file
    try:
        with open(output_file, 'w') as f:
            for event in events:
                f.write(json.dumps(event) + '\n')
        print(f"✅ Successfully saved {num_events} evolved schema events to {output_file}")
        
        # Show sample events
        print(f"\nSample events (first 3):")
        for i, event in enumerate(events[:3]):
            print(f"Event {i+1}: {json.dumps(event)}")
        print("\n")
        
        return True
    except Exception as e:
        print(f"Error saving mock data: {e}")
        return False

# Get arguments
args = parse_args()

# Check if we should just generate mock data and exit
if args.generate_mock:
    output_file = "/Users/kartikrai/CRED-HLD-PROJECT/data/mock_evolved_schema.jsonl"
    generate_evolved_schema_mock_data(args.events, output_file)
    print("Mock data generation complete. Exiting without running Spark job.")
    import sys
    sys.exit(0)
    
# Calculate UI wait time with priority: ENV VAR > Command Line Arg > Default
ui_wait_time = int(os.environ.get("SPARK_UI_WAIT_SECONDS", str(args.ui_wait)))

# Ensure Spark UI is properly enabled and configured for network access
# For Bitnami Spark, configure UI to be accessible from outside the container
container_ip = os.environ.get("SPARK_LOCAL_IP", "0.0.0.0")

# Create the unique Spark config for Bitnami container with UI enabled
print("\n=== CONFIGURING SPARK UI FOR BITNAMI SPARK CONTAINER ===")
print(f"Container IP/Bind Address: {container_ip}")
print(f"Enabling UI on port 4040")
print(f"UI will remain accessible for {ui_wait_time} seconds after job completion")

# Create SparkSession with Hudi and UI configuration
spark = SparkSession.builder \
    .appName("HudiSchemaEvolution") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.driver.bindAddress", container_ip) \
    .config("spark.driver.host", container_ip) \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .master("local[*]") \
    .getOrCreate()

# Check if we should read from mock file or Kafka
if args.use_mock_file:
    print("\n===== READING FROM MOCK EVOLVED SCHEMA FILE =====")
    mock_file_path = args.mock_file
    # Try adjusting the path if running in container
    if not os.path.exists(mock_file_path):
        for path in ["/app/data/mock_evolved_schema.jsonl", "/data/mock_evolved_schema.jsonl", "/demo/data/mock_evolved_schema.jsonl"]:
            if os.path.exists(path):
                mock_file_path = path
                break
    
    print(f"Reading evolved schema data from file: {mock_file_path}")
    
    try:
        # Read data from JSONL file
        orders_df = spark.read.json(mock_file_path)
        
        # Show the inferred schema
        print("\nSchema from mock data file:")
        for field in orders_df.schema.fields:
            print(f"  - {field.name}: {field.dataType} (nullable={field.nullable})")
        
        print(f"\nLoaded {orders_df.count()} records from mock file with evolved schema")
        
        # Sample of the data
        print("\nSample data from mock file:")
        orders_df.show(3, truncate=False)
    except Exception as e:
        print(f"Error reading mock data file: {e}")
        print("Creating empty dataframe with expected schema")
        # Create empty dataframe with expected schema as fallback
        orders_df = spark.createDataFrame([], schema="order_id STRING, customer_id STRING, amount INT, status STRUCT<code:STRING, message:STRING, severity:STRING, updated_at:STRING>, created_at STRING, __lsn LONG, payment_method STRING, shipping_provider STRING, items_count INT")
        
else:
    print("\n===== READING LATEST RECORDS FROM KAFKA =====")
    # Read latest events from Kafka to get the evolved schema
    print("Reading from Kafka to infer schema...")
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")) \
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "orders_cdc")) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Parse JSON using schema inference
    from pyspark.sql.functions import col, from_json, to_date, date_format, schema_of_json
    
    # First, get a sample of the JSON data to infer schema
    print("Inferring schema from JSON data...")
    sample_json_df = df.select(col("value").cast("string").alias("json_str")).limit(10)
    
    # Check if we have data
    if sample_json_df.count() > 0:
        # Infer schema from the JSON data
        first_json = sample_json_df.first()[0]
        print(f"Sample JSON for schema inference: {first_json[:200]}..." if len(first_json) > 200 else first_json)
        
        # Use Spark's JSON reader to infer the schema
        inferred_schema = spark.read.json(spark.sparkContext.parallelize([first_json])).schema
        print("\nDynamically inferred schema:")
        for field in inferred_schema.fields:
            print(f"  - {field.name}: {field.dataType} (nullable={field.nullable})")
            
        # Parse all records with the inferred schema
        orders_df = df.select(from_json(col("value").cast("string"), inferred_schema).alias("data")).select("data.*")
        
    else:
        print("No data found in Kafka topic for schema inference!")
        print("Checking for mock evolved schema file as fallback...")
        fallback_file = "/app/data/mock_evolved_schema.jsonl"
        
        if os.path.exists(fallback_file):
            print(f"Using fallback mock file: {fallback_file}")
            orders_df = spark.read.json(fallback_file)
        else:
            # Fallback to direct JSON parsing if no sample available
            print("No fallback file found, attempting to parse Kafka data directly...")
            orders_df = spark.read.json(df.select(col("value").cast("string")).rdd.map(lambda x: x[0]))

# Add partition field - same as original
def add_partition_field(df):
    return df.withColumn("partition_date", date_format(to_date(col("created_at")), "yyyy-MM-dd"))

orders_df = add_partition_field(orders_df)

# Show the evolved schema
print("\n===== EVOLVED SCHEMA WITH NEW FIELDS =====")
orders_df.printSchema()

# Show sample data with new fields
print("\n===== SAMPLE RECORDS WITH FIELDS =====")

# Check which columns actually exist in the dataframe
existing_columns = orders_df.columns
print(f"Available columns: {existing_columns}")

# Define the columns we want to display, including the new schema evolution fields
requested_columns = ["order_id", "customer_id", "amount"]
evolution_columns = ["payment_method", "shipping_provider", "items_count"]

# Only include evolution columns that actually exist
valid_columns = requested_columns + [col for col in evolution_columns if col in existing_columns]

if set(evolution_columns).issubset(set(existing_columns)):
    print("Schema evolution fields found in the data!")
else:
    print("\nNOTE: Some schema evolution fields are not present in the data yet.")
    print("This is expected if you haven't run the evolved_producer.py script first.")
    print("The script will continue but will only show the available fields.")

# Display available columns
orders_df.select(*valid_columns).show(5, truncate=False)

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
print(f"Total records in table: {total_count}")

# Check if evolution fields exist in the result dataframe
result_columns = result_df.columns
has_evolution_fields = "payment_method" in result_columns

if has_evolution_fields:
    # If the evolution fields exist, count and show records with them
    new_schema_count = result_df.filter(col("payment_method").isNotNull()).count()
    print(f"Records with new fields: {new_schema_count}")
    
    # Show sample of records with new fields using SQL
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
else:
    print("\nNOTE: Schema evolution fields (payment_method, shipping_provider, items_count)")
    print("are not present in the table yet. To add these fields, run:")
    print("1. First run './demo/evolved_producer.py' to send events with new fields")
    print("2. Then run this script again to process the schema evolution")
    
    # Show sample of existing data
    print("\nSample of current data (without evolution fields):")
    spark.sql("""
      SELECT 
        order_id, 
        customer_id,
        amount,
        created_at
      FROM orders
      LIMIT 5
    """).show(truncate=False)

print("\n✅ Schema evolution successful! Hudi table now includes the new fields.")

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
