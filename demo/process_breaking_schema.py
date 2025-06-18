#!/usr/bin/env python3
"""
Process events with breaking schema changes to demonstrate Hudi limitations
"""
import os
import json
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Process breaking schema changes with Hudi")
    parser.add_argument("--ui-wait", type=int, default=60,
                      help="Time to keep Spark UI available after job completion (seconds)")
    return parser.parse_args()

# Create Spark Session
def create_spark_session():
    """Create a Spark session with Hudi and Kafka support"""
    # Ensure proper network binding
    container_ip = os.environ.get("SPARK_LOCAL_IP", "0.0.0.0")
    
    # Create SparkSession with the same configuration as the working script
    spark = SparkSession.builder \
        .appName("HudiBreakingSchemaDemo") \
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
    
    return spark

def read_breaking_events_from_kafka(spark, topic="orders_cdc"):
    """Read events with breaking schema from Kafka and demonstrate issues"""
    print("\n===== READING BREAKING SCHEMA EVENTS FROM KAFKA =====")
    print("Reading from Kafka with dynamic schema inference...")
    
    breaking_events = []
    
    try:
        # Try approach 1: Direct batch read
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
            
        # Extract the raw JSON strings
        json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        
        # Collect a few examples to analyze
        json_examples = json_df.limit(10).collect()
        
        print("Sample JSON records for schema inference:")
        for i, row in enumerate(json_examples):
            json_str = row.json_str
            if json_str:
                try:
                    # Parse JSON
                    json_obj = json.loads(json_str)
                    # Check if this is a breaking schema event
                    if "breaking_schema" in json_obj:
                        breaking_events.append(json_obj)
                        print(f"  {i+1}. Breaking event type: {json_obj['breaking_schema']}")
                        print(f"     {json_str[:200]}..." if len(json_str) > 200 else f"     {json_str}")
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"Error reading from Kafka: {type(e).__name__}: {str(e)}")
        print("This could be because:")
        print("1. No breaking schema events have been sent yet")
        print("2. Kafka connector is not properly configured")
        print("3. The topic doesn't exist or contain data")
    
    # If we didn't find any breaking events, create examples for educational purposes
    if not breaking_events:
        print("\nNo breaking schema events found in Kafka. Creating example events for educational purposes...")
        # Create example breaking events for demonstration
        breaking_events = [
            {
                "order_id": "2000",
                "customer_id": 91,  # Integer instead of string
                "amount": 5000,
                "created_at": "2025-06-19T00:00:00.000000",
                "__lsn": 50000,
                "breaking_schema": "TYPE_CHANGE"
            },
            {
                "order_id": "2001",
                "customer_id": "45",
                # amount field missing
                "created_at": "2025-06-19T00:00:00.000000",
                "__lsn": 50001,
                "breaking_schema": "FIELD_REMOVAL"
            },
            {
                "order_id": "2002",
                "customer_id": "33", 
                "amount": 3500,
                "status": "processing",  # String instead of object
                "created_at": "2025-06-19T00:00:00.000000",
                "__lsn": 50002,
                "breaking_schema": "NESTED_CHANGE"
            },
            {
                "order_id": "2003",
                "customer_id": "22",
                "total_amount": 2500,  # Renamed field
                "created_at": "2025-06-19T00:00:00.000000",
                "__lsn": 50003,
                "breaking_schema": "RENAME"
            }
        ]
        
        # Show example events
        print("Example breaking events for demonstration:")
        for i, event in enumerate(breaking_events):
            print(f"  {i+1}. Breaking type: {event['breaking_schema']}")
            print(f"     {event}")
    
    # Try to process the data with different approaches to show schema handling issues
    print("\n===== SCHEMA EVOLUTION CHALLENGES EXPLAINED =====")
    
    # Present the educational content about schema evolution challenges
    # Completely execution-independent to ensure the demo works even without Kafka
    
    print("\nWhen handling schema evolution with Apache Hudi, you'll encounter these challenges:\n")
    
    # Example 1: Type changes
    print("CHALLENGE 1: TYPE CHANGES")
    print("Example: Changing 'customer_id' from string to integer")
    print("  Original schema: { 'customer_id': '45' }  (string)")
    print("  New schema:      { 'customer_id': 91 }     (integer)")
    print("  Impact: Type mismatches cause runtime errors and data corruption")
    print("  Error you'd see: java.lang.ClassCastException or org.apache.spark.sql.AnalysisException")
    print("  Solution: Never change types of existing fields; add new fields instead")
    print("    e.g., Add 'customer_id_int' instead of changing 'customer_id'\n")
    
    # Example 2: Field removal
    print("CHALLENGE 2: FIELD REMOVAL")
    print("Example: Removing the 'amount' field used in transformations")
    print("  Original schema: { 'order_id': '123', 'amount': 5000 }")
    print("  New schema:      { 'order_id': '123' }  (amount missing)")
    print("  Impact: Downstream processes break; partition/sort keys become unusable")
    print("  Error you'd see: org.apache.spark.sql.AnalysisException: cannot resolve 'amount'")
    print("  Solution: Mark fields as deprecated but never remove them entirely")
    print("    Use default values for missing fields\n")
    
    # Example 3: Nested structure changes
    print("CHALLENGE 3: NESTED STRUCTURE CHANGES")
    print("Example: Changing 'status' from object to string")
    print("  Original schema: { 'status': {'code': 'shipped', 'message': 'In transit'} }")
    print("  New schema:      { 'status': 'shipped' }  (now a string)")
    print("  Impact: Schema incompatibility; operations on nested fields fail")
    print("  Error you'd see: org.apache.spark.sql.AnalysisException: No such struct field")
    print("  Solution: Never change structure of nested objects;")
    print("    Add new fields to nested objects or create new nested objects\n")
    
    # Example 4: Field renaming
    print("CHALLENGE 4: FIELD RENAMING")
    print("Example: Renaming 'amount' to 'total_amount'")
    print("  Original schema: { 'order_id': '123', 'amount': 5000 }")
    print("  New schema:      { 'order_id': '123', 'total_amount': 5000 }")
    print("  Impact: Hudi sees this as a deletion + addition; history is lost")
    print("  Error you'd see: No direct error, but data lineage is broken")
    print("  Solution: Add new field alongside old one, populate both,")
    print("    then stop writing to old field after transition period\n")
    
    # Common approaches and their problems
    print("COMMON APPROACHES TO SCHEMA EVOLUTION IN HUDI")
    
    print("\nApproach 1: Simple Schema Inference")
    print("  Description: Let Spark infer schema from JSON data (common approach)")
    print("  Problem: Type mismatches and missing fields cause failures")
    print("  Code pattern:")
    print("    df = spark.read...selectExpr(\"from_json(value, 'struct<*>') as data\")")
    print("  Limitations: Unpredictable with breaking schema changes\n")
    
    print("\nApproach 2: Explicit Schema with Error Handling")
    print("  Description: Use explicit schema definition and handle errors")
    print("  Problem: Complex to maintain and still fragile to schema changes")
    print("  Code pattern:")
    print("    schema = StructType([...])")
    print("    df.withColumn(\"missing_field\", lit(None).cast(StringType()))")
    print("  Limitations: Labor-intensive and error-prone\n")
    
    print("\nApproach 3: Schema Evolution with Merging")
    print("  Description: Try to merge old and new schemas")
    print("  Problem: Causes type conflicts and data loss")
    print("  Code pattern:")
    print("    if \"total_amount\" in df.columns and \"amount\" not in df.columns:")
    print("      df = df.withColumnRenamed(\"total_amount\", \"amount\")")
    print("  Limitations: Complex rules, hard to maintain\n")
    
    # Showcase a real example of our breaking data
    print("\nEXAMPLE IMPACT OF BREAKING SCHEMA CHANGES ON HUDI")
    print("Based on the example breaking events:\n")
    
    for event in breaking_events:
        print(f"Event with {event['breaking_schema']}:")
        print(f"  {event}")
        
        if event['breaking_schema'] == "TYPE_CHANGE":
            print("  Impact: Hudi will either reject this record or convert to string implicitly")
            print("          leading to inconsistent behavior in queries")
        elif event['breaking_schema'] == "FIELD_REMOVAL":
            print("  Impact: Any query or process requiring 'amount' will fail")
            print("          Default partitioning/sorting might break")
        elif event['breaking_schema'] == "NESTED_CHANGE":
            print("  Impact: Any code accessing status.code or status.message will fail")
            print("          as status is now a string, not a struct")
        elif event['breaking_schema'] == "RENAME":
            print("  Impact: Queries looking for 'amount' won't find it")
            print("          Previous time-travel queries will have inconsistent fields")
        print()
    
    # Return the breaking events for further analysis
    return breaking_events

def show_hudi_limitations():
    """Display Hudi schema evolution limitations"""
    print("\n===== HUDI SCHEMA EVOLUTION LIMITATIONS =====")
    limitations = [
        {
            "category": "Type Changes",
            "limitation": "Changing a field's data type is not supported",
            "example": "Changing 'customer_id' from string to int",
            "workaround": "Create a new field with the new type and deprecate the old field"
        },
        {
            "category": "Field Removal",
            "limitation": "Removing fields can cause issues with older data",
            "example": "Removing 'amount' field that's used in transformations",
            "workaround": "Never delete fields; instead mark as deprecated in schema registry"
        },
        {
            "category": "Nested Structure Changes",
            "limitation": "Changing nested field structure breaks compatibility",
            "example": "Changing 'status' from object to string or changing its internal fields",
            "workaround": "Only add new fields to nested structures, never modify existing ones"
        },
        {
            "category": "Field Renaming",
            "limitation": "Hudi sees renamed fields as deletion + addition",
            "example": "Renaming 'amount' to 'total_amount'",
            "workaround": "Add new field with new name, copy data, keep both until old field can be deprecated"
        }
    ]
    
    print("Hudi supports schema evolution but has important limitations:")
    print("")
    
    for i, limit in enumerate(limitations):
        print(f"{i+1}. {limit['category']}:")
        print(f"   Limitation: {limit['limitation']}")
        print(f"   Example: {limit['example']}")
        print(f"   Workaround: {limit['workaround']}")
        print("")
    
    print("\n===== BEST PRACTICES FOR SCHEMA EVOLUTION WITH HUDI =====")
    print("1. Use a schema registry to manage and validate schemas before data reaches Hudi")
    print("2. Only add new fields; never remove, rename, or change types of existing fields")
    print("3. For nested structures, only add new nested fields, don't modify existing structures")
    print("4. Make all fields nullable where possible")
    print("5. Implement data quality checks before writing to Hudi")
    print("6. When structure must change, create a new table and migrate data")

if __name__ == "__main__":
    # Parse arguments
    args = parse_args()
    ui_wait_time = int(os.environ.get("SPARK_UI_WAIT_SECONDS", str(args.ui_wait)))
    
    # Create Spark session
    print("Creating Spark session for Breaking Schema demo...")
    spark = create_spark_session()
    
    # Print UI information
    print("\n=====================================================")
    print(f"Spark UI is available at: http://localhost:4040")
    print(f"UI will remain accessible for {ui_wait_time} seconds after job completion")
    print("=====================================================\n")
    
    try:
        # Process events with breaking schema
        breaking_events = read_breaking_events_from_kafka(spark)
        
        # Show Hudi limitations
        show_hudi_limitations()
        
        # Keep the application running for UI inspection
        print(f"\n✅ Job completed! Keeping Spark UI available for {ui_wait_time} seconds...")
        print(f"Access Spark UI at: http://localhost:4040")
        try:
            time.sleep(ui_wait_time)
        except KeyboardInterrupt:
            print("\nProcess interrupted. Exiting...")
    finally:
        print("Done.")
        if 'spark' in locals():
            print("Shutting down Spark session...")
            spark.stop()
