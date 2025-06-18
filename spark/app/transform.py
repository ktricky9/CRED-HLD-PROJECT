#!/usr/bin/env python3
"""
Transformation job to read from Hudi tables and apply transformations
"""
import os
import json
import time
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    """Create SparkSession with Hudi configurations"""
    # For Bitnami Spark, configure UI to be accessible from outside the container
    container_ip = os.environ.get("SPARK_LOCAL_IP", "0.0.0.0")
    
    # Create the unique Spark config for Bitnami container
    print("\n=== CONFIGURING SPARK UI FOR BITNAMI SPARK CONTAINER ===")
    print(f"Container IP/Bind Address: {container_ip}")
    print(f"Enabling UI on port 4040")
    
    # Simplified configuration specifically for Bitnami Spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("HudiTransformationJob") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1") \
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
    
    return spark

def load_config(config_path):
    """Load transformation configuration from JSON file"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def read_hudi_table(spark, config):
    """Read data from Hudi table with schema inference"""
    input_path = f"{config['input_bucket']}/{config['source_table']}"
    print(f"Reading from Hudi table: {input_path}")
    
    # Use Hudi format to read the table with schema inference
    # Schema inference is automatic when reading Hudi tables
    df = spark.read.format("hudi").load(input_path)
    
    print(f"Inferred schema of source table:")
    df.printSchema()
    
    # Print a sample row to show the actual data structure
    print("\nSample data from source table:")
    df.show(1, truncate=False)
    
    return df

def apply_transformations(df, config):
    """Apply transformations based on configuration"""
    transform_column = config["transformation_column"]
    transform_type = config["transformation_type"]
    
    print(f"Applying transformation '{transform_type}' to column '{transform_column}'")
    
    if transform_type == "json_flatten":
        # Check the schema to determine if the field is already a struct or a JSON string
        field_type = [field.dataType for field in df.schema.fields if field.name == transform_column][0]
        print(f"\nDetected field type for '{transform_column}': {field_type}")
        
        is_already_parsed = False
        # Check if it's already a struct (parsed) or a string (needs parsing)
        if str(field_type).startswith('StructType'):
            print(f"Field '{transform_column}' is already parsed as a struct, accessing directly")
            is_already_parsed = True
        else:
            print(f"Field '{transform_column}' is a string, parsing as JSON")
            # Define the schema for the nested JSON
            json_schema = StructType([
                StructField("code", StringType()),
                StructField("message", StringType()),
                StructField("updated_at", StringType()),
                StructField("severity", StringType())
            ])
            
            # Parse the JSON string into a struct
            df = df.withColumn(f"{transform_column}_parsed", F.from_json(
                F.col(transform_column), 
                json_schema
            ))
            
        # Flatten the nested structure - extract each field as a separate column
        if is_already_parsed:
            # Direct access for already parsed structs
            df = df.withColumn(f"{transform_column}_code", F.col(f"{transform_column}.code"))
            df = df.withColumn(f"{transform_column}_message", F.col(f"{transform_column}.message"))
            df = df.withColumn(f"{transform_column}_updated_at", F.col(f"{transform_column}.updated_at"))
            df = df.withColumn(f"{transform_column}_severity", F.col(f"{transform_column}.severity"))
        else:
            # Access through the parsed intermediate field
            df = df.withColumn(f"{transform_column}_code", F.col(f"{transform_column}_parsed.code"))
            df = df.withColumn(f"{transform_column}_message", F.col(f"{transform_column}_parsed.message"))
            df = df.withColumn(f"{transform_column}_updated_at", F.col(f"{transform_column}_parsed.updated_at"))
            df = df.withColumn(f"{transform_column}_severity", F.col(f"{transform_column}_parsed.severity"))
            # Drop the intermediate parsing column
            df = df.drop(f"{transform_column}_parsed")
        
        # Print sample data to show the transformation
        print("\nSample row after transformation:")
        df.select("order_id", transform_column, 
                 f"{transform_column}_code", 
                 f"{transform_column}_message",
                 f"{transform_column}_severity").show(1, truncate=False)
    
    elif transform_type == "array_to_string":
        # This would handle array-to-string transformations
        pass
    
    # We could add more transformation types here
    
    return df

def write_transformed_data(df, config):
    """Write transformed data to a new Hudi table using the same partition field
    as the source data (partition_date)"""
    output_path = f"{config['output_bucket']}/{config['target_table']}"
    print(f"Writing transformed data to: {output_path}")
    
    # Ensure partition_date is present and properly formatted
    print("\nVerifying partition column: partition_date")
    
    # Check if partition_date exists in the dataframe
    columns = df.columns
    if "partition_date" not in columns:
        raise ValueError("partition_date column is missing from the source data")
    
    print(f"Using 'partition_date' as partition field for transformed data")
    
    # Write to Hudi with the same partition field as source data
    df.write.format("hudi") \
        .option("hoodie.table.name", config["target_table"]) \
        .option("hoodie.datasource.write.recordkey.field", "order_id") \
        .option("hoodie.datasource.write.precombine.field", "__lsn") \
        .option("hoodie.datasource.write.partitionpath.field", "partition_date") \
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ") \
        .option("hoodie.datasource.hive_sync.enable", "false") \
        .option("hoodie.datasource.write.hive_style_partitioning", "true") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"Successfully wrote transformed data to {output_path}")
    return output_path

def run_transformation(config_path):
    """Run the transformation job"""
    config = load_config(config_path)
    spark = create_spark_session()
    
    try:
        # Read source data
        source_df = read_hudi_table(spark, config)
        
        # Apply transformations
        transformed_df = apply_transformations(source_df, config)
        
        # Show sample of transformed data
        print("Sample of transformed data:")
        transformed_df.show(5, truncate=False)
        print("Schema of transformed table:")
        transformed_df.printSchema()
        
        # Write transformed data
        output_path = write_transformed_data(transformed_df, config)
        
        # Verify results
        print("\n📊 Data Summary for Transformed Table")
        print("=======================================================\n")
        print(f"Reading transformed table from {output_path}")
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
        
        # Keep the UI available for inspection (sleep timer within the function)
        ui_wait_time = int(os.environ.get("SPARK_UI_WAIT_SECONDS", "60"))
        print(f"\n✅ Transformation job completed! Keeping Spark UI available for {ui_wait_time} seconds...")
        print(f"Access the Spark UI at: http://localhost:4040")
        try:
            time.sleep(ui_wait_time)
        except KeyboardInterrupt:
            print("\nProcess interrupted. Exiting...")
            pass
        
        print("UI wait period completed.")
        
    finally:
        # Return the spark session - don't stop it here
        # This allows the main program to potentially use it further
        pass
    
    # Return the spark session for potential further use
    return spark

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run Hudi transformation job")
    parser.add_argument("--config", default="/app/transform_config.json", 
                        help="Path to transformation config JSON")
    parser.add_argument("--ui-wait", type=int, default=60,
                        help="Time to keep Spark UI available after job completion (seconds)")
    args = parser.parse_args()
    
    # Print UI information
    print("\n=====================================================\n")
    print(f"Spark UI is available at: http://localhost:4040")
    print("\n=====================================================\n")
    
    # Run the transformation job - the UI wait time is handled inside the function
    spark = run_transformation(args.config)
    
    # Properly stop the spark session after it's no longer needed
    if spark:
        print("Shutting down Spark session...")
        spark.stop()
    
    print("\n✅ Transformation complete!")
    print("You can verify this in MinIO UI (http://localhost:9001)")
    print("Look for the hudi-data/orders_transformed directory")
