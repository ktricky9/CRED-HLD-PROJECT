#!/usr/bin/env python3
"""
Transformation job to read from Hudi tables and apply transformations
"""
import os
import json
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    """Create SparkSession with Hudi configurations"""
    return SparkSession.builder \
        .appName("HudiTransformationJob") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1") \
        .getOrCreate()

def load_config(config_path):
    """Load transformation configuration from JSON file"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def read_hudi_table(spark, config):
    """Read data from Hudi table"""
    input_path = f"{config['input_bucket']}/{config['source_table']}"
    print(f"Reading from Hudi table: {input_path}")
    
    # Use Hudi format to read the table
    df = spark.read.format("hudi").load(input_path)
    print(f"Schema of source table:")
    df.printSchema()
    return df

def apply_transformations(df, config):
    """Apply transformations based on configuration"""
    transform_column = config["transformation_column"]
    transform_type = config["transformation_type"]
    
    print(f"Applying transformation '{transform_type}' to column '{transform_column}'")
    
    if transform_type == "json_flatten":
        # The status column now contains JSON strings like:
        # {"code":"created","message":"Order placed successfully","updated_at":"2025-06-15T11:42:06.123456","severity":"low"}
        
        # Define the schema for the nested JSON
        json_schema = StructType([
            StructField("code", StringType()),
            StructField("message", StringType()),
            StructField("updated_at", StringType()),
            StructField("severity", StringType())
        ])
        
        # Extract fields from the JSON string
        df = df.withColumn(f"{transform_column}_parsed", F.from_json(
            F.col(transform_column), 
            json_schema
        ))
        
        # Flatten the nested structure - extract each field as a separate column
        df = df.withColumn(f"{transform_column}_code", F.col(f"{transform_column}_parsed.code"))
        df = df.withColumn(f"{transform_column}_message", F.col(f"{transform_column}_parsed.message"))
        df = df.withColumn(f"{transform_column}_updated_at", F.col(f"{transform_column}_parsed.updated_at"))
        df = df.withColumn(f"{transform_column}_severity", F.col(f"{transform_column}_parsed.severity"))
        
        # Drop the intermediate parsing column but keep the original JSON string
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
    """Write transformed data to a new Hudi table"""
    output_path = f"{config['output_bucket']}/{config['target_table']}"
    print(f"Writing transformed data to: {output_path}")
    
    # Write to Hudi
    df.write.format("hudi") \
        .option("hoodie.table.name", config["target_table"]) \
        .option("hoodie.datasource.write.recordkey.field", "order_id") \
        .option("hoodie.datasource.write.precombine.field", "__lsn") \
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
        print("Transformation complete. Verifying results...")
        result_df = spark.read.format("hudi").load(output_path)
        print("Result row count:", result_df.count())
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Hudi transformation job")
    parser.add_argument("--config", default="/app/transform_config.json", 
                        help="Path to transformation config JSON")
    args = parser.parse_args()
    
    run_transformation(args.config)
