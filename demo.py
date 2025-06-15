#!/usr/bin/env python3
"""Demo script for Kafka to Hudi Pipeline
"""

import os
import time
import argparse
from spark.app.main import produce_mock_cdc_events, run_spark_hudi_job
from spark.app.init_minio import create_buckets
from spark.app.transform import run_transformation

def setup_demo():
    """Initialize MinIO bucket"""
    print("Setting up demo: Creating MinIO bucket...")
    create_buckets()
    print("✓ MinIO bucket created")

def generate_events(num_events=10):
    """Generate a configurable number of mock events to Kafka"""
    print(f"Generating {num_events} mock CDC events to Kafka...")
    topic = "orders_cdc"
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
    produce_mock_cdc_events(topic, kafka_bootstrap, num_events=num_events)
    print(f"✓ {num_events} events sent to {topic}")

def process_events():
    """Run the Spark Hudi job to process events from Kafka"""
    print("Processing events with Spark and storing in Hudi...")
    config_path = os.path.join(os.path.dirname(__file__), "spark/app/config.json")
    run_spark_hudi_job(config_path)
    print("✓ Data processed and stored in Hudi format")
    
def transform_data():
    """Run the transformation job to flatten JSON columns"""
    print("Transforming data from Hudi table...")
    config_path = os.path.join(os.path.dirname(__file__), "spark/app/transform_config.json")
    run_transformation(config_path)
    print("✓ Transformation complete - created transformed table")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo the Kafka to Hudi Pipeline")
    parser.add_argument("--setup", action="store_true", help="Setup demo (create buckets)")
    parser.add_argument("--events", type=int, default=10, help="Number of events to generate")
    parser.add_argument("--process", action="store_true", help="Process events with Spark/Hudi")
    parser.add_argument("--transform", action="store_true", help="Transform data (flatten JSON columns)")
    parser.add_argument("--all", action="store_true", help="Run complete demo")
    
    args = parser.parse_args()
    
    if args.setup or args.all:
        setup_demo()
        
    if args.events > 0 or args.all:
        generate_events(args.events)
        
    if args.process or args.all:
        process_events()
        
    if args.transform or args.all:
        transform_data()
        
    if not any([args.setup, args.events > 0, args.process, args.transform, args.all]):
        print("Demo script for Kafka to Hudi Pipeline")
        print("Usage examples:")
        print("  python demo.py --setup        # Create MinIO buckets")
        print("  python demo.py --events 50    # Generate 50 events to Kafka")
        print("  python demo.py --process      # Run Spark job to process events")
        print("  python demo.py --transform    # Transform data (flatten JSON)")
        print("  python demo.py --all          # Run complete pipeline")
