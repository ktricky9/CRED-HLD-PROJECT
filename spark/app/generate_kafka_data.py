#!/usr/bin/env python3
# generate_kafka_data.py
# Script to generate mock CDC events and publish them to a Kafka topic

import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer

def create_buckets():
    """Ensure MinIO bucket exists"""
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError
    except ImportError:
        print("WARNING: boto3 not installed. Install with 'pip install boto3'")
        print("Skipping bucket creation - assuming buckets already exist")
        return
    
    print("Connecting to MinIO at", os.environ.get("MINIO_ENDPOINT"))
    
    s3 = boto3.resource('s3',
                       endpoint_url=os.environ.get("MINIO_ENDPOINT"),
                       aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
                       aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
                       config=Config(signature_version='s3v4'),
                       region_name='us-east-1')
    
    bucket_name = "hudi-data"
    exists = True
    
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError:
        exists = False
        
    if not exists:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket '{bucket_name}' already exists")

def generate_order_event(index, start_id=1, start_lsn=10000):
    """Generate a mock CDC event for an order with sequential IDs
    
    Args:
        index: The index of the event (0-based) used to generate sequential IDs
        start_id: The starting order_id value
        start_lsn: The starting LSN value
    """
    # Generate sequential order_id based on index and start_id
    order_id = start_id + index
    
    # Customer ID can remain random
    customer_id = random.randint(1, 50)
    amount = random.randint(1000, 10000)
    
    # Random status with JSON structure
    statuses = [
        {"code": "created", "message": "Order initiated", "severity": "low"},
        {"code": "processing", "message": "Items being packed", "severity": "low"},
        {"code": "processing", "message": "Payment confirmed", "severity": "medium"},
        {"code": "shipped", "message": "Order in transit", "severity": "medium"},
        {"code": "delivered", "message": "Received by customer", "severity": "high"},
        {"code": "cancelled", "message": "Order cancelled by customer", "severity": "high"}
    ]
    
    status = random.choice(statuses)
    
    # Add timestamp to status
    status["updated_at"] = datetime.now().isoformat()
    
    # Transaction timestamp
    created_at = datetime.now() - timedelta(hours=random.randint(0, 3))
    
    # Sequential LSN based on index and start_lsn
    lsn = start_lsn + index
    
    return {
        "order_id": str(order_id),
        "customer_id": str(customer_id),
        "amount": amount,
        "status": status,
        "created_at": created_at.isoformat(),
        "__lsn": lsn
    }

def generate_mock_cdc_events(num_events=100, output_file=None, start_id=1, start_lsn=10000):
    """Generate mock CDC events and save to a file
    
    Args:
        num_events: Number of events to generate
        output_file: Path to output file (optional)
        start_id: Starting order_id value
        start_lsn: Starting LSN value
    """
    events = []
    data_file = output_file or os.path.join(os.path.dirname(__file__), "mock_cdc_events.jsonl")
    print(f"Generating {num_events} mock CDC events and saving to {data_file}")
    print(f"Using sequential IDs starting from order_id={start_id}, LSN={start_lsn}")
    
    # Generate events with sequential IDs
    for i in range(num_events):
        event = generate_order_event(i, start_id, start_lsn)
        events.append(event)
        
        # Log some of the events for visibility
        if i % 20 == 0 or i < 2 or i >= num_events - 2:
            print(f"Generated event {i+1}/{num_events}: order_id={event['order_id']}, LSN={event['__lsn']}")
    
    
    # Write events to file (overwriting previous content)
    with open(data_file, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"Successfully generated and saved {num_events} events to {data_file}")
    return data_file, events

def produce_mock_cdc_events(topic, bootstrap_servers, num_events=100, input_file=None, start_id=1, start_lsn=10000):
    """Read CDC events from a file and produce them to Kafka topic
    If the input file is not provided or doesn't exist, new events will be generated"""
    if not input_file:
        data_file, events = generate_mock_cdc_events(num_events, start_id=start_id, start_lsn=start_lsn)
    else:
        data_file = input_file
        events = []
        with open(data_file, 'r') as f:
            for line in f:
                events.append(json.loads(line.strip()))
        num_events = len(events)
    
    print(f"Producing {num_events} events from {data_file} to Kafka topic: {topic}")
    
    # Connect to Kafka and send events
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Send each event to Kafka
    for i, event in enumerate(events):
        producer.send(topic, event)
        
        # Log some of the events for visibility
        if i % 20 == 0:
            print(f"Sending event {i+1}/{num_events} to Kafka: {event}")
    
    producer.flush()
    producer.close()
    print(f"Successfully produced {num_events} events to Kafka topic: {topic}")
    print(f"Event data is persisted in: {data_file}")
    return data_file

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Generate mock CDC events and publish to Kafka")
    parser.add_argument("--topic", default="orders_cdc", 
                        help="Kafka topic to publish events")
    parser.add_argument("--bootstrap", default="kafka:29092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--events", type=int, default=100,
                        help="Number of events to generate")
    parser.add_argument("--start-id", type=int, default=1,
                        help="Starting order_id value for sequential generation")
    parser.add_argument("--start-lsn", type=int, default=10000,
                        help="Starting LSN value for sequential generation")
    parser.add_argument("--create-buckets", action="store_true",
                        help="Create MinIO buckets if they don't exist")
    parser.add_argument("--file", default=None, 
                        help="Path to save/load events data (default: app/mock_cdc_events.jsonl)")
    parser.add_argument("--generate-only", action="store_true",
                        help="Only generate events to file without sending to Kafka")
    
    return parser.parse_args()
    
if __name__ == "__main__":
    try:
        args = parse_args()
        
        # Ensure environment variables are set
        if not os.environ.get("MINIO_ENDPOINT"):
            print("Warning: MINIO_ENDPOINT environment variable not set")
            print("Setting default: http://minio:9000")
            os.environ["MINIO_ENDPOINT"] = "http://minio:9000"
            
        if not os.environ.get("MINIO_ACCESS_KEY"):
            print("Warning: MINIO_ACCESS_KEY not set, using default")
            os.environ["MINIO_ACCESS_KEY"] = "minio"
            
        if not os.environ.get("MINIO_SECRET_KEY"):
            print("Warning: MINIO_SECRET_KEY not set, using default")
            os.environ["MINIO_SECRET_KEY"] = "minio123"
        
        # Optionally create buckets
        if args.create_buckets:
            print("Ensuring MinIO bucket exists...")
            create_buckets()
        
        if args.generate_only:
            # Only generate events to file, don't send to Kafka
            data_file, _ = generate_mock_cdc_events(
                num_events=args.events, 
                output_file=args.file,
                start_id=args.start_id,
                start_lsn=args.start_lsn
            )
            print("\n✅ Data generation complete!")
            print(f"Generated {args.events} events and saved to {data_file}")
            print(f"Used sequential IDs: order_ids {args.start_id}-{args.start_id + args.events - 1}")
            print(f"Used sequential LSNs: {args.start_lsn}-{args.start_lsn + args.events - 1}")
            print("Run this script again without --generate-only to send these events to Kafka")
        else:
            # Generate events and send to Kafka
            kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", args.bootstrap)
            print(f"Using Kafka bootstrap servers: {kafka_bootstrap}")
            print(f"Target Kafka topic: {args.topic}")
            
            # Generate or read events, then publish to Kafka
            data_file = produce_mock_cdc_events(
                args.topic, 
                kafka_bootstrap, 
                num_events=args.events, 
                input_file=args.file,
                start_id=args.start_id,
                start_lsn=args.start_lsn
            )
            
            print("\n✅ Data generation and publishing complete!")
            print(f"Generated and published {args.events} events to Kafka topic: {args.topic}")
            print(f"Event data is persisted in file: {data_file}")
            print("You can verify by checking Kafka UI (http://localhost:8081)")
        
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {str(e)}")
        print("Make sure Kafka is running and accessible (if sending to Kafka)")
        print("Try installing required packages: pip install kafka-python boto3")
        import traceback
        traceback.print_exc()
        exit(1)
