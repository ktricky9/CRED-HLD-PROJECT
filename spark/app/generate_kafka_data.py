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

def generate_order_event(order_id=None):
    """Generate a mock CDC event for an order"""
    if not order_id:
        order_id = random.randint(1, 1000)
        
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
    
    # LSN (Log Sequence Number) for CDC ordering
    lsn = random.randint(10000, 30000)
    
    return {
        "order_id": str(order_id),
        "customer_id": str(customer_id),
        "amount": amount,
        "status": status,
        "created_at": created_at.isoformat(),
        "__lsn": lsn
    }

def produce_mock_cdc_events(topic, bootstrap_servers, num_events=100):
    """Produce mock CDC events to a Kafka topic"""
    print(f"Producing {num_events} mock CDC events to topic: {topic}")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for i in range(num_events):
        event = generate_order_event()
        producer.send(topic, event)
        
        # Log some of the events for visibility
        if i % 20 == 0:
            print(f"Produced event {i+1}/{num_events}: {event}")
    
    producer.flush()
    producer.close()
    print(f"Successfully produced {num_events} events to {topic}")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Generate mock CDC events and publish to Kafka")
    parser.add_argument("--topic", default="orders_cdc", 
                        help="Kafka topic to publish events")
    parser.add_argument("--bootstrap", default="kafka:29092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--events", type=int, default=100,
                        help="Number of events to generate")
    parser.add_argument("--create-buckets", action="store_true",
                        help="Create MinIO buckets if they don't exist")
    
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
        
        # Get bootstrap servers from env var or argument
        kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", args.bootstrap)
        print(f"Using Kafka bootstrap servers: {kafka_bootstrap}")
        print(f"Target Kafka topic: {args.topic}")
        
        # Generate and publish events
        produce_mock_cdc_events(args.topic, kafka_bootstrap, num_events=args.events)
        
        print("\n✅ Data generation complete!")
        print(f"Generated {args.events} events in Kafka topic: {args.topic}")
        print("You can verify by checking Kafka UI (http://localhost:8081)")
        
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {str(e)}")
        print("Make sure Kafka is running and accessible")
        print("Try installing required packages: pip install kafka-python boto3")
        import traceback
        traceback.print_exc()
        exit(1)
