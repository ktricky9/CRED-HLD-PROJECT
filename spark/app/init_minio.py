#!/usr/bin/env python3

import os
import time
from minio import Minio
from minio.error import S3Error

def create_buckets():
    """Create required MinIO buckets if they don't exist."""
    # Get MinIO connection details from environment variables
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_endpoint = minio_endpoint.replace("http://", "").replace("https://", "")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    # Remove protocol part from endpoint if present
    if "://" in minio_endpoint:
        minio_endpoint = minio_endpoint.split("://")[1]
    
    print(f"Connecting to MinIO at {minio_endpoint}")
    
    # Initialize MinIO client
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to True if using HTTPS
    )
    
    # Define required bucket
    bucket_name = "hudi-data"
    
    # Wait for MinIO to be ready
    for _ in range(5):
        try:
            # Create bucket if it doesn't exist
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created successfully")
            else:
                print(f"Bucket '{bucket_name}' already exists")
            return True
        except S3Error as e:
            print(f"Error: {e}. Retrying in 2 seconds...")
            time.sleep(2)
    
    print("Failed to connect to MinIO after several retries")
    return False

if __name__ == "__main__":
    create_buckets()
