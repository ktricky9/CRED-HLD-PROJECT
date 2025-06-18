#!/usr/bin/env python3
"""
Breaking Schema Producer - Demonstrates schema changes that cause compatibility issues with Hudi
This script generates events with schema changes that are known to cause problems in Hudi
"""
import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer

def produce_breaking_events(bootstrap_servers, topic, num_events=10):
    """Produce events with schema breaking changes that Hudi doesn't handle well"""
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Track which breaking scenario each event belongs to for output clarity
    breaking_types = [
        "TYPE_CHANGE",       # Change a field's data type (e.g., string -> integer)
        "FIELD_REMOVAL",     # Remove a previously existing field
        "NESTED_CHANGE",     # Change nested structure (status field)
        "RENAME",            # Rename a field (Hudi sees as delete + add)
    ]
    
    print(f"Generating {num_events} events with breaking schema changes")
    print("The following breaking changes will be demonstrated:")
    print("  1. TYPE_CHANGE: Changing a field's data type (e.g., string to integer)")
    print("  2. FIELD_REMOVAL: Removing a required field")
    print("  3. NESTED_CHANGE: Changing nested structure")
    print("  4. RENAME: Renaming fields (Hudi sees this as delete + add)")
    
    # Start with high IDs to distinguish from other data
    start_id = 2000
    
    for i in range(num_events):
        order_id = start_id + i
        
        # Choose which breaking change to demonstrate
        breaking_type = breaking_types[i % len(breaking_types)]
        
        # Base data - will be modified based on breaking type
        base_data = {
            "order_id": str(order_id),
            "customer_id": str(random.randint(1, 100)),
            "amount": random.randint(100, 10000),
            "created_at": datetime.now().isoformat(),
            "__lsn": 50000 + i  # Higher LSN than original or evolved data
        }
        
        # Apply breaking changes based on scenario type
        if breaking_type == "TYPE_CHANGE":
            # Change data type of customer_id from string to integer
            base_data["customer_id"] = random.randint(1, 100)  # Integer instead of string
            print(f"TYPE_CHANGE: customer_id changed from string to integer: {base_data['customer_id']}")
            
        elif breaking_type == "FIELD_REMOVAL":
            # Remove an existing field that might be expected
            # For demo, delete 'amount' field which is used in transformations
            del base_data["amount"]
            print(f"FIELD_REMOVAL: Removed 'amount' field which existed in original schema")
            
        elif breaking_type == "NESTED_CHANGE":
            # Change status from object to string or vice versa
            if i % 2 == 0:
                # Change from expected struct to string
                base_data["status"] = "processing"  # String instead of object
                print(f"NESTED_CHANGE: Changed 'status' from struct to string: {base_data['status']}")
            else:
                # Keep as struct but change internal structure
                base_data["status"] = {
                    "state": "processing",  # 'state' instead of 'code'
                    "notes": "Being processed"  # 'notes' instead of 'message'
                    # Missing 'severity' and 'updated_at'
                }
                print(f"NESTED_CHANGE: Changed internal structure of 'status' field")
                
        elif breaking_type == "RENAME":
            # Rename fields (delete existing + add new)
            if "amount" in base_data:
                base_data["total_amount"] = base_data["amount"]  # Renamed field
                del base_data["amount"]
                print(f"RENAME: Renamed 'amount' to 'total_amount': {base_data['total_amount']}")
        
        # Add a marker field to easily identify these as breaking schema events
        base_data["breaking_schema"] = breaking_type
        
        # Send to Kafka
        producer.send(topic, value=base_data)
        print(f"Sent breaking event {i+1}/{num_events}: order_id={order_id}, breaking_type={breaking_type}")
        time.sleep(0.5)  # Slow down for demo visibility
    
    producer.flush()
    producer.close()
    print("✅ Breaking schema events generation complete!")

if __name__ == "__main__":
    # Get bootstrap servers from env var or default to container service name
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.environ.get("KAFKA_TOPIC", "orders_cdc")
    
    print(f"Connecting to Kafka at {bootstrap_servers}")
    print(f"Using topic: {topic}")
    produce_breaking_events(bootstrap_servers, topic)
