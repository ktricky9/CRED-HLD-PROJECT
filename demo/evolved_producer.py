#!/usr/bin/env python3
"""
Evolved schema producer for Kafka - adds new fields to demonstrate Hudi schema evolution
"""
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

def generate_evolved_mock_data(output_file, num_events=25):
    """Generate mock data with evolved schema (new fields) and save to JSONL file"""
    # Status code options
    order_statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
    
    # Reasons/messages for each status
    status_messages = {
        "created": ["Order placed successfully", "New order received", "Order initiated"],
        "processing": ["Payment confirmed", "Items being packed", "Processing in warehouse"],
        "shipped": ["Package en route", "Shipped via express", "Carrier picked up"],
        "delivered": ["Package delivered", "Received by customer", "Delivery confirmed"],
        "cancelled": ["Customer requested cancellation", "Payment failed", "Items unavailable"]
    }
    
    # Payment methods - NEW FIELD
    payment_methods = ["credit_card", "debit_card", "wallet", "bank_transfer", "cash_on_delivery"]
    
    # Shipping providers - NEW FIELD
    shipping_providers = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"]
    
    print(f"\n===== GENERATING MOCK EVOLVED SCHEMA DATA =====")
    print(f"Generating {num_events} events with evolved schema")
    print(f"Output file: {output_file}")
    
    events = []
    start_id = 1000  # Start with high IDs to distinguish from original data
    
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
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
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
        
        return events
    except Exception as e:
        print(f"Error saving mock data: {e}")
        return []

def produce_evolved_events(bootstrap_servers, topic, num_events=20, use_mock_file=True, mock_file_path=None, generate_file=False):
    """Produce events with evolved schema (new fields)
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic to send events to
        num_events: Number of events to generate if not using mock file
        use_mock_file: Whether to use mock file (if it exists)
        mock_file_path: Path to mock file
        generate_file: Whether to generate a new mock file (will overwrite existing)
    """
    # First, handle the file generation if requested
    if generate_file and mock_file_path:
        events = generate_evolved_mock_data(mock_file_path, num_events)
    else:
        events = []
    
    # Now establish Kafka connection
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Determine if we should read from the mock file
    if use_mock_file and mock_file_path and not events:  # 'events' would be populated if we just generated the file
        print(f"Reading evolved schema events from mock file: {mock_file_path}")
        try:
            events = []
            with open(mock_file_path, 'r') as f:
                for line in f:
                    events.append(json.loads(line))
            
            print(f"Loaded {len(events)} events from mock file")
        except Exception as e:
            print(f"Error reading from mock file: {e}")
            print("Will generate events dynamically...")
            events = []
    
    # If we have events (either from file or newly generated), send them to Kafka
    if events:
        print(f"Sending {len(events)} evolved schema events to Kafka topic: {topic}")
        
        # Send each event to Kafka
        for event in events:
            producer.send(topic, value=event)
            print(f"Sent order {event['order_id']} with payment: {event.get('payment_method', 'N/A')}, items: {event.get('items_count', 'N/A')}")
            time.sleep(0.5)  # Slow down for demo visibility
            
        producer.flush()
        producer.close()
        print(f"✅ Successfully sent {len(events)} evolved schema events to Kafka")
        return
    
    # If we get here, we need to generate events on-the-fly
    print("Generating evolved schema events dynamically and sending to Kafka...")
    
    # No events loaded from file, generate dynamically
    
    # Status code options
    order_statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
    
    # Reasons/messages for each status
    status_messages = {
        "created": ["Order placed successfully", "New order received", "Order initiated"],
        "processing": ["Payment confirmed", "Items being packed", "Processing in warehouse"],
        "shipped": ["Package en route", "Shipped via express", "Carrier picked up"],
        "delivered": ["Package delivered", "Received by customer", "Delivery confirmed"],
        "cancelled": ["Customer requested cancellation", "Payment failed", "Items unavailable"]
    }
    
    # Payment methods - NEW FIELD
    payment_methods = ["credit_card", "debit_card", "wallet", "bank_transfer", "cash_on_delivery"]
    
    # Shipping providers - NEW FIELD
    shipping_providers = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"]
    
    print(f"Generating {num_events} events with evolved schema (new fields: payment_method, shipping_provider, items_count)")
    
    start_id = 1000  # Start with high IDs to distinguish from original data
    
    for i in range(num_events):
        order_id = start_id + i
        status_code = random.choice(order_statuses)
        
        # Create nested JSON for status
        status_json = {
            "code": status_code,
            "message": random.choice(status_messages[status_code]),
            "updated_at": datetime.now().isoformat(),
            "severity": random.choice(["low", "medium", "high"])
        }
        
        # Keep status as a nested JSON object (struct in Spark)
        # DO NOT convert to string - this breaks schema compatibility
        
        # Basic fields (same as original)
        data = {
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
        
        producer.send(topic, value=data)
        print(f"Sent order {order_id} with payment: {data['payment_method']}, items: {data['items_count']}")
        time.sleep(0.5)  # Slow down for demo visibility
    
    producer.flush()
    producer.close()
    print("✅ Schema evolution complete - new fields added to the data!")

if __name__ == "__main__":
    # Use internal Kafka address when running inside container
    import os
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Send evolved schema events to Kafka")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                      help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="orders_cdc",
                      help="Kafka topic to send events to")
    parser.add_argument("--events", type=int, default=25,
                      help="Number of events to generate if not using mock file")
    parser.add_argument("--no-mock", action="store_true",
                      help="Don't use mock file, generate events from scratch")
    parser.add_argument("--mock-file", default="../data/mock_evolved_schema.jsonl",
                      help="Path to mock evolved schema data file")
    parser.add_argument("--generate-file", action="store_true",
                      help="Generate a new mock file (overwrites existing)")
    parser.add_argument("--file-only", action="store_true",
                      help="Only generate mock file, don't send to Kafka")
    
    args = parser.parse_args()
    
    # Check for mock file in various paths (for container compatibility)
    mock_file_path = args.mock_file
    if not os.path.exists(os.path.dirname(mock_file_path)):
        # Try alternative paths
        for path in ["/app/data/mock_evolved_schema.jsonl", "/data/mock_evolved_schema.jsonl"]:
            if os.path.exists(os.path.dirname(path)):
                mock_file_path = path
                break
    
    # If file-only mode, just generate the file and exit
    if args.file_only:
        if not args.generate_file:
            args.generate_file = True  # Ensure we generate the file
            
        generate_evolved_mock_data(mock_file_path, args.events)
        print(f"Mock file generation complete. File saved at: {mock_file_path}")
        print("Exiting without sending to Kafka.")
        import sys
        sys.exit(0)
    
    print(f"Connecting to Kafka at {args.bootstrap_servers}")
    produce_evolved_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        num_events=args.events,
        use_mock_file=not args.no_mock,
        mock_file_path=mock_file_path,
        generate_file=args.generate_file
    )
