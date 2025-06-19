#!/usr/bin/env python3
"""
Generate mock data with evolved schema (new fields) and save to JSONL file
"""
import os
import json
import random
import argparse
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Generate mock data with evolved schema")
    parser.add_argument("--events", type=int, default=25,
                      help="Number of mock events to generate")
    parser.add_argument("--output", default="../data/mock_evolved_schema.jsonl",
                      help="Output file path")
    return parser.parse_args()

def generate_evolved_schema_mock_data(num_events=25, output_file="../data/mock_evolved_schema.jsonl"):
    """Generate mock data with evolved schema and save to JSONL file"""
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
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

if __name__ == "__main__":
    args = parse_args()
    generate_evolved_schema_mock_data(args.events, args.output)
