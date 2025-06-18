#!/usr/bin/env python3
"""
Evolved schema producer for Kafka - adds new fields to demonstrate Hudi schema evolution
"""
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

def produce_evolved_events(bootstrap_servers, topic, num_events=20):
    """Produce events with evolved schema (new fields)"""
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
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
        
        # Convert to string (this is what would typically happen when storing JSON in a string column)
        status_str = json.dumps(status_json)
        
        # Basic fields (same as original)
        data = {
            "order_id": str(order_id),
            "customer_id": str(random.randint(1, 100)),
            "amount": random.randint(100, 10000),
            "status": status_str,
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
    # Get bootstrap servers from env var or default to container service name
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = "orders_cdc"
    print(f"Connecting to Kafka at {bootstrap_servers}")
    produce_evolved_events(bootstrap_servers, topic)
