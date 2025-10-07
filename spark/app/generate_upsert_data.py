#!/usr/bin/env python3
"""
Generate upsert events based on existing events for Hudi demo
This script reads events from the existing jsonl file and generates upsert events
"""
import os
import json
import random
import argparse
from datetime import datetime, timedelta
import time

def parse_args():
    parser = argparse.ArgumentParser(description="Generate upsert data for Hudi demo")
    parser.add_argument("--source", default="/app/data/mock_cdc_events.jsonl", 
                      help="Source file with original events")
    parser.add_argument("--output", default="/app/data/mock_upserts.jsonl", 
                      help="Output file for upsert events")
    parser.add_argument("--count", type=int, default=25, 
                      help="Number of events to generate upserts for")
    return parser.parse_args()

def read_source_events(source_file):
    """Read events from source file"""
    events = []
    try:
        with open(source_file, 'r') as f:
            for line in f:
                events.append(json.loads(line.strip()))
        print(f"Read {len(events)} events from {source_file}")
        return events
    except Exception as e:
        print(f"Error reading source file: {e}")
        return []

def generate_upserts(events, count):
    """Generate upsert events from source events"""
    # Ensure we don't try to generate more upserts than available events
    count = min(count, len(events))
    
    # Select random events to upsert
    selected_events = random.sample(events, count)
    upserts = []
    
    print(f"Generating {count} upsert events...")
    
    for event in selected_events:
        # Create a modified copy of the event (upsert)
        upsert = event.copy()
        
        # Increment LSN to ensure this is treated as a newer version
        upsert["__lsn"] = event["__lsn"] + random.randint(10000, 20000)
        
        # DO NOT update created_at timestamp - it should remain the same as the original record
        # Only update the status field and its updated_at timestamp
        
        # Modify status field
        if isinstance(upsert["status"], dict):
            new_statuses = [
                {"code": "shipped", "message": "Order in transit", "severity": "medium"},
                {"code": "delivered", "message": "Received by customer", "severity": "high"},
                {"code": "cancelled", "message": "Order cancelled by customer", "severity": "high"}
            ]
            upsert["status"] = random.choice(new_statuses)
            upsert["status"]["updated_at"] = datetime.now().isoformat()
        
        # Update amount (random adjustment)
        adjustment = random.choice([0.9, 1.0, 1.1])  # 10% decrease, no change, or 10% increase
        upsert["amount"] = int(event["amount"] * adjustment)
        
        upserts.append(upsert)
    
    return upserts

def write_upserts(upserts, output_file):
    """Write upsert events to output file"""
    try:
        with open(output_file, 'w') as f:
            for upsert in upserts:
                f.write(json.dumps(upsert) + '\n')
        print(f"Wrote {len(upserts)} upsert events to {output_file}")
        return True
    except Exception as e:
        print(f"Error writing upserts: {e}")
        return False

if __name__ == "__main__":
    args = parse_args()
    
    # Read source events
    events = read_source_events(args.source)
    if not events:
        print("No source events found. Please run data generation first.")
        exit(1)
    
    # Generate upserts
    upserts = generate_upserts(events, args.count)
    
    # Write upserts to file
    if write_upserts(upserts, args.output):
        print("\n✅ Upsert data generation complete!")
        print(f"Generated {len(upserts)} upsert events in {args.output}")
        print("Run the upsert script to send these to Kafka and test Hudi upserts")
    else:
        print("❌ Failed to generate upsert data")
