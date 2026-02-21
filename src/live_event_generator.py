#generates new events that look like they are coming from a live API, 
# complete with duplicates and late arrivals.

import json
import os
import uuid
import random
from datetime import datetime, timedelta
import argparse

def generate_live_events(output_dir, num_events):
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    file_path = os.path.join(output_dir, f"{today}_events.jsonl")
    
    vendors = ["vendor_alpha", "vendor_beta"]
    event_types = ["order_created", "payment_attempt", "shipment_updated"]

    print(f"Generating {num_events} live events to {file_path}...")

    with open(file_path, 'w') as f:
        for _ in range(num_events):
            event_type = random.choice(event_types)
            vendor = random.choice(vendors)
            # Create events for both old and new order IDs
            order_id = f"ORDER-{random.randint(1000, 1100)}" 
            
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_time": (datetime.utcnow() - timedelta(minutes=random.randint(0, 120))).isoformat(),
                "vendor": vendor,
                "payload": {
                    "order_id": order_id,
                    "status": random.choice(["pending", "processing", "shipped"]) if "shipment" in event_type else None,
                    "amount": round(random.uniform(50, 500), 2)
                }
            }
            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='data/live_events')
    parser.add_argument('--events', type=int, default=100)
    args = parser.parse_args()
    
    generate_live_events(args.out, args.events)