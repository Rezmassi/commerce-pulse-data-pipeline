import json
import os
import hashlib
import uuid
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# Loading environment variables
load_dotenv()

# MongoDB Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "commercepulse")
COLLECTION_NAME = "events_raw"

# Files to bootstrap
DATA_FILES = {
    "historical_order": "data/bootstrap/orders_2023.json",
    "historical_payment": "data/bootstrap/payments_2023.json",
    "historical_shipment": "data/bootstrap/shipments_2023.json",
    "historical_refund": "data/bootstrap/refunds_2023.json",
}

def generate_event_id(vendor, record_id, event_type):
    """Generates a deterministic hash based on vendor, record ID, and event type."""
    hash_str = f"{vendor}_{record_id}_{event_type}"
    return hashlib.sha256(hash_str.encode()).hexdigest()

def bootstrap_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Create an index on event_id for performance and uniqueness 
    collection.create_index("event_id", unique=True)

    total_inserted = 0 

    for event_type, file_path in DATA_FILES.items():
        if not os.path.exists(file_path):
            print(f"Skipping {file_path}: File not found.")
            continue

        print(f"Processing {file_path}...")
        
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Standardize data to a list of records
        records = data if isinstance(data, list) else [data]
        
        operations = []
        for record in records:
            # Extract basic info ( based on the preset keys)
            vendor = record.get("vendor_id", "unknown_vendor")
            record_id = record.get("id") or record.get("order_id") or record.get("payment_id")
            event_time = record.get("created_at") or record.get("timestamp") or datetime.utcnow().isoformat()

            # 1. Generate event_id
            event_id = generate_event_id(vendor, record_id, event_type)

            # 2. Wrap as synthetic event
            raw_event = {
                "event_id": event_id,
                "event_type": event_type,
                "event_time": event_time,
                "vendor": vendor,
                "payload": record, 
                "ingested_at": datetime.utcnow()
            }
            
            # 3. Prepare for upsert (to prevent duplicates)
            operations.append(
                UpdateOne(
                    {"event_id": event_id}, 
                    {"$set": raw_event},
                    upsert=True
                )
            )
        
        if operations:
            result = collection.bulk_write(operations)
            total_inserted += result.upserted_count + result.modified_count
            print(f"Loaded {len(records)} records for {event_type}.")

    print(f"Bootstrap complete. Total documents in {COLLECTION_NAME}: {total_inserted}")
    client.close()

if __name__ == "__main__":
    bootstrap_data()