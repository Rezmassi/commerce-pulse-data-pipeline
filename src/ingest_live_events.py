#ingests events from event generator

import json
import os
import glob
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "commercepulse")
COLLECTION_NAME = "events_raw"
LIVE_EVENTS_DIR = "data/live_events"

def ingest_live_events():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    files = glob.glob(os.path.join(LIVE_EVENTS_DIR, "*.jsonl"))
    
    total_ingested = 0
    for file_path in files:
        print(f"Ingesting {file_path}...")
        operations = []
        with open(file_path, 'r') as f:
            for line in f:
                event = json.loads(line)
                event['ingested_at'] = datetime.utcnow()
                
                # Upsert based on event_id to handle duplicates
                operations.append(
                    UpdateOne(
                        {"event_id": event['event_id']},
                        {"$set": event},
                        upsert=True
                    )
                )
        
        if operations:
            result = collection.bulk_write(operations)
            total_ingested += result.upserted_count + result.modified_count
            print(f"Ingested {len(operations)} events.")

    print(f"Total documents in {COLLECTION_NAME}: {collection.count_documents({})}")
    client.close()

if __name__ == "__main__":
    ingest_live_events()