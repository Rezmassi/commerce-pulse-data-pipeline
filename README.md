🚀 CommercePulse: Unified Data Engineering Pipeline

📖 Project Overview
CommercePulse faced a critical data silo issue: historical order data was stored in flat legacy files, while new vendor data arrived via a nested JSON event stream. This project implements a Medallion Architecture to ingest, transform, and unify these streams into a Google BigQuery data warehouse.


🏗️ Architecture & Design Decisions

1. The Data Stack
Source: MongoDB (Landing Zone for raw, schema-less JSON events).

Processing: Python (Pandas) for schema normalization and payload extraction.

Warehouse: Google BigQuery (Star Schema for high-speed analytics).

2. Assumptions
ID Persistence: We assume order_id is the primary join key across all events (orders, payments, refunds).

Vendor Drift: We assume future vendors may use different keys (e.g., price vs amount). The pipeline uses a fallback-chain logic to handle this.

3. Trade-offs:

* Schema-on-Read (MongoDB) vs. Schema-on-Write (SQL Landing)
We chose to land the raw data in MongoDB first rather than streaming it directly into a structured SQL table.

The Trade-off: By using MongoDB as a "Bronze" layer, we traded immediate structure for durability.

The Benefit: If a new vendor joins tomorrow and completely changes their JSON structure, the ingestion script won't crash. MongoDB will just store the abnormal JSON. If we had gone straight to a SQL landing zone, the pipeline would have broken the moment the schema shifted.

The Cost: We have to do more "heavy lifting" in Python to parse the data later.

* State Management: Truncate vs. Upsert
In the load_to_bq function, we used WRITE_TRUNCATE.

The Trade-off: Traded "Historical Versioning" for Idempotency.

The Benefit: The pipeline is "Self-Healing." If it fails halfway through, you can just run it again without worrying about doubling your revenue numbers or creating duplicate orders.

The Cost: Lose the ability to see how a record changed over time. If a customer changes their email address, the old email is gone forever. In a more complex "Gold" layer,  use an UPSERT (Merge) strategy to update existing records and insert new ones.


🛠️ Repository Structure
Plaintext
├── data/                   # Raw historical and live event samples
├── sql/                    # SQL DDL and Data Quality Views
│   ├── fact_tables.sql     # Schema definitions for Orders, Payments, Refunds
│   └── dq_views.sql        # Automated Data Quality monitoring
├── src/                    # Python Source Code
│   ├── bootstrap.py        # Ingests historical data
│   ├── live_generator.py   # Simulates live event stream
│   └── transformation.py   # Unified ETL pipeline (The "Engine")
├── .env                    # Config (Excluded via .gitignore)
├── requirements.txt        # Project dependencies
└── README.md               # You are here!


📊 Data Quality & Monitoring
A key deliverable was the creation of Daily Data Quality Reports. We implemented a custom BigQuery View (v_data_quality_metrics) that monitors:

Completeness: Tracks unknown_vendor rates.

Accuracy: Identifies zero-value orders or missing emails.

Consistency: Ensures payments and refunds link back to valid orders.

🚀 How to Run the Pipeline
1. Prerequisites
Python 3.11+

MongoDB (Local or Atlas)

Google Cloud Service Account with BigQuery Admin permissions.

2. Setup
Bash
# Install dependencies
pip install -r requirements.txt

# Configure your environment
# Create a .env file with your MONGO_URI and BQ_PROJECT
3. Execution
Bash
# Step 1: Ingest historical data
python src/bootstrap.py

# Step 2: Generate live events
python src/live_event_generator.py

# Step 3: Run the Unified Transformation Pipeline
python src/transformation_pipeline.py


📈 Final Business Results
The pipeline successfully unified 1,684 records, delivering the following net revenue insights:

Historical Net Revenue: $17,465,047.72

Vendor Alpha Net Revenue: $73,243.16

Vendor Beta Net Revenue: $68,383.25