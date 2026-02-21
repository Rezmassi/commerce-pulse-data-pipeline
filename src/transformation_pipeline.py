import os
import pandas as pd
from pymongo import MongoClient
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv() #this will load the .env file and set the environment variables for MongoDB and BigQuery credentials. Make sure to have the .env file in the same directory with the following content:

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "commercepulse")
BQ_PROJECT = "studious-plate-471701-h8"
BQ_DATASET = "analytics"

def load_to_bq(client, dataframe, table_name):
    if dataframe.empty:
        return
    
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    # Use WRITE_TRUNCATE if you want to replace data, or WRITE_APPEND to add to it
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    
    print(f"Loading {len(dataframe)} rows to {table_id}...")
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
    print(f" Successfully loaded {table_name}.")

def transform_and_load():
    print("Step 1: Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_data = list(db["events_raw"].find())
    print(f"Fetched {len(raw_data)} records.")

    if not raw_data:
        print(" No data found in MongoDB!")
        return

    df = pd.DataFrame(raw_data)
    print("DEBUG: Columns in MongoDB are:", df.columns.tolist())
    bq_client = bigquery.Client()

#create a key-helper to return empty values when columns are missing in the dataframe, 
# this is to help avoid KeyErrors during transformation. 
    def safe_get(col_name): return df[col_name] if col_name in df.columns else pd.Series([None] * len(df))


    print("Step 2: Transforming Data...")
    
#_____________________________________________________________________________________

    #shared helper for each field

    def get_order_id(row):
        # Checks top level first
        if pd.notna(row.get('order_id')):
            return row['order_id']
        # Then check inside payload if it's a dict
        if isinstance(row.get('payload'), dict):
            return row['payload'].get('order_id')
        return None
    
    df['effective_order_id'] = df.apply(get_order_id, axis=1)
    
        # ─── 1. PROCESS ORDERS ─────────────────────────────────────────────── 
    ord_mask = (
        df['effective_order_id'].notna()
        & safe_get('refund_id').isna()
        & safe_get('payment_id').isna()
    )
    ord_df = df[ord_mask].copy()

    if not ord_df.empty:
        ord_final = pd.DataFrame()
        ord_final['order_id']       = ord_df['effective_order_id']
        ord_final['customer_email'] = safe_get('email').fillna(safe_get('buyerEmail'))
        ord_final['vendor_id']      = safe_get('vendor').fillna(safe_get('vendor_id')).fillna(safe_get('region')).fillna('unknown_vendor')
        ord_final['order_date']     = safe_get('created_at').fillna(safe_get('created')).fillna(safe_get('event_time'))

        def get_amount(row):
            for field in ['amount', 'totalAmount', 'total']:
                if pd.notna(row.get(field)):
                    return row[field]
            if isinstance(row.get('payload'), dict):
                p = row['payload']
                return p.get('amount', p.get('total_amount', p.get('price', 0)))
            return 0

        ord_final['order_amount'] = ord_df.apply(get_amount, axis=1)

        load_to_bq(bq_client, ord_final, "fact_orders")
        print(f"Successfully loaded {len(ord_final)} rows to fact_orders.")


    # ─── 2. PROCESS PAYMENTS ─────────────────────────────────────────────
    
    pay_mask = (
        df['paid_at'].notna()
        | df['paidAt'].notna()
        | df['amountPaid'].notna()
        | df['payment_status'].notna()
        | df['transaction_id'].notna()
        | df['txRef'].notna()
    )
    
    pay_df = df[pay_mask].copy()
    
    print(f"DEBUG: Detected {len(pay_df)} potential payment records")
    
    if not pay_df.empty:
        pay_final = pd.DataFrame()
    
    
    # Using a fallback chain for payment_id (picks the most reliable identifier)
        pay_final['payment_id'] = (
    pay_df['transaction_id']
    .fillna(pay_df['txRef'])
    .fillna(pay_df['txn'])
    .fillna(pay_df['event_id'])
    .fillna(pd.Series(pay_df.index.astype(str), index=pay_df.index))  # ← fixed
)
        
        pay_final['order_id'] = pay_df['effective_order_id']
        
        pay_final['vendor_id'] = safe_get('vendor').fillna(
            safe_get('vendor_id')
        ).fillna('unknown_vendor')
        
        # Amount — prefer amountPaid, fallback to amount / totalAmount
        def get_pay_amt(row):
            if pd.notna(row.get('amountPaid')):
                return row['amountPaid']
            for f in ['amount', 'totalAmount', 'total']:
                if pd.notna(row.get(f)):
                    return row[f]
            if isinstance(row.get('payload'), dict):
                p = row['payload']
                return p.get('amount_paid', p.get('amount', 0))
            return 0
        
        pay_final['amount_paid'] = pay_df.apply(get_pay_amt, axis=1)
        
        # Status with sensible default
        pay_final['status'] = pay_df['payment_status'].fillna(
            pay_df['status']
        ).fillna('success')   # assume success if unknown
        
        # Optional: capture payment timestamp
        pay_final['paid_at'] = pay_df['paid_at'].fillna(pay_df['paidAt'])
        
        print(f"Loading {len(pay_final)} rows to fact_payments...")
        load_to_bq(bq_client, pay_final, "fact_payments")
        print(f"Successfully loaded {len(pay_final)} rows to fact_payments.")



    # ─── 3. PROCESS REFUNDS ──────────────────────────────────────────────
    ref_mask = (
        df['refunded_at'].notna()
        | df['refundedAt'].notna()
        | df['refundAmount'].notna()
        | df['refund_reason'].notna()
    )
    
    ref_df = df[ref_mask].copy()
    
    print(f"DEBUG: Detected {len(ref_df)} potential refund records")

    if not ref_df.empty:
        ref_final = pd.DataFrame()


        ref_final['refund_id'] = (
    ref_df['event_id']
    .fillna(pd.Series(ref_df.index.astype(str), index=ref_df.index))
)
        
        ref_final['order_id'] = ref_df['effective_order_id']
        
        ref_final['vendor_id'] = safe_get('vendor').fillna(
            safe_get('vendor_id')
        ).fillna('unknown_vendor')
        
        # Amount refunded
        def get_ref_amt(row):
            if pd.notna(row.get('refundAmount')):
                return row['refundAmount']
            if isinstance(row.get('payload'), dict):
                p = row['payload']
                return p.get('refund_amount', p.get('amount_refunded', 0))
            return 0
        
        ref_final['amount_refunded'] = ref_df.apply(get_ref_amt, axis=1)
        
        # Optional: refund timestamp and reason
        ref_final['refunded_at'] = ref_df['refunded_at'].fillna(ref_df['refundedAt'])
        ref_final['reason']      = ref_df['refund_reason'].fillna(ref_df['reason'])
        
        print(f"Loading {len(ref_final)} rows to fact_refunds...")
        load_to_bq(bq_client, ref_final, "fact_refunds")
        print(f"Successfully loaded {len(ref_final)} rows to fact_refunds.")

    print(" Pipeline finished successfully!")

    #___________________DQ CHECK_________________________

def run_dq_checks(bq_client):
    query = f"""
    SELECT missing_vendor_pct, total_records 
    FROM `{BQ_PROJECT}.{BQ_DATASET}.v_data_quality_metrics`
    """
    results = bq_client.query(query).to_dataframe()
    
    if not results.empty:
        pct = results['missing_vendor_pct'].iloc[0]
        total = results['total_records'].iloc[0]
        print(f"\n--- DATA QUALITY REPORT ---")
        print(f"Total Records Processed: {total}")
        print(f"Missing Vendor Rate: {pct}%")
        
        if pct > 70:
            print("⚠️ WARNING: high rate of unknown vendors detected!")
        else:
            print("✅ Data Quality within acceptable limits.")


if __name__ == "__main__":
    transform_and_load()
    # Add this line
    run_dq_checks(bigquery.Client())

