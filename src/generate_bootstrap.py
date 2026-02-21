import json
import os
import uuid
from datetime import datetime, timedelta

# Ensure the directory does exist
os.makedirs("data/bootstrap", exist_ok=True)

def generate_historical_data():
    vendors = ["vendor_alpha", "vendor_beta"]
    orders = []
    payments = []
    shipments = []
    refunds = []

    for i in range(1, 101):  # Generate 100 historical orders
        order_id = f"HIST-ORD-{i:04d}"
        vendor = vendors[i % 2]
        order_date = datetime(2023, 1, 1) + timedelta(days=i % 365)
        
        # 1. Create Order
        orders.append({
            "order_id": order_id,
            "vendor_id": vendor,
            "amount": 100.0 + i,
            "customer_email": f"user{i}@example.com",
            "created_at": order_date.isoformat()
        })

        # 2. Create Payment (90% success rate)
        if i % 10 != 0:
            payments.append({
                "payment_id": f"PAY-{uuid.uuid4().hex[:8]}",
                "order_id": order_id,
                "status": "success",
                "vendor_id": vendor,
                "amount_paid": 100.0 + i,
                "timestamp": (order_date + timedelta(minutes=15)).isoformat()
            })

        # 3. Create Shipment (for successful payments)
        if i % 10 != 0:
            shipments.append({
                "shipment_id": f"SHP-{uuid.uuid4().hex[:8]}",
                "order_id": order_id,
                "vendor_id": vendor,
                "carrier": "LogiTrans",
                "status": "delivered",
                "delivered_at": (order_date + timedelta(days=2)).isoformat()
            })

        # 4. Create a few Refunds
        if i % 20 == 0: 
            refunds.append({
                "refund_id": f"REF-{uuid.uuid4().hex[:8]}",
                "order_id": order_id,
                "vendor_id": vendor,
                "amount_refunded": 50.0,
                "reason": "customer_request",
                "timestamp": (order_date + timedelta(days=5)).isoformat()
            })

    # Save to files
    files = {
        "orders_2023.json": orders,
        "payments_2023.json": payments,
        "shipments_2023.json": shipments,
        "refunds_2023.json": refunds
    }

    for filename, content in files.items():
        with open(f"data/bootstrap/{filename}", "w") as f:
            json.dump(content, f, indent=4)
        print(f"Created data/bootstrap/{filename}")

if __name__ == "__main__":
    generate_historical_data()