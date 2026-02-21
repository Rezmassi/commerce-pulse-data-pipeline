-- Schema for Order Analytics
CREATE TABLE IF NOT EXISTS `analytics.fact_orders` (
    order_id STRING,
    customer_email STRING,
    vendor_id STRING,
    order_date TIMESTAMP,
    order_amount FLOAT64
);

-- Schema for Payment Tracking
CREATE TABLE IF NOT EXISTS `analytics.fact_payments` (
    payment_id STRING,
    order_id STRING,
    vendor_id STRING,
    amount_paid FLOAT64,
    status STRING,
    paid_at TIMESTAMP
);

-- Schema for Refund Tracking
CREATE TABLE IF NOT EXISTS `analytics.fact_refunds` (
    refund_id STRING,
    order_id STRING,
    vendor_id STRING,
    amount_refunded FLOAT64,
    refunded_at TIMESTAMP,
    reason STRING
);