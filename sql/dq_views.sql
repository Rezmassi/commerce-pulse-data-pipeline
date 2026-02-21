-- Data Quality Monitoring View
CREATE OR REPLACE VIEW `analytics.v_data_quality_metrics` AS
SELECT 
  'fact_orders' as table_name,
  COUNT(*) as total_records,
  -- Tracks missing identifiers
  SUM(CASE WHEN vendor_id = 'unknown_vendor' THEN 1 ELSE 0 END) as missing_vendor_count,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN vendor_id = 'unknown_vendor' THEN 1 ELSE 0 END), COUNT(*)) * 100, 2) as missing_vendor_pct,
  -- Financial integrity checks
  SUM(CASE WHEN order_amount <= 0 THEN 1 ELSE 0 END) as zero_or_negative_orders,
  -- Data completeness
  SUM(CASE WHEN customer_email IS NULL THEN 1 ELSE 0 END) as missing_email_count
FROM `analytics.fact_orders`;