{{ config(
    materialized='view',
    schema='bronze'
) }}

-- This view reads from raw Iceberg table
-- Bronze tables are usually views over raw data

SELECT
  order_id,
  customer_id,
  order_amount,
  order_date,
  current_timestamp() AS _ingested_at,
  CAST(current_date AS DATE) AS ingest_date
FROM (
  VALUES
  (1, 101, 100.50, '2024-01-01'::date),
  (2, 102, 250.75, '2024-01-02'::date),
  (3, 101, 150.00, '2024-01-03'::date)
) AS t(order_id, customer_id, order_amount, order_date)
