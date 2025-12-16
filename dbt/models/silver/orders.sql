{{ config(
    materialized='table',
    schema='silver'
) }}

-- Silver layer: cleaned and validated orders

SELECT
  order_id,
  customer_id,
  ROUND(order_amount, 2) AS order_amount,
  order_date,
  _ingested_at,
  ingest_date
FROM {{ ref('orders_raw') }}
WHERE order_amount > 0
  AND order_date <= current_date
