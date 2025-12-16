{{ config(
    materialized='table',
    schema='gold'
) }}

-- Gold layer: aggregated daily sales by customer

SELECT
  o.order_date,
  o.customer_id,
  c.email,
  c.name,
  SUM(o.order_amount) AS total_order_amount,
  COUNT(*) AS order_count,
  ROUND(AVG(o.order_amount), 2) AS avg_order_amount
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('customers') }} c
  ON o.customer_id = c.customer_id
GROUP BY o.order_date, o.customer_id, c.email, c.name
ORDER BY o.order_date DESC, o.customer_id
