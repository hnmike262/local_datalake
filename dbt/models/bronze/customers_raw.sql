{{ config(
    materialized='view',
    schema='bronze'
) }}

SELECT
  customer_id,
  name,
  email,
  current_timestamp() AS _ingested_at,
  CAST(current_date AS DATE) AS ingest_date
FROM (
  VALUES
  (101, 'Alice Smith', 'alice@example.com'),
  (102, 'Bob Johnson', 'bob@example.com'),
  (103, 'Charlie Brown', 'charlie@example.com')
) AS t(customer_id, name, email)
