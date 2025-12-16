{{ config(
    materialized='table',
    schema='silver'
) }}

-- Silver layer: deduplicated and cleaned customers

SELECT DISTINCT
  customer_id,
  TRIM(LOWER(email)) AS email,
  INITCAP(TRIM(name)) AS name,
  MAX(_ingested_at) AS last_ingested_at,
  MAX(ingest_date) AS last_ingest_date
FROM {{ ref('customers_raw') }}
WHERE email IS NOT NULL
GROUP BY customer_id, email, name
