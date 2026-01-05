{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Date 

WITH dates AS (
    SELECT DISTINCT
        DATE(from_unixtime(game_start_timestamp / 1000.0)) AS calendar_date
    FROM {{ ref('stg_matches_participants') }}
    WHERE game_start_timestamp IS NOT NULL
)

SELECT
    CAST(date_format(calendar_date, '%Y%m%d') AS INTEGER) AS date_key,
    calendar_date,
    EXTRACT(year FROM calendar_date)    AS year,
    EXTRACT(quarter FROM calendar_date) AS quarter,
    EXTRACT(month FROM calendar_date)   AS month,
    date_format(calendar_date, '%b')    AS month_name,
    EXTRACT(week FROM calendar_date)    AS week_of_year,
    EXTRACT(day FROM calendar_date)     AS day,
    EXTRACT(dow FROM calendar_date)     AS day_of_week,
    date_format(calendar_date, '%a')    AS day_name,
    CASE 
        WHEN EXTRACT(dow FROM calendar_date) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM dates
