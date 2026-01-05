{{ config(
    materialized='table',
    schema='silver',
    on_table_exists='drop'
) }}

--  Clean and deduplicate ladder data

WITH src AS (
    SELECT
        puuid,
        -- Normalize platform to UPPERCASE for consistent joining
        UPPER(platform) AS platform,
        tier,
        league_points,
        wins,
        losses,
        wins + losses AS total_games,
        CASE 
            WHEN (wins + losses) > 0 
            THEN CAST(wins AS DOUBLE) / CAST((wins + losses) AS DOUBLE)
            ELSE 0.0
        END AS winrate,
        from_iso8601_timestamp(ingest_ts) AS ingest_ts
    FROM {{ source('bronze', 'ladder') }}
    WHERE puuid IS NOT NULL  -- Filter out rows without PUUID
),

-- Get latest record per player
latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY puuid ORDER BY ingest_ts DESC) AS rn
    FROM src
)

SELECT
    puuid,
    platform,
    tier,
    league_points,
    wins,
    losses,
    total_games,
    winrate,
    ingest_ts
FROM latest
WHERE rn = 1
