{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Champion (with Data Dragon image URLs)
-- Grain: 1 row per unique champion_id

WITH champions AS (
    SELECT DISTINCT
        champion_id,
        champion_name
    FROM {{ ref('stg_matches_participants') }}
    WHERE champion_id IS NOT NULL
)

SELECT
    champion_id AS champion_key,
    champion_id,
    champion_name,
    -- Data Dragon Image URLs (v14.24.1)
    CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/champion/', champion_name, '.png') AS champion_image_url,
    CONCAT('https://ddragon.leagueoflegends.com/cdn/img/champion/splash/', champion_name, '_0.jpg') AS champion_splash_url,
    CONCAT('https://ddragon.leagueoflegends.com/cdn/img/champion/loading/', champion_name, '_0.jpg') AS champion_loading_url
FROM champions
