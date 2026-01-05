{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Items (with Data Dragon image URLs)
-- Grain: 1 row per unique item_id
-- Maps item0-item6 in fct_participant_match

WITH items_used AS (
    SELECT DISTINCT item_id
    FROM (
        SELECT item0 AS item_id FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item1 FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item2 FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item3 FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item4 FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item5 FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT item6 FROM {{ ref('stg_matches_participants') }}
    ) all_items
    WHERE item_id IS NOT NULL AND item_id > 0
)

SELECT
    item_id AS item_key,
    item_id,
    -- Data Dragon Image URL (v14.24.1)
    CONCAT(
        'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/',
        CAST(item_id AS VARCHAR),
        '.png'
    ) AS item_image_url
FROM items_used
