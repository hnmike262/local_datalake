{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Queue + Patch 

WITH queue_patches AS (
    SELECT DISTINCT
        queue_id,
        game_mode,
        game_version
    FROM {{ ref('stg_matches_participants') }}
    WHERE queue_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY queue_id, game_version) AS queue_patch_key,
    -- Queue 
    queue_id,
    game_mode,
    CASE 
        WHEN queue_id IN (420, 440) THEN TRUE  -- Ranked Solo/Duo, Ranked Flex
        ELSE FALSE 
    END AS is_ranked,
    CASE
        WHEN queue_id = 420 THEN 'Ranked Solo/Duo'
        WHEN queue_id = 440 THEN 'Ranked Flex'
        WHEN queue_id = 450 THEN 'ARAM'
        WHEN queue_id = 400 THEN 'Normal Draft'
        WHEN queue_id = 430 THEN 'Normal Blind'
        ELSE 'Other'
    END AS queue_name,
    
    -- Patch attributes
    game_version,
    CONCAT(
        split_part(game_version, '.', 1), 
        '.', 
        split_part(game_version, '.', 2)
    ) AS patch_major,
    split_part(game_version, '.', 1) AS patch_season
FROM queue_patches
