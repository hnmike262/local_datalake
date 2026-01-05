{{ config(
    materialized='table',
    schema='silver',
    on_table_exists='drop'
) }}

-- Clean team data
SELECT
    match_id,
    team_id,
    side,
    win,
    
    -- Objectives
    baron_kills,
    dragon_kills,
    rift_herald_kills,
    tower_kills,
    inhibitor_kills,
    champion_kills,
    
    -- First objectives
    first_baron,
    first_dragon,
    first_rift_herald,
    first_tower,
    first_inhibitor,
    first_blood,
    
    -- Bans (individual columns)
    ban_1,
    ban_2,
    ban_3,
    ban_4,
    ban_5,
    
    -- Game info
    game_version,
    queue_id,
    
    -- Timestamps
    from_iso8601_timestamp(ingest_ts) AS ingest_ts
    
FROM {{ source('bronze', 'matches_teams') }}
