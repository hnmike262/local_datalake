{{ config(
    materialized='table',
    schema='silver'
) }}

-- Clean and enrich participant data

SELECT
    -- Match identifiers
    match_id,
    platform,
    queue_id,
    game_mode,
    game_version,
    
    -- Game timing
    game_duration,
    game_creation,
    game_start_timestamp,
    game_end_timestamp,
    
    -- Player identifiers
    puuid,
    summoner_name,
    riot_id_name,
    riot_id_tagline,
    participant_id,
    team_id,
    side,
    
    -- Champion info
    champion_id,
    champion_name,
    team_position,
    lane,
    
    -- Game outcome
    win,
    
    -- Combat stats
    kills,
    deaths,
    assists,
    CASE 
        WHEN deaths = 0 THEN CAST((kills + assists) AS DOUBLE)
        ELSE CAST((kills + assists) AS DOUBLE) / CAST(deaths AS DOUBLE)
    END AS kda,
    
    -- Economy
    gold_earned,
    total_minions_killed,
    neutral_minions_killed,
    total_ally_jungle_minions_killed,
    total_enemy_jungle_minions_killed,
    total_minions_killed + neutral_minions_killed AS total_cs,
    
    -- Damage stats
    total_damage_dealt_to_champions,
    total_damage_taken,
    damage_self_mitigated,
    total_damage_shielded_on_teammates,
    total_heals_on_teammates,
    damage_dealt_to_buildings,
    damage_dealt_to_objectives,
    
    -- Objectives
    dragon_kills,
    turret_kills,
    turrets_lost,
    objectives_stolen,
    
    -- Vision
    vision_score,
    wards_placed,
    wards_killed,
    control_wards_placed,
    
    -- First blood/tower
    first_blood_kill,
    first_blood_assist,
    first_tower_kill,
    first_tower_assist,
    
    -- Game end conditions
    game_ended_in_early_surrender,
    game_ended_in_surrender,
    
    -- Performance
    longest_time_spent_living,
    largest_killing_spree,
    total_time_cc_dealt,
    total_time_spent_dead,
    
    -- Summoner spells
    summoner1_id,
    summoner2_id,
    
    -- Items
    item0, item1, item2, item3, item4, item5, item6,
    
    -- Runes
    perk_keystone,
    perk_primary_row_1,
    perk_primary_row_2,
    perk_primary_row_3,
    perk_secondary_row_1,
    perk_secondary_row_2,
    perk_primary_style,
    perk_secondary_style,
    perk_shard_defense,
    perk_shard_flex,
    perk_shard_offense,
    
    -- Derived metrics
    CASE 
        WHEN game_duration > 0 THEN CAST(gold_earned AS DOUBLE) / (CAST(game_duration AS DOUBLE) / 60)
        ELSE 0.0
    END AS gold_per_minute,
    
    CASE 
        WHEN game_duration > 0 THEN CAST((total_minions_killed + neutral_minions_killed) AS DOUBLE) / (CAST(game_duration AS DOUBLE) / 60)
        ELSE 0.0
    END AS cs_per_minute,
    
    -- Timestamps
    from_iso8601_timestamp(ingest_ts) AS ingest_ts
    
FROM {{ source('bronze', 'matches_participants') }}
