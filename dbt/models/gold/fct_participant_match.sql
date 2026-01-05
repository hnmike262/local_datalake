{{ config(
    materialized='table',
    schema='gold',
    properties={
        "partitioning": "ARRAY['month(game_start_datetime)']"
    }
) }}

-- Fact: Participant Match Performance (with all Image URLs for Power BI)


WITH participants AS (
    SELECT
        -- Match identifiers
        match_id,
        puuid,
        participant_id,
        team_id,
        
        -- Match context
        platform,
        queue_id,
        game_mode,
        game_version,
        
        -- Timing
        game_duration,
        game_start_timestamp,
        
        -- Player context
        side,
        team_position,
        champion_id,
        champion_name,
        
        -- Outcome
        win,
        
        -- Combat stats
        kills,
        deaths,
        assists,
        kda,
        largest_killing_spree,
        
        -- Economy
        gold_earned,
        total_cs,
        gold_per_minute,
        cs_per_minute,
        
        -- Damage
        total_damage_dealt_to_champions,
        total_damage_taken,
        
        -- Vision
        vision_score,
        wards_placed,
        wards_killed,
        
        -- Performance
        total_time_spent_dead,
        
        -- Spells 
        summoner1_id,
        summoner2_id,
        
        -- Runes 
        perk_keystone,
        perk_primary_style,
        perk_secondary_style,
        
        -- Items
        item0, item1, item2, item3, item4, item5, item6,
        
        ingest_ts
    FROM {{ ref('stg_matches_participants') }}
),

participants_with_time AS (
    SELECT
        p.*,
        from_unixtime(game_start_timestamp / 1000.0) AS game_start_datetime,
        CAST(date_format(
            DATE(from_unixtime(game_start_timestamp / 1000.0)), 
            '%Y%m%d'
        ) AS INTEGER) AS date_key
    FROM participants p
),

-- Join team stats 
participants_with_team AS (
    SELECT
        p.*,
        t.baron_kills    AS team_baron_kills,
        t.dragon_kills   AS team_dragon_kills,
        t.rift_herald_kills AS team_rift_herald_kills,
        t.tower_kills    AS team_tower_kills,
        t.champion_kills AS team_total_kills,
        t.first_blood    AS team_first_blood,
        t.first_dragon   AS team_first_dragon,
        t.first_baron    AS team_first_baron
    FROM participants_with_time p
    LEFT JOIN {{ ref('stg_matches_teams') }} t
        ON p.match_id = t.match_id
        AND p.team_id = t.team_id
),

-- Join rank snapshot 
participants_with_rank AS (
    SELECT
        p.*,
        l.tier          AS rank_tier_at_match,
        l.league_points AS rank_lp_at_match
    FROM participants_with_team p
    LEFT JOIN {{ ref('stg_ladder') }} l
        ON p.puuid = l.puuid
        AND p.platform = l.platform
),

-- dimension keys
final AS (
    SELECT
        p.*,
        dp.player_key,
        dqp.queue_patch_key
    FROM participants_with_rank p
    LEFT JOIN {{ ref('dim_player') }} dp
        ON p.puuid = dp.puuid
    LEFT JOIN {{ ref('dim_queue_patch') }} dqp
        ON p.queue_id = dqp.queue_id
        AND p.game_version = dqp.game_version
)

SELECT
    -- === KEYS ===
    match_id,
    puuid,
    participant_id,
    team_id,
    date_key,                           
    player_key,                         
    champion_id AS champion_key,        
    queue_patch_key,                    

    -- === CONTEXT ===
    platform,
    side,                               
    team_position,                       
    game_start_datetime,
    game_duration AS game_duration_seconds,

    -- === CHAMPION INFO + IMAGE ===
    champion_id,
    champion_name,
    CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/champion/', champion_name, '.png') AS champion_image_url,
    CONCAT('https://ddragon.leagueoflegends.com/cdn/img/champion/splash/', champion_name, '_0.jpg') AS champion_splash_url,

    -- === OUTCOME ===
    win,
    
    -- === COMBAT ===
    kills,
    deaths,
    assists,
    kda,
    largest_killing_spree,
    total_damage_dealt_to_champions AS damage_to_champions,
    total_damage_taken AS damage_taken,
    
    -- === ECONOMY ===
    gold_earned,
    total_cs,
    gold_per_minute,
    cs_per_minute,
    
    -- === VISION ===
    vision_score,
    wards_placed,
    wards_killed,
    
    total_time_spent_dead AS time_spent_dead,
    
    -- === SPELLS + IMAGES ===
    summoner1_id AS spell1_id,
    summoner2_id AS spell2_id,
    -- Spell Image URLs (Data Dragon)
    CASE summoner1_id
        WHEN 21 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerBarrier.png'
        WHEN 1 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerBoost.png'
        WHEN 14 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerDot.png'
        WHEN 3 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerExhaust.png'
        WHEN 4 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerFlash.png'
        WHEN 6 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerHaste.png'
        WHEN 7 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerHeal.png'
        WHEN 13 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerMana.png'
        WHEN 11 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerSmite.png'
        WHEN 32 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerSnowball.png'
        WHEN 12 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerTeleport.png'
        ELSE NULL
    END AS spell1_image_url,
    CASE summoner2_id
        WHEN 21 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerBarrier.png'
        WHEN 1 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerBoost.png'
        WHEN 14 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerDot.png'
        WHEN 3 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerExhaust.png'
        WHEN 4 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerFlash.png'
        WHEN 6 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerHaste.png'
        WHEN 7 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerHeal.png'
        WHEN 13 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerMana.png'
        WHEN 11 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerSmite.png'
        WHEN 32 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerSnowball.png'
        WHEN 12 THEN 'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/SummonerTeleport.png'
        ELSE NULL
    END AS spell2_image_url,

    -- === RUNES + IMAGES ===
    perk_keystone AS keystone_rune_id,
    perk_primary_style AS primary_rune_tree,
    perk_secondary_style AS secondary_rune_tree,
    -- Keystone Rune Image URLs (Data Dragon)
    CASE perk_keystone
        -- Precision
        WHEN 8005 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Precision/PressTheAttack/PressTheAttack.png'
        WHEN 8008 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Precision/LethalTempo/LethalTempoTemp.png'
        WHEN 8021 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Precision/FleetFootwork/FleetFootwork.png'
        WHEN 8010 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Precision/Conqueror/Conqueror.png'
        -- Domination
        WHEN 8112 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Domination/Electrocute/Electrocute.png'
        WHEN 8124 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Domination/Predator/Predator.png'
        WHEN 8128 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Domination/DarkHarvest/DarkHarvest.png'
        WHEN 9923 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Domination/HailOfBlades/HailOfBlades.png'
        -- Sorcery
        WHEN 8214 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Sorcery/SummonAery/SummonAery.png'
        WHEN 8229 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Sorcery/ArcaneComet/ArcaneComet.png'
        WHEN 8230 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Sorcery/PhaseRush/PhaseRush.png'
        -- Resolve
        WHEN 8437 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Resolve/GraspOfTheUndying/GraspOfTheUndying.png'
        WHEN 8439 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Resolve/VeteranAftershock/VeteranAftershock.png'
        WHEN 8465 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Resolve/Guardian/Guardian.png'
        -- Inspiration
        WHEN 8351 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Inspiration/GlacialAugment/GlacialAugment.png'
        WHEN 8360 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Inspiration/UnsealedSpellbook/UnsealedSpellbook.png'
        WHEN 8369 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/Inspiration/FirstStrike/FirstStrike.png'
        ELSE NULL
    END AS keystone_rune_image_url,
    -- Primary Rune Tree Image
    CASE perk_primary_style
        WHEN 8000 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7201_Precision.png'
        WHEN 8100 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7200_Domination.png'
        WHEN 8200 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7202_Sorcery.png'
        WHEN 8300 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7203_Inspiration.png'
        WHEN 8400 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7204_Resolve.png'
        ELSE NULL
    END AS primary_rune_tree_image_url,
    -- Secondary Rune Tree Image
    CASE perk_secondary_style
        WHEN 8000 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7201_Precision.png'
        WHEN 8100 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7200_Domination.png'
        WHEN 8200 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7202_Sorcery.png'
        WHEN 8300 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7203_Inspiration.png'
        WHEN 8400 THEN 'https://ddragon.leagueoflegends.com/cdn/img/perk-images/Styles/7204_Resolve.png'
        ELSE NULL
    END AS secondary_rune_tree_image_url,

    -- === ITEMS + IMAGES ===
    item0, item1, item2, item3, item4, item5, item6,
    -- Item Image URLs (Data Dragon)
    CASE WHEN item0 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item0 AS VARCHAR), '.png') ELSE NULL END AS item0_image_url,
    CASE WHEN item1 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item1 AS VARCHAR), '.png') ELSE NULL END AS item1_image_url,
    CASE WHEN item2 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item2 AS VARCHAR), '.png') ELSE NULL END AS item2_image_url,
    CASE WHEN item3 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item3 AS VARCHAR), '.png') ELSE NULL END AS item3_image_url,
    CASE WHEN item4 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item4 AS VARCHAR), '.png') ELSE NULL END AS item4_image_url,
    CASE WHEN item5 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item5 AS VARCHAR), '.png') ELSE NULL END AS item5_image_url,
    CASE WHEN item6 > 0 THEN CONCAT('https://ddragon.leagueoflegends.com/cdn/14.24.1/img/item/', CAST(item6 AS VARCHAR), '.png') ELSE NULL END AS item6_image_url,

    -- === ROLE ICON (Unicode for simple display) ===
    CASE team_position
        WHEN 'TOP' THEN '🛡️'
        WHEN 'JUNGLE' THEN '🌲'
        WHEN 'MIDDLE' THEN '⚔️'
        WHEN 'BOTTOM' THEN '🎯'
        WHEN 'UTILITY' THEN '💚'
        ELSE '❓'
    END AS role_icon,

    -- === TEAM OBJECTIVES ===
    team_baron_kills,
    team_dragon_kills,
    team_rift_herald_kills,
    team_tower_kills,
    team_total_kills,
    team_first_blood,
    team_first_dragon,
    team_first_baron,
    
    -- === RANK AT MATCH ===
    rank_tier_at_match,
    rank_lp_at_match,
    
    ingest_ts

FROM final
