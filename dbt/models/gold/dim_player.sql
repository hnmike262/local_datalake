{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Player (summoner identity + current rank)
-- Grain: 1 row per unique player (puuid)
-- Combines player identity from matches + current rank from ladder

WITH players_from_matches AS (
    SELECT
        puuid,
        platform,
        -- Clean empty summoner_name, fallback to riot_id
        COALESCE(
            NULLIF(TRIM(summoner_name), ''),
            riot_id_name
        ) AS summoner_name,
        riot_id_name,
        riot_id_tagline,
        ingest_ts,
        ROW_NUMBER() OVER (
            PARTITION BY puuid
            ORDER BY 
                CASE WHEN NULLIF(TRIM(summoner_name), '') IS NOT NULL THEN 0 ELSE 1 END,
                ingest_ts DESC
        ) AS rn
    FROM {{ ref('stg_matches_participants') }}
    WHERE puuid IS NOT NULL
),

latest_players AS (
    SELECT
        puuid,
        platform,
        summoner_name,
        riot_id_name,
        riot_id_tagline
    FROM players_from_matches
    WHERE rn = 1
),

ladder_data AS (
    SELECT
        puuid,
        platform,
        tier AS current_tier,
        league_points AS current_lp,
        wins AS current_wins,
        losses AS current_losses,
        winrate AS current_winrate
    FROM {{ ref('stg_ladder') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY p.puuid) AS player_key,
    p.puuid,
    
    -- Player identity
    p.platform,
    p.summoner_name,
    p.riot_id_name,
    p.riot_id_tagline,
    
    -- Current rank from ladder 
    l.current_tier,
    l.current_lp,
    l.current_wins,
    l.current_losses,
    l.current_winrate,
    
    -- Rank Emblem Image URL (using correct CommunityDragon path)
    CASE l.current_tier
        WHEN 'CHALLENGER' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-challenger.png'
        WHEN 'GRANDMASTER' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-grandmaster.png'
        WHEN 'MASTER' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-master.png'
        WHEN 'DIAMOND' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-diamond.png'
        WHEN 'EMERALD' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-emerald.png'
        WHEN 'PLATINUM' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-platinum.png'
        WHEN 'GOLD' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-gold.png'
        WHEN 'SILVER' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-silver.png'
        WHEN 'BRONZE' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-bronze.png'
        WHEN 'IRON' THEN 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-static-assets/global/default/ranked-emblem/emblem-iron.png'
   
    END AS rank_emblem_url
FROM latest_players p
LEFT JOIN ladder_data l
    ON p.puuid = l.puuid
    AND p.platform = l.platform
