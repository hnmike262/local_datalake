{{ config(
    materialized='table',
    schema='gold'
) }}

-- Dimension: Summoner Spells (with Data Dragon image URLs)
-- Grain: 1 row per unique spell_id
-- Maps spell1_id, spell2_id in fct_participant_match

WITH spell_reference AS (
    -- Static mapping: spell_id -> spell_key (Data Dragon uses spell_key for image filenames)
    SELECT * FROM (VALUES
        (21, 'SummonerBarrier', 'Barrier'),
        (1, 'SummonerBoost', 'Cleanse'),
        (14, 'SummonerDot', 'Ignite'),
        (3, 'SummonerExhaust', 'Exhaust'),
        (4, 'SummonerFlash', 'Flash'),
        (6, 'SummonerHaste', 'Ghost'),
        (7, 'SummonerHeal', 'Heal'),
        (13, 'SummonerMana', 'Clarity'),
        (11, 'SummonerSmite', 'Smite'),
        (32, 'SummonerSnowball', 'Mark'),
        (12, 'SummonerTeleport', 'Teleport')
    ) AS t(spell_id, spell_key, spell_name)
),

-- Get distinct spells used in matches
spells_used AS (
    SELECT DISTINCT spell_id
    FROM (
        SELECT summoner1_id AS spell_id FROM {{ ref('stg_matches_participants') }}
        UNION
        SELECT summoner2_id FROM {{ ref('stg_matches_participants') }}
    ) all_spells
    WHERE spell_id IS NOT NULL AND spell_id > 0
)

SELECT
    s.spell_id AS spell_key,
    s.spell_id,
    COALESCE(r.spell_name, CONCAT('Spell ', CAST(s.spell_id AS VARCHAR))) AS spell_name,
    COALESCE(r.spell_key, CONCAT('Summoner', CAST(s.spell_id AS VARCHAR))) AS spell_key_name,
    -- Data Dragon Image URL (v14.24.1)
    CONCAT(
        'https://ddragon.leagueoflegends.com/cdn/14.24.1/img/spell/',
        COALESCE(r.spell_key, CONCAT('Summoner', CAST(s.spell_id AS VARCHAR))),
        '.png'
    ) AS spell_image_url
FROM spells_used s
LEFT JOIN spell_reference r ON s.spell_id = r.spell_id
