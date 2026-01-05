-- Check Iceberg Bronze tables metadata
-- Run: docker exec -i trino trino < scripts/check_iceberg_metadata.sql

-- 1. List all bronze tables
SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema = 'bronze';

-- 2. Row counts
SELECT 'ladder' as table_name, COUNT(*) as row_count FROM iceberg.bronze.ladder
UNION ALL
SELECT 'match_ids', COUNT(*) FROM iceberg.bronze.match_ids
UNION ALL
SELECT 'matches_participants', COUNT(*) FROM iceberg.bronze.matches_participants
UNION ALL
SELECT 'matches_teams', COUNT(*) FROM iceberg.bronze.matches_teams;

-- 3. Table properties
SHOW CREATE TABLE iceberg.bronze.matches_participants;
