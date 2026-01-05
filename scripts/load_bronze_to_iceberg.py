# -*- coding: utf-8 -*-
"""
Load Bronze parquet files into Iceberg tables via Trino.

This script is part of the data pipeline for League of Legends analytics:

Data Pipeline Overview:
    1. EXTRACTION (data/extraction/):
       - 01_ladder.py: Extract ranked ladder data from Riot API
       - 02_matchids.py: Extract match IDs for players from Riot API  
       - 03_matchids.py: Extract full match details from Riot API
       - riot_client.py: Centralized Riot API client with rate limiting
       - config.py: Centralized configuration (API keys, regions, MinIO settings)
       - minio_upload.py: Upload extracted data to MinIO object storage
       
    2. LOADING (this script):
       - Creates Iceberg tables in Trino for the Bronze layer
       - Schema definitions match the parquet files from extraction
       - Actual data loading is done via Spark (see load_bronze.py)

Configuration:
    This script uses environment variables for Trino container settings.
    For Riot API and MinIO configuration, see: data/extraction/config.py
    
    Environment Variables:
        TRINO_CONTAINER: Docker container name for Trino (default: 'trino')
        
See Also:
    - data/extraction/config.py: Centralized Riot API and MinIO configuration
    - docs/services/riot-api.md: Riot API integration documentation
"""
import logging
import os
import subprocess
import sys
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurable Trino command via environment variable
TRINO_CONTAINER = os.getenv("TRINO_CONTAINER", "trino")
TRINO_CMD: list[str] = ["docker", "exec", TRINO_CONTAINER, "trino", "--execute"]


def run_trino(sql: str) -> bool:
    """Execute SQL on Trino.
    
    Args:
        sql: The SQL statement to execute.
        
    Returns:
        True if execution succeeded, False otherwise.
    """
    result = subprocess.run(
        TRINO_CMD + [sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Trino execution failed: {result.stderr}")
        return False
    if result.stdout.strip():
        logger.info(result.stdout.strip())
    return True


def main() -> None:
    """Main entry point for loading Bronze data into Iceberg tables."""
    logger.info("=== Loading Bronze data into Iceberg tables ===")
    
    # Create Iceberg tables from local parquet files using Python + PyIceberg
    # Alternative: Use Spark to load
    
    tables: Dict[str, str] = {
        'ladder': """
            CREATE TABLE IF NOT EXISTS iceberg.bronze.ladder (
                platform VARCHAR,
                tier VARCHAR,
                league_points BIGINT,
                wins BIGINT,
                losses BIGINT,
                puuid VARCHAR,
                ingest_ts TIMESTAMP(6)
            )
        """,
        'match_ids': """
            CREATE TABLE IF NOT EXISTS iceberg.bronze.match_ids (
                match_id VARCHAR,
                puuid VARCHAR,
                platform VARCHAR,
                region VARCHAR,
                queue_id INTEGER,
                ingest_ts TIMESTAMP(6)
            )
        """,
        'matches_participants': """
            CREATE TABLE IF NOT EXISTS iceberg.bronze.matches_participants (
                match_id VARCHAR,
                platform VARCHAR,
                queue_id INTEGER,
                game_mode VARCHAR,
                game_version VARCHAR,
                game_duration BIGINT,
                game_creation BIGINT,
                game_start_timestamp BIGINT,
                game_end_timestamp BIGINT,
                puuid VARCHAR,
                summoner_name VARCHAR,
                riot_id_name VARCHAR,
                riot_id_tagline VARCHAR,
                participant_id INTEGER,
                team_id INTEGER,
                champion_id INTEGER,
                champion_name VARCHAR,
                champion_level INTEGER,
                kills INTEGER,
                deaths INTEGER,
                assists INTEGER,
                gold_earned INTEGER,
                total_damage_dealt INTEGER,
                total_damage_to_champions INTEGER,
                total_minions_killed INTEGER,
                vision_score INTEGER,
                win BOOLEAN,
                ingest_ts TIMESTAMP(6)
            )
        """,
        'matches_teams': """
            CREATE TABLE IF NOT EXISTS iceberg.bronze.matches_teams (
                match_id VARCHAR,
                team_id INTEGER,
                side VARCHAR,
                win BOOLEAN,
                baron_kills INTEGER,
                dragon_kills INTEGER,
                rift_herald_kills INTEGER,
                tower_kills INTEGER,
                inhibitor_kills INTEGER,
                champion_kills INTEGER,
                first_baron BOOLEAN,
                first_dragon BOOLEAN,
                first_rift_herald BOOLEAN,
                first_tower BOOLEAN,
                first_inhibitor BOOLEAN,
                first_blood BOOLEAN,
                ban_1 INTEGER,
                ban_2 INTEGER,
                ban_3 INTEGER,
                ban_4 INTEGER,
                ban_5 INTEGER,
                game_version VARCHAR,
                queue_id INTEGER,
                ingest_ts TIMESTAMP(6)
            )
        """
    }
    
    for table_name, ddl in tables.items():
        logger.info(f"Creating iceberg.bronze.{table_name}...")
        if run_trino(ddl.strip()):
            logger.info(f"  [OK] Created {table_name}")
        else:
            logger.error(f"  [FAIL] {table_name}")
    
    logger.info("=== Tables created, now load data via Spark ===")
    logger.info("Run: docker exec spark python /opt/spark/jobs/load_bronze.py")


if __name__ == '__main__':
    main()
