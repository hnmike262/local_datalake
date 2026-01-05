# -*- coding: utf-8 -*-
"""
Verify Iceberg table schema matches Parquet file schema
Compare: Parquet (source of truth) vs Iceberg (current tables)

Riot API Configuration Status:
------------------------------
This file does NOT contain any Riot API configuration.
It only defines expected Iceberg schemas for data validation purposes.

Contains Riot-related field references (data schema only, not API config):
- riot_id_name: Player's Riot ID name (from Match-V5 API response)
- riot_id_tagline: Player's Riot ID tagline (from Match-V5 API response)

These fields are part of the matches_participants schema and represent
data extracted from Riot API, not API configuration settings.
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pyarrow.parquet as pq

from schema_utils import arrow_type_to_trino

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_parquet_schema(parquet_path: Path) -> List[Tuple[str, str]]:
    """
    Read schema from parquet file.

    Args:
        parquet_path: Path to the parquet file

    Returns:
        List of (column_name, trino_type) tuples
    """
    schema = pq.read_schema(parquet_path)
    columns: List[Tuple[str, str]] = []
    for field in schema:
        trino_type = arrow_type_to_trino(str(field.type))
        columns.append((field.name, trino_type))
    return columns


def get_parquet_row_count(parquet_path: Path) -> Optional[int]:
    """
    Get the row count from a parquet file.

    Args:
        parquet_path: Path to the parquet file

    Returns:
        Number of rows in the parquet file, or None if file doesn't exist
    """
    if not parquet_path.exists():
        return None
    parquet_file = pq.ParquetFile(parquet_path)
    return parquet_file.metadata.num_rows


def compare_schemas(
    parquet_cols: List[Tuple[str, str]], iceberg_cols: List[Tuple[str, str]]
) -> Tuple[bool, List[str]]:
    """
    Compare Parquet and Iceberg schemas.

    Args:
        parquet_cols: List of (name, type) from Parquet
        iceberg_cols: List of (name, type) from Iceberg

    Returns:
        Tuple of (all_match: bool, mismatches: list of mismatch descriptions)
    """
    mismatches: List[str] = []

    if len(parquet_cols) != len(iceberg_cols):
        mismatches.append(
            f"Column count differs: Parquet={len(parquet_cols)}, Iceberg={len(iceberg_cols)}"
        )
        return False, mismatches

    for i, (pq_col, ice_col) in enumerate(zip(parquet_cols, iceberg_cols)):
        pq_name, pq_type = pq_col
        ice_name, ice_type = ice_col

        if pq_name != ice_name:
            mismatches.append(f"[{i}] Name: Parquet={pq_name} vs Iceberg={ice_name}")
        if pq_type != ice_type:
            mismatches.append(
                f"[{i}] Type: {pq_name}: Parquet={pq_type} vs Iceberg={ice_type}"
            )

    return len(mismatches) == 0, mismatches


def main() -> None:
    """Main verification routine."""
    logger.info("=" * 70)
    logger.info("SCHEMA VERIFICATION: Parquet vs Iceberg")
    logger.info("=" * 70)

    tables: Dict[str, str] = {
        "ladder": "data/bronze/ladder.parquet",
        "match_ids": "data/bronze/match_ids.parquet",
        "matches_participants": "data/bronze/matches_participants.parquet",
        "matches_teams": "data/bronze/matches_teams.parquet",
    }

    # Iceberg schemas from DESCRIBE output (captured from Trino)
    iceberg_schemas: Dict[str, List[Tuple[str, str]]] = {
        "ladder": [
            ("platform", "varchar"),
            ("tier", "varchar"),
            ("league_points", "bigint"),
            ("wins", "bigint"),
            ("losses", "bigint"),
            ("puuid", "varchar"),
            ("ingest_ts", "varchar"),
        ],
        "match_ids": [
            ("match_id", "varchar"),
            ("puuid", "varchar"),
            ("platform", "varchar"),
            ("region", "varchar"),
            ("queue_id", "bigint"),
            ("ingest_ts", "varchar"),
        ],
        "matches_teams": [
            ("match_id", "varchar"),
            ("team_id", "bigint"),
            ("side", "varchar"),
            ("win", "boolean"),
            ("baron_kills", "bigint"),
            ("dragon_kills", "bigint"),
            ("rift_herald_kills", "bigint"),
            ("tower_kills", "bigint"),
            ("inhibitor_kills", "bigint"),
            ("champion_kills", "bigint"),
            ("first_baron", "boolean"),
            ("first_dragon", "boolean"),
            ("first_rift_herald", "boolean"),
            ("first_tower", "boolean"),
            ("first_inhibitor", "boolean"),
            ("first_blood", "boolean"),
            ("ban_1", "bigint"),
            ("ban_2", "bigint"),
            ("ban_3", "bigint"),
            ("ban_4", "bigint"),
            ("ban_5", "bigint"),
            ("game_version", "varchar"),
            ("queue_id", "bigint"),
            ("ingest_ts", "varchar"),
        ],
        "matches_participants": [
            ("match_id", "varchar"),
            ("platform", "varchar"),
            ("queue_id", "bigint"),
            ("game_mode", "varchar"),
            ("game_version", "varchar"),
            ("game_duration", "bigint"),
            ("game_creation", "bigint"),
            ("game_start_timestamp", "bigint"),
            ("game_end_timestamp", "bigint"),
            ("puuid", "varchar"),
            ("summoner_name", "varchar"),
            ("riot_id_name", "varchar"),
            ("riot_id_tagline", "varchar"),
            ("participant_id", "bigint"),
            ("team_id", "bigint"),
            ("side", "varchar"),
            ("champion_id", "bigint"),
            ("champion_name", "varchar"),
            ("team_position", "varchar"),
            ("lane", "varchar"),
            ("win", "boolean"),
            ("kills", "bigint"),
            ("deaths", "bigint"),
            ("assists", "bigint"),
            ("gold_earned", "bigint"),
            ("total_minions_killed", "bigint"),
            ("neutral_minions_killed", "bigint"),
            ("total_ally_jungle_minions_killed", "bigint"),
            ("total_enemy_jungle_minions_killed", "bigint"),
            ("total_damage_dealt_to_champions", "bigint"),
            ("total_damage_taken", "bigint"),
            ("damage_self_mitigated", "bigint"),
            ("total_damage_shielded_on_teammates", "bigint"),
            ("total_heals_on_teammates", "bigint"),
            ("damage_dealt_to_buildings", "bigint"),
            ("damage_dealt_to_objectives", "bigint"),
            ("dragon_kills", "bigint"),
            ("turret_kills", "bigint"),
            ("turrets_lost", "bigint"),
            ("objectives_stolen", "bigint"),
            ("vision_score", "bigint"),
            ("wards_placed", "bigint"),
            ("wards_killed", "bigint"),
            ("control_wards_placed", "bigint"),
            ("first_blood_kill", "boolean"),
            ("first_blood_assist", "boolean"),
            ("first_tower_kill", "boolean"),
            ("first_tower_assist", "boolean"),
            ("game_ended_in_early_surrender", "boolean"),
            ("game_ended_in_surrender", "boolean"),
            ("longest_time_spent_living", "bigint"),
            ("largest_killing_spree", "bigint"),
            ("total_time_cc_dealt", "bigint"),
            ("total_time_spent_dead", "bigint"),
            ("summoner1_id", "bigint"),
            ("summoner2_id", "bigint"),
            ("item0", "bigint"),
            ("item1", "bigint"),
            ("item2", "bigint"),
            ("item3", "bigint"),
            ("item4", "bigint"),
            ("item5", "bigint"),
            ("item6", "bigint"),
            ("perk_keystone", "bigint"),
            ("perk_primary_row_1", "bigint"),
            ("perk_primary_row_2", "bigint"),
            ("perk_primary_row_3", "bigint"),
            ("perk_secondary_row_1", "bigint"),
            ("perk_secondary_row_2", "bigint"),
            ("perk_primary_style", "bigint"),
            ("perk_secondary_style", "bigint"),
            ("perk_shard_defense", "bigint"),
            ("perk_shard_flex", "bigint"),
            ("perk_shard_offense", "bigint"),
            ("ingest_ts", "varchar"),
        ],
    }

    all_match: bool = True
    row_counts: Dict[str, Optional[int]] = {}

    for table_name, parquet_path_str in tables.items():
        parquet_path = Path(parquet_path_str)

        logger.info("=" * 70)
        logger.info(f"TABLE: {table_name}")
        logger.info("=" * 70)

        # Get Parquet schema
        if not parquet_path.exists():
            logger.warning(f"{parquet_path} not found")
            row_counts[table_name] = None
            continue

        # Get row count dynamically
        row_counts[table_name] = get_parquet_row_count(parquet_path)

        parquet_cols = get_parquet_schema(parquet_path)
        iceberg_cols = iceberg_schemas.get(table_name, [])

        logger.info(f"Parquet columns: {len(parquet_cols)}")
        logger.info(f"Iceberg columns: {len(iceberg_cols)}")

        # Compare schemas
        match, mismatches = compare_schemas(parquet_cols, iceberg_cols)

        if not match:
            logger.error("MISMATCHES FOUND:")
            for m in mismatches:
                logger.error(f"  {m}")
            all_match = False
        else:
            logger.info("All columns match!")

    # Summary
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    if all_match:
        logger.info("ALL TABLES MATCH - Iceberg schema = Parquet schema")
    else:
        logger.error("SOME MISMATCHES FOUND - Review above")

    # Row counts (dynamically read from Parquet files)
    logger.info("")
    logger.info("Row counts in Parquet files:")
    for table_name, count in row_counts.items():
        if count is not None:
            logger.info(f"  {table_name}: {count:,} rows")
        else:
            logger.warning(f"  {table_name}: file not found")


if __name__ == "__main__":
    main()
