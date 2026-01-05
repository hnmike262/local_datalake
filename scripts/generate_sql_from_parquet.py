# -*- coding: utf-8 -*-
"""
Generate Trino SQL from Parquet schema
Auto-infer schema from parquet files → Generate CREATE TABLE statements

Usage:
    python scripts/generate_sql_from_parquet.py
    python scripts/generate_sql_from_parquet.py --data-dir /path/to/data --output /path/to/output.sql

Output:
    - Logs SQL for Hive external tables
    - Logs SQL for Iceberg CTAS
    - Saves to scripts/generated_sql.sql (or custom output path)

=== RIOT API EXTRACTION PIPELINE RELATIONSHIP ===
This script is part of the data pipeline that processes Riot API data.

Data Flow:
    1. Riot API → Extraction Scripts → Parquet Files → This Script → SQL Tables

Related Files:
    - data/extraction/config.py: Central Riot API configuration
        - RIOT_API_KEY: Environment variable (required)
        - RIOT_PLATFORM: Platform routing (default: 'vn2')
        - RIOT_REGION: Regional routing (default: 'sea')
        - BRONZE_DIR: Output directory for parquet files
    
    - data/extraction/01_ladder.py: Extracts ladder/ranking data → ladder.parquet
    - data/extraction/02_matchids.py: Extracts match IDs → match_ids.parquet
    - data/extraction/03_matches.py: Extracts match details → matches_participants.parquet, matches_teams.parquet
    
    - data/extraction/riot_client.py: HTTP client for Riot API
    - data/extraction/minio_upload.py: Uploads parquet to MinIO

Parquet Files Processed:
    - data/bronze/ladder.parquet
    - data/bronze/match_ids.parquet
    - data/bronze/matches_participants.parquet
    - data/bronze/matches_teams.parquet

Note: This script does NOT directly use Riot API configuration.
      It only reads the parquet files produced by the extraction pipeline.
      Riot API config is centralized in data/extraction/config.py.
=====================================================
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow.parquet as pq

from schema_utils import ARROW_TO_TRINO, arrow_type_to_trino

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_DATA_DIR = Path("data/bronze")
DEFAULT_OUTPUT_PATH = Path("scripts/generated_sql.sql")
DEFAULT_S3_BUCKET = "lol-bronze"


def get_table_config(data_dir: Path, s3_bucket: str) -> Dict[str, Dict[str, str]]:
    """
    Get table configuration with parquet paths and S3 locations.

    Args:
        data_dir: Base directory for parquet files
        s3_bucket: S3 bucket name for external locations

    Returns:
        Dictionary mapping table names to their configuration
    """
    return {
        "ladder": {
            "parquet": str(data_dir / "ladder.parquet"),
            "s3_location": f"s3a://{s3_bucket}/ladder/",
        },
        "match_ids": {
            "parquet": str(data_dir / "match_ids.parquet"),
            "s3_location": f"s3a://{s3_bucket}/match_ids/",
        },
        "matches_participants": {
            "parquet": str(data_dir / "matches_participants.parquet"),
            "s3_location": f"s3a://{s3_bucket}/matches_participants/",
        },
        "matches_teams": {
            "parquet": str(data_dir / "matches_teams.parquet"),
            "s3_location": f"s3a://{s3_bucket}/matches_teams/",
        },
    }


def generate_hive_create_table(table_name: str, schema: Any, s3_location: str) -> str:
    """
    Generate Hive external table CREATE statement.

    Args:
        table_name: Name of the table to create
        schema: PyArrow schema object
        s3_location: S3 location for external table data

    Returns:
        SQL CREATE TABLE statement as string
    """
    columns: List[str] = []
    for field in schema:
        trino_type = arrow_type_to_trino(field.type)
        columns.append(f"    {field.name} {trino_type}")

    columns_sql = ",\n".join(columns)

    return f"""-- Hive External Table: {table_name}
CREATE TABLE IF NOT EXISTS hive.lol_bronze.{table_name} (
{columns_sql}
) WITH (
    format = 'PARQUET',
    external_location = '{s3_location}'
);
"""


def generate_iceberg_ctas(table_name: str) -> str:
    """
    Generate Iceberg CTAS from Hive table.

    Args:
        table_name: Name of the table to create

    Returns:
        SQL CREATE TABLE AS SELECT statement as string
    """
    return f"""-- Iceberg CTAS: {table_name} (auto schema from Hive)
CREATE TABLE IF NOT EXISTS iceberg.bronze.{table_name} AS
SELECT * FROM hive.lol_bronze.{table_name};
"""


def generate_iceberg_create_table(table_name: str, schema: Any) -> str:
    """
    Generate Iceberg CREATE TABLE with explicit schema.

    Args:
        table_name: Name of the table to create
        schema: PyArrow schema object

    Returns:
        SQL CREATE TABLE statement as string
    """
    columns: List[str] = []
    for field in schema:
        trino_type = arrow_type_to_trino(field.type)
        columns.append(f"    {field.name} {trino_type}")

    columns_sql = ",\n".join(columns)

    return f"""-- Iceberg Table: {table_name}
CREATE TABLE IF NOT EXISTS iceberg.bronze.{table_name} (
{columns_sql}
) WITH (
    format = 'PARQUET'
);
"""


def generate_insert_into(table_name: str) -> str:
    """
    Generate INSERT INTO statement.

    Args:
        table_name: Name of the table

    Returns:
        SQL INSERT INTO statement as string
    """
    return f"""-- Copy data from Hive to Iceberg
INSERT INTO iceberg.bronze.{table_name}
SELECT * FROM hive.lol_bronze.{table_name};
"""


def process_tables(tables: Dict[str, Dict[str, str]]) -> tuple[List[str], int]:
    """
    Process all tables and generate SQL statements.

    Args:
        tables: Dictionary of table configurations

    Returns:
        Tuple of (list of SQL statements, count of processed tables)
    """
    all_sql: List[str] = []
    processed_count = 0

    # Header
    all_sql.append("-- ============================================")
    all_sql.append("-- AUTO-GENERATED SQL FROM PARQUET SCHEMA")
    all_sql.append("-- Generated by: scripts/generate_sql_from_parquet.py")
    all_sql.append("-- ============================================\n")

    # Create schemas
    all_sql.append("-- Create schemas")
    all_sql.append("CREATE SCHEMA IF NOT EXISTS hive.lol_bronze;")
    all_sql.append("CREATE SCHEMA IF NOT EXISTS iceberg.bronze;\n")

    # Process each table
    for table_name, config in tables.items():
        parquet_path = config["parquet"]
        s3_location = config["s3_location"]

        logger.info(f"Processing table: {table_name}")

        # Check if file exists
        if not Path(parquet_path).exists():
            logger.warning(f"Parquet file not found: {parquet_path}, skipping")
            continue

        # Read schema
        schema = pq.read_schema(parquet_path)
        logger.info(f"  Columns: {len(schema)}")

        # Log column info
        for field in schema:
            trino_type = arrow_type_to_trino(field.type)
            logger.debug(f"    {field.name}: {field.type} -> {trino_type}")

        # Generate SQL
        all_sql.append(f"\n-- ========== {table_name.upper()} ==========\n")

        # Option 1: Hive external + Iceberg CTAS (recommended)
        all_sql.append("-- OPTION 1: Hive External → Iceberg CTAS (auto schema)")
        all_sql.append(generate_hive_create_table(table_name, schema, s3_location))
        all_sql.append(generate_iceberg_ctas(table_name))

        # Option 2: Explicit Iceberg + INSERT
        all_sql.append("\n-- OPTION 2: Explicit Iceberg CREATE + INSERT")
        all_sql.append(generate_iceberg_create_table(table_name, schema))
        all_sql.append(generate_insert_into(table_name))

        processed_count += 1

    # Verification queries
    all_sql.append("\n-- ========== VERIFICATION ==========\n")
    for table_name in tables.keys():
        all_sql.append(
            f"SELECT '{table_name}' as table_name, COUNT(*) as row_count "
            f"FROM iceberg.bronze.{table_name};"
        )

    return all_sql, processed_count


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Trino SQL from Parquet schema"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing parquet files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output SQL file path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=DEFAULT_S3_BUCKET,
        help=f"S3 bucket name for external locations (default: {DEFAULT_S3_BUCKET})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose (debug) logging"
    )
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the SQL generator.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()

    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("GENERATE SQL FROM PARQUET SCHEMA")
    logger.info("=" * 60)
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Output file: {args.output}")
    logger.info(f"S3 bucket: {args.s3_bucket}")

    # Get table configuration
    tables = get_table_config(args.data_dir, args.s3_bucket)

    # Process tables
    all_sql, processed_count = process_tables(tables)

    if processed_count == 0:
        logger.error("No tables were processed. Check if parquet files exist.")
        return 1

    # Join all SQL
    full_sql = "\n".join(all_sql)

    # Ensure output directory exists
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Save to file
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(full_sql)

    logger.info("=" * 60)
    logger.info(f"SQL saved to: {args.output}")
    logger.info(f"Tables processed: {processed_count}")
    logger.info("=" * 60)

    # Log the SQL content at debug level
    logger.debug("\n" + full_sql)

    return 0


if __name__ == "__main__":
    sys.exit(main())
