# -*- coding: utf-8 -*-
"""
Load Bronze parquet files into Iceberg tables via Trino INSERT.

Using trino-python-client to load parquet data extracted from the Riot API
into Iceberg tables for downstream analytics.

Data Pipeline Context:
    This script is part of the League of Legends data pipeline. The upstream
    data extraction is handled by scripts in `data/extraction/`:
        - 01_ladder.py: Extracts ranked ladder data from Riot API
        - 02_matchids.py: Extracts match IDs for players on the ladder
        - 03_matches.py: Extracts detailed match data including participants/teams
        - riot_client.py: Riot API client with rate limiting
        - config.py: Centralized configuration for API keys, regions, and paths

    Those scripts output parquet files to `data/bronze/`, which this script
    loads into Iceberg tables via Trino for querying and further transformation.

Riot API Configuration Analysis:
    ===============================
    This script does NOT directly use the Riot API. It consumes data that was
    previously extracted by the upstream extraction scripts.

    The Riot API configuration is managed in `data/extraction/config.py`:
        - RIOT_API_KEY: Loaded from environment variable `RIOT_API_KEY`
        - Configuration pattern: Environment variable with no default
        - The API key is required for extraction scripts (01_*, 02_*, 03_*)
        - This loader script only needs Trino access, not Riot API access

    Data Flow:
        1. Riot API (requires RIOT_API_KEY env var)
           ↓
        2. Extraction scripts (data/extraction/01_*, 02_*, 03_*.py)
           ↓
        3. Bronze parquet files (data/bronze/*.parquet)
           ↓
        4. This script (load_bronze_trino.py) - loads into Iceberg
           ↓
        5. Iceberg tables (iceberg.bronze.*)

    Configuration Files Referenced:
        - data/extraction/config.py: RIOT_API_KEY (env var), REGION, paths
        - data/extraction/riot_client.py: Uses config.py for API key
        - This file: Uses TRINO_* env vars for database connection

Environment Variables:
    TRINO_HOST: Trino coordinator hostname (default: 'localhost')
    TRINO_PORT: Trino coordinator port (default: 8082)
    TRINO_USER: Trino username (default: 'admin')
    TRINO_CATALOG: Target catalog (default: 'iceberg')
    TRINO_SCHEMA: Target schema (default: 'bronze')

See Also:
    - data/extraction/config.py for Riot API and MinIO configuration patterns
    - docs/services/riot-api.md for full API documentation
"""

import logging
import os
from typing import Any, Dict, List

import pandas as pd
from trino.dbapi import connect
from trino.dbapi import Connection, Cursor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Trino connection configuration from environment variables
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "bronze")


def format_sql_value(val: Any) -> str:
    """
    Format a Python value for SQL insertion.

    Handles NULL values, strings (with quote escaping), booleans,
    timestamps, and other types.

    Args:
        val: The value to format for SQL

    Returns:
        A string representation suitable for SQL INSERT statements
    """
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, str):
        # Escape single quotes
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    elif isinstance(val, pd.Timestamp):
        return f"TIMESTAMP '{val}'"
    else:
        return str(val)


def format_row_values(row: pd.Series) -> List[str]:
    """
    Format all values in a DataFrame row for SQL insertion.

    Args:
        row: A pandas Series representing a single row

    Returns:
        A list of formatted SQL value strings
    """
    return [format_sql_value(val) for val in row.values]


def get_trino_connection() -> Connection:
    """
    Create and return a Trino database connection using configured parameters.

    Returns:
        A Trino database connection
    """
    logger.info(
        f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT} "
        f"(catalog={TRINO_CATALOG}, schema={TRINO_SCHEMA})"
    )
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def load_table(
    cursor: Cursor, table_name: str, path: str, batch_size: int = 100
) -> int:
    """
    Load a parquet file into a Trino/Iceberg table.

    Args:
        cursor: Trino database cursor
        table_name: Name of the target table
        path: Path to the parquet file
        batch_size: Number of rows to insert per batch

    Returns:
        Number of rows successfully inserted
    """
    logger.info(f"Processing {table_name} from {path}")

    df = pd.read_parquet(path)
    logger.info(f"Read {len(df)} rows, {len(df.columns)} columns")

    # Truncate existing data
    cursor.execute(f"DELETE FROM iceberg.bronze.{table_name}")
    logger.info("Cleared existing data")

    # Insert in batches
    inserted = 0
    cols = ", ".join(f'"{c}"' for c in df.columns)

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        values = []

        for _, row in batch.iterrows():
            row_vals = format_row_values(row)
            values.append(f"({', '.join(row_vals)})")

        sql = f"INSERT INTO iceberg.bronze.{table_name} ({cols}) VALUES {', '.join(values)}"

        try:
            cursor.execute(sql)
            inserted += len(batch)
        except Exception as e:
            logger.warning(f"Batch error at row {i}: {str(e)[:100]}")
            # Try row by row as fallback
            for _, row in batch.iterrows():
                try:
                    row_vals = format_row_values(row)
                    sql = f"INSERT INTO iceberg.bronze.{table_name} ({cols}) VALUES ({', '.join(row_vals)})"
                    cursor.execute(sql)
                    inserted += 1
                except Exception as e2:
                    logger.debug(f"Skipping problem row: {str(e2)[:50]}")

    logger.info(f"Inserted {inserted}/{len(df)} rows into {table_name}")
    return inserted


def verify_tables(cursor: Cursor, table_names: List[str]) -> Dict[str, int]:
    """
    Verify row counts for loaded tables.

    Args:
        cursor: Trino database cursor
        table_names: List of table names to verify

    Returns:
        Dictionary mapping table names to row counts
    """
    logger.info("Verifying table row counts")
    counts: Dict[str, int] = {}

    for table_name in table_names:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.bronze.{table_name}")
        count = cursor.fetchone()[0]
        counts[table_name] = count
        logger.info(f"iceberg.bronze.{table_name}: {count} rows")

    return counts


def main() -> None:
    """Main entry point for loading Bronze data into Iceberg via Trino."""
    logger.info("=== Loading Bronze data into Iceberg via Trino ===")

    conn = get_trino_connection()
    cursor = conn.cursor()

    # Test connection
    cursor.execute("SELECT 1")
    logger.info("Connected to Trino successfully")

    # Load parquet files
    files: Dict[str, str] = {
        "ladder": "data/bronze/ladder.parquet",
        "match_ids": "data/bronze/match_ids.parquet",
        "matches_participants": "data/bronze/matches_participants.parquet",
        "matches_teams": "data/bronze/matches_teams.parquet",
    }

    for table_name, path in files.items():
        try:
            load_table(cursor, table_name, path)
        except Exception as e:
            logger.error(f"ERROR with {table_name}: {e}")

    # Verify
    verify_tables(cursor, list(files.keys()))

    cursor.close()
    conn.close()
    logger.info("Done!")


if __name__ == "__main__":
    main()
