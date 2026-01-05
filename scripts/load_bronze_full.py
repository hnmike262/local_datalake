# -*- coding: utf-8 -*-
"""
Load Bronze parquet files into Iceberg tables via Trino.

Creates tables with schema matching parquet files, then loads data.

Data Pipeline Overview:
    This script is part of a larger data pipeline for League of Legends analytics:

    1. EXTRACTION (Riot API) - See data/extraction/ for source scripts:
       - data/extraction/01_ladder.py: Fetches ranked ladder data
       - data/extraction/02_matchids.py: Collects match IDs for players
       - data/extraction/03_matches.py: Retrieves detailed match data
       - data/extraction/riot_client.py: Riot API client with rate limiting
       - data/extraction/config.py: Centralized API configuration

    2. LOADING (This script):
       - Reads Bronze parquet files produced by extraction scripts
       - Creates Iceberg tables in Trino with matching schemas
       - Loads data via batch INSERT statements

    3. TRANSFORMATION (Downstream):
       - Silver/Gold layer transformations in Trino/dbt

Configuration:
    This script uses environment variables for Trino connection settings.
    For Riot API extraction configuration, see data/extraction/config.py
    which provides a centralized configuration pattern with validation.

Riot API Configuration Pattern:
    =============================
    This script does NOT directly use the Riot API - it only loads data from
    parquet files that were produced by the extraction scripts. However, for
    reference, here is how Riot API configuration is handled in the extraction
    layer:

    Configuration Location:
        - data/extraction/config.py: Centralized configuration module

    Configuration Method:
        - Environment Variable: RIOT_API_KEY
        - The extraction scripts use environment variables (NOT hardcoded keys)
        - Config is loaded via os.getenv() with validation

    Related Extraction Scripts:
        - 01_ladder.py: Uses config.py -> RiotConfig.api_key
        - 02_matchids.py: Uses config.py -> RiotConfig.api_key
        - 03_matches.py: Uses config.py -> RiotConfig.api_key
        - riot_client.py: Receives API key from config, handles rate limiting

    Security Note:
        The Riot API key should NEVER be hardcoded. Always use environment
        variables or a secure secrets manager. See data/extraction/config.py
        for the recommended configuration pattern.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from trino.dbapi import connect
from trino.dbapi import Connection, Cursor

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# =============================================================================
# TRINO CONNECTION CONFIGURATION (via environment variables)
# =============================================================================
TRINO_HOST: str = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT: int = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER: str = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG: str = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA: str = os.getenv("TRINO_SCHEMA", "bronze")

# =============================================================================
# TABLE PATHS CONFIGURATION
# =============================================================================
# These parquet files are produced by the Riot API extraction scripts in
# data/extraction/. The extraction pipeline fetches data from the Riot API
# and saves it to the data/bronze/ directory in parquet format.
#
# Source extraction scripts:
#   - ladder.parquet       <- data/extraction/01_ladder.py
#   - match_ids.parquet    <- data/extraction/02_matchids.py
#   - matches_*.parquet    <- data/extraction/03_matches.py
BRONZE_TABLE_PATHS: Dict[str, str] = {
    "ladder": "data/bronze/ladder.parquet",
    "match_ids": "data/bronze/match_ids.parquet",
    "matches_participants": "data/bronze/matches_participants.parquet",
    "matches_teams": "data/bronze/matches_teams.parquet",
}

# =============================================================================
# BATCH CONFIGURATION
# =============================================================================
BATCH_SIZE: int = int(os.getenv("BRONZE_BATCH_SIZE", "50"))
LOG_INTERVAL: int = int(os.getenv("BRONZE_LOG_INTERVAL", "500"))


def get_trino_type(arrow_type: pa.DataType) -> str:
    """Convert PyArrow type to Trino SQL type.

    Args:
        arrow_type: The PyArrow data type to convert.

    Returns:
        The corresponding Trino SQL type as a string.
    """
    type_str: str = str(arrow_type)
    if "int64" in type_str or "long" in type_str:
        return "BIGINT"
    elif "int32" in type_str or "int" in type_str:
        return "INTEGER"
    elif "float" in type_str or "double" in type_str:
        return "DOUBLE"
    elif "bool" in type_str:
        return "BOOLEAN"
    elif "timestamp" in type_str:
        return "TIMESTAMP(6)"
    else:
        return "VARCHAR"


def create_table_ddl(table_name: str, parquet_path: str) -> Tuple[str, pd.DataFrame]:
    """Generate CREATE TABLE DDL from parquet schema.

    Args:
        table_name: Name of the table to create.
        parquet_path: Path to the parquet file to read schema from.

    Returns:
        A tuple containing the DDL statement and the DataFrame with data.
    """
    pq_file: pa.Table = pq.read_table(parquet_path)
    schema: pa.Schema = pq_file.schema

    columns: List[str] = []
    for field in schema:
        col_name: str = field.name
        col_type: str = get_trino_type(field.type)
        columns.append(f'    "{col_name}" {col_type}')

    cols_str: str = ",\n".join(columns)
    ddl: str = f"CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name} (\n{cols_str}\n)"
    return ddl, pq_file.to_pandas()


def format_value(val: Any, col_type: Optional[str] = None) -> str:
    """Format value for SQL INSERT.

    Args:
        val: The value to format.
        col_type: Optional column type hint (currently unused).

    Returns:
        The formatted SQL value as a string.
    """
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    elif isinstance(val, str):
        escaped: str = val.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(val, pd.Timestamp):
        return f"TIMESTAMP '{val}'"
    else:
        return str(val)


def get_trino_connection() -> Connection:
    """Create and return a Trino database connection.

    Returns:
        A Trino connection object.
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
    cursor: Cursor,
    table_name: str,
    parquet_path: str,
) -> int:
    """Load a single table from parquet file into Iceberg.

    Args:
        cursor: The Trino cursor to execute queries.
        table_name: Name of the target table.
        parquet_path: Path to the source parquet file.

    Returns:
        The number of rows successfully inserted.
    """
    logger.info(f"Processing table: {table_name}")
    logger.info(f"Source: {parquet_path}")

    # Create table from parquet schema
    ddl, df = create_table_ddl(table_name, parquet_path)
    logger.info(f"Schema has {len(df.columns)} columns, {len(df)} rows")

    # Drop existing table
    full_table_name: str = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"
    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    logger.debug(f"Dropped existing table: {full_table_name}")

    # Create new table
    cursor.execute(ddl)
    logger.info(f"Created table with parquet schema")

    # Insert data in batches
    inserted: int = 0

    # Get column names from dataframe
    cols: List[str] = list(df.columns)
    cols_sql: str = ", ".join(f'"{c}"' for c in cols)

    for i in range(0, len(df), BATCH_SIZE):
        batch: pd.DataFrame = df.iloc[i : i + BATCH_SIZE]
        values_list: List[str] = []

        for _, row in batch.iterrows():
            row_vals: List[str] = []
            for col in cols:
                val: Any = row[col]
                row_vals.append(format_value(val))
            values_list.append(f"({', '.join(row_vals)})")

        if values_list:
            sql: str = (
                f"INSERT INTO {full_table_name} ({cols_sql}) "
                f"VALUES {', '.join(values_list)}"
            )
            try:
                cursor.execute(sql)
                inserted += len(batch)
                if inserted % LOG_INTERVAL == 0 or inserted == len(df):
                    logger.info(f"  Inserted {inserted}/{len(df)} rows")
            except Exception as e:
                logger.error(f"  Batch error at {i}: {str(e)[:80]}...")

    logger.info(f"Loaded {inserted}/{len(df)} rows into {TRINO_SCHEMA}.{table_name}")
    return inserted


def verify_tables(cursor: Cursor, table_names: List[str]) -> None:
    """Verify row counts for loaded tables.

    Args:
        cursor: The Trino cursor to execute queries.
        table_names: List of table names to verify.
    """
    logger.info("=" * 50)
    logger.info("=== Verification ===")
    logger.info("=" * 50)

    for table_name in table_names:
        full_table_name: str = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            result = cursor.fetchone()
            count: int = result[0] if result else 0
            logger.info(f"{full_table_name}: {count} rows")
        except Exception as e:
            logger.error(f"{full_table_name}: ERROR - {e}")


def main() -> None:
    """Main entry point for loading Bronze data into Iceberg via Trino."""
    logger.info("=== Loading Bronze data into Iceberg via Trino ===")

    conn: Connection = get_trino_connection()
    cursor: Cursor = conn.cursor()

    try:
        # Test connection
        cursor.execute("SELECT 1")
        logger.info("Connected to Trino")

        # Ensure schema exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
        logger.info(f"Schema {TRINO_SCHEMA} ready")

        # Process each table
        for table_name, path in BRONZE_TABLE_PATHS.items():
            logger.info("")
            logger.info("=" * 50)
            try:
                load_table(cursor, table_name, path)
            except Exception as e:
                logger.error(f"ERROR with {table_name}: {e}")

        # Verify loaded data
        verify_tables(cursor, list(BRONZE_TABLE_PATHS.keys()))

    finally:
        cursor.close()
        conn.close()
        logger.info("Done!")


if __name__ == "__main__":
    main()
