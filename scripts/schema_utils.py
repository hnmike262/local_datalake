# -*- coding: utf-8 -*-
"""
Shared utilities for schema conversion between Arrow/Parquet and Trino/Iceberg.

This module consolidates type mapping logic used across multiple scripts:
- verify_schema.py
- generate_sql_from_parquet.py
"""

from typing import Dict

# Arrow type -> Trino/Iceberg type mapping
# Consolidated from verify_schema.py and generate_sql_from_parquet.py
ARROW_TO_TRINO: Dict[str, str] = {
    "string": "varchar",
    "large_string": "varchar",
    "int64": "bigint",
    "int32": "integer",
    "int16": "smallint",
    "int8": "tinyint",
    "float": "real",
    "double": "double",
    "bool": "boolean",
    "date32[day]": "date",
    "timestamp[us]": "timestamp(6)",
    "timestamp[ms]": "timestamp(3)",
    "timestamp[ns]": "timestamp(9)",
    "timestamp[us, tz=UTC]": "timestamp(6) with time zone",
}


def arrow_type_to_trino(arrow_type: str, uppercase: bool = False) -> str:
    """
    Convert Arrow type string to Trino type.

    Args:
        arrow_type: The Arrow type string (e.g., 'int64', 'string')
        uppercase: If True, return uppercase type names (e.g., 'BIGINT' vs 'bigint')

    Returns:
        The corresponding Trino type string, defaults to 'varchar' if unknown.
    """
    type_str = str(arrow_type)
    trino_type = ARROW_TO_TRINO.get(type_str, "varchar")
    return trino_type.upper() if uppercase else trino_type
