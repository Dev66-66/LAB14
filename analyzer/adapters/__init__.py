"""Адаптерный слой анализатора."""

from .arrow_client import ArrowFlightClient
from .duckdb_analyzer import DuckDBAnalyzer
from .parquet_store import ParquetStore

__all__ = [
    "ArrowFlightClient",
    "DuckDBAnalyzer",
    "ParquetStore",
]
