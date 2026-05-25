"""Адаптерный слой анализатора."""

from analyzer.adapters.arrow_client import ArrowFlightClient
from analyzer.adapters.duckdb_analyzer import DuckDBAnalyzer
from analyzer.adapters.parquet_store import ParquetStore

__all__ = [
    "ArrowFlightClient",
    "DuckDBAnalyzer",
    "ParquetStore",
]
