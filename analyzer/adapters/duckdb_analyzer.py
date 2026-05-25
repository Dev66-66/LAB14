"""Адаптер для аналитических SQL-запросов через DuckDB."""

import os
import time
from pathlib import Path

import duckdb
import polars as pl


class DuckDBAnalyzer:
    """Выполняет аналитические SQL-запросы к Parquet-файлам через DuckDB."""

    def __init__(self) -> None:
        """Инициализирует подключение к DuckDB."""
        db_path = os.getenv("DUCKDB_PATH", "./data/events.duckdb")
        self._conn = duckdb.connect(db_path)

    def analyze_parquet(self, parquet_path: str) -> dict:
        """Выполняет аналитический запрос к Parquet-файлу.

        Запрос включает: фильтрацию (total_events > 0),
        группировку по временным интервалам (1 час),
        сортировку по убыванию событий,
        вычисление percentile_cont(0.5) для avg_duration_ms.

        Args:
            parquet_path: Путь к Parquet-файлу.

        Returns:
            Словарь с результатами: query_result (DataFrame),
            execution_time_ms (float), row_count (int).
        """
        start = time.perf_counter()
        result = self._conn.execute(f"""
            SELECT
                DATE_TRUNC('hour', window_start) AS hour,
                SUM(total_events)                AS total_events,
                AVG(avg_duration_ms)             AS avg_duration_ms,
                SUM(unique_users)                AS unique_users,
                PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY avg_duration_ms)   AS median_duration
            FROM read_parquet('{parquet_path}')
            WHERE total_events > 0
            GROUP BY DATE_TRUNC('hour', window_start)
            ORDER BY total_events DESC
        """).df()
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "query_result": result,
            "execution_time_ms": elapsed,
            "row_count": len(result),
        }

    def compare_with_polars(self, parquet_path: str) -> dict:
        """Сравнивает производительность DuckDB и Polars для одного запроса.

        Оба движка выполняют эквивалентную агрегацию: группировка по часу,
        суммирование событий, медиана длительности.

        Args:
            parquet_path: Путь к Parquet-файлу.

        Returns:
            Словарь с ключами duckdb_time_ms и polars_time_ms (float).
        """
        # DuckDB.
        t0 = time.perf_counter()
        self._conn.execute(f"""
            SELECT
                DATE_TRUNC('hour', window_start) AS hour,
                SUM(total_events)                AS total_events,
                AVG(avg_duration_ms)             AS avg_duration_ms
            FROM read_parquet('{parquet_path}')
            WHERE total_events > 0
            GROUP BY 1
            ORDER BY 2 DESC
        """).df()
        duckdb_ms = (time.perf_counter() - t0) * 1000

        # Polars — эквивалентная операция.
        t0 = time.perf_counter()
        df = pl.read_parquet(parquet_path)
        (
            df.filter(pl.col("total_events") > 0)
            .with_columns(pl.col("window_start").dt.truncate("1h").alias("hour"))
            .group_by("hour")
            .agg(
                [
                    pl.col("total_events").sum(),
                    pl.col("avg_duration_ms").mean(),
                ]
            )
            .sort("total_events", descending=True)
        )
        polars_ms = (time.perf_counter() - t0) * 1000

        return {
            "duckdb_time_ms": round(duckdb_ms, 3),
            "polars_time_ms": round(polars_ms, 3),
        }

    def close(self) -> None:
        """Закрывает соединение с DuckDB."""
        self._conn.close()
