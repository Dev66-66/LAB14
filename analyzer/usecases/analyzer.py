"""Use case: аналитическая обработка данных."""

import logging
from typing import Dict, List

from adapters.duckdb_analyzer import DuckDBAnalyzer
from domain.models import AggregatedWindow, AnalysisResult

logger = logging.getLogger(__name__)


class DataAnalyzer:
    """Выполняет аналитическую обработку агрегированных окон."""

    def __init__(self, duckdb_analyzer: DuckDBAnalyzer) -> None:
        """Args:
        duckdb_analyzer: Экземпляр DuckDB-адаптера.
        """
        self._duckdb = duckdb_analyzer

    def analyze(
        self,
        windows: List[AggregatedWindow],
        parquet_path: str,
    ) -> AnalysisResult:
        """Выполняет полный анализ: агрегаты + DuckDB SQL + бенчмарк.

        Args:
            windows: Список агрегированных окон.
            parquet_path: Путь к Parquet для DuckDB-запроса.

        Returns:
            AnalysisResult с метриками и peak_window.
        """
        if not windows:
            return AnalysisResult(
                total_windows=0,
                total_events=0,
                avg_events_per_window=0.0,
                peak_window=None,
                event_distribution={},
            )

        total_windows = len(windows)
        total_events = sum(w.total_events for w in windows)
        avg_events_per_window = total_events / total_windows

        peak_window = max(windows, key=lambda w: w.total_events)

        # Суммируем event_counts по всем окнам.
        event_distribution: Dict[str, int] = {}
        for w in windows:
            for event_type, count in w.event_counts.items():
                event_distribution[event_type] = (
                    event_distribution.get(event_type, 0) + count
                )

        # DuckDB SQL-анализ (не прерывает пайплайн при ошибке).
        try:
            duckdb_result = self._duckdb.analyze_parquet(parquet_path)
            logger.info(
                "DuckDB analysis complete: %d rows in %.2f ms",
                duckdb_result["row_count"],
                duckdb_result["execution_time_ms"],
            )
        except Exception as exc:
            logger.warning("DuckDB analysis failed: %s", exc)

        return AnalysisResult(
            total_windows=total_windows,
            total_events=total_events,
            avg_events_per_window=avg_events_per_window,
            peak_window=peak_window,
            event_distribution=event_distribution,
        )

    def benchmark_polars_vs_duckdb(self, parquet_path: str) -> Dict[str, float]:
        """Сравнивает время выполнения Polars и DuckDB для одного запроса.

        Оба инструмента выполняют идентичную агрегацию по часам.
        Результаты логируются.

        Args:
            parquet_path: Путь к Parquet-файлу.

        Returns:
            Словарь {"polars_ms": float, "duckdb_ms": float, "speedup": float}.
        """
        result = self._duckdb.compare_with_polars(parquet_path)
        duckdb_ms: float = result["duckdb_time_ms"]
        polars_ms: float = result["polars_time_ms"]
        speedup: float = round(polars_ms / duckdb_ms, 2) if duckdb_ms > 0 else 0.0

        logger.info(
            "Benchmark: DuckDB=%.2f ms, Polars=%.2f ms, speedup=%.2fx",
            duckdb_ms,
            polars_ms,
            speedup,
        )

        return {
            "polars_ms": polars_ms,
            "duckdb_ms": duckdb_ms,
            "speedup": speedup,
        }
