"""Тесты DataAnalyzer."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from domain.models import AnalysisResult, AggregatedWindow, PageStat
from usecases.analyzer import DataAnalyzer

_START = datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
_END = datetime(2024, 1, 1, 14, 10, 0, tzinfo=timezone.utc)


def _make_window(total_events: int = 10) -> AggregatedWindow:
    return AggregatedWindow(
        window_start=_START,
        window_end=_END,
        total_events=total_events,
        event_counts={"click": total_events},
        unique_users=3,
        avg_duration_ms=100.0,
        top_pages=[PageStat(page="/", count=total_events)],
    )


class TestDataAnalyzer:
    """Тесты класса DataAnalyzer."""

    def setup_method(self) -> None:
        """Инициализация перед каждым тестом."""
        self.mock_duckdb = MagicMock()
        self.analyzer = DataAnalyzer(self.mock_duckdb)

    def test_benchmark_returns_all_keys(self) -> None:
        """Проверяет что benchmark возвращает polars_ms, duckdb_ms, speedup."""
        self.mock_duckdb.compare_with_polars.return_value = {
            "duckdb_time_ms": 50.0,
            "polars_time_ms": 100.0,
        }
        result = self.analyzer.benchmark_polars_vs_duckdb("dummy.parquet")
        assert "polars_ms" in result
        assert "duckdb_ms" in result
        assert "speedup" in result
        assert result["speedup"] == pytest.approx(2.0)

    def test_analyze_returns_analysis_result(self) -> None:
        """Проверяет что analyze возвращает AnalysisResult с корректными полями."""
        self.mock_duckdb.analyze_parquet.side_effect = RuntimeError("no parquet")
        windows = [_make_window(total_events=10), _make_window(total_events=20)]
        result = self.analyzer.analyze(windows, "dummy.parquet")
        assert isinstance(result, AnalysisResult)
        assert result.total_windows == 2
        assert result.total_events == 30
        assert result.peak_window is not None
        assert result.peak_window.total_events == 20
