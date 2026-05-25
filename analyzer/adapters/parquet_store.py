"""Адаптер для сохранения и загрузки данных в формате Parquet."""

import os
from pathlib import Path
from typing import List

import polars as pl

from domain.models import AggregatedWindow, PageStat


class ParquetStore:
    """Управляет сохранением и загрузкой агрегированных окон в Parquet."""

    def __init__(self) -> None:
        """Инициализирует хранилище с директорией из переменных окружения."""
        self._output_dir = Path(os.getenv("PARQUET_OUTPUT_DIR", "./data"))
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def save_windows(self, windows: List[AggregatedWindow], filename: str) -> Path:
        """Сохраняет окна в Parquet-файл через Polars DataFrame.

        Args:
            windows: Список агрегированных окон для сохранения.
            filename: Имя файла без расширения.

        Returns:
            Путь к созданному файлу.
        """
        df = self._windows_to_dataframe(windows)
        path = self._output_dir / f"{filename}.parquet"
        df.write_parquet(path)
        return path

    def load_windows(self, filename: str) -> pl.DataFrame:
        """Загружает окна из Parquet-файла.

        Args:
            filename: Имя файла без расширения.

        Returns:
            Polars DataFrame с данными окон.
        """
        path = self._output_dir / f"{filename}.parquet"
        return pl.read_parquet(path)

    def _windows_to_dataframe(self, windows: List[AggregatedWindow]) -> pl.DataFrame:
        """Конвертирует список окон в Polars DataFrame.

        Сериализует top_pages в JSON-строку для хранения в Parquet.

        Args:
            windows: Список AggregatedWindow для конвертации.

        Returns:
            Polars DataFrame с плоской схемой.
        """
        import json

        rows = []
        for w in windows:
            top_pages_json = json.dumps(
                [{"page": p.page, "count": p.count} for p in w.top_pages]
            )
            rows.append(
                {
                    "window_start": w.window_start,
                    "window_end": w.window_end,
                    "total_events": w.total_events,
                    "unique_users": w.unique_users,
                    "avg_duration_ms": w.avg_duration_ms,
                    "top_pages": top_pages_json,
                }
            )

        if not rows:
            return pl.DataFrame(
                schema={
                    "window_start": pl.Datetime("ms", "UTC"),
                    "window_end": pl.Datetime("ms", "UTC"),
                    "total_events": pl.Int64,
                    "unique_users": pl.Int64,
                    "avg_duration_ms": pl.Float64,
                    "top_pages": pl.Utf8,
                }
            )

        return pl.DataFrame(rows).with_columns(
            [
                pl.col("window_start").cast(pl.Datetime("ms", "UTC")),
                pl.col("window_end").cast(pl.Datetime("ms", "UTC")),
            ]
        )

    def dataframe_to_windows(self, df: pl.DataFrame) -> List[AggregatedWindow]:
        """Восстанавливает список AggregatedWindow из Polars DataFrame.

        Args:
            df: Polars DataFrame, ранее созданный через _windows_to_dataframe.

        Returns:
            Список AggregatedWindow.
        """
        import json

        windows: List[AggregatedWindow] = []
        for row in df.iter_rows(named=True):
            top_pages_raw = json.loads(row["top_pages"]) if row["top_pages"] else []
            top_pages = [PageStat(page=p["page"], count=p["count"]) for p in top_pages_raw]
            windows.append(
                AggregatedWindow(
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    total_events=row["total_events"],
                    event_counts={},
                    unique_users=row["unique_users"],
                    avg_duration_ms=row["avg_duration_ms"],
                    top_pages=top_pages,
                )
            )
        return windows
