"""Use case: трансформация и очистка данных через Polars."""

import json
import logging
from typing import List, Tuple

import polars as pl

from domain.models import AggregatedWindow

logger = logging.getLogger(__name__)


class DataTransformer:
    """Выполняет трансформацию и очистку агрегированных окон."""

    def __init__(self) -> None:
        """Инициализирует трансформер и сбрасывает лог шагов очистки."""
        self._cleaning_log: List[Tuple[str, int, int]] = []

    def transform(self, windows: List[AggregatedWindow]) -> pl.DataFrame:
        """Конвертирует окна в очищенный Polars DataFrame.

        Шаги очистки:
        1. Конвертация в DataFrame
        2. Удаление дубликатов по (window_start, window_end)
        3. Фильтрация: total_events > 0
        4. Приведение типов: window_start/end → Datetime[ms, UTC]
        5. Добавление колонки window_duration_sec
        6. Добавление колонки events_per_second = total_events / window_duration_sec

        Args:
            windows: Список агрегированных окон.

        Returns:
            Очищенный Polars DataFrame.
        """
        self._cleaning_log = []

        rows = [
            {
                "window_start": w.window_start,
                "window_end": w.window_end,
                "total_events": w.total_events,
                "unique_users": w.unique_users,
                "avg_duration_ms": w.avg_duration_ms,
                "event_counts": json.dumps(w.event_counts),
            }
            for w in windows
        ]

        # Шаг 1: конвертация в DataFrame.
        df = (
            pl.DataFrame(rows)
            if rows
            else pl.DataFrame(
                schema={
                    "window_start": pl.Datetime("ms", "UTC"),
                    "window_end": pl.Datetime("ms", "UTC"),
                    "total_events": pl.Int64,
                    "unique_users": pl.Int64,
                    "avg_duration_ms": pl.Float64,
                    "event_counts": pl.Utf8,
                }
            )
        )
        before = len(df)
        self._cleaning_log.append(("Конвертация в DataFrame", before, before))
        logger.debug("Step 1: %d rows loaded", before)

        # Шаг 2: удаление дубликатов по (window_start, window_end).
        df = df.unique(subset=["window_start", "window_end"], keep="first")
        after = len(df)
        self._cleaning_log.append(("Удаление дубликатов", before, after))
        logger.debug("Step 2: dedup %d → %d", before, after)
        before = after

        # Шаг 3: фильтрация total_events > 0.
        df = df.filter(pl.col("total_events") > 0)
        after = len(df)
        self._cleaning_log.append(("Фильтрация total_events > 0", before, after))
        logger.debug("Step 3: filter %d → %d", before, after)
        before = after

        # Шаг 4: приведение типов datetime.
        df = df.with_columns(
            [
                pl.col("window_start").cast(pl.Datetime("ms", "UTC")),
                pl.col("window_end").cast(pl.Datetime("ms", "UTC")),
            ]
        )
        self._cleaning_log.append(
            ("Приведение типов datetime[ms, UTC]", before, before)
        )

        # Шаг 5: window_duration_sec через разность Int64-представлений (ms → s).
        df = df.with_columns(
            [
                (
                    (
                        pl.col("window_end").cast(pl.Int64)
                        - pl.col("window_start").cast(pl.Int64)
                    )
                    / 1_000
                ).alias("window_duration_sec")
            ]
        )
        self._cleaning_log.append(("Добавление window_duration_sec", before, before))

        # Шаг 6: events_per_second; защита от деления на ноль.
        df = df.with_columns(
            [
                pl.when(pl.col("window_duration_sec") > 0)
                .then(
                    pl.col("total_events").cast(pl.Float64)
                    / pl.col("window_duration_sec")
                )
                .otherwise(pl.lit(0.0))
                .alias("events_per_second")
            ]
        )
        self._cleaning_log.append(("Добавление events_per_second", before, before))

        return df

    def aggregate_by_hour(self, df: pl.DataFrame) -> pl.DataFrame:
        """Группирует данные по часам.

        Вычисляет: SUM total_events, AVG avg_duration_ms,
        SUM unique_users, COUNT окон.

        Args:
            df: Очищенный DataFrame от transform().

        Returns:
            DataFrame с почасовой агрегацией, отсортированный по часу.
        """
        return (
            df.with_columns(pl.col("window_start").dt.truncate("1h").alias("hour"))
            .group_by("hour")
            .agg(
                [
                    pl.col("total_events").sum(),
                    pl.col("avg_duration_ms").mean(),
                    pl.col("unique_users").sum(),
                    pl.len().alias("window_count"),
                ]
            )
            .sort("hour")
        )

    def get_event_distribution(self, df: pl.DataFrame) -> pl.DataFrame:
        """Возвращает распределение событий по типам из колонки event_counts.

        Args:
            df: DataFrame с колонкой event_counts (JSON-строка).

        Returns:
            DataFrame с колонками event_type и total_count.
        """
        distribution: dict[str, int] = {}
        for raw in df["event_counts"].to_list():
            if not raw:
                continue
            counts: dict = json.loads(raw)
            for et, cnt in counts.items():
                distribution[et] = distribution.get(et, 0) + int(cnt)

        if not distribution:
            return pl.DataFrame({"event_type": [], "total_count": []})

        return pl.DataFrame(
            {
                "event_type": list(distribution.keys()),
                "total_count": list(distribution.values()),
            }
        ).sort("total_count", descending=True)

    def document_cleaning_steps(self, df: pl.DataFrame) -> List[str]:
        """Возвращает список описаний шагов очистки с количеством строк.

        Использует лог, сохранённый во время последнего вызова transform().

        Args:
            df: Итоговый DataFrame (используется для подтверждения финального счёта).

        Returns:
            Список строк вида "Шаг N: описание (было X строк → стало Y строк)".
        """
        steps: List[str] = []
        for i, (description, before, after) in enumerate(self._cleaning_log, start=1):
            steps.append(
                f"Шаг {i}: {description} (было {before} строк → стало {after} строк)"
            )
        steps.append(
            f"Итого: финальный DataFrame содержит {len(df)} строк, "
            f"{len(df.columns)} колонок"
        )
        return steps
