"""Основной конвейер: получение данных → трансформация → анализ → Parquet."""

import logging
import time
from datetime import datetime

from adapters.arrow_client import ArrowFlightClient
from adapters.duckdb_analyzer import DuckDBAnalyzer
from adapters.parquet_store import ParquetStore
from usecases.analyzer import DataAnalyzer
from usecases.transformer import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='{"level":"%(levelname)s","ts":"%(asctime)s","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """Запускает один цикл конвейера обработки данных.

    Шаги:
    1. Получение окон через Arrow Flight
    2. Валидация через Rust (event_validator если доступен)
    3. Трансформация через Polars
    4. Сохранение в Parquet
    5. Анализ через DuckDB + бенчмарк vs Polars
    6. Логирование результатов
    """
    arrow_client = ArrowFlightClient()
    parquet_store = ParquetStore()
    duckdb_analyzer = DuckDBAnalyzer()
    transformer = DataTransformer()
    analyzer = DataAnalyzer(duckdb_analyzer)

    try:
        # Шаг 1: получение окон через Arrow Flight.
        logger.info("fetching windows from Arrow Flight")
        windows = arrow_client.fetch_windows()

        if not windows:
            logger.info("no windows available, skipping cycle")
            return

        logger.info("fetched %d windows from Go server", len(windows))

        # Шаг 2: валидация через Rust PyO3 (опционально).
        try:
            import event_validator  # type: ignore[import]
            import json

            validated = []
            for w in windows:
                payload = json.dumps(
                    {
                        "id": "window",
                        "user_id": "system",
                        "session_id": "pipeline",
                        "event_type": "page_view",
                        "page": "/",
                        "duration_ms": w.avg_duration_ms,
                    }
                )
                result = json.loads(event_validator.validate_event(payload))
                if result["is_valid"]:
                    validated.append(w)
                else:
                    logger.warning("window dropped by validator: %s", result["errors"])
            windows = validated
            logger.info("after validation: %d windows remain", len(windows))
        except ImportError:
            logger.info("Rust validator not available, skipping validation step")

        # Шаг 3: трансформация через Polars.
        df = transformer.transform(windows)
        for step in transformer.document_cleaning_steps(df):
            logger.info(step)

        # Шаг 4: сохранение в Parquet.
        filename = f"windows_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        parquet_path = parquet_store.save_windows(windows, filename)
        logger.info("saved %d rows to %s", len(df), parquet_path)

        # Шаг 5: DuckDB-анализ + бенчмарк vs Polars.
        result = analyzer.analyze(windows, str(parquet_path))
        benchmark = analyzer.benchmark_polars_vs_duckdb(str(parquet_path))

        # Шаг 6: итоговое логирование.
        logger.info(
            "pipeline complete: windows=%d, events=%d, avg_per_window=%.1f",
            result.total_windows,
            result.total_events,
            result.avg_events_per_window,
        )
        if result.peak_window:
            logger.info(
                "peak window: %s → %d events",
                result.peak_window.window_start.isoformat(),
                result.peak_window.total_events,
            )
        logger.info(
            "benchmark: DuckDB=%.2f ms, Polars=%.2f ms, speedup=%.2fx",
            benchmark["duckdb_ms"],
            benchmark["polars_ms"],
            benchmark["speedup"],
        )

    finally:
        arrow_client.close()
        duckdb_analyzer.close()


if __name__ == "__main__":
    while True:
        run_pipeline()
        time.sleep(30)
