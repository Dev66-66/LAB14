"""Клиент Apache Arrow Flight для получения данных от Go-сервера."""

import os
from datetime import datetime, timezone
from typing import List

import pyarrow as pa
import pyarrow.flight as flight

from domain.models import AggregatedWindow


class ArrowFlightClient:
    """Клиент для получения агрегированных окон через Arrow Flight RPC."""

    def __init__(self) -> None:
        """Инициализирует клиент с параметрами из переменных окружения."""
        host = os.getenv("ARROW_FLIGHT_HOST", "localhost")
        port = int(os.getenv("ARROW_FLIGHT_PORT", "8815"))
        self._client = flight.connect(f"grpc://{host}:{port}")

    def fetch_windows(self) -> List[AggregatedWindow]:
        """Получает все накопленные агрегированные окна с Go-сервера.

        Returns:
            Список AggregatedWindow, полученных через Arrow Flight.

        Raises:
            flight.FlightUnavailableError: если сервер недоступен.
        """
        descriptor = flight.FlightDescriptor.for_command(b"aggregated_windows")
        info = self._client.get_flight_info(descriptor)
        reader = self._client.do_get(info.endpoints[0].ticket)
        table = reader.read_all()
        return self._table_to_windows(table)

    def _table_to_windows(self, table: pa.Table) -> List[AggregatedWindow]:
        """Конвертирует Arrow Table в список AggregatedWindow.

        Args:
            table: Arrow Table с колонками WindowSchema.

        Returns:
            Список AggregatedWindow, восстановленных из колонок таблицы.
        """
        windows: List[AggregatedWindow] = []

        ws_col = table.column("window_start").to_pylist()
        we_col = table.column("window_end").to_pylist()
        te_col = table.column("total_events").to_pylist()
        uu_col = table.column("unique_users").to_pylist()
        ad_col = table.column("avg_duration_ms").to_pylist()

        for ws, we, te, uu, ad in zip(ws_col, we_col, te_col, uu_col, ad_col):
            # Arrow timestamp[ms, UTC] → Python datetime (aware, UTC).
            window_start = _arrow_ts_to_datetime(ws)
            window_end = _arrow_ts_to_datetime(we)

            windows.append(
                AggregatedWindow(
                    window_start=window_start,
                    window_end=window_end,
                    total_events=int(te),
                    event_counts={},  # агрегированные окна не несут breakdown по типам
                    unique_users=int(uu),
                    avg_duration_ms=float(ad),
                    top_pages=[],
                )
            )

        return windows

    def close(self) -> None:
        """Закрывает соединение с Flight-сервером."""
        self._client.close()


def _arrow_ts_to_datetime(value: object) -> datetime:
    """Конвертирует значение из Arrow timestamp-колонки в datetime (UTC).

    Args:
        value: Целое число (Unix-миллисекунды) или datetime от PyArrow.

    Returns:
        datetime с tzinfo=UTC.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    # PyArrow возвращает int для timestamp при to_pylist() с некоторыми типами.
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
