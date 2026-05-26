"""Тесты DataTransformer."""

from datetime import datetime, timedelta, timezone

from domain.models import AggregatedWindow, PageStat
from usecases.transformer import DataTransformer

_BASE_START = datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
_BASE_END = datetime(2024, 1, 1, 14, 10, 0, tzinfo=timezone.utc)


def make_window(
    total_events: int,
    unique_users: int,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> AggregatedWindow:
    """Фабричная функция для создания тестового окна."""
    return AggregatedWindow(
        window_start=window_start or _BASE_START,
        window_end=window_end or _BASE_END,
        total_events=total_events,
        event_counts={"click": total_events} if total_events > 0 else {},
        unique_users=unique_users,
        avg_duration_ms=100.0,
        top_pages=[PageStat(page="/", count=total_events)] if total_events > 0 else [],
    )


class TestDataTransformer:
    """Тесты класса DataTransformer."""

    def setup_method(self) -> None:
        """Инициализация перед каждым тестом."""
        self.transformer = DataTransformer()

    def test_transform_removes_duplicates(self) -> None:
        """Проверяет удаление дубликатов по (window_start, window_end)."""
        w1 = make_window(total_events=5, unique_users=2)
        w2 = make_window(total_events=10, unique_users=3)  # одинаковые start/end
        df = self.transformer.transform([w1, w2])
        assert len(df) == 1

    def test_transform_filters_zero_events(self) -> None:
        """Проверяет фильтрацию окон с total_events == 0."""
        start2 = _BASE_START + timedelta(minutes=10)
        end2 = _BASE_END + timedelta(minutes=10)
        w_zero = make_window(total_events=0, unique_users=0)
        w_valid = make_window(
            total_events=5, unique_users=2, window_start=start2, window_end=end2
        )
        df = self.transformer.transform([w_zero, w_valid])
        assert len(df) == 1
        assert df["total_events"][0] == 5

    def test_transform_adds_events_per_second(self) -> None:
        """Проверяет наличие колонки events_per_second."""
        w = make_window(total_events=10, unique_users=3)
        df = self.transformer.transform([w])
        assert "events_per_second" in df.columns

    def test_aggregate_by_hour_groups_correctly(self) -> None:
        """Проверяет группировку по часам.

        Два окна в одном часе (14:00 и 14:30) должны дать одну строку
        с суммарным total_events.
        """
        start2 = datetime(2024, 1, 1, 14, 30, 0, tzinfo=timezone.utc)
        end2 = datetime(2024, 1, 1, 14, 40, 0, tzinfo=timezone.utc)
        w1 = make_window(total_events=5, unique_users=2)
        w2 = make_window(
            total_events=3, unique_users=1, window_start=start2, window_end=end2
        )
        df = self.transformer.transform([w1, w2])
        hourly = self.transformer.aggregate_by_hour(df)
        assert len(hourly) == 1
        assert hourly["total_events"][0] == 8

    def test_document_cleaning_steps_not_empty(self) -> None:
        """Проверяет что список шагов очистки непустой."""
        w = make_window(total_events=5, unique_users=2)
        df = self.transformer.transform([w])
        steps = self.transformer.document_cleaning_steps(df)
        assert len(steps) > 0
