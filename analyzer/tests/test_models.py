"""Тесты domain-моделей."""

from datetime import datetime, timezone

from domain.models import AggregatedWindow, EventType


class TestEventType:
    """Тесты перечисления EventType."""

    def test_all_values_defined(self) -> None:
        """Проверяет что все 6 типов событий определены."""
        assert len(EventType) == 6

    def test_string_comparison(self) -> None:
        """EventType совместим со строками (наследует от str)."""
        assert EventType.CLICK == "click"
        assert EventType.PAGE_VIEW == "page_view"
        assert EventType.PURCHASE == "purchase"
        assert EventType.LOGIN == "login"
        assert EventType.LOGOUT == "logout"
        assert EventType.SEARCH == "search"


class TestAggregatedWindow:
    """Тесты датакласса AggregatedWindow."""

    def test_creation_with_defaults(self) -> None:
        """Создание окна с минимальным набором полей."""
        now = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)
        window = AggregatedWindow(
            window_start=now,
            window_end=now,
            total_events=0,
            event_counts={},
            unique_users=0,
            avg_duration_ms=0.0,
            top_pages=[],
        )
        assert window.total_events == 0
        assert window.event_counts == {}
        assert window.top_pages == []
        assert window.window_start == now
