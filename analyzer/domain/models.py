"""Доменные модели для анализатора пользовательских событий."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class EventType(str, Enum):
    """Перечисление типов пользовательских событий."""

    CLICK = "click"
    PAGE_VIEW = "page_view"
    PURCHASE = "purchase"
    LOGIN = "login"
    LOGOUT = "logout"
    SEARCH = "search"


@dataclass
class UserEvent:
    """Одно пользовательское событие из веб-приложения.

    Args:
        id: Уникальный идентификатор события (UUID).
        user_id: Идентификатор пользователя.
        session_id: Идентификатор сессии.
        event_type: Тип события из перечисления EventType.
        page: URL-путь страницы (должен начинаться с '/').
        duration_ms: Длительность события в миллисекундах.
        timestamp: Временная метка события (UTC).
        metadata: Дополнительные атрибуты (device, browser, country и др.).
    """

    id: str
    user_id: str
    session_id: str
    event_type: EventType
    page: str
    duration_ms: float
    timestamp: datetime
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PageStat:
    """Статистика по одной странице.

    Args:
        page: URL-путь страницы.
        count: Количество событий на странице за окно.
    """

    page: str
    count: int


@dataclass
class AggregatedWindow:
    """Агрегированное окно событий (результат tumbling window в Go).

    Args:
        window_start: Начало временного окна (UTC).
        window_end: Конец временного окна (UTC).
        total_events: Общее количество событий в окне.
        event_counts: Распределение событий по типам.
        unique_users: Количество уникальных пользователей.
        avg_duration_ms: Средняя длительность события в мс.
        top_pages: Топ-5 страниц по количеству событий.
    """

    window_start: datetime
    window_end: datetime
    total_events: int
    event_counts: Dict[str, int]
    unique_users: int
    avg_duration_ms: float
    top_pages: List[PageStat]


@dataclass
class AnalysisResult:
    """Результат аналитической обработки батча окон.

    Args:
        total_windows: Количество обработанных окон.
        total_events: Суммарное количество событий.
        avg_events_per_window: Среднее количество событий на окно.
        peak_window: Окно с наибольшим количеством событий.
        event_distribution: Суммарное распределение событий по типам.
        generated_at: Время генерации результата (UTC).
    """

    total_windows: int
    total_events: int
    avg_events_per_window: float
    peak_window: Optional[AggregatedWindow]
    event_distribution: Dict[str, int]
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
