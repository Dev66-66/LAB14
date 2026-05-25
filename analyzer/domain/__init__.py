"""Доменный слой анализатора."""

from analyzer.domain.models import (
    AggregatedWindow,
    AnalysisResult,
    EventType,
    PageStat,
    UserEvent,
)

__all__ = [
    "AggregatedWindow",
    "AnalysisResult",
    "EventType",
    "PageStat",
    "UserEvent",
]
