"""Python asyncio-реализация сборщика пользовательских событий (вариант 21).

Аналог Go-эмиттера: N асинхронных воркеров генерируют батчи событий →
asyncio.Queue (имитация буферизованного канала). Используется в бенчмарке
задания 6 (Go vs Python сборщик).
"""

import asyncio
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict

_EVENT_TYPES = ["click", "page_view", "purchase", "login", "logout", "search"]
_DEVICES = ["desktop", "mobile", "tablet"]
_BROWSERS = ["chrome", "firefox", "safari", "edge"]
_COUNTRIES = ["RU", "US", "DE", "FR", "CN", "BR", "IN", "GB"]
_PAGES = [
    "/", "/catalog", "/product/123", "/product/456",
    "/product/789", "/cart", "/checkout", "/profile",
]
_USERS = [f"user-{i:03d}" for i in range(1, 51)]
_SESSIONS = [str(uuid.uuid4()) for _ in range(20)]


@dataclass
class UserEvent:
    """Пользовательское событие — аналог domain.UserEvent из Go."""

    id: str
    user_id: str
    session_id: str
    event_type: str
    page: str
    duration_ms: float
    timestamp: str
    metadata: Dict[str, str] = field(default_factory=dict)


def _make_event() -> UserEvent:
    """Генерирует одно случайное событие (зеркало Go Emitter.Generate)."""
    return UserEvent(
        id=str(uuid.uuid4()),
        user_id=random.choice(_USERS),
        session_id=random.choice(_SESSIONS),
        event_type=random.choice(_EVENT_TYPES),
        page=random.choice(_PAGES),
        duration_ms=10.0 + random.random() * 4990.0,
        timestamp=datetime.now(timezone.utc).isoformat(),
        metadata={
            "device": random.choice(_DEVICES),
            "browser": random.choice(_BROWSERS),
            "country": random.choice(_COUNTRIES),
        },
    )


class AsyncEventCollector:
    """Asyncio-сборщик: N воркеров → asyncio.Queue → потребитель.

    Архитектурно идентичен Go-коллектору:
      - N emit-воркеров (горутины → корутины)
      - asyncio.Queue с буфером (канал Go)
      - consumer-воркер (Consumer.ReadEvents)
      - интервал батча 100 мс (как в Go)
    """

    def __init__(
        self,
        num_workers: int = 3,
        batch_size: int = 100,
        queue_maxsize: int = 10_000,
    ) -> None:
        self.num_workers = num_workers
        self.batch_size = batch_size
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._total_sent = 0
        self._total_consumed = 0
        self._stop = asyncio.Event()

    async def _emit_worker(self, _worker_id: int) -> None:
        """Воркер: генерирует batch_size событий каждые 100 мс."""
        while not self._stop.is_set():
            batch = [_make_event() for _ in range(self.batch_size)]
            for event in batch:
                await self._queue.put(event)
            self._total_sent += len(batch)
            try:
                await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.15)
            except asyncio.TimeoutError:
                pass

    async def _consume_worker(self) -> None:
        """Потребитель: вычитывает из очереди, имитируя Kafka-консьюмер."""
        while not self._stop.is_set() or not self._queue.empty():
            try:
                await asyncio.wait_for(self._queue.get(), timeout=0.05)
                self._total_consumed += 1
                self._queue.task_done()
            except asyncio.TimeoutError:
                pass

    async def run(self, duration_sec: float) -> Dict[str, int]:
        """Запускает коллектор на duration_sec секунд.

        Returns:
            {total_sent, total_consumed}
        """
        self._stop.clear()
        self._total_sent = 0
        self._total_consumed = 0

        producers = [
            asyncio.create_task(self._emit_worker(i))
            for i in range(self.num_workers)
        ]
        consumer = asyncio.create_task(self._consume_worker())

        await asyncio.sleep(duration_sec)
        self._stop.set()
        await asyncio.gather(*producers, return_exceptions=True)
        await consumer

        return {
            "total_sent": self._total_sent,
            "total_consumed": self._total_consumed,
        }


class SyncEventCollector:
    """Синхронный однопоточный сборщик (без asyncio) — базовая линия Python.

    Отражает наивную реализацию без конкурентности: один цикл генерирует
    батч → «отправляет» (счётчик) → спит 100 мс.
    """

    def __init__(self, batch_size: int = 100) -> None:
        self.batch_size = batch_size

    def run(self, duration_sec: float) -> Dict[str, int]:
        """Генерирует события в цикле на duration_sec секунд.

        Returns:
            {total_sent}
        """
        total = 0
        deadline = time.monotonic() + duration_sec
        while time.monotonic() < deadline:
            total += self.batch_size
            for _ in range(self.batch_size):
                _make_event()
            time.sleep(0.1)
        return {"total_sent": total}
