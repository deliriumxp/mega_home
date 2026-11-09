"""Пауза перед повтором канала к менеджеру (`link.py`).

Обычный обрыв — растущая пауза 5 → 60 с: объект может быть вне сети надолго, и мёртвый
канал — нормальное состояние, а не повод долбить облако.

⚠ Плановый перезапуск менеджера (код закрытия 1012, «Service Restart», RFC 6455) — частый
повтор со сдвигом. Растущая пауза здесь стоила полминуты: менеджер лежал 47 с, попытка
35-й секунды проигрывала, и все дома ждали следующую — на 75-й (отчёт обновления
2026-11-09), а приложение жильца всё это время показывало «Устанавливаются обновления».
Сдвиг — чтобы десятки домов не стучали в один такт. Окно — срок подъёма проверки живости
менеджера (`update-gate.sh`, 600 с): смена зависимостей держит его минутами, дальше —
обычная растущая пауза.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

FIRST_RETRY = 5.0
MAX_RETRY = 60.0
RESTART_CODE = 1012
RESTART_RETRY = 2.0
RESTART_JITTER = 1.5
RESTART_WINDOW = 600.0


class Relink:
    """Чья очередь стучаться: `connected` при подъёме, `closed` с кодом закрытия, `next` — пауза."""

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._clock = clock
        self._rand = rand
        self._delay = FIRST_RETRY
        self._restart_until = 0.0

    def connected(self) -> None:
        self._delay = FIRST_RETRY
        self._restart_until = 0.0

    def closed(self, code: int | None) -> None:
        if code == RESTART_CODE:
            self._restart_until = self._clock() + RESTART_WINDOW

    def next(self) -> float:
        if self._clock() < self._restart_until:
            return RESTART_RETRY + self._rand() * RESTART_JITTER
        delay = self._delay
        self._delay = min(delay * 2, MAX_RETRY)
        return delay
