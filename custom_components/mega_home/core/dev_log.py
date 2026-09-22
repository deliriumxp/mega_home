"""Лог ДЛЯ РАЗРАБОТКИ: отказы дома и приложения — менеджеру, а не пересказом.

План — `docs/plan-dev-logs.md` менеджера, шаг 4. Сегодня диагноз с объекта
доезжает словами жильца, а журнал Home Assistant лежит там, куда разработчик не
дотягивается. Здесь — МЕХАНИЗМ, а не список сообщений:

- ⚠ Один обработчик `logging` на логгер интеграции: весь существующий код
  покрыт без правки в местах логирования, и новая строка лога в коде появляется
  у разработчика сама. Ради этого релиз и нужен один раз.
- ЧТО писать — решает КОНФИГ (`devLog: {level, modules, sample}`), не код.
- Очередь на ДИСКЕ (потолок и срок): объект без связи копит и отдаёт при
  подключении. Доставка — кадром `dev-log` с подтверждением (`event-ack`), тем
  же приёмом, что события устройств (`device_events.py`).
- Приложение внутри дома пишет СЮДА же (`api/dev-log`): сессии менеджера на
  странице дома нет.

⚠ Учётки маскируются на ВХОДЕ, до записи на диск: замаскировать при отправке —
значит уже записать.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from collections.abc import Callable
from time import time
from typing import Any
from uuid import uuid4

from .host import Host
from .ops_base import dumps

STORE_KEY = "mega_home_dev_log"
QUEUE_LIMIT = 200
QUEUE_TTL_S = 24 * 3600.0
SAVE_DELAY_S = 30.0
MAX_RECORD_BYTES = 8 * 1024
MAX_APP_BATCH = 50
LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}

# Учётка в адресе (`rtsp://user:pass@…`) и пары «ключ=значение» / «"ключ": "значение"»
# с секретным именем.
_URL_CREDENTIALS = re.compile(r"(//)[^/@\s:]+:[^/@\s]*@")
_SECRET_PAIR = re.compile(
    r"""(?i)(["']?(?:pass(?:word)?|secret|token|auth(?:orization)?|key|cookie)["']?\s*[:=]\s*["']?)[^"'&\s,}]+"""
)


def mask(text: str) -> str:
    """Вырезать учётки из строки записи."""
    return _SECRET_PAIR.sub(r"\1***", _URL_CREDENTIALS.sub(r"\1***@", text))


def _masked(value: Any, depth: int = 0) -> Any:
    if depth > 5:
        return "…"
    if isinstance(value, str):
        return mask(value)
    if isinstance(value, dict):
        return {
            str(k): "***" if re.search(r"(?i)pass|secret|token|auth|key|cookie", str(k)) else _masked(v, depth + 1)
            for k, v in list(value.items())[:50]
        }
    if isinstance(value, (list, tuple)):
        return [_masked(v, depth + 1) for v in list(value)[:20]]
    return value


class DevLog:
    """Очередь записей до подтверждения менеджером и обработчик `logging`."""

    def __init__(self, env: Host, context: Callable[[], dict[str, Any]]) -> None:
        self._store = env.store(STORE_KEY)
        self._context = context
        self._queue: list[dict[str, Any]] = []
        self._sender: Callable[[dict[str, Any]], None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.level = logging.WARNING
        self.modules: set[str] = set()
        self.sample = 1.0
        self.dropped = 0
        self.handler = _Handler(self)

    async def async_load(self) -> None:
        self._loop = asyncio.get_running_loop()
        cached = await self._store.async_load() or {}
        self._queue = [r for r in cached.get("queue", []) if isinstance(r, dict)]
        self._expire()

    def apply(self, config: dict[str, Any] | None) -> None:
        """Фильтр из конфига объекта. Нет ключа — `warn` и все наши модули."""
        block = (config or {}).get("devLog")
        block = block if isinstance(block, dict) else {}
        self.level = LEVELS.get(str(block.get("level") or "warn"), logging.WARNING)
        modules = block.get("modules")
        self.modules = {str(m) for m in modules} if isinstance(modules, list) else set()
        try:
            self.sample = min(max(float(block.get("sample", 1.0)), 0.0), 1.0)
        except (TypeError, ValueError):
            self.sample = 1.0

    def add(self, source: str, level: str, code: str, message: str, data: Any = None) -> None:
        """Одна запись в очередь. Из цикла событий — `add_threadsafe` зовёт сюда."""
        record = {
            "id": uuid4().hex,
            "at": int(time() * 1000),
            "source": source,
            "level": level,
            "code": code[:64],
            "message": mask(message)[: MAX_RECORD_BYTES // 2],
            "data": _masked(data) if data is not None else None,
            "ctx": self._context(),
        }
        if len(dumps(record)) > MAX_RECORD_BYTES:
            record["data"] = {"truncated": True}
        self._expire()
        self._queue.append(record)
        if len(self._queue) > QUEUE_LIMIT:
            del self._queue[: len(self._queue) - QUEUE_LIMIT]
            self.dropped += 1
        self._store.async_delay_save(lambda: {"queue": self._queue}, SAVE_DELAY_S)
        if self._sender is not None:
            self._sender(_frame(record))

    def add_app(self, records: Any) -> int:
        """Записи приложения (`api/dev-log`): форма та же, источник — `app`."""
        if not isinstance(records, list):
            return 0
        taken = 0
        for item in records[:MAX_APP_BATCH]:
            if not isinstance(item, dict):
                continue
            level = str(item.get("level") or "error")
            self.add(
                "app",
                level if level in ("info", "warn", "error") else "error",
                str(item.get("code") or "app"),
                str(item.get("message") or ""),
                item.get("data"),
            )
            taken += 1
        return taken

    def add_threadsafe(self, record: logging.LogRecord) -> None:
        """Из `logging`: запись может прийти из потока executor'а."""
        if self._loop is None or self._loop.is_closed():
            return
        if record.levelno < self.level or (self.modules and record.module not in self.modules):
            return
        if self.sample < 1.0 and random.random() >= self.sample:  # noqa: S311 — не криптография
            return
        level = "error" if record.levelno >= logging.ERROR else "warn" if record.levelno >= logging.WARNING else "info"
        data = {"trace": logging.Formatter().formatException(record.exc_info)} if record.exc_info else None
        self._loop.call_soon_threadsafe(self.add, "home", level, record.module, record.getMessage(), data)

    def attach(self, sender: Callable[[dict[str, Any]], None]) -> list[dict[str, Any]]:
        """Канал поднялся: вернуть неподтверждённое в исходном порядке."""
        self._sender = sender
        self._expire()
        return [_frame(record) for record in self._queue]

    def detach(self) -> None:
        self._sender = None

    def ack(self, record_id: Any) -> None:
        before = len(self._queue)
        self._queue = [r for r in self._queue if r["id"] != record_id]
        if len(self._queue) != before:
            self._store.async_delay_save(lambda: {"queue": self._queue}, SAVE_DELAY_S)

    def state(self) -> dict[str, Any]:
        return {"queued": len(self._queue), "dropped": self.dropped, "level": logging.getLevelName(self.level)}

    def _expire(self) -> None:
        cutoff = (time() - QUEUE_TTL_S) * 1000
        self._queue = [r for r in self._queue if (r.get("at") or 0) >= cutoff]


def _frame(record: dict[str, Any]) -> dict[str, Any]:
    return {"t": "dev-log", "id": record["id"], "record": record}


class _Handler(logging.Handler):
    """Мост `logging` → очередь. Сам ничего не пишет: иначе поймал бы себя."""

    def __init__(self, log: DevLog) -> None:
        super().__init__(logging.DEBUG)
        self._log = log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._log.add_threadsafe(record)
        except Exception:  # noqa: BLE001 — лог разработки не роняет дом
            self.handleError(record)
