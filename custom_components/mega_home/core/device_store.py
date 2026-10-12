"""Хранилище событий устройств: лента на диске, живёт без менеджера.

Часть F плана тонкого шлюза (`docs/plan-thin-gateway.md`). До 0.4.0 то же самое
держал вендорский модуль регистратора видеонаблюдения — сутками копил ленту на
диске, с тем же смыслом потолка и срока хранения, и приложение показывало
историю звонков и движения даже без менеджера. Здесь та же мысль БЕЗ вендора:
каждое опубликованное ЛОКАЛЬНОЕ событие устройства (`device_events.EventHub`,
`local=True`) ложится сюда по `access` — id устройства, — а маршрут
`api/device-events` отдаёт ленту и дома, и переносом, одним кодом
(`ops.device_events`).

⚠ Запись — раз в период и на остановке (`host.PeriodicSave`), а не на каждый кадр:
иначе лента событий превращалась бы в запись на каждый чих слушателя.
"""

from __future__ import annotations

import json
from time import time
from typing import Any

from .host import Host, PeriodicSave

STORE_VERSION = 1
STORE_KEY = "mega_home_device_events"

# Потолок и срок — на КАЖДОЕ устройство, не на весь дом: звонок в дверь и
# датчик протечки не должны тесниться в одном лимите.
# ⚠ Пределы и по ЧИСЛУ, и по БАЙТАМ: дом без менеджера живёт неделями, а кадр
# опроса несёт тело ответа до 64 КБ (`device_events.MAX_DATA_BYTES`) — 2000 таких
# на устройство давали сотню мегабайт JSON, переписываемого целиком раз в минуту.
CAP_PER_DEVICE = 500
MAX_BYTES_PER_DEVICE = 1024 * 1024
# Тело одного события в ленте. Крупнее — метка вместо тела: ленте нужен факт
# события, а не выкачанный ответ устройства (его менеджер получил кадром).
MAX_EVENT_DATA_BYTES = 4 * 1024
RETENTION_S = 7 * 24 * 3600.0


class DeviceEventStore:
    """События устройств на диске: до `CAP_PER_DEVICE` и `RETENTION_S` на `access`."""

    def __init__(self, env: Host) -> None:
        self._store = env.store(STORE_KEY, STORE_VERSION)
        self._saver = PeriodicSave(self._store, self._snapshot)
        self._by_access: dict[str, list[dict[str, Any]]] = {}

    async def async_load(self) -> None:
        """Поднять ленту из кэша — до первого события, как у сторожа."""
        cached = await self._store.async_load() or {}
        by_access = cached.get("byAccess")
        if not isinstance(by_access, dict):
            return
        self._by_access = {
            str(access): [event for event in events if isinstance(event, dict)]
            for access, events in by_access.items()
            if isinstance(events, list)
        }
        # Кэш прежних выпусков мог держать 2000 кадров с крупными телами — режем сразу.
        self._snapshot()

    def add(self, frame: dict[str, Any]) -> None:
        """Одно опубликованное ЛОКАЛЬНОЕ событие (`EventHub.publish`) — в ленту его устройства."""
        access = str(frame.get("access") or "")
        if not access:
            return
        events = self._by_access.setdefault(access, [])
        events.append(
            {
                "id": frame.get("id"),
                "at": frame.get("at"),
                "source": frame.get("source"),
                "event": frame.get("event"),
                "data": _small(frame.get("data")),
            }
        )
        self._by_access[access] = _trimmed(events)
        self._saver.touch()

    def mark(self, access: Any, event_id: str, file: dict[str, Any]) -> None:
        """Событию приложен файл (`event_files.py`): приложению не гадать, есть ли кадр."""
        for event in reversed(self._by_access.get(str(access or ""), [])):
            if event.get("id") == event_id:
                event["file"] = file
                self._saver.touch()
                return

    def _snapshot(self) -> dict[str, Any]:
        """Срок — ВСЕМ устройствам при каждой записи: замолчавшее устройство
        иначе держало бы свою ленту вечно, ведь режет её только новое событие."""
        trimmed = {access: _fit(_trimmed(events)) for access, events in self._by_access.items()}
        self._by_access = {access: events for access, events in trimmed.items() if events}
        return {"byAccess": self._by_access}

    def list(self, access: str, limit: int, before: float | None) -> list[dict[str, Any]]:
        """Лента устройства, новые первыми — то, что отдаёт `api/device-events`."""
        events = _trimmed(self._by_access.get(access, []))
        if before is not None:
            events = [event for event in events if (event.get("at") or 0) < before]
        return list(reversed(events))[:limit]


def _trimmed(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Отсеять старше `RETENTION_S` и обрезать до `CAP_PER_DEVICE`, по возрастанию `at`."""
    cutoff = time() - RETENTION_S
    fresh = sorted(
        (event for event in events if (event.get("at") or 0) >= cutoff),
        key=lambda event: event.get("at") or 0,
    )
    return fresh[-CAP_PER_DEVICE:]


def _fit(fresh: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Новейшие в `MAX_BYTES_PER_DEVICE`. ⚠ Только при записи (`_snapshot`), не на
    каждое событие: счёт размера всей ленты на кадр опроса — лишний проход на
    слабом процессоре, а в памяти между записями ленту держит `CAP_PER_DEVICE`."""
    total, keep = 0, 0
    for event in reversed(fresh):
        total += len(json.dumps(event, default=str))
        if total > MAX_BYTES_PER_DEVICE:
            break
        keep += 1
    return fresh[len(fresh) - keep :]


def _small(data: Any) -> Any:
    """Тело события не больше `MAX_EVENT_DATA_BYTES`; крупнее — метка с размером."""
    size = len(json.dumps(data, default=str))
    return {"truncated": True, "bytes": size} if size > MAX_EVENT_DATA_BYTES else data
