"""Хранилище событий устройств: лента на диске, живёт без менеджера.

Часть F плана тонкого шлюза (`docs/plan-thin-gateway.md`). До 0.4.0 то же самое
держал вендорский модуль регистратора видеонаблюдения — сутками копил ленту на
диске, с тем же смыслом потолка и срока хранения, и приложение показывало
историю звонков и движения даже без менеджера. Здесь та же мысль БЕЗ вендора:
каждое опубликованное ЛОКАЛЬНОЕ событие устройства (`device_events.EventHub`,
`local=True`) ложится сюда по `access` — id устройства, — а маршрут
`api/device-events` отдаёт ленту и дома, и переносом, одним кодом
(`ops.device_events`).

⚠ Запись ОТЛОЖЕННАЯ (60 с, `Host.store().async_delay_save`), как у сторожа
(`agent.py`) и у прежнего вендорского модуля: звонок в дверь пишется на диск не
чаще раза в минуту, а не при каждом кадре — иначе лента событий превращалась
бы в запись на каждый чих слушателя.
"""

from __future__ import annotations

from time import time
from typing import Any

from .host import Host

STORE_VERSION = 1
STORE_KEY = "mega_home_device_events"
SAVE_DELAY_S = 60.0

# Потолок и срок — на КАЖДОЕ устройство, не на весь дом: звонок в дверь и
# датчик протечки не должны тесниться в одном лимите.
CAP_PER_DEVICE = 2000
RETENTION_S = 7 * 24 * 3600.0


class DeviceEventStore:
    """События устройств на диске: до `CAP_PER_DEVICE` и `RETENTION_S` на `access`."""

    def __init__(self, env: Host) -> None:
        self._store = env.store(STORE_KEY, STORE_VERSION)
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
                "data": frame.get("data"),
            }
        )
        self._by_access[access] = _trimmed(events)
        self._store.async_delay_save(lambda: {"byAccess": self._by_access}, SAVE_DELAY_S)

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
