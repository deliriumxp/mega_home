"""Включение по сети на ЛЮБУЮ команду «включи» из источника, а не только из приложения.

Зачем (решение заказчика 2026-10-13). Телевизор LG в Home Assistant сам не включается: его
интеграция зовёт тех, кто подписан на её триггер «Device is requested to turn on»
(`webostv.turn_on`), и без подписчика команда уходит в пустоту, а бита TURN_ON у плеера нет вовсе.
Приложение будит прибор магическим пакетом само, но голос (Алиса через HA), автоматизации и
интерфейс HA зовут `media_player.turn_on` — и телевизор не включался. Дом подписывается на этот
триггер САМ, без автоматизации в HA, и на срабатывание шлёт тот же пакет.

- Что подписывать и чем будить, решает МЕНЕДЖЕР: у плитки `wake` — готовый вызов `connect`
  (менеджер: `wake-on-lan.util.ts`), `wakeTrigger` — имя триггера источника. Дом не знает ни
  производителей, ни пакета.
- Подписку держит источник (`StateSource.on_turn_on`); у источника без неё модуль молчит.
- ⚠ Подписка живёт без жильца: телефон выключен, менеджер офлайн, а «Алиса, включи телевизор»
  обязана сработать (`docs/home-gateway.md`, закрытый список, пункт 1).
- ⚠ Переподписка — только на смену триггера. Координатор зовёт `track` на каждый опрос, а подписка
  и отписка в HA переписывают состояние плеера (у него меняется бит TURN_ON), то есть каждый опрос
  давал бы лишнюю запись в истории прибора.
- ⚠ Неудача подписки (интеграция плеера ещё не загружена, у сущности нет устройства) повторяется
  не чаще `RETRY_S`: HA пишет отказ в свой журнал уровнем ERROR, и опрос раз в минуту засыпал бы его.

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from . import connect
from .const import LOGGER
from .ops_base import OpError
from .source import StateSource

# Повтор неудачной подписки: реже опроса координатора, чаще, чем инсталлятор успеет заметить.
RETRY_S = 300.0


def wake_tiles(config: dict[str, Any] | None) -> dict[str, tuple[str, dict[str, Any]]]:
    """Сущность → (триггер источника, вызов `connect`) по плиткам конфига."""
    found: dict[str, tuple[str, dict[str, Any]]] = {}
    for tile in (config or {}).get("tiles", []) or []:
        entity_id, trigger, wake = tile.get("entityId"), tile.get("wakeTrigger"), tile.get("wake")
        if isinstance(entity_id, str) and entity_id and isinstance(trigger, str) and trigger and isinstance(wake, dict):
            found[entity_id] = (trigger, wake)
    return found


class WakeOnRequest:
    """Подписки «прибор просят включить → послать пакет» по плиткам конфига."""

    def __init__(
        self,
        source: StateSource,
        send: Callable[[dict[str, Any]], Awaitable[Any]] = connect.perform,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._send = send
        self._clock = clock
        self._lock = asyncio.Lock()
        # Сущность → (триггер, отписка); вызов `connect` читается при срабатывании из `_wake`.
        self._attached: dict[str, tuple[str, Callable[[], None]]] = {}
        self._wake: dict[str, dict[str, Any]] = {}
        # Сущность → (триггер, когда не удалось).
        self._failed: dict[str, tuple[str, float]] = {}
        self._fired = 0
        self._last_error: str | None = None
        self._stopped = False

    async def track(self, config: dict[str, Any] | None) -> None:
        """Привести подписки к составу. Зовётся на каждую смену конфига (и на опрос)."""
        attach = getattr(self._source, "on_turn_on", None)
        if attach is None:
            return
        async with self._lock:
            if self._stopped:
                return
            wanted = wake_tiles(config)
            self._wake = {entity_id: wake for entity_id, (_, wake) in wanted.items()}
            for entity_id, (trigger, unsub) in list(self._attached.items()):
                if wanted.get(entity_id, ("",))[0] != trigger:
                    unsub()
                    del self._attached[entity_id]
            self._failed = {e: f for e, f in self._failed.items() if wanted.get(e, ("",))[0] == f[0]}
            now = self._clock()
            for entity_id, (trigger, _) in wanted.items():
                if entity_id in self._attached:
                    continue
                failed = self._failed.get(entity_id)
                if failed and now - failed[1] < RETRY_S:
                    continue
                try:
                    unsub = await attach(entity_id, trigger, self._action(entity_id))
                except Exception as err:  # noqa: BLE001
                    # ⚠ Исключение источника — та же неудача, что `None`: иначе задача падала, запись
                    # в `_failed` не появлялась (повтор на каждом опросе без паузы `RETRY_S`), а
                    # плитки дальше по проходу оставались без подписки из-за одного прибора.
                    LOGGER.warning("Wake on %s for %s not attached: %s", trigger, entity_id, err)
                    self._last_error = f"{entity_id}: {err}"
                    unsub = None
                if unsub is not None and self._stopped:
                    # Запись выгрузили, пока HA подключал: подписка пережила бы её.
                    unsub()
                    return
                if unsub is None:
                    self._failed[entity_id] = (trigger, now)
                    continue
                self._failed.pop(entity_id, None)
                self._attached[entity_id] = (trigger, unsub)
                LOGGER.info("Wake on %s for %s attached", trigger, entity_id)

    def stop(self) -> None:
        self._stopped = True
        for _, unsub in self._attached.values():
            unsub()
        self._attached.clear()

    def _action(self, entity_id: str) -> Callable[[], Awaitable[None]]:
        async def fire() -> None:
            wake = self._wake.get(entity_id)
            if wake is None:
                return
            self._fired += 1
            try:
                await self._send(wake)
            except OpError as err:
                self._last_error = f"{entity_id}: {err.message}"
                LOGGER.warning("Wake of %s failed: %s", entity_id, err.message)
                return
            LOGGER.info("Wake packet sent for %s", entity_id)

        return fire

    @property
    def summary(self) -> dict[str, Any]:
        """Для диагностики: что подписано, что нет и почему молчит телевизор."""
        return {
            "attached": {entity_id: trigger for entity_id, (trigger, _) in self._attached.items()},
            "failed": {entity_id: trigger for entity_id, (trigger, _) in self._failed.items()},
            "fired": self._fired,
            "last_error": self._last_error,
        }
