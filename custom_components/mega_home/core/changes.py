"""С какого момента прибор в своём состоянии — то, что HA зовёт `last_changed`, но переживающее перезапуск.

Зачем. Приложение пишет у прибора «Вкл · 3 ч назад» (решение заказчика 2026-10-12). Своё
`last_changed` Home Assistant сбрасывает на каждом старте: сущность при запуске пишет
состояние заново (`State.__init__`, `last_changed = last_updated` — `src-core.py` в копии ядра
менеджера), и «включено с утра» после ночного перезапуска превращалось бы в «2 минуты назад».
Поэтому дом помнит пару «значение и с какого момента» сам, в своём хранилище.

Правила (заказчик 2026-10-12):
- Время двигает только СМЕНА значения. Тот же `on` после перезапуска HA, обновления дома или
  перезагрузки прибора — время прежнее.
- `unavailable`/`unknown` сменой не считаются: прибор, пропавший на минуту и вернувшийся в
  то же состояние, не обнуляет время. В HA считаются — здесь сознательно иначе.
- Смена значения, пока HA лежал, видна только на старте: время — `last_changed` первого
  увиденного состояния, то есть момент старта. Точнее узнать неоткуда.

⚠ Следит САМ, своей подпиской, а не потоком приложения: поток (`events.StateStream`) живёт,
только пока открыт телефон, а смена ночью без единого открытого клиента иначе прошла бы мимо.

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`): источник и хранилище — параметрами.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .host import Host, PeriodicSave
from .source import EntityState, StateSource

STORE_KEY = "mega_home_changes"
# Датчик меняется часто; на диск — раз в период и на остановке (`host.PeriodicSave`), а не на
# каждое изменение: износ карты у Raspberry. Сбой питания теряет только время смен за период.

# Значения «прибора не видно», а не его состояния (`entity-index.md` HA: `unavailable`,
# `unknown`). Их время не пишем — см. правила выше.
NOT_A_STATE = frozenset({"unavailable", "unknown"})


def _ms(state: EntityState) -> int:
    # `last_changed` — у HA; источник без него (`PlainState`) отдаёт только `last_updated`.
    at = getattr(state, "last_changed", None) or state.last_updated
    return int(at.timestamp() * 1000)


class Changes:
    """Время последней смены значения по сущностям плиток."""

    def __init__(self, env: Host, source: StateSource) -> None:
        self._store = env.store(STORE_KEY)
        self._saver = PeriodicSave(self._store, lambda: dict(self._seen))
        self._source = source
        # entity_id → {"v": значение, "at": мс}
        self._seen: dict[str, dict[str, Any]] = {}
        self._unsubscribe: Callable[[], None] | None = None

    async def async_load(self) -> None:
        cached = await self._store.async_load() or {}
        self._seen = {
            str(k): v for k, v in cached.items() if isinstance(v, dict) and isinstance(v.get("at"), int)
        }

    def track(self, entity_ids: list[str]) -> None:
        """Следить за этими сущностями (зовётся на каждую смену состава).

        Записи сущностей, которых в составе больше нет, уходят: хранилище не копит снятые приборы.
        """
        self.stop()
        wanted = set(entity_ids)
        dropped = [k for k in self._seen if k not in wanted]
        for entity_id in dropped:
            del self._seen[entity_id]
        changed = bool(dropped)
        for entity_id in wanted:
            changed |= self._observe(entity_id, self._source.get(entity_id))
        if changed:
            self._save()
        if wanted:
            self._unsubscribe = self._source.subscribe(sorted(wanted), self._on_change)

    def stop(self) -> None:
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None

    def since(self, entity_id: str | None, state: EntityState | None) -> int | None:
        """С какого момента (мс) у сущности её значение; `None` — не знаем.

        Отмечает и само состояние: поток приложения может получить смену раньше, чем подписка
        отсюда, и прежнее время рядом с новым значением было бы ложью.
        """
        if not entity_id:
            return None
        if self._observe(entity_id, state):
            self._save()
        seen = self._seen.get(entity_id)
        return seen["at"] if seen else None

    def _on_change(self, entity_id: str, state: EntityState | None) -> None:
        if self._observe(entity_id, state):
            self._save()

    def _observe(self, entity_id: str, state: EntityState | None) -> bool:
        if state is None or state.state in NOT_A_STATE:
            return False
        seen = self._seen.get(entity_id)
        if seen is not None and seen.get("v") == state.state:
            return False
        self._seen[entity_id] = {"v": state.state, "at": _ms(state)}
        return True

    def _save(self) -> None:
        self._saver.touch()
