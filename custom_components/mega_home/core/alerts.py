"""Уведомления по датчикам: правило ЖИЛЬЦА, которое дом исполняет сам.

Зачем (решения заказчика 2026-10-13, менеджер: `docs/superpowers/specs/2026-10-13-sensor-alerts-design.md`).
Жилец задаёт у датчика «больше/меньше/равно/сработал», выдержку и паузу; дом видит состояние,
сравнивает и при срабатывании шлёт менеджеру событие, а менеджер — push. Ни автоматизаций HA,
ни потока состояний в менеджер.

- Правило — ДОМА, одно на объект, и хранится ЗДЕСЬ: это выбор жильца, как фото и кадры
  (пункт 3 закрытого списка `docs/home-gateway.md`). Правится изнутри без менеджера.
- Модуль толкует только форму «сущность, условие, значение, выдержка, пауза». Что с
  срабатыванием делать (текст, кому слать) — решает менеджер по событию.

Правила (заказчик 2026-10-13):
- Срабатывает только ВХОД в условие; пока значение в нём — повторов нет.
- Выдержка `holdSec`: условие держится столько, выход раньше — отмена.
- Пауза `cooldownSec` от прошлого срабатывания; вход внутри паузы считается учтённым и не
  шлётся позже.
- `unavailable`/`unknown` — ни вход, ни выход (как в `changes.py`).
- Сохранение правила само не срабатывает: точка отсчёта — текущее состояние.

⚠ Следит САМ, своей подпиской: приложение ночью закрыто, а протечка обязана дойти.
⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`): источник, хранилище, часы и отложенный
вызов — параметрами.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from .changes import NOT_A_STATE
from .host import Host, PeriodicSave
from .source import EntityState, StateSource

STORE_KEY = "mega_home_alerts"
DOMAINS = frozenset({"sensor", "binary_sensor"})
OPS = frozenset({"on", "off", "eq", "gt", "lt"})
FIELDS = frozenset({"op", "value", "holdSec", "cooldownSec", "enabled"})
# Тело правила с телефона — меньше килобайта: локальный контур без аутентификации, и диск
# объекта не должен заполняться чем попало (тот же довод, что у фото, `photos.py`).
MAX_RULE_BYTES = 1024
MAX_SEC = 86400
MIN_COOLDOWN_SEC = 60
# Сколько событие ждёт подтверждения менеджера (`device_events.publish`, `ttl`): звонку минута —
# предел, а протечка при моргнувшем интернете обязана дойти, пусть и с пометкой о задержке.
ALERT_TTL_S = 3600.0

Publish = Callable[[str, dict[str, Any]], None]
Later = Callable[[float, Callable[[], None]], Callable[[], None]]


def alert_keys(config: dict[str, Any] | None) -> list[str]:
    """Id плиток, у которых МОЖЕТ быть правило: датчики состава."""
    return [
        tile["id"]
        for tile in (config or {}).get("tiles", []) or []
        if tile.get("id") and tile.get("domain") in DOMAINS
    ]


def alert_key_known(config: dict[str, Any] | None, tile_id: str) -> bool:
    return tile_id in alert_keys(config)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rule_valid(payload: Any) -> dict[str, Any] | None:
    """Правило в нашей форме или `None`. Число у «больше/меньше» приводится к числу."""
    if not isinstance(payload, dict) or not set(payload) <= FIELDS or payload.get("op") not in OPS:
        return None
    op, value = payload["op"], payload.get("value")
    hold, cooldown, enabled = payload.get("holdSec", 0), payload.get("cooldownSec"), payload.get("enabled")
    if not isinstance(enabled, bool):
        return None
    for sec, low in ((hold, 0), (cooldown, MIN_COOLDOWN_SEC)):
        if not isinstance(sec, int) or isinstance(sec, bool) or not low <= sec <= MAX_SEC:
            return None
    if op in ("gt", "lt"):
        value = _number(value)
        if value is None:
            return None
    elif op == "eq":
        if isinstance(value, bool) or not isinstance(value, (str, int, float)) or value == "":
            return None
    else:
        value = None
    return {"op": op, "value": value, "holdSec": hold, "cooldownSec": cooldown, "enabled": enabled}


def matches(rule: dict[str, Any], state: EntityState | None) -> bool | None:
    """В условии ли состояние; `None` — прибора не видно, оценка прежняя."""
    if state is None or state.state in NOT_A_STATE:
        return None
    raw, op, value = state.state, rule["op"], rule.get("value")
    if op == "on":
        return raw == "on"
    if op == "off":
        return raw == "off"
    actual = _number(raw)
    if op == "eq":
        wanted = _number(value)
        return actual == wanted if actual is not None and wanted is not None else raw == str(value)
    if actual is None:
        return False
    return actual > value if op == "gt" else actual < value


def _call_later(delay: float, fn: Callable[[], None]) -> Callable[[], None]:
    return asyncio.get_running_loop().call_later(delay, fn).cancel


class Alerts:
    """Правила жильца по датчикам: хранение, подписка, срабатывание."""

    def __init__(
        self,
        env: Host,
        source: StateSource,
        publish: Publish,
        clock: Callable[[], float] = time.time,
        later: Later = _call_later,
    ) -> None:
        self._store = env.store(STORE_KEY)
        self._saver = PeriodicSave(self._store, self._snapshot)
        self._source, self._publish, self._clock, self._later = source, publish, clock, later
        self._rules: dict[str, dict[str, Any]] = {}
        # tile → {"in": учтён ли текущий вход, "last": время прошлого срабатывания}
        self._seen: dict[str, dict[str, Any]] = {}
        # Плитки, чья выдержка идёт: tile → отмена таймера.
        self._holds: dict[str, Callable[[], None]] = {}
        # entity_id → плитки с включённым правилом (одна сущность бывает на двух плитках).
        self._tiles: dict[str, list[str]] = {}
        self._entity: dict[str, str] = {}
        self._unsubscribe: Callable[[], None] | None = None

    async def async_load(self) -> None:
        cached = await self._store.async_load() or {}
        rules = cached.get("rules") if isinstance(cached, dict) else None
        seen = cached.get("seen") if isinstance(cached, dict) else None
        self._rules = {
            str(k): v for k, v in (rules or {}).items() if isinstance(v, dict) and rule_valid(v) == v
        }
        self._seen = {str(k): v for k, v in (seen or {}).items() if isinstance(v, dict) and k in self._rules}

    def all(self) -> dict[str, dict[str, Any]]:
        return {tile: dict(rule) for tile, rule in self._rules.items()}

    async def save(self, tile: str, rule: dict[str, Any]) -> None:
        """Записать правило (уже проверенное `rule_valid`). Точка отсчёта — текущее состояние."""
        self._rules[tile] = dict(rule)
        self._cancel_hold(tile)
        entity_id = self._entity.get(tile)
        current = self._source.get(entity_id) if entity_id else None
        self._seen[tile] = {"in": matches(rule, current) is True, "last": self._seen.get(tile, {}).get("last")}
        # ⚠ Сразу на диск, а не раз в период: это единственная копия выбора жильца.
        await self.flush()

    async def delete(self, tile: str) -> bool:
        if self._rules.pop(tile, None) is None:
            return False
        self._seen.pop(tile, None)
        self._cancel_hold(tile)
        await self.flush()
        return True

    async def prune(self, keys: list[str]) -> None:
        """Правила плиток, которых в свежем составе нет.

        ⚠ Только по конфигу ОТ МЕНЕДЖЕРА и только при непустом составе — как фото и кадры
        (`coordinator._async_prune_resident_files`): состав без датчиков скорее сбой, чем пустой дом.
        """
        if not keys:
            return
        gone = [tile for tile in self._rules if tile not in keys]
        for tile in gone:
            self._rules.pop(tile)
            self._seen.pop(tile, None)
            self._cancel_hold(tile)
        if gone:
            await self.flush()

    async def flush(self) -> None:
        await self._saver.flush()

    def track(self, config: dict[str, Any] | None) -> None:
        """Подписаться на сущности включённых правил (зовётся на смену состава и правил).

        ⚠ Идущая выдержка ПЕРЕЖИВАЕТ пересборку: координатор зовёт это на каждый опрос, и
        отменённый и заведённый заново таймер считал бы срок от опроса, а не от входа.
        """
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        self._entity = {
            tile["id"]: tile["entityId"]
            for tile in (config or {}).get("tiles", []) or []
            if tile.get("id") and tile.get("entityId") and tile.get("domain") in DOMAINS
        }
        self._tiles = {}
        for tile, rule in self._rules.items():
            entity_id = self._entity.get(tile)
            if rule["enabled"] and entity_id:
                self._tiles.setdefault(entity_id, []).append(tile)
        watched = {tile for tiles in self._tiles.values() for tile in tiles}
        for tile in [t for t in self._holds if t not in watched]:
            self._cancel_hold(tile)
        # Вход, случившийся без нас (HA лежал, правило ещё ждало выдержку), — вход.
        for entity_id in self._tiles:
            self._on_change(entity_id, self._source.get(entity_id))
        if self._tiles:
            self._unsubscribe = self._source.subscribe(sorted(self._tiles), self._on_change)

    def stop(self) -> None:
        for tile in list(self._holds):
            self._cancel_hold(tile)
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None

    def _on_change(self, entity_id: str, state: EntityState | None) -> None:
        for tile in self._tiles.get(entity_id, []):
            self._evaluate(tile, state)

    def _evaluate(self, tile: str, state: EntityState | None) -> None:
        rule = self._rules.get(tile)
        hit = matches(rule, state) if rule else None
        if hit is None:
            return
        seen = self._seen.setdefault(tile, {"in": False, "last": None})
        if not hit:
            self._cancel_hold(tile)
            if seen["in"]:
                seen["in"] = False
                self._saver.touch()
            return
        if seen["in"] or tile in self._holds:
            return
        if rule["holdSec"]:
            self._holds[tile] = self._later(rule["holdSec"], lambda: self._held(tile))
        else:
            self._fire(tile, state)

    def _held(self, tile: str) -> None:
        self._holds.pop(tile, None)
        state = self._source.get(self._entity.get(tile, ""))
        rule = self._rules.get(tile)
        if rule and matches(rule, state) is True:
            self._fire(tile, state)

    def _fire(self, tile: str, state: EntityState | None) -> None:
        rule, seen = self._rules[tile], self._seen[tile]
        now = self._clock()
        seen["in"] = True
        if seen.get("last") is None or now - seen["last"] >= rule["cooldownSec"]:
            seen["last"] = now
            unit = state.attributes.get("unit_of_measurement") if state else None
            self._publish(
                tile, {"op": rule["op"], "value": rule["value"], "actual": state.state if state else None, "unit": unit}
            )
        self._saver.touch()

    def _cancel_hold(self, tile: str) -> None:
        cancel = self._holds.pop(tile, None)
        if cancel:
            cancel()

    def _snapshot(self) -> dict[str, Any]:
        return {"rules": self.all(), "seen": {tile: dict(v) for tile, v in self._seen.items()}}
