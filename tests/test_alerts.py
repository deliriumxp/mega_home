"""Уведомления по датчикам (`core/alerts.py`): правило жильца, которое дом исполняет сам.

Срабатывание — только при ВХОДЕ в условие, с выдержкой и паузой; `unavailable`/`unknown` ничего
не меняют; память переживает перезапуск; сохранение правила само не срабатывает.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from fake_host import FakeHost, FakeSource
from mega_home.core.alerts import (
    STORE_KEY,
    Alerts,
    alert_key_known,
    alert_keys,
    rule_valid,
)

CONFIG = {
    "tiles": [
        {"id": "t-temp", "entityId": "sensor.temp", "domain": "sensor"},
        {"id": "t-leak", "entityId": "binary_sensor.leak", "domain": "binary_sensor"},
        {"id": "t-light", "entityId": "light.a", "domain": "light"},
    ]
}


@dataclass
class S:
    state: str
    attributes: dict[str, Any] = field(default_factory=dict)


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


class Later:
    """Отложенные вызовы под рукой спеки: `fire()` — «время вышло»."""

    def __init__(self) -> None:
        self.pending: list[list[Any]] = []

    def __call__(self, delay: float, fn: Any) -> Any:
        entry = [delay, fn, True]
        self.pending.append(entry)

        def cancel() -> None:
            entry[2] = False

        return cancel

    def fire(self) -> None:
        due, self.pending = self.pending, []
        for _, fn, alive in due:
            if alive:
                fn()


class Rig:
    def __init__(self, states: dict[str, Any], host: FakeHost | None = None) -> None:
        self.host = host or FakeHost()
        self.source = FakeSource(states)
        self.clock = Clock()
        self.later = Later()
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.alerts = Alerts(
            self.host,
            self.source,
            lambda tile, data: self.sent.append((tile, data)),
            clock=self.clock,
            later=self.later,
        )
        asyncio.run(self.alerts.async_load())
        self.alerts.track(CONFIG)

    def save(self, tile: str, **rule: Any) -> None:
        body = {"op": "gt", "value": 25, "holdSec": 0, "cooldownSec": 900, "enabled": True, **rule}
        asyncio.run(self.alerts.save(tile, rule_valid(body)))
        self.alerts.track(CONFIG)

    def change(self, entity_id: str, state: S) -> None:
        self.source.states[entity_id] = state
        _, on_change = self.source.subscriptions[-1]
        on_change(entity_id, state)


def test_срабатывает_при_входе_в_условие_и_не_повторяет_пока_в_нём() -> None:
    rig = Rig({"sensor.temp": S("20", {"unit_of_measurement": "°C"})})
    rig.save("t-temp")
    rig.change("sensor.temp", S("26", {"unit_of_measurement": "°C"}))
    rig.change("sensor.temp", S("27", {"unit_of_measurement": "°C"}))
    assert rig.sent == [("t-temp", {"op": "gt", "value": 25, "actual": "26", "unit": "°C"})]


def test_повторный_вход_только_после_паузы() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp", cooldownSec=900)
    rig.change("sensor.temp", S("26"))
    rig.change("sensor.temp", S("20"))
    rig.clock.now += 60
    rig.change("sensor.temp", S("26"))
    assert len(rig.sent) == 1
    rig.change("sensor.temp", S("20"))
    rig.clock.now += 900
    rig.change("sensor.temp", S("26"))
    assert len(rig.sent) == 2


def test_выдержка_отменяется_выходом_из_условия() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp", holdSec=60)
    rig.change("sensor.temp", S("26"))
    assert rig.sent == []
    rig.change("sensor.temp", S("24"))
    rig.later.fire()
    assert rig.sent == []

    rig.change("sensor.temp", S("26"))
    # Новое значение внутри условия выдержку не перезапускает.
    rig.change("sensor.temp", S("27"))
    assert len([entry for entry in rig.later.pending if entry[2]]) == 1
    rig.later.fire()
    assert len(rig.sent) == 1
    assert rig.sent[0][1]["actual"] == "27"


def test_обновление_состава_не_сбрасывает_идущую_выдержку() -> None:
    # Координатор зовёт `track` на каждый опрос — выдержка обязана дожить до своего срока.
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp", holdSec=60)
    rig.change("sensor.temp", S("26"))
    rig.alerts.track(CONFIG)
    # Тот же таймер, а не отменённый и заведённый заново (новый считал бы срок с опроса).
    assert [entry[2] for entry in rig.later.pending] == [True]
    rig.later.fire()
    assert len(rig.sent) == 1


def test_недоступность_не_вход_и_не_выход() -> None:
    rig = Rig({"binary_sensor.leak": S("off")})
    rig.save("t-leak", op="on", value=None)
    rig.change("binary_sensor.leak", S("on"))
    rig.change("binary_sensor.leak", S("unavailable"))
    rig.change("binary_sensor.leak", S("on"))
    assert len(rig.sent) == 1
    # Из недоступности в норму — выход; снова протечка — новый вход (паузу пропускаем).
    rig.change("binary_sensor.leak", S("off"))
    rig.clock.now += 900
    rig.change("binary_sensor.leak", S("unknown"))
    rig.change("binary_sensor.leak", S("on"))
    assert len(rig.sent) == 2


def test_сохранение_правила_само_не_срабатывает() -> None:
    rig = Rig({"sensor.temp": S("30")})
    rig.save("t-temp")
    rig.change("sensor.temp", S("31"))
    assert rig.sent == []


def test_выключенное_правило_молчит() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp", enabled=False)
    assert not any("sensor.temp" in ids for ids, _ in rig.source.subscriptions)
    assert rig.sent == []


def test_перезапуск_не_сбрасывает_паузу_и_не_повторяет_старое() -> None:
    first = Rig({"sensor.temp": S("20")})
    first.save("t-temp")
    first.change("sensor.temp", S("26"))
    assert len(first.sent) == 1
    asyncio.run(first.alerts.flush())

    second = Rig({"sensor.temp": S("26")}, host=first.host)
    assert second.sent == []
    assert second.alerts.all()["t-temp"]["op"] == "gt"


def test_вход_за_время_простоя_срабатывает_на_старте() -> None:
    first = Rig({"sensor.temp": S("20")})
    first.save("t-temp")
    asyncio.run(first.alerts.flush())

    second = Rig({"sensor.temp": S("30")}, host=first.host)
    assert [tile for tile, _ in second.sent] == ["t-temp"]


def test_снятая_плитка_чистится_только_по_составу_с_плитками() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp")
    asyncio.run(rig.alerts.prune([]))
    assert "t-temp" in rig.alerts.all()
    asyncio.run(rig.alerts.prune(["t-leak"]))
    assert rig.alerts.all() == {}


def test_удаление() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp")
    assert asyncio.run(rig.alerts.delete("t-temp")) is True
    assert asyncio.run(rig.alerts.delete("t-temp")) is False
    assert rig.host.stores[STORE_KEY].data["rules"] == {}


def test_ключи_только_датчики_состава() -> None:
    assert alert_keys(CONFIG) == ["t-temp", "t-leak"]
    assert alert_key_known(CONFIG, "t-leak")
    assert not alert_key_known(CONFIG, "t-light")
    assert not alert_key_known(CONFIG, "нет")


def test_форма_правила() -> None:
    ok = {"op": "lt", "value": "18.5", "holdSec": 60, "cooldownSec": 600, "enabled": True}
    assert rule_valid(ok) == {**ok, "value": 18.5}
    assert rule_valid({**ok, "op": "between"}) is None
    assert rule_valid({**ok, "value": "тепло"}) is None  # больше/меньше — только число
    assert rule_valid({**ok, "op": "eq", "value": "dry"})["value"] == "dry"
    assert rule_valid({**ok, "op": "on", "value": None})["value"] is None
    assert rule_valid({**ok, "cooldownSec": 10}) is None  # не чаще раза в минуту
    assert rule_valid({**ok, "holdSec": -1}) is None
    assert rule_valid({**ok, "enabled": "да"}) is None
    assert rule_valid({**ok, "extra": 1}) is None
    assert rule_valid([]) is None


def test_порог_nan_и_бесконечность_не_число() -> None:
    ok = {"op": "gt", "value": 25, "holdSec": 0, "cooldownSec": 900, "enabled": True}
    assert rule_valid({**ok, "value": "nan"}) is None
    assert rule_valid({**ok, "value": "inf"}) is None


def test_испорченная_память_срабатываний_не_роняет_правило() -> None:
    host = FakeHost()
    first = Rig({"sensor.temp": S("20")}, host=host)
    first.save("t-temp")
    asyncio.run(first.alerts.flush())
    host.stores[STORE_KEY].data["seen"]["t-temp"] = {"in": False, "last": "вчера"}

    second = Rig({"sensor.temp": S("20")}, host=host)
    second.change("sensor.temp", S("26"))
    assert [tile for tile, _ in second.sent] == ["t-temp"]


def test_срабатывание_пишется_на_диск_сразу() -> None:
    rig = Rig({"sensor.temp": S("20")})
    rig.save("t-temp")
    rig.change("sensor.temp", S("26"))
    assert "mega_home alerts flush" in rig.host.spawned
