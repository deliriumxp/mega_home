"""Прибор с состоянием по опросу (`core/polled_device.py`).

Ответы — как у контроллера вентиляции офиса (`json?state`, запись `POST /conf`
телом «ключ=значение »), но смысл полей приходит описанием: проверяется раскладка,
множитель, запись и «недоступен» не с первой потери.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from mega_home.core.polled_device import MISSES_UNAVAILABLE, PolledDevice, spec_of

STATE = {"state": {"set": {"power": 1, "temp": 180, "fan": 2}, "ai": {"channel": 17.3}, "algo": {"status": 4}, "alarms": 0}}

BLOCK = {
    "id": "vent", "name": "Вентиляция", "access": "atlas-1",
    "read": {"path": "/json?state"}, "every": 60,
    "fields": {
        "power": {"path": "state.set.power"},
        "target": {"path": "state.set.temp", "scale": 0.1},
        "current": {"path": ["state", "ai", "channel"]},
        "fan": {"path": "state.set.fan"},
        "missing": {"path": "state.nope"},
    },
    "write": {"path": "/conf", "body": "{key}={value} "},
    "writes": {"power": {"key": "set-power"}, "target": {"key": "set-temp", "scale": 10}, "fan": {"key": "set-fan"}},
    "climate": {"onMode": "fan_only"},
}


class Device:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail = False
        self.state = json.loads(json.dumps(STATE))

    async def perform(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        if self.fail:
            raise ConnectionError("потерян кадр")
        if payload["method"] == "POST":
            return {"status": 200, "body": '{"result":"ok"}'}
        return {"status": 200, "body": json.dumps(self.state)}


def make(fake: Device, changes: list[int] | None = None, request: dict[str, Any] | None = None) -> PolledDevice:
    spec = spec_of(BLOCK)
    assert spec is not None
    return PolledDevice(
        spec, lambda: request if request is not None else {"host": "192.168.88.125", "port": 80},
        fake.perform, lambda: (changes if changes is not None else []).append(1),
    )


def test_разбор_описания_и_мусор() -> None:
    spec = spec_of(BLOCK)
    assert spec is not None
    assert spec.fields["target"].scale == 0.1 and spec.fields["current"].path == ("state", "ai", "channel")
    assert spec.extra == {"climate": {"onMode": "fan_only"}}
    assert spec_of({"id": "x", "access": "a", "read": {"path": "json"}}) is None
    assert spec_of({"id": "x", "read": {"path": "/json"}}) is None
    assert spec_of("мусор") is None
    # Опрос чаще двух секунд описание не выпросит.
    assert spec_of({**BLOCK, "every": 0.1}).every == 2.0  # type: ignore[union-attr]


def test_поля_по_путям_с_множителем() -> None:
    fake, changes = Device(), []
    device = make(fake, changes)
    asyncio.run(device.refresh())
    assert device.available
    assert device.values == {"power": 1, "target": 18.0, "current": 17.3, "fan": 2, "missing": None}
    assert fake.calls[0] == {"host": "192.168.88.125", "port": 80, "kind": "http", "method": "GET", "path": "/json?state"}
    assert changes == [1]
    # Тот же ответ — не изменение: сущность не пишет одно состояние в историю каждые 10 с.
    asyncio.run(device.refresh())
    assert changes == [1]


def test_запись_телом_описания_и_внеочередной_опрос() -> None:
    fake = Device()
    device = make(fake)

    async def run() -> None:
        await device.write("target", 18.5)
        await device.write("fan", 3)
        await device.write("power", 0)

    asyncio.run(run())
    bodies = [c["body"] for c in fake.calls if c["method"] == "POST"]
    # Уставка 18,5 × 10 — «185», без хвоста «.0»: контроллер ждёт целое.
    assert bodies == ["set-temp=185 ", "set-fan=3 ", "set-power=0 "]
    assert all(c["path"] == "/conf" for c in fake.calls)


def test_незнакомая_запись_и_отказ_прибора() -> None:
    fake = Device()
    device = make(fake)

    async def refused(payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": 404, "body": "{\"error\":\"unknown url (404)\"}"}

    try:
        asyncio.run(device.write("humid", 40))
    except ValueError:
        pass
    else:
        raise AssertionError("запись поля без описания должна отказать")
    device._perform = refused  # noqa: SLF001
    try:
        asyncio.run(device.write("power", 1))
    except ConnectionError:
        pass
    else:
        raise AssertionError("отказ прибора в записи должен дойти до сущности")


def test_недоступен_только_после_нескольких_потерь() -> None:
    fake, changes = Device(), []
    device = make(fake, changes)
    asyncio.run(device.refresh())
    fake.fail = True
    for _ in range(MISSES_UNAVAILABLE - 1):
        asyncio.run(device.refresh())
        assert device.available
    asyncio.run(device.refresh())
    assert not device.available and device.why == "ConnectionError"
    assert changes == [1, 1]
    fake.fail = False
    asyncio.run(device.refresh())
    assert device.available and changes == [1, 1, 1]


def test_без_доступа_в_конфиге_прибор_не_отвечает() -> None:
    fake = Device()
    spec = spec_of(BLOCK)
    assert spec is not None
    device = PolledDevice(spec, lambda: None, fake.perform, lambda: None)
    for _ in range(MISSES_UNAVAILABLE):
        asyncio.run(device.refresh())
    assert not device.available and fake.calls == [] and device.why == "LookupError"


def test_цикл_опроса_просыпается_после_записи() -> None:
    fake = Device()
    device = make(fake)

    async def run() -> None:
        device.start()
        await asyncio.sleep(0.05)
        await device.write("fan", 4)
        await asyncio.sleep(0.05)
        await device.shutdown()

    asyncio.run(run())
    reads = [c for c in fake.calls if c["method"] == "GET"]
    # Опрос при старте и сразу после записи, а не через минуту описания.
    assert len(reads) == 2
