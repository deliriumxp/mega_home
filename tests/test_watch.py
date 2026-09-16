"""Живые состояния по каналу: что дом отдаёт менеджеру для жильца снаружи.

Проверяется ровно то, что отличает этот путь от локального потока (`events.py`,
его покрывает `test_events.py`): кадры канала, порядок «подписка → снимок»,
переполнение и ответ на операцию — по нему менеджер решает, умеет ли дом
подписку вовсе.
"""

from __future__ import annotations

import asyncio
from typing import Any

from homeassistant.core import State
from homeassistant.helpers import event as event_helper

from mega_home import ops
from mega_home.link import ManagerLink
from mega_home.watch import LinkWatch

CONFIG: dict[str, Any] = {
    "version": "sha256:abc",
    "tiles": [{"id": "t1", "domain": "light", "entityId": "light.kitchen"}],
}


class _Socket:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, payload: dict, **kwargs: Any) -> None:
        self.sent.append(payload)


class _Coordinator:
    def __init__(self) -> None:
        self.data = dict(CONFIG)
        self.listener: Any = None

    @property
    def version(self) -> str:
        return self.data["version"]

    def async_add_listener(self, callback: Any) -> Any:
        self.listener = callback
        return lambda: None


class _Event:
    def __init__(self, entity_id: str, new_state: Any) -> None:
        self.data = {"entity_id": entity_id, "new_state": new_state}


def _patch_states(order: list[str]):
    original = ops.states

    def fake(hass, coordinator):
        order.append("snapshot")
        return {"connected": True, "entities": []}

    ops.states = fake
    return lambda: setattr(ops, "states", original)


def test_снимок_уходит_после_подписки_и_первым_кадром():
    order: list[str] = []
    restore = _patch_states(order)
    track = event_helper.async_track_state_change_event
    track.calls.clear()
    socket = _Socket()

    async def run():
        watch = LinkWatch(object(), _Coordinator(), socket)
        watch.start()
        await asyncio.sleep(0)
        # ⚠ Подписка раньше снимка: изменение между ними иначе потерялось бы.
        assert track.calls, "подписки нет"
        watch.stop()

    try:
        asyncio.run(run())
    finally:
        restore()
    assert order == ["snapshot"]
    assert socket.sent[0] == {
        "t": "watch",
        "event": "states",
        "payload": {"connected": True, "entities": []},
    }


def test_изменение_состояния_уходит_кадром_канала():
    restore = _patch_states([])
    socket = _Socket()

    async def run():
        watch = LinkWatch(object(), _Coordinator(), socket)
        watch.start()
        await asyncio.sleep(0)
        watch._stream._on_state(_Event("light.kitchen", State("on", {"brightness": 128})))
        await asyncio.sleep(0)
        watch.stop()

    try:
        asyncio.run(run())
    finally:
        restore()
    entity = [frame for frame in socket.sent if frame["event"] == "entity"]
    assert len(entity) == 1
    assert entity[0]["payload"]["id"] == "t1"
    assert entity[0]["payload"]["state"] == {"value": "on"}


def test_переполнение_отдаёт_свежий_снимок_а_не_рвёт_подписку():
    from mega_home import events

    order: list[str] = []
    restore = _patch_states(order)
    limit = events.QUEUE_LIMIT
    events.QUEUE_LIMIT = 1
    socket = _Socket()

    async def run():
        watch = LinkWatch(object(), _Coordinator(), socket)
        watch.start()
        for _ in range(3):
            await asyncio.sleep(0)
        stream = watch._stream
        for _ in range(3):
            stream._put("entity", {"id": "t1"})
        for _ in range(5):
            await asyncio.sleep(0)
        assert watch.running
        watch.stop()

    try:
        asyncio.run(run())
    finally:
        restore()
        events.QUEUE_LIMIT = limit
    # Первый снимок — при подписке, второй — вместо потерянных событий.
    assert order == ["snapshot", "snapshot"]
    assert [frame["event"] for frame in socket.sent].count("states") == 2


def _link() -> ManagerLink:
    instance = ManagerLink.__new__(ManagerLink)
    instance._hass = object()
    instance._coordinator = _Coordinator()
    instance._answers = set()
    instance._watch = None
    return instance


def test_операция_включает_и_выключает_подписку_с_ответом():
    restore = _patch_states([])
    socket = _Socket()
    instance = _link()

    async def run():
        await instance._handle({"t": "req", "id": "r1", "op": "watch", "payload": {"on": True}}, socket)
        assert instance._watch is not None and instance._watch.running
        await asyncio.sleep(0)
        await instance._handle({"t": "req", "id": "r2", "op": "watch", "payload": {"on": False}}, socket)
        assert instance._watch is None

    try:
        asyncio.run(run())
    finally:
        restore()
    answers = [frame for frame in socket.sent if frame.get("t") == "res"]
    # ⚠ Ответ обязателен: по нему менеджер отличает дом, умеющий подписку, от
    # дома со старым кодом («Неизвестная операция»).
    assert answers == [
        {"t": "res", "id": "r1", "ok": True, "payload": {"on": True}},
        {"t": "res", "id": "r2", "ok": True, "payload": {"on": False}},
    ]


def test_повторное_включение_не_заводит_вторую_подписку_но_шлёт_снимок():
    restore = _patch_states([])
    track = event_helper.async_track_state_change_event
    track.calls.clear()
    socket = _Socket()
    instance = _link()

    async def run():
        for request_id in ("r1", "r2"):
            await instance._handle(
                {"t": "req", "id": request_id, "op": "watch", "payload": {"on": True}}, socket
            )
            for _ in range(3):
                await asyncio.sleep(0)
        instance._watch.stop()

    try:
        asyncio.run(run())
    finally:
        restore()
    # Подписка в доме одна, сколько бы телефонов ни смотрело…
    assert len(track.calls) == 1
    # …а новый телефон получает весь дом: менеджер копии не держит.
    assert [frame.get("event") for frame in socket.sent].count("states") == 2
