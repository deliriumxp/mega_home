"""Поток состояний: что именно уходит подключённому приложению.

Проверяется наполнение очереди, а не транспорт aiohttp: SSE-обёртка — это
`response.write` в цикле, а вся смысловая часть в том, какие события кладутся в
очередь и на какие сущности стоит подписка.
"""

from __future__ import annotations

import asyncio
from typing import Any

from homeassistant.core import State
from homeassistant.helpers import event as event_helper

from mega_home.events import StateStream

CONFIG: dict[str, Any] = {
    "version": "sha256:abc",
    "tiles": [
        {"id": "t1", "roomId": "r1", "name": "Бра", "domain": "light", "entityId": "light.kitchen"},
        {"id": "t2", "roomId": "r1", "name": "Тот же прибор", "domain": "light", "entityId": "light.kitchen"},
        {"id": "t3", "roomId": "r1", "name": "Без сущности", "domain": "switch", "entityId": None},
    ],
}


class _StateEvent:
    """Home Assistant отдаёт событие объектом с полем `data`."""

    def __init__(self, entity_id: str, new_state: Any) -> None:
        self.data = {"entity_id": entity_id, "new_state": new_state}


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


def _drain(stream: StateStream) -> list[tuple[str, Any]]:
    items = []
    while not stream.queue.empty():
        items.append(stream.queue.get_nowait())
    return items


def test_подписка_только_на_сущности_плиток():
    event_helper.async_track_state_change_event.calls.clear()
    stream = StateStream(object(), _Coordinator())
    stream.start()

    entity_ids, _ = event_helper.async_track_state_change_event.calls[-1]
    # Плитка без сущности подписки не даёт, дубликат сущности — одной записи.
    assert entity_ids == ["light.kitchen"]


def test_изменение_состояния_уходит_каждой_своей_плитке():
    stream = StateStream(object(), _Coordinator())
    stream.start()
    stream._on_state(_StateEvent("light.kitchen", State("on", {"brightness": 255})))

    events = _drain(stream)
    assert [name for name, _ in events] == ["entity", "entity"]
    # Один прибор в двух комнатах приложения — законный случай: обе плитки
    # обязаны обновиться, иначе одна из них врёт до перезагрузки страницы.
    assert [payload["id"] for _, payload in events] == ["t1", "t2"]
    # Дом пересылает сырое состояние Home Assistant — толкует его приложение.
    assert events[0][1]["state"] == {"value": "on"}


def test_смена_конфига_переподписывает_и_сообщает_версию():
    event_helper.async_track_state_change_event.calls.clear()
    coordinator = _Coordinator()
    stream = StateStream(object(), coordinator)
    stream.start()

    coordinator.data = {
        "version": "sha256:def",
        "tiles": [{"id": "t9", "domain": "light", "entityId": "light.hall"}],
    }
    coordinator.listener()

    # Инсталлятор добавил розетку — жилец не должен перезагружать страницу, а
    # плитка с новым entity_id обязана начать стримить.
    entity_ids, _ = event_helper.async_track_state_change_event.calls[-1]
    assert entity_ids == ["light.hall"]
    assert _drain(stream) == [("config", {"configVersion": "sha256:def"})]


def test_переполнение_очереди_закрывает_поток_а_не_копит_память():
    stream = StateStream(object(), _Coordinator())
    stream.queue = asyncio.Queue(2)
    for _ in range(5):
        stream._put("entity", {"id": "t1"})

    # Клиент не читает: поток закрывается сигналом, EventSource переподключится
    # и возьмёт полный снимок — это дешевле, чем копить события в памяти HA.
    assert _drain(stream)[-1] == ("overflow", None)
