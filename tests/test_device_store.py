"""Хранилище событий устройств (`device_store.DeviceEventStore`) — часть F плана."""

from __future__ import annotations

import asyncio
import time

from fake_host import FakeHost
from mega_home.core.device_store import CAP_PER_DEVICE, RETENTION_S, DeviceEventStore


def _frame(access: str, at: float, event: str = "motion") -> dict:
    return {"id": f"e-{at}", "at": at, "access": access, "source": "cam1", "event": event, "data": {"x": 1}}


def test_запись_и_чтение_новые_первыми() -> None:
    store = DeviceEventStore(FakeHost())
    now = time.time()
    store.add(_frame("dev1", now - 2))
    store.add(_frame("dev1", now - 1))
    store.add(_frame("dev1", now))

    events = store.list("dev1", 10, None)

    assert [e["at"] for e in events] == [now, now - 1, now - 2]
    assert events[0]["event"] == "motion"


def test_чужое_устройство_не_видно() -> None:
    store = DeviceEventStore(FakeHost())
    store.add(_frame("dev1", time.time()))

    assert store.list("dev2", 10, None) == []


def test_потолок_2000_на_устройство() -> None:
    store = DeviceEventStore(FakeHost())
    now = time.time()
    for i in range(CAP_PER_DEVICE + 50):
        store.add(_frame("dev1", now - (CAP_PER_DEVICE + 50 - i)))

    events = store.list("dev1", CAP_PER_DEVICE + 100, None)

    assert len(events) == CAP_PER_DEVICE
    # Обрезаны САМЫЕ СТАРЫЕ — новые первыми, значит последний элемент моложе первого.
    assert events[0]["at"] > events[-1]["at"]


def test_срок_7_суток() -> None:
    store = DeviceEventStore(FakeHost())
    now = time.time()
    store.add(_frame("dev1", now - RETENTION_S - 10))  # старше срока
    store.add(_frame("dev1", now))

    events = store.list("dev1", 10, None)

    assert len(events) == 1
    assert events[0]["at"] == now


def test_before_режет_ленту() -> None:
    store = DeviceEventStore(FakeHost())
    now = time.time()
    store.add(_frame("dev1", now - 2))
    store.add(_frame("dev1", now - 1))
    store.add(_frame("dev1", now))

    events = store.list("dev1", 10, before=now)

    assert [e["at"] for e in events] == [now - 1, now - 2]


def test_запись_откладывается_и_переживает_перезапуск() -> None:
    host = FakeHost()
    store = DeviceEventStore(host)
    frame = _frame("dev1", time.time())
    store.add(frame)

    # `FakeHost.store().async_delay_save` пишет сразу (спеке важен факт, не срок).
    reloaded = DeviceEventStore(host)
    asyncio.run(reloaded.async_load())

    assert reloaded.list("dev1", 10, None)[0]["id"] == frame["id"]
