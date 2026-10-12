"""Хозяин без Home Assistant (`host.py`): то, на чём поднимется демон."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path

from mega_home.core.host import PlainHost


def test_хранилище_в_конверте_ha(tmp_path: Path) -> None:
    """Дом, переехавший с HA на демон, поднимается со своим кэшем."""
    host = PlainHost(tmp_path)
    store = host.store("mega_home_agent", 1)

    async def scenario() -> object:
        assert await store.async_load() is None
        await store.async_save({"rules": [1]})
        return await host.store("mega_home_agent").async_load()

    assert asyncio.run(scenario()) == {"rules": [1]}
    envelope = json.loads((tmp_path / "mega_home_agent").read_text("utf-8"))
    assert envelope["key"] == "mega_home_agent"
    assert envelope["version"] == 1
    assert envelope["data"] == {"rules": [1]}
    assert not list(tmp_path.glob("*.tmp"))


def test_таймер_тикает_и_снимается(tmp_path: Path) -> None:
    async def scenario() -> int:
        host = PlainHost(tmp_path)
        ticks: list[object] = []

        async def tick(now: object) -> None:
            ticks.append(now)

        unsub = host.every(timedelta(seconds=0.01), tick)
        # Ждём два тика с запасом, а не фиксированную паузу: под нагрузкой
        # цикл событий медленнее, и спека на время иначе плавает.
        for _ in range(200):
            if len(ticks) >= 2:
                break
            await asyncio.sleep(0.01)
        unsub()
        seen = len(ticks)
        await asyncio.sleep(0.03)
        assert len(ticks) == seen
        await host.close()
        return seen

    assert asyncio.run(scenario()) >= 2


def test_отложенная_запись_сливается_в_одну(tmp_path: Path) -> None:
    """Опрос каждые пять секунд не должен писать на флешку каждый раз."""

    async def scenario() -> object:
        store = PlainHost(tmp_path).store("feed")
        store.async_delay_save(lambda: {"n": 1}, 0.02)
        store.async_delay_save(lambda: {"n": 2}, 0.02)
        assert not (tmp_path / "feed").exists()
        for _ in range(200):
            if (tmp_path / "feed").exists():
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.02)
        return await store.async_load()

    assert asyncio.run(scenario()) == {"n": 2}


def test_запись_раз_в_период_а_не_дребезг(monkeypatch) -> None:
    """Частые изменения не переносят запись: одна отложенная на период, на остановке — сразу."""
    from mega_home.core import host as host_mod

    calls: list[float] = []

    class Store:
        def async_delay_save(self, data_func, delay=0):  # noqa: ANN001
            calls.append(delay)

        async def async_save(self, data):  # noqa: ANN001
            calls.append(-1)

    clock = [1000.0]
    monkeypatch.setattr(host_mod.time, "monotonic", lambda: clock[0])
    saver = host_mod.PeriodicSave(Store(), lambda: {}, 600)
    for step in range(100):
        clock[0] = 1000.0 + step * 5
        saver.touch()
    assert calls == [600]
    clock[0] = 1601.0
    saver.touch()
    assert calls == [600, 600]
    asyncio.run(saver.flush())
    saver.touch()
    assert calls == [600, 600, -1, 600]
