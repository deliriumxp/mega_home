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
