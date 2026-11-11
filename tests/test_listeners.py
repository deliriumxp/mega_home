"""Входящие источники событий (`core/listeners.py`): журнал не растёт на повторах."""

from __future__ import annotations

import asyncio
import errno
import logging

from mega_home.core import listeners as ls


def test_занятый_порт_пишется_в_журнал_раз_на_смену_причины(caplog, monkeypatch) -> None:
    causes = iter(["Address in use"] * 4 + ["Permission denied"] + [None])

    class _Site:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        async def start(self) -> None:
            cause = next(causes)
            if cause:
                raise OSError(errno.EADDRINUSE, cause)

    async def no_wait(_delay: float) -> None:
        pass

    monkeypatch.setattr(ls.web, "TCPSite", _Site)
    monkeypatch.setattr(ls.asyncio, "sleep", no_wait)
    listeners = ls.Listeners(None, None, None)

    async def go() -> None:
        await listeners._hook_server()  # noqa: SLF001
        await listeners._runner.cleanup()  # noqa: SLF001

    with caplog.at_level(logging.WARNING):
        asyncio.run(go())
    warned = [r.message for r in caplog.records if "занят" in r.message]
    assert len(warned) == 2 and "Permission denied" in warned[1]
    assert listeners.why == ""


def test_источник_работает_пока_идёт_его_цикл(monkeypatch) -> None:
    """Доступность датчиков по событиям (`event_sensors.py`): отказ и пауза перед повтором — «не работает»,
    остановка слушателей снимает все отметки."""
    from types import SimpleNamespace

    from mega_home.core.device_events import EventHub

    hub = EventHub()
    seen: list[tuple[str, str, bool]] = []
    hub.watch_alive(lambda access, source, alive: seen.append((access, source, alive)))
    listeners = ls.Listeners(None, None, hub)
    calls = {"n": 0}

    async def worker(_ctx, _descriptor, _source, _spec) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("подписка не поднялась")
        await asyncio.sleep(3600)

    async def no_wait(_delay: float) -> None:
        pass

    async def go() -> None:
        real_sleep = asyncio.sleep
        monkeypatch.setattr(ls.asyncio, "sleep", lambda d: no_wait(d) if d < 3600 else real_sleep(d))
        task = asyncio.ensure_future(listeners._forever(worker, SimpleNamespace(id="cam"), "events", {}, "cam/events"))  # noqa: SLF001
        listeners._tasks.append(task)  # noqa: SLF001
        for _ in range(5):
            await real_sleep(0)
        assert hub.alive("cam", "events")
        await listeners._stop_all()  # noqa: SLF001

    asyncio.run(go())
    assert seen == [("cam", "events", True), ("cam", "events", False), ("cam", "events", True), ("cam", "events", False)]
    assert not hub.alive("cam", "events")
