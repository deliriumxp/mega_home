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
