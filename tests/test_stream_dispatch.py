"""Открытие сессии — задачей, а не в цикле чтения канала (`Streams.dispatch`).

⚠ Предмет — ВРЕМЯ цикла чтения и гонка «закрытие обогнало открытие». Пока
`stream.open` ждал TCP до устройства по месту, дом не читал ни одного кадра до
10 с, и жилец получал «дом не на связи» из-за сеанса инженера к молчащей камере.
"""

from __future__ import annotations

import asyncio
import time

from mega_home.core import stream as stream_mod
from mega_home.core.stream import Streams

from tests.test_stream import FakeSocket, allow_loopback, echo_server, settle


def slow_connect(monkeypatch, delay: float) -> None:
    """Устройство, до которого TCP поднимается `delay` секунд (адрес за упавшим коммутатором)."""
    real = asyncio.open_connection

    async def slow(*args, **kwargs):
        await asyncio.sleep(delay)
        return await real(*args, **kwargs)

    monkeypatch.setattr(stream_mod.asyncio, "open_connection", slow)


def test_dispatch_returns_before_the_device_answers(monkeypatch):
    """Цикл чтения канала не ждёт соединения с устройством."""

    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        allow_loopback(monkeypatch)
        slow_connect(monkeypatch, 0.3)
        try:
            started = time.monotonic()
            await streams.dispatch(
                {"t": "stream.open", "id": 1, "req": {"kind": "tcp", "host": "127.0.0.1", "port": port}}
            )
            elapsed = time.monotonic() - started
            # Открытие идёт: подтверждения ещё нет, цикл чтения уже свободен.
            assert socket.kinds() == []
            await asyncio.sleep(0.5)
            assert socket.kinds() == ["stream.ok"]
        finally:
            await streams.close_all()
            server.close()
        return elapsed

    assert asyncio.run(scenario()) < 0.1


def test_close_that_overtakes_open_is_honored(monkeypatch):
    """Менеджер закрыл сессию раньше, чем поднялось соединение — оно гасится, а не живёт до срока молчания."""

    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        allow_loopback(monkeypatch)
        slow_connect(monkeypatch, 0.2)
        before = stream_mod._total()
        try:
            await streams.dispatch(
                {"t": "stream.open", "id": 5, "req": {"kind": "tcp", "host": "127.0.0.1", "port": port}}
            )
            await streams.dispatch({"t": "stream.close", "id": 5})
            await asyncio.sleep(0.4)
            await settle()
            return socket.kinds(), dict(streams._streams), stream_mod._total() - before
        finally:
            await streams.close_all()
            server.close()

    kinds, live, delta = asyncio.run(scenario())
    # Ни подтверждения (менеджер сессию уже забыл), ни живой сессии, ни места в потолке.
    assert kinds == []
    assert live == {}
    assert delta == 0


def test_close_all_cancels_pending_opens(monkeypatch):
    """Канал оборвался посреди открытия — ни задачи, ни сессии после уборки не остаётся."""

    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        allow_loopback(monkeypatch)
        slow_connect(monkeypatch, 0.3)
        try:
            await streams.dispatch(
                {"t": "stream.open", "id": 2, "req": {"kind": "tcp", "host": "127.0.0.1", "port": port}}
            )
            await streams.close_all()
            await asyncio.sleep(0.5)
            return socket.kinds(), dict(streams._streams), set(streams._opening_tasks)
        finally:
            server.close()

    kinds, live, tasks = asyncio.run(scenario())
    assert kinds == []
    assert live == {}
    assert tasks == set()


def test_pending_opens_count_towards_the_ceiling(monkeypatch):
    """Шесть параллельных запросов браузера не обходят потолок, пока соединения ещё поднимаются."""

    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        allow_loopback(monkeypatch)
        slow_connect(monkeypatch, 0.2)
        monkeypatch.setattr(stream_mod, "MAX_STREAMS", 1)
        try:
            for stream_id in (1, 2):
                await streams.dispatch(
                    {
                        "t": "stream.open",
                        "id": stream_id,
                        "req": {"kind": "tcp", "host": "127.0.0.1", "port": port},
                    }
                )
            await asyncio.sleep(0.4)
            return socket.kinds()
        finally:
            await streams.close_all()
            server.close()

    kinds = asyncio.run(scenario())
    assert sorted(kinds) == ["stream.error", "stream.ok"]
