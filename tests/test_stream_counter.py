"""Счёт сессий ДОМА (`_open_total`) возвращается к нулю, когда вкладка уходит.

⚠ Зачем отдельный файл: соседние тесты проверяют кадры, ушедшие вызывающему, а
общий счёт дома не проверял никто. Потеря одного уменьшения молчит годами: места
у дома кончаются по одному, и однажды ни у кого не открывается ни камера, ни
архив — «дом держит уже N соединений» при пустом доме. Объект упирался в потолок
2026-09-21; этот файл — замок на то, что пути ухода вкладки счёт не теряют.

⚠ Сокеты настоящие, на localhost, как в `test_stream.py`: суть — в том, что
происходит с живым соединением, когда сторона уходит.
"""

from __future__ import annotations

import asyncio

from aiohttp import web

from mega_home.core import stream as stream_mod
from mega_home.core.stream import Streams


class FakeSocket:
    async def send_json(self, payload: dict) -> None:
        pass

    async def send_bytes(self, payload: bytes) -> None:
        pass


def allow_loopback(monkeypatch) -> None:
    from mega_home.core import connect as connect_mod

    monkeypatch.setattr(
        connect_mod, "resolve_address", lambda req: (str(req.get("host")), int(req.get("port")))
    )


async def devices():
    """Устройства: сокет go2rtc (ответил и молчит), обрыв без закрытия, длинный опрос."""

    async def ws(request):
        sock = web.WebSocketResponse()
        await sock.prepare(request)
        await sock.send_str('{"type":"webrtc/answer","value":"x"}')
        async for _ in sock:
            pass
        return sock

    async def ws_drop(request):
        sock = web.WebSocketResponse()
        await sock.prepare(request)
        request.transport.abort()
        return sock

    async def poll(request):
        await asyncio.sleep(30)
        return web.Response(text="{}")

    app = web.Application()
    app.router.add_get("/ws", ws)
    app.router.add_get("/ws_drop", ws_drop)
    app.router.add_get("/poll", poll)
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return site._server.sockets[0].getsockname()[1], runner


def count_after(monkeypatch, tab) -> int:
    allow_loopback(monkeypatch)
    monkeypatch.setattr(stream_mod, "_open_total", 0)

    async def scenario():
        port, runner = await devices()
        try:
            await tab(port)
            await asyncio.sleep(0.2)
        finally:
            await runner.cleanup()
        return stream_mod._open_total

    return asyncio.run(scenario())


def open_frame(stream_id: int, kind: str, port: int, path: str) -> dict:
    return {"t": "stream.open", "id": stream_id, "req": {"kind": kind, "host": "127.0.0.1", "port": port, "path": path}}


def test_вкладка_закрылась_при_живых_сессиях_go2rtc(monkeypatch):
    async def tab(port):
        streams = Streams(FakeSocket())
        for i in range(3):
            await streams.handle(open_frame(i, "ws", port, "/ws"))
        await asyncio.sleep(0.2)
        assert stream_mod._open_total == 3
        await streams.close_all()

    assert count_after(monkeypatch, tab) == 0


def test_вкладка_закрылась_посреди_длинного_опроса(monkeypatch):
    async def tab(port):
        streams = Streams(FakeSocket())
        for i in range(2):
            await streams.handle(open_frame(i, "http", port, "/poll"))
        await asyncio.sleep(0.2)
        await streams.close_all()

    assert count_after(monkeypatch, tab) == 0


def test_устройство_оборвало_соединение(monkeypatch):
    async def tab(port):
        streams = Streams(FakeSocket())
        for i in range(3):
            await streams.handle(open_frame(i, "ws", port, "/ws_drop"))
        await streams.close_all()

    assert count_after(monkeypatch, tab) == 0


def test_вкладка_закрыла_сессии_сама_и_ушла(monkeypatch):
    async def tab(port):
        streams = Streams(FakeSocket())
        for i in range(3):
            await streams.handle(open_frame(i, "ws", port, "/ws"))
        for i in range(3):
            await streams.handle({"t": "stream.close", "id": i})
        await streams.close_all()

    assert count_after(monkeypatch, tab) == 0


def test_вкладка_ушла_пока_сессии_открываются(monkeypatch):
    async def tab(port):
        streams = Streams(FakeSocket())
        for i in range(3):
            await streams.dispatch(open_frame(i, "ws", port, "/ws"))
        await streams.close_all()

    assert count_after(monkeypatch, tab) == 0


def test_штатный_уход_вкладки_при_отмене_обработчика_как_у_ha(monkeypatch):
    """⚠ Та самая утечка объекта 2026-09-21, воспроизведённая настоящим сервером.

    Home Assistant отменяет обработчик запроса, когда клиент закрыл соединение
    (`handler_cancellation=True`), и отмена прилетала в `close_all` посреди
    закрытия первой сессии — вычитание после цикла пропускалось. Четыре вкладки
    по две сессии давали счёт 8 вместо 0, и навсегда.
    """
    allow_loopback(monkeypatch)
    monkeypatch.setattr(stream_mod, "_open_total", 0)

    async def scenario():
        import aiohttp

        port, device = await devices()

        async def door(request):
            sock = web.WebSocketResponse(heartbeat=25)
            await sock.prepare(request)
            await stream_mod.serve(sock)
            return sock

        house = web.Application()
        house.router.add_get("/connect", door)
        runner = web.AppRunner(house, handler_cancellation=True, shutdown_timeout=0.5)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        house_port = site._server.sockets[0].getsockname()[1]
        counts = []
        try:
            async with aiohttp.ClientSession() as client:
                for _ in range(4):
                    tab = await client.ws_connect(f"http://127.0.0.1:{house_port}/connect")
                    for i in range(2):
                        await tab.send_json(open_frame(i + 1, "ws", port, "/ws"))
                    await asyncio.sleep(0.3)
                    await asyncio.wait_for(tab.close(), 3)
                    await asyncio.sleep(0.5)
                    counts.append(stream_mod._open_total)
        finally:
            await runner.cleanup()
            await device.cleanup()
        return counts

    assert asyncio.run(scenario()) == [0, 0, 0, 0]


def test_потолок_дома_покрывает_самый_тяжёлый_дом():
    """Решение заказчика 2026-09-21: 5 телефонов + 5 мониторов по 8 сессий и канал менеджера."""
    heaviest = (5 + 5) * 8 + stream_mod.MAX_STREAMS
    assert stream_mod.TOTAL_STREAMS >= heaviest
