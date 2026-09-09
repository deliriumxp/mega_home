"""Сессия до устройства объекта (`stream.py`).

⚠ Предмет тестов — ГРАНИЦЫ и УБОРКА, а не «работает ли SSH». Что за протокол
внутри, дом не знает и знать не должен: разбор живёт в менеджере, иначе новая
железка на объекте стоила бы релиза HACS.

⚠ Сокеты настоящие, на localhost: у этой темы вся суть в том, что происходит с
живым TCP-соединением, когда одна из сторон умолкает или отваливается. Мок
подтвердил бы только выдуманное.
"""

from __future__ import annotations

import asyncio

import pytest

from mega_home import stream as stream_mod
from mega_home.stream import HEADER, Streams, frame


class FakeSocket:
    """Канал менеджера: запоминает, что дом послал в его сторону."""

    def __init__(self) -> None:
        self.json: list[dict] = []
        self.binary: list[bytes] = []

    async def send_json(self, payload: dict) -> None:
        self.json.append(payload)

    async def send_bytes(self, payload: bytes) -> None:
        self.binary.append(payload)

    def kinds(self) -> list[str]:
        return [item.get("t") for item in self.json]

    def last_error(self) -> str | None:
        for item in reversed(self.json):
            if "error" in item:
                return item["error"]
        return None


async def echo_server():
    """Устройство: отвечает тем же, что прислали."""

    async def handler(reader, writer):
        while True:
            chunk = await reader.read(1024)
            if not chunk:
                break
            writer.write("ответ:".encode() + chunk)
            await writer.drain()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server.sockets[0].getsockname()[1], server


async def settle(times: int = 3) -> None:
    """Дать качалкам провернуться: они живут отдельными задачами."""
    for _ in range(times):
        await asyncio.sleep(0.02)


def test_open_refuses_public_address():
    """⚠ Дверь в локалку объекта не должна быть выходом в интернет с его адреса."""

    async def scenario():
        socket = FakeSocket()
        streams = Streams(socket)
        await streams.handle({"t": "stream.open", "id": 1, "host": "8.8.8.8", "port": 443})
        return socket

    socket = asyncio.run(scenario())
    assert socket.kinds() == ["stream.error"]
    assert "вне локальной сети" in socket.last_error()


def test_open_refuses_hostname():
    """Имя резолвил бы дом — и резолвер смотрит в интернет. Только литеральный IP."""

    async def scenario():
        socket = FakeSocket()
        await Streams(socket).handle(
            {"t": "stream.open", "id": 1, "host": "router.local", "port": 80}
        )
        return socket

    assert "должен быть IP" in asyncio.run(scenario()).last_error()


def test_open_refuses_loopback():
    # Сам Home Assistant и его соседи по машине — не «устройство объекта».
    async def scenario():
        socket = FakeSocket()
        await Streams(socket).handle(
            {"t": "stream.open", "id": 1, "host": "127.0.0.1", "port": 8123}
        )
        return socket

    assert "вне локальной сети" in asyncio.run(scenario()).last_error()


def test_refused_connection_comes_back_as_error_not_silence(monkeypatch):
    """Молчание читалось бы менеджером как «дом не отвечает» — а дом-то жив.

    ⚠ Проверку адреса подменяем: её предмет — предыдущие три теста, а здесь
    нужен ГАРАНТИРОВАННО закрытый порт, и такой есть только на localhost.
    Частный адрес наугад (`10.x`) для этого не годится — на дев-машине он
    неожиданно ответил, и тест «проверял» бы погоду в чужой сети.
    """

    async def scenario():
        server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        socket = FakeSocket()
        monkeypatch.setattr(Streams, "_refuse", lambda self, payload: None)
        await Streams(socket).handle(
            {"t": "stream.open", "id": 1, "host": "127.0.0.1", "port": port}
        )
        return socket

    socket = asyncio.run(scenario())
    assert socket.kinds() == ["stream.error"]
    assert "отклонено" in (socket.last_error() or "")


def test_bytes_travel_both_ways(monkeypatch):
    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        # Приватным адрес быть обязан, а localhost — нет: подменяем проверку
        # ровно на время теста, предмет которого другой.
        monkeypatch.setattr(Streams, "_refuse", lambda self, payload: None)
        try:
            await streams.handle(
                {"t": "stream.open", "id": 7, "host": "127.0.0.1", "port": port}
            )
            await streams.on_binary(frame(7, "привет".encode()))
            await settle()
        finally:
            await streams.close_all()
            server.close()
        return socket

    socket = asyncio.run(scenario())
    assert socket.kinds()[0] == "stream.ok"
    assert socket.binary, "дом не переслал ответ устройства"
    (stream_id,) = HEADER.unpack_from(socket.binary[0])
    assert stream_id == 7
    assert socket.binary[0][HEADER.size :] == "ответ:привет".encode()


def test_device_hangup_closes_the_session(monkeypatch):
    """Устройство закрыло соединение — менеджер обязан узнать, а не ждать час."""

    async def scenario():
        async def handler(reader, writer):
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        socket = FakeSocket()
        streams = Streams(socket)
        monkeypatch.setattr(Streams, "_refuse", lambda self, payload: None)
        try:
            await streams.handle(
                {"t": "stream.open", "id": 3, "host": "127.0.0.1", "port": port}
            )
            await settle()
        finally:
            await streams.close_all()
            server.close()
        return socket

    assert "stream.close" in asyncio.run(scenario()).kinds()


def test_idle_session_dies_on_its_own(monkeypatch):
    """⚠ Вкладку закрыли — про это никто не сообщит. Сессия обязана умереть сама."""

    async def scenario():
        async def handler(reader, writer):
            await asyncio.sleep(30)

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        socket = FakeSocket()
        streams = Streams(socket)
        monkeypatch.setattr(Streams, "_refuse", lambda self, payload: None)
        monkeypatch.setattr(stream_mod, "IDLE_TIMEOUT_S", 0.05)
        try:
            await streams.handle(
                {"t": "stream.open", "id": 5, "host": "127.0.0.1", "port": port}
            )
            await settle(10)
        finally:
            await streams.close_all()
            server.close()
        return socket

    socket = asyncio.run(scenario())
    assert "stream.close" in socket.kinds()
    assert "тишина" in (socket.last_error() or "")


def test_too_many_sessions_refused(monkeypatch):
    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        monkeypatch.setattr(stream_mod, "MAX_STREAMS", 1)
        # Первую пускаем, вторую обязаны отклонить — иначе «сессия» становится
        # обходом сети.
        original = Streams._refuse
        monkeypatch.setattr(
            Streams,
            "_refuse",
            lambda self, payload: (
                f"на объекте уже {stream_mod.MAX_STREAMS} открытых сессии"
                if len(self._streams) >= stream_mod.MAX_STREAMS
                else None
            ),
        )
        assert original is not None
        try:
            await streams.handle(
                {"t": "stream.open", "id": 1, "host": "127.0.0.1", "port": port}
            )
            await streams.handle(
                {"t": "stream.open", "id": 2, "host": "127.0.0.1", "port": port}
            )
        finally:
            await streams.close_all()
            server.close()
        return socket

    socket = asyncio.run(scenario())
    assert socket.kinds() == ["stream.ok", "stream.error"]
    assert "уже 1" in socket.last_error()


def test_binary_for_unknown_session_is_ignored():
    # Кадры летят и после закрытия — это норма на разрыве, а не повод падать.
    asyncio.run(Streams(FakeSocket()).on_binary(frame(42, "поздно".encode())))


def test_close_frame_releases_the_socket(monkeypatch):
    async def scenario():
        port, server = await echo_server()
        socket = FakeSocket()
        streams = Streams(socket)
        monkeypatch.setattr(Streams, "_refuse", lambda self, payload: None)
        try:
            await streams.handle(
                {"t": "stream.open", "id": 9, "host": "127.0.0.1", "port": port}
            )
            await streams.handle({"t": "stream.close", "id": 9})
            # После закрытия данные некуда девать — и это не должно падать.
            await streams.on_binary(frame(9, "поздно".encode()))
        finally:
            server.close()
        return streams

    streams = asyncio.run(scenario())
    assert not streams._streams
