"""Минимальный клиент MQTT 3.1.1 — для доступа вида `mqtt` и подписок на события.

⚠ Свой клиент, а не зависимость: HACS ставит только каталог интеграции, `pip`
на объекте требует интернета, а объекты бывают офлайн
(`docs/home-gateway.md` в менеджере). Поэтому здесь ровно то, что нужно шлюзу:
CONNECT, PUBLISH (QoS 0 и 1), SUBSCRIBE, PING, DISCONNECT.

Формат пакетов — стандарт OASIS «MQTT Version 3.1.1» (2014), разделы 2 и 3:
фиксированный заголовок (тип и флаги в старшем байте, остаток длины — число
переменной длины по 7 бит), строки — длина двумя байтами и UTF-8.
"""

from __future__ import annotations

import asyncio
import ssl
import struct
from typing import Any, Callable

from .const import LOGGER

CONNECT, CONNACK, PUBLISH, PUBACK = 0x10, 0x20, 0x30, 0x40
SUBSCRIBE, SUBACK, PINGREQ, PINGRESP, DISCONNECT = 0x82, 0x90, 0xC0, 0xD0, 0xE0
KEEPALIVE = 60
MAX_PACKET = 1024 * 1024
# Коды отказа CONNACK (3.2.2.3) словами — их читает инсталлятор.
REFUSALS = {
    1: "брокер не принимает версию протокола 3.1.1",
    2: "брокер отверг идентификатор клиента",
    3: "брокер недоступен",
    4: "неверный логин или пароль брокера",
    5: "брокер не разрешил подключение",
}


class MqttError(Exception):
    pass


def _string(text: str) -> bytes:
    raw = text.encode("utf-8")
    return struct.pack(">H", len(raw)) + raw


def _length(size: int) -> bytes:
    out = bytearray()
    while True:
        byte, size = size % 128, size // 128
        out.append(byte | (0x80 if size else 0))
        if not size:
            return bytes(out)


def packet(kind: int, body: bytes) -> bytes:
    return bytes([kind]) + _length(len(body)) + body


def connect_packet(client_id: str, username: str, password: str, keepalive: int) -> bytes:
    flags = 0x02  # чистая сессия: подписки заводим заново при каждом подключении
    payload = _string(client_id)
    if username:
        flags |= 0x80
        payload += _string(username)
        if password:
            flags |= 0x40
            payload += _string(password)
    header = _string("MQTT") + bytes([4, flags]) + struct.pack(">H", keepalive)
    return packet(CONNECT, header + payload)


def publish_packet(topic: str, data: bytes, qos: int, retain: bool, packet_id: int) -> bytes:
    body = _string(topic) + (struct.pack(">H", packet_id) if qos else b"") + data
    return packet(PUBLISH | (qos << 1) | (1 if retain else 0), body)


def subscribe_packet(topics: list[str], packet_id: int) -> bytes:
    body = struct.pack(">H", packet_id) + b"".join(_string(t) + b"\x01" for t in topics)
    return packet(SUBSCRIBE, body)


async def read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    head = await reader.readexactly(1)
    size, shift = 0, 0
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        size |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    else:
        raise MqttError("испорченная длина пакета")
    if size > MAX_PACKET:
        raise MqttError("пакет больше потолка")
    return head[0], await reader.readexactly(size) if size else b""


def parse_publish(flags: int, body: bytes) -> tuple[str, bytes, int, int]:
    (size,) = struct.unpack_from(">H", body)
    topic = body[2 : 2 + size].decode("utf-8", "replace")
    offset, qos, packet_id = 2 + size, (flags >> 1) & 0x03, 0
    if qos:
        (packet_id,) = struct.unpack_from(">H", body, offset)
        offset += 2
    return topic, body[offset:], qos, packet_id


class MqttClient:
    """Одно подключение к брокеру. Переподключение — дело владельца."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
        tls: bool = False,
        client_id: str = "mega_home",
        on_message: Callable[[str, bytes], None] | None = None,
    ) -> None:
        self._host, self._port, self._tls = host, port, tls
        self._username, self._password, self._client_id = username, password, client_id
        self._on_message = on_message
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_id = 0
        self._acks: dict[int, asyncio.Future[bytes]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self.closed = asyncio.Event()

    async def connect(self, timeout: float = 10.0) -> None:
        context = ssl.create_default_context() if self._tls else None
        if context is not None:
            # ⚠ Брокер в LAN объекта — самоподписанный сертификат, доверие по адресу
            # из конфига (то же решение, что у HTTP-доступа).
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port, ssl=context), timeout
            )
            self._writer.write(connect_packet(self._client_id, self._username, self._password, KEEPALIVE))
            await self._writer.drain()
            kind, body = await asyncio.wait_for(read_packet(self._reader), timeout)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as err:
            raise MqttError(f"брокер недоступен: {err or 'таймаут'}") from err
        if kind & 0xF0 != CONNACK or len(body) < 2:
            raise MqttError("брокер ответил не CONNACK")
        if body[1]:
            raise MqttError(REFUSALS.get(body[1], f"брокер отказал (код {body[1]})"))
        self._tasks = [asyncio.ensure_future(self._read_loop()), asyncio.ensure_future(self._ping_loop())]

    async def publish(self, topic: str, data: bytes, qos: int = 0, retain: bool = False) -> None:
        packet_id = self._packet_id() if qos else 0
        waiter = self._expect(packet_id) if qos else None
        await self._send(publish_packet(topic, data, 1 if qos else 0, retain, packet_id))
        if waiter is not None:
            await self._wait(waiter, "брокер не подтвердил публикацию")

    async def subscribe(self, topics: list[str]) -> None:
        if not topics:
            return
        packet_id = self._packet_id()
        waiter = self._expect(packet_id)
        await self._send(subscribe_packet(topics, packet_id))
        granted = await self._wait(waiter, "брокер не подтвердил подписку")
        if b"\x80" in granted:
            raise MqttError("брокер отказал в подписке")

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._writer is not None:
            try:
                self._writer.write(packet(DISCONNECT, b""))
                self._writer.close()
            except OSError:
                pass
        self.closed.set()

    def _packet_id(self) -> int:
        self._next_id = self._next_id % 65535 + 1
        return self._next_id

    def _expect(self, packet_id: int) -> asyncio.Future[bytes]:
        future: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self._acks[packet_id] = future
        return future

    async def _wait(self, future: asyncio.Future[bytes], what: str) -> bytes:
        try:
            return await asyncio.wait_for(future, 10)
        except asyncio.TimeoutError as err:
            raise MqttError(what) from err

    async def _send(self, data: bytes) -> None:
        if self._writer is None or self.closed.is_set():
            raise MqttError("нет подключения к брокеру")
        try:
            self._writer.write(data)
            await self._writer.drain()
        except OSError as err:
            self.closed.set()
            raise MqttError(f"подключение к брокеру порвалось: {err}") from err

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                head, body = await read_packet(self._reader)
                kind = head & 0xF0
                if kind == PUBLISH:
                    topic, data, qos, packet_id = parse_publish(head & 0x0F, body)
                    if qos == 1:
                        await self._send(packet(PUBACK, struct.pack(">H", packet_id)))
                    if self._on_message is not None:
                        self._on_message(topic, data)
                elif kind in (PUBACK, SUBACK) and len(body) >= 2:
                    (packet_id,) = struct.unpack_from(">H", body)
                    future = self._acks.pop(packet_id, None)
                    if future is not None and not future.done():
                        future.set_result(body[2:])
        except asyncio.CancelledError:
            raise
        except (OSError, asyncio.IncompleteReadError, MqttError, struct.error) as err:
            LOGGER.debug("MQTT %s:%s: чтение остановлено: %s", self._host, self._port, err)
        finally:
            self.closed.set()

    async def _ping_loop(self) -> None:
        # ⚠ Не таймер вокруг чужой системы, а требование протокола (3.1.2.10):
        # без пакета за `keepalive` брокер сам разрывает подключение.
        try:
            while not self.closed.is_set():
                await asyncio.sleep(KEEPALIVE / 2)
                await self._send(packet(PINGREQ, b""))
        except (asyncio.CancelledError, MqttError):
            pass


def as_bytes(value: Any) -> bytes:
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value).encode("utf-8")
    return str(value if value is not None else "").encode("utf-8")
