"""Минимальный клиент MQTT 3.1.1 — подписки слушателей (`listeners_out.mqtt`) и
вид `connect` `mqtt` (`exchange`).

⚠ Свой клиент, а не зависимость: HACS ставит только каталог интеграции, `pip`
на объекте требует интернета, а объекты бывают офлайн
(`docs/home-gateway.md` в менеджере). Поэтому здесь ровно то, что нужно
подписке и одиночному обмену: CONNECT, SUBSCRIBE, PUBLISH в обе стороны (с PUBACK
на QoS 1), PING, DISCONNECT.

Формат пакетов — стандарт OASIS «MQTT Version 3.1.1» (2014), разделы 2 и 3:
фиксированный заголовок (тип и флаги в старшем байте, остаток длины — число
переменной длины по 7 бит), строки — длина двумя байтами и UTF-8.
"""

from __future__ import annotations

import asyncio
import ssl
import struct
from secrets import token_hex
from typing import Any, Callable

from .connect import (
    MAX_READ,
    MAX_SEND,
    _body_answer,
    _decode_b64,
    _timeout,
    pin_socket,
    resolve_address,
    socket_target,
)
from .connect_tls import tls_context
from .const import LOGGER
from .ops_base import OpError

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


def subscribe_packet(topics: list[str], packet_id: int) -> bytes:
    body = struct.pack(">H", packet_id) + b"".join(_string(t) + b"\x01" for t in topics)
    return packet(SUBSCRIBE, body)


def publish_packet(topic: str, data: bytes, qos: int, retain: bool, packet_id: int) -> bytes:
    # 3.3.1: флаги — QoS битами 1–2 и RETAIN битом 0; номер пакета только при QoS > 0 (3.3.2.2).
    body = _string(topic) + (struct.pack(">H", packet_id) if qos else b"") + data
    return packet(PUBLISH | (qos << 1) | (1 if retain else 0), body)


def matches(pattern: str, topic: str) -> bool:
    """Тема под фильтром подписки: `+` — одно звено, `#` — остаток (4.7.1)."""
    want, got = pattern.split("/"), topic.split("/")
    for index, part in enumerate(want):
        if part == "#":
            return True
        if index >= len(got) or part not in ("+", got[index]):
            return False
    return len(want) == len(got)


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
        on_message: Callable[[str, bytes, bool], None] | None = None,
        context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        tls_payload: dict[str, Any] | None = None,
    ) -> None:
        # Описание с опциями TLS (`connect_tls.py`): отпечаток сверяется после рукопожатия.
        self._tls_payload = tls_payload
        self._host, self._port, self._tls = host, port, tls
        self._username, self._password, self._client_id = username, password, client_id
        self._on_message = on_message
        # Готовый TLS и имя для SNI — у брокера по имени хоста (`connect.socket_target`).
        self._context, self._server_hostname = context, server_hostname
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_id = 0
        self._acks: dict[int, asyncio.Future[bytes]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self.closed = asyncio.Event()

    async def connect(self, timeout: float = 10.0) -> None:
        # ⚠ Брокер в LAN объекта — самоподписанный сертификат, доверие по адресу
        # из конфига (то же решение и тот же контекст, что у `connect.py`).
        context = self._context or (tls_context() if self._tls else None)
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self._host, self._port, ssl=context, server_hostname=self._server_hostname if context else None
                ),
                timeout,
            )
            if context is not None:
                pin_socket(self._writer, self._tls_payload)
            self._writer.write(connect_packet(self._client_id, self._username, self._password, KEEPALIVE))
            await self._writer.drain()
            kind, body = await asyncio.wait_for(read_packet(self._reader), timeout)
            if kind & 0xF0 != CONNACK or len(body) < 2:
                raise MqttError("брокер ответил не CONNACK")
            if body[1]:
                raise MqttError(REFUSALS.get(body[1], f"брокер отказал (код {body[1]})"))
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, MqttError) as err:
            # ⚠ Сокет закрывается на ЛЮБОМ отказе: подписчик повторяет вход раз в
            # минуту, и с неверным паролем дескрипторы копились бы без конца.
            await self.close()
            if isinstance(err, MqttError):
                raise
            raise MqttError(f"брокер недоступен: {err or 'таймаут'}") from err
        self._pong = asyncio.Event()
        self._tasks = [asyncio.ensure_future(self._read_loop()), asyncio.ensure_future(self._ping_loop())]

    async def subscribe(self, topics: list[str]) -> None:
        if not topics:
            return
        granted = await self._acked(lambda packet_id: subscribe_packet(topics, packet_id), "подписку")
        if b"\x80" in granted:
            raise MqttError("брокер отказал в подписке")

    async def publish(self, topic: str, data: bytes, qos: int = 0, retain: bool = False) -> None:
        if not qos:
            await self._send(publish_packet(topic, data, 0, retain, 0))
            return
        await self._acked(lambda packet_id: publish_packet(topic, data, qos, retain, packet_id), "публикацию")

    async def _acked(self, build: Callable[[int], bytes], what: str) -> bytes:
        """Пакет с номером и ожидание его подтверждения (SUBACK, PUBACK)."""
        packet_id = self._next_id = self._next_id % 65535 + 1
        waiter: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self._acks[packet_id] = waiter
        try:
            await self._send(build(packet_id))
            return await asyncio.wait_for(waiter, 10)
        except asyncio.TimeoutError as err:
            raise MqttError(f"брокер не подтвердил {what}") from err
        finally:
            # ⚠ Ожидание снимается на ЛЮБОМ исходе: не дождавшееся брокера
            # оставалось в `_acks` до закрытия подключения.
            self._acks.pop(packet_id, None)

    async def close(self) -> None:
        for task in self._tasks:
            if task is not asyncio.current_task():
                task.cancel()
        if self._writer is not None:
            try:
                self._writer.write(packet(DISCONNECT, b""))
                self._writer.close()
            except (OSError, RuntimeError):
                pass
        self._mark_closed()

    def _mark_closed(self) -> None:
        # Ожидающие подтверждения проваливаются СРАЗУ, а не по своим 10 с.
        for future in self._acks.values():
            if not future.done():
                future.set_exception(MqttError("подключение к брокеру закрыто"))
        self._acks.clear()
        self.closed.set()

    async def _send(self, data: bytes) -> None:
        if self._writer is None or self.closed.is_set():
            raise MqttError("нет подключения к брокеру")
        try:
            self._writer.write(data)
            await self._writer.drain()
        except OSError as err:
            self._mark_closed()
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
                        self._on_message(topic, data, bool(head & 0x01))
                elif kind in (SUBACK, PUBACK) and len(body) >= 2:
                    (packet_id,) = struct.unpack_from(">H", body)
                    future = self._acks.pop(packet_id, None)
                    if future is not None and not future.done():
                        future.set_result(body[2:])
                elif kind == PINGRESP:
                    self._pong.set()
        except asyncio.CancelledError:
            raise
        except (OSError, asyncio.IncompleteReadError, MqttError, struct.error, UnicodeError) as err:
            LOGGER.debug("MQTT %s:%s: чтение остановлено: %s", self._host, self._port, err)
        finally:
            self._mark_closed()

    async def _ping_loop(self) -> None:
        # ⚠ Не таймер вокруг чужой системы, а требование протокола (3.1.2.10):
        # без пакета за `keepalive` брокер сам разрывает подключение. Ответ
        # PINGRESP ЖДЁМ: брокер, пропавший без FIN, иначе обнаружился бы только
        # через минуты ретрансмитов ядра — всё это время события терялись бы.
        try:
            while not self.closed.is_set():
                await asyncio.sleep(KEEPALIVE / 2)
                self._pong.clear()
                await self._send(packet(PINGREQ, b""))
                try:
                    await asyncio.wait_for(self._pong.wait(), KEEPALIVE / 2)
                except asyncio.TimeoutError:
                    LOGGER.debug("MQTT %s:%s: брокер не ответил на PING", self._host, self._port)
                    await self.close()
                    return
        except (asyncio.CancelledError, MqttError):
            pass


# Потолок сообщений одного обмена: снимок контроллера — тысячи сохранённых тем,
# объём сверху держит общий `MAX_READ` вызова.
MAX_MESSAGES = 20000


async def exchange(payload: dict[str, Any]) -> dict[str, Any]:
    """Вид `connect` `mqtt`: подключиться, подписаться, опубликовать, собрать, отключиться.

    Подписка подтверждается брокером (SUBACK) ДО публикации — иначе ответ на
    запрос RPC мог бы прийти раньше подписки на него. Сбор кончается первым из:
    сообщение под фильтром `until`, набралось `count`, тишина `quiet` секунд с
    последнего сообщения, истёк `deadline`. ⚠ `quiet` — не таймер вокруг чужой
    системы: сигнала «сохранённые сообщения кончились» в MQTT 3.1.1 нет, снимок
    брокера иначе не закончить. Смысла тем дом не знает.
    """
    host, port = resolve_address(payload)
    timeout = _timeout(payload)
    auth = payload.get("auth") if isinstance(payload.get("auth"), dict) else {}
    topics = [str(t) for t in payload.get("subscribe") or [] if str(t)] if isinstance(payload.get("subscribe") or [], list) else None
    if topics is None:
        raise OpError("subscribe — список фильтров")
    outgoing = [_outgoing(item) for item in (payload.get("publish") if isinstance(payload.get("publish"), list) else [])]
    collect = payload.get("collect") if isinstance(payload.get("collect"), dict) else {}
    until = str(collect.get("until") or "")
    count = _count(collect.get("count"))
    quiet = _timeout({"timeout": collect.get("quiet")}, 0) if collect.get("quiet") is not None else 0
    deadline = _timeout({"timeout": collect.get("deadline")}, timeout) if collect.get("deadline") is not None else timeout
    address, context, sni = await socket_target(host, port, payload.get("tls") is True, payload=payload)
    got: list[dict[str, Any]] = []
    state = {"size": 0, "done": False}
    arrived = asyncio.Event()

    def on_message(topic: str, data: bytes, retained: bool) -> None:
        if state["done"]:
            return
        state["size"] += len(topic) + len(data)
        if len(got) >= MAX_MESSAGES or state["size"] > MAX_READ:
            state["done"] = True
        else:
            got.append({"topic": topic, **_body_answer(data), "retained": retained})
            state["done"] = bool(until and matches(until, topic)) or bool(count and len(got) >= count)
        arrived.set()

    client = MqttClient(
        address, port, str(auth.get("user") or ""), str(auth.get("pass") or ""),
        client_id=f"mega_home-connect-{token_hex(4)}", on_message=on_message, context=context, server_hostname=sni,
        tls_payload=payload,
    )
    try:
        await client.connect(timeout)
        await client.subscribe(topics)
        for topic, data, qos, retain in outgoing:
            await client.publish(topic, data, qos, retain)
        if topics:
            await _collect(client, arrived, state, quiet, deadline)
    except MqttError as err:
        raise OpError(f"Брокер: {err}", 502) from err
    finally:
        await client.close()
    return {"messages": got, "truncated": len(got) >= MAX_MESSAGES or state["size"] > MAX_READ}


async def _collect(client: MqttClient, arrived: asyncio.Event, state: dict[str, Any], quiet: float, deadline: float) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline
    while not state["done"] and not client.closed.is_set():
        left = end - loop.time()
        if left <= 0:
            return
        arrived.clear()
        waiters = [asyncio.ensure_future(arrived.wait()), asyncio.ensure_future(client.closed.wait())]
        finished, pending = await asyncio.wait(waiters, timeout=min(left, quiet) if quiet else left, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        if not finished:
            return


def _outgoing(item: Any) -> tuple[str, bytes, int, bool]:
    if not isinstance(item, dict):
        raise OpError("publish[] — объекты {topic, payload}")
    topic = str(item.get("topic") or "")
    if not topic or "+" in topic or "#" in topic:
        raise OpError("publish[].topic — тема без подстановочных знаков")
    if item.get("payloadBase64"):
        data = _decode_b64(item["payloadBase64"], "publish[].payloadBase64")
    else:
        data = str(item.get("payload") or "").encode("utf-8")
        if len(data) > MAX_SEND:
            raise OpError(f"publish[].payload: больше {MAX_SEND} байт")
    qos = item.get("qos", 0)
    if qos not in (0, 1):
        raise OpError("publish[].qos — 0 или 1")
    return topic, data, qos, item.get("retain") is True


def _count(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError) as err:
        raise OpError("collect.count — число") from err
