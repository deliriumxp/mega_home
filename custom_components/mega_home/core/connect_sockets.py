"""Сырые виды `connect`: `tcp`, `udp`, `ws` — байты и кадры как пришли.

Вынесены из `connect.py` по порогу размера файла: здесь только исполнение вида,
а разбор описания, адресат и служба дома — в `connect.py`, общие для всех видов.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import re
import socket
from typing import Any

import aiohttp

from .connect import (
    MAX_READ,
    MAX_WS_MESSAGES,
    _body_answer,
    _decode_b64,
    _port,
    _reason,
    _timeout,
    connector_for,
    no_redirects,
    open_ws,
    pin_socket,
    resolve_address,
    socket_target,
)
from .ops_base import OpError

async def tcp(payload: dict[str, Any]) -> dict[str, Any]:
    host, port = resolve_address(payload)
    timeout = _timeout(payload)
    send = _decode_b64(payload.get("sendBase64"), "sendBase64")
    read_until = payload.get("readUntil") if isinstance(payload.get("readUntil"), dict) else {}
    delimiter = _decode_b64(read_until.get("delimiter"), "readUntil.delimiter")
    pattern = _pattern(read_until.get("pattern"))
    try:
        want = max(int(read_until.get("bytes") or 0), 0)
    except (TypeError, ValueError) as err:
        raise OpError("readUntil.bytes — число") from err
    deadline = _timeout({"timeout": read_until.get("deadline")}, timeout) if read_until.get("deadline") is not None else timeout
    # ⚠ `tls` — поле того же описания, что у сессии (`stream.py`): без него
    # `tls: true` молча уходил открытым текстом только в одиночном вызове.
    address, context, sni = await socket_target(host, port, payload.get("tls") is True, payload=payload)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port, ssl=context, server_hostname=sni), timeout
        )
    except (asyncio.TimeoutError, OSError) as err:
        raise OpError(f"Устройство не приняло соединение: {err or 'таймаут'}", 502) from err
    if context is not None:
        try:
            pin_socket(writer, payload)
        except OpError:
            writer.close()
            raise
    got = bytearray()
    closed = False
    try:
        if send:
            writer.write(send)
            await writer.drain()
        loop = asyncio.get_running_loop()
        end = loop.time() + deadline
        matched = False
        while len(got) < MAX_READ:
            if delimiter and delimiter in got:
                break
            if pattern is not None and pattern.search(got.decode("utf-8", "replace")):
                matched = True
                break
            if want and len(got) >= want:
                break
            left = end - loop.time()
            if left <= 0:
                break
            try:
                chunk = await asyncio.wait_for(reader.read(64 * 1024), left)
            except asyncio.TimeoutError:
                break
            if not chunk:
                closed = True
                break
            got.extend(chunk)
    except OSError as err:
        raise OpError(f"Соединение с устройством порвалось: {err}", 502) from err
    finally:
        writer.close()
    answer = _body_answer(bytes(got))
    answer["closed"] = closed
    if pattern is not None:
        answer["matched"] = matched or bool(pattern.search(bytes(got).decode("utf-8", "replace")))
    return answer

# Признак конца ответа выражением (`readUntil.pattern`): протоколы, которые соединение не закрывают и
# отвечают текстом без фиксированного разделителя (`sysmand` Control4: «OK» в конце строки, RTSP: строка
# статуса). ⚠ Длина ограничена: выражение приходит и от бандла, а поиск идёт в цикле событий.
MAX_PATTERN = 256

def _pattern(value: Any) -> re.Pattern[str] | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or len(value) > MAX_PATTERN:
        raise OpError(f"readUntil.pattern — строка до {MAX_PATTERN} знаков")
    try:
        return re.compile(value)
    except re.error as err:
        raise OpError(f"readUntil.pattern: {err}") from err

class _UdpCollector(asyncio.DatagramProtocol):
    """Ответные датаграммы с адресом отправителя; `answer` — первая, `enough` — набрано `want`."""

    def __init__(self, want: int = 1) -> None:
        loop = asyncio.get_running_loop()
        self.answer: asyncio.Future[bytes] = loop.create_future()
        self.enough: asyncio.Future[None] = loop.create_future()
        self.got: list[tuple[str, bytes]] = []
        self._want, self._size = want, 0

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if not self.answer.done():
            self.answer.set_result(data)
        if self.enough.done():
            return
        self._size += len(data)
        if self._size > MAX_READ:
            self.enough.set_result(None)
            return
        self.got.append((str(addr[0]) if addr else "", data))
        if len(self.got) >= self._want:
            self.enough.set_result(None)

# Мультикаст — только групп локальной сети (обнаружение устройств объекта: SSDP 239.255.255.250,
# mDNS 224.0.0.251, WS-Discovery 239.255.255.250): глобальные группы дом не трогает.
_LOCAL_GROUPS = (ipaddress.ip_network("224.0.0.0/24"), ipaddress.ip_network("239.0.0.0/8"))
MAX_UDP_ANSWERS = 64

def _multicast_group(payload: dict[str, Any]) -> str | None:
    """Группа мультикаста описания (`multicast: {ttl?}` и `host` — адрес группы) или `None`."""
    if not isinstance(payload.get("multicast"), dict):
        return None
    try:
        group = ipaddress.ip_address(str(payload.get("host") or "").strip())
    except ValueError as err:
        raise OpError("multicast: host — адрес группы") from err
    if group.version != 4 or not any(group in net for net in _LOCAL_GROUPS):
        raise OpError("multicast: только группы локальной сети (224.0.0.0/24, 239.0.0.0/8)")
    return str(group)

async def udp(payload: dict[str, Any]) -> dict[str, Any]:
    """Датаграмма и ответы на неё. `readUntil.count` — сколько ответов собрать (по умолчанию один,
    `body` — первый); при `count` больше одного все — в `answers` с адресом отправителя: обнаружение
    широковещанием и мультикастом отвечает многими устройствами разом."""
    group = _multicast_group(payload)
    if group is not None:
        host, port = group, _port(payload)
    else:
        host, port = resolve_address(payload)
        # Сокет ниже — IPv4 (широковещание объекта), поэтому имя — только его IPv4-адресом.
        host = (await socket_target(host, port, False, socket.AF_INET))[0]
    send = _decode_b64(payload.get("sendBase64"), "sendBase64")
    read_until = payload.get("readUntil") if isinstance(payload.get("readUntil"), dict) else {}
    deadline = read_until.get("deadline")
    window = 0.0 if deadline == 0 else _timeout({"timeout": deadline}, 2.0)
    try:
        want = min(max(int(read_until.get("count") or 1), 1), MAX_UDP_ANSWERS)
    except (TypeError, ValueError) as err:
        raise OpError("readUntil.count — число") from err
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        if group is not None:
            ttl = payload["multicast"].get("ttl", 2)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, min(max(int(ttl), 1), 8))
        sock.bind(("0.0.0.0", 0))
        transport, protocol = await loop.create_datagram_endpoint(lambda: _UdpCollector(want), sock=sock)
    except (OSError, TypeError, ValueError) as err:
        sock.close()
        raise OpError(f"Датаграмма не ушла: {err}", 502) from err
    try:
        transport.sendto(send, (host, port))
        if window:
            await asyncio.wait_for(asyncio.shield(protocol.enough), window)
    except asyncio.TimeoutError:
        pass
    finally:
        transport.close()
    answer = _body_answer(protocol.got[0][1] if protocol.got else b"")
    if want > 1:
        answer["answers"] = [{"from": sender, **_body_answer(data)} for sender, data in protocol.got]
    return answer

async def ws(payload: dict[str, Any]) -> dict[str, Any]:
    host, port = resolve_address(payload)
    tls = payload.get("tls") is True
    path = str(payload.get("path") or "/")
    url = f"{'wss' if tls else 'ws'}://{host}:{port}{path}"
    timeout = _timeout(payload)
    headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else None
    messages = payload.get("messages") if isinstance(payload.get("messages"), list) else []
    collect = payload.get("collect") if isinstance(payload.get("collect"), dict) else {}
    try:
        want = min(max(int(collect.get("count", 1) or 0), 0), MAX_WS_MESSAGES)
    except (TypeError, ValueError):
        want = 1
    window = (
        _timeout({"timeout": collect.get("deadline")}, timeout)
        if collect.get("deadline") is not None
        else timeout
    )
    try:
        # ⚠ `tls` — как у HTTP-ветки: устройству LAN сертификат не проверяется, имени — да.
        connector = await connector_for(host, port, tls, payload)
        async with aiohttp.ClientSession(connector=connector, trace_configs=no_redirects()) as session:
            # Срок — на рукопожатие (`timeout=` числом у aiohttp — лишь срок
            # закрытия, и он устарел): к мёртвому адресу иначе ждали бы минуты.
            socket_ = await asyncio.wait_for(session.ws_connect(url, headers=headers), timeout)
            got: list[dict[str, Any]] = []
            try:
                await asyncio.wait_for(open_ws(payload, socket_), timeout)
                for item in messages:
                    if isinstance(item, dict) and "base64" in item:
                        await socket_.send_bytes(_decode_b64(item["base64"], "messages[].base64"))
                    else:
                        await socket_.send_str(item if isinstance(item, str) else json.dumps(item))
                loop = asyncio.get_running_loop()
                deadline_at = loop.time() + window
                while len(got) < want:
                    left = deadline_at - loop.time()
                    if left <= 0:
                        break
                    try:
                        message = await socket_.receive(timeout=left)
                    except asyncio.TimeoutError:
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        got.append({"text": message.data})
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        got.append({"bodyBase64": base64.b64encode(message.data).decode("ascii")})
                    else:
                        break
            finally:
                await socket_.close()
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise OpError(_reason(err), 502) from err
    return {"messages": got}
