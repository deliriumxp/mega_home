"""Сырые виды `connect`: `tcp`, `udp`, `ws` — байты и кадры как пришли.

Вынесены из `connect.py` по порогу размера файла: здесь только исполнение вида,
а разбор описания, адресат и служба дома — в `connect.py`, общие для всех видов.
"""

from __future__ import annotations

import asyncio
import base64
import json
import socket
from typing import Any

import aiohttp

from .connect import (
    MAX_READ,
    MAX_WS_MESSAGES,
    _body_answer,
    _decode_b64,
    _reason,
    _timeout,
    connector_for,
    no_redirects,
    open_ws,
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
    try:
        want = max(int(read_until.get("bytes") or 0), 0)
    except (TypeError, ValueError) as err:
        raise OpError("readUntil.bytes — число") from err
    deadline = _timeout({"timeout": read_until.get("deadline")}, timeout) if read_until.get("deadline") is not None else timeout
    # ⚠ `tls` — поле того же описания, что у сессии (`stream.py`): без него
    # `tls: true` молча уходил открытым текстом только в одиночном вызове.
    address, context, sni = await socket_target(host, port, payload.get("tls") is True)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port, ssl=context, server_hostname=sni), timeout
        )
    except (asyncio.TimeoutError, OSError) as err:
        raise OpError(f"Устройство не приняло соединение: {err or 'таймаут'}", 502) from err
    got = bytearray()
    closed = False
    try:
        if send:
            writer.write(send)
            await writer.drain()
        loop = asyncio.get_running_loop()
        end = loop.time() + deadline
        while len(got) < MAX_READ:
            if delimiter and delimiter in got:
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
    return answer

class _UdpCollector(asyncio.DatagramProtocol):
    """Первая ответная датаграмма — фьючерсом, а не флагом под опросом."""

    def __init__(self) -> None:
        self.answer: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if not self.answer.done():
            self.answer.set_result(data)

async def udp(payload: dict[str, Any]) -> dict[str, Any]:
    host, port = resolve_address(payload)
    # Сокет ниже — IPv4 (широковещание объекта), поэтому имя — только его IPv4-адресом.
    host = (await socket_target(host, port, False, socket.AF_INET))[0]
    send = _decode_b64(payload.get("sendBase64"), "sendBase64")
    read_until = payload.get("readUntil") if isinstance(payload.get("readUntil"), dict) else {}
    deadline = read_until.get("deadline")
    window = 0.0 if deadline == 0 else _timeout({"timeout": deadline}, 2.0)
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", 0))
        transport, protocol = await loop.create_datagram_endpoint(_UdpCollector, sock=sock)
    except OSError as err:
        sock.close()
        raise OpError(f"Датаграмма не ушла: {err}", 502) from err
    data = b""
    try:
        transport.sendto(send, (host, port))
        if window:
            data = await asyncio.wait_for(protocol.answer, window)
    except asyncio.TimeoutError:
        pass
    finally:
        transport.close()
    return _body_answer(data)

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
        connector = await connector_for(host, port, tls)
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
