"""Доступы `tcp` и `udp`: обмен байтами по описанию, без знания протокола.

Зачем. Часть устройств говорит не HTTP: проекторы и матрицы — строками по TCP,
пробуждение по сети и опрос устройств — датаграммами. Протокол знает бандл (или
менеджер), дом только несёт байты и отдаёт ответ как есть
(`docs/home-gateway.md` в менеджере).

Вызов: `{send: <base64>, until?: <base64 разделитель>, bytes?: N, timeout?: с}`.
Ответ TCP — всё прочитанное до разделителя, числа байт, закрытия или срока; UDP —
список датаграмм, пришедших в окне, с адресом отправителя.
"""

from __future__ import annotations

import asyncio
import base64
import socket
from typing import Any

from .access import AccessDescriptor
from .access_http import AccessDenied, AccessUnreachable

MAX_SEND = 64 * 1024
MAX_READ = 1024 * 1024
MAX_WINDOW = 30.0
MAX_DATAGRAMS = 64


def _bytes(value: Any, what: str) -> bytes:
    if value in (None, ""):
        return b""
    try:
        raw = base64.b64decode(str(value), validate=True)
    except ValueError as err:
        raise AccessDenied(f"{what}: ожидается base64") from err
    if len(raw) > MAX_SEND:
        raise AccessDenied(f"{what}: больше {MAX_SEND} байт")
    return raw


def _window(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(number, 0.05), MAX_WINDOW)


async def tcp_exchange(descriptor: AccessDescriptor, call: dict[str, Any]) -> dict[str, Any]:
    send = _bytes(call.get("send"), "send")
    until = _bytes(call.get("until"), "until")
    try:
        want = max(int(call.get("bytes") or 0), 0)
    except (TypeError, ValueError) as err:
        raise AccessDenied("bytes: ожидается число") from err
    window = _window(call.get("timeout"), descriptor.timeout)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(descriptor.host, descriptor.port), window
        )
    except (asyncio.TimeoutError, OSError) as err:
        raise AccessUnreachable(f"Устройство не принимает соединение: {err or 'таймаут'}") from err
    got = bytearray()
    closed = False
    try:
        if send:
            writer.write(send)
            await writer.drain()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + window
        while len(got) < MAX_READ:
            if until and until in got:
                break
            if want and len(got) >= want:
                break
            left = deadline - loop.time()
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
        raise AccessUnreachable(f"Соединение с устройством порвалось: {err}") from err
    finally:
        writer.close()
    return {"data": base64.b64encode(bytes(got)).decode("ascii"), "closed": closed}


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if len(self.items) < MAX_DATAGRAMS:
            self.items.append({"from": addr[0], "data": base64.b64encode(data).decode("ascii")})


async def udp_exchange(descriptor: AccessDescriptor, call: dict[str, Any]) -> dict[str, Any]:
    send = _bytes(call.get("send"), "send")
    # Окно ответов: 0 — только отправить (пробуждение по сети ответа не ждёт).
    window = 0.0 if call.get("timeout") == 0 else _window(call.get("timeout"), 2.0)
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Разрешение широковещания на сокете ставится всегда: по какому адресу
        # уйдёт датаграмма, решает АДРЕС ДОСТУПА из конфига объекта, а не запрос
        # приложения; угадывать «широковещательный ли он» по маске не беремся.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", 0))
        transport, protocol = await loop.create_datagram_endpoint(_Collector, sock=sock)
    except OSError as err:
        sock.close()
        raise AccessUnreachable(f"Датаграмма не ушла: {err}") from err
    try:
        transport.sendto(send, (descriptor.host, descriptor.port))
        if window:
            await asyncio.sleep(window)
    except OSError as err:
        raise AccessUnreachable(f"Датаграмма не ушла: {err}") from err
    finally:
        transport.close()
    return {"datagrams": protocol.items}
