"""Доступ вида `ws`: вызов по WebSocket устройства (`docs/home-gateway.md`).

Зачем. Часть устройств говорит только WebSocket: Shelly Gen2 (RPC), UniFi Protect,
Frigate, часть контроллеров и домофонов. Протокол знает бандл — дом подключается
с авторизацией описания, шлёт сообщения и возвращает ответы как есть.

Вызов: `{path, params?, send: [строка | {base64}], receive?: N, until?: подстрока,
timeout?: с}`. Ответ — список полученных сообщений (`text` или `base64`). Подписка
на поток сообщений — источник событий `ws` (`listeners.py`).
"""

from __future__ import annotations

import asyncio
import base64
from typing import Any

import aiohttp

from .access import AccessDescriptor
from .access_http import AccessDenied, AccessUnreachable
from .access_raw import MAX_SEND, MAX_WINDOW

MAX_MESSAGES = 64


def outgoing(items: Any) -> list[str | bytes]:
    """Сообщения к устройству: строка — текстом, `{base64}` — байтами."""
    out: list[str | bytes] = []
    for item in items if isinstance(items, list) else ([items] if items else []):
        if isinstance(item, dict) and "base64" in item:
            try:
                out.append(base64.b64decode(str(item["base64"]), validate=True))
            except ValueError as err:
                raise AccessDenied("send: ожидается base64") from err
        elif isinstance(item, (dict, list)):
            import json

            out.append(json.dumps(item))
        else:
            out.append(str(item))
    if sum(len(x) for x in out) > MAX_SEND:
        raise AccessDenied(f"send: больше {MAX_SEND} байт")
    return out


def incoming(message: Any) -> dict[str, Any] | None:
    if message.type == aiohttp.WSMsgType.TEXT:
        return {"text": message.data}
    if message.type == aiohttp.WSMsgType.BINARY:
        return {"base64": base64.b64encode(message.data).decode("ascii")}
    return None


async def ws_exchange(door: Any, descriptor: AccessDescriptor, call: dict[str, Any], scope: str) -> dict[str, Any]:
    from .gateway import AccessGateway

    path = str(call.get("path") or "/")
    path = AccessGateway.check(descriptor, "GET", path, scope)
    send = outgoing(call.get("send"))
    try:
        # `receive: 0` — послать и не ждать (уведомление RPC без ответа).
        want = min(max(int(1 if call.get("receive") is None else call["receive"]), 0), MAX_MESSAGES)
        window = min(max(float(call.get("timeout") or descriptor.timeout), 0.1), MAX_WINDOW)
    except (TypeError, ValueError) as err:
        raise AccessDenied("receive и timeout — числа") from err
    until = str(call.get("until") or "")
    params = call.get("params") if isinstance(call.get("params"), dict) else None
    headers = call.get("headers") if isinstance(call.get("headers"), dict) else None
    socket = await door.http.ws(descriptor, path, params, headers)
    got: list[dict[str, Any]] = []
    try:
        for item in send:
            if isinstance(item, bytes):
                await socket.send_bytes(item)
            else:
                await socket.send_str(item)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + window
        while len(got) < want:
            left = deadline - loop.time()
            if left <= 0:
                break
            try:
                message = await socket.receive(timeout=left)
            except asyncio.TimeoutError:
                break
            item = incoming(message)
            if item is None:
                break
            got.append(item)
            if until and until in item.get("text", ""):
                break
    except (aiohttp.ClientError, OSError) as err:
        raise AccessUnreachable(f"WebSocket устройства порвался: {err}") from err
    finally:
        await socket.close()
    return {"messages": got}
