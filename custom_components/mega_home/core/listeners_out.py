"""Исходящие источники событий: дом сам держит подписку на устройство.

Виды (`events[]` описания, `docs/home-gateway.md`): `poll`, `stream`, `ws`,
`mqtt`, `tcp`. Все строки источника — ШАБЛОНЫ описания (`templating.py`): в них
видны адрес, поля учётки и значения, которые источник забрал из ответов.

  * курсор опроса — `carry: {имя: путь}`: поле ответа становится значением
    шаблона следующего запроса (`since`, `lastUpdateId`, адрес подписки);
  * `setup` — запрос ДО цикла (создать подписку ONVIF PullPoint, bootstrap
    UniFi) с теми же `carry`;
  * `steps` у `ws`/`tcp` — вход внутри соединения: послать шаблон, забрать поля
    ответа (`capture`) — Shelly Gen2 с паролем, TCP с вызовом-ответом;
  * `keepalive: {every, send}` — прикладной ping, без которого устройство рвёт
    соединение;
  * `idle` — сколько молчать до переподключения: устройство, пропавшее без FIN,
    иначе держало бы чтение вечно (повторное ревью 2026-09-19).

⚠ Шаблоны пишет только менеджер: бандл в источник событий не попадает никак.
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

from .access import AccessDescriptor
from .access_secrets import template_values
from .templating import pick, render

MIN_PAUSE_S = 1.0
IDLE_S = 120.0
MAX_CHUNK = 64 * 1024


def body_of(raw: bytes) -> dict[str, Any]:
    try:
        return {"text": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"base64": base64.b64encode(raw).decode("ascii")}


def seconds(value: Any, default: float, low: float = 0.1) -> float:
    try:
        return max(float(value), low)
    except (TypeError, ValueError):
        return default


def delimiter(spec: dict[str, Any], default: bytes) -> bytes:
    """Разделитель кусков. ⚠ Пустой — бесконечный цикл на первом куске, поэтому умолчание."""
    try:
        value = base64.b64decode(str(spec["until"]), validate=True) if spec.get("until") else default
    except ValueError:
        value = default
    return value or default


def carry(values: dict[str, Any], spec: dict[str, Any], payload: bytes) -> None:
    """Поля ответа → значения шаблонов следующих запросов (`carry: {имя: путь}`)."""
    fields = spec.get("carry")
    if not isinstance(fields, dict):
        return
    try:
        data = json.loads(payload.decode("utf-8", "ignore")) if payload else None
    except ValueError:
        data = None
    for name, path in fields.items():
        found = pick(data, str(path))
        if found:
            values[f"carry.{name}"] = found


def _render_obj(value: Any, values: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return render(value, values)
    if isinstance(value, dict):
        return {str(k): _render_obj(v, values) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_obj(v, values) for v in value]
    return value


async def base_values(door: Any, descriptor: AccessDescriptor) -> dict[str, Any]:
    return template_values(descriptor, await door.secret_of(descriptor))


async def split(reader: Any, sep: bytes, emit: Any, idle: float) -> None:
    """Читать до конца, отдавая куски между разделителями; молчание дольше `idle` — обрыв."""
    buffer = b""
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(MAX_CHUNK), idle)
        except asyncio.TimeoutError as err:
            raise ConnectionError("устройство молчит дольше срока — переподключаюсь") from err
        if not chunk:
            if buffer.strip():
                emit(buffer)
            return
        buffer += chunk
        while sep in buffer:
            item, buffer = buffer.split(sep, 1)
            if item.strip():
                emit(item)
        if len(buffer) > MAX_CHUNK:
            emit(buffer)
            buffer = b""


async def _request(door: Any, descriptor: AccessDescriptor, block: dict[str, Any], values: dict[str, Any]) -> tuple[int, str, bytes]:
    from .gateway import SCOPE_MANAGER

    body = block.get("body")
    raw = None
    if isinstance(body, (dict, list)):
        raw = json.dumps(_render_obj(body, values)).encode()
    elif isinstance(body, str) and body:
        raw = render(body, values).encode()
    params = block.get("params")
    status, kind, payload, _ = await door.call_full(
        descriptor.id,
        str(block.get("method") or "GET"),
        render(str(block.get("path") or "/"), values),
        _render_obj(params, values) if isinstance(params, dict) else None,
        raw,
        headers=_render_obj(block["headers"], values) if isinstance(block.get("headers"), dict) else None,
        scope=SCOPE_MANAGER,
    )
    return status, kind, payload


async def poll(ctx: Any, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
    values = await base_values(ctx.door, descriptor)
    if isinstance(spec.get("setup"), dict):
        _, _, payload = await _request(ctx.door, descriptor, spec["setup"], values)
        carry(values, spec["setup"], payload)
    pause = seconds(spec.get("pause"), MIN_PAUSE_S, MIN_PAUSE_S)
    # ⚠ Одинаковый ответ подряд гасится только у КОРОТКОГО опроса (или по
    # `changesOnly`): у долгого два одинаковых ответа — два события (два нажатия
    # звонка), и второе пропадало бы.
    long_poll = descriptor.is_long_poll(str(spec.get("path") or "/")) or spec.get("longPoll") is True
    changes_only = spec.get("changesOnly", not long_poll) is True
    last: tuple[int, bytes] | None = None
    while True:
        status, kind, payload = await _request(ctx.door, descriptor, spec, values)
        carry(values, spec, payload)
        if not changes_only or (status, payload) != last:
            last = (status, payload)
            ctx.emit(descriptor, source, spec, "response", {"status": status, "contentType": kind, **body_of(payload)})
        await asyncio.sleep(0 if long_poll else pause)


async def stream(ctx: Any, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
    import re

    from .gateway import SCOPE_MANAGER, AccessGateway

    values = await base_values(ctx.door, descriptor)
    method = str(spec.get("method") or "GET").upper()
    path = AccessGateway.check(descriptor, method, render(str(spec.get("path") or "/"), values), SCOPE_MANAGER)
    params = _render_obj(spec["params"], values) if isinstance(spec.get("params"), dict) else None
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    async with ctx.door.http.stream(descriptor, method, path, params, None) as response:
        sep = delimiter(spec, b"")
        if not sep:
            match = re.search(r"boundary=\"?([^\";]+)", response.headers.get("Content-Type", ""))
            sep = (b"--" + match.group(1).encode()) if match else b"\n\n"
        await split(response.content, sep, lambda item: ctx.emit(descriptor, source, spec, "part", body_of(item)), idle)


async def _steps(send: Any, receive: Any, spec: dict[str, Any], values: dict[str, Any]) -> None:
    """Вход внутри соединения: послать шаблон, забрать поля JSON-ответа."""
    for step in spec.get("steps") or []:
        if not isinstance(step, dict):
            continue
        if step.get("send") is not None:
            await send(render(str(step["send"]), values))
        if isinstance(step.get("capture"), dict):
            reply = await asyncio.wait_for(receive(), seconds(step.get("timeout"), 10.0))
            try:
                data = json.loads(reply)
            except (TypeError, ValueError):
                data = None
            for name, path in step["capture"].items():
                values[f"capture.{name}"] = pick(data, str(path)) if data is not None else str(reply)


async def _keepalive(send: Any, spec: dict[str, Any], values: dict[str, Any]) -> None:
    block = spec.get("keepalive")
    if not isinstance(block, dict) or not block.get("send"):
        return
    every = seconds(block.get("every"), 30.0, 1.0)
    while True:
        await asyncio.sleep(every)
        await send(render(str(block["send"]), values))


async def ws(ctx: Any, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
    import aiohttp

    from .access_ws import incoming

    values = await base_values(ctx.door, descriptor)
    socket = await ctx.door.http.ws(descriptor, render(str(spec.get("path") or "/"), values))
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)

    async def receive() -> str:
        message = await socket.receive()
        return message.data if message.type == aiohttp.WSMsgType.TEXT else ""

    keep: asyncio.Task[None] | None = None
    try:
        await _steps(socket.send_str, receive, spec, values)
        keep = asyncio.ensure_future(_keepalive(socket.send_str, spec, values))
        while True:
            message = await socket.receive(timeout=idle)
            item = incoming(message)
            if item is None:
                return
            ctx.emit(descriptor, source, spec, "message", item)
    finally:
        if keep is not None:
            keep.cancel()
        await socket.close()


async def mqtt(ctx: Any, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
    topics = [str(t) for t in spec.get("topics") or [] if str(t)]

    def on_message(topic: str, data: bytes) -> None:
        ctx.emit(descriptor, source, spec, "message", {"topic": topic, **body_of(data)})

    client = await ctx.door.mqtt_client(descriptor, on_message)
    try:
        await client.subscribe(topics)
        await client.closed.wait()
    finally:
        await client.close()


async def tcp(ctx: Any, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
    values = await base_values(ctx.door, descriptor)
    sep = delimiter(spec, b"\n")
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(descriptor.host, int(spec.get("port") or descriptor.port)), 10
    )

    async def send(text: str) -> None:
        writer.write(text.encode("utf-8"))
        await writer.drain()

    async def receive() -> str:
        line = await reader.readuntil(sep)
        return line[: -len(sep)].decode("utf-8", "replace")

    keep: asyncio.Task[None] | None = None
    try:
        if spec.get("send"):
            writer.write(base64.b64decode(str(spec["send"]), validate=True))
            await writer.drain()
        await _steps(send, receive, spec, values)
        keep = asyncio.ensure_future(_keepalive(send, spec, values))
        await split(reader, sep, lambda item: ctx.emit(descriptor, source, spec, "data", body_of(item)), idle)
    finally:
        if keep is not None:
            keep.cancel()
        writer.close()
