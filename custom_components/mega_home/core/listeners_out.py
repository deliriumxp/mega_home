"""Исходящие источники событий: дом сам держит подписку на устройство.

Виды (`events[]` описания, `docs/plan-thin-gateway.md`): `poll`, `stream`, `ws`,
`mqtt`, `tcp`. Все строки источника — ШАБЛОНЫ описания (`templating.py`): в них
видны адрес, поля учётки и значения, которые источник забрал из ответов.

  * курсор опроса — `carry: {имя: путь}`: поле ответа становится значением
    шаблона следующего запроса (`since`, `lastUpdateId`, адрес подписки);
  * `setup` — запрос ДО цикла (создать подписку у камеры протоколом
    PullPoint, поднять сессию UniFi) с теми же `carry`;
  * `steps` у `ws`/`tcp` — вход внутри соединения: послать шаблон, забрать поля
    ответа (`capture`) — Shelly Gen2 с паролем, TCP с вызовом-ответом;
  * `keepalive: {every, send}` — прикладной ping, без которого устройство рвёт
    соединение;
  * `idle` — сколько молчать до переподключения: устройство, пропавшее без FIN,
    иначе держало бы чтение вечно.

⚠ Шаблоны пишет только менеджер: бандл в источник событий не попадает никак.

⚠ Авторизация — только `basic`/`digest`, как у `connect`: учётка — поля
описания устройства, дом ничего не вычисляет (`templating.py`). Раньше вход шёл
через универсальную дверь (`gateway.py`, снесена) с сессиями и вычисляемыми
шаблонами — здесь дом сам держит `aiohttp.ClientSession` на источник.
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

import aiohttp

from . import digest
from .const import LOGGER
from .devices import DeviceDescriptor
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

def base_values(descriptor: DeviceDescriptor) -> dict[str, Any]:
    return {
        "host": descriptor.host,
        "port": str(descriptor.port),
        "id": descriptor.id,
        "user": descriptor.auth.user,
        "pass": descriptor.auth.password,
    }

def _auth_kwargs(descriptor: DeviceDescriptor) -> dict[str, Any]:
    """Только `basic`: `digest` идёт вторым кругом в `_request` (RFC 7616, `digest.py`)."""
    if descriptor.auth.type == "basic":
        return {"auth": aiohttp.BasicAuth(descriptor.auth.user, descriptor.auth.password)}
    return {}

def _url(descriptor: DeviceDescriptor, path: str, scheme: str = "http") -> str:
    return f"{scheme}://{descriptor.host}:{descriptor.port}{path}"

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

async def _request(
    session: aiohttp.ClientSession, descriptor: DeviceDescriptor, block: dict[str, Any], values: dict[str, Any]
) -> tuple[int, str, bytes]:
    body = block.get("body")
    raw = None
    if isinstance(body, (dict, list)):
        raw = json.dumps(_render_obj(body, values)).encode()
    elif isinstance(body, str) and body:
        raw = render(body, values).encode()
    params = block.get("params")
    scheme = "https" if descriptor.tls else "http"
    path = render(str(block.get("path") or "/"), values)
    url = _url(descriptor, path, scheme)
    headers = _render_obj(block["headers"], values) if isinstance(block.get("headers"), dict) else None
    method = str(block.get("method") or "GET")
    long_poll = block.get("longPoll") is True
    timeout = aiohttp.ClientTimeout(total=descriptor.timeout if not long_poll else max(descriptor.timeout, 120.0))
    query = _render_obj(params, values) if isinstance(params, dict) else None
    async with session.request(
        method, url, params=query, data=raw, headers=headers, timeout=timeout, **_auth_kwargs(descriptor)
    ) as response:
        status = response.status
        payload = await response.read()
        kind = response.content_type
    # ⚠ Digest — второй круг, как у `connect.py`: устройство отвечает 401 с
    # `WWW-Authenticate` только на первый запрос без `Authorization` (RFC 7616,
    # `digest.py`), а вычислять его дважды тем же кодом не хотим.
    if descriptor.auth.type == "digest" and status == 401:
        www_auth = response.headers.get("WWW-Authenticate", "")
        try:
            digest_params = digest.parse_www_auth(www_auth)
        except ValueError:
            digest_params = None
        if digest_params:
            signed = dict(headers or {})
            signed["Authorization"] = digest.authorization(
                method, path, descriptor.auth.user, descriptor.auth.password, digest_params
            )
            async with session.request(
                method, url, params=query, data=raw, headers=signed, timeout=timeout
            ) as response:
                status = response.status
                payload = await response.read()
                kind = response.content_type
    return status, kind, payload

def _resetup_matches(resetup: dict[str, Any], payload: bytes) -> bool:
    """Ответ говорит «сессия умерла» (`resetup.path` == `resetup.equals`)."""
    try:
        data = json.loads(payload.decode("utf-8", "ignore")) if payload else None
    except ValueError:
        data = None
    found = pick(data, str(resetup.get("path", "")))
    return found is not None and str(found) == str(resetup.get("equals"))

async def poll(ctx: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    values = base_values(descriptor)
    setup_block = spec.get("setup") if isinstance(spec.get("setup"), dict) else None
    # ⚠ `resetup` — сессия устройства умерла (регистратор видеонаблюдения
    # отвечает телом 200 `{"error_code":"no session"}` вечно, без 401):
    # описание говорит, по какому полю ответа и какому значению это видно, дом
    # входит ЗАНОВО (обновляет `carry` из `setup`) и повторяет запрос. Общая
    # семантика опроса, не вендорский код — вендора решает менеджер строкой
    # `path`/`equals`.
    resetup = spec.get("resetup") if isinstance(spec.get("resetup"), dict) else None
    async with aiohttp.ClientSession() as session:
        async def do_setup() -> None:
            if setup_block is None:
                return
            _, _, setup_payload = await _request(session, descriptor, setup_block, values)
            carry(values, setup_block, setup_payload)

        await do_setup()
        pause = seconds(spec.get("pause"), MIN_PAUSE_S, MIN_PAUSE_S)
        # ⚠ Одинаковый ответ подряд гасится только у КОРОТКОГО опроса (или по
        # `changesOnly`): у долгого два одинаковых ответа — два события (два
        # нажатия звонка), и второе пропадало бы.
        long_poll = spec.get("longPoll") is True
        changes_only = spec.get("changesOnly", not long_poll) is True
        last: tuple[int, bytes] | None = None
        loop = asyncio.get_running_loop()
        # Право войти сразу, если самый первый ответ уже скажет «нет сессии».
        last_resetup = loop.time() - pause
        while True:
            started = loop.time()
            status, kind, payload = await _request(session, descriptor, spec, values)
            if resetup is not None and setup_block is not None and _resetup_matches(resetup, payload):
                # Отказ сессии — не событие жильцу (это состояние опроса, а не
                # устройства), и без него самого дом просто повторит запрос.
                if loop.time() - last_resetup >= pause:
                    last_resetup = loop.time()
                    await do_setup()
                else:
                    # Вход не чаще `pause` — защита от горячего цикла, если
                    # устройство отвечает «нет сессии» и после входа.
                    await asyncio.sleep(pause)
                continue
            carry(values, spec, payload)
            if not changes_only or (status, payload) != last:
                last = (status, payload)
                ctx.emit(descriptor, source, spec, "response", {"status": status, "contentType": kind, **body_of(payload)})
            # ⚠ Долгий опрос без паузы — только пока устройство ДЕРЖИТ запрос. Ответ
            # быстрее секунды или отказ (401 после смены пароля, 404) без паузы
            # превращался в горячий цикл: дом долбил устройство и слал событие на
            # каждый ответ.
            quick = loop.time() - started < MIN_PAUSE_S
            await asyncio.sleep(pause if not long_poll or quick or status >= 400 else 0)

async def stream(ctx: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    import re

    values = base_values(descriptor)
    method = str(spec.get("method") or "GET").upper()
    path = render(str(spec.get("path") or "/"), values)
    params = _render_obj(spec["params"], values) if isinstance(spec.get("params"), dict) else None
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    scheme = "https" if descriptor.tls else "http"
    async with aiohttp.ClientSession() as session:
        async with session.request(
            method,
            _url(descriptor, path, scheme),
            params=params,
            timeout=aiohttp.ClientTimeout(total=None),
            **_auth_kwargs(descriptor),
        ) as response:
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
    try:
        while True:
            await asyncio.sleep(every)
            await send(render(str(block["send"]), values))
    except Exception as err:  # noqa: BLE001 — сокет уже закрыт: обрыв заметит чтение
        LOGGER.debug("keepalive источника остановлен: %s", type(err).__name__)

async def ws(ctx: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    values = base_values(descriptor)
    scheme = "wss" if descriptor.tls else "ws"
    path = render(str(spec.get("path") or "/"), values)
    # `params` и `headers` описания — как у `stream`: часть устройств просит
    # ключ в query или свой заголовок уже на рукопожатии.
    params = _render_obj(spec["params"], values) if isinstance(spec.get("params"), dict) else None
    headers = _render_obj(spec["headers"], values) if isinstance(spec.get("headers"), dict) else None
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    async with aiohttp.ClientSession() as session:
        socket_ = await session.ws_connect(
            _url(descriptor, path, scheme), params=params, headers=headers, heartbeat=30
        )

        async def receive() -> str:
            message = await socket_.receive()
            return message.data if message.type == aiohttp.WSMsgType.TEXT else ""

        keep: asyncio.Task[None] | None = None
        try:
            await _steps(socket_.send_str, receive, spec, values)
            keep = asyncio.ensure_future(_keepalive(socket_.send_str, spec, values))
            while True:
                message = await socket_.receive(timeout=idle)
                if message.type == aiohttp.WSMsgType.TEXT:
                    item: dict[str, Any] | None = {"text": message.data}
                elif message.type == aiohttp.WSMsgType.BINARY:
                    item = {"base64": base64.b64encode(message.data).decode("ascii")}
                else:
                    item = None
                if item is None:
                    return
                ctx.emit(descriptor, source, spec, "message", item)
        finally:
            if keep is not None:
                keep.cancel()
            await socket_.close()

async def mqtt(ctx: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    from secrets import token_hex

    from .mqtt import MqttClient

    topics = [str(t) for t in spec.get("topics") or [] if str(t)]

    def on_message(topic: str, data: bytes) -> None:
        ctx.emit(descriptor, source, spec, "message", {"topic": topic, **body_of(data)})

    client = MqttClient(
        descriptor.host,
        descriptor.port,
        descriptor.auth.user,
        descriptor.auth.password,
        tls=descriptor.tls,
        client_id=f"mega_home-{descriptor.id}-sub-{token_hex(4)}",
        on_message=on_message,
    )
    await client.connect()
    try:
        await client.subscribe(topics)
        await client.closed.wait()
    finally:
        await client.close()

async def tcp(ctx: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    values = base_values(descriptor)
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
