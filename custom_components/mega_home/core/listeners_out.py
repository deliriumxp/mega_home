"""Исходящие источники событий: дом сам держит подписку на устройство.

Виды (`events[]` описания, `docs/plan-thin-gateway.md`): `poll`, `stream`, `ws`,
`mqtt`, `tcp`. Все строки источника — ШАБЛОНЫ описания (`templating.py`): в них
видны адрес, поля учётки и значения, которые источник забрал из ответов.

  * курсор опроса — `carry: {имя: путь}`: поле ответа становится значением
    шаблона следующего запроса (`since`, `lastUpdateId`, адрес подписки);
  * `setup` — запрос ДО цикла (создать подписку у камеры протоколом
    PullPoint, поднять сессию UniFi) с теми же `carry`; `renew` — продление сессии
    раз в `every`, `teardown` — её закрытие перед новым входом и на остановке;
  * `steps` у `ws`/`tcp` — вход внутри соединения: послать шаблон, забрать поля
    ответа (`capture`) — Shelly Gen2 с паролем, TCP с вызовом-ответом;
  * `keepalive: {every, send}` — прикладной ping, без которого устройство рвёт
    соединение;
  * `idle` — сколько молчать до переподключения: устройство, пропавшее без FIN,
    иначе держало бы чтение вечно.

⚠ Шаблоны пишет только менеджер: бандл в источник событий не попадает никак.

⚠ Авторизация — `basic`/`digest`/`wsse`, как у `connect`: учётка — поля
описания устройства, шаблоны ничего не вычисляют (`templating.py`); подпись
стандартных схем ставит общий клиент (`connect.http_send`). Раньше вход шёл
через универсальную дверь (`gateway.py`, снесена) с сессиями и вычисляемыми
шаблонами — здесь дом сам держит `aiohttp.ClientSession` на источник.
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import aiohttp

from . import wsse
from .connect import http_send
from .const import LOGGER
from .devices import DeviceDescriptor
from .templating import parsed, pick, render

MIN_PAUSE_S = 1.0
IDLE_S = 120.0
MAX_CHUNK = 64 * 1024
# Закрытие сессии устройства не держит остановку источника дольше этого.
TEARDOWN_S = 5.0

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
    data = parsed(payload)
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

def _session(descriptor: DeviceDescriptor) -> aiohttp.ClientSession:
    """Сессия к устройству: `tls` — без проверки сертификата, как у `connect.py`.

    ⚠ Без этого источник к устройству с самоподписанным сертификатом (регистратор
    видеонаблюдения, 8080/https) падал на первом же `setup` и молча перезапускался
    — объект остался без ленты событий (живой объект 2026-09-20). Проверять
    сертификат нечем: адресаты — только частные адреса объекта.
    """
    connector = aiohttp.TCPConnector(ssl=False) if descriptor.tls else None
    return aiohttp.ClientSession(connector=connector)


def _call(descriptor: DeviceDescriptor, block: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    """Шаблон запроса описания → вызов для ОБЩЕГО клиента дома (`connect.http_send`).

    ⚠ `params` дописываются в путь ЗДЕСЬ, а не отдаются aiohttp: Digest
    подписывает путь вместе со строкой запроса, и подпись без неё устройство
    отвергает (так и было до 0.5.6). Кодируются они тоже здесь: подставленный
    в `path` пароль со спецсимволом уезжал бы битым (`docs/home-gateway.md`).
    """
    body = block.get("body")
    raw = b""
    if isinstance(body, (dict, list)):
        raw = json.dumps(_render_obj(body, values)).encode()
    elif isinstance(body, str) and body:
        raw = render(body, values).encode()
    path = render(str(block.get("path") or "/"), values)
    port = descriptor.port
    if path.startswith(("http://", "https://")):
        # Адрес, который устройство выдало ответом (подписка PullPoint: полный URL
        # в `carry`), — берётся путь, а порт — только у ТОГО ЖЕ хоста (сервис событий
        # камеры бывает на своём порту): чужой адрес из ответа дом не открывает.
        parts = urlsplit(path)
        if parts.hostname == descriptor.host and parts.port:
            port = parts.port
        path = f"{parts.path or '/'}{'?' + parts.query if parts.query else ''}"
    if isinstance(block.get("params"), dict):
        query = urlencode(_render_obj(block["params"], values), quote_via=quote)
        path = f"{path}{'&' if '?' in path else '?'}{query}" if query else path
    headers = _render_obj(block["headers"], values) if isinstance(block.get("headers"), dict) else {}
    auth = descriptor.auth
    return {
        "method": str(block.get("method") or "GET").upper(),
        "path": path,
        "url": f"{'https' if descriptor.tls else 'http'}://{descriptor.host}:{port}{path}",
        "headers": headers,
        "body": raw,
        "basic": aiohttp.BasicAuth(auth.user, auth.password) if auth.type == "basic" else None,
        "digest": (auth.user, auth.password) if auth.type == "digest" else None,
        "wsse": wsse.Credentials(auth.user, auth.password, auth.clock) if auth.type == "wsse" else None,
    }

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
    total = max(descriptor.timeout, 120.0) if block.get("longPoll") is True else descriptor.timeout
    call = _call(descriptor, block, values)
    async with await http_send(session, call, aiohttp.ClientTimeout(total=total)) as response:
        return response.status, response.content_type, await response.read()

def _resetup_matches(resetup: dict[str, Any], status: int, payload: bytes) -> bool:
    """Ответ говорит «сессия умерла»: код ответа не ниже `resetup.status` либо
    поле `resetup.path` равно `resetup.equals`.

    ⚠ `status` — у подписок, которые устройство хоронит отказом: истёкшая
    подписка PullPoint отвечает SOAP Fault с кодом 400/500, и без перехода к
    `setup` дом долбил бы мёртвый адрес вечно.
    """
    floor = resetup.get("status")
    if isinstance(floor, int) and not isinstance(floor, bool) and status >= floor:
        return True
    if "path" not in resetup:
        return False
    return pick(parsed(payload), str(resetup.get("path", ""))) == str(resetup.get("equals"))

def _wanted(spec: dict[str, Any], payload: bytes) -> bool:
    """Публиковать ли ответ: `onlyIf.contains` — подстрока, без которой ответ тишина.

    ⚠ У длинного опроса с метками в теле (`CurrentTime` PullPoint) каждый пустой
    ответ отличен от прошлого, и `changesOnly` его не гасит: хранилище ленты
    (`device_store.py`) за часы вытесняло бы настоящие события пустыми.
    """
    rule = spec.get("onlyIf")
    if not isinstance(rule, dict) or not rule.get("contains"):
        return True
    return str(rule["contains"]).encode() in payload

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
    # ⚠ `renew` и `teardown` — продление и закрытие сессии устройства (подписка PullPoint:
    # `Renew`, `Unsubscribe` WS-BaseNotification, как у интеграции камер HA). Без закрытия брошенная
    # подписка живёт у устройства до своего срока, а их число ограничено: стенд Tapo держит ОДНУ,
    # и после перезапуска дома события молчали бы до десяти минут.
    renew_block = spec.get("renew") if isinstance(spec.get("renew"), dict) else None
    teardown_block = spec.get("teardown") if isinstance(spec.get("teardown"), dict) else None
    opened = False
    async with _session(descriptor) as session:
        async def do_teardown() -> None:
            nonlocal opened
            if teardown_block is None or not opened:
                return
            opened = False
            try:
                await asyncio.wait_for(_request(session, descriptor, teardown_block, values), TEARDOWN_S)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                LOGGER.debug("Источник %s/%s: закрыть сессию не вышло: %s", descriptor.id, source, type(err).__name__)

        async def do_setup() -> None:
            nonlocal opened
            if setup_block is None:
                return
            await do_teardown()
            status, _, setup_payload = await _request(session, descriptor, setup_block, values)
            carry(values, setup_block, setup_payload)
            opened = status < 400

        renewing = asyncio.ensure_future(_renew(session, descriptor, renew_block, values, lambda: opened)) if renew_block else None
        try:
            await _poll_loop(ctx, session, descriptor, source, spec, values, do_setup, setup_block, resetup)
        finally:
            if renewing is not None:
                renewing.cancel()
            await do_teardown()

async def _renew(session: aiohttp.ClientSession, descriptor: DeviceDescriptor, block: dict[str, Any], values: dict[str, Any], opened: Any) -> None:
    """Продлевать сессию устройства раз в `every`, пока она открыта; отказ продления ловит сам опрос."""
    every = seconds(block.get("every"), 300.0, 5.0)
    while True:
        await asyncio.sleep(every)
        if not opened():
            continue
        try:
            await _request(session, descriptor, block, values)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            LOGGER.debug("Источник %s: продление не прошло: %s", descriptor.id, type(err).__name__)

async def _poll_loop(
    ctx: Any, session: aiohttp.ClientSession, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any],
    values: dict[str, Any], do_setup: Any, setup_block: dict[str, Any] | None, resetup: dict[str, Any] | None,
) -> None:
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
        if resetup is not None and setup_block is not None and _resetup_matches(resetup, status, payload):
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
        if _wanted(spec, payload) and (not changes_only or (status, payload) != last):
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

    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    call = _call(descriptor, spec, base_values(descriptor))
    async with _session(descriptor) as session:
        async with await http_send(session, call, aiohttp.ClientTimeout(total=None)) as response:
            # ⚠ Отказ устройства — ОТКАЗ, а не поток: тело 401 уходило бы
            # «событиями», а источник тихо перезапускался бы по кругу.
            if response.status >= 400:
                raise ConnectionError(f"устройство ответило {response.status}")
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
    # `params` и `headers` описания — как у `stream`: часть устройств просит
    # ключ в query или свой заголовок уже на рукопожатии.
    call = _call(descriptor, spec, values)
    idle = seconds(spec.get("idle"), IDLE_S, 5.0)
    async with _session(descriptor) as session:
        socket_ = await session.ws_connect(
            call["url"].replace("http", "ws", 1), headers=call["headers"], heartbeat=30
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
