"""Единственный контракт транспорта наружу — `connect` (`docs/plan-thin-gateway.md`).

У него ТРИ формы потребления ответа, и все три — один и тот же разбор описания:
- данные коду: `POST api/connect` → тело в JSON (`perform`);
- ресурс браузеру: `GET api/connect?req=…` → тело как есть (`resource`);
- удержание: `GET api/connect` с `Upgrade: websocket` → кадры `stream.*`
  (`stream.py`, исполняет ту же форму `ConnectRequest`).

⚠ `stream: true` ФЛАГОМ не обслуживается и не будет: односторонняя форма не даёт
послать в открытую сессию то, что вызывающий посчитал сам (одноразовая метка,
подпись, кадр бинарного протокола), а браузер потоковое тело запроса не умеет
вовсе. Держать соединение — это третья форма выше, а не поле у одиночного вызова.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import re
import socket
import ssl
from typing import Any
from urllib.parse import unquote

import aiohttp

from . import digest, services
from .ops_base import OpError

MAX_SEND = 64 * 1024
MAX_READ = 4 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
# 200 с, не 120: бандл держит длинный опрос архива регистратора видеонаблюдения
# 180 с (`HOME_LONG_POLL_MS`, менеджер) — доездом из описания доступа до 0.4.0
# (`longPoll: { timeout: 180 }`). Меньший потолок срезал бы запрос и выродил
# длинный опрос в частый.
DEFAULT_TIMEOUT = 15.0
MAX_TIMEOUT = 200.0
MAX_WS_MESSAGES = 64
KINDS = ("tcp", "udp", "http", "ws")
# Кадр камеры живёт ровно период обновления плитки (`TILE_REFRESH_MS` бандла):
# столько же браузеру позволено не ходить за ним второй раз. `private` —
# ответ принадлежит этому жильцу, общим кэшам по пути в нём делать нечего.
RESOURCE_CACHE = "private, max-age=30"

async def perform(payload: dict[str, Any]) -> dict[str, Any]:
    """Выполнить один вызов `connect`. Отказ ДОМА — `OpError`; ответ адресата
    с любым статусом — данные (`status` в ответе), толкует его вызывающий."""
    if not isinstance(payload, dict):
        raise OpError("Ожидается объект запроса")
    if payload.get("stream") is True:
        raise OpError(
            "Держать соединение — форма Upgrade того же api/connect, а не флаг", 501
        )
    kind = str(payload.get("kind") or "")
    if kind == "http":
        return await _http(payload)
    if kind == "tcp":
        return await _tcp(payload)
    if kind == "udp":
        return await _udp(payload)
    if kind == "ws":
        return await _ws(payload)
    raise OpError(f"Вид «{kind}» connect не умеет — {', '.join(KINDS)}")

async def resource(payload: dict[str, Any]) -> tuple[int, str, bytes, str]:
    """Тот же вызов, но ответ — РЕСУРС для браузера: `(статус, тип, байты, кэш)`.

    ⚠ Ось «потребление ответа» (`docs/home-gateway.md`, «Полнота двери»). У
    `perform` ответ приходит данными КОДУ — JSON с телом в base64, — и `<img>`,
    `<video>` или ссылка на файл взять его не могут. Кадр плитки из-за этого шёл
    тремя ходками через `api/connect` вместо одной, а дом обзавёлся маршрутом
    `api/camera-frame/<id>` в обход собственной двери. Здесь тот же `connect`
    отдаёт тело КАК ЕСТЬ, с типом от адресата, — и адрес снова можно просто
    подставить в тег.

    ⚠ БЕЗ УЧЁТКИ и только `GET`. Описание вызова едет параметром адреса, а
    адреса попадают в журналы (HA, менеджер, прокси на пути) — пароль в них
    попасть не должен. Это не ограничение «на всякий случай», а граница класса:
    ресурс браузера — то, что вендор и так отдаёт по ссылке (кадр по токену,
    `frame.jpeg` у go2rtc). Нужна учётка — это не ресурс, иди обычным `POST`.
    """
    if not isinstance(payload, dict):
        raise OpError("Ожидается объект запроса")
    kind = str(payload.get("kind") or "http")
    if kind != "http":
        raise OpError("Ресурсом отдаётся только http", 501)
    if payload.get("auth"):
        raise OpError("Ресурс не носит учётку — этот вызов идёт POST", 400)
    if payload.get("body") or payload.get("bodyBase64"):
        raise OpError("У ресурса нет тела запроса", 400)
    method = str(payload.get("method") or "GET").upper()
    if method != "GET":
        raise OpError("Ресурс берётся только GET", 405)
    answer = await _http({**payload, "method": "GET"})
    raw = (
        base64.b64decode(answer["bodyBase64"])
        if answer.get("bodyBase64") is not None
        else str(answer.get("body") or "").encode("utf-8")
    )
    headers = answer.get("headers") or {}
    content_type = next(
        (str(v) for k, v in headers.items() if str(k).lower() == "content-type"),
        "application/octet-stream",
    )
    # ⚠ Метка кэша — НАША и короткая, а не адресата: за дверью живёт кадр
    # «сейчас», и чужой `max-age` (у go2rtc его нет вовсе, у регистратора он
    # про статику) либо заморозил бы картинку, либо не дал бы браузеру не
    # ходить второй раз в тот же период обновления плитки.
    return int(answer.get("status") or 200), content_type, raw, RESOURCE_CACHE

def resolve_address(payload: dict[str, Any]) -> tuple[str, int]:
    """`host` — частный IPv4 объекта или имя ПОДНЯТОЙ службы дома."""
    host = str(payload.get("host") or "").strip()
    if not host:
        raise OpError("host не указан")
    service_port = services.resolve(host)
    if service_port is not None:
        return "127.0.0.1", service_port
    try:
        address = ipaddress.ip_address(host)
    except ValueError as err:
        raise OpError("host — частный IPv4 объекта или имя службы дома") from err
    if (
        address.version != 4
        or address.is_loopback
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
        or not address.is_private
    ):
        raise OpError("host вне локальной сети объекта")
    try:
        port = int(payload.get("port"))
    except (TypeError, ValueError) as err:
        raise OpError("port не указан") from err
    if not 1 <= port <= 65535:
        raise OpError("port не указан")
    return str(address), port

def prepare_http(payload: dict[str, Any]) -> dict[str, Any]:
    """Разобрать описание HTTP-вызова: адрес, заголовки, тело, учётка, срок.

    ⚠ ОДИН разбор на обе формы `connect`: одиночный вызов (`_http`) и удержание
    (`stream.py`, сессия вида `http`). Второй разбор разошёлся бы с первым на
    первой же правке, а расходиться здесь значит «Digest работает в вызове и не
    работает в сессии» — ровно тот класс дефекта, ради которого у транспорта
    один контракт.
    """
    host, port = resolve_address(payload)
    method = str(payload.get("method") or "GET").upper()
    path = str(payload.get("path") or "/")
    if not path.startswith("/"):
        raise OpError("path начинается с «/»")
    _refuse_double_encoded(path)
    tls = payload.get("tls") is True
    basic: aiohttp.BasicAuth | None = None
    digest_creds: tuple[str, str] | None = None
    auth = payload.get("auth") if isinstance(payload.get("auth"), dict) else None
    if auth:
        auth_type = str(auth.get("type") or "")
        user, secret = str(auth.get("user") or ""), str(auth.get("pass") or "")
        if auth_type == "basic":
            basic = aiohttp.BasicAuth(user, secret)
        elif auth_type == "digest":
            digest_creds = (user, secret)
        elif auth_type and auth_type != "none":
            raise OpError("auth.type — basic или digest")
    return {
        "method": method,
        "path": path,
        "url": f"{'https' if tls else 'http'}://{host}:{port}{path}",
        "headers": dict(payload.get("headers")) if isinstance(payload.get("headers"), dict) else {},
        "body": _body_of(payload),
        "timeout": _timeout(payload),
        "basic": basic,
        "digest": digest_creds,
        # ⚠ Сертификат устройства объекта самоподписанный, проверять его нечем:
        # `resolve_address` пускает только частные адреса (та же причина, что у
        # ветки `ws` ниже).
        "connector": aiohttp.TCPConnector(ssl=False) if tls else None,
    }

async def http_send(
    session: aiohttp.ClientSession, call: dict[str, Any], timeout: aiohttp.ClientTimeout
) -> aiohttp.ClientResponse:
    """Запрос по разобранному описанию (`prepare_http`) — ОТКРЫТЫЙ ответ.

    ⚠ ЕДИНСТВЕННЫЙ HTTP-клиент дома: одиночный вызов (`_http`), сессия вида
    `http` (`stream.py`) и слушатели (`listeners_out.py`). До 0.5.6 их было
    три, и третий разошёлся: подписывал Digest путём БЕЗ строки запроса (устройство
    сверяет `uri` с адресом и отказывает), а поток событий Digest не умел вовсе.

    ⚠ Digest требует ДВА круга: параметры приходят только в ответе на запрос без
    `Authorization`. Второй круг дом делает сам, чтобы вызывающему на другом
    конце канала не гонять их через менеджер — RFC 7616 это стандарт HTTP, не
    вендор (`digest.py`). Подписывается `call["path"]` — путь СО строкой запроса.
    """
    method, url, body, headers = call["method"], call["url"], call["body"] or None, call["headers"]
    response = await session.request(
        method, url, data=body, headers=headers, auth=call["basic"], allow_redirects=False, timeout=timeout
    )
    if not call["digest"] or response.status != 401:
        return response
    try:
        params = digest.parse_www_auth(response.headers.get("WWW-Authenticate", ""))
    except ValueError:
        return response
    response.release()
    signed = {**headers, "Authorization": digest.authorization(method, call["path"], *call["digest"], params)}
    return await session.request(
        method, url, data=body, headers=signed, allow_redirects=False, timeout=timeout
    )

async def _http(payload: dict[str, Any]) -> dict[str, Any]:
    call = prepare_http(payload)
    try:
        async with aiohttp.ClientSession(connector=call["connector"]) as session:
            timeout = aiohttp.ClientTimeout(total=call["timeout"])
            async with await http_send(session, call, timeout) as response:
                raw = await _read_all(response)
                headers = {str(k): str(v) for k, v in response.headers.items()}
                return _http_answer(response.status, headers, raw)
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise OpError(_reason(err), 502) from err

_TLS: ssl.SSLContext | None = None

def tls_context() -> ssl.SSLContext:
    """TLS БЕЗ проверки сертификата — один на весь дом (`connect`, сессии, MQTT, проба).

    ⚠ Проверять нечем: сертификаты устройств объекта самоподписанные, а адреса
    пускает только частная сеть (`resolve_address`). ⚠ `SSLContext(...)`, а не
    `create_default_context()`: тот грузит системные сертификаты — блокирующий
    вызов в цикле событий (Home Assistant ругается на `load_default_certs`), и
    всё ради проверки, которую мы тут же выключаем.
    """
    global _TLS
    if _TLS is None:
        _TLS = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        _TLS.check_hostname = False
        _TLS.verify_mode = ssl.CERT_NONE
    return _TLS

def _refuse_double_encoded(path: str) -> None:
    """Двойное %-кодирование пути — отказ (сохранённая проверка `gateway.py`)."""
    encoded = path.split("?", 1)[0]
    if re.search(r"%[0-9A-Fa-f]{2}", unquote(encoded)):
        raise OpError("Путь закодирован дважды — через connect не ходит")

def _body_of(payload: dict[str, Any]) -> bytes:
    if payload.get("bodyBase64") is not None:
        return _decode_b64(payload["bodyBase64"], "bodyBase64")
    body = payload.get("body")
    if body is None:
        return b""
    if isinstance(body, (dict, list)):
        raise OpError("body — строка; для байтов bodyBase64")
    return str(body).encode("utf-8")

def _http_answer(status: int, headers: dict[str, str], raw: bytes) -> dict[str, Any]:
    out: dict[str, Any] = {"status": int(status), "headers": headers}
    out.update(_body_answer(raw))
    return out

async def _read_all(response: Any) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise OpError("Ответ больше потолка connect", 507)
        chunks.append(chunk)
    return b"".join(chunks)

async def _tcp(payload: dict[str, Any]) -> dict[str, Any]:
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
    context = tls_context() if payload.get("tls") is True else None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=context), timeout)
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

async def _udp(payload: dict[str, Any]) -> dict[str, Any]:
    host, port = resolve_address(payload)
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

def _body_answer(raw: bytes) -> dict[str, Any]:
    try:
        return {"body": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"bodyBase64": base64.b64encode(raw).decode("ascii")}

async def _ws(payload: dict[str, Any]) -> dict[str, Any]:
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
        # ⚠ `tls` — как у HTTP-ветки: сертификат устройства самоподписанный,
        # проверять его нечем (`resolve_address` пускает только частные адреса).
        connector = aiohttp.TCPConnector(ssl=False) if payload.get("tls") is True else None
        async with aiohttp.ClientSession(connector=connector) as session:
            socket_ = await session.ws_connect(url, headers=headers, timeout=timeout)
            got: list[dict[str, Any]] = []
            try:
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

def _decode_b64(value: Any, what: str) -> bytes:
    if value in (None, ""):
        return b""
    try:
        raw = base64.b64decode(str(value), validate=True)
    except ValueError as err:
        raise OpError(f"{what}: ожидается base64") from err
    if len(raw) > MAX_SEND:
        raise OpError(f"{what}: больше {MAX_SEND} байт")
    return raw

def _timeout(payload: dict[str, Any], default: float = DEFAULT_TIMEOUT) -> float:
    try:
        value = float(payload.get("timeout"))
    except (TypeError, ValueError):
        return default
    return min(max(value, 0.1), MAX_TIMEOUT) if value > 0 else default

def _reason(err: Exception) -> str:
    """Причина отказа словами — без адреса запроса (учётка бывает в query)."""
    status = getattr(err, "status", None)
    if status:
        return f"Система ответила ошибкой (HTTP {status})"
    if isinstance(err, asyncio.TimeoutError):
        return "Система не отвечает: таймаут"
    return f"Система не отвечает ({type(err).__name__})"
