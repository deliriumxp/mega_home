"""Единственный контракт транспорта наружу — `connect` (`docs/plan-thin-gateway.md`).

⚠ `stream: true` в этом модуле НЕ обслуживается: держать соединение и слать
кадры — работа `stream.py` (сессии `stream.open/close`, те же лимиты
`MAX_STREAMS`/`IDLE_TIMEOUT_S`/`MAX_BYTES`), которая ходит тем же реестром
служб. Не переизобретаем вторую сессионную машину поверх этого контракта.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import re
import socket
from typing import Any
from urllib.parse import unquote

import aiohttp

from . import digest, services
from .ops_base import OpError

MAX_SEND = 64 * 1024
MAX_READ = 4 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT = 15.0
MAX_TIMEOUT = 120.0
MAX_WS_MESSAGES = 64
KINDS = ("tcp", "udp", "http", "ws")

async def perform(payload: dict[str, Any]) -> dict[str, Any]:
    """Выполнить один вызов `connect`; ошибка и статус ≥ 400 — `OpError`."""
    if not isinstance(payload, dict):
        raise OpError("Ожидается объект запроса")
    if payload.get("stream") is True:
        raise OpError(
            "Держать соединение — служебные кадры stream.*, а не connect", 501
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

async def _http(payload: dict[str, Any]) -> dict[str, Any]:
    host, port = resolve_address(payload)
    method = str(payload.get("method") or "GET").upper()
    path = str(payload.get("path") or "/")
    if not path.startswith("/"):
        raise OpError("path начинается с «/»")
    _refuse_double_encoded(path)
    tls = payload.get("tls") is True
    url = f"{'https' if tls else 'http'}://{host}:{port}{path}"
    headers = dict(payload.get("headers")) if isinstance(payload.get("headers"), dict) else {}
    body = _body_of(payload)
    timeout = _timeout(payload)
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
    connector = aiohttp.TCPConnector(ssl=False) if tls else None
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            status, out_headers, raw = await _request_once(
                session, method, url, headers, basic, body, timeout
            )
            # ⚠ Digest требует ДВА круга: digest_params приходит только в ответе на
            # первый запрос без Authorization. Второй круг дом делает сам, чтобы
            # бандлу на другом конце канала не гонять их через менеджер —
            # RFC 7616 остаётся стандартом HTTP, не вендором (`digest.py`).
            if digest_creds and status == 401:
                www_auth = next(
                    (v for k, v in out_headers.items() if k.lower() == "www-authenticate"), ""
                )
                try:
                    digest_params = digest.parse_www_auth(www_auth)
                except ValueError:
                    digest_params = None
                if digest_params:
                    signed = dict(headers)
                    signed["Authorization"] = digest.authorization(
                        method, path, digest_creds[0], digest_creds[1], digest_params
                    )
                    status, out_headers, raw = await _request_once(
                        session, method, url, signed, None, body, timeout
                    )
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise OpError(_reason(err), 502) from err
    return _http_answer(status, out_headers, raw)

async def _request_once(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    headers: dict[str, str],
    auth: aiohttp.BasicAuth | None,
    body: bytes,
    timeout: float,
) -> tuple[int, dict[str, str], bytes]:
    async with session.request(
        method,
        url,
        data=body or None,
        headers=headers,
        auth=auth,
        allow_redirects=False,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:
        raw = await _read_all(response)
        return response.status, {str(k): str(v) for k, v in response.headers.items()}, raw

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
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
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
    def __init__(self) -> None:
        self.data = b""
        self.got = False

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if not self.got:
            self.data = data
            self.got = True

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
    try:
        transport.sendto(send, (host, port))
        if window:
            end = loop.time() + window
            while loop.time() < end and not protocol.got:
                await asyncio.sleep(0.02)
    finally:
        transport.close()
    return _body_answer(protocol.data)

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
        async with aiohttp.ClientSession() as session:
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
