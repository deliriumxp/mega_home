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
from aiohttp.abc import AbstractResolver

from . import digest, services, wsse
from . import reduce as reduce_mod
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
    payload = with_service(payload)
    kind = str(payload.get("kind") or "")
    # Обработка ответа (`reduce.py`) — разбор описания ДО вызова: ошибка в нём не
    # должна стоить источнику мегабайтов истории.
    plan = reduce_mod.parse(payload["reduce"]) if payload.get("reduce") is not None else None
    if plan is not None and kind != "http":
        raise OpError("reduce обрабатывает только ответ kind: http")
    if kind == "http":
        return await _http(payload, plan)
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
    if payload.get("reduce") is not None:
        raise OpError("Обработанный ответ — данные коду, он идёт POST", 400)
    method = str(payload.get("method") or "GET").upper()
    if method != "GET":
        raise OpError("Ресурс берётся только GET", 405)
    # ⚠ Ресурсом — только устройство LAN и служба дома, не интернет: ответ ресурса браузер
    # открывает в ORIGIN ДВЕРИ (HA объекта, менеджер), и чужая страница по ссылке
    # `api/connect?req=…` исполнила бы там свой скрипт с ключом жильца или сессией инженера.
    # Данные из интернета — коду, `POST` (`perform`); картинки интернета браузер берёт сам.
    if is_name(str(payload.get("host") or "").strip()) and services.entry(str(payload.get("host") or "").strip()) is None:
        raise OpError("Ресурсом отдаётся устройство объекта или служба дома; интернет — данными коду (POST)", 403)
    answer = await _http(with_service({**payload, "method": "GET"}))
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

def with_service(payload: dict[str, Any]) -> dict[str, Any]:
    """Вызов к службе дома получает то, что служба знает о себе: TLS и учётку.

    ⚠ Заголовки службы ПЕРЕБИВАЮТ заголовки вызывающего — учётку службы ставит
    дом (`services.Service`), чужой `Authorization` к ней не доедет.
    """
    service = services.entry(str(payload.get("host") or "").strip())
    if service is None:
        return payload
    body = payload.get("body") if isinstance(payload.get("body"), str) else ""
    if payload.get("bodyBase64"):
        body = _decode_b64(payload["bodyBase64"], "bodyBase64").decode("utf-8", "replace")
    for what, value in [("path", str(payload.get("path") or "/")), ("body", body)] + [
        ("frame", item if isinstance(item, str) else json.dumps(item))
        for item in (payload.get("messages") if isinstance(payload.get("messages"), list) else [])
    ]:
        reason = service.guard(what, value) if service.guard else None
        if reason:
            raise OpError(reason, 403)
    headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else {}
    return {
        **payload,
        "tls": service.tls or payload.get("tls") is True,
        "headers": {**headers, **service.headers},
    }

def refused(payload: dict[str, Any], what: str, value: str) -> str | None:
    """Причина отказа службы на кадр открытой сессии; `None` — пропустить (`Service.guard`)."""
    service = services.entry(str(payload.get("host") or "").strip())
    return service.guard(what, value) if service is not None and service.guard else None

async def open_ws(payload: dict[str, Any], socket_: Any) -> None:
    """Рукопожатие службы на только что открытом сокете — до кадров вызывающего."""
    service = services.entry(str(payload.get("host") or "").strip())
    if service is not None and service.ws_open is not None:
        await service.ws_open(socket_)

def resolve_address(payload: dict[str, Any]) -> tuple[str, int]:
    """`host` — частный IPv4 объекта, имя ПОДНЯТОЙ службы дома или имя хоста в интернете.

    ⚠ Ось «адресат» (`docs/home-gateway.md`, решение заказчика 2026-10-20): дом ходит и в
    интернет — каталоги, ленты, облака вендоров, — но ТОЛЬКО ПО ИМЕНИ. Литеральный адрес
    по-прежнему лишь частный: у голого IP нет имени, по которому проверить сертификат, а
    у имени есть (`connector_for`, `pinned`). Имя здесь проверяется лишь написанием;
    куда оно ведёт, решает `pinned` перед соединением.
    """
    host = str(payload.get("host") or "").strip()
    if not host:
        raise OpError("host не указан")
    service_port = services.resolve(host)
    if service_port is not None:
        return "127.0.0.1", service_port
    try:
        address = ipaddress.ip_address(host)
    except ValueError as err:
        if not is_name(host):
            raise OpError("host — частный IPv4 объекта, имя службы дома или имя хоста в интернете") from err
        return host.lower(), _port(payload)
    if (
        address.version != 4
        or address.is_loopback
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
        or not address.is_private
    ):
        raise OpError("host вне локальной сети объекта (в интернет — по имени хоста)")
    return str(address), _port(payload)

def _port(payload: dict[str, Any]) -> int:
    try:
        port = int(payload.get("port"))
    except (TypeError, ValueError) as err:
        raise OpError("port не указан") from err
    if not 1 <= port <= 65535:
        raise OpError("port не указан")
    return port

# Имя хоста по RFC 1123: метки из букв, цифр и дефиса, хотя бы одна точка. Без точки — это
# имя службы дома (`services.py`), которая не поднята, а не адрес в интернете.
_NAME = re.compile(r"^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{0,61}[a-z0-9]$")

def is_name(host: str) -> bool:
    """Адресат по имени хоста (а не литеральный IP и не служба дома)."""
    return bool(_NAME.match(host.lower()))

async def pinned(host: str, port: int, family: int = socket.AF_UNSPEC) -> list[dict[str, Any]]:
    """Адреса имени, куда дом согласен идти, — в форме резолвера aiohttp.

    ⚠ Отказ, если ХОТЬ ОДИН адрес — сама машина, link-local (169.254/16: метаданные облака,
    автонастройка), мультикаст или служебный: такое имя — не «хост в интернете», а обход
    правила «loopback только по имени службы». Частные адреса по имени пускаются: до них
    вызывающий и так достаёт литералом. Соединение идёт РОВНО на проверенные адреса
    (`_Pinned`), второго разрешения имени нет — подмена DNS между проверкой и соединением
    ничего не даёт.
    """
    try:
        infos = await _getaddrinfo(host, port, family)
    except OSError as err:
        raise OpError(f"Имя {host} не находится в DNS", 502) from err
    found: list[dict[str, Any]] = []
    for af, _, proto, _, sockaddr in infos:
        address = ipaddress.ip_address(str(sockaddr[0]).split("%", 1)[0])
        if address.is_loopback or address.is_link_local or address.is_multicast or address.is_unspecified or address.is_reserved:
            raise OpError(f"Имя {host} ведёт на служебный адрес {address}", 403)
        found.append({"hostname": host, "host": str(sockaddr[0]), "port": port, "family": af, "proto": proto, "flags": socket.AI_NUMERICHOST})
    if not found:
        raise OpError(f"Имя {host} не находится в DNS", 502)
    return found

# Срок разрешения имени — свой: оно идёт ДО срока вызова (соединителю нужны готовые адреса), и
# молчащий DNS иначе прибавлял бы к `timeout` вызывающего ещё и свои минуты.
DNS_TIMEOUT = 10.0

async def _getaddrinfo(host: str, port: int, family: int) -> list[Any]:
    try:
        return await asyncio.wait_for(
            asyncio.get_running_loop().getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM), DNS_TIMEOUT
        )
    except asyncio.TimeoutError as err:
        raise OSError("DNS не ответил") from err

async def _refuse_redirect(session: Any, context: Any, params: Any) -> None:
    raise aiohttp.ClientError("Адресат перенаправляет запрос — connect по перенаправлениям не ходит")

def no_redirects() -> list[aiohttp.TraceConfig]:
    """Отказ на перенаправление — для `ws_connect`, где `allow_redirects` не задать.

    ⚠ Перенаправление на литеральный адрес aiohttp соединяет МИМО резолвера (`_Pinned`): имя в
    интернете увело бы дом на его же loopback или 169.254. У HTTP перенаправлений нет и так
    (`http_send`, `allow_redirects=False`). Подпись трассировки — с aiohttp 3.0, в отличие от
    `socket_factory` (3.12): парк HA объектов старше.
    """
    trace = aiohttp.TraceConfig()
    trace.on_request_redirect.append(_refuse_redirect)
    return [trace]

class _Pinned(AbstractResolver):
    """Резолвер одного соединения: отдаёт адреса, уже проверенные `pinned`."""

    def __init__(self, addresses: list[dict[str, Any]]) -> None:
        self._addresses = addresses

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET) -> list[dict[str, Any]]:
        picked = [a for a in self._addresses if family in (socket.AF_UNSPEC, a["family"])]
        if host.lower() != self._addresses[0]["hostname"] or not picked:
            raise OSError(f"{host}: адрес не проверен домом")
        return picked

    async def close(self) -> None:
        return None

async def connector_for(host: str, port: int, tls: bool) -> aiohttp.TCPConnector | None:
    """Соединитель aiohttp для адресата `resolve_address`.

    ⚠ По имени сертификат ПРОВЕРЯЕТСЯ (`ssl` по умолчанию у aiohttp — проверенный контекст,
    собранный при его импорте, без блокирующей загрузки в цикле): в интернете самоподписанных
    устройств объекта нет, а без проверки ответ подменит любой узел по пути. Литеральный адрес
    — устройство LAN, его сертификат проверять нечем (`tls_context`).
    """
    if is_name(host):
        return aiohttp.TCPConnector(resolver=_Pinned(await pinned(host, port)))
    return aiohttp.TCPConnector(ssl=False) if tls else None

async def socket_target(host: str, port: int, tls: bool, family: int = socket.AF_UNSPEC) -> tuple[str, ssl.SSLContext | None, str | None]:
    """Адрес, TLS и имя для SNI голого сокета (`tcp`, `udp`, сессии): имя — проверенным адресом."""
    if not is_name(host):
        return host, tls_context() if tls else None, None
    address = (await pinned(host, port, family))[0]["host"]
    return address, await verified_tls() if tls else None, host if tls else None

_VERIFIED: ssl.SSLContext | None = None

async def verified_tls() -> ssl.SSLContext:
    """TLS С проверкой сертификата — для сокетов по имени. Системные сертификаты грузятся
    в исполнителе: в цикле событий это блокирующий вызов (`tls_context`)."""
    global _VERIFIED
    if _VERIFIED is None:
        _VERIFIED = await asyncio.get_running_loop().run_in_executor(None, ssl.create_default_context)
    return _VERIFIED

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
    signer: wsse.Credentials | None = None
    auth = payload.get("auth") if isinstance(payload.get("auth"), dict) else None
    if auth:
        auth_type = str(auth.get("type") or "")
        user, secret = str(auth.get("user") or ""), str(auth.get("pass") or "")
        if auth_type == "basic":
            basic = aiohttp.BasicAuth(user, secret)
        elif auth_type == "digest":
            digest_creds = (user, secret)
        elif auth_type == "wsse":
            signer = wsse.Credentials(user, secret, auth.get("clock") if isinstance(auth.get("clock"), dict) else None)
        elif auth_type and auth_type != "none":
            raise OpError("auth.type — basic, digest или wsse")
    return {
        "method": method,
        "path": path,
        "url": f"{'https' if tls else 'http'}://{host}:{port}{path}",
        "headers": dict(payload.get("headers")) if isinstance(payload.get("headers"), dict) else {},
        "body": _body_of(payload),
        "timeout": _timeout(payload),
        "basic": basic,
        "digest": digest_creds,
        "wsse": signer,
        # Соединитель — `client_for`: адрес по имени проверяется DNS-запросом, а он асинхронный.
        "host": host,
        "port": port,
        "tls": tls,
    }

async def client_for(call: dict[str, Any]) -> aiohttp.ClientSession:
    """Сессия aiohttp под разобранный вызов (`prepare_http`): соединитель — `connector_for`."""
    return aiohttp.ClientSession(connector=await connector_for(call["host"], call["port"], call["tls"]))

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

    WS-Security UsernameToken (`wsse.py`) — подпись в теле SOAP, свежая на каждый запрос, по часам
    устройства; отказ подписанному запросу забывает сдвиг часов.
    """
    method, url, body, headers = call["method"], call["url"], call["body"] or None, call["headers"]
    signer = call.get("wsse")
    if signer is not None and body:
        body = wsse.sign(body, signer, await wsse.offset(session, call, signer))
    response = await session.request(
        method, url, data=body, headers=headers, auth=call["basic"], allow_redirects=False, timeout=timeout
    )
    if signer is not None and response.status >= 400:
        wsse.forget(call, signer)
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

async def _http(payload: dict[str, Any], plan: reduce_mod.Plan | None = None) -> dict[str, Any]:
    call = prepare_http(payload)
    try:
        async with await client_for(call) as session:
            timeout = aiohttp.ClientTimeout(total=call["timeout"])
            async with await http_send(session, call, timeout) as response:
                raw = await _read_all(response)
                headers = {str(k): str(v) for k, v in response.headers.items()}
                status = response.status
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise OpError(_reason(err), 502) from err
    # ⚠ Отказ адресата и не-JSON уходят как есть: чужую ошибку толкует вызывающий.
    # Свёртка — в исполнителе: разбор мегабайтов JSON в цикле событий остановил
    # бы весь Home Assistant (у ядра без HA `host` здесь нет — исполнитель цикла).
    if plan is not None and status < 400:
        reduced = await asyncio.get_running_loop().run_in_executor(None, reduce_mod.apply, plan, raw)
        if reduced is not None:
            return {"status": int(status), "headers": headers, "reduced": reduced}
    return _http_answer(status, headers, raw)

_TLS: ssl.SSLContext | None = None

def tls_context() -> ssl.SSLContext:
    """TLS БЕЗ проверки сертификата — один на весь дом (`connect`, сессии, MQTT, проба).

    ⚠ Проверять нечем: сертификаты устройств объекта самоподписанные, а литеральные адреса
    пускает только частная сеть (`resolve_address`; по имени — `verified_tls`). ⚠ `SSLContext(...)`, а не
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

async def _udp(payload: dict[str, Any]) -> dict[str, Any]:
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
