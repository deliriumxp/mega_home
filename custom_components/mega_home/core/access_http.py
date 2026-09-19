"""HTTP-доступ: исполнение описанного вызова с авторизацией, которую ставит дом.

Виды авторизации — данные описания (`access.AuthSpec`): `none`, `basic`,
`digest`, `bearer`, `session`, плюс заголовки, параметры и обёртка тела
шаблонами описания на каждый вызов. Телефон жильца знает пути, но не пароли.

⚠ Ответ отдаётся КАК ЕСТЬ — разбор живёт в бандле (`gateway.py`).
"""

from __future__ import annotations

import asyncio
import base64
import json
from contextlib import asynccontextmanager
from time import monotonic
from typing import Any, AsyncIterator, Awaitable, Callable

import aiohttp

from .access import AccessDescriptor, RequestSpec
from .access_secrets import SecretBook, template_values
from .const import LOGGER
from .templating import live_values, pick, render

# Потолок ответа. Кадр полного размера — ~390 КБ, конфиг регистратора — сотни
# килобайт; восемь мегабайт ловят ошибку «просим не то», а не ограничивают работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_MARKER_BYTES = 4096
# Заголовки, которые бандл НЕ задаёт: авторизацию и куки ставит дом, адресата
# берёт из описания, остальное — дело соединения.
HEADERS_DENIED = {
    "authorization", "proxy-authorization", "cookie", "host", "connection",
    "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
}
HEADERS_SHOWN = {
    "content-type", "content-disposition", "location", "etag", "last-modified",
    "www-authenticate", "retry-after", "x-total-count",
}

SidProvider = Callable[[bool], Awaitable[str]]


class AccessUnreachable(Exception):
    """Та система не ответила, отказала во входе или порвала соединение."""


class AccessDenied(Exception):
    """Вызов не проходит ПОЛИТИКУ двери — та система тут ни при чём."""


class HttpAccess:
    """HTTP-сторона двери: соединение, сессии и авторизация по описанию."""

    def __init__(
        self, secrets: SecretBook, sid_provider: SidProvider | None = None, provider_access: str | None = None
    ) -> None:
        self._secrets = secrets
        # ⚠ ЖИВАЯ сессия драйвера вендора (Trassir): дверь обязана говорить ТОЙ ЖЕ
        # сессией, что открыла поток — вторая сессия того же пользователя чужой
        # поток не видит (замер стенда 2026-09-13). Привязана к СВОЕМУ доступу;
        # ключ `*` — прежнее поведение без привязки (спеки).
        self._providers: dict[str, SidProvider] = {}
        if sid_provider is not None:
            self._providers[provider_access or "*"] = sid_provider
        self._clients: dict[bool, aiohttp.ClientSession] = {}
        self._sids: dict[str, tuple[str, float]] = {}
        # ⚠ Замок на вход по доступу: N вызовов с протухшей сессией не делают N
        # входов (у регистраторов предел сессий и бан адреса за частый вход).
        self._login_locks: dict[str, asyncio.Lock] = {}
        self._closed = False

    def bind(self, access: str, provider: SidProvider) -> None:
        self._providers[access] = provider

    def reset(self) -> None:
        self._sids.clear()

    # --- вызов ------------------------------------------------------------

    async def request(
        self,
        descriptor: AccessDescriptor,
        method: str,
        path: str,
        params: Any,
        body: bytes | None,
        session: dict[str, str] | None = None,
        headers: Any = None,
    ) -> tuple[int, str, bytes, dict[str, str]]:
        timeout = descriptor.long_poll_timeout if descriptor.is_long_poll(path) else descriptor.timeout
        for attempt in (1, 2):
            async with self._send(descriptor, method, path, params, body, session, headers, attempt == 2, timeout) as response:
                payload = await read_all(response)
                shown = {k.lower(): v for k, v in (getattr(response, "headers", None) or {}).items() if k.lower() in HEADERS_SHOWN}
                status, kind = response.status, response.content_type
            # ⚠ Второй заход — со СВЕЖЕЙ сессией, если система сказала, что прежняя
            # умерла (у Trassir это обычный 200 с телом «no session»).
            if attempt == 1 and self._expired(descriptor, payload):
                LOGGER.debug("Доступ %s не признал сессию — входим заново", descriptor.id)
                self._sids.pop(descriptor.id, None)
                continue
            return status, kind, payload, shown
        raise AccessUnreachable("Система не признала сессию дважды подряд")

    @asynccontextmanager
    async def stream(
        self, descriptor: AccessDescriptor, method: str, path: str, params: Any, body: bytes | None
    ) -> AsyncIterator[Any]:
        """Бесконечный ответ (multipart, chunked, SSE) — для источника событий `stream`."""
        async with self._send(descriptor, method, path, params, body, None, None, False, None) as response:
            yield response

    @asynccontextmanager
    async def _send(
        self,
        descriptor: AccessDescriptor,
        method: str,
        path: str,
        params: Any,
        body: bytes | None,
        session: dict[str, str] | None,
        headers: Any,
        fresh: bool,
        timeout: float | None,
    ) -> AsyncIterator[Any]:
        if params is not None and not isinstance(params, dict):
            raise AccessDenied("Параметры вызова — объект «имя: значение»")
        if headers is not None and not isinstance(headers, dict):
            raise AccessDenied("Заголовки вызова — объект «имя: значение»")
        client = await self._client(descriptor.tls_verify)
        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{path}"
        query, extra, cookies, middlewares, body = await self.authorize(descriptor, path, params, headers, body, fresh)
        query.update(session or {})
        kwargs: dict[str, Any] = {
            "params": query,
            "data": body,
            "headers": extra or None,
            "cookies": cookies or None,
            # ⚠ Без редиректов: устройство увело бы запрос на любой адрес вместе с
            # сессией в query, а бандлу нужен сам ответ с `Location`.
            "allow_redirects": False,
            "timeout": aiohttp.ClientTimeout(total=timeout, sock_connect=descriptor.timeout),
        }
        if middlewares:
            kwargs["middlewares"] = middlewares
        try:
            async with client.request(method, url, **kwargs) as response:
                yield response
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise AccessUnreachable(reason(err)) from err

    async def authorize(
        self,
        descriptor: AccessDescriptor,
        path: str,
        params: Any,
        headers: Any,
        body: bytes | None,
        fresh: bool = False,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str], tuple[Any, ...], bytes | None]:
        """Авторизация по описанию: параметры, заголовки, куки, middleware, тело.

        Одна на HTTP-вызов, поток событий и WebSocket — второй копии вида входа нет.
        """
        query = {str(k): str(v) for k, v in (params or {}).items()}
        extra = {str(k): str(v) for k, v in (headers or {}).items() if str(k).lower() not in HEADERS_DENIED}
        cookies: dict[str, str] = {}
        middlewares: tuple[Any, ...] = ()
        auth = descriptor.auth
        secret = await self._secret(descriptor)
        live = live_values()
        values: dict[str, Any] = template_values(descriptor, secret)
        if auth.type == "session" and auth.session and path.split("?")[0] != auth.session.path:
            sid = await self._sid(descriptor, fresh)
            if sid:
                values["sid"] = sid
                value = render(auth.session.template, values, live)
                _place(auth.session.place, auth.session.name, value, query, extra, cookies)
        elif auth.type == "bearer":
            extra["Authorization"] = f"Bearer {secret.get(auth.token_field, '')}"
        elif auth.type == "basic":
            # Заголовком, а не `aiohttp.BasicAuth`: тот объявлен устаревшим, а замены
            # нет во всех поддерживаемых aiohttp (как в `sip_calls.py`).
            pair = f"{secret.get(auth.user_field, '')}:{secret.get(auth.pass_field, '')}"
            extra["Authorization"] = "Basic " + base64.b64encode(pair.encode()).decode()
        elif auth.type == "digest":
            middlewares = (_digest(secret.get(auth.user_field, ""), secret.get(auth.pass_field, "")),)
        # Сверх вида — шаблоны ОПИСАНИЯ на каждый вызов (свои заголовки ключа,
        # подпись, WS-Security). Бандл их не задаёт и учётки в них не видит.
        for name, template in auth.headers.items():
            extra[name] = render(template, values, live)
        for name, template in auth.query.items():
            query[name] = render(template, values, live)
        if auth.wrap:
            text = (body or b"").decode("utf-8", "replace")
            body = render(auth.wrap, {**values, "body": text}, live).encode("utf-8")
        return query, extra, cookies, middlewares, body

    async def ws(self, descriptor: AccessDescriptor, path: str, params: Any = None, headers: Any = None) -> Any:
        """WebSocket к устройству с той же авторизацией (Digest у WebSocket не бывает)."""
        client = await self._client(descriptor.tls_verify)
        scheme = "wss" if descriptor.scheme in ("https", "wss") or descriptor.tls else "ws"
        query, extra, cookies, _, _ = await self.authorize(descriptor, path, params, headers, None)
        if cookies:
            extra["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            return await client.ws_connect(
                f"{scheme}://{descriptor.host}:{descriptor.port}{path}",
                params=query,
                headers=extra or None,
                heartbeat=30,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise AccessUnreachable(reason(err)) from err

    # --- учётка и сессия --------------------------------------------------

    async def _secret(self, descriptor: AccessDescriptor) -> dict[str, str]:
        if not descriptor.secret and self._secrets.legacy_for(descriptor.id) is None:
            return {}
        if descriptor.auth.type == "none" and not (descriptor.auth.headers or descriptor.auth.query or descriptor.auth.wrap):
            return {}
        try:
            return await self._secrets.get(descriptor.id, descriptor.secret)
        except Exception as err:  # noqa: BLE001 — менеджер недоступен и кэша нет
            raise AccessUnreachable(f"Учётка доступа недоступна: {err}") from err

    def _expired(self, descriptor: AccessDescriptor, payload: bytes) -> bool:
        marker = descriptor.session_expired
        return bool(marker) and marker.encode("utf-8") in payload[:MAX_MARKER_BYTES]

    async def _sid(self, descriptor: AccessDescriptor, fresh: bool = False) -> str:
        spec = descriptor.auth.session
        if spec is None:
            return ""
        provider = self._providers.get(descriptor.id) or (
            self._providers.get("*") if not descriptor.secret else None
        )
        if provider is not None:
            # ⚠ Провайдер — ЧУЖОЙ код (драйвер) и падает своими исключениями;
            # беда регистратора обязана оставаться вердиктом двери, а не 500.
            try:
                sid = await provider(fresh)
            except Exception as err:  # noqa: BLE001
                raise AccessUnreachable(reason(err)) from err
            if sid:
                return str(sid)
        lock = self._login_locks.setdefault(descriptor.id, asyncio.Lock())
        async with lock:
            cached = self._sids.get(descriptor.id)
            if cached and cached[1] > monotonic() and not fresh:
                return cached[0]
            values: dict[str, Any] = template_values(descriptor, await self._secret(descriptor))
            if spec.challenge is not None:
                got = await self._exchange(descriptor, spec.challenge, values)
                values.update({f"challenge.{k}": v for k, v in got.items()})
            got = await self._exchange(descriptor, spec, values, default_field=spec.field)
            sid = got.get("sid", "")
            if not sid:
                # ⚠ Отказ ВХОДА — беда той системы (или учётки), а не политики двери.
                raise AccessUnreachable("Система не пустила дом в сессию")
            self._sids[descriptor.id] = (sid, monotonic() + spec.ttl)
            return sid

    async def _exchange(
        self, descriptor: AccessDescriptor, spec: RequestSpec, values: dict[str, Any], default_field: str = ""
    ) -> dict[str, str]:
        """Запрос, который дом делает сам (вход, шаг до входа), и поля его ответа."""
        live = live_values()
        client = await self._client(descriptor.tls_verify)
        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{spec.path}"
        params = {k: render(v, values, live) for k, v in spec.params.items()}
        kwargs: dict[str, Any] = {
            "timeout": aiohttp.ClientTimeout(total=descriptor.timeout),
            "allow_redirects": False,
        }
        if spec.body:
            kwargs["params"] = params
            kwargs["data"] = render(spec.body, values, live).encode("utf-8")
            if spec.content_type:
                kwargs["headers"] = {"Content-Type": spec.content_type}
        elif spec.method == "GET" or spec.format == "query":
            kwargs["params"] = params
        elif spec.format == "form":
            kwargs["data"] = params
        else:
            kwargs["json"] = params
        try:
            async with (
                client.get(url, **kwargs) if spec.method == "GET" else client.request(spec.method, url, **kwargs)
            ) as response:
                status, raw = response.status, await response.content.read()
                headers = getattr(response, "headers", None) or {}
                cookies = getattr(response, "cookies", None) or {}
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise AccessUnreachable(reason(err)) from err
        if status >= 400:
            raise AccessUnreachable(f"Система отказала во входе (HTTP {status})")
        try:
            data: Any = json.loads(raw.decode("utf-8", "ignore")) if raw else None
        except ValueError:
            data = None
        wanted = dict(spec.fields)
        if default_field and "sid" not in wanted:
            wanted["sid"] = default_field
        out: dict[str, str] = {}
        for name, where in wanted.items():
            if where.startswith("header:"):
                out[name] = str(headers.get(where[7:], ""))
            elif where.startswith("cookie:"):
                cookie = cookies.get(where[7:])
                out[name] = cookie.value if cookie is not None else ""
            elif where == "text":
                out[name] = raw.decode("utf-8", "ignore").strip()
            else:
                out[name] = pick(data, where)
        return out

    async def _client(self, verify: bool) -> aiohttp.ClientSession:
        if self._closed:
            raise AccessUnreachable("Дом перезапускает интеграцию — повторите запрос")
        client = self._clients.get(verify)
        if client is None or client.closed:
            # ⚠ Сертификат устройства в LAN обычно САМОПОДПИСАННЫЙ: доверие держится
            # на том, что адрес взят ИЗ КОНФИГА объекта, а не из запроса приложения
            # (решение заказчика 2026-09-12). Проверку включает описание.
            client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=None if verify else False))
            self._clients[verify] = client
        return client

    async def close(self) -> None:
        self._closed = True
        for client in self._clients.values():
            if not client.closed:
                await client.close()
        self._clients.clear()
        self._sids.clear()


def _place(place: str, name: str, value: str, query: dict[str, str], headers: dict[str, str], cookies: dict[str, str]) -> None:
    if place == "header":
        headers[name] = value
    elif place == "cookie":
        cookies[name] = value
    else:
        query[name] = value


def _digest(user: str, password: str) -> Any:
    """Digest (RFC 7616) средствами `aiohttp` — своего разбора вызова нет."""
    middleware = getattr(aiohttp, "DigestAuthMiddleware", None)
    if middleware is None:
        raise AccessDenied("Digest требует aiohttp 3.12 или новее")
    return middleware(user, password)


async def read_all(response: Any) -> bytes:
    """Тело ЦЕЛИКОМ, но не больше потолка.

    ⚠ `content.read(N)` отдаёт то, что уже в буфере, — на потоковом ответе это
    ПЕРВЫЙ КУСОК (замер 2026-09-13: календарь вернулся двумя байтами `[\\n`).
    Поэтому читаем кусками до конца и проверяем потолок ПО ХОДУ.
    """
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise AccessDenied("Ответ больше потолка двери")
        chunks.append(chunk)
    return b"".join(chunks)


def reason(err: Exception) -> str:
    """Причина отказа словами — БЕЗ адреса запроса.

    ⚠ `str()` ошибок aiohttp несёт URL с query, а там уже подставлены учётка из
    `auth.query` и `sid` сессии; ответ видит локальный контур без аутентификации
    (повторное ревью 2026-09-19). Поэтому — тип, код и причина ОС, но не адрес.
    """
    import re

    status = getattr(err, "status", None)
    if status:
        return f"Система ответила ошибкой (HTTP {status})"
    if isinstance(err, asyncio.TimeoutError):
        return "Система не отвечает: таймаут"
    # Причина словами остаётся («Server disconnected»), адреса — вырезаются.
    text = re.sub(r"\b[a-z][a-z0-9+.-]*://\S+", "<адрес>", str(err)).strip()
    if "url=" in text or "?" in text:
        text = ""
    return f"Система не отвечает: {text}" if text else f"Система не отвечает ({type(err).__name__})"


def field_of(payload: bytes, name: str) -> str:
    """Поле ответа по пути — единственный разбор, который двери позволен."""
    try:
        data = json.loads(payload.decode("utf-8", "ignore")) if payload else None
    except ValueError:
        return ""
    return pick(data, name)
