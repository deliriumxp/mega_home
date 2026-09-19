"""HTTP-доступ: исполнение описанного вызова с авторизацией, которую ставит дом.

Виды авторизации — данные описания (`access.AuthSpec`): `none`, `basic`,
`digest`, `bearer`, `session`. Телефон жильца знает пути, но не пароли.

⚠ Ответ отдаётся КАК ЕСТЬ — разбор живёт в бандле (`gateway.py`).
"""

from __future__ import annotations

import asyncio
import base64
import json
from time import monotonic
from typing import Any, Awaitable, Callable

import aiohttp

from .access import AccessDescriptor, fill
from .access_secrets import SecretBook, template_values
from .const import LOGGER

# Потолок ответа. Кадр полного размера — ~390 КБ, конфиг регистратора — сотни
# килобайт; восемь мегабайт ловят ошибку «просим не то», а не ограничивают работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
# Сколько байт ответа смотрим на маркер протухшей сессии.
MAX_MARKER_BYTES = 4096
# Заголовки, которые бандл НЕ задаёт: авторизацию и куки ставит дом, адресата
# берёт из описания, остальное — дело соединения.
HEADERS_DENIED = {
    "authorization", "proxy-authorization", "cookie", "host", "connection",
    "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
}
# Заголовки ответа, которые уезжают бандлу (прочие — служебные для соединения).
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
        # поток не видит (замер стенда 2026-09-13). Привязана к ОДНОМУ доступу
        # (`provider_access`): второй доступ с сессией получил бы чужую. Без
        # привязки — прежнее поведение: любой доступ без своей учётки.
        self._sid_provider = sid_provider
        self._provider_access = provider_access
        self._clients: dict[bool, aiohttp.ClientSession] = {}
        self._sids: dict[str, tuple[str, float]] = {}

    def reset(self) -> None:
        self._sids.clear()

    async def request(
        self,
        descriptor: AccessDescriptor,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        body: bytes | None,
        session: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes, dict[str, str]]:
        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{path}"
        client = await self._client(descriptor.tls_verify)
        timeout = descriptor.long_poll_timeout if descriptor.is_long_poll(path) else descriptor.timeout
        sent_headers = {
            str(k): str(v) for k, v in (headers or {}).items() if str(k).lower() not in HEADERS_DENIED
        }
        auth = descriptor.auth
        # ⚠ Два захода: второй — со СВЕЖЕЙ сессией, если система сказала, что
        # прежняя умерла (у Trassir это обычный 200 с телом «no session»).
        for attempt in (1, 2):
            query = {str(key): str(value) for key, value in (params or {}).items()}
            cookies: dict[str, str] = {}
            extra = dict(sent_headers)
            middlewares: tuple[Any, ...] = ()
            if auth.type == "session" and auth.session and path.split("?")[0] != auth.session.path:
                sid = await self._sid(descriptor, attempt == 2)
                if sid:
                    _place(auth.session.place, auth.session.name, sid, query, extra, cookies)
            elif auth.type in ("basic", "digest", "bearer"):
                secret = await self._secret(descriptor)
                if auth.type == "bearer":
                    extra["Authorization"] = f"Bearer {secret.get(auth.token_field, '')}"
                elif auth.type == "basic":
                    # Заголовком, а не `aiohttp.BasicAuth`: тот объявлен устаревшим,
                    # а замены нет во всех поддерживаемых aiohttp (как в `sip_calls.py`).
                    pair = f"{secret.get(auth.user_field, '')}:{secret.get(auth.pass_field, '')}"
                    extra["Authorization"] = "Basic " + base64.b64encode(pair.encode()).decode()
                else:
                    middlewares = (_digest(secret.get(auth.user_field, ""), secret.get(auth.pass_field, "")),)
            query.update(session or {})
            try:
                async with client.request(
                    method,
                    url,
                    params=query,
                    data=body,
                    headers=extra or None,
                    cookies=cookies or None,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    **({"middlewares": middlewares} if middlewares else {}),
                ) as response:
                    payload = await read_all(response)
                    shown = {k.lower(): v for k, v in (getattr(response, "headers", None) or {}).items() if k.lower() in HEADERS_SHOWN}
                    status, kind = response.status, response.content_type
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise AccessUnreachable(reason(err)) from err
            if attempt == 1 and self._expired(descriptor, payload):
                LOGGER.debug("Доступ %s не признал сессию — входим заново", descriptor.id)
                self._sids.pop(descriptor.id, None)
                continue
            return status, kind, payload, shown
        raise AccessUnreachable("Система не признала сессию дважды подряд")

    async def token_stream(self, descriptor: AccessDescriptor, camera: str, quality: str) -> str:
        """Адрес потока по токену (прежняя форма Trassir): дом идёт за токеном сам."""
        values = {"camera": camera, "quality": quality}
        params = {key: fill(value, values) for key, value in descriptor.stream_params.items()}
        status, _, payload, _ = await self.request(descriptor, "GET", descriptor.stream_path, params, None)
        token = field_of(payload, descriptor.stream_field)
        if status != 200 or not token:
            raise AccessUnreachable("Система не выдала поток")
        return fill(descriptor.stream_url, {"host": descriptor.host, "rtspPort": str(descriptor.rtsp_port), "token": token})

    async def _secret(self, descriptor: AccessDescriptor) -> dict[str, str]:
        try:
            return await self._secrets.get(descriptor.id, descriptor.secret)
        except Exception as err:  # noqa: BLE001 — нет учётки = отказ, а не 500
            raise AccessDenied(f"Учётка доступа недоступна: {err}") from err

    def _expired(self, descriptor: AccessDescriptor, payload: bytes) -> bool:
        marker = descriptor.session_expired
        return bool(marker) and marker.encode("utf-8") in payload[:MAX_MARKER_BYTES]

    async def _sid(self, descriptor: AccessDescriptor, fresh: bool = False) -> str:
        spec = descriptor.auth.session
        if spec is None:
            return ""
        owned = (
            descriptor.id == self._provider_access
            if self._provider_access is not None
            else not descriptor.secret
        )
        if self._sid_provider is not None and owned:
            # ⚠ Провайдер — ЧУЖОЙ код (драйвер) и падает своими исключениями;
            # беда регистратора обязана оставаться вердиктом двери, а не 500.
            try:
                sid = await self._sid_provider(fresh)
            except Exception as err:  # noqa: BLE001
                raise AccessUnreachable(reason(err)) from err
            if sid:
                return str(sid)
        cached = self._sids.get(descriptor.id)
        if cached and cached[1] > monotonic() and not fresh:
            return cached[0]
        secret = await self._secret(descriptor)
        values = template_values(descriptor, secret)
        params = {key: fill(value, values) for key, value in spec.params.items()}
        client = await self._client(descriptor.tls_verify)
        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{spec.path}"
        timeout = aiohttp.ClientTimeout(total=descriptor.timeout)
        try:
            async with (
                client.get(url, params=params, timeout=timeout)
                if spec.method == "GET"
                else client.post(url, json=params, timeout=timeout)
            ) as response:
                status, payload = response.status, await response.content.read()
                header_sid = (getattr(response, "headers", None) or {}).get(spec.field, "") if spec.place == "header" else ""
                cookie = (getattr(response, "cookies", None) or {}).get(spec.field)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise AccessUnreachable(reason(err)) from err
        sid = field_of(payload, spec.field) or header_sid or (cookie.value if cookie else "")
        if status != 200 or not sid:
            # ⚠ Отказ ВХОДА — беда той системы (или учётки), а не политики двери.
            raise AccessUnreachable("Система не пустила дом в сессию")
        self._sids[descriptor.id] = (sid, monotonic() + spec.ttl)
        return sid

    async def _client(self, verify: bool) -> aiohttp.ClientSession:
        client = self._clients.get(verify)
        if client is None or client.closed:
            # ⚠ Сертификат устройства в LAN обычно САМОПОДПИСАННЫЙ: доверие держится
            # на том, что адрес взят ИЗ КОНФИГА объекта, а не из запроса приложения
            # (решение заказчика 2026-09-12). Проверку включает описание.
            client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=None if verify else False))
            self._clients[verify] = client
        return client

    async def close(self) -> None:
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
    """Причина отказа словами — у сетевых ошибок сообщение часто пустое."""
    text = str(err).strip()
    return f"Система не отвечает: {text}" if text else "Система не отвечает"


def field_of(payload: bytes, name: str) -> str:
    """Поле ответа по имени — единственный разбор, который двери позволен."""
    if not name or not payload:
        return ""
    try:
        data = json.loads(payload.decode("utf-8", "ignore"))
    except ValueError:
        return ""
    if isinstance(data, dict):
        value = data.get(name)
        return str(value) if isinstance(value, (str, int)) else ""
    return ""
