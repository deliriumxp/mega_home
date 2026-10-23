"""WS-Security UsernameToken (OASIS WSS UsernameToken Profile 1.0, §3.1, `PasswordDigest`).

Зачем в доме. Подпись нужна на КАЖДЫЙ запрос, и слушатель, живущий без менеджера (подписка на
события камеры), без неё не держит устройство, понимающее только этот вход (стандарт камер
допускает его у «legacy devices»; стенд — камера `.90.147`, без подписи `ter:NotAuthorized`).
Это стандартная схема входа, как Digest (`digest.py`), а не вендорский хэш: дом считает её по типу
учётки `wsse`, шаблоны ничего не вычисляют (`templating.py`).

Как у библиотеки SOAP-камер интеграции Home Assistant (её же берёт Frigate):
`PasswordDigest = Base64(SHA1(nonce + created + password))`, `Created` — UTC с `Z` по часам
УСТРОЙСТВА. Сдвиг часов дом узнаёт запросом `clock` описания: путь, тело и поля даты — данные
менеджера, дом только складывает шесть чисел в момент. ⚠ Камера без NTP (сеть камер без
интернета — обычное дело) после перезагрузки живёт в 1970 году, и подпись по нашим часам она
отвергнет: поэтому сдвиг помнится недолго и забывается на первом отказе.

⚠ Слова `nonce`/`sha1` здесь законны и попадают в исключение замка слов
(`tests/test_thin_gateway.py`, рядом с `digest.py`): это стандарт, а не вендорский код.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

import aiohttp

from .templating import parsed, pick

# Сдвиг часов устройства живёт столько; отказ ответа забывает его сразу.
OFFSET_TTL_S = 600.0
# Часы не ответили — подписываем своими, но спрашиваем снова не раньше этого.
RETRY_S = 60.0
CLOCK_TIMEOUT_S = 10.0
_DIGEST = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest"
_BASE64 = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-soap-message-security-1.0#Base64Binary"
_SECEXT = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
_UTILITY = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
_ENVELOPE = re.compile(rb"<((?:[\w.-]+:)?)Envelope[\s>]")
_HEADER = re.compile(rb"<((?:[\w.-]+:)?)Header\s*>")
_BODY = re.compile(rb"<((?:[\w.-]+:)?)Body[\s>/]")
_CLOCK_FIELDS = ("year", "month", "day", "hour", "minute", "second")
# Адрес часов (`host:port`) → (сдвиг, до какого момента монотонных часов верить).
_offsets: dict[str, tuple[float, float]] = {}


@dataclass(frozen=True)
class Credentials:
    """Учётка `wsse` описания: `clock` — запрос часов устройства и пути шести полей даты."""

    user: str
    password: str
    clock: dict[str, Any] | None = field(default=None, compare=False)


def token(user: str, password: str, offset: float, nonce: bytes | None = None, now: float | None = None) -> str:
    """Элемент `Security` с `UsernameToken` на момент `now + offset` (UTC, секунды)."""
    nonce = nonce if nonce is not None else os.urandom(16)
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime((time.time() if now is None else now) + offset))
    digest = base64.b64encode(hashlib.sha1(nonce + created.encode() + password.encode()).digest()).decode()
    return (
        f'<wsse:Security xmlns:wsse="{_SECEXT}" xmlns:wsu="{_UTILITY}"><wsse:UsernameToken>'
        f"<wsse:Username>{escape(user)}</wsse:Username>"
        f'<wsse:Password Type="{_DIGEST}">{digest}</wsse:Password>'
        f'<wsse:Nonce EncodingType="{_BASE64}">{base64.b64encode(nonce).decode()}</wsse:Nonce>'
        f"<wsu:Created>{created}</wsu:Created></wsse:UsernameToken></wsse:Security>"
    )


def sign(body: bytes, credentials: Credentials, offset: float) -> bytes:
    """Вставить подпись в шапку конверта SOAP; шапки нет — завести её перед телом.

    Тело без конверта уходит как есть: подписывать нечего, и отказ устройства скажет об этом сам.
    """
    security = token(credentials.user, credentials.password, offset).encode()
    header = _HEADER.search(body)
    if header:
        return body[: header.end()] + security + body[header.end() :]
    envelope, found = _ENVELOPE.search(body), _BODY.search(body)
    if envelope is None or found is None:
        return body
    prefix = found.group(1)
    return body[: found.start()] + b"<" + prefix + b"Header>" + security + b"</" + prefix + b"Header>" + body[found.start() :]


def _clock_url(call: dict[str, Any], clock: dict[str, Any]) -> str:
    parts = urlsplit(call["url"])
    port = int(clock.get("port") or parts.port or (443 if parts.scheme == "https" else 80))
    return f"{parts.scheme}://{parts.hostname}:{port}{str(clock.get('path') or '/')}"


def offset_of(payload: bytes, clock: dict[str, Any], now: float) -> float | None:
    """Часы устройства из ответа минус наши, секунды; полей нет — None."""
    paths = clock.get("utc") if isinstance(clock.get("utc"), dict) else {}
    data = parsed(payload)
    try:
        parts = [int(pick(data, str(paths.get(name) or ""))) for name in _CLOCK_FIELDS]
        return datetime(*parts, tzinfo=timezone.utc).timestamp() - now
    except (TypeError, ValueError):
        return None


async def offset(session: aiohttp.ClientSession, call: dict[str, Any], credentials: Credentials) -> float:
    """Сдвиг часов устройства для подписи; запроса часов нет — подпись по часам дома."""
    clock = credentials.clock
    if not isinstance(clock, dict) or not clock.get("path"):
        return 0.0
    url = _clock_url(call, clock)
    key = urlsplit(url).netloc
    known = _offsets.get(key)
    if known is not None and time.monotonic() < known[1]:
        return known[0]
    found: float | None = None
    try:
        headers = clock.get("headers") if isinstance(clock.get("headers"), dict) else {}
        async with session.request(
            str(clock.get("method") or "POST").upper(), url, data=str(clock.get("body") or "").encode() or None,
            headers=headers, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=CLOCK_TIMEOUT_S),
        ) as answer:
            found = offset_of(await answer.read(), clock, time.time())
    except (aiohttp.ClientError, asyncio.TimeoutError):
        found = None
    _offsets[key] = (found or 0.0, time.monotonic() + (OFFSET_TTL_S if found is not None else RETRY_S))
    return found or 0.0


def forget(call: dict[str, Any], credentials: Credentials) -> None:
    """Устройство отказало подписанному запросу: часы переспросить на следующем."""
    if isinstance(credentials.clock, dict) and credentials.clock.get("path"):
        _offsets.pop(urlsplit(_clock_url(call, credentials.clock)).netloc, None)
