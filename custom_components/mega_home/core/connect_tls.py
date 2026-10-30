"""TLS вызова `connect`: доверие, отпечаток, клиентский сертификат — полями описания.

Ось «личность» (`docs/home-gateway.md` менеджера, «Полнота двери»). До неё у описания был один
`tls: true`: устройству LAN сертификат не проверялся, имени в интернете — проверялся всегда. Ни
облака с самоподписанным сертификатом, ни устройства, которое пускает только по клиентскому
сертификату (mTLS), описанием было не выразить — первая такая встреча стоила бы релиза дома.

Поля описания (все необязательные, работают только при `tls: true`):
- `tlsInsecure: true` — не проверять сертификат и по имени хоста (устройству LAN не проверяется и так);
- `tlsFingerprint` — SHA-256 сертификата адресата (hex, двоеточия можно): совпал — пускаем, проверка
  цепочки при этом не нужна, это и есть доверие для самоподписанного;
- `tlsClient: {cert, key}` — клиентский сертификат и ключ в PEM.

⚠ Смысла сертификатов дом не знает: что подставить, решает менеджер описанием, как и учётку.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import ssl
import tempfile
from typing import Any

from .ops_base import OpError

# PEM сертификата с цепочкой и ключа — единицы килобайт; больше — не сертификат.
MAX_PEM = 32 * 1024
# Собранных контекстов с клиентским сертификатом: устройств с mTLS на объекте — единицы.
MAX_CLIENT_CONTEXTS = 16

_TLS: ssl.SSLContext | None = None
_VERIFIED: ssl.SSLContext | None = None
_CLIENT: dict[str, ssl.SSLContext] = {}


def tls_context() -> ssl.SSLContext:
    """TLS БЕЗ проверки сертификата — один на весь дом (`connect`, сессии, MQTT, проба).

    ⚠ Проверять нечем: сертификаты устройств объекта самоподписанные, а литеральные адреса
    пускает только частная сеть (`connect.resolve_address`; по имени — `verified_tls`). ⚠ `SSLContext(...)`,
    а не `create_default_context()`: тот грузит системные сертификаты — блокирующий
    вызов в цикле событий (Home Assistant ругается на `load_default_certs`), и
    всё ради проверки, которую мы тут же выключаем.
    """
    global _TLS
    if _TLS is None:
        _TLS = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        _TLS.check_hostname = False
        _TLS.verify_mode = ssl.CERT_NONE
    return _TLS


async def verified_tls() -> ssl.SSLContext:
    """TLS С проверкой сертификата — для сокетов по имени. Системные сертификаты грузятся
    в исполнителе: в цикле событий это блокирующий вызов (`tls_context`)."""
    global _VERIFIED
    if _VERIFIED is None:
        _VERIFIED = await asyncio.get_running_loop().run_in_executor(None, ssl.create_default_context)
    return _VERIFIED


def options(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Опции TLS описания, проверенные по форме; пусто — их нет."""
    payload = payload or {}
    out: dict[str, Any] = {}
    if payload.get("tlsInsecure") is True:
        out["insecure"] = True
    pin = payload.get("tlsFingerprint")
    if pin not in (None, ""):
        digest = re.sub(r"[:\s]", "", str(pin)).lower()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise OpError("tlsFingerprint — SHA-256 сертификата (64 знака hex)")
        out["pin"] = digest
    client = payload.get("tlsClient")
    if client is not None:
        cert = client.get("cert") if isinstance(client, dict) else None
        key = client.get("key") if isinstance(client, dict) else None
        if not isinstance(cert, str) or not isinstance(key, str) or "-----BEGIN" not in cert or "-----BEGIN" not in key:
            raise OpError("tlsClient — {cert, key} в PEM")
        if len(cert) + len(key) > MAX_PEM:
            raise OpError(f"tlsClient больше {MAX_PEM} байт")
        out["client"] = (cert, key)
    return out


def trusts_any(named: bool, opts: dict[str, Any]) -> bool:
    """Цепочку сертификата не проверяем: устройство LAN, «не проверять» или доверие по отпечатку."""
    return not named or bool(opts.get("insecure") or opts.get("pin"))


async def context_for(named: bool, opts: dict[str, Any]) -> ssl.SSLContext:
    """Контекст TLS под адресата и опции описания."""
    if "client" not in opts:
        return tls_context() if trusts_any(named, opts) else await verified_tls()
    cert, key = opts["client"]
    loose = trusts_any(named, opts)
    cache_key = hashlib.sha256(f"{loose}\0{cert}\0{key}".encode()).hexdigest()
    found = _CLIENT.get(cache_key)
    if found is None:
        found = await asyncio.get_running_loop().run_in_executor(None, _client_context, cert, key, loose)
        if len(_CLIENT) >= MAX_CLIENT_CONTEXTS:
            _CLIENT.pop(next(iter(_CLIENT)))
        _CLIENT[cache_key] = found
    return found


def _client_context(cert: str, key: str, loose: bool) -> ssl.SSLContext:
    """Контекст с клиентским сертификатом. `load_cert_chain` берёт только ФАЙЛЫ — PEM ложится во
    временные файлы и тут же стирается (ключ на диске не остаётся)."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if loose else ssl.create_default_context()
    if loose:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    paths: list[str] = []
    try:
        for text in (cert, key):
            handle, path = tempfile.mkstemp(suffix=".pem")
            paths.append(path)
            with os.fdopen(handle, "w", encoding="utf-8") as file:
                file.write(text)
        context.load_cert_chain(paths[0], paths[1])
    except (ssl.SSLError, OSError) as err:
        raise OpError(f"tlsClient не принят: {err}") from err
    finally:
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass
    return context


async def aiohttp_ssl(named: bool, opts: dict[str, Any]) -> Any:
    """Параметр `ssl` соединителя aiohttp (HTTP и WebSocket). Отпечаток сверяет САМ соединитель
    (`aiohttp.Fingerprint`) — до отправки запроса: сверка после ответа отдала бы учётку подменённому
    адресату. ⚠ Отпечаток вместе с клиентским сертификатом соединитель aiohttp не умеет — отказ
    словами; голым сокетам (`tcp`, `mqtt`, сессии) можно и то и другое (`check_pin`)."""
    import aiohttp

    if opts.get("pin") and "client" in opts:
        raise OpError("tlsFingerprint вместе с tlsClient — только у tcp и mqtt; у http и ws — что-то одно")
    if opts.get("pin"):
        return aiohttp.Fingerprint(bytes.fromhex(opts["pin"]))
    if "client" in opts:
        return await context_for(named, opts)
    # Как до опций: имя — проверенный контекст aiohttp, устройство LAN — без проверки.
    return False if trusts_any(named, opts) else True


def check_pin(ssl_object: Any, opts: dict[str, Any]) -> None:
    """Сертификат адресата голого сокета совпал с отпечатком описания — иначе отказ. Зовётся сразу
    после рукопожатия, до первого байта запроса."""
    pin = opts.get("pin")
    if not pin:
        return
    der = ssl_object.getpeercert(binary_form=True) if ssl_object is not None else None
    if not der or hashlib.sha256(der).hexdigest() != pin:
        raise OpError("Сертификат адресата не совпал с tlsFingerprint", 495)
