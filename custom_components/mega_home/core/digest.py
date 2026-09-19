"""Digest-аутентификация HTTP по RFC 7616 — свой разбор, без `aiohttp.DigestAuthMiddleware`.

⚠ Зачем свой разбор, а не библиотечный. `DigestAuthMiddleware` есть в `aiohttp`
только с 3.12, а версию `aiohttp` на объекте задаёт Home Assistant, не мы: на
старом HA вызов панели Akuvox (у неё Digest, не Basic) тихо остался бы без
входа. Алгоритм стандартный и небольшой — держать его здесь дешевле, чем
требовать апгрейд платформы ради одного вызывающего.

Поддержаны MD5 и SHA-256, `qop=auth` (единственный, что встречается на
объектах). Вариантов `-sess` нет — устройства их не присылают.

⚠ Слова `md5`/`sha256`/`nonce` здесь законны и попадают в исключение замка 1
(`tests/test_thin_gateway.py`, рядом с `sip_*.py`): это стандарт HTTP, а не
вендорский код.
"""

from __future__ import annotations

import hashlib
import secrets


def parse_www_auth(header: str) -> dict[str, str]:
    """Разобрать заголовок `WWW-Authenticate: Digest ...` в словарь параметров."""
    prefix = "digest "
    if not header.lower().startswith(prefix):
        raise ValueError("не Digest-вызов")
    out: dict[str, str] = {}
    for part in _split_params(header[len(prefix) :]):
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        key = key.strip().lower()
        value = value.strip()
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
        if key:
            out[key] = value
    return out


def _split_params(rest: str) -> list[str]:
    """Разбить по запятой, не трогая запятые внутри кавычек (список qop)."""
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    for char in rest:
        if char == '"':
            quoted = not quoted
            current.append(char)
        elif char == "," and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current))
    return parts


def _digest_hex(algorithm: str, text: str) -> str:
    name = "sha256" if algorithm.upper().replace("-SESS", "") == "SHA-256" else "md5"
    return hashlib.new(name, text.encode("utf-8")).hexdigest()


def authorization(
    method: str,
    uri: str,
    username: str,
    password: str,
    challenge: dict[str, str],
    *,
    cnonce: str | None = None,
    nc: int = 1,
) -> str:
    """Заголовок `Authorization` в ответ на `WWW-Authenticate` устройства."""
    realm = challenge.get("realm", "")
    nonce = challenge.get("nonce", "")
    opaque = challenge.get("opaque")
    algorithm = challenge.get("algorithm", "MD5")
    offered = [q.strip() for q in challenge.get("qop", "").split(",") if q.strip()]
    qop = "auth" if "auth" in offered else (offered[0] if offered else None)
    cnonce = cnonce or secrets.token_hex(16)
    nc_value = f"{nc:08x}"

    ha1 = _digest_hex(algorithm, f"{username}:{realm}:{password}")
    ha2 = _digest_hex(algorithm, f"{method}:{uri}")
    response = (
        _digest_hex(algorithm, f"{ha1}:{nonce}:{nc_value}:{cnonce}:{qop}:{ha2}")
        if qop
        else _digest_hex(algorithm, f"{ha1}:{nonce}:{ha2}")
    )

    parts = [
        f'username="{username}"',
        f'realm="{realm}"',
        f'nonce="{nonce}"',
        f'uri="{uri}"',
        f'response="{response}"',
    ]
    if challenge.get("algorithm"):
        parts.append(f"algorithm={algorithm}")
    if qop:
        parts.append(f"qop={qop}")
        parts.append(f"nc={nc_value}")
        parts.append(f'cnonce="{cnonce}"')
    if opaque:
        parts.append(f'opaque="{opaque}"')
    return "Digest " + ", ".join(parts)
