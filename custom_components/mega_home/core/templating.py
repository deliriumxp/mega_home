"""Шаблоны описания доступа: подстановка значений и закрытый набор фильтров.

Зачем. Вход в устройство бывает вычислением, а не подстановкой пароля: Dahua и XM
просят `md5(user:realm:pass)` с `realm` из первого ответа, ONVIF (WS-Security
UsernameToken) — `base64(sha1(nonce + created + password))`. Посчитать это может
только тот, кто знает пароль, — то есть дом; бандл пароля не видит. Поэтому
шаблоны ЗДЕСЬ, а сами шаблоны пишет менеджер в описании доступа
(`docs/home-gateway.md`).

Синтаксис:
  `{имя}`                 значение;
  `{имя|ф1|ф2}`           значение через фильтры;
  `{=[a]:[b]:[c]|ф}`      склейка значений и текста, затем фильтры.
Фильтры: `url`, `md5`, `sha1`, `sha256` (дают БАЙТЫ), `base64`, `hex`, `upper`,
`lower`. Хэш-фильтры работают над байтами, чтобы `sha1(nonce + …)` брал СЫРОЙ
nonce, как требует WS-Security. Байты на выходе шаблона — всегда hex.
Живые значения на один рендер: `nonce` (16 случайных байт), `created` (UTC,
ISO 8601 с `Z`), `ts` (секунды эпохи), `tsMs`.

⚠ ОДИН проход: подставленное значение повторно не раскрывается — значение поля
вида `{secret.password}` не превращается в пароль.
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")
_PART = re.compile(r"\[([^\[\]]+)\]")
FILTERS = ("url", "md5", "sha1", "sha256", "base64", "hex", "upper", "lower")


def live_values() -> dict[str, Any]:
    """Значения, которые ОДИН рендер видит одинаковыми во всех местах шаблона."""
    now = datetime.now(timezone.utc)
    return {
        "nonce": os.urandom(16),
        "created": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "ts": str(int(now.timestamp())),
        "tsMs": str(int(now.timestamp() * 1000)),
    }


def _bytes(value: Any) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode("utf-8")


def _text(value: Any) -> str:
    # ⚠ Байты (nonce, хэш) — ВСЕГДА hex, а не «UTF-8, если декодируется»: 16
    # случайных байт раз в десятки тысяч входов складывались в допустимый UTF-8,
    # и вход в устройство не проходил «иногда» (ревью 2026-09-19).
    return value.hex() if isinstance(value, bytes) else str(value)


def _apply(value: Any, name: str) -> Any:
    if name == "md5":
        return hashlib.md5(_bytes(value)).digest()  # noqa: S324 — так требует протокол устройства
    if name == "sha1":
        return hashlib.sha1(_bytes(value)).digest()  # noqa: S324
    if name == "sha256":
        return hashlib.sha256(_bytes(value)).digest()
    if name == "base64":
        return base64.b64encode(_bytes(value)).decode("ascii")
    if name == "hex":
        return _bytes(value).hex()
    if name == "url":
        return quote(_text(value), safe="")
    if name == "upper":
        return _text(value).upper()
    if name == "lower":
        return _text(value).lower()
    raise KeyError(name)


def render(template: str, values: dict[str, Any], live: dict[str, Any] | None = None) -> str:
    """Подставить значения; незнакомое имя или фильтр оставляют место как есть."""
    context = {**(live if live is not None else live_values()), **values}

    def one(match: re.Match[str]) -> str:
        inner = match.group(1)
        if inner in context:
            return _text(context[inner])
        head, *filters = inner.split("|")
        if head.startswith("="):
            missing = [p for p in _PART.findall(head) if p not in context]
            if missing:
                return match.group(0)
            pieces = _PART.split(head[1:])
            # `split` с группой чередует текст и имена: чётные — текст, нечётные — имена.
            value: Any = b"".join(
                _bytes(context[piece]) if index % 2 else piece.encode("utf-8")
                for index, piece in enumerate(pieces)
            )
        elif head in context:
            value = context[head]
        else:
            return match.group(0)
        if any(f not in FILTERS for f in filters):
            return match.group(0)
        for name in filters:
            value = _apply(value, name)
        return _text(value)

    return _PLACEHOLDER.sub(one, template)


def pick(data: Any, path: str) -> str:
    """Поле ответа по пути через точку (`data.token`, `result.session`, `list.0.id`)."""
    current = data
    for step in path.split(".") if path else []:
        if isinstance(current, dict):
            current = current.get(step)
        elif isinstance(current, list) and step.isdigit() and int(step) < len(current):
            current = current[int(step)]
        else:
            return ""
    return str(current) if isinstance(current, (str, int, float)) and not isinstance(current, bool) else ""
