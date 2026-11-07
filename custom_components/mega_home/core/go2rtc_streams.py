"""Поток своего go2rtc заводится ОДНИМ путём: только пропавший или сменивший источник.

⚠ `PUT /api/streams` всегда подменяет объект потока (`streams.New`), даже с тем же источником: идущий читатель
(запись) остаётся на прежнем, а новый потребитель (живое HD) открывает к камере ещё одно подключение — Tapo на
.90.250 буферизовала при каждом открытии, вызывная панель с малым числом RTSP-сессий переставала отдавать поток.

⚠ Вид адреса в ответе API — не наш контракт. go2rtc из образа HA (`shutil.which`, версию выбирает HA) с 1.9.15
прячет учётку в КАЖДОМ адресе ответа (`pkg/creds/secrets.go`: `://<userinfo>@` → `://***@`), 1.9.14 отдавал как
есть; сравнение «как записано» не сходилось, и каждый перезапуск любого ffmpeg камеры перезаводил все её потоки.
Поэтому главный признак — память этого процесса «имя → источник, который заводили мы»: поток с таким именем ЕСТЬ
— значит, он наш и прежний, в каком бы виде go2rtc ни показывал адрес. Сравнение адреса (маской с обеих сторон) —
только для потока, которого процесс ещё не заводил (перезагрузка записи интеграции при живом go2rtc).
Подмена потока с потребителями пишется предупреждением: следующая смена вида ответа станет видна в логе разработки
одной строкой, а не буферизацией у жильца.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from .const import LOGGER

# Тот же класс символов userinfo, что у go2rtc (RFC 3986: unreserved + sub-delims + `%:`).
_USERINFO = re.compile(r"://[A-Za-z0-9\-._~!$&'()*+,;=%:]+@")

# Имя потока → источник, который заводил этот процесс. Живёт с модулем: перезагрузка записи интеграции его не
# сбрасывает, а перезапуск HA снимает и go2rtc (его дочерний процесс), то есть и сами потоки.
registered: dict[str, str] = {}


def masked(url: str) -> str:
    """Адрес так, как его покажет go2rtc 1.9.15+; адрес 1.9.14 и уже скрытый — к тому же виду."""
    return _USERINFO.sub("://***@", url)


def _producers(listing: Any, name: str) -> list[Any]:
    stream = listing.get(name) if isinstance(listing, dict) else None
    return (stream.get("producers") or []) if isinstance(stream, dict) else []


def _consumers(listing: Any, name: str) -> int:
    stream = listing.get(name) if isinstance(listing, dict) else None
    return len((stream.get("consumers") or []) if isinstance(stream, dict) else [])


def live(listing: Any, name: str, source: str) -> bool:
    """Поток `name` уже заведён с источником `source` — повторный PUT не нужен."""
    producers = _producers(listing, name)
    if not producers:
        return False
    if name in registered:
        return registered[name] == source
    return any(isinstance(p, dict) and masked(str(p.get("url") or "")) == masked(source) for p in producers)


async def ensure(session: Any, api: str, name: str, source: str, listing: Any = None, timeout: Any = None) -> None:
    """Завести поток, если его нет или источник сменился. `listing` — уже прочитанный `GET /api/streams`."""
    extra = {"timeout": timeout} if timeout is not None else {}
    if listing is None:
        async with session.get(f"{api}/api/streams", **extra) as answer:
            listing = (await answer.json(content_type=None) if answer.status < 400 else None) or {}
    if live(listing, name, source):
        registered[name] = source
        return
    readers = _consumers(listing, name)
    if readers:
        # Источник сменился — подмена законна, но читатели остаются на прежнем объекте до своего перезапуска.
        LOGGER.warning("go2rtc: поток %s перезаводится при %s читателях (%s)", name, readers,
                       "сменился источник" if name in registered else "адрес в ответе go2rtc не опознан")
    url = f"{api}/api/streams?name={quote(name, safe='')}&src={quote(source, safe='')}"
    async with session.put(url, **extra) as answer:
        if answer.status >= 400:
            raise RuntimeError(f"go2rtc не принял поток {name} ({answer.status})")
    registered[name] = source
