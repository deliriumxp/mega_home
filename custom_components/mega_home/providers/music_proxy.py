"""Короткие адреса потока Яндекс Музыки для плееров с пределом длины адреса.

Зачем это нужно. Плееру нельзя отдать подписанную ссылку Яндекса: у Sonos `mediaUrl`
не длиннее 1024 символов (официальная документация Sonos, `playback objects`), а ссылка
Яндекса на трек — около 1100. Sonos молча обрезает адрес, CDN отвечает 404, и плеер
не играет. Замер obj68 (2026-11-19): `GetMediaInfo.CurrentURI` — 1023 знака, `GET` → 404.
Вторая причина — ссылка Яндекса живёт ограниченное время, а плеер перезапрашивает поток
после паузы и перемотки.

Поэтому дом отдаёт плееру короткий адрес СВОЕГО маршрута, а ссылку Яндекса разрешает в
момент запроса — тем же путём, что и приложение (`providers/music_stream.py`). Это ровно
приём стрим-сервера Music Assistant: короткий адрес на дом, полный — только внутри.

⚠ Адрес — сам себе пропуск (случайный токен): плеер в локальной сети не умеет приносить
учётку, а учётная запись Яндекса живёт только в доме и наружу не выдаётся. Токен живёт
в памяти: после перезапуска дома текущий трек всё равно не продолжается.

⚠ Почему отдельный маршрут, а не конверт `connect`: поток забирает САМО устройство LAN
по адресу, который ему дал HA. `connect` — это дом → устройство, а не устройство → дом.
Медиапорт дома уже назван исключением (`docs/home-gateway.md`, закрытый список, п. 2).
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any

from ..core.const import DOMAIN, URL_PREFIX
from .music_pick import pick_stream, stream_content_type, stream_extension

# Сколько живёт короткий адрес. Сутки: ночная пауза не должна превращать
# возобновление в 404, а токен переиспользуется по треку (см. `address`).
LINK_TTL_S = 24 * 3600
# Готовая ссылка Яндекса кэшируется на время трека: иначе каждая часть `Range`
# стоила бы нового запроса к API. Истёкшую ссылку заменяет повторное разрешение
# по отказу CDN (401/403/404/410), а не таймер.
URL_TTL_S = 10 * 60
# Потолок памяти: старые адреса вытесняются при ВЫДАЧЕ, а не при чтении.
MAX_LINKS = 1024


@dataclass(frozen=True)
class _Link:
    track: str
    at: float
    extension: str = ""


@dataclass(frozen=True)
class _Ready:
    url: str
    content_type: str
    at: float


class MusicStreamLinks:
    """Адреса потока: токен → трек, и разрешённая ссылка Яндекса к нему.

    ⚠ `origin` — строка или функция: адрес дома приезжает конфигом от менеджера
    и может смениться (порт, хост). Функция читает свежий конфиг на каждую выдачу,
    поэтому правка в карточке не ждёт перезапуска Home Assistant.
    """

    def __init__(self, origin: str | Callable[[], str | None] | None) -> None:
        # `None` — дом не знает своего адреса, и коротких ссылок не будет
        # (прямая, как прежде; для плеера с пределом длины это по-прежнему отказ).
        self._origin = origin
        self._links: dict[str, _Link] = {}
        self._ready: dict[str, _Ready] = {}
        # Тот же трек — тот же адрес: иначе непрерывная игра растит таблицу
        # на каждый запуск и вытесняет ещё играющие ссылки.
        self._by_track: dict[str, str] = {}
        # Одно разрешение на токен: пачка `Range` не должна звать Яндекс столько же раз.
        self._locks: dict[str, asyncio.Lock] = {}

    def _base(self) -> str | None:
        value = self._origin() if callable(self._origin) else self._origin
        return value.rstrip("/") if value else None

    def address(self, track: Any, extension: str = "") -> str | None:
        """Короткий адрес для трека или `None`, если дом не знает своего адреса.

        `extension` (`.mp3`/`.aac`/`.m4a`) — подсказка формата для плееров, которые
        определяют его по имени файла; в реестре токен хранится без неё.
        """
        base = self._base()
        if not base or not isinstance(track, str) or not track:
            return None
        self._expire()
        token = self._by_track.get(track)
        if token is not None and token in self._links:
            self._links[token] = _Link(track, monotonic(), extension)
            return f"{base}{URL_PREFIX}/stream/{token}{extension}"
        while len(self._links) >= MAX_LINKS:
            self._drop(next(iter(self._links)))
        token = secrets.token_urlsafe(9)
        while token in self._links:
            token = secrets.token_urlsafe(9)
        self._links[token] = _Link(track, monotonic(), extension)
        self._by_track[track] = token
        return f"{base}{URL_PREFIX}/stream/{token}{extension}"

    def with_address(self, track: Any, answer: dict) -> dict:
        """Ответ дома на действие `stream`: рядом с прямыми ссылками — короткая.

        Ссылку, которую дом уже разрешил для этого ответа, запоминаем сразу: плеер
        придёт за ней через секунды, и второй запрос к Яндексу ему не нужен.
        """
        chosen = pick_stream(answer.get("infos"))
        extension = stream_extension(chosen.get("codec")) if chosen is not None else ""
        address = self.address(track, extension)
        if not address:
            return answer
        if chosen is not None:
            self.remember(address, chosen["url"], stream_content_type(chosen.get("codec")))
        token = self._key(address)
        return {
            **answer,
            "proxy": address,
            # HA сам разберёт этот id: Sonos получит MIME в метаданных и трек-формат с
            # перемоткой, HEOS — URL через `play_url` (media_source платформа дома).
            "mediaSource": f"media-source://{DOMAIN}/track/{token}",
        }

    def public(self, token: str) -> str | None:
        """Короткий адрес, который плеер реально заберёт (с расширением формата)."""
        base = self._base()
        key = self._key(token)
        link = self._links.get(key)
        if not base or link is None:
            return None
        return f"{base}{URL_PREFIX}/stream/{key}{link.extension}"

    def track(self, token: str) -> str | None:
        """Трек по токену; неизвестный или просроченный — `None`."""
        self._expire()
        link = self._links.get(self._key(token))
        return link.track if link is not None else None

    def resolved(self, token: str) -> tuple[str, str] | None:
        """Готовая ссылка Яндекса и её `Content-Type` — если ещё не истекла."""
        self._expire()
        ready = self._ready.get(self._key(token))
        if ready is None or monotonic() - ready.at > URL_TTL_S:
            return None
        return ready.url, ready.content_type

    def remember(self, token: str, url: str, content_type: str) -> None:
        self._expire()
        self._ready[self._key(token)] = _Ready(url, content_type, monotonic())

    def forget(self, token: str) -> None:
        self._ready.pop(self._key(token), None)

    def lock(self, token: str) -> asyncio.Lock:
        """Один на токен: параллельные запросы ждут одно разрешение, а не своё."""
        return self._locks.setdefault(self._key(token), asyncio.Lock())

    @staticmethod
    def _key(token: str) -> str:
        """Токен из адреса или из пути без расширения: `…/stream/<токен>.mp3` → `<токен>`."""
        name = token.rsplit("/", 1)[-1]
        return name.split(".", 1)[0]

    def _drop(self, token: str) -> None:
        link = self._links.pop(token, None)
        self._ready.pop(token, None)
        if link is not None and self._by_track.get(link.track) == token:
            del self._by_track[link.track]
        held = self._locks.get(token)
        if held is not None and not held.locked():
            del self._locks[token]

    def _expire(self) -> None:
        """Только время: потолок применяется в `address`, иначе чтение вытесняло бы живое."""
        now = monotonic()
        for token, link in list(self._links.items()):
            if now - link.at > LINK_TTL_S:
                self._drop(token)
        for token, ready in list(self._ready.items()):
            if now - ready.at > URL_TTL_S:
                del self._ready[token]


async def resolve_for_media_source(
    music: object, links: MusicStreamLinks, token: str
) -> tuple[str, str] | None:
    """Короткий адрес и MIME для `media_source` — без HA-адаптера.

    Возвращает `(url, mime_type)`: HA передаёт MIME плееру, и Sonos включает
    трек-формат (длительность, позиция, `Seek`), а не радио.
    """
    track = links.track(token)
    if track is None:
        return None
    ready = links.resolved(token)
    if ready is not None:
        url = links.public(token)
        return (url, ready[1]) if url else None
    answer = await music.call(False, {"action": "stream", "id": track})  # type: ignore[attr-defined]
    chosen = pick_stream(answer.get("infos") if isinstance(answer, dict) else None)
    if chosen is None:
        return None
    content_type = stream_content_type(chosen.get("codec"))
    links.remember(token, chosen["url"], content_type)
    url = links.public(token)
    return (url, content_type) if url else None

