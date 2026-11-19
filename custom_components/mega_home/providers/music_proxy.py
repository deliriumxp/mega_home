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

import secrets
from dataclasses import dataclass
from time import monotonic
from typing import Any

from ..core.const import URL_PREFIX
from .music_pick import pick_stream, stream_format

# Сколько живёт короткий адрес. Дольше трека с большим запасом: Sonos может
# перезапросить поток после долгой паузы, и новый адрес ему никто не пришлёт.
LINK_TTL_S = 12 * 3600
# Готовая ссылка Яндекса кэшируется на время трека: иначе каждая часть `Range`
# стоила бы нового запроса к API. Истёкшую ссылку заменяет повторное разрешение
# по отказу CDN (403/404), а не таймер.
URL_TTL_S = 10 * 60
# Потолок памяти: старые адреса вытесняются, а не копятся до перезапуска.
MAX_LINKS = 1024


@dataclass(frozen=True)
class _Link:
    track: str
    at: float


@dataclass(frozen=True)
class _Ready:
    url: str
    format: str
    at: float


class MusicStreamLinks:
    """Адреса потока: токен → трек, и разрешённая ссылка Яндекса к нему."""

    def __init__(self, origin: str | None) -> None:
        # `origin` — адрес дома, по которому плеер LAN заберёт поток; `None` —
        # дом не знает своего адреса, и коротких ссылок не будет (прямая, как
        # прежде; для Sonos это по-прежнему отказ).
        self._origin = origin.rstrip("/") if origin else None
        self._links: dict[str, _Link] = {}
        self._ready: dict[str, _Ready] = {}

    def address(self, track: Any) -> str | None:
        """Короткий адрес для трека или `None`, если дом не знает своего адреса."""
        if not self._origin or not isinstance(track, str) or not track:
            return None
        self._expire()
        token = secrets.token_urlsafe(9)
        self._links[token] = _Link(track, monotonic())
        return f"{self._origin}{URL_PREFIX}/stream/{token}"

    def with_address(self, track: Any, answer: dict) -> dict:
        """Ответ дома на действие `stream`: рядом с прямыми ссылками — короткая.

        Ссылку, которую дом уже разрешил для этого ответа, запоминаем сразу: плеер
        придёт за ней через секунды, и второй запрос к Яндексу ему не нужен.
        """
        address = self.address(track)
        if not address:
            return answer
        chosen = pick_stream(answer.get("infos"))
        if chosen is not None:
            token = address.rsplit("/", 1)[1]
            self.remember(token, chosen["url"], stream_format(chosen.get("codec")))
        return {**answer, "proxy": address}

    def track(self, token: str) -> str | None:
        """Трек по токену; неизвестный или просроченный — `None`."""
        self._expire()
        link = self._links.get(token)
        return link.track if link is not None else None

    def resolved(self, token: str) -> tuple[str, str] | None:
        """Готовая ссылка Яндекса и формат — если ещё не истекла."""
        self._expire()
        ready = self._ready.get(token)
        if ready is None or monotonic() - ready.at > URL_TTL_S:
            return None
        return ready.url, ready.format

    def remember(self, token: str, url: str, format: str) -> None:
        self._expire()
        self._ready[token] = _Ready(url, format, monotonic())

    def forget(self, token: str) -> None:
        self._ready.pop(token, None)

    def _expire(self) -> None:
        now = monotonic()
        for token, link in list(self._links.items()):
            if now - link.at > LINK_TTL_S:
                del self._links[token]
                self._ready.pop(token, None)
        for token, ready in list(self._ready.items()):
            if now - ready.at > URL_TTL_S:
                del self._ready[token]
        while len(self._links) >= MAX_LINKS:
            oldest = next(iter(self._links))
            del self._links[oldest]
            self._ready.pop(oldest, None)
