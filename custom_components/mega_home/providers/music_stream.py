"""Варианты потока трека Яндекс Музыки — для плеера дома.

Ссылки на файл выдаёт дом: музыкальный токен живёт только у него. Приложение получает
варианты и само выбирает кодек (`core/track-play.ts`). Никаких новых сетевых путей:
используем существующий `tracks_download_info` клиента `yandex_music`.

⚠ Дом не толкует вендорский ответ: он отдаёт варианты как есть (`url`, `codec`,
`bitrate`). Выбор MP3/битрейта и тип `play_media` — в бандле. Дверь и токен остаются
в доме, разбор — снаружи (`docs/plan-thin-integration.md`, `docs/home-gateway.md`).
"""

from __future__ import annotations

import asyncio
from typing import Any
from xml.parsers.expat import ExpatError

from yandex_music.exceptions import YandexMusicError

from ..core.const import LOGGER
from ..core.ops_base import OpError
from .music_catalog import TRACK_ID
from .music_pick import pick_stream, stream_format


async def stream(client: Any, request: dict) -> dict:
    """Ответ на действие `stream`: варианты `{url, codec, bitrate}` и прежняя готовая ссылка."""
    track_id = request.get("id")
    match = TRACK_ID.fullmatch(track_id) if isinstance(track_id, str) else None
    if match is None:
        raise OpError("Неверный адрес трека")
    infos = await client.tracks_download_info(match[1])
    # Флаг get_direct_links разрешает варианты по одному и роняет весь трек при
    # отказе одного CDN. Каждый вариант получает ссылку тем же методом SDK.
    links = await asyncio.gather(*(_resolve(info) for info in infos or []))
    variants = [link for link in links if link is not None]
    chosen = pick_stream(variants)
    if chosen is None:
        LOGGER.warning("Яндекс Музыка: действие stream, нет доступных ссылок CDN (вариантов: %d)", len(links))
        raise OpError("Яндекс Музыка не отдала поток трека. Попробуйте ещё раз", 503)
    return {
        "infos": variants,
        # ⚠ Совместимость: бандлы до 0.6.34 читают `url`/`format` и с `infos` получают
        # «Дом не отдал ссылку на трек». Убрать при плановом обновлении по правилу «расширить,
        # потом сузить» (`docs/plan-bundle-compat.md`).
        "url": chosen["url"],
        "format": stream_format(chosen["codec"]),
    }


async def _resolve(info: Any) -> dict | None:
    if not isinstance(info.codec, str) or not info.codec or getattr(info, "preview", False):
        return None
    try:
        url = await info.get_direct_link_async()
    except (YandexMusicError, TimeoutError, ValueError, TypeError, ExpatError, OSError):
        # Отказ CDN касается одной ссылки и не должен обновлять токен Музыки.
        return None
    if not isinstance(url, str) or not url.startswith("https://"):
        return None
    return {"url": url, "codec": info.codec, "bitrate": info.bitrate_in_kbps or 0}
