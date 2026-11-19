"""Варианты потока трека Яндекс Музыки — для плеера дома.

Ссылки на файл выдаёт дом: музыкальный токен живёт только у него. Приложение получает
варианты и само выбирает кодек (`core/track-play.ts`). Никаких новых сетевых путей:
используем существующий `tracks_download_info` клиента `yandex_music`.

⚠ Дом не толкует вендорский ответ: он отдаёт варианты как есть (`url`, `codec`,
`bitrate`). Выбор MP3/битрейта и тип `play_media` — в бандле. Дверь и токен остаются
в доме, разбор — снаружи (`docs/plan-thin-integration.md`, `docs/home-gateway.md`).
"""

from __future__ import annotations

import re
from typing import Any

from yandex_music.exceptions import UnauthorizedError, YandexMusicError

from ..core.ops_base import OpError
from .music_pick import pick_stream, stream_format

TRACK_ID = re.compile(r"[0-9]{1,20}")


async def stream(client: Any, request: dict) -> dict:
    """Ответ на действие `stream`: варианты `{url, codec, bitrate}` и прежняя готовая ссылка."""
    track_id = request.get("id")
    if not isinstance(track_id, str) or not TRACK_ID.fullmatch(track_id):
        raise OpError("Неверный адрес трека")
    try:
        infos = await client.tracks_download_info(track_id, get_direct_links=True)
    except UnauthorizedError:
        raise
    except (YandexMusicError, TimeoutError, ValueError, OSError):
        infos = []
    variants = [
        {
            "url": info.direct_link,
            "codec": info.codec,
            "bitrate": info.bitrate_in_kbps or 0,
        }
        for info in infos or []
        if getattr(info, "direct_link", None) and isinstance(getattr(info, "codec", None), str)
    ]
    chosen = pick_stream(variants)
    if chosen is None:
        raise OpError("Яндекс Музыка не отдала поток трека. Попробуйте ещё раз", 503)
    return {
        "infos": variants,
        # ⚠ Совместимость: бандлы до 0.6.34 читают `url`/`format` и с `infos` получают
        # «Дом не отдал ссылку на трек». Убрать отдельным релизом по правилу «расширить,
        # потом сузить» (`docs/plan-bundle-compat.md`).
        "url": chosen["url"],
        "format": stream_format(chosen["codec"]),
    }
