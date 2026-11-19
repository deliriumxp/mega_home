"""Выбор варианта потока и формат `play_media` — без вендорского клиента.

Чистые функции: их зовёт и служба сценария (`music_services.py`), и тесты без
`yandex_music`. Вендорский клиент живёт в `music_stream.py`.
"""

from __future__ import annotations

from typing import Any

# Кодеки, которые понимает бандл (`core/track-play.ts`). FLAC и контейнеры не берём.
PLAYABLE = ("mp3", "mpeg", "aac", "aac-mp4", "he-aac", "he-aac-mp4")


def pick_stream(infos: Any) -> dict | None:
    """MP3, затем AAC, внутри — больший битрейт. То же правило в бандле (`core/track-play.ts`)."""
    usable = [
        info
        for info in infos or []
        if isinstance(info, dict)
        and isinstance(info.get("url"), str)
        and info["url"]
        and isinstance(info.get("codec"), str)
        and info["codec"].lower() in PLAYABLE
    ]
    if not usable:
        return None
    mp3 = [info for info in usable if info["codec"].lower() in ("mp3", "mpeg")]
    aac = [info for info in usable if info["codec"].lower().startswith(("aac", "he-aac"))]
    return max(mp3 or aac or usable, key=lambda info: int(info.get("bitrate") or 0))


def stream_format(codec: Any) -> str:
    """Кодек Яндекса → формат `play_media` (`mp3` или `aac`)."""
    name = codec.lower() if isinstance(codec, str) else ""
    return "aac" if name.startswith(("aac", "he-aac")) else "mp3"
