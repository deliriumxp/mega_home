"""Службы Яндекс Музыки для сценариев Home Assistant.

Сценарий исполняет HA, а музыкальный токен живёт в `mega_home` — приложение и сценарий
не могут сходить в Яндекс сами. Поэтому дом отдаёт две службы:

- `mega_home.yandex_music` — те же действия каталога, что у приложения (`search`, `liked`,
  `playlists`, `playlist`, `stream`), с ответом. Сценарий берёт треки/ссылку и сам решает,
  что с ними делать;
- `mega_home.yandex_music_play` — разрешить трек и сразу отдать его плееру через
  `media_player.play_media` (тип — из описания плитки, без «радио»-схемы: трек это файл).

⚠ Новый шаг сценария на этих действиях — правка МЕНЕДЖЕРА, а не релиз интеграции. Службы
заведены один раз и остаются стабильными; управление привязкой (вход/отключение) сюда не
отдаём — оно только у жильца в приложении.
"""

from __future__ import annotations

import json
from typing import Any

from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import HomeAssistantError

from .core.const import DOMAIN, SERVICE_MUSIC, SERVICE_MUSIC_PLAY
from .core.ops_base import OpError

# `homeassistant.const.ATTR_ENTITY_ID` — та же строка; не тянем модуль ради одной константы.
ENTITY = "entity_id"

# Что сценарию можно: чтение каталога и потока. `start/cancel/disconnect` — только приложение.
READ_ACTIONS = frozenset({"search", "liked", "playlists", "playlist", "album", "artist", "stream"})


def async_register_music_services(hass: HomeAssistant) -> None:
    """Зарегистрировать службы один раз на HA (не на запись дома)."""
    hass.services.async_register(
        DOMAIN,
        SERVICE_MUSIC,
        _music_call,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(DOMAIN, SERVICE_MUSIC_PLAY, _music_play)


async def _music_call(call: ServiceCall) -> dict[str, Any]:
    """Ответ на действие каталога — тем же контрактом, что `connect` у приложения."""
    request = _request(call)
    try:
        return await _music(call)(False, json.dumps(request))
    except OpError as error:
        raise HomeAssistantError(str(error)) from error


async def _music_play(call: ServiceCall) -> None:
    """Разрешить трек и запустить его на плеере; тип берётся из описания плитки."""
    track_id = call.data.get("id")
    entity = call.data.get(ENTITY)
    if not isinstance(track_id, str) or not entity:
        raise HomeAssistantError("Нужны id трека и плеер")
    try:
        stream = await _music(call)(False, json.dumps({"action": "stream", "id": track_id}))
    except OpError as error:
        raise HomeAssistantError(str(error)) from error
    url = stream.get("url") if isinstance(stream, dict) else None
    if not isinstance(url, str) or not url:
        raise HomeAssistantError("Яндекс Музыка не отдала ссылку на трек")
    content_type = _content_type(str(call.data.get("media_content_type") or "music"), str(stream.get("format") or "mp3"))
    await call.hass.services.async_call(
        "media_player",
        "play_media",
        {"media_content_id": url, "media_content_type": content_type},
        target={ENTITY: entity},
        blocking=True,
    )


def _request(call: ServiceCall) -> dict[str, Any]:
    request = call.data.get("request")
    if isinstance(request, str):
        try:
            request = json.loads(request)
        except ValueError as error:
            raise HomeAssistantError("request — объект или JSON") from error
    action = request.get("action") if isinstance(request, dict) else None
    if action not in READ_ACTIONS:
        raise HomeAssistantError("Служба читает каталог и поток; вход и отключение — в приложении")
    return request


def _music(call: ServiceCall):
    """Привязка Яндекс Музыки нужного дома; без `entry_id` — единственного загруженного."""
    entries = call.hass.config_entries.async_loaded_entries(DOMAIN)
    entry_id = call.data.get("entry_id")
    if entry_id:
        entries = [entry for entry in entries if entry.entry_id == entry_id]
    if len(entries) != 1:
        raise HomeAssistantError("Укажите entry_id дома с Яндекс Музыкой")
    music = getattr(entries[0].runtime_data, "resident_services", {}).get("yandex-music")
    if music is None:
        raise HomeAssistantError("Яндекс Музыка не подключена в этом доме")
    return music


def _content_type(template: str, format: str) -> str:
    """Поля `play_media`: формат трека подставляется в тип описания плитки.

    ⚠ Схему радио (`x-rincon-mp3radio`) не применяем: она для бесконечного потока, а трек —
    файл; обычный http(s) нужен ему и на Sonos (`core/track-play.ts`).
    """
    mime, ext = ("audio/mpeg", "mp3") if format == "mp3" else ("audio/aac", "aac")
    return template.replace("{mime}", mime).replace("{ext}", ext)
