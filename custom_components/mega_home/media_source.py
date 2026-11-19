"""Поток трека Яндекс Музыки как источник медиа Home Assistant.

Зачем это, если есть короткий адрес дома (`/mega-home/stream/<токен>`). Sonos
играет адрес со схемой `x-rincon-mp3radio` как РАДИО: ни длительности, ни
позиции, ни перемотки. Home Assistant умеет отдать Sonos трек, если адрес пришёл
из `media_source` (`PlayMedia`): тогда он передаёт MIME в метаданных, и плеер
включает трек-формат — с длительностью, позицией и `Seek`. HEOS по тому же id
получает URL и играет его через `play_url`.

`media-source://mega_home/track/<токен>` — короткий id, дом разрешает его в свой
поток. Токен тот же, что у короткого адреса (`providers/music_proxy.py`):
учётка Яндекса наружу не уходит, а ссылка обновляется по запросу.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.media_player import MediaClass, MediaType
from homeassistant.components.media_source import (
    MediaSource,
    MediaSourceItem,
    PlayMedia,
    Unresolvable,
)
from homeassistant.components.media_source.models import BrowseMediaSource

from .core.const import DOMAIN
from .core.ops_base import OpError
from .http_door import _coordinator
from .providers.music_proxy import resolve_for_media_source

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

TRACK = "track/"


async def async_get_media_source(hass: HomeAssistant) -> MediaSource:
    """Платформа `media_source` интеграции: HA зовёт её один раз при загрузке."""
    return MusicTrackSource(hass)


class MusicTrackSource(MediaSource):
    """Треки Яндекс Музыки, которые дом уже умеет отдавать коротким адресом."""

    name = "Mega Home — Яндекс Музыка"

    def __init__(self, hass: HomeAssistant) -> None:
        super().__init__(DOMAIN)
        self.hass = hass

    async def async_resolve_media(self, item: MediaSourceItem) -> PlayMedia:
        coordinator = _coordinator(self.hass)
        links = getattr(coordinator, "music_links", None)
        music = getattr(coordinator, "music", None)
        if links is None or music is None:
            raise Unresolvable("Яндекс Музыка не подключена в этом доме")
        identifier = item.identifier or ""
        token = identifier[len(TRACK) :] if identifier.startswith(TRACK) else identifier
        try:
            ready = await resolve_for_media_source(music, links, token)
        except OpError as err:
            raise Unresolvable(err.message) from err
        if ready is None:
            raise Unresolvable("Ссылка на трек устарела")
        url, content_type = ready
        return PlayMedia(url, content_type)

    async def async_browse_media(self, item: MediaSourceItem) -> BrowseMediaSource:
        """Список не нужен: id выдаёт дом, а не браузер медиатеки."""
        return BrowseMediaSource(
            domain=self.domain,
            identifier=item.identifier or None,
            media_class=MediaClass.APP,
            media_content_type=MediaType.APP,
            title=self.name,
            can_play=False,
            can_expand=False,
        )
