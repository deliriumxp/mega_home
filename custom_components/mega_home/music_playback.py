"""Музыкальная служба дома: каталог, выходы и очереди доступны через connect."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .core.ops_base import OpError, find
from .providers.media_queue import PlayerQueue
from .providers.music_catalog import queue_ids
from .providers.music_proxy import MusicStreamLinks
from .providers.player_output import MediaItem, Output, native_output, number, stream_output
if TYPE_CHECKING:
    from .providers.yandex_music import MusicService
    from .station_auth import StationAuth

TOPIC = "media/playback"
FAVORITES_TOPIC = "media/favorites"


class MusicPlayback:
    def __init__(self, coordinator: Any, music: MusicService, station: StationAuth, links: MusicStreamLinks) -> None:
        self._home = coordinator
        self._music = music
        self._station = station
        self._links = links
        self._players: dict[str, PlayerQueue] = {}
        self._profiles: dict[str, dict] = {}
        self._changed()

    def _changed(self) -> None:
        self._home.topics.bump(TOPIC)

    def _tiles(self) -> list[dict]:
        return (self._home.data or {}).get("tiles", [])

    def _entity(self, tile: dict | None) -> str | None:
        if tile and tile.get("domain") == "media_player" and isinstance(tile.get("stream"), dict):
            return tile.get("entityId")
        return None

    async def call(self, can_manage: bool, request: dict) -> dict:
        action = request.get("action")
        if action == "playback_state":
            return {"players": self._views()}
        if action == "play_queue":
            await self._play(request)
            return {"accepted": True}
        if action == "queue_control":
            command = request.get("command")
            if command not in ("previous", "next", "stop"):
                raise OpError("Команда очереди не поддерживается")
            entity = self._entity(find(self._tiles(), request.get("player")))
            queue = self._players.get(entity) if entity else None
            return {"handled": await queue.control(command) if queue else False}
        if action == "track_details":
            raise OpError("Неизвестное действие Яндекс Музыки", 404)
        answer = await self._station.call(can_manage, request)
        if action == "set_favorite":
            self._home.topics.bump(FAVORITES_TOPIC)
        if action == "disconnect":
            for queue in self._players.values():
                queue.clear()
        return self._links.with_address(request.get("id"), answer) if action == "stream" else answer

    async def _play(self, request: dict) -> None:
        tile = find(self._tiles(), request.get("player"))
        entity = self._entity(tile)
        if not entity or tile is None:
            raise OpError("Воспроизведение на этом плеере недоступно", 404)
        ids = queue_ids(request.get("ids"))
        start = request.get("index")
        if type(start) is not int or not 0 <= start < len(ids):
            raise OpError("Неверный начальный трек очереди")
        cursor = _cursor(request.get("continuation"))
        profile = tile["stream"]
        source = self._home.source
        queue = self._players.get(entity)
        if queue is not None and self._profiles.get(entity) != profile:
            queue.close()
            queue = None
        if queue is None:
            queue = PlayerQueue(entity, source, self._home.env, self._changed, self._links.unwatch)
            self._players[entity] = queue
            self._profiles[entity] = profile

        async def load() -> tuple[tuple[MediaItem, ...], int]:
            answer = await self._music.call(False, {"action": "track_details", "ids": ids})
            details = {item["id"]: _media(item) for item in answer["items"]}
            if ids[start] not in details:
                raise OpError("Трек Яндекс Музыки недоступен", 404)
            if not any(self._entity(current) == entity and current["stream"] == profile for current in self._tiles()):
                raise OpError("Настройки плеера изменились. Включите трек повторно", 409)
            return tuple(details[id_] for id_ in ids if id_ in details), sum(id_ in details for id_ in ids[:start])

        async def prepare(item: MediaItem, opened: Callable[[], None]) -> Output:
            state = source.get(entity)
            if not state or state.state in ("unknown", "unavailable") or (
                profile.get("needsKnownState") and state.attributes.get("assumed_state") is True
            ):
                raise OpError("Этот плеер сейчас недоступен", 409)
            if output := native_output(item.id, profile):
                return output
            answer = await self._music.call(False, {"action": "stream", "id": item.id})
            return stream_output(
                item, profile, answer,
                lambda id_, stream: self._links.with_address(id_, stream, opened),
            )

        async def more() -> tuple[tuple[MediaItem, ...], bool]:
            answer = await self._music.call(False, cursor)
            cursor["page"] += 1
            items = tuple(_media(item) for item in answer["items"] if item.get("kind") == "track")
            return items, bool(answer.get("more")) and cursor["page"] <= 999

        await queue.replace(load, prepare, more if cursor else None)

    def _views(self) -> dict[str, dict]:
        players = {}
        for tile in self._tiles():
            entity = self._entity(tile)
            if entity and (queue := self._players.get(entity)) and (view := queue.view()):
                players[tile["id"]] = view
        return players

    def config_changed(self) -> None:
        current = {entity: tile["stream"] for tile in self._tiles() if (entity := self._entity(tile))}
        for entity, profile in list(self._profiles.items()):
            if current.get(entity) != profile:
                self._players.pop(entity).close()
                del self._profiles[entity]

    async def play_configured(self, entity_ids: list[str], track: str) -> list[str]:
        """Запустить трек из сценария на известных выходах и вернуть остальные цели."""
        known = {entity: tile["id"] for tile in self._tiles() if (entity := self._entity(tile))}
        await asyncio.gather(*(
            self._play({"player": known[entity], "ids": [track], "index": 0})
            for entity in dict.fromkeys(entity_ids) if entity in known
        ))
        return [entity for entity in entity_ids if entity not in known]

    def close(self) -> None:
        for queue in self._players.values():
            queue.close()
        self._players.clear()
        self._profiles.clear()


def _media(item: dict) -> MediaItem:
    image = item.get("cover") or ""
    if not image.startswith(("https://", "http://")):
        image = ""
    return MediaItem(
        id=item["id"], title=item["title"], artist=item.get("artist") or "", album=item.get("album") or "",
        image=image, duration=(number(item.get("durationMs")) or 0) / 1000,
    )


def _cursor(value: Any) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise OpError("Неверное продолжение каталога")
    action, page = value.get("action"), value.get("page")
    if type(page) is not int or not 1 <= page <= 999:
        raise OpError("Неверная страница продолжения каталога")
    if action in ("liked", "album", "artist", "playlist"):
        return {"action": action, "page": page, "id": value.get("id")}
    if action == "search":
        return {"action": action, "page": page, "query": value.get("query"), "kind": "track"}
    raise OpError("Этот раздел нельзя продолжить в очереди")
