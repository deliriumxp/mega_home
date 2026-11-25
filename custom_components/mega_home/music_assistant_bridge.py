"""Адаптер приложения к зарегистрированным в HA плеерам Music Assistant."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from homeassistant.helpers import entity_registry
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .core.const import LOGGER
from .core.ops_base import OpError, find
from .providers.music_assistant_auth import AUTH_ACTIONS, MusicAssistantAuth
from .providers.music_catalog import QUEUE_LIMIT

DOMAIN = "music_assistant"
PAGE_SIZE = 30
COLLECTIONS = {
    "album": "music/albums/album_tracks",
    "artist": "music/artists/artist_tracks",
    "playlist": "music/playlists/playlist_tracks",
}


class MusicAssistantBridge:
    def __init__(self, hass: Any, tiles: Callable[[], list[dict]], changed: Callable[[], None], admin_token: str = "") -> None:
        self._hass = hass
        self._admin_token = admin_token
        self._tiles = tiles
        self._auth = MusicAssistantAuth()
        self._changed = changed
        self._client: Any = None
        self._unsubscribe: Callable[[], None] | None = None
        self._radio: dict[str, dict] = {}

    def close(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        self._client = None
        self._radio.clear()
        self._auth.reset()

    def _on_event(self, event: Any) -> None:
        if event.event == "queue_updated":
            self._changed()

    def _listen(self, mass: Any) -> None:
        if self._client is mass:
            return
        self.close()
        if mass is not None:
            self._unsubscribe = mass.subscribe(self._on_event)
            self._client = mass

    def _entry(self) -> Any:
        entries = self._hass.config_entries.async_loaded_entries(DOMAIN)
        if not entries:
            return None
        if len(entries) != 1:
            raise OpError("Для этого дома нужен один сервер Music Assistant", 409)
        return entries[0]

    def player_id(self, tile: dict | None) -> str | None:
        entity = tile.get("entityId") if tile else None
        if not isinstance(entity, str) or not entity.startswith("media_player."):
            return None
        entry = entity_registry.async_get(self._hass).async_get(entity)
        if entry is not None and entry.platform == DOMAIN:
            return entry.unique_id
        return None

    async def call(self, can_manage: bool, request: dict) -> dict | None:
        try:
            return await self._dispatch(can_manage, request)
        except OpError:
            raise
        except Exception as error:
            LOGGER.debug("Запрос Music Assistant завершился ошибкой %s", type(error).__name__)
            raise OpError("Music Assistant отклонил запрос или недоступен", 502) from error

    async def _dispatch(self, can_manage: bool, request: dict) -> dict | None:
        action = request.get("action")
        if not isinstance(action, str):
            return None
        if not any((tile.get("stream") or {}).get("externalQueue") for tile in self._tiles()):
            self.close()
            return None
        entry = self._entry()
        mass = entry.runtime_data.mass if entry else None
        self._listen(mass)
        if mass is None:
            raise OpError("Подключение к Music Assistant недоступно", 503)
        if action in AUTH_ACTIONS or action in ("station.status", "station.connect"):
            return await self._account(entry, mass, can_manage, action)
        if action in ("play_queue", "play_station", "queue_control"):
            player = self.player_id(find(self._tiles(), request.get("player")))
            if player is None:
                ids = request.get("ids")
                if action == "play_queue" and isinstance(ids, list) and any(_uri(item, "track") for item in ids):
                    raise OpError("Выберите плеер Music Assistant", 409)
                return None
            return await self._playback(mass, player, request)
        if action == "playback_state":
            return {"players": await self._queues(mass)}
        if action in ("search", "liked", "playlists", *COLLECTIONS, "set_favorite"):
            return await self._catalog(mass, request)
        return None

    async def _account(self, entry: Any, mass: Any, can_manage: bool, action: str) -> dict:
        if action == "status" and (not can_manage or not self._admin_token):
            return await self._auth.call(mass, False, action) | {"canManage": can_manage}
        if action == "station.status":
            return await self._auth.stations(mass, can_manage, False)
        if not can_manage:
            raise OpError("Интеграции подключает жилец дома", 403)
        if not self._admin_token:
            raise OpError("Добавьте токен настройки Music Assistant в параметры Mega Home в HA", 409)
        async with asyncio.timeout(30):
            session = async_get_clientsession(self._hass)
            async with type(mass)(entry.data["url"], session, token=self._admin_token) as admin:
                if action == "station.connect":
                    return await self._auth.stations(admin, can_manage, True)
                return await self._auth.call(admin, can_manage, action)

    async def _playback(self, mass: Any, player: str, request: dict) -> dict:
        action = request["action"]
        if action == "queue_control":
            command = request.get("command")
            data = {}
            if command == "seek":
                position = request.get("value")
                if type(position) not in (int, float) or not 0 <= position <= 86_400:
                    raise OpError("Неверная позиция трека")
                data["position"] = int(position)
            elif command not in ("previous", "next", "stop"):
                raise OpError("Команда очереди не поддерживается")
            queue = await mass.send_command("player_queues/get_active_queue", player_id=player)
            if not queue or not queue.get("active"):
                return {"handled": False}
            await mass.send_command(f"player_queues/{command}", queue_id=queue["queue_id"], **data)
            return {"handled": True}
        if action == "play_queue":
            media = request.get("ids")
            if not isinstance(media, list) or not 1 <= len(media) <= QUEUE_LIMIT:
                raise OpError("Неверный список треков")
            index = request.get("index")
            if type(index) is not int or not 0 <= index < len(media):
                raise OpError("Неверный начальный трек очереди")
            media = [_track_uri(item) for item in media[index:]]
            if any(item is None for item in media):
                raise OpError("Заново откройте каталог Music Assistant")
        else:
            station = request.get("station")
            url = station.get("url") if isinstance(station, dict) else None
            if not isinstance(url, str) or not _radio_url(url):
                raise OpError("Music Assistant не принимает этот адрес радиостанции")
            station_id = station.get("id")
            if not isinstance(station_id, str) or not station_id or len(station_id) > 128:
                raise OpError("Станция без идентификатора")
            media = url
        await mass.send_command("player_queues/play_media", queue_id=player, media=media, option="replace")
        if action == "play_station":
            if len(self._radio) >= 100:
                self._radio.pop(next(iter(self._radio)))
            self._radio[media] = station
        return {"accepted": True}

    async def _queues(self, mass: Any) -> dict:
        tiles = {
            tile["id"]: self._active_queue_id(tile, player)
            for tile in self._tiles() if (player := self.player_id(tile))
        }
        queues = {queue["queue_id"]: queue for queue in await mass.send_command("player_queues/all")}
        result = {}
        for tile_id, player in tiles.items():
            queue = queues.get(player)
            current = queue.get("current_item") if queue else None
            item = current.get("media_item") if current else None
            if not current or not item:
                continue
            track = _item(item)
            station = self._radio.get(item.get("item_id")) if item.get("provider") == "builtin" else None
            if station:
                track["id"] = f"station:{station['id']}"
                track["title"] = station.get("title") or track["title"]
                track["cover"] = station.get("image") or ""
            result[tile_id] = {
                "track": {
                    "id": track["id"], "title": track["title"], "artist": track["artist"],
                    "album": track["album"], "image": track.get("cover", ""),
                    "duration": track["durationMs"] / 1000,
                },
                "started": queue.get("state") in ("playing", "paused"),
                "next": bool(queue.get("next_item")),
                "previous": (queue.get("current_index") or 0) > 0,
                "seek": track["durationMs"] > 0,
            }
        return result

    def _active_queue_id(self, tile: dict, player: str) -> str:
        state = self._hass.states.get(tile["entityId"])
        return (state.attributes.get("active_queue") if state else None) or player

    async def _catalog(self, mass: Any, request: dict) -> dict:
        action = request["action"]
        configs = await self._auth.providers(mass)
        if len(configs) != 1:
            raise OpError("Подключите один аккаунт Яндекс Музыки в Music Assistant", 409)
        provider = configs[0]["instance_id"]
        page = request.get("page", 1)
        if type(page) is not int or not 1 <= page <= 999:
            raise OpError("Неверная страница каталога")
        offset = (page - 1) * PAGE_SIZE
        if action == "search":
            query, kind = request.get("query"), request.get("kind", "track")
            if not isinstance(query, str) or kind not in ("track", "artist", "album"):
                raise OpError("Неверный запрос поиска")
            answer = await mass.send_command(
                "music/search", search_query=query, media_types=[kind],
                providers=[provider], limit=min(offset + PAGE_SIZE + 1, 200),
            )
            rows = answer.get(f"{kind}s", [])[offset:]
        elif action in ("liked", "playlists"):
            media = "tracks" if action == "liked" else "playlists"
            filters = {"favorite": True} if action == "liked" else {}
            rows = await mass.send_command(
                f"music/{media}/library_items", provider=provider,
                offset=offset, limit=PAGE_SIZE + 1, **filters,
            )
        elif action in COLLECTIONS:
            uri = _uri(request.get("id"), action)
            if not uri or uri[0] not in (provider, "yandex_music", "library"):
                raise OpError("Неверный адрес коллекции")
            rows = await mass.send_command(
                COLLECTIONS[action], item_id=uri[1], provider_instance_id_or_domain=uri[0],
            )
            rows = rows[offset:]
        else:
            track_uri, favorite = request.get("id"), request.get("liked")
            if not _uri(track_uri, "track") or type(favorite) is not bool:
                raise OpError("Неверная отметка трека")
            if favorite:
                await mass.send_command("music/favorites/add_item", item=track_uri)
            else:
                track = await mass.send_command("music/item_by_uri", uri=track_uri)
                if track.get("provider") == "library":
                    await mass.send_command(
                        "music/favorites/remove_item", media_type="track", library_item_id=track["item_id"],
                    )
            return {"id": track_uri, "liked": favorite}
        return {"items": [_item(item, provider) for item in rows[:PAGE_SIZE]], "more": len(rows) > PAGE_SIZE}


def _track_uri(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"[0-9]{1,24}(?::[0-9]{1,24})?", value):
        return f"yandex_music://track/{value.split(':', 1)[0]}"
    return value if _uri(value, "track") else None


def _uri(value: object, kind: str) -> tuple[str, str] | None:
    if not isinstance(value, str) or len(value) > 256:
        return None
    provider, separator, rest = value.partition("://")
    media_type, slash, item_id = rest.partition("/")
    if not separator or not slash or media_type != kind:
        return None
    if not re.fullmatch(r"library|yandex_music(?:--[a-zA-Z0-9_-]+)?", provider):
        return None
    if not re.fullmatch(r"[a-zA-Z0-9:_-]+", item_id):
        return None
    return provider, item_id


def _radio_url(value: str) -> bool:
    try:
        url = urlsplit(value)
        return (
            url.scheme in ("http", "https")
            and url.hostname == "radiorecord.hostingradio.ru"
            and not url.username and not url.password and url.port is None
        )
    except ValueError:
        return False


def _item(item: dict, provider: str | None = None) -> dict:
    metadata = item.get("metadata") or {}
    cover = next((
        image["path"] for image in metadata.get("images") or []
        if image.get("remotely_accessible") and image.get("type") == "thumb"
    ), "")
    album = item.get("album") or {}
    kind = item.get("media_type") or "track"
    uri = item.get("uri") or ""
    if kind == "track":
        for mapping in item.get("provider_mappings") or []:
            instance = mapping.get("provider_instance")
            if mapping.get("provider_domain") == "yandex_music" and instance and (provider is None or provider == instance):
                uri = f"{instance}://track/{mapping['item_id']}"
                break
    return {
        "id": uri,
        "kind": kind,
        "title": item.get("name") or "",
        "artist": ", ".join(artist["name"] for artist in item.get("artists") or []),
        "album": album.get("name") or "",
        "albumId": album.get("uri"),
        "durationMs": (item.get("duration") or 0) * 1000,
        "cover": cover,
    }
