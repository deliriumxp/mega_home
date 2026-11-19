"""Поля каталога Яндекс Музыки, которые использует приложение жильца."""

from __future__ import annotations

import re
from typing import Any

from ..core.ops_base import OpError

PAGE_SIZE = 50
PLAYLIST_ID = re.compile(r"[0-9]{1,20}:[0-9]{1,12}")


def _item(value: Any, kind: str, item_id: Any = None) -> dict | None:
    item_id = item_id if item_id is not None else getattr(value, "id", None)
    title = getattr(value, "title", None) or getattr(value, "name", None)
    if item_id is None or not title:
        return None
    artists = getattr(value, "artists", None) or []
    result = {"id": str(item_id), "title": title, "kind": kind}
    if artists:
        result["artist"] = ", ".join(artist.name for artist in artists if artist.name)
    return result


def _items(values: Any, kind: str) -> list[dict]:
    return [item for value in values if (item := _item(value, kind)) is not None]


async def _search(client: Any, request: dict, page: int) -> dict:
    query, kind = request.get("query"), request.get("kind")
    if not isinstance(query, str) or not query.strip() or len(query) > 160:
        raise OpError("Введите запрос до 160 символов")
    if kind not in ("track", "artist", "album"):
        raise OpError("Выберите треки, исполнителей или альбомы")
    result = await client.search(query.strip(), type_=kind, page=page, nocorrect=False)
    group = getattr(result, f"{kind}s", None)
    return {
        "items": _items(group.results or [], kind) if group else [],
        "more": bool(group and group.per_page and (page + 1) * group.per_page < group.total),
    }


async def _liked(client: Any, page: int) -> dict:
    result = await client.users_likes_tracks()
    tracks = sorted(result.tracks or [], key=lambda track: str(track.timestamp or ""), reverse=True) if result else []
    track_ids = [str(track.track_id) for track in tracks if track.track_id]
    offset = page * PAGE_SIZE
    ids = track_ids[offset:offset + PAGE_SIZE]
    details = await client.tracks(ids) if ids else []
    return {"items": _items(details or [], "track"), "more": offset + PAGE_SIZE < len(track_ids)}


async def _playlists(client: Any, page: int) -> dict:
    own = await client.users_playlists_list() or []
    saved = await client.users_likes_playlists() or []
    items: dict[str, dict] = {}
    for playlist in [*own, *(entry.playlist for entry in saved if entry.playlist)]:
        owner = playlist.owner.uid if playlist.owner else client.account_uid
        key = f"{owner}:{playlist.kind}"
        if PLAYLIST_ID.fullmatch(key) and (item := _item(playlist, "playlist", key)):
            items.setdefault(key, item)
    offset = page * PAGE_SIZE
    return {"items": list(items.values())[offset:offset + PAGE_SIZE], "more": offset + PAGE_SIZE < len(items)}


async def _playlist(client: Any, playlist_id: Any, page: int) -> dict:
    if not isinstance(playlist_id, str) or not PLAYLIST_ID.fullmatch(playlist_id):
        raise OpError("Неверный адрес плейлиста")
    owner, kind = playlist_id.split(":")
    playlist = await client.users_playlists(kind=int(kind), user_id=owner)
    if playlist is None:
        raise OpError("Плейлист не найден", 404)
    tracks = playlist.tracks or []
    offset = page * PAGE_SIZE
    batch = tracks[offset:offset + PAGE_SIZE]
    missing = [str(entry.track_id) for entry in batch if not entry.track and entry.track_id]
    resolved = {str(track.id): track for track in await client.tracks(missing)} if missing else {}
    values = [entry.track or resolved.get(str(entry.track_id).split(":", 1)[0]) for entry in batch]
    return {"items": _items(values, "track"), "more": offset + PAGE_SIZE < len(tracks)}


async def catalog(client: Any, action: str, request: dict) -> dict:
    page = request.get("page", 0)
    if type(page) is not int or not 0 <= page <= 999:
        raise OpError("Неверный номер страницы")
    if action == "search":
        return await _search(client, request, page)
    if action == "liked":
        return await _liked(client, page)
    if action == "playlists":
        return await _playlists(client, page)
    if action == "playlist":
        return await _playlist(client, request.get("id"), page)
    raise OpError("Неизвестное действие каталога", 404)
