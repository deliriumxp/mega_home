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
    if uri := _cover_uri(value):
        result["coverUri"] = uri
        # ⚠ Совместимость: бандлы до 0.6.34 читают только готовый `cover` и без него
        # показывают значок прибора. Убрать при плановом обновлении по правилу «расширить, потом
        # сузить», когда ни одному объекту не раздаётся бандл без `coverUri`
        # (`docs/plan-bundle-compat.md`).
        result["cover"] = _cover_url(uri)
    if kind == "track":
        album = (getattr(value, "albums", None) or [None])[0]
        if album is not None:
            if album_title := getattr(album, "title", None):
                result["album"] = album_title
            if album_id := getattr(album, "id", None):
                result["albumId"] = str(album_id)
            if year := getattr(album, "original_release_year", None) or getattr(album, "year", None):
                result["year"] = int(year) if str(year).isdigit() else year
            if genre := getattr(album, "genre", None):
                result["genre"] = genre
        if duration := getattr(value, "duration_ms", None):
            result["durationMs"] = int(duration)
        for source, target in (
            ("explicit", "explicit"),
            ("lyrics_available", "lyricsAvailable"),
            ("content_warning", "contentWarning"),
            ("version", "version"),
        ):
            if (field := getattr(value, source, None)) is not None:
                result[target] = field
    return result


def _cover_uri(value: Any) -> str | None:
    """Сырой `cover_uri` Яндекса; размер вместо `%%` подставляет бандл (`core/music-art.ts`)."""
    uri = getattr(value, "cover_uri", None)
    if not uri:
        albums = getattr(value, "albums", None) or []
        uri = getattr(albums[0], "cover_uri", None) if albums else None
    return uri if isinstance(uri, str) and uri else None


def _cover_url(uri: str) -> str:
    """Готовая обложка 400×400 — прежний вид ответа для бандлов до 0.6.34."""
    value = uri.replace("%%", "400x400")
    return value if value.startswith(("https://", "http://")) else f"https://{value.lstrip('/')}"


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
    details = await _tracks_by_id(client, ids)
    return {"items": _items((details.get(track_id.split(':', 1)[0]) for track_id in ids), "track"), "more": offset + PAGE_SIZE < len(track_ids)}


async def _tracks_by_id(client: Any, ids: list[str]) -> dict[str, Any]:
    tracks = await client.tracks(ids) if ids else []
    return {str(track.id).partition(":")[0]: track for track in tracks or [] if track is not None}


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
    resolved = await _tracks_by_id(client, missing)
    values = [entry.track or resolved.get(str(entry.track_id).split(":", 1)[0]) for entry in batch]
    return {"items": _items(values, "track"), "more": offset + PAGE_SIZE < len(tracks)}


async def _album(client: Any, album_id: Any, page: int) -> dict:
    """Треки альбома — страницами; сценарий может играть альбом целиком."""
    if not isinstance(album_id, str) or not album_id.isdigit():
        raise OpError("Неверный адрес альбома")
    album = await client.albums_with_tracks(album_id)
    if album is None:
        raise OpError("Альбом не найден", 404)
    tracks = [track for volume in album.volumes or [] for track in volume]
    offset = page * PAGE_SIZE
    return {"items": _items(tracks[offset:offset + PAGE_SIZE], "track"), "more": offset + PAGE_SIZE < len(tracks)}


async def _artist(client: Any, artist_id: Any, page: int) -> dict:
    """Треки исполнителя — страницами; предел знает `pager`, без него — полнота страницы."""
    if not isinstance(artist_id, str) or not artist_id.isdigit():
        raise OpError("Неверный адрес исполнителя")
    result = await client.artists_tracks(artist_id, page=page, page_size=PAGE_SIZE)
    if result is None:
        raise OpError("Исполнитель не найден", 404)
    tracks = result.tracks or []
    total = result.pager.total if result.pager else None
    more = (page + 1) * PAGE_SIZE < total if total is not None else len(tracks) >= PAGE_SIZE
    return {"items": _items(tracks, "track"), "more": more}


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
    if action == "album":
        return await _album(client, request.get("id"), page)
    if action == "artist":
        return await _artist(client, request.get("id"), page)
    raise OpError("Неизвестное действие каталога", 404)
