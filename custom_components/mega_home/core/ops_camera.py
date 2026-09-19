"""Камера источника: сущность, адреса её картинки и кадр плитки (`camera.*`
Home Assistant).

⚠ Кадр видеонаблюдения сюда не входит и входить не должен: видео объекта —
дело бандла через `connect` к его go2rtc, а не этого модуля
(`docs/plan-thin-gateway.md`).
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any
from urllib.parse import quote

from .ops_base import OpError, find
from .source import Cameras

async def camera_frame(
    coordinator: Any, payload: dict[str, Any]
) -> tuple[str, bytes]:
    """Один кадр камеры источника плитки (`Cameras.snapshot`).

    ⚠ Не операция канала, а обработчик ПУТИ: зовётся и локальной дверью
    (`http.py`), и переносом (`relay_api.py`) на одном и том же пути
    (`api/camera-frame/<tileId>`) — снаружи у приложения нет ни одного адреса
    Home Assistant, и без переноса кадр плитки там не открылся бы вовсе.

    ⚠ Отдаёт `(contentType, bytes)`, а не base64: кодирование — забота двери,
    которой оно нужно (`relay_api.handle`, одна форма ответа на картинку и на
    JSON, см. его докстринг); локальная дверь (`http.py`) отдаёт эти байты
    браузеру как есть.
    """
    return await source_cameras(coordinator).snapshot(camera_entity(coordinator, payload))

def camera_entity(
    coordinator: Any, payload: dict[str, Any]
) -> str:
    """Плитка-камера из конфига → сущность Home Assistant.

    ⚠ Это и есть вся защита от «покажи мне чужую камеру»: сущность берётся не
    из запроса, а из СОСТАВА ЭТОГО дома по id плитки. Пустить сюда `entity_id`
    из запроса значило бы открыть жильцу любую камеру Home Assistant —
    включая те, которых нет в его приложении.
    """
    tile = find(coordinator.data.get("tiles", []), payload.get("id"))
    if tile is None:
        raise OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)
    if tile.get("domain") != "camera":
        raise OpError("Это устройство не камера")
    if not tile.get("entityId"):
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — смотреть пока нечего",
            HTTPStatus.NOT_FOUND,
        )
    return tile["entityId"]

def _camera_urls(entity_id: Any, attributes: Any, source: str | None = None) -> dict[str, str]:
    """Still frame and MJPEG stream - the very paths the HA frontend uses.

    Relative, and signed with the entity's rotating `access_token`. Absolute
    would be wrong twice over: outside the home they are unreachable anyway, and
    inside it the app is served by this integration and shares an origin with
    Home Assistant, so a relative path is exactly right.

    Both are built EXPLICITLY rather than by patching `entity_picture`. The
    manager builds the same shape in smart-home-view.util.ts, and "replace
    camera_proxy with camera_proxy_stream" would drift between the two
    implementations at the first change in Home Assistant.

    ⚠ `source` — адрес живого потока (обычно RTSP, с учёткой внутри строки),
    который берёт бандл для переговоров со СВОИМ go2rtc (`docs/plan-thin-gateway.md`,
    часть B). Он идёт из КЭША `source.Cameras.cached_source`, а не читается
    здесь: сеть внутри опроса состояний недопустима. Нет адреса в кэше — поля
    нет вовсе, а не пустая строка: приложение должно отличать «ещё не прогрелось»
    от «пусто».
    """
    token = (attributes or {}).get("access_token")
    if not entity_id or not isinstance(token, str) or not token:
        # A frame without a token will not open, so we do not promise one: a
        # broken image on the tile reads as a broken camera.
        return {"picture": "", "stream": ""}
    query = f"?token={quote(token, safe='')}"
    ident = quote(str(entity_id), safe="")
    urls = {
        "picture": f"/api/camera_proxy/{ident}{query}",
        "stream": f"/api/camera_proxy_stream/{ident}{query}",
    }
    if source:
        urls["source"] = source
    return urls

def _warm_cameras(coordinator: Any) -> None:
    """Держать наготове кадр каждой камеры, пока приложение открыто.

    ⚠ Опрос состояний — единственный признак «приложение открыто», который у
    дома есть, и он же лучший момент для подготовки: камеру открывают из сетки
    плиток, то есть через секунду-другую после этого запроса. Сам снимок стоит
    секунду с лишним (ffmpeg у камеры без снапшот-адреса), и добывать его в
    момент открытия — значит показывать пустой прямоугольник ровно столько,
    сколько идут переговоры (жалоба 2026-09-08). Частоту ограничивает сам
    `Cameras.warm`, здесь только перечень камер.
    """
    cameras = getattr(coordinator.source, "cameras", None)
    if cameras is None:
        return
    for tile in coordinator.data.get("tiles", []):
        if tile.get("domain") == "camera" and tile.get("entityId"):
            cameras.warm(tile["entityId"])

def source_cameras(coordinator: Any) -> Cameras:
    """Камеры источника дома (`source.Cameras`) — или отказ, понятный жильцу."""
    cameras = getattr(coordinator.source, "cameras", None)
    if cameras is None:
        raise OpError("Камеры этого дома показать нечем", HTTPStatus.NOT_IMPLEMENTED)
    return cameras
