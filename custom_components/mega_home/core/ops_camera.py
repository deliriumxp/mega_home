"""Класс КАМЕРА: кадр плитки, сущность камеры и адреса её картинки.

⚠ Кадр — ОДИН путь на всё (`api/camera-frame/{tile}`): за плиткой может стоять
камера Home Assistant или канал видеонаблюдения, и решает это дом, а не
приложение. Плитка — просто картинка, которую иногда надо обновлять.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any
from urllib.parse import quote

from .const import LOGGER
from .ops_base import OpError, find
from .ops_video import _trassir_guid, trassir, video_id
from .source import Cameras


async def camera_frame(
    coordinator: Any, payload: dict[str, Any]
) -> tuple[str, bytes]:
    """Один кадр камеры — постер, пока идут переговоры (`Cameras.snapshot`).

    ⚠ Не операция канала, а обработчик ПУТИ: зовётся и локальной дверью
    (`http.py`), и переносом (`relay_api.py`). Новых именованных операций мы не
    заводим — ровно для этого перенос и сделан.

    ⚠ Отдаёт `(contentType, bytes)`, а не base64: base64 — форма ответа
    `relay_api.handle` (одна форма на картинку и на JSON, см. его докстринг),
    а не этого обработчика. Кодирование — забота двери, которой оно нужно.
    """
    guid = _trassir_guid(coordinator, payload.get("id"))
    if guid:
        # ⚠ Кадр берётся у РЕГИСТРАТОРА, а не у Home Assistant: камеры
        # видеонаблюдения в HA нет вовсе. Живой кадр — это `timestamp=0`.
        gateway = trassir(coordinator)
        client = gateway.client
        if client is None:
            raise OpError("Видеонаблюдение объекта не настроено", HTTPStatus.NOT_FOUND)
        from .trassir_client import TrassirError

        try:
            # ⚠ СУБПОТОК: регистратор сам отдаёт 704×576 и 10 КБ. Прежде брали
            # полный кадр (1920×1128, 398 КБ) и уменьшали его Pillow'ом — то
            # есть делали за регистратор работу, которую он делает лучше и
            # быстрее.
            return "image/jpeg", await client.async_live_frame(guid)
        except TrassirError as err:
            raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err

    return await source_cameras(coordinator).snapshot(camera_entity(coordinator, payload))

def camera_entity(
    coordinator: Any, payload: dict[str, Any]
) -> str:
    """Плитка-камера из конфига → сущность Home Assistant.

    ⚠ Это и есть вся защита от «покажи мне чужую камеру»: сущность берётся не из
    запроса, а из СОСТАВА ЭТОГО дома по id плитки. Пустить сюда `entity_id` из
    запроса значило бы открыть жильцу любую камеру Home Assistant — включая те,
    которых нет в его приложении.
    """
    tile = find(coordinator.data.get("tiles", []), payload.get("id"))
    if tile is None:
        raise OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)
    if tile.get("domain") != "camera":
        raise OpError("Это устройство не камера")
    if not tile.get("entityId"):
        # ⚠ У камеры ВИДЕОНАБЛЮДЕНИЯ сущности Home Assistant нет и не будет —
        # это самостоятельная система, её показывает дом сам. Отказ здесь
        # означал бы «камера не настроена» там, где всё настроено.
        # ⚠ Через `video_id`, а не по полю на месте. Имя поля однажды сменилось
        # (`trassirGuid` → `videoId`), и ЗДЕСЬ оно осталось старым: камера
        # видеонаблюдения объясняла себя чужими словами — «Элемент ещё не
        # отправлен в Home Assistant» вместо «Это камера видеонаблюдения».
        # Читатель имени в доме должен быть ОДИН.
        if video_id(tile):
            raise OpError("Это камера видеонаблюдения", HTTPStatus.CONFLICT)
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — смотреть пока нечего",
            HTTPStatus.NOT_FOUND,
        )
    return tile["entityId"]

def _camera_urls(entity_id: Any, attributes: Any) -> dict[str, str]:
    """Still frame and MJPEG stream - the very paths the HA frontend uses.

    Relative, and signed with the entity's rotating `access_token`. Absolute
    would be wrong twice over: outside the home they are unreachable anyway, and
    inside it the app is served by this integration and shares an origin with
    Home Assistant, so a relative path is exactly right.

    Both are built EXPLICITLY rather than by patching `entity_picture`. The
    manager builds the same shape in smart-home-view.util.ts, and "replace
    camera_proxy with camera_proxy_stream" would drift between the two
    implementations at the first change in Home Assistant.
    """
    token = (attributes or {}).get("access_token")
    if not entity_id or not isinstance(token, str) or not token:
        # A frame without a token will not open, so we do not promise one: a
        # broken image on the tile reads as a broken camera.
        return {"picture": "", "stream": ""}
    query = f"?token={quote(token, safe='')}"
    ident = quote(str(entity_id), safe="")
    return {
        "picture": f"/api/camera_proxy/{ident}{query}",
        "stream": f"/api/camera_proxy_stream/{ident}{query}",
    }

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
