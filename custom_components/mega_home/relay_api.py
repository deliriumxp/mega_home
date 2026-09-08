"""One HTTP surface for the app — the same one, wherever the resident stands.

Приложение жильца всегда говорит с API СВОЕГО ДОМА. Дома оно ходит по этим
путям напрямую (`/mega-home/api/...`, `http.py`), а снаружи те же самые запросы
переносит менеджер по живому каналу и приводит сюда. Разница между «дома» и
«снаружи» — только адрес базы; ни одного отдельного маршрута, отдельного
хранилища и отдельного поведения у удалённого доступа нет.

⚠ Это НЕ «ещё одна операция под фотографии». Именно потому, что канал переносит
HTTP, а не именованные функции, следующая возможность приложения (звук, план
этажа, что угодно) не будет стоить релиза этой интеграции: путь уже есть.
Прежняя схема с белым списком операций (`config`, `states`, `command`,
`scenario`) означала, что жилец СНАРУЖИ не мог сделать то, что дома делает
одной кнопкой, — например поставить фотографию комнаты: она оседала в браузере
телефона и не доезжала никуда.

⚠ Границы у переноса свои, и они не в путях, а в размере и в наборе методов:
менеджер уже проверил сессию жильца и выбрал объект по ней, дом же обязан не
дать превратить канал в загрузку чего угодно. Отсюда потолки на запрос и ответ
и явный список поддержанных путей: неизвестный путь — 404, а не «попробуем
угадать».
"""

from __future__ import annotations

import base64
import json
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from homeassistant.core import HomeAssistant

from . import ops
from .coordinator import MegaHomeCoordinator
from .photos import (
    JPEG_MAGIC,
    MAX_PHOTO_BYTES,
    photo_key_known,
    photo_keys,
    stock_version,
)

JSON_TYPE = "application/json"
JPEG_TYPE = "image/jpeg"
# Ответ крупнее фотографии бывает ровно один — конфиг большого дома, и он
# текстовый. Потолок вдвое выше запроса: он ловит ошибку («отдаём не то»), а не
# ограничивает нормальную работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
IMMUTABLE = "public, max-age=31536000, immutable"


async def handle(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Выполнить один перенесённый запрос и вернуть ответ для менеджера.

    Ответ — всегда `{status, contentType, body}`, где тело в base64: одна форма
    на картинку и на JSON. Две формы означали бы две ветки на каждой из трёх
    сторон и вопрос «а это точно текст?» в каждой.
    """
    method = str(payload.get("method") or "GET").upper()
    path = _path(payload.get("path"))
    body = _body(payload.get("body"))

    if len(body) > MAX_PHOTO_BYTES:
        raise ops.OpError("Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)

    result = await _dispatch(hass, coordinator, method, path, body)
    if len(result[2]) > MAX_RESPONSE_BYTES:
        raise ops.OpError("Ответ слишком большой", HTTPStatus.INSUFFICIENT_STORAGE)
    status, content_type, raw, cache = result
    answer: dict[str, Any] = {
        "status": int(status),
        "contentType": content_type,
        "body": base64.b64encode(raw).decode("ascii"),
    }
    if cache:
        answer["cacheControl"] = cache
    return answer


async def _dispatch(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator,
    method: str,
    path: str,
    body: bytes,
) -> tuple[int, str, bytes, str]:
    """(status, content-type, тело, cache-control) для одного пути."""
    if path == "api/config" and method == "GET":
        return _json(ops.config(coordinator))
    if path == "api/states" and method == "GET":
        return _json(ops.states(hass, coordinator))
    if path == "api/command" and method == "POST":
        return _json(await ops.command(hass, coordinator, _json_body(body)))
    if path == "api/scenario" and method == "POST":
        return _json(await ops.scenario(hass, coordinator, _json_body(body)))
    if path == "api/photos" and method == "GET":
        versions = await hass.async_add_executor_job(
            coordinator.photos.versions, photo_keys(coordinator.data)
        )
        return _json({"photos": versions})
    if path.startswith("api/photo/"):
        return await _photo(hass, coordinator, method, unquote(path[len("api/photo/") :]), body)
    if path.startswith("api/stock-photo/") and method == "GET":
        return await _stock(hass, coordinator, unquote(path[len("api/stock-photo/") :]))
    if path.startswith("api/asset/") and method == "GET":
        return await _asset(hass, coordinator, unquote(path[len("api/asset/") :]))
    if path.startswith("icons/") and method == "GET":
        return await _icon(hass, coordinator, unquote(path[len("icons/") :]))
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)


async def _photo(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator,
    method: str,
    key: str,
    body: bytes,
) -> tuple[int, str, bytes, str]:
    """Фон, снятый САМИМ ЖИЛЬЦОМ: комната или плитка (`tile:<id>`).

    ⚠ Проверки те же, что у локального маршрута (`http.py`), и это не
    дублирование ради полноты: ключ ограничен составом, формат — JPEG, размер —
    потолком. Без них перенос стал бы дырой, которой нет у той же двери дома.
    """
    if not photo_key_known(coordinator.data, key):
        raise ops.OpError("Комната или плитка не найдена", HTTPStatus.NOT_FOUND)
    target = coordinator.photos.path(key)
    if method == "GET":
        if not await hass.async_add_executor_job(target.is_file):
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        return (HTTPStatus.OK, JPEG_TYPE, await _read(hass, target), IMMUTABLE)
    if method == "POST":
        if not body.startswith(JPEG_MAGIC):
            raise ops.OpError("Ожидается фотография JPEG", HTTPStatus.BAD_REQUEST)
        version = await hass.async_add_executor_job(coordinator.photos.save, key, body)
        return _json({"accepted": True, "version": version})
    if method == "DELETE":
        removed = await hass.async_add_executor_job(coordinator.photos.delete, key)
        if not removed:
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        return _json({"accepted": True})
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.METHOD_NOT_ALLOWED)


async def _stock(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, key: str
) -> tuple[int, str, bytes, str]:
    """Фон, пришедший из менеджера. Версию берём ИЗ КОНФИГА, как и локально."""
    version = stock_version(coordinator.data, key)
    if not version:
        raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
    target = coordinator.stock_photos.path(key, version)
    if not await hass.async_add_executor_job(target.is_file):
        raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
    return (HTTPStatus.OK, JPEG_TYPE, await _read(hass, target), IMMUTABLE)


async def _asset(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, key: str
) -> tuple[int, str, bytes, str]:
    """Любой файл общего канала: тип и версия — из манифеста в конфиге."""
    entry = (coordinator.data.get("assets") or {}).get(key)
    if not isinstance(entry, dict) or not isinstance(entry.get("v"), str):
        raise ops.OpError("Файл не найден", HTTPStatus.NOT_FOUND)
    target = coordinator.assets.path(key, entry["v"])
    if not await hass.async_add_executor_job(target.is_file):
        raise ops.OpError("Файл не найден", HTTPStatus.NOT_FOUND)
    content_type = entry.get("type")
    return (
        HTTPStatus.OK,
        content_type if isinstance(content_type, str) and content_type else "application/octet-stream",
        await _read(hass, target),
        IMMUTABLE,
    )


async def _icon(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, name: str
) -> tuple[int, str, bytes, str]:
    """Иконка сценария из выкачанных домом.

    ⚠ Имя ПРОВЕРЯЕТСЯ, а не подставляется: локально этот путь раздаёт статика
    aiohttp, которая сама не выпускает за каталог, а здесь такого сторожа нет.
    """
    if not name or "/" in name or "\\" in name or ".." in name:
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    target = coordinator.icons_dir / name
    if not await hass.async_add_executor_job(target.is_file):
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    return (HTTPStatus.OK, "image/png", await _read(hass, target), IMMUTABLE)


async def _read(hass: HomeAssistant, target: Path) -> bytes:
    return await hass.async_add_executor_job(target.read_bytes)


def _json(payload: Any) -> tuple[int, str, bytes, str]:
    return (HTTPStatus.OK, JSON_TYPE, json.dumps(payload).encode("utf-8"), "")


def _json_body(body: bytes) -> dict[str, Any]:
    if not body:
        return {}
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as err:
        raise ops.OpError("Ожидается JSON", HTTPStatus.BAD_REQUEST) from err
    if not isinstance(parsed, dict):
        raise ops.OpError("Ожидается объект JSON", HTTPStatus.BAD_REQUEST)
    return parsed


def _path(value: Any) -> str:
    """Путь запроса без ведущего слэша и без выхода за пределы своего API."""
    path = str(value or "").split("?", 1)[0].lstrip("/")
    if ".." in path:
        raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)
    return path


def _body(value: Any) -> bytes:
    if not value:
        return b""
    try:
        return base64.b64decode(str(value), validate=True)
    except (ValueError, TypeError) as err:
        raise ops.OpError("Повреждённое тело запроса", HTTPStatus.BAD_REQUEST) from err

