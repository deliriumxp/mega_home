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

⚠ Список путей ЗАПЕРТ (`docs/plan-thin-gateway.md`, замок 2): дом — транспорт,
процессы и хранилище, новый маршрут — только с доказательством, что это одно
из них.

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
from urllib.parse import parse_qsl, unquote

from . import ops
from .host import Host
from .crops import crop_key_known, crop_keys, crop_value_valid
from .imaging import asset_file, photo_file
from .photos import (
    JPEG_MAGIC,
    MAX_PHOTO_BYTES,
    photo_key_known,
    photo_keys,
)

JSON_TYPE = "application/json"
JPEG_TYPE = "image/jpeg"
# Ответ крупнее фотографии бывает ровно один — конфиг большого дома, и он
# текстовый. Потолок вдвое выше запроса: он ловит ошибку («отдаём не то»), а не
# ограничивает нормальную работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
IMMUTABLE = "public, max-age=31536000, immutable"

async def handle(
    coordinator: Any,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Выполнить один перенесённый запрос и вернуть ответ для менеджера.

    Ответ — всегда `{status, contentType, body}`, где тело в base64: одна форма
    на картинку и на JSON. Две формы означали бы две ветки на каждой из трёх
    сторон и вопрос «а это точно текст?» в каждой.
    """
    method = str(payload.get("method") or "GET").upper()
    path, query = _path(payload.get("path"))
    body = _body(payload.get("body"))

    if len(body) > MAX_PHOTO_BYTES:
        raise ops.OpError("Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)

    result = await _dispatch(coordinator, method, path, body, query)
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
    coordinator: Any,
    method: str,
    path: str,
    body: bytes,
    query: dict[str, str] | None = None,
) -> tuple[int, str, bytes, str]:
    """(status, content-type, тело, cache-control) для одного пути."""
    query = query or {}
    if path == "api/config" and method == "GET":
        return _json(ops.config(coordinator))
    if path == "api/states" and method == "GET":
        return _json(ops.states(coordinator))
    if path == "api/command" and method == "POST":
        return _json(await ops.command(coordinator, _json_body(body)))
    if path == "api/scenario" and method == "POST":
        return _json(await ops.scenario(coordinator, _json_body(body)))
    if path == "api/intercom" and method == "POST":
        # Отбой идущего вызова домофонии: тот же код, что у локальной двери
        # (`http.py`) и у операции канала — ответы не должны разъезжаться.
        return _json(await ops.intercom(coordinator, _json_body(body)))
    if path == "api/connect" and method == "POST":
        # Единственный контракт транспорта наружу — тот же код, что у операции
        # канала (`ops.py`, `op == "connect"`): ответы не должны разъезжаться.
        return _json(await ops.connect(_json_body(body)))
    if path == "api/connect" and method == "GET":
        # ⚠ ТОТ ЖЕ путь и тот же контракт, другой способ ПОТРЕБИТЬ ответ: тело
        # адресата уезжает как есть, с его типом, — кадр и файл браузер берёт
        # сам одной ходкой (`docs/home-gateway.md`, «Полнота двери»). Не новый
        # маршрут: замок 2 считает пути, а не методы.
        return await ops.connect_resource(_resource_request(query))
    if path.startswith("api/photo"):
        # `api/photos` (список) и `api/photo/<ключ>` (файл) — ОДИН префикс:
        # два разных `startswith` заперлись бы замком маршрутов как две
        # записи вместо одной (`docs/plan-thin-gateway.md`, замок 2).
        rest = path[len("api/photo") :]
        if rest in ("", "s") and method == "GET":
            versions = await coordinator.env.run(
                coordinator.photos.versions, photo_keys(coordinator.data)
            )
            # `imaging` — тот же флаг, что у локальной двери (`http.py`).
            return _json({"photos": versions, "imaging": True})
        if rest.startswith("/"):
            return await _photo(coordinator, method, unquote(rest[1:]), body, query)
    if path.startswith("api/crop"):
        rest = path[len("api/crop") :]
        if rest in ("", "s") and method == "GET":
            crops = await coordinator.env.run(
                coordinator.crops.all, crop_keys(coordinator.data)
            )
            return _json({"crops": crops})
        if rest.startswith("/"):
            return await _crop(coordinator, method, unquote(rest[1:]), body)
    if path.startswith("api/asset/") and method == "GET":
        return await _asset(coordinator, unquote(path[len("api/asset/") :]), query)
    if path.startswith("api/camera-frame/") and method == "GET":
        return await _camera_frame(coordinator, unquote(path[len("api/camera-frame/") :]))
    if path == "api/device-events" and method == "GET":
        # Лента устройства из хранилища на диске — часть F плана, не вендор
        # (`docs/plan-thin-gateway.md`; замок 2, `tests/test_thin_gateway.py`).
        return _json(ops.device_events(coordinator, query))
    if path.startswith("icons/") and method == "GET":
        return await _icon(coordinator, unquote(path[len("icons/") :]))
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)

async def _photo(
    coordinator: Any,
    method: str,
    key: str,
    body: bytes,
    query: dict[str, str] | None = None,
) -> tuple[int, str, bytes, str]:
    """Фон, снятый САМИМ ЖИЛЬЦОМ: комната или плитка (`tile:<id>`).

    ⚠ Проверки те же, что у локального маршрута (`http.py`), И РОВНО ТАМ ЖЕ:
    ключ сверяется с составом только на ЗАПИСИ. Это не небрежность — это
    единственное правило на обе двери. Сверка на чтении казалась строже, а на
    деле разводила их: комната, которую инсталлятор скрыл в приложении или
    переименовал, дома продолжала показывать свой фон, а снаружи отдавала 404 —
    то самое «дома работает, снаружи нет», ради которого перенос и делался.
    Ограничение набора ключей нужно затем, чтобы диск объекта нельзя было
    забить, а прочитать можно только то, что там уже лежит.
    """
    if method == "GET":
        # Вариант по query — тем же разбором, что дома (`imaging.py`).
        target = await photo_file(coordinator.env, coordinator, key, query or {})
        if target is None:
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        return (HTTPStatus.OK, JPEG_TYPE, await _read(coordinator.env, target), IMMUTABLE)
    if method == "POST":
        if not photo_key_known(coordinator.data, key):
            raise ops.OpError("Комната или плитка не найдена", HTTPStatus.NOT_FOUND)
        if not body.startswith(JPEG_MAGIC):
            raise ops.OpError("Ожидается фотография JPEG", HTTPStatus.BAD_REQUEST)
        version = await coordinator.env.run(coordinator.photos.save, key, body)
        return _json({"accepted": True, "version": version})
    if method == "DELETE":
        removed = await coordinator.env.run(coordinator.photos.delete, key)
        if not removed:
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        return _json({"accepted": True})
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.METHOD_NOT_ALLOWED)

async def _crop(
    coordinator: Any,
    method: str,
    tile: str,
    body: bytes,
) -> tuple[int, str, bytes, str]:
    """Участок кадра камеры, подправленный САМИМ ЖИЛЬЦОМ: записать, снять.

    ⚠ Проверки те же, что у локального маршрута (`http.py`), той же дисциплиной,
    что и у `_photo`: ключ сверяется с составом только на ЗАПИСИ.
    """
    if method == "POST":
        if not crop_key_known(coordinator.data, tile):
            raise ops.OpError("Камера не найдена", HTTPStatus.NOT_FOUND)
        payload = _json_body(body)
        if not crop_value_valid(payload):
            raise ops.OpError("Некорректный участок кадра", HTTPStatus.BAD_REQUEST)
        await coordinator.env.run(coordinator.crops.save, tile, payload)
        return _json({"accepted": True, "crop": payload})
    if method == "DELETE":
        removed = await coordinator.env.run(coordinator.crops.delete, tile)
        if not removed:
            raise ops.OpError("Кадр не найден", HTTPStatus.NOT_FOUND)
        return _json({"accepted": True})
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.METHOD_NOT_ALLOWED)

async def _asset(
    coordinator: Any,
    key: str,
    query: dict[str, str] | None = None,
) -> tuple[int, str, bytes, str]:
    """Любой файл общего канала: тип и версия — из манифеста в конфиге."""
    found = await asset_file(coordinator.env, coordinator, key, query or {})
    if found is None:
        raise ops.OpError("Файл не найден", HTTPStatus.NOT_FOUND)
    target, content_type = found
    return (HTTPStatus.OK, content_type, await _read(coordinator.env, target), IMMUTABLE)

async def _camera_frame(coordinator: Any, tile: str) -> tuple[int, str, bytes, str]:
    """Кадр камеры источника плитки (`ops.camera_frame`) — часть B, не вендор.

    ⚠ Снаружи у приложения нет ни одного адреса Home Assistant
    (`state.picture` недостижим), и без этого пути плитка камеры вне дома
    остаётся без картинки (`docs/plan-thin-gateway.md`).
    """
    content_type, body = await ops.camera_frame(coordinator, {"id": tile})
    return (HTTPStatus.OK, content_type, body, "private, max-age=3")

async def _icon(
    coordinator: Any, name: str
) -> tuple[int, str, bytes, str]:
    """Иконка сценария из выкачанных домом.

    ⚠ Имя ПРОВЕРЯЕТСЯ, а не подставляется: локально этот путь раздаёт статика
    aiohttp, которая сама не выпускает за каталог, а здесь такого сторожа нет.
    """
    if not name or "/" in name or "\\" in name or ".." in name:
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    target = coordinator.icons_dir / name
    if not await coordinator.env.run(target.is_file):
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    return (HTTPStatus.OK, "image/png", await _read(coordinator.env, target), IMMUTABLE)

async def _read(env: Host, target: Path) -> bytes:
    return await env.run(target.read_bytes)

def _json(payload: Any) -> tuple[int, str, bytes, str]:
    return (HTTPStatus.OK, JSON_TYPE, json.dumps(payload).encode("utf-8"), "")

def _resource_request(query: dict[str, str]) -> dict[str, Any]:
    """Описание вызова из параметра `req` — base64url от JSON.

    ⚠ Одним параметром, а не россыпью `host`/`port`/`path`: описание составляет
    БАНДЛ и оно должно доехать без толкования по дороге. Разбирать его на поля
    в адресе значило бы завести второй формат запроса рядом с `ConnectRequest`,
    который разойдётся с ним на первой же правке.
    """
    raw = query.get("req") or ""
    if not raw:
        raise ops.OpError("Нет описания вызова (req)", HTTPStatus.BAD_REQUEST)
    try:
        padded = raw + "=" * (-len(raw) % 4)
        parsed = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, TypeError, UnicodeDecodeError) as err:
        raise ops.OpError("Описание вызова повреждено", HTTPStatus.BAD_REQUEST) from err
    if not isinstance(parsed, dict):
        raise ops.OpError("Описание вызова — объект JSON", HTTPStatus.BAD_REQUEST)
    return parsed

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

def _path(value: Any) -> tuple[str, dict[str, str]]:
    """Путь и РАЗОБРАННЫЙ query запроса, без выхода за пределы своего API.

    ⚠ Query здесь не отбрасывается, и это не украшение. Перенос обещает, что
    приложение снаружи делает то же самое, что дома, — а «то же самое» у
    HTTP-запроса включает `?guid=…&before=…`. Пока query выбрасывался, любой
    будущий маршрут с параметрами молча вёл бы себя снаружи иначе: не отказ, не
    ошибка, а тихо другой ответ. Менеджер их присылает (`resident-proxy`
    передаёт `originalUrl` целиком), терял их только дом.
    """
    raw = str(value or "")
    path = raw.split("?", 1)[0].lstrip("/")
    if ".." in path:
        raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)
    query = dict(parse_qsl(raw.split("?", 1)[1])) if "?" in raw else {}
    return path, query

def _body(value: Any) -> bytes:
    if not value:
        return b""
    try:
        return base64.b64decode(str(value), validate=True)
    except (ValueError, TypeError) as err:
        raise ops.OpError("Повреждённое тело запроса", HTTPStatus.BAD_REQUEST) from err
