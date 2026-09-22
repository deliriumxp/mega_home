"""One HTTP surface for the app — the same one, wherever the resident stands.

Приложение жильца всегда говорит с API СВОЕГО ДОМА. Дома оно ходит по этим
путям напрямую (`/mega-home/api/...`, `http.py`), а снаружи те же самые запросы
переносит менеджер по живому каналу и приводит сюда. Разница между «дома» и
«снаружи» — только адрес базы; ни одного отдельного маршрута, отдельного
хранилища и отдельного поведения у удалённого доступа нет.

⚠ `dispatch` — ЕДИНСТВЕННЫЙ маршрутизатор путей жильца: его зовут и локальные
двери (`http.py`, `_RoutedView`), и перенос (`handle`). До 0.5.6 у каждой двери
была своя копия проверок (фото, кропы, файлы, кадр, лента, ресурс `connect`) —
и копии расходились: сверка ключа фона на чтении была только снаружи, и
скрытая комната «дома показывает фон, снаружи — 404». Второй копии не заводить.

⚠ Это НЕ «ещё одна операция под фотографии». Именно потому, что канал переносит
HTTP, а не именованные функции, следующая возможность приложения (звук, план
этажа, что угодно) не будет стоить релиза этой интеграции: путь уже есть.

⚠ Список путей ЗАПЕРТ (`docs/plan-thin-gateway.md`, замок 2): дом — транспорт,
процессы и хранилище, новый маршрут — только с доказательством, что это одно
из них.

⚠ Границы — в размере и в наборе методов: дом обязан не дать превратить дверь
в загрузку чего угодно. Отсюда потолки на запрос и ответ и явный список путей:
неизвестный путь — 404, а не «попробуем угадать».
"""

from __future__ import annotations

import base64
import json
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote

from . import ops
from .crops import crop_key_known, crop_keys, crop_value_valid
from .imaging import asset_file, photo_file
from .ops_base import dumps
from .photos import JPEG_MAGIC, MAX_PHOTO_BYTES, photo_key_known, photo_keys

JSON_TYPE = "application/json"
JPEG_TYPE = "image/jpeg"
# Самый большой законный запрос — фотография жильца.
MAX_REQUEST_BYTES = MAX_PHOTO_BYTES
# Ответ крупнее фотографии бывает ровно один — конфиг большого дома, и он
# текстовый. Потолок вдвое выше запроса: он ловит ошибку («отдаём не то»), а не
# ограничивает нормальную работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
IMMUTABLE = "public, max-age=31536000, immutable"

# (статус, тип, тело, cache-control). Тело — байты или ФАЙЛ: локальная дверь
# отдаёт файл потоком (`FileResponse`), перенос читает его в байты.
Reply = tuple[int, str, "bytes | Path", str]

async def handle(coordinator: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Выполнить один перенесённый запрос и вернуть ответ для менеджера.

    Ответ — всегда `{status, contentType, body}`, где тело в base64: одна форма
    на картинку и на JSON. Две формы означали бы две ветки на каждой из трёх
    сторон и вопрос «а это точно текст?» в каждой.
    """
    path, query = _path(payload.get("path"))
    method = str(payload.get("method") or "GET").upper()
    status, content_type, raw, cache = await dispatch(
        coordinator, method, path, _body(payload.get("body")), query
    )
    if isinstance(raw, Path):
        raw = await coordinator.env.run(raw.read_bytes)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ops.OpError("Ответ слишком большой", HTTPStatus.INSUFFICIENT_STORAGE)
    answer: dict[str, Any] = {
        "status": int(status),
        "contentType": content_type,
        "body": base64.b64encode(raw).decode("ascii"),
    }
    if cache:
        answer["cacheControl"] = cache
    return answer

async def dispatch(
    coordinator: Any, method: str, path: str, body: bytes, query: dict[str, str]
) -> Reply:
    """Один запрос жильца по пути `api/...` — одинаково для обеих дверей."""
    if coordinator is None or not coordinator.data:
        raise ops.OpError("Дом ещё не синхронизирован с менеджером", HTTPStatus.SERVICE_UNAVAILABLE)
    if ".." in path:
        raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)
    if len(body) > MAX_REQUEST_BYTES:
        raise ops.OpError("Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    if path == "api/config" and method == "GET":
        return _json(ops.config(coordinator))
    if path == "api/states" and method == "GET":
        return _json(ops.states(coordinator))
    if path == "api/command" and method == "POST":
        return _json(await ops.command(coordinator, _json_body(body)))
    if path == "api/scenario" and method == "POST":
        return _json(await ops.scenario(coordinator, _json_body(body)))
    if path == "api/intercom" and method == "POST":
        # Отбой идущего вызова домофонии: тот же код, что у операции канала.
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
            # `imaging` — дом сам готовит варианты фото (`imaging.py`). Приложение
            # узнаёт это ОТВЕТОМ, а не номером версии.
            return _json({"photos": versions, "imaging": True})
        if rest.startswith("/"):
            return await _photo(coordinator, method, unquote(rest[1:]), body, query)
    if path.startswith("api/crop"):
        rest = path[len("api/crop") :]
        if rest in ("", "s") and method == "GET":
            crops = await coordinator.env.run(coordinator.crops.all, crop_keys(coordinator.data))
            return _json({"crops": crops})
        if rest.startswith("/"):
            return await _crop(coordinator, method, unquote(rest[1:]), body)
    if path.startswith("api/asset/") and method == "GET":
        # Файлы менеджера: ключ, версия и тип — из манифеста в конфиге
        # (`assets.py`), а не из `?v=` адреса — тот лишь метка кэша.
        found = await asset_file(coordinator.env, coordinator, unquote(path[len("api/asset/") :]), query)
        if found is None:
            raise ops.OpError("Файл не найден", HTTPStatus.NOT_FOUND)
        return (HTTPStatus.OK, found[1], found[0], IMMUTABLE)
    if path.startswith("api/camera-frame/") and method == "GET":
        # Кадр камеры источника плитки — часть B, не вендор: снаружи у
        # приложения нет ни одного адреса Home Assistant.
        tile = unquote(path[len("api/camera-frame/") :])
        content_type, frame = await ops.camera_frame(coordinator, {"id": tile})
        return (HTTPStatus.OK, content_type, frame, "private, max-age=3")
    if path == "api/device-events" and method == "GET":
        # Лента устройства из хранилища на диске — часть F плана, не вендор.
        return _json(ops.device_events(coordinator, query))
    if path == "api/event-file" and method == "GET":
        # Вложение к событию (`event_files.py`): кадр гостя и всё, что за ним.
        # Хранилище жильца на объекте — часть F; снаружи — тем же переносом.
        files = getattr(coordinator, "event_files", None)
        found = await coordinator.env.run(files.find, str(query.get("id") or "")) if files else None
        if found is None:
            raise ops.OpError("Вложения нет", HTTPStatus.NOT_FOUND)
        return (HTTPStatus.OK, found[1], found[0], "private, max-age=604800")
    if path == "api/dev-log" and method == "POST":
        # Лог разработки от приложения ВНУТРИ дома (`dev_log.py`): сессии
        # менеджера на странице дома нет, запись уходит каналом дома.
        log = getattr(coordinator, "dev_log", None)
        taken = log.add_app(_json_body(body).get("records")) if log else 0
        return _json({"accepted": taken})
    if path.startswith("icons/") and method == "GET":
        return await _icon(coordinator, unquote(path[len("icons/") :]))
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.NOT_FOUND)

async def _photo(
    coordinator: Any, method: str, key: str, body: bytes, query: dict[str, str]
) -> Reply:
    """Фон, снятый САМИМ ЖИЛЬЦОМ: комната или плитка (`tile:<id>`).

    ⚠ Ключ сверяется с составом только на ЗАПИСИ. Сверка на чтении казалась
    строже, а на деле разводила двери: комната, которую инсталлятор скрыл или
    переименовал, дома продолжала показывать фон, а снаружи отдавала 404.
    Ограничение набора ключей нужно затем, чтобы диск объекта нельзя было
    забить, а прочитать можно только то, что там уже лежит.
    """
    if method == "GET":
        # Вариант по query (`?w=1080&blur=14`, `imaging.py`).
        target = await photo_file(coordinator.env, coordinator, key, query)
        if target is None:
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        # Адрес несёт версию файла (`?v=<mtime>`): сменилось фото — сменился адрес.
        return (HTTPStatus.OK, JPEG_TYPE, target, IMMUTABLE)
    if method == "POST":
        if not photo_key_known(coordinator.data, key):
            raise ops.OpError("Комната или плитка не найдена", HTTPStatus.NOT_FOUND)
        if not body.startswith(JPEG_MAGIC):
            # Приложение всегда пережимает снимок в JPEG само: сюда попадает
            # либо чужой клиент, либо оборванная загрузка.
            raise ops.OpError("Ожидается фотография JPEG", HTTPStatus.BAD_REQUEST)
        version = await coordinator.env.run(coordinator.photos.save, key, body)
        return _json({"accepted": True, "version": version})
    if method == "DELETE":
        if not await coordinator.env.run(coordinator.photos.delete, key):
            raise ops.OpError("Фото не найдено", HTTPStatus.NOT_FOUND)
        return _json({"accepted": True})
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.METHOD_NOT_ALLOWED)

async def _crop(coordinator: Any, method: str, tile: str, body: bytes) -> Reply:
    """Участок кадра камеры, подправленный САМИМ ЖИЛЬЦОМ: записать, снять.

    Кадр из менеджера остаётся ЗАГОТОВКОЙ: снял жилец свою правку — вернулся он.
    Ключ — только камера текущего состава, той же дисциплиной, что у `_photo`.
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
        if not await coordinator.env.run(coordinator.crops.delete, tile):
            raise ops.OpError("Кадр не найден", HTTPStatus.NOT_FOUND)
        return _json({"accepted": True})
    raise ops.OpError("Дом не знает такого запроса", HTTPStatus.METHOD_NOT_ALLOWED)

async def _icon(coordinator: Any, name: str) -> Reply:
    """Иконка сценария из выкачанных домом.

    ⚠ Имя ПРОВЕРЯЕТСЯ, а не подставляется: локально этот путь раздаёт статика
    aiohttp, которая сама не выпускает за каталог, а здесь такого сторожа нет.
    """
    if not name or "/" in name or "\\" in name or ".." in name:
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    target = coordinator.icons_dir / name
    if not await coordinator.env.run(target.is_file):
        raise ops.OpError("Иконка не найдена", HTTPStatus.NOT_FOUND)
    return (HTTPStatus.OK, "image/png", target, IMMUTABLE)

def _json(payload: Any) -> Reply:
    return (HTTPStatus.OK, JSON_TYPE, dumps(payload).encode("utf-8"), "")

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

    ⚠ Query здесь не отбрасывается: «то же самое, что дома» у HTTP-запроса
    включает `?guid=…&before=…`. Пока query выбрасывался, любой маршрут с
    параметрами молча вёл бы себя снаружи иначе — не отказ, а тихо другой ответ.
    """
    raw = str(value or "")
    path = raw.split("?", 1)[0].lstrip("/")
    query = dict(parse_qsl(raw.split("?", 1)[1])) if "?" in raw else {}
    return path, query

def _body(value: Any) -> bytes:
    if not value:
        return b""
    try:
        return base64.b64decode(str(value), validate=True)
    except (ValueError, TypeError) as err:
        raise ops.OpError("Повреждённое тело запроса", HTTPStatus.BAD_REQUEST) from err
