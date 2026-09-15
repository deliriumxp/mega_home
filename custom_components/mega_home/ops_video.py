"""Класс ВИДЕОНАБЛЮДЕНИЕ: канал за плиткой, лента, запись, кадры архива.

⚠ Имя вендора внутри этого модуля законно: модуль и ЕСТЬ драйвер вендора.
Запрещено оно в общей части — в паспорте объекта, в проекции сущностей и в
маршрутах двери (там классы: `api/video/*`). Второй регистратор приходит сюда
новым модулем, а не правкой базы (`CLAUDE.md`, «Каждая интеграция — МОДУЛЬ»).

⚠ Дом ИСПОЛНЯЕТ описанные бандлом вызовы и отдаёт ответ как есть: разбор дат,
шкал и суток живёт в бандле (`tests/test_thin_gateway.py` это стережёт).
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from homeassistant.core import HomeAssistant

from .const import LOGGER
from .coordinator import MegaHomeCoordinator
from .ops_base import OpError, _int, find, number


def trassir(coordinator: MegaHomeCoordinator) -> Any:
    """Шлюз к регистратору объекта — или понятный отказ, если его нет."""
    gateway = getattr(coordinator, "trassir", None)
    if gateway is None or not gateway.configured:
        raise OpError(
            "У этого объекта не настроено видеонаблюдение", HTTPStatus.NOT_FOUND
        )
    return gateway

# --- TRASSIR: лента событий объекта ---
#
# ⚠ Обработчики ПУТЕЙ, а не именованные операции канала: их зовут обе двери —
# локальная (`http.py`) и перенос запроса снаружи (`relay_api.py`). Ровно ради
# этого перенос и заведён, и заводить под видеонаблюдение свою операцию значило
# бы строить вторую трубу (docs/trassir-integration-plan.md §3 у менеджера).


def video_id(tile: dict[str, Any] | None) -> str | None:
    """Камера ВИДЕОНАБЛЮДЕНИЯ у плитки — или None, если она не за ним.

    ⚠ ЕДИНСТВЕННЫЙ читатель этого имени в доме, и это не педантизм. Поле
    называется `videoId`, а не именем вендора: плитке всё равно, что за ней
    стоит — это просто картинка, которую надо иногда обновлять, — и следующий
    регистратор не должен требовать правок ни в плитке, ни в приложении
    (решение заказчика 2026-09-13: «универсальное решение всегда и никак иначе»).

    ⚠ Читателей было ТРИ, и два из них остались на старом имени: доступность
    плитки (`entity_view`) и объяснение «это камера видеонаблюдения»
    (`camera_entity`). Пока менеджер шлёт оба имени, это незаметно; как только
    старое уйдёт — камера видеонаблюдения станет «недоступной» с надписью «Нет
    данных» поверх работающей картинки. Отсюда правило: имя читает ОДНА функция.

    ⚠ Старое имя (`trassirGuid`) СНЯТО 2026-09-14, вместе со сломом маршрутов:
    менеджер шлёт нейтральное с 0.2.56, а окно слома совместимости открыто
    одно, и тащить замену рядом с заменяемым дальше незачем
    (`docs/plan-video-rework.md`, этап 1).
    """
    if not tile:
        return None
    guid = tile.get("videoId")
    return guid if isinstance(guid, str) and guid else None

def _trassir_guid(coordinator: MegaHomeCoordinator, tile_id: Any) -> str | None:
    """То же самое, но по id плитки: искать её в составе дома нужно почти всем."""
    return video_id(find((coordinator.data or {}).get("tiles", []), tile_id))

async def trassir_cameras(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator
) -> dict[str, Any]:
    """Камеры регистратора: id, имя, кодек, архив и ПЛИТКА дома, если она есть."""
    return {"cameras": await trassir(coordinator).async_cameras(await _tiles_by_guid(hass, coordinator))}

async def _tiles_by_guid(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator
) -> dict[str, str]:
    """Плитки-камеры дома, разложенные по guid канала Trassir.

    ⚠ Опознаём по АДРЕСУ ПОТОКА камеры, а не по имени: постоянная ссылка
    Trassir несёт guid прямо в пути (`rtsp://host:555/<guid>_m/`). Имена правят
    с обеих сторон, и совпадение по ним однажды подсунуло бы жильцу записи
    ЧУЖОЙ камеры — это хуже, чем отсутствие связи вовсе.

    Ошибка одной камеры не роняет список: у дома их несколько, и молчать обо
    всех из-за одной нельзя.
    """
    found: dict[str, str] = {}
    for tile in (coordinator.data or {}).get("tiles", []):
        if tile.get("domain") != "camera" or not tile.get("entityId"):
            continue
        try:
            from . import webrtc

            camera = webrtc._camera(hass, tile["entityId"])  # noqa: SLF001
            source = await camera.stream_source()
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("Адрес потока камеры %s не прочитан: %s", tile.get("id"), err)
            continue
        guid = _guid_of(source)
        if guid:
            found[guid] = str(tile.get("id"))
    return found

def _guid_of(source: str | None) -> str | None:
    """`rtsp://host:555/<guid>_m/` → guid. Не наш адрес — None."""
    if not source:
        return None
    import re

    match = re.search(r"/([A-Za-z0-9]{6,})_(?:m|s)/?$", source.split("?")[0])
    return match.group(1) if match else None

def trassir_events(
    coordinator: MegaHomeCoordinator, query: dict[str, Any]
) -> dict[str, Any]:
    """Лента событий, новые сверху; можно по одной камере и постранично.

    ⚠ `timestampUs` уходит наружу КАК ЕСТЬ — в шкале самого Trassir. Приложение
    показывает время из него же и возвращает его обратно, открывая запись; наши
    часы в этой цепочке не участвуют вовсе, и это единственный способ не
    промахнуться на часовой пояс сервера.
    """
    gateway = trassir(coordinator)
    return {
        "events": gateway.events(
            guid=query.get("guid") or None,
            limit=_int(query.get("limit"), 50),
            before=_int(query.get("before"), 0) or None,
        )
    }

async def gateway_call(
    coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> Any:
    """Исполнить ОПИСАННЫЙ вызов — универсальная дверь наружу.

    ⚠ Дом не знает ни одного вендора и не разбирает ни одного ответа: он
    подставляет сессию (и токен открытой записи), выполняет вызов у доступа из
    конфига объекта и отдаёт ответ КАК ЕСТЬ
    (`gateway.py`, docs/plan-thin-integration.md).

    ⚠ Дверь берётся у КООРДИНАТОРА, а не у драйвера видеонаблюдения: она несёт
    вызовы к любой описанной системе, и объект без регистратора обязан ею
    пользоваться так же (`docs/plan-video-rework.md`, «Сквозной принцип»).
    """
    import base64 as _base64
    import json as _json

    from .gateway import AccessDenied, AccessUnreachable

    door = getattr(coordinator, "accesses", None)
    if door is None:
        raise OpError("У объекта нет ни одного доступа", HTTPStatus.NOT_FOUND)
    # ⚠ Описания ещё не приехали (конфиг дома постарше) — доступа ЭТОГО НЕТ, и
    # это 404, а не отказ: по отказу бандл решил бы, что ему нельзя, и вызов у
    # жильца упал бы с ошибкой (живой отчёт 2026-09-12).
    if door.descriptor(payload.get("access")) is None:
        raise OpError("У объекта нет такого доступа", HTTPStatus.NOT_FOUND)

    session: dict[str, str] = {}
    clip_id = payload.get("clip")
    gateway = getattr(coordinator, "trassir", None)
    if clip_id and gateway is not None:
        token = gateway.clips.token_of(str(clip_id))
        if token:
            session["token"] = token

    body = payload.get("body")
    # ⚠ Тело строкой — это base64 (им же носит файлы реле); объект — это JSON.
    # `str(dict)` давал питоновский repr с одинарными кавычками: получатель
    # такого тела не разберёт, а понять по ответу, что ушло, невозможно.
    if isinstance(body, str) and body:
        raw = _base64.b64decode(body)
    elif isinstance(body, (dict, list)):
        raw = _json.dumps(body).encode("utf-8")
    else:
        raw = None
    try:
        status, content_type, answer = await door.call(
            payload.get("access"),
            str(payload.get("method") or "GET"),
            str(payload.get("path") or ""),
            payload.get("params"),
            raw,
            session,
        )
    except AccessUnreachable as err:
        # ⚠ 502, а НЕ 403. Разница не косметическая: отказ политики — это наша
        # ошибка в описании вызова, а недоступность той системы — беда объекта,
        # и жилец обязан прочитать её словами. Пока обе беды приезжали одним
        # кодом, жилец читал «Дом не смог выполнить запрос» и поломку шли
        # искать в доме (ревизия 2026-09-13).
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err
    except AccessDenied as err:
        raise OpError(str(err), HTTPStatus.FORBIDDEN) from err
    if "json" in (content_type or "") and not payload.get("binary"):
        return _json.loads(answer.decode("utf-8", "ignore"))
    return {
        "status": status,
        "contentType": content_type or "",
        "body": _base64.b64encode(answer).decode("ascii"),
    }

async def trassir_clip_at(
    coordinator: MegaHomeCoordinator,
    guid: str,
    timestamp_us: int | None = None,
    camera_name: str | None = None,
    remote: bool = False,
    quality: str | None = None,
    window_start_us: int | None = None,
    window_stop_us: int | None = None,
) -> dict[str, Any]:
    """Открыть АРХИВ КАНАЛА на метке — классический просмотр по дню и времени.

    ⚠ Событие здесь не нужно вовсе, и это не мелочь: событие — лишь ОДНА из
    причин посмотреть запись, а смотреть хотят и «что было вчера в 21:40», где
    события в нашей летописи может и не быть (она вообще конечной глубины).
    Запись живёт в архиве регистратора и открывается по метке.

    ⚠ Метки нет — «последняя запись»: дом считает её своими часами, регистратор
    сам встаёт на ближайший записанный кадр. Приложение своей метки в шкале
    Trassir не имеет: там пояс сервера, и «сейчас» телефона сдвинуло бы
    открытие на часы.
    """
    from .trassir_client import TrassirError

    try:
        return await trassir(coordinator).clips.async_open_at(
            guid,
            timestamp_us,
            camera_name=camera_name,
            remote=remote,
            quality=quality,
            window_start_us=window_start_us,
            window_stop_us=window_stop_us,
        )
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err

async def trassir_ready(
    coordinator: MegaHomeCoordinator,
    clip_id: str,
    position_us: Any = None,
    window_start_us: Any = None,
    window_stop_us: Any = None,
) -> dict[str, Any]:
    """Телефон собрал тракт: отдать архиву единственную команду старта.

    ⚠ Окно приложение может уточнить ИМЕННО ЗДЕСЬ, и это не прихоть: узнать, с
    какого места играть, оно способно только у открытого потока (календарь
    регистратор отдаёт лишь потоку с потребителем), а поток открывается на шаг
    раньше. Дом присланные числа не толкует — кладёт в команду как есть.
    """
    from .trassir_client import TrassirError

    def number(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    try:
        return await trassir(coordinator).clips.async_ready(
            clip_id,
            number(position_us),
            number(window_start_us),
            number(window_stop_us),
        )
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err

async def trassir_preview(
    coordinator: MegaHomeCoordinator, channel: str, timestamp_us: Any
) -> tuple[str, bytes]:
    """Маленький кадр архива канала на метке — превью при перемотке."""
    from .trassir_client import TrassirError

    try:
        at = int(timestamp_us)
    except (TypeError, ValueError) as err:
        raise OpError("Метка — микросекунды числом", HTTPStatus.BAD_REQUEST) from err
    try:
        return "image/jpeg", await trassir(coordinator).async_preview(channel, at)
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err

async def trassir_thumb(
    coordinator: MegaHomeCoordinator, event_id: str, lead_s: int | None = None
) -> tuple[str, bytes]:
    """Превью события — кадр архива, уже уменьшенный домом.

    ⚠ `lead_s` — на сколько секунд ПОЗЖЕ метки взять кадр, и присылает его
    приложение. Это решение о том, что показать человеку, а не свойство
    регистратора: детектор срабатывает, когда причина ещё только входит в кадр.
    Держать такое в Python значит платить за него релизом HACS на каждом
    объекте (docs/plan-thin-integration.md).
    """
    from .trassir_client import TrassirError

    try:
        return "image/jpeg", await trassir(coordinator).async_thumb(event_id, lead_s)
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err

def lead_of(value: Any) -> int | None:
    """Сдвиг превью из запроса приложения: секунды числом или «не прислали».

    ⚠ Не `_int` с умолчанием: «не прислали» и «прислали ноль» — РАЗНЫЕ вещи.
    Ноль означает «кадр ровно на метке», а отсутствие — «реши сам» (старый
    бандл), и склеивать их значит молча отобрать у приложения выбор.
    """
    if value is None or value == "":
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
