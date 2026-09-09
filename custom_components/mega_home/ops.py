"""The four operations the resident app needs, independent of transport.

They are reached two ways and must answer identically:

* locally — the app served inside the house calls the HTTP views (`http.py`);
* remotely — the manager forwards the resident's request over the live link
  (`link.py`), because the resident's phone cannot reach this house directly.

⚠ That is the whole reason this module exists. Before it, the logic lived in the
view handlers, welded to `web.Request` and `web.Response`; the link would have
had to fake HTTP requests or grow a second copy of the same rules — and two
copies of "which service does this command map to" is exactly the kind of pair
that silently drifts apart.

Answers are plain data; refusals are `OpError`, which each transport renders in
its own way (an HTTP status here, a frame field there) with the SAME wording:
the resident must read the same sentence whether they are home or away.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import voluptuous as vol

from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import ServiceNotFound

from .const import LOGGER
from .coordinator import MegaHomeCoordinator


class OpError(Exception):
    """A refusal the resident should read, with the status that fits it."""

    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.message = message
        self.status = int(status)


async def run(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator | None,
    op: str,
    payload: dict[str, Any] | None,
    remote: bool = False,
) -> Any:
    """Run one operation by name. Unknown name is a refusal, not a crash.

    ⚠ `remote` — жилец пришёл ЧЕРЕЗ МЕНЕДЖЕРА, а не локальной дверью. Ответы от
    этого не меняются и меняться не должны: разница «дома/снаружи» живёт в
    адресе базы, а не в наборе функций. Признак нужен ровно там, где физика
    разная, — сколько ждать внешний адрес дома (дома он не нужен вовсе).
    """
    if coordinator is None or not coordinator.data:
        raise OpError(
            "Дом ещё не синхронизирован с менеджером", HTTPStatus.SERVICE_UNAVAILABLE
        )
    data = payload or {}
    if op == "config":
        return config(coordinator)
    if op == "states":
        return states(hass, coordinator)
    if op == "command":
        return await command(hass, coordinator, data)
    if op == "scenario":
        return await scenario(hass, coordinator, data)
    if op == "webrtc":
        return await webrtc_offer(hass, coordinator, data, remote)
    if op == "webrtc-close":
        return webrtc_close(hass, coordinator, data)
    if op == "http":
        # Перенос ОБЫЧНОГО запроса к API этого дома: жилец снаружи должен уметь
        # ровно то же, что дома, и теми же путями (`relay_api.py`).
        #
        # ⚠ Именованные операции выше остаются ради уже работающих домов и
        # менеджеров. НОВЫХ сюда добавлять не надо: каждая такая операция — это
        # функция, которой снаружи нет, пока её не написали в трёх местах и не
        # раскатали релизом на каждый объект. Для этого и есть перенос.
        #
        # ⚠ Импорт ЛОКАЛЬНЫЙ: `relay_api` зовёт этот модуль, и на уровне файла
        # это был бы цикл.
        from .relay_api import handle

        return await handle(hass, coordinator, data)
    if op == "probe":
        # Проба устройства объекта по заданию МЕНЕДЖЕРА (`probe.py`): мониторинг
        # больше не ходит в LAN объекта по WG-туннелю, которого у части парка
        # нет вовсе. Здесь только примитивы — что и зачем спрашивать, знает
        # менеджер, и меняется это его деплоем, а не релизом HACS.
        #
        # ⚠ Импорт ЛОКАЛЬНЫЙ: `probe` берёт `OpError` отсюда, и на уровне файла
        # это был бы цикл (то же, что у `relay_api`).
        from .probe import run as run_probe

        return await run_probe(hass, data)
    if op == "scan":
        # Обход локальной сети объекта по заказу МЕНЕДЖЕРА (`scan.py`): какие
        # устройства стоят и куда из них можно войти по веб-интерфейсу. Список
        # DHCP-аренд роутера этого не даёт — статика в нём не появляется.
        #
        # ⚠ Импорт ЛОКАЛЬНЫЙ по той же причине, что у `probe`.
        from .scan import run as run_scan

        return await run_scan(hass, data)
    raise OpError("Неизвестная операция", HTTPStatus.NOT_FOUND)


def config(coordinator: MegaHomeCoordinator) -> dict[str, Any]:
    """The cached home config: floors, rooms, tiles, scenarios."""
    return coordinator.data


def states(hass: HomeAssistant, coordinator: MegaHomeCoordinator) -> dict[str, Any]:
    """Current states of every tile, read straight from this Home Assistant."""
    entities = [
        entity_view(tile, hass.states.get(tile["entityId"]) if tile.get("entityId") else None)
        for tile in coordinator.data.get("tiles", [])
    ]
    _warm_cameras(hass, coordinator)
    return {
        # Always connected: this runs inside the home, so there is no link to
        # lose between here and Home Assistant. When the manager forwards this
        # answer to a resident who is away, the link being up is what let the
        # answer arrive at all.
        "connected": True,
        # `configVersion` rides along on purpose: the app polls states every few
        # seconds anyway, so this is the cheapest way to tell a phone that has
        # been open for days that the installer added a socket.
        "configVersion": coordinator.version,
        "appVersion": coordinator.bundle.version if coordinator.bundle else None,
        "entities": entities,
    }


def _warm_cameras(hass: HomeAssistant, coordinator: MegaHomeCoordinator) -> None:
    """Держать наготове кадр каждой камеры, пока приложение открыто.

    ⚠ Опрос состояний — единственный признак «приложение открыто», который у
    дома есть, и он же лучший момент для подготовки: камеру открывают из сетки
    плиток, то есть через секунду-другую после этого запроса. Сам снимок стоит
    секунду с лишним (ffmpeg у камеры без снапшот-адреса), и добывать его в
    момент открытия — значит показывать пустой прямоугольник ровно столько,
    сколько идут переговоры (жалоба 2026-09-08). Частоту ограничивает сам
    `webrtc.warm`, здесь только перечень камер.
    """
    from . import webrtc

    for tile in coordinator.data.get("tiles", []):
        if tile.get("domain") == "camera" and tile.get("entityId"):
            webrtc.warm(hass, tile["entityId"])


async def command(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> dict[str, Any]:
    """One command for one tile, mapped onto a Home Assistant service call."""
    tile = find(coordinator.data.get("tiles", []), payload.get("id"))
    if tile is None:
        raise OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)
    if not tile.get("entityId"):
        # Same wording as the manager: the element is in the project but was
        # never pushed to Home Assistant, so there is nothing to command.
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — управлять им пока нечем"
        )

    name = payload.get("command")
    spec = command_spec(tile, name)
    if spec is None:
        raise OpError("Команда не поддерживается устройством", HTTPStatus.NOT_FOUND)

    try:
        data = service_data(spec, payload.get("value"))
    except ValueError as err:
        raise OpError(str(err)) from err

    await call(
        hass, spec["domain"], spec["service"], {"entity_id": tile["entityId"], **data}
    )
    # ⚠ Ответ несёт НОВОЕ состояние плитки, а не только «принято». Иначе
    # приложению остаётся либо ждать следующего снимка (тап выглядит
    # непринятым почти секунду), либо рисовать угаданное состояние — и то и
    # другое неправильно там, где настоящее состояние лежит в двух шагах.
    # Служба вызвана блокирующе, поэтому машина состояний уже обновлена.
    return {
        "accepted": True,
        "entity": entity_view(tile, hass.states.get(tile["entityId"])),
    }


async def scenario(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> dict[str, Any]:
    """Run one scenario (a Home Assistant script)."""
    item = find(coordinator.data.get("scenarios", []), payload.get("id"))
    if item is None:
        raise OpError("Сценарий не найден", HTTPStatus.NOT_FOUND)
    if not item.get("entityId"):
        raise OpError("Сценарий не создан в Home Assistant", HTTPStatus.NOT_FOUND)
    return await call(hass, "script", "turn_on", {"entity_id": item["entityId"]})


async def webrtc_offer(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator,
    payload: dict[str, Any],
    remote: bool = False,
) -> dict[str, Any]:
    """Свести телефон жильца, который СНАРУЖИ, с камерой этого дома напрямую.

    Через менеджер проходит только этот обмен (килобайты SDP), видео идёт мимо
    него — ради этого всё и затевалось (remote-access.md у менеджера).

    ⚠ Импорт локальный: `webrtc.py` берёт отсюда `OpError`, и разорвать
    кольцо иначе нечем. Заодно модуль камеры Home Assistant не грузится в домах,
    где камер нет вовсе.
    """
    from . import webrtc
    from .trassir_clip import CLIP_PREFIX

    sdp = payload.get("offer")
    if not isinstance(sdp, str) or not sdp:
        raise OpError("Предложение WebRTC не передано")
    # ⚠ Запись события идёт ТОЙ ЖЕ операцией, что и живая камера, и это не
    # экономия строк: своя операция под архив означала бы второй сеанс со своими
    # сроками, своим закрытием и своей диагностикой — то есть вторую трубу
    # (docs/trassir-integration-plan.md, §5а у менеджера). Отличается только
    # источник, и решает это приставка id.
    tile = payload.get("id")
    if isinstance(tile, str) and tile.startswith(CLIP_PREFIX):
        return await trassir(coordinator).clips.async_offer(hass, tile, sdp, remote)
    guid = _trassir_guid(coordinator, tile)
    if guid:
        # Живая камера регистратора: ссылка постоянная, сеанса и токена нет —
        # но путь тот же самый, что у камеры Home Assistant.
        #
        # ⚠ Качество приезжает В ПРЕДЛОЖЕНИИ, а не отдельной операцией: у живой
        # камеры смена качества — это смена ИСТОЧНИКА, то есть ровно те же
        # переговоры заново. Своя операция здесь означала бы состояние сеанса
        # там, где его нет вовсе.
        quality = payload.get("quality")
        gateway = trassir(coordinator)
        from .go2rtc_embed import is_running

        if not is_running():
            raise OpError(
                "Дом не может отдать камеру: не поднят его go2rtc",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        # ⚠ Два пути внутри: постоянный адрес канала и — если его у канала нет —
        # документированный токен. Решает это сам сеанс, потому что там же живут
        # пинг и уборка, которые запасному пути нужны (`async_live_offer`).
        return await gateway.clips.async_live_offer(
            hass, guid, sdp, "sub" if quality == "sub" else "main", remote
        )
    return await webrtc.negotiate(hass, camera_entity(coordinator, payload), sdp, remote)


def webrtc_close(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> dict[str, Any]:
    """Жилец закрыл просмотр — отпустить камеру, не дожидаясь развала связи."""
    from . import webrtc
    from .trassir_clip import CLIP_PREFIX

    session_id = payload.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        raise OpError("Сессия не указана")
    tile = payload.get("id")
    gateway = getattr(coordinator, "trassir", None)
    clip_id = tile if isinstance(tile, str) and tile.startswith(CLIP_PREFIX) else None
    if clip_id is None and gateway is not None:
        # Приложение могло закрыть просмотр, не назвав клип: сессия — тот же
        # ключ, и потерять уборку из-за отсутствующего поля нельзя.
        clip_id = gateway.clips.clip_of_session(session_id)
    if clip_id is not None and gateway is not None:
        hass.async_create_task(gateway.clips.async_close(hass, clip_id, session_id))
        return {"closed": True}
    return webrtc.close(hass, camera_entity(coordinator, payload), session_id)


async def camera_frame(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> tuple[str, bytes]:
    """Один кадр камеры — постер, пока идут переговоры (`webrtc.snapshot`).

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
        from .trassir import _shrink
        from .trassir_client import TrassirError

        try:
            raw = await client.async_screenshot(guid)
        except TrassirError as err:
            raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err
        # Тот же размер, что у превью события: полный кадр регистратора — это
        # полмегабайта на каждое открытие шторки.
        return "image/jpeg", await hass.async_add_executor_job(_shrink, raw)

    from . import webrtc

    return await webrtc.snapshot(hass, camera_entity(coordinator, payload))


def camera_entity(
    coordinator: MegaHomeCoordinator, payload: dict[str, Any]
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
        if tile.get("trassirGuid"):
            raise OpError("Это камера видеонаблюдения", HTTPStatus.CONFLICT)
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — смотреть пока нечего",
            HTTPStatus.NOT_FOUND,
        )
    return tile["entityId"]


async def call(
    hass: HomeAssistant, domain: str, service: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Call a Home Assistant service and turn its refusals into plain answers.

    A service can be missing outright — `climate.set_temperature` does not exist
    on an installation with no climate integration loaded — and that raises.
    Without this the resident would get a bare 500 for a house that is simply
    not set up yet.
    """
    try:
        # blocking=True: ответ обязан нести состояние ПОСЛЕ выполнения команды
        # (см. command). Служба выполняется внутри того же Home Assistant, так
        # что ожидание здесь — это доли миллисекунды, а не сетевой поход.
        await hass.services.async_call(domain, service, data, blocking=True)
    except ServiceNotFound as err:
        LOGGER.warning("Service %s.%s is not available", domain, service)
        raise OpError(
            "Home Assistant не умеет выполнять эту команду на этом объекте",
            HTTPStatus.NOT_FOUND,
        ) from err
    except vol.Invalid as err:
        LOGGER.warning("Service %s.%s rejected the payload: %s", domain, service, err)
        raise OpError("Home Assistant отклонил команду") from err
    return {"accepted": True}


def find(items: list[dict[str, Any]], item_id: Any) -> dict[str, Any] | None:
    if not isinstance(item_id, str):
        return None
    return next((item for item in items if item.get("id") == item_id), None)


def command_spec(tile: dict[str, Any], name: Any) -> dict[str, Any] | None:
    """Чем исполнять команду: службой ИЗ КОНФИГА — другого источника больше нет.

    ⚠ Ради этого затевался тонкий шлюз (docs/plan-thin-integration.md, фаза 2).
    Пока карта команд жила в Python, новый управляемый домен — вентилятор, замок,
    пылесос — стоил релиза HACS и перезапуска Home Assistant НА КАЖДОМ объекте.
    Теперь менеджер кладёт службу в конфиг плитки, и она доезжает обычной
    синхронизацией. Не заводи таблицу доменов здесь снова: она немедленно начнёт
    расходиться с менеджерской (`tileCommands` в `smart-home-view.util.ts`), а
    чинится такое расхождение только выездом.

    ⚠ Фолбэк `COMMAND_SERVICES`/`LEGACY_ARGS` убран в 0.1.14 (фаза 3). Он жил
    ровно один выпуск — на дом, чей кэш конфига старше кода. Теперь такого дома
    не бывает: обновление интеграции идёт через HACS, то есть по интернету, а
    тот же интернет приносит и конфиг с картой команд.
    """
    if not isinstance(name, str):
        return None
    described = (tile.get("commands") or {}).get(name)
    if not isinstance(described, dict) or not described.get("service"):
        return None
    return {
        "domain": described.get("domain") or tile["domain"],
        "service": described["service"],
        "arg": described.get("arg"),
        "min": described.get("min"),
        "max": described.get("max"),
    }


def service_data(spec: dict[str, Any], value: Any) -> dict[str, Any]:
    """Единственный аргумент команды, проверенный по описанным границам.

    ⚠ Границы приходят из конфига, но проверяет их ЭТА сторона: службу зовём мы,
    а браузеру жильца верить нельзя. Аргумент без границ — строковый (режим
    термостата), с границами — число.
    """
    arg = spec.get("arg")
    if not arg:
        return {}
    low, high = spec.get("min"), spec.get("max")
    if low is None or high is None:
        return {arg: str(value or "")}
    return {arg: number(value, low, high)}


def number(value: Any, low: int, high: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Значение должно быть от {low} до {high}") from err
    if not low <= parsed <= high:
        raise ValueError(f"Значение должно быть от {low} до {high}")
    return parsed


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


# Что из атрибутов наружу НЕ уходит.
#
# ⚠ Список короткий намеренно. Это не «фильтр полезного» — атрибуты уходят
# ЦЕЛИКОМ (решение 2026-09-06, см. смежный комментарий в
# smart-home-view.util.ts менеджера): приложение живёт только внутри Home
# Assistant, и сокращать то, что он уже посчитал, — работа без выгоды. Здесь
# только то, чему в браузере жильца делать нечего: `access_token` это секрет, из
# которого мы уже собрали адреса кадра и потока камеры, и отдать его отдельным
# полем значит отдать право собрать любой другой адрес того же HA.
HIDDEN_ATTRIBUTES = frozenset({"access_token"})


def _public_attributes(attributes: Any) -> dict[str, Any]:
    return {
        key: value
        for key, value in (attributes or {}).items()
        if key not in HIDDEN_ATTRIBUTES
    }


def entity_view(tile: dict[str, Any], state: State | None) -> dict[str, Any]:
    """Что дом отвечает о приборе: сырое состояние Home Assistant и атрибуты.

    ⚠ Проекции здесь БОЛЬШЕ НЕТ (docs/plan-thin-integration.md, фаза 1).
    `power`, `playing`, яркость, позицию, температуру и способности считает
    ПРИЛОЖЕНИЕ — в одном месте на весь продукт (`ha-entity.ts`). Раньше то же
    самое считалось трижды: здесь, в менеджере и на экране, — и каждое новое
    поле экрана стоило правки в двух репозиториях, из которых ЭТОТ доезжает до
    объекта только релизом HACS и перезапуском Home Assistant. Ради того, чтобы
    в интеграции нечему было ломаться, всё это отсюда и убрано: не возвращай.

    ⚠ Камера — единственное исключение, и оно не растёт: адреса кадра и потока
    строятся из `entity_id` и `access_token`, а наружу не уходит ни то, ни
    другое (`entity_id` намеренно, токен как секрет).

    ⚠ `name` и `roomId` УБРАНЫ в 0.1.14 (фаза 3), после того как бандл со
    склейкой повышен в релиз. Подписи и комнаты приложение берёт из конфига
    (`HomeTiles` в `home-shape.ts`) — дом отвечает только тем, что знает Home
    Assistant. Не возвращай их «для надёжности»: два источника одного имени
    разъедутся ровно тогда, когда инсталлятор переименует прибор, а конфиг и
    состояния придут разными дорогами.

    ⚠ У МЕНЕДЖЕРА (`toEntityView` в `smart-home-view.util.ts`) те же два поля
    ОСТАЮТСЯ, и это не рассинхрон: его ответ читает ещё и превью в карточке
    объекта (`ManagerHomeBackend`), а оно склейку с конфигом не делает — там
    конфига нет вовсе.
    """
    domain = tile["domain"]
    raw = state.state if state else None
    attributes = state.attributes if state else {}
    unavailable = raw in ("unavailable", "unknown")
    values: dict[str, Any] = {"value": raw}

    if domain == "camera":
        values.update(_camera_urls(tile.get("entityId"), attributes))
        # Каким способом Home Assistant отдаёт живое видео: `hls` или `web_rtc`.
        # Пока не читает никто — приложение показывает MJPEG, который умеет любой
        # браузер без единой зависимости, — но от поля зависит удалённый просмотр,
        # и узнать его задним числом неоткуда.
        values["streamType"] = attributes.get("frontend_stream_type")

    # ⚠ Камера видеонаблюдения ДОСТУПНА без сущности Home Assistant: её показывает
    # сам дом, забирая поток у регистратора. Считать её недоступной значило бы
    # написать жильцу «Нет данных» поверх работающей камеры.
    available = bool(tile.get("trassirGuid")) or (state is not None and not unavailable)

    return {
        "id": tile["id"],
        "domain": domain,
        "state": values,
        "attributes": _public_attributes(attributes),
        "available": available,
        "updatedAt": int(state.last_updated.timestamp() * 1000) if state else None,
    }


# --- TRASSIR: лента событий объекта ---
#
# ⚠ Обработчики ПУТЕЙ, а не именованные операции канала: их зовут обе двери —
# локальная (`http.py`) и перенос запроса снаружи (`relay_api.py`). Ровно ради
# этого перенос и заведён, и заводить под видеонаблюдение свою операцию значило
# бы строить вторую трубу (docs/trassir-integration-plan.md, §5а у менеджера).


def _trassir_guid(coordinator: MegaHomeCoordinator, tile_id: Any) -> str | None:
    """Канал регистратора у плитки — или None, если это обычная камера."""
    tile = find((coordinator.data or {}).get("tiles", []), tile_id)
    guid = tile.get("trassirGuid") if tile else None
    return guid if isinstance(guid, str) and guid else None


def trassir(coordinator: MegaHomeCoordinator) -> Any:
    """Шлюз к регистратору объекта — или понятный отказ, если его нет."""
    gateway = getattr(coordinator, "trassir", None)
    if gateway is None or not gateway.configured:
        raise OpError(
            "У этого объекта не настроено видеонаблюдение", HTTPStatus.NOT_FOUND
        )
    return gateway


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


async def trassir_play(
    coordinator: MegaHomeCoordinator,
    event_id: str,
    remote: bool = False,
    quality: str | None = None,
) -> dict[str, Any]:
    """Открыть запись события и вернуть её id — дальше обычный просмотр.

    ⚠ Возвращает НЕ ссылку на видео: адрес потока наружу не уходит вовсе.
    Приложение получает id, который отдаёт в `webrtc` ровно так же, как id
    плитки камеры, — и поэтому снаружи запись работает тем же путём, что живой
    просмотр, без единой новой трубы.

    ⚠ КАЧЕСТВО выбирает ПРИЛОЖЕНИЕ и присылает его сюда (`quality`). Оно знает
    свою дверь лучше нас — у него для этого два разных транспорта, —  а правило
    «дома основной, снаружи суб» это ПОЛИТИКА, а не физика. Политика, лежащая в
    Python, стоит релиза HACS и перезапуска Home Assistant на каждом объекте
    (docs/plan-thin-integration.md), поэтому её здесь больше нет.

    ⚠ `remote` остался ТОЛЬКО как умолчание для старых бандлов, которые качества
    не присылают: без него удалённый жилец получил бы основной архив на мобильном
    канале. СНЯТЬ вместе со свёрткой событий, когда релизный бандл поднимут, —
    правило выпуска запрещает убирать замену и заменяемое одним выпуском.
    """
    from .trassir_client import TrassirError

    try:
        return await trassir(coordinator).clips.async_open(
            event_id, remote=remote, quality=quality
        )
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err


async def trassir_seek(
    coordinator: MegaHomeCoordinator,
    clip_id: str,
    position_us: int | None,
    quality: str | None = None,
) -> dict[str, Any]:
    """Перемотка ПЕРЕОТКРЫТИЕМ: ответ — новый клип, телефон сводит заново.

    ⚠ Повтором команды по тому же соединению — нельзя: стенд показал, что со
    второй-третьей команды данные встают. Поэтому здесь новый токен, новый
    поток и новые переговоры (`trassir_clip.async_seek`), а не «та же команда
    с новым стартом».

    ⚠ Кнопка качества у записи идёт ЭТОЙ ЖЕ дверью: поток и токен привязаны к
    качеству, значит смена качества — то же переоткрытие, только позиция
    остаётся прежней. Своя операция дала бы вторую механику того же самого.
    """
    from .trassir_client import TrassirError

    try:
        return await trassir(coordinator).clips.async_seek(
            clip_id, position_us, quality
        )
    except TrassirError as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err


async def trassir_ready(
    coordinator: MegaHomeCoordinator, clip_id: str
) -> dict[str, Any]:
    """Телефон собрал тракт: отдать архиву единственную команду старта."""
    from .trassir_client import TrassirError

    try:
        return await trassir(coordinator).clips.async_ready(clip_id)
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


def _int(value: Any, default: int) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default
