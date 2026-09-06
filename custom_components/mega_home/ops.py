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

from .const import COMMAND_SERVICES, LOGGER
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
) -> Any:
    """Run one operation by name. Unknown name is a refusal, not a crash."""
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
    """Чем исполнять команду: службой ИЗ КОНФИГА, а не из таблицы в этом файле.

    ⚠ Ради этого затевался тонкий шлюз (docs/plan-thin-integration.md, фаза 2).
    Пока карта команд жила здесь, новый управляемый домен — вентилятор, замок,
    пылесос — стоил релиза HACS и перезапуска Home Assistant НА КАЖДОМ объекте.
    Теперь менеджер кладёт службу в конфиг плитки, и она доезжает обычной
    синхронизацией.

    ⚠ Фолбэк на `COMMAND_SERVICES` оставлен на одну версию: конфиг в кэше дома
    старше этого кода ровно до первой синхронизации, и без фолбэка объект после
    обновления интеграции остался бы без управления до неё.
    """
    if not isinstance(name, str):
        return None
    described = (tile.get("commands") or {}).get(name)
    if isinstance(described, dict) and described.get("service"):
        return {
            "domain": described.get("domain") or tile["domain"],
            "service": described["service"],
            "arg": described.get("arg"),
            "min": described.get("min"),
            "max": described.get("max"),
        }
    service = COMMAND_SERVICES.get(tile["domain"], {}).get(name)
    if not service:
        return None
    return {"domain": tile["domain"], "service": service, **LEGACY_ARGS.get(name, {})}


# Аргументы команд для домов, чей кэш конфига ещё без карты команд. Уходит
# вместе с `COMMAND_SERVICES` следующим выпуском.
LEGACY_ARGS: dict[str, dict[str, Any]] = {
    "set_brightness": {"arg": "brightness_pct", "min": 0, "max": 100},
    "set_position": {"arg": "position", "min": 0, "max": 100},
    "set_temperature": {"arg": "temperature", "min": 5, "max": 40},
    "set_mode": {"arg": "hvac_mode"},
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

    ⚠ `name` и `roomId` пока едут: приложение берёт их из конфига только начиная
    с бандла 2026-09-06, и убрать их можно лишь ПОСЛЕ того, как этот бандл
    повышен в релиз (правило выпуска: поле не убирают в том же выпуске, в
    котором появилась его замена).
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

    return {
        "id": tile["id"],
        "roomId": tile.get("roomId"),
        "name": tile.get("name"),
        "domain": domain,
        "state": values,
        "attributes": _public_attributes(attributes),
        "available": state is not None and not unavailable,
        "updatedAt": int(state.last_updated.timestamp() * 1000) if state else None,
    }
