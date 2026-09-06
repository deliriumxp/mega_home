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

from .const import CAPABILITIES, COMMAND_SERVICES, LOGGER
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
    service = COMMAND_SERVICES.get(tile["domain"], {}).get(name)
    if not service:
        raise OpError("Команда не поддерживается устройством", HTTPStatus.NOT_FOUND)
    if name == "set_brightness" and not tile.get("dimmable"):
        raise OpError(
            "Устройство не поддерживает регулировку яркости", HTTPStatus.NOT_FOUND
        )

    try:
        data = service_data(name, payload.get("value"))
    except ValueError as err:
        raise OpError(str(err)) from err

    await call(hass, tile["domain"], service, {"entity_id": tile["entityId"], **data})
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


def service_data(command_name: str, value: Any) -> dict[str, Any]:
    """Validate the one numeric argument a command may carry."""
    if command_name == "set_brightness":
        return {"brightness_pct": number(value, 0, 100)}
    if command_name == "set_position":
        return {"position": number(value, 0, 100)}
    if command_name == "set_temperature":
        return {"temperature": number(value, 5, 40)}
    if command_name == "set_mode":
        return {"hvac_mode": str(value or "")}
    return {}


def number(value: Any, low: int, high: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Значение должно быть от {low} до {high}") from err
    if not low <= parsed <= high:
        raise ValueError(f"Значение должно быть от {low} до {high}")
    return parsed


def _brightness_capable(attributes: Any) -> bool:
    """Поддерживает ли сущность яркость — по её живому состоянию."""
    modes = (attributes or {}).get("supported_color_modes")
    if isinstance(modes, (list, tuple)) and modes:
        return any(mode != "onoff" for mode in modes)
    return isinstance((attributes or {}).get("brightness"), (int, float))


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


# ⚠ Биты — из `MediaPlayerEntityFeature` Home Assistant, это его ПУБЛИЧНЫЙ
# контракт (их читают интеграции по всему миру, менять их HA не может). Тот же
# список лежит в менеджере (smart-home-view.util.ts): проекций две, и они обязаны
# отвечать одинаково — правится в обоих местах одной правкой.
MEDIA_FEATURES: tuple[tuple[int, str], ...] = (
    (1, "pause"),
    (4, "volume"),
    (8, "mute"),
    (16, "previous"),
    (32, "next"),
    (128, "turn_on"),
    (256, "turn_off"),
    (2048, "source"),
    (16384, "play"),
)


def _media_capabilities(attributes: Any) -> list[str]:
    """Что умеет ЭТОТ плеер — из маски `supported_features`."""
    mask = (attributes or {}).get("supported_features")
    if not isinstance(mask, int) or mask <= 0:
        return []
    out = [name for bit, name in MEDIA_FEATURES if mask & bit == bit]
    # Плитка спрашивает одним словом: «есть ли вкл/выкл» и «есть ли пуск/пауза».
    if "turn_on" in out or "turn_off" in out:
        out.append("power")
    if "play" in out or "pause" in out:
        out.append("play_pause")
    return out


def entity_view(tile: dict[str, Any], state: State | None) -> dict[str, Any]:
    """Project one Home Assistant state into what the app's screens read.

    ⚠ This is the same projection the manager does in smart-home-view.util.ts.
    The two live in different repositories on purpose (the manager is out of the
    runtime path), so the shape is a contract: change it here and there in the
    same breath.
    """
    domain = tile["domain"]
    raw = state.state if state else None
    attributes = state.attributes if state else {}
    unavailable = raw in ("unavailable", "unknown")
    values: dict[str, Any] = {"value": raw}

    if domain == "light":
        values["power"] = raw == "on"
        brightness = attributes.get("brightness")
        if isinstance(brightness, (int, float)):
            values["brightness"] = round(brightness / 255 * 100)
    elif domain in ("switch", "binary_sensor"):
        values["power"] = raw == "on"
        if domain == "binary_sensor":
            values["deviceClass"] = attributes.get("device_class")
    elif domain == "cover":
        position = attributes.get("current_position")
        if isinstance(position, (int, float)):
            values["position"] = position
    elif domain == "climate":
        values["temperature"] = attributes.get("current_temperature")
        values["targetTemperature"] = attributes.get("temperature")
        values["mode"] = raw
    elif domain == "media_player":
        # ⚠ Состояний у плеера пять (off/idle/playing/paused/standby), и сводить
        # их к `power` нельзя: пауза — это ВКЛЮЧЁН, но не играет, и кнопка на
        # плитке в этих двух случаях разная.
        values["power"] = raw not in ("off", "standby") and not unavailable
        values["playing"] = raw == "playing"
        # Показываем ровно то, что показывает своей плиткой сам Home Assistant.
        values["title"] = attributes.get("media_title")
        values["subtitle"] = attributes.get("media_artist") or attributes.get("app_name")
        values["source"] = attributes.get("source")
        volume = attributes.get("volume_level")
        if isinstance(volume, (int, float)):
            values["volume"] = round(volume * 100)
        values["muted"] = attributes.get("is_volume_muted") is True
    elif domain == "camera":
        # A camera has no on/off: its state is idle/recording/streaming. An
        # invented `power` would turn the tile into a switch with nothing to
        # switch, so we only hand over where to look.
        values.update(_camera_urls(tile.get("entityId"), attributes))
        # Which way Home Assistant serves live video: `hls` or `web_rtc`. Nothing
        # reads it yet - the app shows MJPEG, which every browser plays without a
        # single dependency. It travels from the start because remote viewing
        # depends on it and there is nowhere to learn it after the fact.
        values["streamType"] = attributes.get("frontend_stream_type")

    capabilities = (
        _media_capabilities(attributes)
        if domain == "media_player"
        else list(CAPABILITIES.get(domain, []))
    )
    # Диммируемость KNX-плитки известна из адреса ETS (`dimmable`), а у сущности,
    # найденной сканом HA, — только по живому состоянию: в реестре атрибутов нет.
    # Та же развилка в менеджере (smart-home-view.util.ts) — контракт общий.
    if domain == "light" and (tile.get("dimmable") or _brightness_capable(attributes)):
        capabilities.append("brightness")

    return {
        "id": tile["id"],
        "roomId": tile.get("roomId"),
        "name": tile.get("name"),
        "domain": domain,
        "capabilities": capabilities,
        "state": values,
        "available": state is not None and not unavailable,
        "updatedAt": int(state.last_updated.timestamp() * 1000) if state else None,
    }
