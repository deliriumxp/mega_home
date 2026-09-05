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

    return await call(hass, tile["domain"], service, {"entity_id": tile["entityId"], **data})


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
        await hass.services.async_call(domain, service, data, blocking=False)
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

    capabilities = list(CAPABILITIES.get(domain, []))
    if domain == "light" and tile.get("dimmable"):
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
