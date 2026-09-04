"""The HTTP surface the resident app talks to.

Everything the app needs is served by Home Assistant itself, under one prefix:
the bundle as static files, the config from the local cache, and states and
commands straight from `hass`. The manager takes no part at runtime.

⚠ Until authentication is designed (phase 4 of docs/local-ha-app.md in the
manager repo) these views are open to anyone who can reach Home Assistant on the
local network. That is a deliberate, temporary risk for development — do not put
this on a customer object in this state.
"""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from typing import Any

import voluptuous as vol
from aiohttp import web

from homeassistant.components.http import HomeAssistantView, StaticPathConfig
from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import ServiceNotFound

from .const import (
    CAPABILITIES,
    COMMAND_SERVICES,
    DOMAIN,
    LOGGER,
    URL_API,
    URL_ICONS,
    URL_PREFIX,
)
from .bundle import PACKAGED_DIR
from .coordinator import MegaHomeCoordinator

# Упакованная копия объявлена в `bundle.py` — она её и раздаёт как фолбэк.
BUNDLE_DIR = PACKAGED_DIR


async def async_register_http(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator
) -> None:
    """Register the static paths and views once per Home Assistant run.

    Views and static paths live on the aiohttp app, which outlives a config
    entry reload, and registering the same route twice raises. So this runs once
    and the views resolve the current coordinator through hass.data instead of
    capturing it.
    """
    hass.data.setdefault(DOMAIN, {})
    if hass.data[DOMAIN].get("http_registered"):
        return

    await hass.async_add_executor_job(
        coordinator.icons_dir.mkdir, 0o755, True, True
    )
    # ⚠ Order matters: aiohttp resolves a prefix route to the FIRST static
    # resource whose prefix matches and then serves (or 404s) from it — it does
    # not fall through to the next one. `/mega-home/icons` therefore has to be
    # registered before `/mega-home`, or every icon would be looked up inside
    # the bundle directory and 404.
    static = [
        StaticPathConfig(URL_ICONS, str(coordinator.icons_dir), True),
    ]
    await hass.http.async_register_static_paths(static)

    # ⚠ Порядок регистрации несущий. Каталог бандла раздаётся НАШИМ view, а не
    # статическим путём: `async_register_static_paths` привязывает каталог в
    # момент регистрации, а перерегистрировать маршруты без перезапуска Home
    # Assistant нельзя — то есть смена версии интерфейса снова упёрлась бы в
    # перезапуск. View читает файл из АКТИВНОГО каталога, и переключение версии
    # это присваивание переменной.
    #
    # Поэтому же API-маршруты регистрируются ПЕРВЫМИ: `/mega-home/{path:.*}`
    # накрывает и их тоже, а aiohttp отдаёт запрос первому подошедшему ресурсу.
    for view in (
        MegaHomeConfigView,
        MegaHomeStatesView,
        MegaHomeCommandView,
        MegaHomeScenarioView,
        MegaHomeAppRootView,
        MegaHomeAppView,
    ):
        hass.http.register_view(view())
    hass.data[DOMAIN]["http_registered"] = True


def _coordinator(hass: HomeAssistant) -> MegaHomeCoordinator | None:
    """Return the coordinator of the loaded entry, if there is one.

    One Home Assistant serves one home, so the first loaded entry is the home.
    """
    for entry in hass.config_entries.async_loaded_entries(DOMAIN):
        return entry.runtime_data
    return None


class _MegaHomeView(HomeAssistantView):
    """Base view: no auth yet (see the module docstring) and no auth needed.

    The app never gets a Home Assistant token: states and service calls are made
    by this integration from inside Python, and only our own shape goes out.
    """

    requires_auth = False

    def coordinator_or_error(
        self, request: web.Request
    ) -> tuple[MegaHomeCoordinator | None, web.Response | None]:
        coordinator = _coordinator(request.app["hass"])
        if coordinator is None or not coordinator.data:
            return None, self.json_message(
                "Дом ещё не синхронизирован с менеджером",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        return coordinator, None


class MegaHomeConfigView(_MegaHomeView):
    """The cached home config: floors, rooms, tiles, scenarios."""

    url = f"{URL_API}/config"
    name = "api:mega_home:config"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        return self.json(coordinator.data)


class MegaHomeStatesView(_MegaHomeView):
    """Current states of every tile, read straight from this Home Assistant."""

    url = f"{URL_API}/states"
    name = "api:mega_home:states"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        entities = [
            _entity_view(tile, hass.states.get(tile["entityId"]) if tile.get("entityId") else None)
            for tile in coordinator.data.get("tiles", [])
        ]
        # Always connected: this runs inside the home, so there is no link to
        # lose between the app and Home Assistant.
        #
        # `configVersion` rides along on purpose. The app polls this endpoint
        # every few seconds anyway, so it is the cheapest possible way to tell a
        # phone that has been open for days that the installer added a socket:
        # the version moves, the app re-reads the config and redraws itself. No
        # extra request, no page reload, nobody pressing anything.
        return self.json(
            {
                "connected": True,
                "configVersion": coordinator.version,
                # Версия интерфейса — тем же способом: изменилась, значит на
                # объекте лежит новый бандл, и открытая вкладка обязана на него
                # перейти сама (docs/mega-home-updates.md).
                "appVersion": coordinator.bundle.version if coordinator.bundle else None,
                "entities": entities,
            }
        )


class MegaHomeCommandView(_MegaHomeView):
    """One command for one tile, mapped onto a Home Assistant service call."""

    url = f"{URL_API}/command"
    name = "api:mega_home:command"

    async def post(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Некорректный запрос", HTTPStatus.BAD_REQUEST)

        tile = _find(coordinator.data.get("tiles", []), payload.get("id"))
        if tile is None:
            return self.json_message("Устройство не найдено", HTTPStatus.NOT_FOUND)
        if not tile.get("entityId"):
            # Same wording as the manager: the element is in the project but was
            # never pushed to Home Assistant, so there is nothing to command.
            return self.json_message(
                "Элемент ещё не отправлен в Home Assistant — управлять им пока нечем",
                HTTPStatus.BAD_REQUEST,
            )

        command = payload.get("command")
        service = COMMAND_SERVICES.get(tile["domain"], {}).get(command)
        if not service:
            return self.json_message(
                "Команда не поддерживается устройством", HTTPStatus.NOT_FOUND
            )
        if command == "set_brightness" and not tile.get("dimmable"):
            return self.json_message(
                "Устройство не поддерживает регулировку яркости", HTTPStatus.NOT_FOUND
            )

        try:
            data = _service_data(command, payload.get("value"))
        except ValueError as err:
            return self.json_message(str(err), HTTPStatus.BAD_REQUEST)

        hass: HomeAssistant = request.app["hass"]
        return await _async_call(
            hass, tile["domain"], service, {"entity_id": tile["entityId"], **data}, self
        )


class MegaHomeScenarioView(_MegaHomeView):
    """Run one scenario (a Home Assistant script)."""

    url = f"{URL_API}/scenario"
    name = "api:mega_home:scenario"

    async def post(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Некорректный запрос", HTTPStatus.BAD_REQUEST)

        scenario = _find(coordinator.data.get("scenarios", []), payload.get("id"))
        if scenario is None:
            return self.json_message("Сценарий не найден", HTTPStatus.NOT_FOUND)
        if not scenario.get("entityId"):
            return self.json_message(
                "Сценарий не создан в Home Assistant", HTTPStatus.NOT_FOUND
            )

        hass: HomeAssistant = request.app["hass"]
        return await _async_call(
            hass, "script", "turn_on", {"entity_id": scenario["entityId"]}, self
        )


async def _async_call(
    hass: HomeAssistant,
    domain: str,
    service: str,
    data: dict[str, Any],
    view: _MegaHomeView,
) -> web.Response:
    """Call a Home Assistant service and turn its refusals into plain answers.

    A service can be missing outright — `climate.set_temperature` does not exist
    on an installation with no climate integration loaded — and that raises.
    Without this the resident would get a bare 500 for a house that is simply
    not set up yet.
    """
    try:
        await hass.services.async_call(domain, service, data, blocking=False)
    except ServiceNotFound:
        LOGGER.warning("Service %s.%s is not available", domain, service)
        return view.json_message(
            "Home Assistant не умеет выполнять эту команду на этом объекте",
            HTTPStatus.NOT_FOUND,
        )
    except vol.Invalid as err:
        LOGGER.warning("Service %s.%s rejected the payload: %s", domain, service, err)
        return view.json_message("Home Assistant отклонил команду", HTTPStatus.BAD_REQUEST)
    return view.json({"accepted": True})


class MegaHomeAppRootView(_MegaHomeView):
    """The bare prefix: hand out the app itself."""

    url = URL_PREFIX
    extra_urls = [f"{URL_PREFIX}/"]
    name = "mega_home:app_root"

    async def get(self, request: web.Request) -> web.StreamResponse:
        return _serve(request, "index.html")


class MegaHomeAppView(_MegaHomeView):
    """Everything else under the prefix: bundle files.

    ⚠ `path` is a positional argument, not something to dig out of the request:
    Home Assistant calls the handler as `handler(request, **request.match_info)`,
    so a signature without it raises `unexpected keyword argument 'path'` and the
    browser gets a bare 500. Found by running it.
    """

    url = f"{URL_PREFIX}/{{path:.*}}"
    name = "mega_home:app"

    async def get(self, request: web.Request, path: str) -> web.StreamResponse:
        return _serve(request, path)


def _serve(request: web.Request, relative: str) -> web.StreamResponse:
    """Serve one file of the ACTIVE bundle version.

    The active directory is asked for per request on purpose: that is what makes
    switching to a freshly downloaded interface a variable assignment instead of
    a Home Assistant restart.
    """
    coordinator = _coordinator(request.app["hass"])
    root = (
        coordinator.bundle.active_dir
        if coordinator is not None and coordinator.bundle is not None
        else BUNDLE_DIR
    )
    if not relative or relative.endswith("/"):
        relative = f"{relative}index.html"
    try:
        target = (root / relative).resolve()
        target.relative_to(root.resolve())
    except (ValueError, OSError):
        return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
    if not target.is_file():
        return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")

    # Кэш как у менеджера: хешированные бандлы неизменяемы, index.html — никогда.
    # Иначе браузер после обновления просит удалённые чанки.
    headers = (
        {"Cache-Control": "no-cache"}
        if target.name == "index.html"
        else {"Cache-Control": "public, max-age=31536000, immutable"}
    )
    return web.FileResponse(target, headers=headers)


def _find(items: list[dict[str, Any]], item_id: Any) -> dict[str, Any] | None:
    if not isinstance(item_id, str):
        return None
    return next((item for item in items if item.get("id") == item_id), None)


def _service_data(command: str, value: Any) -> dict[str, Any]:
    """Validate the one numeric argument a command may carry."""
    if command == "set_brightness":
        return {"brightness_pct": _number(value, 0, 100)}
    if command == "set_position":
        return {"position": _number(value, 0, 100)}
    if command == "set_temperature":
        return {"temperature": _number(value, 5, 40)}
    if command == "set_mode":
        return {"hvac_mode": str(value or "")}
    return {}


def _number(value: Any, low: int, high: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Значение должно быть от {low} до {high}") from err
    if not low <= parsed <= high:
        raise ValueError(f"Значение должно быть от {low} до {high}")
    return parsed


def _entity_view(tile: dict[str, Any], state: State | None) -> dict[str, Any]:
    """Project one Home Assistant state into what the app's screens read.

    ⚠ This is the same projection the manager does in smart-home-view.util.ts.
    The two live in different repositories on purpose (the whole point of the
    phase is that the manager is out of the runtime path), so the shape is a
    contract: change it here and there in the same breath.
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
