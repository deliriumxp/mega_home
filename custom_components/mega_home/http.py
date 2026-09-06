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

from aiohttp import web

from homeassistant.components.http import HomeAssistantView, StaticPathConfig
from homeassistant.core import HomeAssistant

from .const import (
    DOMAIN,
    LOGGER,
    URL_API,
    URL_ICONS,
    URL_PREFIX,
)
from . import ops
from .bundle import PACKAGED_DIR
from .coordinator import MegaHomeCoordinator
from .events import StateStream
from .photos import JPEG_MAGIC, MAX_PHOTO_BYTES

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
        MegaHomeEventsView,
        MegaHomeCommandView,
        MegaHomeScenarioView,
        MegaHomePhotosView,
        MegaHomePhotoView,
        MegaHomeStockPhotoView,
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

    # ⚠ Сами операции живут в `ops.py`, а не здесь: тот же код обслуживает
    # запрос жильца, пришедший СНАРУЖИ через менеджер по живому каналу
    # (`link.py`). Копия правил на каждый транспорт разъехалась бы.
    async def run(self, request: web.Request, op: str) -> web.Response:
        hass: HomeAssistant = request.app["hass"]
        try:
            return self.json(await ops.run(hass, _coordinator(hass), op, None))
        except ops.OpError as err:
            return self.json_message(err.message, err.status)

    async def run_async(self, request: web.Request, op: str) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Некорректный запрос", HTTPStatus.BAD_REQUEST)
        hass: HomeAssistant = request.app["hass"]
        try:
            return self.json(await ops.run(hass, _coordinator(hass), op, payload))
        except ops.OpError as err:
            return self.json_message(err.message, err.status)


class MegaHomeConfigView(_MegaHomeView):
    """The cached home config: floors, rooms, tiles, scenarios."""

    url = f"{URL_API}/config"
    name = "api:mega_home:config"

    async def get(self, request: web.Request) -> web.Response:
        return await self.run(request, "config")


class MegaHomeStatesView(_MegaHomeView):
    """Current states of every tile, read straight from this Home Assistant."""

    url = f"{URL_API}/states"
    name = "api:mega_home:states"

    async def get(self, request: web.Request) -> web.Response:
        return await self.run(request, "states")


class MegaHomeEventsView(_MegaHomeView):
    """Live states: one Server-Sent Events stream per open app.

    ⚠ Заменяет опрос `api/states` раз в 3 с. Внутри дома опрашивать нечего:
    Home Assistant сам отдаёт нам каждое изменение состояния, и плитка обязана
    меняться тогда же, когда щёлкнуло реле, а не на следующем тике.
    """

    url = f"{URL_API}/events"
    name = "api:mega_home:events"

    async def get(self, request: web.Request) -> web.StreamResponse:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        return await StateStream(hass, coordinator).run(request)


class MegaHomeCommandView(_MegaHomeView):
    """One command for one tile, mapped onto a Home Assistant service call."""

    url = f"{URL_API}/command"
    name = "api:mega_home:command"

    async def post(self, request: web.Request) -> web.Response:
        return await self.run_async(request, "command")


class MegaHomeScenarioView(_MegaHomeView):
    """Run one scenario (a Home Assistant script)."""

    url = f"{URL_API}/scenario"
    name = "api:mega_home:scenario"

    async def post(self, request: web.Request) -> web.Response:
        return await self.run_async(request, "scenario")


class MegaHomePhotosView(_MegaHomeView):
    """Which rooms have a background photo, and what version it is."""

    url = f"{URL_API}/photos"
    name = "api:mega_home:photos"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        rooms = [
            room["id"] for room in coordinator.data.get("rooms", []) if room.get("id")
        ]
        versions = await hass.async_add_executor_job(coordinator.photos.versions, rooms)
        return self.json({"photos": versions})


class MegaHomePhotoView(_MegaHomeView):
    """One room's background: read it, replace it, remove it.

    ⚠ `room` is a positional argument, not something to dig out of the request:
    Home Assistant calls handlers as `handler(request, **request.match_info)`.

    Only a room the current config knows can be written. That is the bound on
    this endpoint — without it anyone on the local network could fill the
    object's disk (there is no authentication yet, see the module docstring).
    """

    url = f"{URL_API}/photo/{{room}}"
    name = "api:mega_home:photo"

    async def get(self, request: web.Request, room: str) -> web.StreamResponse:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        target = coordinator.photos.path(room)
        if not await hass.async_add_executor_job(target.is_file):
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        # Адрес несёт версию файла (`?v=<mtime>`), поэтому картинку можно отдать
        # неизменяемой: сменилось фото — сменился адрес.
        return web.FileResponse(
            target, headers={"Cache-Control": "public, max-age=31536000, immutable"}
        )

    async def post(self, request: web.Request, room: str) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        if not _room_exists(coordinator.data, room):
            return self.json_message("Комната не найдена", HTTPStatus.NOT_FOUND)
        # Размер проверяется по заголовку ДО чтения тела: иначе четыре мегабайта
        # ограничения превращаются в столько памяти, сколько прислали.
        if (request.content_length or 0) > MAX_PHOTO_BYTES:
            return self.json_message("Фото слишком большое", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        payload = await request.read()
        if len(payload) > MAX_PHOTO_BYTES:
            return self.json_message("Фото слишком большое", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        if not payload.startswith(JPEG_MAGIC):
            # Приложение всегда пережимает снимок в JPEG само, так что сюда
            # попадает либо чужой клиент, либо оборванная загрузка.
            return self.json_message("Ожидается фотография JPEG", HTTPStatus.BAD_REQUEST)

        hass: HomeAssistant = request.app["hass"]
        try:
            version = await hass.async_add_executor_job(
                coordinator.photos.save, room, payload
            )
        except OSError as err:
            LOGGER.warning("Could not store the photo of room %s: %s", room, err)
            return self.json_message(
                "Дом не смог сохранить фото", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        return self.json({"accepted": True, "version": version})

    async def delete(self, request: web.Request, room: str) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        removed = await hass.async_add_executor_job(coordinator.photos.delete, room)
        if not removed:
            return self.json_message("Фото не найдено", HTTPStatus.NOT_FOUND)
        return self.json({"accepted": True})


def _room_exists(config: dict[str, Any], room_id: str) -> bool:
    return any(room.get("id") == room_id for room in config.get("rooms", []))


class MegaHomeStockPhotoView(_MegaHomeView):
    """The INSTALLER's background for one room, mirrored from the manager.

    ⚠ Версию берём ИЗ КОНФИГА, а не из адреса: `?v=` в адресе — метка кэша для
    браузера, и доверять ей как имени файла значило бы отдавать по чужой ссылке
    то, чего в конфиге уже нет. Конфиг тут единственный источник правды: какая
    заготовка у комнаты сейчас, ту дом и показывает.
    """

    url = f"{URL_API}/stock-photo/{{room}}"
    name = "api:mega_home:stock-photo"

    async def get(self, request: web.Request, room: str) -> web.StreamResponse:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        version = _stock_version(coordinator.data, room)
        if not version:
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        hass: HomeAssistant = request.app["hass"]
        target = coordinator.stock_photos.path(room, version)
        if not await hass.async_add_executor_job(target.is_file):
            # Конфиг заготовку обещает, а файла ещё нет: синхронизация не дошла
            # (дом только что поднялся, менеджер был недоступен). Это не ошибка
            # приложения — оно просто нарисует градиент до следующего опроса.
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        return web.FileResponse(
            target, headers={"Cache-Control": "public, max-age=31536000, immutable"}
        )


def _stock_version(config: dict[str, Any], room_id: str) -> str | None:
    for room in config.get("rooms", []):
        if room.get("id") == room_id:
            version = room.get("photoVersion")
            return version if isinstance(version, str) and version else None
    return None


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


