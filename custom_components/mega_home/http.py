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
from typing import Any

from aiohttp import web

from homeassistant.components.http import HomeAssistantView, StaticPathConfig
from homeassistant.core import HomeAssistant

from .const import (
    DOMAIN,
    LOGGER,
    TILE_PHOTO_PREFIX,
    URL_API,
    URL_ICONS,
    URL_PREFIX,
)
from . import ops
from .api import ManagerError
from .coordinator import MegaHomeCoordinator
from .events import StateStream
from .photos import (
    JPEG_MAGIC,
    MAX_PHOTO_BYTES,
    photo_key_known,
    photo_keys,
    stock_version,
)

# ⚠ Копии интерфейса в релизе НЕТ (2026-09-06): пока бандл не скачан, отдаём эту
# страницу. Первый запуск считаем онлайн — а взамен релиз интеграции перестал
# весить ~700 КБ собранного фронтенда и зависеть от него.
# Перезагружается сама: бандл приезжает фоном, и жильцу нечего нажимать.
PLACEHOLDER = (
    "<!doctype html><html lang=ru><head><meta charset=utf-8>"
    "<meta name=viewport content='width=device-width,initial-scale=1'>"
    "<meta http-equiv=refresh content=5>"
    "<title>Mega Home</title>"
    "<style>html{color-scheme:light dark}body{margin:0;min-height:100vh;display:flex;"
    "align-items:center;justify-content:center;font:16px/1.5 system-ui,sans-serif;"
    "text-align:center;padding:24px}</style></head>"
    "<body><p>Подключаюсь к менеджеру…<br>Интерфейс загрузится сам.</p></body></html>"
)


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
        # ⚠ Общий канал: один маршрут на любой файл и одна розетка на любой
        # запрос-ответ. Оба заведены ради того, чтобы новая функция не стоила
        # выпуска этой интеграции (`assets.py`).
        MegaHomeAssetView,
        MegaHomeRelayView,
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
    """What has a background photo, and what version it is.

    ⚠ Не только комнаты. Фон бывает и у ОДНОЙ ПЛИТКИ: инсталлятор снимает в
    квартире сам прибор — группу света, телевизор, шторы — и ставит снимок
    фоном его плитки (приложение, долгое удержание). Ключ плитки — `tile:<id>`,
    и разведён приставкой не для красоты: идентификаторы плиток и комнат
    приходят из разных источников и совпасть могут запросто.

    Перечислять их обязано именно ЗДЕСЬ: приложение спрашивает «что вообще
    задано» одним запросом на открытие, и плитка, которой нет в этом списке,
    покажется без фона, даже если файл на диске лежит.
    """

    url = f"{URL_API}/photos"
    name = "api:mega_home:photos"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        versions = await hass.async_add_executor_job(
            coordinator.photos.versions, photo_keys(coordinator.data)
        )
        return self.json({"photos": versions})


class MegaHomePhotoView(_MegaHomeView):
    """One background: read it, replace it, remove it.

    ⚠ `room` is a positional argument, not something to dig out of the request:
    Home Assistant calls handlers as `handler(request, **request.match_info)`.
    Имя параметра осталось прежним (маршрут менять нельзя — по нему ходят уже
    работающие дома), но значением приходит КЛЮЧ ФОНА: комната (`kitchen`) или
    плитка (`tile:light.kitchen_main`).

    Only a key the current config knows can be written — комната из состава или
    плитка из него же. That is the bound on this endpoint: без него любой в
    локальной сети забил бы диск объекта файлами (аутентификации у HTTP-контура
    пока нет, см. docstring модуля). Хранилище само по себе ключа не боится —
    имя файла это хеш (`photos.py`), — но неограниченный НАБОР ключей боится.
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
        if not photo_key_known(coordinator.data, room):
            return self.json_message("Комната или плитка не найдена", HTTPStatus.NOT_FOUND)
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
        version = stock_version(coordinator.data, room)
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


class MegaHomeAssetView(_MegaHomeView):
    """ONE route for every file the manager hands to this home.

    ⚠ The route knows nothing about what it serves: key, version and content
    type all come from the manifest in the cached config (`assets.py`). That is
    what keeps a new kind of file — a sound, a font, a floor plan — from costing
    a release of this integration.

    ⚠ The version comes from the CONFIG, never from `?v=` in the URL: the query
    is a cache marker for the browser, and trusting it as a file name would mean
    serving, by an old link, what the config no longer names.
    """

    url = f"{URL_API}/asset/{{key:.+}}"
    name = "api:mega_home:asset"

    async def get(self, request: web.Request, key: str) -> web.StreamResponse:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        entry = (coordinator.data.get("assets") or {}).get(key)
        if not isinstance(entry, dict) or not isinstance(entry.get("v"), str):
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        hass: HomeAssistant = request.app["hass"]
        target = coordinator.assets.path(key, entry["v"])
        if not await hass.async_add_executor_job(target.is_file):
            # Манифест файл обещает, а синхронизация ещё не дошла (дом только
            # поднялся, менеджер был недоступен). Это не ошибка приложения.
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        content_type = entry.get("type")
        return web.FileResponse(
            target,
            headers={
                "Cache-Control": "public, max-age=31536000, immutable",
                "Content-Type": content_type
                if isinstance(content_type, str) and content_type
                else "application/octet-stream",
            },
        )


class MegaHomeRelayView(_MegaHomeView):
    """Ask the MANAGER something on behalf of the app, and return the answer.

    ⚠ The home does not read the question and does not interpret the answer —
    it carries them. The app inside a flat can reach nothing but this
    integration, so without a route like this every future request/response
    feature (the resident's AI chat first of all) would cost a release of this
    integration and an update on every object.

    ⚠ The bound is the manager: the token belongs to the object, the endpoint is
    a single one (`/inbound/home-config/relay`), and what may be asked through
    it is decided there, not here. The manager answers 501 while nothing is
    plugged in — an honest diagnosis instead of silence.
    """

    url = f"{URL_API}/relay"
    name = "api:mega_home:relay"

    async def post(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Ожидается JSON", HTTPStatus.BAD_REQUEST)
        if not isinstance(payload, dict):
            return self.json_message("Ожидается объект JSON", HTTPStatus.BAD_REQUEST)
        try:
            status, body = await coordinator.client.async_relay(payload)
        except ManagerError as err:
            # Менеджер недоступен — это нормальное состояние объекта без
            # интернета, и приложение обязано услышать именно это.
            LOGGER.warning("Relay to the manager failed: %s", err)
            return self.json_message("Менеджер недоступен", HTTPStatus.BAD_GATEWAY)
        return self.json(body if isinstance(body, dict) else {"answer": body}, status)



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
    """Serve one file of the ACTIVE bundle version, or the placeholder.

    The active directory is asked for per request on purpose: that is what makes
    switching to a freshly downloaded interface a variable assignment instead of
    a Home Assistant restart. Until the first download there is no directory at
    all (the release carries no copy), and the page is the placeholder above.
    """
    coordinator = _coordinator(request.app["hass"])
    root = coordinator.bundle.active_dir if coordinator and coordinator.bundle else None
    if root is None:
        # Бандл ещё не скачан. Заглушку отдаём только на саму страницу: запрос
        # файла бандла должен остаться честным 404, иначе браузер получит HTML
        # вместо js и упадёт с ошибкой разбора вместо понятного экрана.
        if relative in ("", "index.html") or relative.endswith("/"):
            return web.Response(
                text=PLACEHOLDER,
                content_type="text/html",
                headers={"Cache-Control": "no-cache"},
            )
        return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
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


