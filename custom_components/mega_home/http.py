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

from aiohttp import web

from homeassistant.components.http import HomeAssistantView, StaticPathConfig
from homeassistant.core import HomeAssistant

from .core.const import (
    DOMAIN,
    LOGGER,
    URL_API,
    URL_ICONS,
    URL_PREFIX,
)
from .core import ops
from .core.api import ManagerError
from .coordinator import MegaHomeCoordinator
from .core.crops import crop_key_known, crop_keys, crop_value_valid, MAX_CROP_BYTES
from .core.events import StateStream
from .core.imaging import asset_file, photo_file
from .core.photos import (
    JPEG_MAGIC,
    MAX_PHOTO_BYTES,
    photo_key_known,
    photo_keys,
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
    for view in VIEWS:
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
            return self.json(await ops.run(_coordinator(hass), op, None))
        except ops.OpError as err:
            return self.json_message(err.message, err.status)

    async def run_async(self, request: web.Request, op: str) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Некорректный запрос", HTTPStatus.BAD_REQUEST)
        hass: HomeAssistant = request.app["hass"]
        try:
            return self.json(await ops.run(_coordinator(hass), op, payload))
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
        return await StateStream(coordinator).run(request)


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


class MegaHomeConnectView(_MegaHomeView):
    """Единственный контракт транспорта наружу (`connect.py`, `ops.py`).

    ⚠ Тот же код, что у операции канала: локальная дверь и перенос через
    менеджера обязаны отвечать одинаково (`docs/plan-thin-gateway.md`).
    """

    url = f"{URL_API}/connect"
    name = "api:mega_home:connect"

    async def post(self, request: web.Request) -> web.Response:
        return await self.run_async(request, "connect")


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
        # `imaging` — дом сам готовит варианты фото (`imaging.py`). Приложение
        # узнаёт это ОТВЕТОМ, а не номером версии: без флага оно рисует вид
        # фильтрами CSS, как раньше.
        return self.json({"photos": versions, "imaging": True})


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
        # Query может просить готовый вариант (`?w=1080&blur=14`, `imaging.py`).
        target = await photo_file(coordinator.env, coordinator, room, request.query)
        if target is None:
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


class MegaHomeCropsView(_MegaHomeView):
    """What camera tiles have their OWN crop, and what it is.

    ⚠ Тот же приём, что `MegaHomePhotosView`: приложение спрашивает «что вообще
    подправлено» одним запросом на открытие и мешает ответ поверх кадра из
    конфига (`tiles[].crop`) — сам дом это смешение не делает и делать не
    должен (`docs/local-ha-app.md`): интеграция остаётся тонкой, толкование
    живёт в бандле, как и у остального состояния.
    """

    url = f"{URL_API}/crops"
    name = "api:mega_home:crops"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        crops = await hass.async_add_executor_job(
            coordinator.crops.all, crop_keys(coordinator.data)
        )
        return self.json({"crops": crops})


class MegaHomeCropView(_MegaHomeView):
    """Один участок кадра камеры, который подправил жилец: записать, снять.

    ⚠ Писать можно только КАМЕРУ из ТЕКУЩЕГО состава — та же дисциплина, что у
    `MegaHomePhotoView`, и по той же причине: без неё любой в локальной сети
    забил бы диск объекта файлами (аутентификации у HTTP-контура пока нет, см.
    docstring модуля). Кадр из менеджера при этом остаётся ЗАГОТОВКОЙ: снял
    жилец свою правку — вернулся он, а не пустой центр кадра (`tile-crops.ts`
    в менеджере ведёт то же самое правило для фона плитки).
    """

    url = f"{URL_API}/crop/{{tile}}"
    name = "api:mega_home:crop"

    async def post(self, request: web.Request, tile: str) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        if not crop_key_known(coordinator.data, tile):
            return self.json_message("Камера не найдена", HTTPStatus.NOT_FOUND)
        if (request.content_length or 0) > MAX_CROP_BYTES:
            return self.json_message(
                "Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE
            )
        try:
            payload = await request.json()
        except ValueError:
            return self.json_message("Некорректный запрос", HTTPStatus.BAD_REQUEST)
        if not crop_value_valid(payload):
            return self.json_message("Некорректный участок кадра", HTTPStatus.BAD_REQUEST)

        hass: HomeAssistant = request.app["hass"]
        try:
            await hass.async_add_executor_job(coordinator.crops.save, tile, payload)
        except OSError as err:
            LOGGER.warning("Could not store the crop of tile %s: %s", tile, err)
            return self.json_message(
                "Дом не смог сохранить кадр", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        return self.json({"accepted": True, "crop": payload})

    async def delete(self, request: web.Request, tile: str) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        hass: HomeAssistant = request.app["hass"]
        removed = await hass.async_add_executor_job(coordinator.crops.delete, tile)
        if not removed:
            return self.json_message("Кадр не найден", HTTPStatus.NOT_FOUND)
        return self.json({"accepted": True})


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
        found = await asset_file(coordinator.env, coordinator, key, request.query)
        if found is None:
            return web.Response(status=HTTPStatus.NOT_FOUND, text="404: Not Found")
        target, content_type = found
        return web.FileResponse(
            target,
            headers={
                "Cache-Control": "public, max-age=31536000, immutable",
                "Content-Type": content_type,
            },
        )


class MegaHomeCameraFrameView(_MegaHomeView):
    """Один кадр камеры источника плитки — часть B (источник состояний), не вендор.

    ⚠ Есть и здесь, хотя ДОМА приложение берёт кадр напрямую у Home Assistant
    (`/api/camera_proxy/...`, тот же origin, дешевле на один поход к камере).
    Путь один и тот же с обеих сторон намеренно: расхождение поверхностей —
    та самая болезнь, ради лечения которой заведён перенос (`relay_api.py`).
    Снаружи у приложения нет ни одного адреса Home Assistant, поэтому без этой
    двери плитка камеры вне дома остаётся без картинки.
    """

    url = f"{URL_API}/camera-frame/{{tile}}"
    name = "api:mega_home:camera-frame"

    async def get(self, request: web.Request, tile: str) -> web.StreamResponse:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        try:
            content_type, body = await ops.camera_frame(coordinator, {"id": tile})
        except ops.OpError as err:
            return web.Response(status=err.status, text=err.message)

        # ⚠ Байты как есть, без base64: `ops.camera_frame` отдаёт их уже
        # сырыми — эта дверь не переносит запрос по каналу менеджера,
        # кодировать здесь было бы работой ради самой себя.
        return web.Response(
            body=body,
            headers={
                "Content-Type": content_type,
                # Кадр живой и короткий: закешированный постер показывал бы
                # прошлую минуту, но плитка обновляется опросом раз в 3 с.
                "Cache-Control": "private, max-age=3",
            },
        )


class MegaHomeDeviceEventsView(_MegaHomeView):
    """Лента событий устройства из хранилища на диске — часть F, не вендор.

    ⚠ Тот же обработчик, что у переноса (`relay_api._dispatch`,
    `ops.device_events`): приложение без менеджера обязано видеть историю
    устройства так же, как жилец у экрана дома (`docs/plan-thin-gateway.md`).
    """

    url = f"{URL_API}/device-events"
    name = "api:mega_home:device-events"

    async def get(self, request: web.Request) -> web.Response:
        coordinator, error = self.coordinator_or_error(request)
        if error is not None:
            return error
        assert coordinator is not None
        try:
            return self.json(ops.device_events(coordinator, dict(request.query)))
        except ops.OpError as err:
            return self.json_message(err.message, err.status)


class MegaHomeRelayView(_MegaHomeView):
    """Ask the MANAGER something on behalf of the app, and return the answer.

    ⚠ The home does not read the question and does not interpret the answer —
    it carries them. The app inside a flat can reach nothing but this
    integration, so without a route like this every future request/response
    feature (the resident's AI chat first of all) would cost a release of this
    integration and an update on every object.
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


# Наш собственный service worker. Не кэширует НИЧЕГО и не обязан: его работа —
# ЗАНЯТЬ scope `/mega-home/`.
SERVICE_WORKER = (
    "self.addEventListener('install', () => self.skipWaiting());\n"
    "self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));\n"
)


class MegaHomeServiceWorkerView(_MegaHomeView):
    """`/mega-home/sw.js` — воркер, забирающий scope у воркера Home Assistant."""

    url = f"{URL_PREFIX}/sw.js"
    name = "mega_home:sw"

    async def get(self, request: web.Request) -> web.StreamResponse:
        return web.Response(
            text=SERVICE_WORKER,
            headers={
                "Content-Type": "text/javascript",
                # Воркер меняется раз в никогда, но кэшировать его нельзя:
                # застрявшая копия — это застрявший scope.
                "Cache-Control": "no-store",
                # Scope шире собственного каталога нам не нужен, но заявить его
                # явно дешевле, чем потом гадать, почему регистрация отклонена.
                "Service-Worker-Allowed": f"{URL_PREFIX}/",
            },
        )


class MegaHomeAppRootView(_MegaHomeView):
    """The bare prefix: hand out the app itself — но по АДРЕСУ С ВЕРСИЕЙ В ПУТИ.

    ⚠ Голый `/mega-home/` уводит на `/mega-home/v/<версия бандла>/` — иначе
    браузер иногда отдаёт `index.html` из кэша чужого service worker'а, и он
    честно тянет старый `main-*.js` по хешированным именам.
    """

    url = URL_PREFIX
    extra_urls = [f"{URL_PREFIX}/"]
    name = "mega_home:app_root"

    async def get(self, request: web.Request) -> web.StreamResponse:
        version = _active_version(request)
        if version:
            return web.HTTPFound(
                f"{URL_PREFIX}/v/{version}/",
                headers={"Cache-Control": "no-store"},
            )
        return _serve(request, "index.html")


class MegaHomeAppView(_MegaHomeView):
    """Everything else under the prefix: bundle files.

    ⚠ `path` is a positional argument, not something to dig out of the request:
    Home Assistant calls the handler as `handler(request, **request.match_info)`,
    so a signature without it raises `unexpected keyword argument 'path'` and the
    browser gets a bare 500. Found by running it.

    ⚠ Префикс `v/<версия>/` СРЕЗАЕТСЯ здесь. Он существует только ради ключа
    кэша (см. `MegaHomeAppRootView`), в бандле такого каталога нет, а сами файлы
    приложение просит по-прежнему от `<base href="/mega-home/">`, то есть без
    него. Версию не сверяем: пришли со старой — отдадим текущий интерфейс, и это
    правильнее, чем 404 в лицо жильцу.
    """

    url = f"{URL_PREFIX}/{{path:.*}}"
    name = "mega_home:app"

    async def get(self, request: web.Request, path: str) -> web.StreamResponse:
        return _serve(request, _strip_version(path))


def _active_version(request: web.Request) -> str | None:
    """Имя каталога активного бандла — оно же версия в адресе."""
    coordinator = _coordinator(request.app["hass"])
    bundle = coordinator.bundle if coordinator else None
    if bundle is None or bundle.active_dir is None:
        return None
    return bundle.version


def _strip_version(path: str) -> str:
    """`v/<версия>/<файл>` → `<файл>`; голое `v/<версия>/` → сама страница."""
    if not path.startswith("v/"):
        return path
    rest = path[2:].split("/", 1)
    return rest[1] if len(rest) == 2 and rest[1] else "index.html"


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

    # Хешированные бандлы неизменяемы, `index.html` НЕ ХРАНИМ вовсе.
    headers = (
        {"Cache-Control": "no-store"}
        if target.name == "index.html"
        else {"Cache-Control": "public, max-age=31536000, immutable"}
    )
    return web.FileResponse(target, headers=headers)


# ⚠ СПИСОК РЕГИСТРИРУЕМЫХ ДВЕРЕЙ — модульной константой, а не выражением внутри
# функции, и это не стиль. Класс, ОПРЕДЕЛЁННЫЙ, но не попавший сюда, живёт в
# коде, проходит замок маршрутов и отвечает жильцу обычным 404. Теперь список
# читает и замок.
VIEWS: tuple[type[HomeAssistantView], ...] = (
    MegaHomeConfigView,
    MegaHomeStatesView,
    MegaHomeEventsView,
    MegaHomeCommandView,
    MegaHomeScenarioView,
    MegaHomeConnectView,
    MegaHomePhotosView,
    MegaHomePhotoView,
    MegaHomeCropsView,
    MegaHomeCropView,
    MegaHomeAssetView,
    MegaHomeCameraFrameView,
    MegaHomeDeviceEventsView,
    MegaHomeRelayView,
    MegaHomeServiceWorkerView,
    MegaHomeAppRootView,
    MegaHomeAppView,
)
