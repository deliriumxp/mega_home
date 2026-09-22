"""The HTTP surface the resident app talks to.

Everything the app needs is served by Home Assistant itself, under one prefix:
the bundle as static files, the config from the local cache, and states and
commands straight from `hass`. The manager takes no part at runtime.

⚠ Локальный контур БЕЗ аутентификации — решение заказчика 2026-09-20
(`docs/local-ha-app.md` менеджера): досягаемость у него та же, что у любого
устройства в Wi-Fi объекта. Границы — в составе путей и потолках, а не в учётке.

⚠ Пути жильца разбирает ОДИН маршрутизатор — `relay_api.dispatch`, тот же, что
у переноса через менеджера. Здесь только дверь: запрос aiohttp → `dispatch` →
ответ aiohttp. Своих проверок у двери нет и не будет: две копии уже расходились.
"""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path

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
from .core import ops, relay_api, stream
from .core.api import ManagerError
from .coordinator import MegaHomeCoordinator
from .core.events import StateStream

# Сколько ждём отклика на ping удержания: уснувший телефон сокет не закрывает,
# и без этого сессии к устройствам жили бы до своего часа (`stream.py`).
WS_HEARTBEAT_S = 25
# Кадр крупнее мегабайта — не наш случай: тело запроса едет в описании, а данные
# идут кусками. Огромный кадр рвёт сокет громко, а не теряется молча.
WS_MAX_FRAME = 1024 * 1024

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
    """Base view: the local contour has no auth (see the module docstring).

    The app never gets a Home Assistant token: states and service calls are made
    by this integration from inside Python, and only our own shape goes out.
    """

    requires_auth = False


class _RoutedView(_MegaHomeView):
    """Дверь, чьи запросы разбирает общий маршрутизатор (`relay_api.dispatch`).

    ⚠ Параметры пути (`{room}`, `{tile}`, `{key}`) Home Assistant передаёт
    аргументами (`handler(request, **match_info)`), но маршрутизатор берёт путь
    целиком из `raw_path`, как перенос берёт его из кадра — без второго разбора.
    """

    async def get(self, request: web.Request, **_match: str) -> web.StreamResponse:
        return await self.route(request)

    async def post(self, request: web.Request, **_match: str) -> web.StreamResponse:
        return await self.route(request)

    async def delete(self, request: web.Request, **_match: str) -> web.StreamResponse:
        return await self.route(request)

    async def route(self, request: web.Request) -> web.StreamResponse:
        # Размер — по заголовку ДО чтения тела: иначе потолок превращается в
        # столько памяти, сколько прислали.
        if (request.content_length or 0) > relay_api.MAX_REQUEST_BYTES:
            return self.json_message("Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        body = await request.read() if request.can_read_body else b""
        path = request.raw_path.split("?", 1)[0][len(URL_PREFIX) + 1 :]
        try:
            status, content_type, raw, cache = await relay_api.dispatch(
                _coordinator(request.app["hass"]), request.method, path, body, dict(request.query)
            )
        except ops.OpError as err:
            return self.json_message(err.message, err.status)
        headers = {"Content-Type": content_type, **({"Cache-Control": cache} if cache else {})}
        if isinstance(raw, Path):
            return web.FileResponse(raw, headers=headers)
        return web.Response(status=status, body=raw, headers=headers)


class MegaHomeConfigView(_RoutedView):
    """Состав дома из кэша плюс паспорт самого дома (`ops.config`)."""

    url = f"{URL_API}/config"
    name = "api:mega_home:config"


class MegaHomeStatesView(_RoutedView):
    """Current states of every tile, read straight from this Home Assistant."""

    url = f"{URL_API}/states"
    name = "api:mega_home:states"


class MegaHomeCommandView(_RoutedView):
    """One command for one tile, mapped onto a Home Assistant service call."""

    url = f"{URL_API}/command"
    name = "api:mega_home:command"


class MegaHomeScenarioView(_RoutedView):
    """Run one scenario (a Home Assistant script)."""

    url = f"{URL_API}/scenario"
    name = "api:mega_home:scenario"


class MegaHomeIntercomView(_RoutedView):
    """Действие жильца над идущим вызовом домофонии (сегодня — «отклонить»)."""

    url = f"{URL_API}/intercom"
    name = "api:mega_home:intercom"


class MegaHomePhotosView(_RoutedView):
    """Что имеет фон — комнаты и ОТДЕЛЬНЫЕ ПЛИТКИ (`tile:<id>`) — и какой версии."""

    url = f"{URL_API}/photos"
    name = "api:mega_home:photos"


class MegaHomePhotoView(_RoutedView):
    """Один фон: прочитать, заменить, снять.

    ⚠ Имя параметра `room` осталось прежним (маршрут менять нельзя — по нему
    ходят уже работающие дома), но значением приходит КЛЮЧ ФОНА: комната
    (`kitchen`) или плитка (`tile:light.kitchen_main`).
    """

    url = f"{URL_API}/photo/{{room}}"
    name = "api:mega_home:photo"


class MegaHomeCropsView(_RoutedView):
    """Какие камеры имеют СВОЙ участок кадра, подправленный жильцом, и какой."""

    url = f"{URL_API}/crops"
    name = "api:mega_home:crops"


class MegaHomeCropView(_RoutedView):
    """Один участок кадра камеры, который подправил жилец: записать, снять."""

    url = f"{URL_API}/crop/{{tile}}"
    name = "api:mega_home:crop"


class MegaHomeAssetView(_RoutedView):
    """ONE route for every file the manager hands to this home (`assets.py`)."""

    url = f"{URL_API}/asset/{{key:.+}}"
    name = "api:mega_home:asset"


class MegaHomeCameraFrameView(_RoutedView):
    """Один кадр камеры источника плитки — часть B (источник состояний), не вендор.

    ⚠ Путь один и тот же с обеих сторон намеренно, хотя дома кадр есть и у
    самого HA (`/api/camera_proxy/...`): снаружи у приложения нет ни одного
    адреса Home Assistant, и расхождение поверхностей — та болезнь, ради лечения
    которой заведён перенос.
    """

    url = f"{URL_API}/camera-frame/{{tile}}"
    name = "api:mega_home:camera-frame"


class MegaHomeDeviceEventsView(_RoutedView):
    """Лента событий устройства из хранилища на диске — часть F, не вендор."""

    url = f"{URL_API}/device-events"
    name = "api:mega_home:device-events"


class MegaHomeEventFileView(_RoutedView):
    """Вложение к событию устройства (кадр гостя) — хранилище на объекте, часть F."""

    url = f"{URL_API}/event-file"
    name = "api:mega_home:event-file"


class MegaHomeDevLogView(_RoutedView):
    """Лог разработки от приложения внутри дома — в очередь дома к менеджеру."""

    url = f"{URL_API}/dev-log"
    name = "api:mega_home:dev-log"


class MegaHomeConnectView(_RoutedView):
    """Единственный контракт транспорта наружу (`connect.py`).

    Три формы ОДНОГО пути: данные коду (`POST`), ресурс тегу (`GET`), кадры
    сессии (`GET` с Upgrade). Меняется способ потребить ответ, описание вызова
    одно — список маршрутов от этого не растёт, замок считает пути.
    """

    url = f"{URL_API}/connect"
    name = "api:mega_home:connect"

    async def get(self, request: web.Request, **_match: str) -> web.StreamResponse:
        """⚠ Удержание внутри дома — без аутентификации, как весь локальный
        контур: досягаемость у него та же, что у `POST api/connect`. Снаружи эта
        же форма идёт переносом менеджера и закрыта сессией жильца."""
        if request.headers.get("Upgrade", "").lower() == "websocket":
            socket = web.WebSocketResponse(heartbeat=WS_HEARTBEAT_S, max_msg_size=WS_MAX_FRAME)
            await socket.prepare(request)
            await stream.serve(socket)
            return socket
        return await self.route(request)


class MegaHomeEventsView(_MegaHomeView):
    """Live states: one Server-Sent Events stream per open app.

    ⚠ Заменяет опрос `api/states` раз в 3 с. Внутри дома опрашивать нечего:
    Home Assistant сам отдаёт нам каждое изменение состояния, и плитка обязана
    меняться тогда же, когда щёлкнуло реле, а не на следующем тике.
    """

    url = f"{URL_API}/events"
    name = "api:mega_home:events"

    async def get(self, request: web.Request) -> web.StreamResponse:
        coordinator = _coordinator(request.app["hass"])
        if coordinator is None or not coordinator.data:
            return self.json_message(
                "Дом ещё не синхронизирован с менеджером", HTTPStatus.SERVICE_UNAVAILABLE
            )
        return await StateStream(coordinator).run(request)


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
        coordinator = _coordinator(request.app["hass"])
        if coordinator is None or not coordinator.data:
            return self.json_message(
                "Дом ещё не синхронизирован с менеджером", HTTPStatus.SERVICE_UNAVAILABLE
            )
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


# Запасной воркер — пока бандла нет. Его работа — ЗАНЯТЬ scope `/mega-home/`.
FALLBACK_SERVICE_WORKER = (
    "self.addEventListener('install', () => self.skipWaiting());\n"
    "self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));\n"
)
SERVICE_WORKER_HEADERS = {
    # Застрявшая копия воркера — это застрявший scope.
    "Cache-Control": "no-store",
    # Scope шире собственного каталога нам не нужен, но заявить его явно
    # дешевле, чем потом гадать, почему регистрация отклонена.
    "Service-Worker-Allowed": f"{URL_PREFIX}/",
}


class MegaHomeServiceWorkerView(_MegaHomeView):
    """`/mega-home/sw.js` — воркер ИЗ БАНДЛА, забирающий scope у воркера HA.

    ⚠ Из бандла, а не строкой отсюда (долг «ждёт релиза дома» №2): воркер
    бандла умеет запасную страницу вместо мёртвой (`navigate`) и push, а строка
    в коде дома менялась только релизом интеграции — открытие PWA во время
    перезапуска HA давало страницу ошибки браузера, из которой без адресной
    строки не выйти. Строка осталась ЗАПАСНОЙ: до первой загрузки бандла.
    """

    url = f"{URL_PREFIX}/sw.js"
    name = "mega_home:sw"

    async def get(self, request: web.Request) -> web.StreamResponse:
        hass: HomeAssistant = request.app["hass"]
        root = _active_dir(request)
        worker = root / "sw.js" if root is not None else None
        if worker is not None and await hass.async_add_executor_job(worker.is_file):
            return web.FileResponse(
                worker, headers={"Content-Type": "text/javascript", **SERVICE_WORKER_HEADERS}
            )
        return web.Response(
            text=FALLBACK_SERVICE_WORKER,
            headers={"Content-Type": "text/javascript", **SERVICE_WORKER_HEADERS},
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


def _active_dir(request: web.Request) -> Path | None:
    coordinator = _coordinator(request.app["hass"])
    return coordinator.bundle.active_dir if coordinator and coordinator.bundle else None


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
    root = _active_dir(request)
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
    MegaHomeIntercomView,
    MegaHomeConnectView,
    MegaHomePhotosView,
    MegaHomePhotoView,
    MegaHomeCropsView,
    MegaHomeCropView,
    MegaHomeAssetView,
    MegaHomeCameraFrameView,
    MegaHomeDeviceEventsView,
    MegaHomeEventFileView,
    MegaHomeDevLogView,
    MegaHomeRelayView,
    MegaHomeServiceWorkerView,
    MegaHomeAppRootView,
    MegaHomeAppView,
)
