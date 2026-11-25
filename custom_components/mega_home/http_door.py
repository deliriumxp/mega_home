"""Локальная дверь: доступ устройства и передача запроса общему маршрутизатору."""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import web
from homeassistant.components.http import HomeAssistantView

from .core import local_keys, ops, relay_api
from .core.const import DOMAIN, URL_PREFIX

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .coordinator import MegaHomeCoordinator


# Ответы API дома — ДАННЫЕ, а не страницы: открытый по ссылке, ответ не исполняет скрипт в origin
# HA. Ресурс `api/connect?req=…` несёт чужое тело с чужим типом (устройство LAN), и без этих
# заголовков HTML или SVG устройства стал бы страницей HA со всеми его ключами.
CONTAINED = {"X-Content-Type-Options": "nosniff", "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; sandbox"}


def _coordinator(hass: HomeAssistant) -> MegaHomeCoordinator | None:
    """Return the coordinator of the loaded entry, if there is one.

    One Home Assistant serves one home, so the first loaded entry is the home.
    """
    for entry in hass.config_entries.async_loaded_entries(DOMAIN):
        return entry.runtime_data
    return None


class _MegaHomeView(HomeAssistantView):
    """Base view: the local contour has no Home Assistant auth (see the module docstring).

    The app never gets a Home Assistant token: states and service calls are made
    by this integration from inside Python, and only our own shape goes out.
    """

    requires_auth = False

    def admit(self, request: web.Request) -> tuple[web.Response | None, str | None, bool]:
        """Ключ устройства (`local_keys.py`): `(отказ или None, ключ, положить ли в куку)`."""
        coordinator = _coordinator(request.app["hass"])
        keys = getattr(coordinator, "local_keys", None)
        if keys is None:
            return None, None, False
        allowed, key, fresh = keys.admit(
            request.cookies.get(local_keys.COOKIE), request.headers.get(local_keys.HEADER)
        )
        if not allowed:
            # Свой экран бандла (`outage: local-closed`): «войдите через интернет» кнопкой на
            # облачный вход дома, без «Повторить» — повтор здесь ничего не даст.
            refusal = web.json_response(
                {
                    "message": "Это устройство не подключено к дому. Войдите в приложение через интернет "
                    "— после входа оно заработает и по Wi-Fi дома.",
                    "code": "local-closed",
                    "outage": "local-closed",
                    **({"cloudUrl": keys.cloud} if keys.cloud else {}),
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return refusal, None, False
        return None, key, fresh


def _keyed(response: web.StreamResponse, key: str | None, fresh: bool) -> web.StreamResponse:
    """Ключ — в ответ (копия для хранилища браузера) и, если новый, в куку.

    ⚠ Кука, а не только заголовок: её несут и `<img>` ресурса `api/connect?req=`,
    и апгрейд сокета, и поток `api/events` — им заголовок не поставить.
    """
    if key:
        response.headers[local_keys.HEADER] = key
    if key and fresh:
        response.set_cookie(
            local_keys.COOKIE, key, max_age=local_keys.COOKIE_MAX_AGE,
            path=URL_PREFIX, httponly=True, samesite="Lax",
        )
    return response


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
        refusal, key, fresh = self.admit(request)
        if refusal is not None:
            return refusal
        # Размер — по заголовку ДО чтения тела: иначе потолок превращается в
        # столько памяти, сколько прислали.
        if (request.content_length or 0) > relay_api.MAX_REQUEST_BYTES:
            return self.json_message("Запрос слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        body = await request.read() if request.can_read_body else b""
        path = request.raw_path.split("?", 1)[0][len(URL_PREFIX) + 1 :]
        coordinator = _coordinator(request.app["hass"])
        keys = getattr(coordinator, "local_keys", None)
        # Каталог доступен через открытую дверь. Привязкой управляет ключ учётки.
        same_origin = request.headers.get("Sec-Fetch-Site") not in ("cross-site", "same-site")
        content_type = request.headers.get("Content-Type", "").partition(";")[0].strip().lower()
        access = None
        if request.method == "POST" and same_origin and content_type == "application/json":
            # Пропуск из облака подтверждает учётку, даже если обычному управлению хватает старой куки.
            offered = request.headers.get(local_keys.HEADER, "").split(",")[:2]
            can_manage = keys and any(keys.is_resident(candidate.strip()) for candidate in [key or "", *offered])
            access = "manage" if can_manage else "read"
        try:
            status, content_type, raw, cache = await relay_api.dispatch(
                coordinator, request.method, path, body, dict(request.query), resident_access=access,
            )
        except ops.OpError as err:
            # `message` — для приложений, не знающих `detail` (сырой причины от источника).
            refusal = {"message": err.message, **({"detail": err.detail} if err.detail else {})}
            return _keyed(self.json(refusal, err.status), key, fresh)
        headers = {"Content-Type": content_type, **({"Cache-Control": cache} if cache else {}), **CONTAINED}
        if isinstance(raw, Path):
            return _keyed(web.FileResponse(raw, headers=headers), key, fresh)
        return _keyed(web.Response(status=status, body=raw, headers=headers), key, fresh)
