"""Долгосрочный токен Home Assistant по заданию менеджера — операция канала `ha-token`.

Зачем. Менеджеру нужен токен своего HA объекта (расписания, реестры, KNX-панель — служба
`ha`, `ha_api.py`), и раньше инсталлятор выпускал его руками в профиле HA и вставлял в
карточку. Теперь дом выпускает его сам тем же вызовом ядра, что и экран профиля: оба
обращения повторяют `websocket_create_long_lived_access_token`
(`homeassistant/components/auth/__init__.py`).

⚠ Операция доступна ТОЛЬКО каналу менеджера: она вызывается из `ops.run` по кадру канала
(`link.py`) и больше ниоткуда. Бандл жильца и локальный контур (`api/connect`, `relay_api`,
служба `ha` с `REFUSED_PATHS`/`REFUSED_FRAMES`) токены по-прежнему выпускать не могут —
иначе такой токен пережил бы любой отзыв доступа. Замок — `tests/test_ha_token.py`.

⚠ Модуль вне `core/`: `hass.auth` — свойство HA, то есть адаптера. В ядре только
диспетчеризация (`ops.py`, как у `host-unit`).

⚠ Токен в журнал не пишем: ни в ответ-лог, ни в текст ошибки.

⚠ У HA одно имя клиента — один долгоживущий токен (`async_create_refresh_token` бросает
`ValueError("… already exists")`), поэтому прежний токен «Mega Manager» снимается ДО
выпуска нового. Новых пользователей HA не заводим.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any

from homeassistant.auth.models import TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN
from homeassistant.core import HomeAssistant

from .core.const import LOGGER
from .core.ops_base import OpError

CLIENT_NAME = "Mega Manager"
LIFETIME = timedelta(days=3650)


def _usable(user: Any) -> bool:
    return bool(user.is_active) and not user.system_generated


async def pick_user(hass: HomeAssistant) -> Any:
    """Владелец HA, а если его нет или он выключен, — первый активный администратор."""
    owner = await hass.auth.async_get_owner()
    if owner is not None and _usable(owner):
        return owner
    for user in await hass.auth.async_get_users():
        if _usable(user) and user.is_admin:
            return user
    raise OpError(
        "В Home Assistant нет активного владельца или администратора, для которого можно выпустить токен",
        HTTPStatus.CONFLICT,
    )


async def issue(hass: HomeAssistant) -> dict[str, str]:
    """Выпустить новый токен «Mega Manager», сняв прежний."""
    user = await pick_user(hass)
    for old in list(user.refresh_tokens.values()):
        if old.token_type == TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN and old.client_name == CLIENT_NAME:
            hass.auth.async_remove_refresh_token(old)
    try:
        refresh = await hass.auth.async_create_refresh_token(
            user,
            client_name=CLIENT_NAME,
            token_type=TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN,
            access_token_expiration=LIFETIME,
        )
        token = hass.auth.async_create_access_token(refresh)
    except Exception as err:  # noqa: BLE001 - отказ ядра отдаём текстом, а не падением канала
        LOGGER.warning("Could not issue a Home Assistant token: %s", type(err).__name__)
        raise OpError("Home Assistant не выпустил токен", HTTPStatus.BAD_GATEWAY) from err
    expires = datetime.now(timezone.utc) + LIFETIME
    return {
        "token": token,
        "expires_at": expires.isoformat(timespec="seconds"),
        "user": user.name or "",
    }


def web_facts(hass: HomeAssistant) -> dict[str, Any] | None:
    """Порт и https собственного веб-сервера HA для `hello`; нет сервера — `None`.

    Менеджер строит по ним адрес входа в HA объекта. Адрес дом шлёт полем `net` и здесь не
    повторяется. Источник — `hass.config.api` (`ApiConfig`, ставит интеграция `http`).
    """
    api = hass.config.api
    if api is None:
        return None
    return {"port": int(api.port), "ssl": bool(api.use_ssl)}
