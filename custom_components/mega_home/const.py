"""Constants for the Mega Home integration."""

from __future__ import annotations

from datetime import timedelta
import logging

DOMAIN = "mega_home"
LOGGER = logging.getLogger(__package__)

CONF_MANAGER_URL = "manager_url"
CONF_TOKEN = "token"
CONF_VERIFY_SSL = "verify_ssl"

# Manager endpoints. The object identifies itself by its own webhook token, so
# there is no id in any path — see the manager's inbound/home-config controller.
API_VERSION = "/inbound/home-config/version"
API_CONFIG = "/inbound/home-config"
API_ICON = "/inbound/home-config/icon"
API_ROOM_PHOTO = "/inbound/home-config/room-photo"
API_APP_MANIFEST = "/inbound/home-config/app/manifest"
API_APP_FILE = "/inbound/home-config/app/file"

# Polling. The manager is asked for a cheap version hash; the body is only
# fetched when that hash moved. An unreachable manager is a normal state on a
# finished installation (the flat may stay offline forever), so failures back
# off instead of hammering, and the cached config keeps serving.
DEFAULT_UPDATE_INTERVAL = timedelta(minutes=15)
MAX_UPDATE_INTERVAL = timedelta(hours=1)
REQUEST_TIMEOUT = 20

# Everything this integration serves lives under one prefix. Home Assistant's
# own frontend keeps `/` and its panels: its IndexView explicitly declines paths
# whose first segment is not a registered panel, which is why a third-party
# prefix like this one is reachable at all.
URL_PREFIX = "/mega-home"
URL_ICONS = f"{URL_PREFIX}/icons"
URL_API = f"{URL_PREFIX}/api"

STORAGE_VERSION = 1
STORAGE_KEY = "mega_home_config"
ICON_DIR = "mega_home_icons"
ICON_SIZE = "300"
# Room background photos the resident uploads from the app. They belong to the
# home, not to a device, so they live next to the rest of our cache.
PHOTO_DIR = "mega_home_photos"
# Фоны-ЗАГОТОВКИ, которые инсталлятор загрузил в менеджере. Они приезжают сюда
# файлами, как иконки сценариев: приложение раздаёт эта интеграция, а дороги от
# телефона жильца до менеджера может не быть вовсе (объект без интернета,
# менеджер за офисным фаерволом) — картинка «из облака» была бы пустым фоном.
STOCK_PHOTO_DIR = "mega_home_stock_photos"

# Command -> Home Assistant service, per domain.
#
# ⚠ ТАБЛИЦА УМИРАЕТ. Карта команд приезжает в конфиге плитки (`commands`), и
# зовёт службу `ops.command_spec`; здесь остался фолбэк на одну версию — для
# дома, чей кэш конфига ещё старее этого кода. Убрать вместе с `LEGACY_ARGS`
# следующим выпуском (docs/plan-thin-integration.md, фаза 3).
COMMAND_SERVICES: dict[str, dict[str, str]] = {
    "switch": {"turn_on": "turn_on", "turn_off": "turn_off"},
    "light": {
        "turn_on": "turn_on",
        "turn_off": "turn_off",
        "set_brightness": "turn_on",
    },
    "cover": {
        "open": "open_cover",
        "close": "close_cover",
        "stop": "stop_cover",
        "set_position": "set_cover_position",
    },
    "climate": {
        "set_temperature": "set_temperature",
        "set_mode": "set_hvac_mode",
    },
    # ⚠ `play_pause` уходит ОДНОЙ службой, а не «play или pause по состоянию»:
    # `media_play_pause` решает это у себя, по свежему состоянию. Решай мы здесь
    # — команда шла бы по снимку трёхсекундной давности, и нажатие на паузу
    # иногда снимало бы плеер с паузы.
    "media_player": {
        "turn_on": "turn_on",
        "turn_off": "turn_off",
        "play_pause": "media_play_pause",
        "previous": "media_previous_track",
        "next": "media_next_track",
    },
}

SERVICE_SYNC = "sync"
