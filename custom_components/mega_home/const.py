"""Constants for the Mega Home integration."""

from __future__ import annotations

from datetime import timedelta
import logging

DOMAIN = "mega_home"
LOGGER = logging.getLogger(__package__)

# Версия ЗАГРУЖЕННОГО кода — та, что уезжает менеджеру в hello (`link.py`).
#
# ⚠ Константа в модуле, а не чтение `manifest.json`: модуль попал в память
# вместе с остальным кодом при старте Home Assistant, поэтому число описывает
# то, что реально исполняется, и не зависит от того, что HACS успел положить на
# диск после. Версию с ДИСКА дом сообщает отдельным полем — разница между ними
# и есть «обновлено в HACS, ждёт перезапуска» (`link.py`, `_disk_version`).
#
# ⚠ Ни то, ни другое число не должно НИЧЕГО запрещать на стороне менеджера:
# после обновления без перезапуска дом живёт в смешанном состоянии — уже
# загруженные модули остаются прежними, а те, что импортируются позже, читаются
# с диска новыми. Поэтому «умеет ли дом такую операцию» выясняется его ОТВЕТОМ
# (docs/remote-access.md в менеджере), а версии нужны человеку, а не коду.
#
# ⚠ Держится в паре с `manifest.json`, их сверяет `tests/test_version.py`:
# поднимать ОБА, одним релизом.
INTEGRATION_VERSION = "0.2.18"

CONF_MANAGER_URL = "manager_url"
CONF_TOKEN = "token"
CONF_VERIFY_SSL = "verify_ssl"

# Manager endpoints. The object identifies itself by its own webhook token, so
# there is no id in any path — see the manager's inbound/home-config controller.
API_VERSION = "/inbound/home-config/version"
API_CONFIG = "/inbound/home-config"
API_ICON = "/inbound/home-config/icon"
API_ROOM_PHOTO = "/inbound/home-config/room-photo"
# Фон ПЛИТКИ — снимок самого прибора, который жилец видит вместо иконки.
API_TILE_PHOTO = "/inbound/home-config/tile-photo"
# Приставка ключа такого фона. Та же строка стоит в приложении
# (`tile-photo-stores.ts`) и в менеджере — контракт трёх сторон, как и
# форма состояния плитки. Комнаты лежат под своими id без приставки.
TILE_PHOTO_PREFIX = "tile:"
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

# ОБЩИЙ канал файлов «менеджер → дом». Один каталог, один маршрут и один цикл
# зеркалирования на ЛЮБОЙ файл, который менеджер назовёт в манифесте конфига:
# сегодня это фоны плиток, завтра звуки или шрифты. Смысл — в том, чтобы новый
# вид файла НЕ СТОИЛ РЕЛИЗА этой интеграции: её код единственный, что не
# обновляется сам, и каждый выпуск это ожидание на всех объектах сразу.
ASSET_DIR = "mega_home_assets"
API_ASSET = "/inbound/home-config/asset"
# Розетка «спросить менеджера»: приложению рано или поздно понадобится не файл,
# а разговор (ИИ-чат жильца первым делом), а до менеджера из квартиры доходит
# только интеграция. Маршрут заведён заранее по той же причине, что и канал
# файлов, — чтобы сама функция не потребовала выпуска.
API_RELAY = "/inbound/home-config/relay"
# ⚠ Свой таймаут, и он больше обычного: за розеткой будет ответ ИИ, который
# честно думает секунды, а не отдаёт файл с диска.
RELAY_TIMEOUT = 60

SERVICE_SYNC = "sync"
