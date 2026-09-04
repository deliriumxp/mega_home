"""The Mega Home integration: the resident app, served by Home Assistant itself.

What it does: pulls one config from Mega Manager (rooms, tiles, scenarios and
their icons), caches it on disk, and serves both that config and the app bundle
under `/mega-home`. States and commands never leave the house — they go straight
through `hass`.

Why it is an integration and not an add-on: it has to work on every Home
Assistant installation, including Container and Core, where add-ons do not
exist at all.
"""

from __future__ import annotations

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.typing import ConfigType

from .api import ManagerAuthError, ManagerClient
from .const import (
    CONF_MANAGER_URL,
    CONF_TOKEN,
    CONF_VERIFY_SSL,
    DOMAIN,
    LOGGER,
    SERVICE_SYNC,
)
from .coordinator import MegaHomeConfigEntry, MegaHomeCoordinator
from .http import async_register_http
from .link import ManagerLink

PLATFORMS: list[Platform] = []


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register the manual-sync action.

    Registered here rather than in async_setup_entry so an automation calling it
    validates even while the entry is not loaded.
    """

    async def handle_sync(call: ServiceCall) -> None:
        for entry in hass.config_entries.async_loaded_entries(DOMAIN):
            coordinator: MegaHomeCoordinator = entry.runtime_data
            await coordinator.async_request_refresh()

    hass.services.async_register(DOMAIN, SERVICE_SYNC, handle_sync)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: MegaHomeConfigEntry) -> bool:
    """Set up one home."""
    client = ManagerClient(
        async_get_clientsession(hass, entry.data.get(CONF_VERIFY_SSL, True)),
        entry.data[CONF_MANAGER_URL],
        entry.data[CONF_TOKEN],
    )
    coordinator = MegaHomeCoordinator(hass, entry, client)

    # ⚠ Что лежит на диске от прошлого запуска — узнаём ДО первого опроса: иначе
    # опрос сравнивал бы манифест с «ничего» и качал бандл, который уже есть.
    # Само хранилище создаёт координатор (см. его конструктор) — от порядка
    # здесь больше ничего не зависит.
    await coordinator.bundle.async_load()

    # The cache comes first, and on purpose. An object that is offline for good
    # still has to come up after a Home Assistant restart, and the only thing it
    # can come up from is the cache. Only a home that has never synchronised has
    # nothing to serve, and only then is a failed first fetch fatal.
    cached = await coordinator.async_load_cache()
    if cached:
        await coordinator.async_refresh()
        if not coordinator.last_update_success:
            LOGGER.info(
                "Manager unreachable at startup — serving the cached config %s",
                coordinator.version,
            )
    else:
        try:
            await coordinator.async_config_entry_first_refresh()
        except ConfigEntryNotReady:
            raise
        except ManagerAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err

    entry.runtime_data = coordinator
    await async_register_http(hass, coordinator)

    # Живой канал к менеджеру: правка состава доезжает за секунды вместо интервала
    # опроса. Опрос при этом остаётся страховкой — канал может не подняться вовсе
    # (объект без интернета), и это нормальный режим, а не авария.
    coordinator.link = ManagerLink(hass, entry, coordinator)
    coordinator.link.start()
    if PLATFORMS:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: MegaHomeConfigEntry) -> bool:
    """Unload one home.

    The views and static paths stay registered: Home Assistant's aiohttp app has
    no way to remove them, and they resolve the coordinator per request anyway,
    so an unloaded entry simply makes them answer "not synchronised yet".
    """
    if PLATFORMS:
        return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    return True


@callback
def async_update_listener(hass: HomeAssistant, entry: MegaHomeConfigEntry) -> None:
    """Reload the entry when its options change."""
    hass.config_entries.async_schedule_reload(entry.entry_id)
