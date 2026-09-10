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

from typing import Any

from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
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
from .trassir import TrassirGateway
from .http import async_register_http
from .agent import AgentRunner
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

    # Периодический опрос (он же страховка на случай лежащего канала) включается
    # ЯВНО — почему, написано в `keep_polling`.
    coordinator.keep_polling(entry)

    await async_register_http(hass, coordinator)

    # Свой go2rtc :8555 stun:8555 без патча HA core — одна схема.
    #
    # ⚠ Остановка процесса и закрытие ws-сессий висят на выгрузке записи И на
    # остановке Home Assistant. Осиротевший go2rtc держит :8555, и следующий
    # запуск слушатель уже не поднимет: снаружи камеры молча перестают
    # открываться, а лечится это только ребутом машины.
    try:
        from . import webrtc as _webrtc
        from .go2rtc_embed import async_start as _go2rtc_start
        from .go2rtc_embed import async_stop as _go2rtc_stop

        async def _shutdown(_event: Any = None) -> None:
            await _webrtc.async_shutdown()
            await _go2rtc_stop()

        await _go2rtc_start(hass)
        entry.async_on_unload(
            hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _shutdown)
        )
        entry.async_on_unload(lambda: hass.async_create_task(_shutdown()))
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("go2rtc not started: %s", err)

    # Видеонаблюдение объекта, если оно у него есть. Заводится ДО живого канала
    # и сразу получает уже загруженный конфиг: адрес регистратора приезжает
    # обычной синхронизацией, и ждать следующего тика опроса (15 минут) ради
    # первой ленты событий незачем.
    gateway = TrassirGateway(
        hass,
        client,
        async_get_clientsession(hass, entry.data.get(CONF_VERIFY_SSL, True)),
    )
    await gateway.async_load()
    coordinator.trassir = gateway
    entry.async_on_unload(lambda: hass.async_create_task(gateway.async_stop()))
    if coordinator.data:
        await gateway.async_apply(coordinator.data)

    # Живой канал к менеджеру: правка состава доезжает за секунды вместо интервала
    # опроса. Опрос при этом остаётся страховкой — канал может не подняться вовсе
    # (объект без интернета), и это нормальный режим, а не авария.
    coordinator.link = ManagerLink(hass, entry, coordinator)
    coordinator.link.start()

    # Сторож объекта: правила менеджера, которые дом крутит САМ (`agent.py`).
    # Поднимается ПОСЛЕ первого опроса, но живёт независимо от него: правила
    # лежат в своём кэше, и объект, потерявший связь с менеджером, продолжает
    # сторожить себя — ровно тогда, когда чинить его больше некому.
    agent = AgentRunner(hass, client)
    await agent.async_start()
    coordinator.agent = agent
    entry.async_on_unload(lambda: hass.async_create_task(agent.async_stop()))
    await agent.async_sync()
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
