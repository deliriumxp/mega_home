"""Keeps the home config in sync with the manager and cached on disk."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import STORAGE_DIR, Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import ManagerClient, ManagerError
from .const import (
    DEFAULT_UPDATE_INTERVAL,
    DOMAIN,
    ICON_DIR,
    ICON_SIZE,
    LOGGER,
    MAX_UPDATE_INTERVAL,
    STORAGE_KEY,
    STORAGE_VERSION,
)

type MegaHomeConfigEntry = ConfigEntry["MegaHomeCoordinator"]


class MegaHomeCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Pulls the home config, caches it, and serves the cache no matter what.

    The cache is the runtime source of truth, not a fallback. A finished
    installation may lose its internet connection permanently and must keep
    working, so an unreachable manager is an ordinary state here: the previous
    config keeps being served and the poll simply backs off.
    """

    config_entry: MegaHomeConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: MegaHomeConfigEntry,
        client: ManagerClient,
    ) -> None:
        super().__init__(
            hass,
            LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=DEFAULT_UPDATE_INTERVAL,
        )
        self.client = client
        # Живой канал к менеджеру; ставится в async_setup_entry после регистрации
        # HTTP, потому что сам канал ничего не раздаёт — он только будит опрос.
        self.link: Any = None
        # Бандл интерфейса: качается с менеджера и раздаётся из кэша, поэтому
        # новая версия приложения не требует ни HACS, ни перезапуска.
        self.bundle: Any = None
        self.last_error: str | None = None
        # DataUpdateCoordinator tracks whether the last refresh succeeded but
        # NOT when it last did, so the timestamp the installer actually asks
        # about ("when did this home last hear from the manager?") is ours.
        self.last_success_at: datetime | None = None
        self._store = Store[dict[str, Any]](hass, STORAGE_VERSION, STORAGE_KEY)
        self._icons_dir = Path(hass.config.path(STORAGE_DIR, ICON_DIR))

    @property
    def icons_dir(self) -> Path:
        """Directory holding the downloaded scenario icons."""
        return self._icons_dir

    @property
    def version(self) -> str | None:
        """Version of the config currently held."""
        return (self.data or {}).get("version")

    async def async_load_cache(self) -> bool:
        """Seed the coordinator from disk. Returns True if a config was found.

        Done before the first network call on purpose: after a Home Assistant
        restart on an offline object the app has to come up anyway, and it can
        only do that from the cache.
        """
        cached = await self._store.async_load()
        if not cached:
            return False
        self.data = cached
        LOGGER.debug("Loaded cached home config %s", cached.get("version"))
        return True

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            version = await self.client.async_version()
            if self.data and version == self.data.get("version"):
                # Nothing changed: this is the common case, and it costs one
                # small response instead of the whole config.
                self._on_success()
                return self.data
            config = await self.client.async_config()
        except ManagerError as err:
            self.last_error = str(err)
            # Back off instead of retrying every 15 minutes forever. The
            # coordinator itself logs the first failure and stays quiet
            # afterwards, which is exactly the "one line per state change" the
            # installers asked for.
            self.update_interval = min(
                self.update_interval * 2 if self.update_interval else MAX_UPDATE_INTERVAL,
                MAX_UPDATE_INTERVAL,
            )
            if self.data:
                # Keep serving the cache: raising UpdateFailed marks the entry
                # as failing without discarding self.data.
                raise UpdateFailed(f"manager unreachable, serving cached config: {err}")
            raise UpdateFailed(f"manager unreachable and nothing is cached: {err}")

        await self._store.async_save(config)
        await self._async_sync_icons(config)
        # Бандл проверяем в том же цикле: отдельный таймер означал бы второй
        # график опроса и второй набор состояний «когда мы последний раз ходили».
        if self.bundle:
            await self.bundle.async_sync()
        self._on_success()
        LOGGER.info("Home config updated to %s", config.get("version"))
        return config

    def _on_success(self) -> None:
        self.last_error = None
        self.last_success_at = dt_util.utcnow()
        self.update_interval = DEFAULT_UPDATE_INTERVAL

    async def _async_sync_icons(self, config: dict[str, Any]) -> None:
        """Download every scenario icon the config names.

        Icons are files, not URLs to the manager: on an object without internet
        a picture served from the cloud is a blank tile.
        """
        wanted = {
            scenario["icon"]
            for scenario in config.get("scenarios", [])
            if isinstance(scenario.get("icon"), str) and scenario["icon"]
        }
        if not wanted:
            return
        await self.hass.async_add_executor_job(
            self._icons_dir.mkdir, 0o755, True, True
        )
        for icon in sorted(wanted):
            target = self._icons_dir / f"{icon}_{ICON_SIZE}.png"
            if await self.hass.async_add_executor_job(target.exists):
                continue
            try:
                payload = await self.client.async_icon(icon)
            except ManagerError as err:
                # A missing icon is not worth failing the whole sync over: the
                # home is usable without one picture.
                LOGGER.warning("Could not fetch scenario icon %s: %s", icon, err)
                continue
            await self.hass.async_add_executor_job(target.write_bytes, payload)
            LOGGER.debug("Stored scenario icon %s", target.name)
