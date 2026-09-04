"""Live link to the manager: "the config changed, come and get it"."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from homeassistant.core import HomeAssistant

from .const import CONF_MANAGER_URL, CONF_TOKEN, CONF_VERIFY_SSL, LOGGER
from .coordinator import MegaHomeConfigEntry, MegaHomeCoordinator

WS_PATH = "/inbound/home"
# Reconnect backoff. An object may be offline for good, so a dead link is an
# ordinary state: back off to a minute and stay quiet rather than retry-storm.
FIRST_RETRY = 5
MAX_RETRY = 60
# Our own keepalive on top of the server's ping: a NAT that silently drops an
# idle connection is the classic way a link looks alive and delivers nothing.
HEARTBEAT = 30


class ManagerLink:
    """Holds one outgoing WebSocket to the manager and refreshes on its nudge.

    ⚠ The socket is OUTGOING — the object dials the cloud, exactly like the
    MegaDriver link does. That is the whole reason this works on a normal home
    connection: no public IP, no port forwarding, no hole in the perimeter. The
    manager pushing *into* the house would need reachability, which is a
    different (later) problem.

    This only ever carries a nudge. The config itself is still fetched over the
    same HTTPS endpoint the poll uses, so there is exactly one code path that
    updates the cache — and the poll stays as the safety net for when this link
    is down.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: MegaHomeConfigEntry,
        coordinator: MegaHomeCoordinator,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._coordinator = coordinator
        self._connected = False
        self._task: asyncio.Task[None] | None = None

    @property
    def connected(self) -> bool:
        """Whether the link is up right now."""
        return self._connected

    def start(self) -> None:
        """Run the link for as long as the config entry is loaded."""
        self._task = self._entry.async_create_background_task(
            self._hass, self._run(), "mega_home_link", eager_start=False
        )

    async def async_stop(self) -> None:
        """Stop the link (the background task dies with the entry anyway)."""
        if self._task:
            self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        from homeassistant.helpers.aiohttp_client import async_get_clientsession

        session = async_get_clientsession(
            self._hass, self._entry.data.get(CONF_VERIFY_SSL, True)
        )
        url = _ws_url(self._entry.data[CONF_MANAGER_URL])
        headers = {"Authorization": f"Bearer {self._entry.data[CONF_TOKEN]}"}
        delay = FIRST_RETRY
        while True:
            try:
                async with session.ws_connect(
                    url, headers=headers, heartbeat=HEARTBEAT
                ) as socket:
                    self._connected = True
                    delay = FIRST_RETRY
                    LOGGER.info("Manager link is up")
                    await socket.send_json(
                        {"t": "hello", "version": _integration_version(self._hass)}
                    )
                    async for message in socket:
                        if message.type is aiohttp.WSMsgType.TEXT:
                            await self._handle(message.json())
                        elif message.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - a link must never die
                # One line per state change, not per attempt: an object that is
                # offline for good would otherwise fill the log for years.
                if self._connected:
                    LOGGER.info("Manager link is down: %s", err)
                else:
                    LOGGER.debug("Manager link retry failed: %s", err)
            finally:
                if self._connected:
                    LOGGER.info("Manager link closed")
                self._connected = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RETRY)

    async def _handle(self, payload: dict[str, Any]) -> None:
        kind = payload.get("t")
        if kind == "app_changed":
            # A new interface was published. Nothing about this home changed, so
            # the config refresh would not notice it on its own.
            if self._coordinator.bundle:
                await self._coordinator.bundle.async_sync(payload.get("version"))
            return
        if kind != "config_changed":
            return
        version = payload.get("version")
        if version and version == self._coordinator.version:
            # Already holding it (the poll got there first) — nothing to do.
            return
        LOGGER.debug("Manager says the config moved to %s", version)
        await self._coordinator.async_request_refresh()


def _ws_url(manager_url: str) -> str:
    base = manager_url.rstrip("/")
    if base.startswith("https://"):
        return f"wss://{base[len('https://'):]}{WS_PATH}"
    if base.startswith("http://"):
        return f"ws://{base[len('http://'):]}{WS_PATH}"
    return f"ws://{base}{WS_PATH}"


def _integration_version(hass: HomeAssistant) -> str:
    """Our own version, so the manager can show which objects lag behind."""
    from homeassistant.loader import async_get_loaded_integration

    from .const import DOMAIN

    try:
        return async_get_loaded_integration(hass, DOMAIN).version or ""
    except Exception:  # noqa: BLE001 - a missing version must not break the link
        return ""
