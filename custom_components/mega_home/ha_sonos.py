"""Связь реестра Sonos в HA с адресами, объявленными колонками в mDNS."""

from __future__ import annotations

import asyncio
from ipaddress import IPv4Address
from typing import TYPE_CHECKING

from .providers.sonos import Sonos, normalize_uid

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

_SERVICE = "_sonos._tcp.local."


def create_sonos(hass: HomeAssistant) -> Sonos:
    from aiosonos.client import SonosLocalApiClient
    from homeassistant.components.zeroconf import async_get_async_instance
    from homeassistant.helpers import entity_registry
    from homeassistant.helpers.aiohttp_client import async_get_clientsession
    from zeroconf import IPVersion, ServiceStateChange
    from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo

    def uid_of(entity: str) -> str | None:
        entry = entity_registry.async_get(hass).async_get(entity)
        return entry.unique_id if entry is not None and entry.platform == "sonos" else None

    async def address_of(uid: str) -> str | None:
        zeroconf = await async_get_async_instance(hass)
        found: asyncio.Queue[str] = asyncio.Queue()

        def discovered(zeroconf, service_type, name, state_change):
            if state_change != ServiceStateChange.Removed and normalize_uid(name.partition("@")[0]) == uid:
                found.put_nowait(name)

        browser = AsyncServiceBrowser(zeroconf.zeroconf, [_SERVICE], handlers=[discovered])
        try:
            async with asyncio.timeout(3):
                while True:
                    name = await found.get()
                    info = AsyncServiceInfo(_SERVICE, name)
                    if not await info.async_request(zeroconf.zeroconf, timeout=1500):
                        continue
                    if normalize_uid(info.decoded_properties.get("uuid") or "") != uid:
                        continue
                    for candidate in info.parsed_addresses(IPVersion.V4Only):
                        address = IPv4Address(candidate)
                        if address.is_private and not (address.is_loopback or address.is_link_local):
                            return candidate
        except TimeoutError:
            return None
        finally:
            await browser.async_cancel()

    return Sonos(uid_of, address_of, lambda address: SonosLocalApiClient(
        address, async_get_clientsession(hass, verify_ssl=False),
    ))
