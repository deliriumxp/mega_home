"""Передача паспортной сессии дома в мастер Yandex Station внутри HA."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from homeassistant.data_entry_flow import UnknownHandler

from .core.ops_base import OpError

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from ya_passport_auth import Credentials

    from .providers.yandex_music import MusicService

STATION = "yandex_station"


class StationAuth:
    def __init__(self, hass: HomeAssistant, music: MusicService) -> None:
        self.hass = hass
        self.music = music

    async def call(self, can_manage: bool, request: dict) -> dict:
        operation = request.get("action")
        if operation not in ("station.status", "station.connect"):
            return await self.music.call(can_manage, request)
        if not can_manage:
            raise OpError("Колонки подключает жилец с учётной записью дома", 403)
        try:
            async with asyncio.timeout(25):
                return await self.music.with_passport(
                    self._status if operation == "station.status" else self._connect
                )
        except (TimeoutError, OSError):
            return {"state": "unavailable"}

    async def _status(self, credentials: Credentials | None) -> dict:
        entries = self.hass.config_entries.async_entries(STATION)
        loaded = self.hass.config_entries.async_loaded_entries(STATION)
        if not credentials:
            return {"state": "connected" if loaded else "needs-music"}
        matches = [entry for entry in entries if (
            (credentials.display_login and entry.unique_id == credentials.display_login)
            or entry.data.get("x_token") == credentials.x_token.get_secret()
        )]
        if any(entry in loaded for entry in matches):
            return {"state": "connected"}
        if not matches and entries:
            return {"state": "other-account"}
        return {"state": "ready"}

    async def _connect(self, credentials: Credentials | None) -> dict:
        status = await self._status(credentials)
        if not credentials or status["state"] != "ready":
            return status
        flow_id = None
        try:
            result = await self.hass.config_entries.flow.async_init(STATION, context={"source": "user"})
            flow_id = result.get("flow_id")
            if result.get("type") != "form" or result.get("step_id") != "user":
                return {"state": "unsupported"}
            result = await self.hass.config_entries.flow.async_configure(flow_id, {"method": "token"})
            if result.get("type") != "form" or result.get("step_id") != "token":
                return {"state": "unsupported"}
            result = await self.hass.config_entries.flow.async_configure(
                flow_id, {"token": credentials.x_token.get_secret()}
            )
            kind = result.get("type")
            if kind == "create_entry" or (kind == "abort" and result.get("reason") == "account_updated"):
                flow_id = None
                connected = await self._status(credentials)
                return connected if connected["state"] == "connected" else {"state": "setting-up"}
            if kind == "abort":
                flow_id = None
            return {"state": "reconnect-music"}
        except UnknownHandler:
            return {"state": "not-installed"}
        except Exception:
            # Чужие исключения и исходный ответ мастера могут содержать реквизиты.
            return {"state": "unavailable"}
        finally:
            if flow_id:
                try:
                    self.hass.config_entries.flow.async_abort(flow_id)
                except Exception:
                    pass
