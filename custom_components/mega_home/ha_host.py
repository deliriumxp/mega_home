"""Хозяин поверх Home Assistant: пять примитивов `host.Host` его средствами."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from datetime import timedelta
from pathlib import Path
from typing import Any, TypeVar

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import STORAGE_DIR, Store

T = TypeVar("T")


class HaHost:
    """`host.Host` на примитивах Home Assistant."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass

    def session(self, verify_ssl: bool = True) -> aiohttp.ClientSession:
        return async_get_clientsession(self.hass, verify_ssl)

    async def run(self, func: Callable[..., T], *args: Any) -> T:
        return await self.hass.async_add_executor_job(func, *args)

    def spawn(self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        # ⚠ Фоновая, а не обычная задача: обычную HA ждёт на старте и
        # остановке, и минутная установка пакетов держала бы их.
        return self.hass.async_create_background_task(coro, name)

    def path(self, *parts: str) -> Path:
        return Path(self.hass.config.path(STORAGE_DIR, *parts))

    def store(self, key: str, version: int = 1) -> Store[Any]:
        return Store[Any](self.hass, version, key)

    def every(
        self, interval: timedelta, action: Callable[[Any], Awaitable[None] | None]
    ) -> Callable[[], None]:
        return async_track_time_interval(self.hass, action, interval)
