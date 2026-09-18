"""Источник состояний поверх Home Assistant: `source.StateSource` его средствами."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, State, callback
from homeassistant.exceptions import ServiceNotFound
from homeassistant.helpers.event import async_track_state_change_event

from . import webrtc
from .source import CommandRejected, CommandUnknown


class HaCameras:
    """Камеры `camera.*` этого HA (`webrtc.py`)."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass

    async def negotiate(
        self, entity_id: str, sdp: str, remote: bool, trickle: bool
    ) -> dict[str, Any]:
        return await webrtc.negotiate(self.hass, entity_id, sdp, remote, trickle)

    def close(self, entity_id: str, session_id: str) -> dict[str, Any]:
        return webrtc.close(self.hass, entity_id, session_id)

    async def snapshot(self, entity_id: str) -> tuple[str, bytes]:
        return await webrtc.snapshot(self.hass, entity_id)

    def warm(self, entity_id: str) -> None:
        webrtc.warm(self.hass, entity_id)

    async def stream_source(self, entity_id: str) -> str | None:
        return await webrtc._camera(self.hass, entity_id).stream_source()  # noqa: SLF001


class HaSource:
    """`source.StateSource` на машине состояний и службах Home Assistant."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.cameras = HaCameras(hass)

    def get(self, entity_id: str) -> State | None:
        return self.hass.states.get(entity_id)

    async def call(self, domain: str, service: str, data: dict[str, Any]) -> None:
        try:
            # blocking=True: ответ обязан нести состояние ПОСЛЕ команды. Служба
            # выполняется внутри того же HA — ожидание здесь доли миллисекунды.
            await self.hass.services.async_call(domain, service, data, blocking=True)
        except ServiceNotFound as err:
            raise CommandUnknown(f"{domain}.{service}") from err
        except vol.Invalid as err:
            raise CommandRejected(str(err)) from err

    def subscribe(
        self,
        entity_ids: list[str],
        on_change: Callable[[str, State | None], None],
    ) -> Callable[[], None]:
        @callback
        def _on_state(event: Event[EventStateChangedData]) -> None:
            on_change(event.data.get("entity_id") or "", event.data.get("new_state"))

        return async_track_state_change_event(self.hass, entity_ids, _on_state)
