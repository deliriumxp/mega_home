"""Источник состояний поверх Home Assistant: `source.StateSource` его средствами."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, State, callback
from homeassistant.exceptions import HomeAssistantError, ServiceNotFound
from homeassistant.helpers.event import async_track_state_change_event

from . import webrtc
from .core.source import CommandRejected, CommandUnknown


class HaCameras:
    """Камеры `camera.*` этого HA: прогрев, кадр плитки и адрес потока (`webrtc.py`)."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        # entity_id → адрес потока, наполняет `warm` фоном; `entity_view` читает
        # синхронно (`ops_camera.cached_source`) — сети внутри опроса состояний
        # быть не должно.
        self._sources: dict[str, str] = {}

    def warm(self, entity_id: str) -> None:
        webrtc.warm(self.hass, entity_id)
        if entity_id not in self._sources:
            # Адрес потока камеры не меняется на лету — читаем его один раз на
            # запуск дома, а не на каждый опрос состояний (раз в 3 с).
            self.hass.async_create_task(self._warm_source(entity_id))

    async def _warm_source(self, entity_id: str) -> None:
        source = await self.stream_source(entity_id)
        if source:
            self._sources[entity_id] = source

    def cached_source(self, entity_id: str) -> str | None:
        """То, что успел прочитать фоновый прогрев — без сети, для `entity_view`."""
        return self._sources.get(entity_id)

    async def snapshot(self, entity_id: str) -> tuple[str, bytes]:
        return await webrtc.snapshot(self.hass, entity_id)

    async def stream_source(self, entity_id: str) -> str | None:
        return await webrtc.stream_source(self.hass, entity_id)


class HaSource:
    """`source.StateSource` на машине состояний и службах Home Assistant."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.cameras = HaCameras(hass)

    def get(self, entity_id: str) -> State | None:
        return self.hass.states.get(entity_id)

    async def call(
        self, domain: str, service: str, data: dict[str, Any], response: bool = False
    ) -> Any:
        try:
            # blocking=True: ответ обязан нести состояние ПОСЛЕ команды. Служба
            # выполняется внутри того же HA — ожидание здесь доли миллисекунды.
            # ⚠ `return_response` только по просьбе: служба без ответа на него
            # падает (dev-api-websocket.md, «return_response»: «Must be included
            # for service actions that return response data»).
            return await self.hass.services.async_call(
                domain, service, data, blocking=True, return_response=response
            )
        except ServiceNotFound as err:
            raise CommandUnknown(f"{domain}.{service}") from err
        except (vol.Invalid, HomeAssistantError) as err:
            # `HomeAssistantError` — отказ самого прибора или службы (режим не из
            # `fan_modes`, служба без ответа), а не поломка дома: жильцу — «HA
            # отклонил команду», а не пятисотка.
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
