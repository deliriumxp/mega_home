"""Push of state changes to the resident app: one Server-Sent Events stream.

⚠ Why this exists. The app used to poll `api/states` every three seconds, so a
tap on a tile changed nothing on screen until the next poll answered — up to a
second even here, inside the house, where Home Assistant is two metres away and
the state is available the moment the device reports it. Polling is the right
shape for a remote link that may be down; it is the wrong shape for code running
inside Home Assistant, which is handed every state change as it happens.

SSE and not a WebSocket: the traffic is one-way (the app sends commands over the
existing POST endpoints), an `EventSource` reconnects on its own, and it survives
the reverse proxies people put in front of Home Assistant without any upgrade
handshake to configure.

Events on the stream:

* `states` — the full snapshot, sent once when the stream opens, so a client that
  just connected needs no separate request;
* `entity`  — one tile whose Home Assistant state changed;
* `config`  — the installer changed the home; carries the new version, and the
  app rereads the config;
* comment lines (`: ping`) every PING_SECONDS to keep proxies from closing an
  idle stream.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aiohttp import web

from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import async_track_state_change_event

from . import ops
from .const import LOGGER
from .coordinator import MegaHomeCoordinator

# Комментарий-пинг: держит соединение открытым через реверс-прокси с таймаутом
# простоя (у nginx по умолчанию 60 с) и даёт заметить оборванный сокет.
PING_SECONDS = 25
# Очередь на одного клиента. Переполнение означает, что клиент не читает: дом
# щёлкает реле быстрее, чем телефон успевает принимать. Тогда стрим закрывается,
# а EventSource переподключается и получает полный снимок — это дешевле, чем
# копить события в памяти Home Assistant.
QUEUE_LIMIT = 100


class StateStream:
    """One connected client: a queue fed by Home Assistant, drained by aiohttp."""

    def __init__(self, hass: HomeAssistant, coordinator: MegaHomeCoordinator) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self.queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(QUEUE_LIMIT)
        self._unsubscribe: Any = None
        # Клиент перестал читать — дальше в очередь не кладём вовсе: поток
        # закрывается, а EventSource переподключится за полным снимком.
        self._overflowed = False
        self._unsubscribe_config: Any = None

    # --- подписка на дом ---

    def start(self) -> None:
        """Subscribe to exactly the entities the app shows, plus config changes."""
        self._subscribe_entities()
        self._unsubscribe_config = self.coordinator.async_add_listener(
            self._on_config
        )

    def stop(self) -> None:
        for unsubscribe in (self._unsubscribe, self._unsubscribe_config):
            if unsubscribe:
                unsubscribe()
        self._unsubscribe = None
        self._unsubscribe_config = None

    def _tiles(self) -> list[dict[str, Any]]:
        return list(self.coordinator.data.get("tiles", []) or [])

    def _subscribe_entities(self) -> None:
        """(Re)subscribe to the entity ids of the current config.

        ⚠ Called again on every config change: the installer adding a socket
        must not require the resident to reload the page, and a tile whose
        entity_id changed would otherwise stream nothing for ever.
        """
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        by_entity: dict[str, list[dict[str, Any]]] = {}
        for tile in self._tiles():
            entity_id = tile.get("entityId")
            if entity_id:
                by_entity.setdefault(entity_id, []).append(tile)
        self._by_entity = by_entity
        if not by_entity:
            return
        self._unsubscribe = async_track_state_change_event(
            self.hass, list(by_entity), self._on_state
        )

    # --- источники событий ---

    @callback
    def _on_state(self, event: Event[EventStateChangedData]) -> None:
        state = event.data.get("new_state")
        entity_id = event.data.get("entity_id")
        # Несколько плиток на одну сущность — законный случай: тот же прибор
        # может стоять в двух комнатах приложения.
        for tile in self._by_entity.get(entity_id or "", []):
            self._put("entity", ops.entity_view(tile, state))

    @callback
    def _on_config(self) -> None:
        self._subscribe_entities()
        self._put("config", {"configVersion": self.coordinator.version})

    @callback
    def _put(self, name: str, payload: Any) -> None:
        if self._overflowed:
            return
        try:
            self.queue.put_nowait((name, payload))
        except asyncio.QueueFull:
            # Клиент не успевает читать (дом щёлкает реле быстрее, чем телефон
            # принимает). Копить события в памяти Home Assistant нельзя, поэтому
            # поток закрывается сигналом: переподключение возьмёт полный снимок.
            self._overflowed = True
            self.queue = asyncio.Queue(1)
            self.queue.put_nowait(("overflow", None))

    # --- выдача клиенту ---

    async def run(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                # Реверс-прокси иначе буферизует поток и «мгновенно» перестаёт
                # быть мгновенным.
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(request)
        self.start()
        try:
            await self._write(response, "states", ops.states(self.hass, self.coordinator))
            while True:
                try:
                    name, payload = await asyncio.wait_for(
                        self.queue.get(), PING_SECONDS
                    )
                except TimeoutError:
                    await response.write(b": ping\n\n")
                    continue
                if name == "overflow":
                    return response
                await self._write(response, name, payload)
        except (ConnectionResetError, asyncio.CancelledError):
            # Обычный уход клиента (закрыл вкладку, ушёл из сети) — не ошибка.
            pass
        except Exception:  # noqa: BLE001 — стрим не должен ронять Home Assistant
            LOGGER.exception("Поток состояний оборвался")
        finally:
            self.stop()
        return response

    async def _write(self, response: web.StreamResponse, name: str, payload: Any) -> None:
        body = json.dumps(payload, default=str)
        await response.write(f"event: {name}\ndata: {body}\n\n".encode())
