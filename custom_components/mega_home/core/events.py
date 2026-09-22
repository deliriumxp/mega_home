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
from typing import Any

from aiohttp import web

from . import ops
from .const import LOGGER
from .ops_base import dumps
from .source import EntityState

# Комментарий-пинг: держит соединение открытым через реверс-прокси с таймаутом
# простоя (у nginx по умолчанию 60 с) и даёт заметить оборванный сокет.
PING_SECONDS = 25
# Очередь на одного клиента. Переполнение означает, что клиент не читает: дом
# щёлкает реле быстрее, чем телефон успевает принимать. Тогда стрим закрывается,
# а EventSource переподключается и получает полный снимок — это дешевле, чем
# копить события в памяти Home Assistant.
QUEUE_LIMIT = 100


class StateStream:
    """One connected client: a queue fed by the source of states, drained by aiohttp."""

    def __init__(self, coordinator: Any) -> None:
        self.coordinator = coordinator
        self.queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(QUEUE_LIMIT)
        self._unsubscribe: Any = None
        # Поток кончается (см. `end`) — дальше в очередь не кладём вовсе.
        self._ended = False
        self._unsubscribe_config: Any = None
        self._unsubscribe_device: Any = None

    # --- подписка на дом ---

    def start(self) -> None:
        """Subscribe to exactly the entities the app shows, plus config changes."""
        self._subscribe_entities()
        self._unsubscribe_config = self.coordinator.async_add_listener(
            self._on_config
        )
        # События устройств (`device_events.py`): настенная панель без интернета
        # обязана узнать о звонке в дверь тем же потоком, что и о состояниях.
        hub = getattr(self.coordinator, "events", None)
        self._unsubscribe_device = (
            hub.subscribe(lambda frame: self._put("device", {k: v for k, v in frame.items() if k != "t"}))
            if hub is not None
            else None
        )

    def stop(self) -> None:
        for unsubscribe in (self._unsubscribe, self._unsubscribe_config, self._unsubscribe_device):
            if unsubscribe:
                unsubscribe()
        self._unsubscribe = None
        self._unsubscribe_config = None
        self._unsubscribe_device = None

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
        self._unsubscribe = self.coordinator.source.subscribe(
            list(by_entity), self._on_state
        )

    # --- источники событий ---

    def _on_state(self, entity_id: str, state: EntityState | None) -> None:
        # Несколько плиток на одну сущность — законный случай: тот же прибор
        # может стоять в двух комнатах приложения.
        for tile in self._by_entity.get(entity_id, []):
            self._put("entity", ops.entity_view(tile, state, self.coordinator.source.cameras))

    def _on_config(self) -> None:
        self._subscribe_entities()
        self._put("config", {"configVersion": self.coordinator.version})

    def _put(self, name: str, payload: Any) -> None:
        if self._ended:
            return
        try:
            self.queue.put_nowait((name, payload))
        except asyncio.QueueFull:
            # Клиент не успевает читать (дом щёлкает реле быстрее, чем телефон
            # принимает). Копить события в памяти Home Assistant нельзя, поэтому
            # поток закрывается сигналом: переподключение возьмёт полный снимок.
            self.end()

    def end(self) -> None:
        """Закончить поток: клиент не читает, или запись интеграции выгружается.

        ⚠ Второй случай — ради перезагрузки записи: дверь HA снять нельзя, и
        поток, открытый до неё, держал бы ПРЕЖНИЙ координатор — слал состояния
        старого набора плиток и молчал о смене конфига. Кончился поток —
        EventSource переподключается сам и попадает к новой записи.

        ⚠ Очередь та же, а не новая: читатель уже ждёт на ней (`run`, `LinkWatch`),
        и подменённую он бы не увидел до ближайшего пинга.
        """
        if self._ended:
            return
        self._ended = True
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queue.put_nowait(("end", None))

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
        # Открытые потоки знает координатор — чтобы закончить их на выгрузке
        # записи (`end`, `__init__.async_unload_entry`).
        streams = getattr(self.coordinator, "streams", None)
        if streams is not None:
            streams.add(self)
        try:
            await self._write(response, "states", ops.states(self.coordinator))
            while True:
                try:
                    name, payload = await asyncio.wait_for(
                        self.queue.get(), PING_SECONDS
                    )
                except TimeoutError:
                    await response.write(b": ping\n\n")
                    continue
                if name == "end":
                    return response
                await self._write(response, name, payload)
        except (ConnectionResetError, asyncio.CancelledError):
            # Обычный уход клиента (закрыл вкладку, ушёл из сети) — не ошибка.
            pass
        except Exception:  # noqa: BLE001 — стрим не должен ронять Home Assistant
            LOGGER.exception("Поток состояний оборвался")
        finally:
            self.stop()
            if streams is not None:
                streams.discard(self)
        return response

    async def _write(self, response: web.StreamResponse, name: str, payload: Any) -> None:
        await response.write(f"event: {name}\ndata: {dumps(payload)}\n\n".encode())
