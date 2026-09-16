"""Live states for a resident who is AWAY: the house's own event stream, over the link.

⚠ Why this exists. Outside the house the app used to POLL `api/states` through
the manager every three seconds: a full snapshot of every tile, carried house →
manager → phone twenty times a minute whether anything changed or not, and a
status that showed up to three seconds late. Inside the house the same app gets
every change the moment Home Assistant does (`events.py`). The Home Assistant
docs name the right shape for this: a state change is something you SUBSCRIBE to
(`docs/docs-ha/dev-integration_listen_events.md` in the manager repo — "use the
helpers, `async_track_state_change_event`"), and the result of a service call is
learned by "listening to `state_changed` events"
(`dev-api-websocket.md`, "Calling a service action").

So this module carries EXACTLY the local stream — the same `StateStream`, the
same subscription on the tiles' entity ids, the same `states` / `entity` /
`config` events — over the link that is already up. One subscription per house,
however many phones are watching: the manager fans it out.

⚠ This is a session, not a request: it lives until the manager says stop or the
link drops. That is the one kind of named link operation the remote-access rules
allow besides WebRTC (manager repo, `docs/remote-access.md`). No HTTP route is
added — the integration's route lock (`tests/test_routes.py`) is untouched.
"""

from __future__ import annotations

import asyncio
import json
from functools import partial
from typing import Any

from . import ops
from .const import LOGGER
from .coordinator import MegaHomeCoordinator
from .events import StateStream

# Атрибуты HA бывают датами и прочим, чего `json` не знает. Поток SSE пишет
# их `default=str` (`events.py`) — канал обязан отдавать ровно то же самое.
_dumps = partial(json.dumps, default=str)


class LinkWatch:
    """One subscription of the house to itself, pumped into the manager link."""

    def __init__(self, hass: Any, coordinator: MegaHomeCoordinator, socket: Any) -> None:
        self._hass = hass
        self._coordinator = coordinator
        self._socket = socket
        self._stream: StateStream | None = None
        self._task: asyncio.Task[None] | None = None
        # Досыл снимка новому зрителю — отдельными задачами; ссылки держим, иначе
        # сборщик мусора вправе снять задачу на полпути.
        self._snapshots: set[asyncio.Task[None]] = set()

    @property
    def running(self) -> bool:
        return self._task is not None

    def start(self) -> None:
        """Subscribe and send the full snapshot first.

        ⚠ A second start sends the snapshot AGAIN instead of doing nothing: the
        manager asks for "on" whenever another phone starts watching, and that
        phone needs the whole house. The manager keeps no copy of it on purpose —
        it is a postman, not a cache (`docs/remote-access.md`).
        """
        if self._task is not None:
            self._snapshots.add(asyncio.ensure_future(self._snapshot()))
            return
        self._task = asyncio.ensure_future(self._pump())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
        for task in self._snapshots:
            task.cancel()
        self._snapshots.clear()
        self._unsubscribe()

    async def _snapshot(self) -> None:
        try:
            await self._send("states", ops.states(self._hass, self._coordinator))
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - следующий зритель попросит снова
            LOGGER.debug("Watch snapshot was not sent: %s", err)
        finally:
            self._snapshots.discard(asyncio.current_task())  # type: ignore[arg-type]

    def _subscribe(self) -> StateStream:
        self._unsubscribe()
        self._stream = StateStream(self._hass, self._coordinator)
        self._stream.start()
        return self._stream

    def _unsubscribe(self) -> None:
        if self._stream is not None:
            self._stream.stop()
            self._stream = None

    async def _pump(self) -> None:
        stream = self._subscribe()
        try:
            # ⚠ Снимок ПОСЛЕ подписки, а не до: изменение, случившееся между
            # ними, иначе не попало бы ни в снимок, ни в поток.
            await self._send("states", ops.states(self._hass, self._coordinator))
            while True:
                name, payload = await stream.queue.get()
                if name == "overflow":
                    # Менеджер не успевал читать (дом щёлкает быстрее, чем уходит
                    # канал). Копить в памяти Home Assistant нельзя — подписываемся
                    # заново и отдаём полный снимок: он и есть «наверстать».
                    stream = self._subscribe()
                    name, payload = "states", ops.states(self._hass, self._coordinator)
                await self._send(name, payload)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - канал переподключится и попросит снова
            LOGGER.debug("Watch over the link stopped: %s", err)
        finally:
            self._unsubscribe()
            # Умершая сама подписка не должна считаться живой: следующий `start`
            # обязан поднять её заново, а не молча ничего не сделать.
            if self._task is asyncio.current_task():
                self._task = None

    async def _send(self, name: str, payload: Any) -> None:
        await self._socket.send_json(
            {"t": "watch", "event": name, "payload": payload}, dumps=_dumps
        )
