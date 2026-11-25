"""Домашняя очередь на плеер. Позиция меняется по событиям выбранного выхода."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any, Protocol

from ..core.const import LOGGER
from ..core.host import Host
from ..core.ops import call
from ..core.ops_base import OpError
from ..core.source import EntityState, StateSource
from .player_output import ACTIVE, START_SLACK, STOPPED, MediaItem, Output, Sample

START_TIMEOUT = 25
END_SLACK = 3
QUEUE_HISTORY = 200

Prepare = Callable[[MediaItem, Callable[[], None]], Awaitable[Output]]
Load = Callable[[], Awaitable[tuple[tuple[MediaItem, ...], int]]]
LoadMore = Callable[[], Awaitable[tuple[tuple[MediaItem, ...], bool]]]


@dataclass
class Attempt:
    before: Sample
    output: Output | None = None
    opened: bool = False
    accepted: bool = False
    started: Sample | None = None
    last: Sample | None = None
    anchor: float = 0
    timeout: asyncio.TimerHandle | None = None


@dataclass
class Queue:
    items: tuple[MediaItem, ...]
    index: int
    prepare: Prepare
    more: LoadMore | None
    attempt: Attempt | None = None
    # Позиция текущего элемента для потока сессии, сек: seek меняет декодер внутри того же ответа.
    position: float = 0.0

    @property
    def item(self) -> MediaItem:
        return self.items[self.index]


class FlowPort(Protocol):
    """Сессия потока плеера глазами очереди (`providers/music_flow.py`).

    ⚠ Очередь владеет элементами, сессия — только звуком: адрес один на сессию, а что в нём
    играет, говорит очередь (`set_items`). Так «что играет» остаётся в одном месте.
    """

    def reading_player(self, player: str) -> bool: ...

    def resume(self) -> None: ...

    def seek(self, position: float) -> None: ...

    def set_items(self, items: tuple[MediaItem, ...], index: int) -> None: ...

    def stop(self) -> None: ...


class PlayerQueue:
    def __init__(
        self, entity: str, source: StateSource, host: Host, changed: Callable[[], None],
        unwatch_stream: Callable[[str], None],
        flow: FlowPort | None = None,
    ) -> None:
        self.entity = entity
        self._source = source
        self._host = host
        self._changed = changed
        self._unwatch_stream = unwatch_stream
        self._flow = flow
        self.group_paused = False
        self._lock = asyncio.Lock()
        self._queue: Queue | None = None
        self._unsubscribe: Callable[[], None] | None = None
        self._advance: asyncio.Task | None = None
        self._closed = False
        self._error: str | None = None
        self._replace_revision = 0

    def view(self) -> dict[str, Any] | None:
        queue = self._queue
        if queue is None:
            return {"error": self._error} if self._error else None
        track = queue.item
        attempt = queue.attempt
        position = None
        if attempt and attempt.started and attempt.output and attempt.output.flow:
            position = int(queue.position + max(0, monotonic() - attempt.anchor))
        return {
            "track": {
                "id": track.id, "title": track.title, "artist": track.artist, "album": track.album,
                "image": track.image, "duration": track.duration,
            },
            "started": bool(attempt and attempt.started),
            "next": queue.index + 1 < len(queue.items) or queue.more is not None,
            "previous": queue.index > 0,
            "seek": track.duration > 0,
            **({"position": min(position, int(track.duration))} if position is not None else {}),
        }

    async def replace(
        self, load: Load, prepare: Prepare, more: LoadMore | None = None,
    ) -> None:
        self._replace_revision += 1
        revision = self._replace_revision
        async with self._lock:
            if revision != self._replace_revision:
                return
            items, index = await load()
            if self._closed:
                raise OpError("Музыкальная очередь отключена", 503)
            if revision != self._replace_revision:
                return
            self._drop()
            self._queue = Queue(items, index, prepare, more)
            # ⚠ Элементы ставим ПОСЛЕ снятия прежней очереди и ДО запуска: у потока-сессии тот же
            # адрес, и смена очереди обязана пройти внутри уже открытого потока, а не оборвать его.
            if self._flow is not None:
                self.group_paused = False
                self._flow.resume()
                self._flow.set_items(tuple(items), index)
            self._unsubscribe = self._source.subscribe([self.entity], self._observe)
            await self._launch(self._queue)

    @property
    def flow(self) -> FlowPort | None:
        return self._flow

    async def promote(self, flow: FlowPort) -> bool:
        async with self._lock:
            queue = self._queue
            if queue is None:
                return False
            self._discard_attempt(queue)
            self._flow = flow
            flow.set_items(queue.items, queue.index)
            await self._launch(queue)
            return True

    async def control(self, action: str, position: float | None = None) -> bool:
        async with self._lock:
            queue = self._queue
            if queue is None:
                return False
            if action == "stop":
                self.clear()
                return True
            if action == "seek":
                # Обычный выход перематывает сам плеер — HA справится напрямую. У потока сессии
                # перемотка происходит ВНУТРИ уже открытого ответа: декодер перезапускается с
                # новой позиции, команда плееру и разрыв соединения не нужны.
                if self._flow is None or queue.item.duration <= 0 or position is None:
                    return False
                seek = min(max(0.0, position), max(0.0, queue.item.duration - 0.5))
                queue.position = seek
                self._flow.seek(seek)
                if (attempt := queue.attempt) is not None:
                    now = Sample.read(self._source.get(self.entity))
                    attempt.started = attempt.last = now
                    attempt.anchor = monotonic()
                self._changed()
                return True
            queue.position = 0.0
            await self._move(queue, 1 if action == "next" else -1)
            return True

    async def _move(self, queue: Queue, step: int) -> bool:
        # Следующая страница нужна лишь у конца очереди. Пустые страницы могут
        # содержать удалённые треки, поэтому пробуем ещё, когда каталог сообщил продолжение.
        if step == 1 and queue.index + 1 == len(queue.items) and queue.more:
            for _ in range(3):
                items, more = await queue.more()
                if self._queue is not queue:
                    return False
                if not more:
                    queue.more = None
                if items:
                    dropped = max(0, queue.index + 1 - QUEUE_HISTORY)
                    queue.items = (*queue.items[dropped:], *items)
                    queue.index -= dropped
                    break
                if not more:
                    self._changed()
                    break
        index = queue.index + step
        if not 0 <= index < len(queue.items):
            return False
        self._discard_attempt(queue)
        queue.index = index
        if self._flow is not None:
            self._flow.set_items(tuple(queue.items), index)
        await self._launch(queue)
        return True

    async def flow_end(self) -> bool:
        """Элемент доигран ВНУТРИ потока сессии: дальше по очереди, без команды плееру."""
        async with self._lock:
            queue = self._queue
            if queue is None:
                return False
            return await self._move(queue, 1)

    def clear(self, error: str | None = None) -> None:
        """Снять очередь. Владелец очереди остановлен — поток сессии закрывается тоже."""
        self._drop(error)
        if self._flow is not None:
            self._flow.stop()

    def _drop(self, error: str | None = None) -> None:
        """Снять очередь, НЕ трогая поток сессии: замена очереди меняет звук внутри него."""
        queue, self._queue = self._queue, None
        changed = queue is not None or error != self._error
        self._error = error
        if queue:
            self._discard_attempt(queue)
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        if changed:
            self._changed()

    def close(self) -> None:
        self._closed = True
        self.clear()
        if self._advance:
            self._advance.cancel()

    def _discard_attempt(self, queue: Queue) -> None:
        attempt, queue.attempt = queue.attempt, None
        if attempt:
            if attempt.timeout:
                attempt.timeout.cancel()
            if attempt.output and attempt.output.proxy:
                self._unwatch_stream(attempt.output.proxy)

    async def _launch(self, queue: Queue) -> None:
        attempt = queue.attempt = Attempt(Sample.read(self._source.get(self.entity)))
        self._changed()

        def opened() -> None:
            if self._queue is queue and queue.attempt is attempt:
                attempt.opened = True
                self._observe(self.entity, self._source.get(self.entity))

        try:
            attempt.output = await queue.prepare(queue.item, opened)
            if self._queue is not queue:
                self._discard_attempt(queue)
                return
            attempt.before = Sample.read(self._source.get(self.entity))
            # ⚠ Поток сессии уже читается устройством: адрес тот же, команды плееру не нужно —
            # элемент сменён внутри открытого потока (`providers/music_flow.py`). Это и есть
            # переключение без «Стоп» у плеера, который новый `media_url` поверх потока не берёт.
            if (attempt.output.flow and self._flow is not None
                    and self._flow.reading_player(self.entity)):
                attempt.accepted = True
                attempt.started = attempt.last = attempt.before
                attempt.anchor = monotonic()
                self._changed()
                return
            await call(self._source, "media_player", "play_media", {
                "entity_id": self.entity, **attempt.output.data(queue.item),
            })
            if self._queue is not queue:
                return
            attempt.accepted = True
            if attempt.output.owner:
                # Содержимое наше, свидетельства от плеера не будет: команду приняла HA — этого
                # довольно, и ждать подтверждения нечем (`Output.owner`).
                attempt.started = attempt.last = Sample.read(self._source.get(self.entity))
                attempt.opened = attempt.started.state == "playing" and attempt.output.matches(attempt.started)
                attempt.anchor = monotonic()
                self._changed()
                return
            attempt.timeout = asyncio.get_running_loop().call_later(START_TIMEOUT, self._timed_out, attempt)
            self._observe(self.entity, self._source.get(self.entity))
        except (Exception, asyncio.CancelledError):
            if self._queue is queue:
                self.clear()
            raise

    def _timed_out(self, attempt: Attempt) -> None:
        if self._queue and self._queue.attempt is attempt and attempt.started is None:
            self.clear("Плеер не подтвердил запуск трека")

    def _observe(self, _entity_id: str, state: EntityState | None) -> None:
        queue = self._queue
        attempt = queue.attempt if queue else None
        if self.group_paused or queue is None or attempt is None or not attempt.accepted or attempt.output is None:
            return
        current = Sample.read(state)
        output = attempt.output
        if output.owner:
            self._observe_station(attempt, current)
            return
        owned = output.matches(current)
        if attempt.started is None:
            reset = current.position is not None and current.position <= START_SLACK < (attempt.before.position or 0)
            changed = current.content != attempt.before.content or attempt.before.state not in ACTIVE or reset
            if (output.flow and attempt.opened) or (current.state == "playing" and (
                (owned and (changed or not output.native)) or
                (output.opaque and attempt.opened and "://" not in current.content)
            )):
                if attempt.timeout:
                    attempt.timeout.cancel()
                attempt.started = attempt.last = current
                attempt.anchor = current.at if current.position is not None else monotonic()
                self._changed()
            return
        prior = attempt.last
        if prior is None:
            return
        if current.state in STOPPED:
            self.clear()
            return
        if current.state == "buffering":
            return
        if output.flow:
            # Позицию очереди меняет декодер. HA сообщает позицию своего HTTP-выхода,
            # которая может обнулиться при буферизации или переподключении.
            attempt.last = current
            return
        opaque_match = output.opaque and current.identity == attempt.started.identity
        cleared_on_pause = current.state == "paused" and not current.content and not any(current.identity)
        if current.state == "playing" and not (owned or opaque_match):
            if output.native and current.content.partition(":")[0].isdigit() and self._finished(queue, attempt, current):
                self._schedule_advance(queue, attempt)
            else:
                self.clear()
            return
        if current.state == "idle" or (current.state == "paused" and (owned or opaque_match or cleared_on_pause)):
            if self._finished(queue, attempt, current):
                self._schedule_advance(queue, attempt)
                return
            if current.state == "idle":
                self.clear()
                return
        elif current.state != "playing":
            self.clear()
            return
        if current.state == "playing" and prior.state == "paused" and current.position is None:
            attempt.anchor += max(0, monotonic() - prior.at)
        attempt.last = current

    def _observe_station(self, attempt: Attempt, current: Sample) -> None:
        # HA может принять команду, пока медиаплеер ещё сообщает старый источник.
        # У радио меняются метаданные эфира, а некоторые плееры скрывают сам URL.
        if current.state in STOPPED:
            self.clear()
            return
        if current.state == "playing":
            if attempt.opened and attempt.last is not None:
                previous = attempt.last
                changed_source = bool(current.identity[1] and previous.identity[1]
                                      and current.identity[1] != previous.identity[1])
                changed_url = ("://" in current.content and "://" in previous.content
                               and current.content != previous.content)
                if changed_source or changed_url:
                    self.clear()
                    return
            attempt.opened = True
            attempt.last = current
        elif current.state == "idle" and (attempt.opened or monotonic() - attempt.anchor >= START_TIMEOUT):
            self.clear()

    @staticmethod
    def _finished(queue: Queue, attempt: Attempt, current: Sample) -> bool:
        prior = attempt.last
        if prior is None or prior.state != "playing":
            return False
        duration = prior.duration or queue.item.duration
        if not duration:
            return current.state == "idle"
        end = duration - min(END_SLACK, duration / 10)
        now = monotonic()
        played = prior.elapsed(now) if prior.position is not None else now - attempt.anchor
        if current.state == "paused":
            return (current.position or 0) >= end or (
                current.position is not None and current.position <= START_SLACK and played >= end
            )
        return played >= end or (current.state == "idle" and (current.position or 0) >= end)

    def _schedule_advance(self, queue: Queue, attempt: Attempt) -> None:
        if self._advance and not self._advance.done():
            return
        self._advance = self._host.spawn(self._advance_from(queue, attempt), "Mega Home: следующий трек")

    async def _advance_from(self, queue: Queue, attempt: Attempt) -> None:
        async with self._lock:
            if self._queue is not queue or queue.attempt is not attempt:
                return
            try:
                if not await self._move(queue, 1) and self._queue is queue:
                    self.clear()
            except OpError as err:
                self.clear(err.message)
            except Exception:
                LOGGER.exception("Не удалось запустить следующий трек на %s", self.entity)
                self.clear("Не удалось запустить следующий трек")
