"""Запуск и завершение общей сессии акустического замера."""

from __future__ import annotations

import asyncio
import shutil
from typing import TYPE_CHECKING, Any

from .core.const import LOGGER
from .core.ops_base import OpError, find
from .providers.calibration import layout, prepare
from .providers.music_flow import FlowSession

if TYPE_CHECKING:
    from .music_playback import MusicPlayback

MARKER_OPEN_TIMEOUT = 4
MARKER_RECONNECT_TIMEOUT = 4
CALIBRATION_TIMEOUT = 300


class MusicCalibration:
    def __init__(self, playback: MusicPlayback) -> None:
        self._playback = playback
        self._session: FlowSession | None = None
        self._watch: asyncio.Task[None] | None = None
        self._last_failure: tuple[str, str] | None = None

    async def interrupt(self, entity: str | None) -> None:
        if self._session is not None and entity in self._session.marker_frames:
            await self.end(self._session.token)

    async def start(self, players: Any, raw: bool = False) -> dict:
        if self._session is not None:
            raise OpError("На этих колонках уже идёт акустический замер", 409)
        if not isinstance(players, list) or len(players) < 2 or len(players) > 8:
            raise OpError("Выберите от двух до восьми систем")
        entities: list[str] = []
        systems: set[str] = set()
        for player in players:
            tile = find(self._playback._tiles(), player)
            entity = self._playback._entity(tile)
            if entity is None:
                raise OpError("Колонка для замера не найдена", 404)
            system = self._playback._system_of(entity) or entity
            if system in systems:
                raise OpError("Для замера выберите одну колонку каждой системы")
            entity = self._output(entity)
            state = self._playback._home.source.get(entity)
            if state is None or state.state in ("unknown", "unavailable") or state.attributes.get("assumed_state") is True:
                raise OpError(f"{self._label(entity)}: колонка для замера недоступна", 409)
            systems.add(system)
            entities.append(entity)

        binary = await self._playback._home.env.run(shutil.which, "ffmpeg")
        if binary is None:
            raise OpError("В доме нет ffmpeg для акустического замера", 503)
        try:
            session = await asyncio.wait_for(prepare(binary, entities), timeout=8)
        except (OSError, RuntimeError, TimeoutError) as error:
            raise OpError("Дом не смог подготовить маркеры", 503) from error
        delays = {entity: 0 if raw else self._playback._delay(entity) for entity in entities}
        session.delay_of = delays.__getitem__
        self._session = session
        try:
            for entity in entities:
                tile = self._playback._tile_of(entity)
                if tile is not None and not self._playback.multiroom.contains(entity):
                    self._playback._drop_queue(entity, tile)
            results = await asyncio.gather(*(
                asyncio.wait_for(
                    self._playback.play_flow(session, entity), timeout=11,
                )
                for entity in entities
            ), return_exceptions=True)
            for entity, result in zip(entities, results, strict=True):
                if isinstance(result, Exception):
                    label = self._label(entity)
                    reason = "колонка не ответила" if isinstance(result, TimeoutError) else str(result)
                    raise OpError(f"{label}: {reason}", 409) from result
                if isinstance(result, BaseException):
                    raise result
            try:
                await asyncio.wait_for(session.marker_ready.wait(), timeout=MARKER_OPEN_TIMEOUT)
            except TimeoutError as error:
                missing = [self._label(entity) for entity in entities if not session.reading_player(entity)]
                names = ", ".join(missing) or ", ".join(map(self._label, entities))
                LOGGER.warning("Маркеры: истекло ожидание подключения (%s)", names)
                raise OpError(f"{names}: истекло ожидание подключения к потоку маркеров", 409) from error
        except BaseException:
            await self.end(session.token)
            raise
        self._watch = asyncio.create_task(self._watch_players(session))
        return {
            "session": session.token, "marker": layout(len(entities)), "delays": list(delays.values()),
            "raw": raw, "status": True, "continuous": True,
        }

    def status(self, token: Any) -> dict:
        if self._session is not None and self._session.token == token:
            if lost := self._session.marker_interrupted:
                return {"active": False, "reason": self._lost_reason(lost)}
            return {"active": True}
        failed = self._last_failure
        return {"active": False, "reason": failed[1] if failed is not None and failed[0] == token
                else "Сеанс акустического замера завершён"}

    async def _watch_players(self, session: FlowSession) -> None:
        clock = asyncio.get_running_loop().time
        deadline = clock() + CALIBRATION_TIMEOUT
        absent_since: dict[str, float] = {}
        while True:
            await asyncio.sleep(1)
            now = clock()
            for entity in session.marker_frames:
                if session.reading_player(entity):
                    absent_since.pop(entity, None)
                else:
                    absent_since.setdefault(entity, now)
            lost = session.marker_interrupted or next(
                (entity for entity, since in absent_since.items() if now - since >= MARKER_RECONNECT_TIMEOUT), None,
            )
            if lost is None and now < deadline:
                continue
            async with self._playback._group_lock:
                if self._session is not session:
                    return
                if lost is not None and session.marker_interrupted is None and session.reading_player(lost):
                    continue
                reason = self._lost_reason(lost) if lost is not None else "Истекло время акустического замера"
                self._last_failure = (session.token, reason)
                LOGGER.warning("Маркеры: %s", reason)
                await self.end(session.token)
                return

    def _lost_reason(self, entity: str) -> str:
        return f"{self._label(entity)}: пропало подключение к потоку маркеров"

    def _label(self, entity: str) -> str:
        return (self._playback._tile_of(entity) or {}).get("name") or entity

    def _output(self, entity: str) -> str:
        owner = self._playback.multiroom.owner(entity)
        if owner is not None:
            queue = self._playback._players.get(owner)
            if queue is not None and not queue.group_paused and (view := queue.view()) and view.get("track"):
                raise OpError("Остановите музыку в группе перед замером", 409)
        if self._playback._system_of(entity) is not None:
            state = self._playback._home.source.get(entity)
            native = state.attributes.get("group_members") if state is not None else None
            if isinstance(native, (list, tuple)) and native and native[0] != entity:
                delegate = native[0]
                if isinstance(delegate, str) and self._playback._system_of(delegate) == self._playback._system_of(entity):
                    return delegate
                raise OpError("Ведущая колонка системы недоступна для замера", 409)
        return entity

    async def end(self, token: Any, *, validate: bool = False) -> None:
        status = self.status(token) if validate else {"active": True}
        session = self._session
        if session is None or session.token != token:
            if not status["active"]:
                raise OpError(status["reason"], 409)
            return
        playing = [entity for entity in session.marker_frames if session.has_opened(entity)]
        self.close()
        results = await asyncio.gather(*(
            asyncio.wait_for(self._playback.multiroom.stop_player(entity), timeout=5) for entity in playing
        ), return_exceptions=True)
        for entity, result in zip(playing, results, strict=True):
            if isinstance(result, Exception):
                LOGGER.warning("Не удалось остановить маркер на %s: %s", entity, result)
        if not status["active"]:
            raise OpError(status["reason"], 409)

    def close(self) -> None:
        watch, self._watch = self._watch, None
        if watch is not None and watch is not asyncio.current_task():
            watch.cancel()
        if self._session is not None:
            self._playback._flow.retire(self._session)
            self._session = None
