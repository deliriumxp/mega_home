"""Запуск и завершение общей сессии акустического замера."""

from __future__ import annotations

import asyncio
import shutil
from typing import TYPE_CHECKING, Any

from .core.const import LOGGER
from .core.ops import call
from .core.ops_base import OpError, find
from .providers.calibration import layout, prepare
from .providers.music_flow import FlowSession

if TYPE_CHECKING:
    from .music_playback import MusicPlayback

MARKER_OPEN_TIMEOUT = 5


class MusicCalibration:
    def __init__(self, playback: MusicPlayback) -> None:
        self._playback = playback
        self._session: FlowSession | None = None
        self._timer: asyncio.TimerHandle | None = None

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
            fields = [(entity, self._playback._stream_output_fields(session, entity)) for entity in entities]
            for entity, _ in fields:
                tile = self._playback._tile_of(entity)
                if tile is not None and not self._playback.multiroom.contains(entity):
                    self._playback._drop_queue(entity, tile)
            results = await asyncio.gather(*(
                asyncio.wait_for(
                    call(self._playback._home.source, "media_player", "play_media", {"entity_id": entity, **data}),
                    timeout=8,
                )
                for entity, data in fields
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
        self._timer = asyncio.get_running_loop().call_later(
            300, lambda: asyncio.create_task(self._expire(session.token)),
        )
        return {"session": session.token, "marker": layout(len(entities)), "delays": list(delays.values()), "raw": raw}

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

    async def _expire(self, token: str) -> None:
        async with self._playback._group_lock:
            await self.end(token)

    async def end(self, token: Any) -> None:
        session = self._session
        if session is None or session.token != token:
            return
        playing = [entity for entity in session.marker_frames if session.has_opened(entity)]
        self.close()
        results = await asyncio.gather(*(
            asyncio.wait_for(self._playback.multiroom.stop_player(entity), timeout=5) for entity in playing
        ), return_exceptions=True)
        for entity, result in zip(playing, results, strict=True):
            if isinstance(result, Exception):
                LOGGER.warning("Не удалось остановить маркер на %s: %s", entity, result)

    def close(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._session is not None:
            self._playback._flow.retire(self._session)
            self._session = None
