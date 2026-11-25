"""Замер статической задержки системы в потоке дома и расчёт сдвига читателей группы.

Зачем: колонки разных вендоров держат звук по-разному (буфер, декодер, сеть), и смешанная группа
расходится на сотни миллисекунд. Задержка — свойство МОДЕЛИ (`stream.latencyMs` плитки: менеджер
кладёт замер системы всем её колонкам), поэтому дом выравнивает группу по замерам на лету, а не по
ручной правке каждой колонки. Микрофон телефона слышит тоны (`providers/music_chirp.py`), дом
отдаёт времена их рассылки (`calibration_result`), приложение вычитает.

Тоны звучат живым потоком, пока приложение не скажет `calibration_stop` (услышало, отказ, отмена) —
схема Apple TV Wireless Audio Sync. Телефон закрыли посреди замера — колонку гасит сторож: без опроса
`IDLE_S` или дольше `LIMIT_S` от начала.
"""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .core.const import LOGGER
from .core.ops_base import OpError, find
from .providers.music_chirp import ChirpProcess, chirp_description
from .providers.music_flow import FlowSession
from .providers.player_output import MediaItem

if TYPE_CHECKING:
    from .music_playback import MusicPlayback

CONNECT_TIMEOUT_S = 15
IDLE_S = 10
LIMIT_S = 120
STALE_S = 300
NOT_CONNECTED = "Колонка не начала играть поток дома"
DISCONNECTED = "Колонка прервала поток дома"
INTERRUPTED = "Замер прерван: на колонке включили другое воспроизведение"
ABANDONED = "Замер остановлен: приложение перестало его спрашивать"
STOPPED = {"done": True, "stopped": True}


@dataclass
class _Measure:
    entity: str
    tile_id: str
    session: FlowSession
    started: float
    polled: float
    result: dict | None = None
    watchdog: asyncio.Task | None = None


def latency_of(tile: dict | None) -> int:
    value = ((tile or {}).get("stream") or {}).get("latencyMs")
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0 else 0


class MusicLatency:
    def __init__(self, playback: MusicPlayback) -> None:
        self._playback = playback
        self._measures: dict[str, _Measure] = {}

    def delay(self, entity: str, max_ms: int) -> int:
        """Сдвиг читателя: самая поздняя система группы минус эта; вендорских участников не считаем."""
        multiroom = self._playback.multiroom
        owner = multiroom.owner(entity) or entity
        members = [owner, *(m for m in multiroom.groups.get(owner, []) if m not in multiroom._vendor)]
        if entity not in members or len(members) < 2:
            return 0
        latencies = [latency_of(self._playback._tile_of(member)) for member in members]
        shift = max(latencies) - latency_of(self._playback._tile_of(entity))
        return max(0, min(int(shift), max_ms))

    @staticmethod
    def ping() -> dict:
        return {"now": asyncio.get_running_loop().time() * 1000}

    async def measure(self, player: Any) -> dict:
        playback = self._playback
        tile = find(playback._tiles(), player)
        entity = playback._entity(tile)
        if not entity or tile is None:
            raise OpError("Колонка для замера не найдена", 404)
        if playback.multiroom.contains(entity):
            raise OpError("Колонка в группе: разъедините её перед замером", 409)
        self.interrupt(entity)
        playback._drop_queue(entity, tile)
        session = playback._flow.session(tile["id"])
        # ⚠ Без фейда и без сдвига: отметка тона — момент ухода куска в кодировщик, как есть.
        session.fade_bytes = 0
        session.delay_of = None
        item = MediaItem(id="calibration", title="Калибровка")
        clock = asyncio.get_running_loop().time()
        measure = _Measure(entity, tile["id"], session, clock, clock)

        async def source_of(_item: MediaItem) -> str:
            return "calibration"

        session.source_of, session.on_end = source_of, None
        session.generated = lambda _item: ChirpProcess()
        session.set_items((item,), 0)
        token = secrets.token_urlsafe(9)
        self._measures[token] = measure
        try:
            await playback.play_flow(session, entity)
        except BaseException:
            self._measures.pop(token, None)
            playback._flow.drop(tile["id"])
            raise
        measure.watchdog = asyncio.create_task(self._watch(measure))
        return {"token": token}

    async def result(self, token: Any) -> dict:
        """Ход замера: `{done:false, sent, chirp}` — времена уже отправленных тонов; отказ — `done` с `error`."""
        measure = self._get(token)
        if measure.result is not None:
            return measure.result
        session = measure.session
        clock = asyncio.get_running_loop().time()
        measure.polled = clock
        if not session.has_opened(measure.entity):
            if clock - measure.started < CONNECT_TIMEOUT_S:
                return {"done": False, "sent": [], "chirp": chirp_description()}
            return await self._end(measure, {"done": True, "error": NOT_CONNECTED})
        if not session.reading:
            return await self._end(measure, {"done": True, "error": DISCONNECTED})
        return {"done": False, "sent": list(session.probe_sent), "chirp": chirp_description()}

    async def stop(self, token: Any) -> dict:
        """Приложение кончило замер (услышало, отказ, отмена): тоны гасим. Повтор безвреден."""
        measure = self._get(token)
        if measure.result is None:
            await self._end(measure, STOPPED)
        return {"stopped": True}

    def _get(self, token: Any) -> _Measure:
        measure = self._measures.get(token) if isinstance(token, str) else None
        if measure is None:
            raise OpError("Неизвестный замер", 404)
        return measure

    async def _end(self, measure: _Measure, outcome: dict) -> dict:
        measure.result = outcome
        await self._finish(measure)
        return outcome

    async def _watch(self, measure: _Measure) -> None:
        loop = asyncio.get_running_loop()
        while measure.result is None:
            await asyncio.sleep(1)
            now = loop.time()
            if measure.result is None and (now - measure.polled > IDLE_S or now - measure.started > LIMIT_S):
                async with self._playback._group_lock:
                    if measure.result is None:
                        await self._end(measure, {"done": True, "error": ABANDONED})

    def interrupt(self, entity: str | None) -> None:
        """Обычное воспроизведение на колонке прерывает замер: его сессию гасим, итог — ошибка."""
        for measure in self._measures.values():
            if measure.entity == entity and measure.result is None:
                measure.result = {"done": True, "error": INTERRUPTED}
                self._playback._flow.drop(measure.tile_id)

    async def _finish(self, measure: _Measure) -> None:
        playback = self._playback
        if measure.watchdog is not None and measure.watchdog is not asyncio.current_task():
            measure.watchdog.cancel()
        self._forget_old()
        try:
            await asyncio.wait_for(playback.multiroom.stop_player(measure.entity), timeout=5)
        except Exception as error:  # noqa: BLE001 — гашение колонки не должно терять итог замера
            LOGGER.warning("Не удалось остановить калибровку на %s: %s", measure.entity, error)
        playback._flow.drop(measure.tile_id)

    def _forget_old(self) -> None:
        now = asyncio.get_running_loop().time()
        for token, measure in list(self._measures.items()):
            if measure.result is not None and now - measure.started > STALE_S:
                del self._measures[token]

    def close(self) -> None:
        for measure in self._measures.values():
            if measure.watchdog is not None:
                measure.watchdog.cancel()
        self._measures.clear()
