"""Замер статической задержки системы в потоке дома и расчёт сдвига читателей группы.

Зачем: колонки разных вендоров держат звук по-разному (буфер, декодер, сеть), и смешанная группа
расходится на сотни миллисекунд. Задержка — свойство МОДЕЛИ (`stream.latencyMs` плитки: менеджер
кладёт замер системы всем её колонкам), поэтому дом выравнивает группу по замерам на лету, а не по
ручной правке каждой колонки. Микрофон телефона слышит ноты мелодии (`providers/music_melody.py`), дом
отдаёт времена их рассылки (`calibration_result`), приложение вычитает.

Мелодия звучит живым потоком, пока приложение не скажет `calibration_stop` (услышало, отказ, отмена) —
схема Apple TV Wireless Audio Sync. Телефон закрыли посреди замера — колонку гасит сторож: без опроса
`IDLE_S` или дольше `LIMIT_S` от начала.

Проверка (`calibration_check`) — та же мелодия синхронно на выбранных колонках с ЕЩЁ НЕ СОХРАНЁННЫМИ
задержками: тем же механизмом, что у группы (один поток, сдвиг читателя `max − своя`), поэтому жилец
слышит ровно то, что будет в группе, и сохраняет, только услышав.
"""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .core.const import LOGGER
from .core.ops_base import OpError, find
from .providers.music_melody import MelodyProcess, loop_pcm, melody_description
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
CHECK_LIMIT = 8
MAX_LATENCY_MS = 5000
STOPPED = {"done": True, "stopped": True}


@dataclass
class _Measure:
    entities: list[str]
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
        """Мелодия на одной колонке; ход — `calibration_result`."""
        return await self._start([self._speaker(player)], None)

    async def check(self, players: Any) -> dict:
        """Мелодия синхронно на колонках `[{player, latencyMs}]` — со сдвигом, как в группе."""
        if not isinstance(players, list) or not 2 <= len(players) <= CHECK_LIMIT:
            raise OpError(f"Для проверки нужно от 2 до {CHECK_LIMIT} колонок")
        speakers, latencies = [], {}
        for row in players:
            latency = row.get("latencyMs") if isinstance(row, dict) else None
            if type(latency) is not int or not 0 <= latency <= MAX_LATENCY_MS:
                raise OpError("Задержка колонки — целое от 0 до 5000 мс")
            tile, entity = self._speaker(row.get("player"))
            if entity in latencies:
                raise OpError("Колонка указана дважды")
            speakers.append((tile, entity))
            latencies[entity] = latency
        top = max(latencies.values())
        return await self._start(speakers, lambda entity: top - latencies.get(entity, top))

    def _speaker(self, player: Any) -> tuple[dict, str]:
        playback = self._playback
        tile = find(playback._tiles(), player)
        entity = playback._entity(tile)
        if not entity or tile is None:
            raise OpError("Колонка для замера не найдена", 404)
        if playback.multiroom.contains(entity):
            raise OpError("Колонка в группе: разъедините её перед замером", 409)
        return tile, entity

    async def _start(self, speakers: list[tuple[dict, str]], delay_of: Callable[[str], int] | None) -> dict:
        playback = self._playback
        # Круг мелодии считается один раз — не в цикле событий.
        await asyncio.get_running_loop().run_in_executor(None, loop_pcm)
        for tile, entity in speakers:
            self.interrupt(entity)
            playback._drop_queue(entity, tile)
        lead = speakers[0][0]["id"]
        session = playback._flow.session(lead)
        # ⚠ Без фейда: отметка ноты — момент, когда её кадр ушёл колонке, как есть.
        session.fade_bytes = 0
        session.delay_of = delay_of
        item = MediaItem(id="calibration", title="Калибровка")
        clock = asyncio.get_running_loop().time()
        measure = _Measure([entity for _, entity in speakers], lead, session, clock, clock)

        async def source_of(_item: MediaItem) -> str:
            return "calibration"

        session.source_of, session.on_end = source_of, None
        session.generated = lambda _item: MelodyProcess()
        session.set_items((item,), 0)
        token = secrets.token_urlsafe(9)
        self._measures[token] = measure
        try:
            for _, entity in speakers:
                await playback.play_flow(session, entity)
        except BaseException:
            self._measures.pop(token, None)
            measure.result = {"done": True, "error": NOT_CONNECTED}
            await self._finish(measure)
            raise
        measure.watchdog = asyncio.create_task(self._watch(measure))
        return {"token": token}

    async def result(self, token: Any) -> dict:
        """Ход: `{done:false, sent:[{at, note}], notes}` — уже отправленные ноты; отказ — `done` с `error`."""
        measure = self._get(token)
        if measure.result is not None:
            return measure.result
        session = measure.session
        clock = asyncio.get_running_loop().time()
        measure.polled = clock
        if not all(session.has_opened(entity) for entity in measure.entities):
            if clock - measure.started < CONNECT_TIMEOUT_S:
                return {"done": False, "sent": [], "notes": melody_description()}
            return await self._end(measure, {"done": True, "error": NOT_CONNECTED})
        if not session.reading:
            return await self._end(measure, {"done": True, "error": DISCONNECTED})
        return {"done": False, "sent": list(session.probe_sent), "notes": melody_description()}

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
            if entity in measure.entities and measure.result is None:
                measure.result = {"done": True, "error": INTERRUPTED}
                self._playback._flow.drop(measure.tile_id)

    async def _finish(self, measure: _Measure) -> None:
        playback = self._playback
        if measure.watchdog is not None and measure.watchdog is not asyncio.current_task():
            measure.watchdog.cancel()
        self._forget_old()
        for entity in measure.entities:
            try:
                await asyncio.wait_for(playback.multiroom.stop_player(entity), timeout=5)
            except Exception as error:  # noqa: BLE001 — гашение колонки не должно терять итог замера
                LOGGER.warning("Не удалось остановить калибровку на %s: %s", entity, error)
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
