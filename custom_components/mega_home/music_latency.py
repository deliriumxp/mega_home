"""Замер статической задержки системы в потоке дома и расчёт сдвига читателей группы.

Зачем: колонки разных вендоров держат звук по-разному (буфер, декодер, сеть), и смешанная группа
расходится на сотни миллисекунд. Задержка — свойство МОДЕЛИ (`stream.latencyMs` плитки: менеджер
кладёт замер системы всем её колонкам), поэтому дом выравнивает группу по замерам на лету, а не по
ручной правке каждой колонки. Микрофон телефона слышит щелчки (`providers/music_chirp.py`), дом
отдаёт времена их рассылки (`calibration_result`), приложение вычитает.
"""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .core.const import LOGGER
from .core.ops_base import OpError, find
from .providers.music_chirp import ChirpProcess, chirp_description
from .providers.music_flow import FlowSession
from .providers.player_output import MediaItem

if TYPE_CHECKING:
    from .music_playback import MusicPlayback

CONNECT_TIMEOUT_S = 15
STALE_S = 300
NOT_CONNECTED = "Колонка не начала играть поток дома"
DISCONNECTED = "Колонка прервала поток дома"
INTERRUPTED = "Замер прерван: на колонке включили другое воспроизведение"


@dataclass
class _Measure:
    entity: str
    tile_id: str
    session: FlowSession
    started: float
    done: asyncio.Event = field(default_factory=asyncio.Event)
    result: dict | None = None


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
        # ⚠ Без фейда и без сдвига: отметка щелчка — момент ухода куска в кодировщик, как есть.
        session.fade_bytes = 0
        session.delay_of = None
        item = MediaItem(id="calibration", title="Калибровка")
        measure = _Measure(entity, tile["id"], session, asyncio.get_running_loop().time())

        async def source_of(_item: MediaItem) -> str:
            return "calibration"

        async def on_end() -> None:
            measure.done.set()

        session.source_of, session.on_end = source_of, on_end
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
        return {"token": token}

    async def result(self, token: Any) -> dict:
        measure = self._measures.get(token) if isinstance(token, str) else None
        if measure is None:
            raise OpError("Неизвестный замер", 404)
        if measure.result is not None:
            return measure.result
        session = measure.session
        clock = asyncio.get_running_loop().time()
        if measure.done.is_set():
            outcome: dict = {"done": True, "sent": list(session.probe_sent), "chirp": chirp_description()}
        elif not session.has_opened(measure.entity):
            if clock - measure.started < CONNECT_TIMEOUT_S:
                return {"done": False}
            outcome = {"done": True, "error": NOT_CONNECTED}
        elif not session.reading:
            outcome = {"done": True, "error": DISCONNECTED}
        else:
            return {"done": False}
        measure.result = outcome
        await self._finish(measure)
        return outcome

    def interrupt(self, entity: str | None) -> None:
        """Обычное воспроизведение на колонке прерывает замер: его сессию гасим, итог — ошибка."""
        for measure in self._measures.values():
            if measure.entity == entity and measure.result is None:
                measure.result = {"done": True, "error": INTERRUPTED}
                self._playback._flow.drop(measure.tile_id)

    async def _finish(self, measure: _Measure) -> None:
        playback = self._playback
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
        self._measures.clear()
