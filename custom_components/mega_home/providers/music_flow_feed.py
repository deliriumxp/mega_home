"""Подача звука в поток сессии: тишина, гашение старого элемента, заранее открытый следующий.

Так устроены потоки Music Assistant и Snapcast: поток к колонке не прерывается НИКОГДА. Пока новый
элемент открывается (ссылка у Яндекса, запуск декодера), идёт тишина — колонка не опустошает буфер и
не набирает его заново (раньше смена трека платила буфер колонки дважды). Ручная смена гасит старое
плавно и включает новое сразу, без фейда; доигранный трек сходится со следующим кроссфейдом, если
следующий уже открыт заранее (`PREFETCH_S` до конца).
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from array import array
from collections.abc import Awaitable, Callable

from .music_fade import FRAME_BYTES
from ..core.const import LOGGER
from .music_ffmpeg import FLOW_CHUNK, FLOW_RATE, STOP_TIMEOUT_S, FlowProcess
from .player_output import MediaItem

# Окно фейда — свойство ПОТОКА, одно на все колонки группы: кроссфейд доигранных элементов и гашение
# старого при ручной смене. Столько звука поток всегда придерживает, поэтому окно короткое.
FADE_MS = 300


def fade_window() -> int:
    """Окно фейда в байтах PCM."""
    return FLOW_RATE * FADE_MS // 1000 * FRAME_BYTES
# Следующий элемент открывается за столько до конца текущего: к стыку он уже звучит.
PREFETCH_S = 15
# Доигранный трек, а следующий ещё открывается: столько ждём его ради кроссфейда, потом — тишина.
PREFETCH_WAIT_S = 2.0
# Кусок тишины, пока новый элемент открывается.
SILENCE_MS = 100

Opened = tuple[FlowProcess, bytes] | None
Opener = Callable[[MediaItem, float], Awaitable[Opened]]


def silence(ms: int) -> bytes:
    return bytes(FLOW_RATE * ms // 1000 * FRAME_BYTES)


def fade_out(pcm: bytes) -> bytes:
    """Линейное гашение до нуля, по кадрам (все каналы отсчёта гаснут вместе)."""
    frames = len(pcm) // FRAME_BYTES
    if not frames or sys.byteorder != "little":  # pragma: no cover — второе недостижимо на парке
        return pcm[: frames * FRAME_BYTES]
    samples = array("h")
    samples.frombytes(pcm[: frames * FRAME_BYTES])
    channels = FRAME_BYTES // 2
    for index in range(len(samples)):
        samples[index] = samples[index] * (frames - index // channels) // frames
    return samples.tobytes()


class Prefetch:
    """Заранее открытый элемент: `start` — открыть, `take` — забрать, если это он, иначе погасить."""

    def __init__(self, opener: Opener) -> None:
        self._opener = opener
        self._id: str | None = None
        self._task: asyncio.Task[Opened] | None = None

    def start(self, item: MediaItem) -> None:
        key = getattr(item, "id", None)
        if key is None or self._id == key:
            return
        self.drop()
        self._id = key
        self._task = asyncio.create_task(self._opener(item, 0.0))

    def take(self, item: MediaItem, start: float) -> asyncio.Task[Opened] | None:
        """Открытие этого элемента, если оно уже идёт с начала; чужое гасится."""
        if self._id is not None and self._id == getattr(item, "id", None) and start <= 0 and self._task is not None:
            task, self._id, self._task = self._task, None, None
            return task
        self.drop()
        return None

    def drop(self) -> None:
        task, self._id, self._task = self._task, None, None
        if task is None:
            return
        if not task.done():
            task.cancel()
            return
        with contextlib.suppress(Exception, asyncio.CancelledError):
            if (opened := task.result()) is not None:
                asyncio.create_task(opened[0].stop())


async def buffer_radio_pcm(process: FlowProcess, buffer: asyncio.Queue[bytes]) -> None:
    try:
        while chunk := await process.read(FLOW_CHUNK):
            await buffer.put(chunk)
    except asyncio.CancelledError:
        raise
    except Exception:
        await buffer.put(b"")
        raise
    await buffer.put(b"")


async def stop_process(process: FlowProcess) -> None:
    """Погасить декодер: элемент доигран, сменён или плеер ушёл."""
    try:
        await asyncio.wait_for(process.stop(), STOP_TIMEOUT_S)
    except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041 — 3.10 и 3.11 зовут по-разному
        LOGGER.warning("Поток сессии: ffmpeg не остановился за %s с", STOP_TIMEOUT_S)


async def read_head(process: FlowProcess, size: int) -> bytes:
    """Первые `size` байт элемента: с ними микшуется придержанный хвост прежнего."""
    if size <= 0:
        return b""
    head = b""
    while len(head) < size:
        chunk = await process.read(size - len(head))
        if not chunk:
            break
        head += chunk
    return head
