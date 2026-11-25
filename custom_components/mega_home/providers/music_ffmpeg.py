"""Процессы ffmpeg потока сессии: декодер элемента в PCM и кодировщик провода в MP3.

Формат провода — как у Music Assistant для плееров из Home Assistant (`providers/hass_players`,
`DEFAULT_PLAYER_CONFIG_ENTRIES = CONF_ENTRY_OUTPUT_CODEC_DEFAULT_MP3`; `helpers/ffmpeg.py`,
`DEFAULT_MP3_BIT_RATE = 320`): мы тоже отдаём звук каждой колонке через `media_player.play_media`
HA. MP3 берут все наши пути — Sonos схемой `x-rincon-mp3radio`, станция `stream.mp3`, ESPHome
(HA перекодирует под устройство сам). Стерео: моно-провод сводил музыку в один канал.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

from ..core.const import LOGGER

FLOW_RATE = 48_000
FLOW_CHANNELS = 2
FLOW_WIDTH = 2
FLOW_CHUNK = 64 * 1024
MP3_BITRATE_K = 320
MP3_FRAME_MS = 24
# 1152 отсчёта на кадр при 48 кГц и CBR без бита дополнения: 1152 / 48000 * 320000 / 8 = 960 байт.
MP3_FRAME_BYTES = 1152 * MP3_BITRATE_K * 1000 // 8 // FLOW_RATE
STOP_TIMEOUT_S = 5


class FlowProcess:
    """Процесс потока: чтение звука (декодер) или запись (кодировщик) и остановка."""

    async def read(self, size: int) -> bytes:  # pragma: no cover — интерфейс
        raise NotImplementedError

    async def write(self, chunk: bytes) -> None:  # pragma: no cover — интерфейс
        raise NotImplementedError

    async def stop(self) -> None:  # pragma: no cover — интерфейс
        raise NotImplementedError


class FfmpegProcess(FlowProcess):
    """Процесс ffmpeg: декодер (PCM в трубе чтения) или кодировщик (PCM в трубе записи)."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self._process = process

    async def read(self, size: int) -> bytes:
        if self._process.stdout is None:  # pragma: no cover — трубу заказывает сам декодер
            return b""
        chunk = await self._process.stdout.read(size)
        if not chunk and (exit_code := await self._process.wait()):
            LOGGER.warning("Аудиопоток: FFmpeg завершил чтение источника с кодом %s", exit_code)
        return chunk

    async def write(self, chunk: bytes) -> None:
        stdin = self._process.stdin
        if stdin is None:  # pragma: no cover — трубу заказывает сам кодировщик
            return
        try:
            stdin.write(chunk)
            await stdin.drain()
        except (ConnectionError, BrokenPipeError):
            # Плеер ушёл — дописывать некуда; закрытие разберёт `stop`.
            return

    async def stop(self) -> None:
        process = self._process
        if process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        with contextlib.suppress(ProcessLookupError):
            await process.wait()


async def ffmpeg_decoder(binary: str, url: str, start: float = 0.0, *, radio: bool = False) -> FlowProcess:
    """Элемент → PCM 48 кГц стерео одной трубой: декодируем, а не перекодируем.

    `start` — перемотка внутри элемента: `-ss` до входа, ffmpeg ищет ближайший ключевой кадр и
    докодирует до точного места. Плееру при этом команда не нужна — поток продолжается внутри.
    Так звук не теряет качества на стыке, а склейка элементов идёт на PCM: ей формат источника не
    важен (MP3 Яндекса и AAC станции в одном потоке). Цена — процессор дома на играющий плеер;
    MA платит тем же (он ещё и перекодирует в FLAC/MP3, у нас играет исходный звук).
    """
    seek = [] if start <= 0 else ["-ss", f"{start:.3f}"]
    # Политика HTTP-повторов Music Assistant для прямого входа ffmpeg.
    reconnect = (
        ["-reconnect", "1", "-reconnect_delay_max", "10", "-reconnect_streamed", "1",
         "-reconnect_on_http_error", "5xx,429", "-reconnect_on_network_error", "1",
         "-rw_timeout", "10000000"]
        if url.startswith(("http://", "https://")) else []
    )
    if radio and reconnect:
        reconnect += ["-reconnect_at_eof", "1"]
    return FfmpegProcess(await asyncio.create_subprocess_exec(
        binary,
        *seek,
        *reconnect,
        "-i", url,
        "-f", "s16le", "-ar", str(FLOW_RATE), "-ac", str(FLOW_CHANNELS),
        "-loglevel", "error", "-nostats",
        "pipe:",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    ))


def mp3_args(channels: int = FLOW_CHANNELS, bitrate_k: int = MP3_BITRATE_K) -> tuple[str, ...]:
    """Кодирование PCM s16le 48 кГц из трубы в MP3 CBR без резервуара: кадры независимы, выход
    может начать чтение с любого кадра потока."""
    return (
        "-f", "s16le", "-ar", str(FLOW_RATE), "-ac", str(channels), "-i", "pipe:",
        "-f", "mp3", "-codec:a", "libmp3lame", "-b:a", f"{bitrate_k}k",
        "-write_xing", "0", "-id3v2_version", "0", "-reservoir", "0",
        "-loglevel", "error", "-nostats", "pipe:",
    )


async def ffmpeg_encoder(binary: str, write: Callable[[bytes], Awaitable[None]]) -> FlowProcess:
    """PCM 48 кГц стерео → MP3 320 кбит/с: провод для участников, которые объявляют свой формат.

    ⚠ Один на сессию: склейка и фейд идут на PCM, а провод у группы один (Яндекс Станция берёт
    только `stream.mp3`/`stream.aac`; ESPHome и Sonos читают что дадут — HA им перекодирует сам).
    """
    process = await asyncio.create_subprocess_exec(
        binary,
        *mp3_args(),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return Mp3Encoder(process, write)


class Mp3Encoder(FlowProcess):
    """Кодировщик сессии: PCM в трубу записи, MP3 из трубы чтения — насосом плееру."""

    def __init__(
        self, process: asyncio.subprocess.Process, write: Callable[[bytes], Awaitable[None]]
    ) -> None:
        self._process = process
        self._write = write
        self._pump = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        stdout = self._process.stdout
        if stdout is None:  # pragma: no cover — трубу заказывает сам кодировщик
            return
        try:
            # Кадр провода — ровно `MP3_FRAME_BYTES` (CBR при 48 кГц без дополнения).
            while chunk := await stdout.readexactly(MP3_FRAME_BYTES):
                await self._write(chunk)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return  # плеер ушёл — остановку разберёт `stop`

    async def read(self, size: int) -> bytes:  # pragma: no cover — у кодировщика читает насос
        return b""

    async def write(self, chunk: bytes) -> None:
        stdin = self._process.stdin
        if stdin is None or stdin.is_closing():  # pragma: no cover
            return
        try:
            stdin.write(chunk)
            await stdin.drain()
        except (ConnectionError, BrokenPipeError):
            return

    async def stop(self) -> None:
        process = self._process
        if process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
        try:
            await asyncio.wait_for(self._pump, STOP_TIMEOUT_S)
        except (TimeoutError, asyncio.TimeoutError):
            self._pump.cancel()
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        with contextlib.suppress(ProcessLookupError):
            await process.wait()

