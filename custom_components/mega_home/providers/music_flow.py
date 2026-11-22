"""Один поток на сессию плеера: адрес не меняется, элементы сменяются внутри открытого ответа.

Зачем (живой случай 2026-11-22, прошитый Amazon Echo — медиаплеер ESPHome): плеер не берёт новый
`media_url` поверх живого потока, а мы посылали ему команду на КАЖДЫЙ элемент — переключение
работало только после «Стоп». Music Assistant решает это тем же приёмом: для плееров из HA он
всегда включает flow mode («For HA media players flow mode is always used… the wide variation in
HA player capabilities makes it the most reliable playback method»,
music-assistant.io/player-support/home-assistant/). Ось «как устройство берёт поток» — свойство
плеера (`stream.flow` описания плитки), а не обход бага.

Звук — WAV 48 кГц моно s16: размеры в заголовке «неизвестно» (`0xFFFFFFFF`), склейка идёт на PCM,
каждый элемент декодируется своим ffmpeg (`-f s16le`, только декодирование). Такой поток читает и
HA (прокси ffmpeg у ESPHome-плеера по `supported_formats`), и плеер, который умеет WAV.

⚠ Сессия — только про ЗВУК и порядок элементов. Что играет (очередь, каталог, подписи, обложки)
остаётся у очереди дома (`music_playback.py`, `providers/media_queue.py`): адрес сессии лишь ещё
один способ отдать элемент плееру — тем же `Output`, что и ссылка на трек.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import struct
import sys
from array import array
from collections.abc import Awaitable, Callable

from ..core.const import LOGGER, URL_PREFIX
from .player_output import MediaItem

FLOW_RATE = 48_000
FLOW_CHANNELS = 1
FLOW_WIDTH = 2
FLOW_CHUNK = 64 * 1024
STOP_TIMEOUT_S = 5

# Потолок размера — «неизвестно»: поток идёт, пока играет очередь. Так же отдают поток живые
# источники, и ffmpeg/плееры это принимают (иначе размер пришлось бы знать заранее).
UNKNOWN_SIZE = 0xFFFFFFFF


def wav_header() -> bytes:
    """Заголовок WAV на весь поток: RIFF + fmt PCM + data неизвестной длины."""
    block = FLOW_CHANNELS * FLOW_WIDTH
    fmt = struct.pack("<HHIIHH", 1, FLOW_CHANNELS, FLOW_RATE, FLOW_RATE * block, block, FLOW_WIDTH * 8)
    return (
        b"RIFF" + struct.pack("<I", UNKNOWN_SIZE) + b"WAVE"
        + b"fmt " + struct.pack("<I", len(fmt)) + fmt
        + b"data" + struct.pack("<I", UNKNOWN_SIZE)
    )


# Чем дом декодирует элемент: свой процесс на элемент, звук — PCM в трубу. Тесты подменяют это.
Decoder = Callable[[str], Awaitable["FlowProcess"]]


class FlowProcess:
    """Процесс потока: чтение звука (декодер) или запись (кодировщик) и остановка."""

    async def read(self, size: int) -> bytes:  # pragma: no cover — интерфейс
        raise NotImplementedError

    async def write(self, chunk: bytes) -> None:  # pragma: no cover — интерфейс
        raise NotImplementedError

    async def stop(self) -> None:  # pragma: no cover — интерфейс
        raise NotImplementedError


class FlowSession:
    """Сессия одного плеера: элементы, позиция в них и признак «устройство читает поток».

    ⚠ Игроки сессии — очередь дома: она задаёт `items`/`index` (`set_items`) и просит следующего
    (`on_end`). Сессия не решает, что играть, — только отдаёт это одним потоком.
    """

    def __init__(self, token: str) -> None:
        self.token = token
        self.items: tuple[MediaItem, ...] = ()
        self.index = 0
        self.source_of: Callable[[MediaItem], Awaitable[str | None]] | None = None
        self.on_end: Callable[[], Awaitable[None]] | None = None
        # Устройство начало читать поток: очередь дома по этому признаку подтверждает запуск
        # (`media_queue.py`, `Attempt.opened`) — у плеера, который ничего о себе не сообщает,
        # другого подтверждения нет и быть не может.
        self.on_read: Callable[[], None] | None = None
        # Окно фейда на стыке элементов, байты PCM (0 — без фейда). Ставит владелец очереди из
        # описания плитки (`fadeMs`): фейд нужен и на смене ТРЕКА, и на смене станции.
        self.fade_bytes = 0
        # Формат провода: `wav` (PCM как есть, декодирование без перекодирования) или `mp3`
        # (когда участник группы объявляет СВОИ типы: Яндекс Станция берёт только `stream.mp3`/
        # `stream.aac` — WAV ей не отдать). Кодек один на группу: участники читают один поток.
        self.codec = "wav"
        # Элемент сменён (next/previous/замена очереди): текущий ffmpeg гасим, поток продолжается.
        self.switch = asyncio.Event()
        # Устройство читает прямо сейчас: с открытым потоком команда плееру не нужна.
        self.reading = False

    @property
    def current(self) -> MediaItem | None:
        """Элемент, который играет сейчас; `None` — очередь пуста."""
        return self.items[self.index] if 0 <= self.index < len(self.items) else None

    def set_items(self, items: tuple[MediaItem, ...], index: int) -> None:
        """Очередь и место в ней — от владельца (очереди дома). Меняет звук, не адрес."""
        self.items, self.index = tuple(items), index
        self.switch.set()

    def stop(self) -> None:
        """Снять звук: очередь снята или плеер оставлен. Поток закроется на ближайшем куске."""
        self.items, self.index = (), 0
        self.switch.set()

    async def play(
        self, write: Callable[[bytes], Awaitable[None]], decoder: Decoder,
        encoder: Callable[[Callable[[bytes], Awaitable[None]]], Awaitable["FlowProcess"]] | None = None,
    ) -> None:
        """Отдать поток до конца очереди. `write` без ответа плеера — поток кончился, выходим.

        Стык элементов смешивается на `fade_bytes` (фейд, `_Fader`): хвост прежнего элемента
        придерживается и микшуется с головкой следующего — и на доигранном треке, и когда элемент
        сменили на ходу (станция, «дальше»).

        `encoder` — только когда провод не PCM (`codec == "mp3"`): звук идёт в него, а плееру
        отдаётся его выход. Фейд и склейка остаются на PCM — кодировщик один на всю сессию.
        """
        pcm = write
        sink: FlowProcess | None = None
        if self.codec == "mp3" and encoder is not None:
            sink = await encoder(write)
            pcm = sink.write
        if self.codec == "wav":
            await pcm(wav_header())
        self.reading = True
        if self.on_read is not None:
            on_read, self.on_read = self.on_read, None
            on_read()
        fader = _Fader(self.fade_bytes)
        try:
            while (item := self.current) is not None:
                if self.source_of is None:
                    return
                source = await self.source_of(item)
                if source is None:
                    LOGGER.warning("Поток сессии %s: источник не отдал ссылку", self.token)
                    return
                self.switch.clear()
                process = await decoder(source)
                try:
                    # Головку нового элемента читаем сразу: с ней микшуется придержанный хвост.
                    out = fader.join(await _read_head(process, self.fade_bytes))
                    if out:
                        await pcm(out)
                    while True:
                        chunk = await process.read(FLOW_CHUNK)
                        if not chunk:
                            break
                        out = fader.feed(chunk)
                        if out:
                            await pcm(out)
                        # Смену элемента замечаем на ближайшем куске звука: он приходит постоянно.
                        if self.switch.is_set() or self.current is not item:
                            break
                finally:
                    await _stop(process)
                if self.switch.is_set() or self.current is not item:
                    continue
                if self.on_end is not None:
                    await self.on_end()
                if self.current is item:  # очередь кончилась: дописываем придержанное и закрываем
                    if tail := fader.flush():
                        await pcm(tail)
                    return
        finally:
            self.reading = False
            if sink is not None:
                await sink.stop()


async def _stop(process: FlowProcess) -> None:
    """Погасить декодер: элемент доигран, сменён или плеер ушёл."""
    try:
        await asyncio.wait_for(process.stop(), STOP_TIMEOUT_S)
    except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041 — 3.10 и 3.11 зовут по-разному
        LOGGER.warning("Поток сессии: ffmpeg не остановился за %s с", STOP_TIMEOUT_S)


async def _read_head(process: FlowProcess, size: int) -> bytes:
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


class _Fader:
    """Фейд на стыке элементов: хвост придерживается и микшуется с головкой следующего.

    ⚠ Хвост держим ВСЕГДА (в размере окна фейда): к моменту стыка его иначе уже не вернуть — звук
    ушёл плееру. Цена — задержка первого звука сессии на окно фейда (сотни миллисекунд).

    Фейд делается и когда элемент сменили на ходу (другая станция, «дальше»): это тот же стык.
    """

    def __init__(self, fade: int) -> None:
        self._fade = max(0, fade)
        self._held = b""

    def feed(self, chunk: bytes) -> bytes:
        """Отдать всё, кроме хвоста окна: он ждёт стыка."""
        if not self._fade:
            return chunk
        data = self._held + chunk
        if len(data) <= self._fade:
            self._held = data
            return b""
        self._held = data[-self._fade :]
        return data[: -self._fade]

    def join(self, head: bytes) -> bytes:
        """Смешать придержанный хвост с головкой нового элемента: микс и остатки обоих кусков."""
        held, self._held = self._held, b""
        if not held:
            return head
        border = min(len(held), len(head)) // 2 * 2
        rest = held[border:] if len(held) > len(head) else head[border:]
        return mix_pcm(held[:border], head[:border]) + rest

    def flush(self) -> bytes:
        """Поток кончился: придержанное дописываем как есть."""
        held, self._held = self._held, b""
        return held


def mix_pcm(tail: bytes, head: bytes) -> bytes:
    """Кроссфейд кусков PCM s16le: хвост гаснет, головка нарастает (линейно).

    ⚠ `array('h')` читает РОДНОЙ порядок байт: PCM у нас little-endian, и на x86/ARM (где живут
    дома) это одно и то же. Дом на big-endian — фейда не делаем: отдаём хвост как есть (шум хуже
    стыка). Длина микса — по короткому куску, байты не теряются.
    """
    count = min(len(tail), len(head)) // 2
    if not count or sys.byteorder != "little":  # pragma: no cover — второе недостижимо на парке
        return tail[: count * 2]
    out, incoming = array("h"), array("h")
    out.frombytes(tail[: count * 2])
    incoming.frombytes(head[: count * 2])
    for index in range(count):
        out[index] = (out[index] * (count - index) + incoming[index] * index) // count
    return out.tobytes()


class FfmpegProcess(FlowProcess):
    """Процесс ffmpeg: декодер (PCM в трубе чтения) или кодировщик (PCM в трубе записи)."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self._process = process

    async def read(self, size: int) -> bytes:
        if self._process.stdout is None:  # pragma: no cover — трубу заказывает сам декодер
            return b""
        return await self._process.stdout.read(size)

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


async def ffmpeg_decoder(binary: str, url: str) -> FlowProcess:
    """Элемент → PCM 48 кГц моно одной трубой: декодируем, а не перекодируем.

    Так звук не теряет качества на стыке, а склейка элементов идёт на PCM: ей формат источника не
    важен (MP3 Яндекса и AAC станции в одном потоке). Цена — процессор дома на играющий плеер;
    MA платит тем же (он ещё и перекодирует в FLAC/MP3, у нас играет исходный звук).
    """
    return FfmpegProcess(await asyncio.create_subprocess_exec(
        binary,
        "-i", url,
        "-f", "s16le", "-ar", str(FLOW_RATE), "-ac", str(FLOW_CHANNELS),
        "-loglevel", "error", "-nostats",
        "pipe:",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    ))


async def ffmpeg_encoder(binary: str, write: Callable[[bytes], Awaitable[None]]) -> FlowProcess:
    """PCM 48 кГц моно → MP3 (192 кбит/с): провод для участников, которые объявляют свой формат.

    ⚠ Один на сессию: склейка и фейд идут на PCM, а провод у группы один (Яндекс Станция берёт
    только `stream.mp3`/`stream.aac`; ESPHome и Sonos читают что дадут — HA им перекодирует сам).
    """
    process = await asyncio.create_subprocess_exec(
        binary,
        "-f", "s16le", "-ar", str(FLOW_RATE), "-ac", str(FLOW_CHANNELS), "-i", "pipe:",
        "-f", "mp3", "-codec:a", "libmp3lame", "-b:a", "192k",
        "-loglevel", "error", "-nostats", "pipe:",
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
            while chunk := await stdout.read(FLOW_CHUNK):
                await self._write(chunk)
        except (ConnectionError, OSError):
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


class FlowStreams:
    """Адреса сессий: токен → сессия, один на плеер (адрес не меняется между элементами).

    Адрес — сам себе пропуск (случайный токен, `providers/music_proxy.py`): плеер в локальной сети
    учётки не приносит.
    """

    def __init__(self, origin: Callable[[], str | None]) -> None:
        self._origin = origin
        self._by_player: dict[str, FlowSession] = {}
        self._tokens: dict[str, FlowSession] = {}

    def session(self, player: str) -> FlowSession:
        """Сессия плеера; у одного плеера — одна на весь дом (адрес её не меняется)."""
        session = self._by_player.get(player)
        if session is None:
            session = FlowSession(secrets.token_urlsafe(9))
            self._by_player[player] = session
            self._tokens[session.token] = session
        return session

    def address(self, session: FlowSession) -> str | None:
        """Абсолютный адрес сессии для плеера; `None` — дом не знает своего адреса."""
        base = self._origin()
        # Тот же путь, что у короткого адреса трека (`http_music.py`): токен различает владельца —
        # сессию потока или ссылку на трек.
        return f"{base.rstrip('/')}{URL_PREFIX}/stream/{session.token}" if base else None

    def find(self, token: str) -> FlowSession | None:
        return self._tokens.get(token)

    def drop(self, player: str) -> None:
        session = self._by_player.pop(player, None)
        if session is not None:
            self._tokens.pop(session.token, None)
            session.stop()

    def close(self) -> None:
        for session in list(self._tokens.values()):
            session.stop()
        self._by_player.clear()
        self._tokens.clear()
