"""Один поток на сессию плеера: адрес не меняется, элементы сменяются внутри открытого ответа.

Зачем (живой случай 2026-11-22, прошитый Amazon Echo — медиаплеер ESPHome): плеер не берёт новый
`media_url` поверх живого потока, а мы посылали ему команду на КАЖДЫЙ элемент — переключение
работало только после «Стоп». Music Assistant решает это тем же приёмом: для плееров из HA он
всегда включает flow mode («For HA media players flow mode is always used… the wide variation in
HA player capabilities makes it the most reliable playback method»,
music-assistant.io/player-support/home-assistant/). Ось «как устройство берёт поток» — свойство
плеера (`stream.flow` описания плитки), а не обход бага.

Склейка и фейд идут на PCM 48 кГц. Выходы получают кадры MP3 без общего битового
резервуара, поэтому выход может начать чтение на текущей позиции потока.

⚠ Сессия — только про ЗВУК и порядок элементов. Что играет (очередь, каталог, подписи, обложки)
остаётся у очереди дома (`music_playback.py`, `providers/media_queue.py`): адрес сессии лишь ещё
один способ отдать элемент плееру — тем же `Output`, что и ссылка на трек.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from collections import deque
from collections.abc import Awaitable, Callable

from ..core.const import LOGGER, URL_PREFIX
from .music_fade import _Fader
from .music_flow_feed import (
    PREFETCH_S, PREFETCH_WAIT_S, SILENCE_MS, Prefetch, buffer_radio_pcm, fade_out, read_head, silence, stop_process,
)
from .music_ffmpeg import (
    FLOW_CHANNELS, FLOW_CHUNK, FLOW_RATE, FLOW_WIDTH, MP3_FRAME_MS, FlowProcess,
    ffmpeg_decoder, ffmpeg_encoder, silent_frame,
)
from .player_output import MediaItem

# Буфер выхода вмещает сдвиг колонки группы до 20 с (`MAX_DELAY_MS`) плюс запас на запись в сокет:
# пока колонке уходят кадры тишины сдвига, насос кладёт ей звук в эту очередь.
READER_QUEUE = 1000
# Сколько насос ждёт, пока одиночная колонка переподключится после ручной смены (`prime_solo`).
RECONNECT_HOLD_S = 5.0
RADIO_RECONNECT_S = 10
RADIO_RECONNECT_FRAMES = 20
# Предзапас каждого читателя: последние кадры провода, ~1 с (кадр MP3 = 24 мс). ⚠ Одинаковый у всех:
# задержка колонки должна быть свойством МОДЕЛИ, а не того, подключилась ли она первой. Раньше
# хвост получал только первый читатель радио, и замер статической задержки зависел от порядка.
PREROLL_FRAMES = 42
RADIO_PCM_CHUNKS = 44  # До 15 с стерео PCM 48 кГц, по 64 КиБ.
# Отсчётов PCM в кадре MP3 (CBR 48 кГц без резервуара): отсчёт N уходит плееру в кадре N // 1152.
MP3_FRAME_SAMPLES = 1152
_PCM_FRAME = FLOW_CHANNELS * FLOW_WIDTH
# Запас PCM перед кодировщиком: больше — смена трека доигрывает старое, меньше — кодировщик голодает.
ENCODER_LEAD_S = 0.3
# Кодировщик не отдал кадр за столько — не ждём его (иначе насос и кодировщик ждали бы друг друга).
ENCODER_STALL_S = 0.5
# Сколько единственный читатель может стоять (пауза с пульта), прежде чем его снимут, сек.
SOLE_READER_STALL_S = 30
# Сколько элементов подряд без звука пропускаем, прежде чем закрыть поток.
MAX_SKIPPED = 5


# Чем дом декодирует элемент: свой процесс на элемент, звук — PCM в трубу. Второй аргумент —
# стартовая позиция элемента, сек (перемотка внутри уже открытого потока). Тесты подменяют это.
Decoder = Callable[[str, float], Awaitable["FlowProcess"]]


class FlowSession:
    """Сессия одного плеера: элементы, позиция в них и читатели одного потока.

    ⚠ Игроки сессии — очередь дома: она задаёт `items`/`index` (`set_items`) и просит следующего
    (`on_end`). Сессия не решает, что играть, — только отдаёт это одним потоком.

    ⚠⚠ ЧИТАТЕЛЕЙ МОЖЕТ БЫТЬ НЕСКОЛЬКО (группа мультирума: им даётся ТОТ ЖЕ адрес), а звук
    декодируется ОДИН раз: `_pump` раскидывает куски по очередям читателей. Иначе каждый участник
    тянул бы свою декодировку того же элемента, и `on_end` срабатывал бы столько раз, сколько
    читателей, — очередь уезжала бы вперёд в N раз, а процессор дома горел бы в N раз сильнее.
    """

    def __init__(self, token: str) -> None:
        self.token = token
        self.items: tuple[MediaItem, ...] = ()
        self.index = 0
        # Позиция перемотки следующего запуска текущего элемента, сек. Плееру команда не идёт:
        # `_run` меняет декодер внутри того же открытого ответа, читатели продолжают адрес.
        self.seek_offset = 0.0
        self.source_of: Callable[[MediaItem], Awaitable[str | None]] | None = None
        self.on_end: Callable[[], Awaitable[None]] | None = None
        # Устройство начало читать поток: очередь дома по этому признаку подтверждает запуск
        # (`media_queue.py`, `Attempt.opened`) — у плеера, который ничего о себе не сообщает,
        # другого подтверждения нет и быть не может.
        self.on_read: Callable[[], None] | None = None
        self.on_read_player: str | None = None
        # Окно фейда, байты PCM (0 — без него): кроссфейд доигранных элементов и гашение при ручной
        # смене. Очередь дома ставит одно на все колонки (`fade_window`, `FADE_MS`). ⚠ Раньше бралось из
        # описания ведущей плитки (`fadeMs`) и было только у Echo — остаток времён, когда через поток
        # дома играл один Echo. Калибровка оставляет 0.
        self.fade_bytes = 0
        # Первый кадр элемента ушёл колонке (элемент, мс по часам цикла): очередь узнаёт, когда трек
        # зазвучит, — «играет» в приложении не раньше, чем его слышно (`media_queue.flow_started`).
        self.on_item_start: Callable[[MediaItem, float], None] | None = None
        # Задержка КОНКРЕТНОГО плеера, мс: дом выводит её на лету из замеров систем группы
        # (`MusicPlayback._delay`). Читается на каждый кусок: состав группы меняется на ходу.
        self.delay_of: Callable[[str], int] | None = None
        # ⚠ Провод у сессии ОДИН и он MP3 — всегда: группе нельзя менять формат на ходу (открытый
        # ответ уже несёт свой), а участник может объявлять свои типы (Яндекс Станция берёт только
        # `stream.mp3`/`stream.aac`). Так же делает MA для плееров из HA («output codec… defaults to
        # MP3 here rather than FLAC»). ESPHome и Sonos получают mp3 без проблем (HA перекодирует).
        # Цена: кодирование на доме — один процесс на играющую сессию.
        # Элемент сменён (next/previous/замена очереди): текущий ffmpeg гасим, поток продолжается.
        self.switch = asyncio.Event()
        self._playing = asyncio.Event()
        self._playing.set()
        # Устройство читает прямо сейчас: с открытым потоком команда плееру не нужна.
        self._readers: dict[asyncio.Queue[bytes | None], str] = {}
        self._opened: set[str] = set()
        self.retired = False
        self._pump: asyncio.Task[None] | None = None
        self._preroll: deque[bytes] = deque(maxlen=PREROLL_FRAMES)
        self._radio_ready = asyncio.Event()
        # Свой звук элемента вместо декодера (калибровка: PCM делает дом). `None` — обычный декодер.
        self.generated: Callable[[MediaItem], FlowProcess | None] | None = None
        # Отмеченные куски (`FlowProcess.mark` — номер ноты замера): `{at, note}`, `at` — мс по часам цикла событий.
        # ⚠ Отметка — когда ПЛЕЕРУ ушёл кадр MP3 с началом тона, а не когда PCM ушёл в кодировщик:
        # между ними трубы и очереди ffmpeg с плавающим запасом в секунды, и замер на объекте не сходился
        # (телефон слышал 22 тона, отметки дома убегали вперёд).
        self.probe_sent: list[dict] = []
        # Ждущие отметки: (номер кадра MP3, доля кадра до отметки, мс, что сделать с моментом ухода).
        self._frame_marks: deque[tuple[int, float, Callable[[float], None]]] = deque()
        # Пачка одиночной колонке, мс (схема Music Assistant): колонка набирает свой буфер сразу, а не
        # в темпе эфира. Ставит владелец (`MusicPlayback`): замеренная задержка колонки или умолчание.
        # `None` — пачек нет (замер, спеки).
        self.solo_burst_of: Callable[[str], int] | None = None
        self._burst_for: str | None = None
        self._burst_frames = 0
        # Кадров к отправке по расписанию из прошлого (предзапас, пачка) — берёт насос на ближайшем кадре.
        self._reschedule = 0
        # Насос стоит, пока одиночная колонка переподключается: иначе начало нового ушло бы в старое
        # соединение и пропало.
        self._released = asyncio.Event()
        self._released.set()
        self._release_timer: asyncio.TimerHandle | None = None

    @property
    def current(self) -> MediaItem | None:
        """Элемент, который играет сейчас; `None` — очередь пуста."""
        return self.items[self.index] if 0 <= self.index < len(self.items) else None

    def set_items(self, items: tuple[MediaItem, ...], index: int) -> None:
        """Очередь и место в ней — от владельца (очереди дома). Меняет звук, не адрес."""
        self.items, self.index = tuple(items), index
        self._preroll.clear()
        self._radio_ready.clear()
        self.seek_offset = 0.0
        self.switch.set()

    def seek(self, position: float) -> None:
        """Перемотать текущий элемент внутри потока; читателям адрес и команда не меняются."""
        self.seek_offset = max(0.0, position)
        self.switch.set()

    def prime_solo(self, player: str) -> bool:
        """Ручной запуск или смена у ОДИНОЧНОЙ колонки: перекомандовать её и дать начало пачкой.

        Так делает Music Assistant: при смене плеер получает `play_media` заново и сбрасывает свой буфер,
        а звук ему отдаётся быстрее эфира, пока буфер не набран. Иначе Станция (буфер ~10 с) доигрывала
        прежнюю станцию 10 с, а первый запуск ждала столько же: буфер в темпе эфира копится в темпе эфира.

        ⚠ Только одиночной: в группе пачка сменила бы глубину буфера колонок, и выравнивание по замерам
        (`delay_of`) разошлось бы. И не замеру (`generated`): он меряет колонку в темпе эфира.
        `True` — колонку надо перекомандовать (`play_media`), даже если она уже читает поток.
        """
        # Прежний неиспользованный заказ снимаем: иначе колонка получила бы его, подключившись уже в группе.
        self._burst_for, self._burst_frames = None, 0
        if self.generated is not None or self.solo_burst_of is None:
            return False
        if any(owner != player for owner in self._readers.values()):
            return False
        burst_ms = self.solo_burst_of(player)
        if burst_ms <= 0:
            return False
        self._burst_for, self._burst_frames = player, round(burst_ms / MP3_FRAME_MS)
        if self.reading_player(player):
            self._released.clear()
            if self._release_timer is not None:
                self._release_timer.cancel()
            self._release_timer = asyncio.get_running_loop().call_later(RECONNECT_HOLD_S, self._release)
        return True

    def _release(self) -> None:
        if self._release_timer is not None:
            self._release_timer.cancel()
            self._release_timer = None
        self._released.set()

    def pause(self) -> None:
        self._playing.clear()

    def resume(self) -> None:
        self._playing.set()

    def stop(self) -> None:
        """Остановить поток и декодер, в том числе при зависшем чтении источника."""
        self.items, self.index = (), 0
        self._preroll.clear()
        self._radio_ready.clear()
        self._playing.set()
        self._burst_for, self._burst_frames = None, 0
        self._release()
        self.switch.set()
        pump = self._pump
        if pump is not None and not pump.cancelling() and pump is not asyncio.current_task(loop=pump.get_loop()):
            pump.cancel()
        for queue in self._readers:
            self._finish_reader(queue, discard=True)

    @property
    def reading(self) -> bool:
        return bool(self._readers)

    async def warm_radio(self, binary: str) -> None:
        """Подготовить начало эфира до команды play_media, с ограниченным ожиданием."""
        if self.retired or self.reading or not self.current or not self.current.source:
            return
        if self._pump is None or self._pump.done():
            self._pump = asyncio.create_task(self._run(
                lambda url, start: ffmpeg_decoder(binary, url, start, radio=bool(self.current and self.current.source)),
                lambda write: ffmpeg_encoder(binary, write),
            ))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._radio_ready.wait(), timeout=0.8)

    def has_opened(self, player: str) -> bool:
        return player in self._opened

    def reading_player(self, player: str) -> bool:
        return player in self._readers.values()

    def forget(self, player: str) -> None:
        self.disconnect(player)
        self._opened.discard(player)

    def disconnect(self, player: str) -> None:
        for queue, owner in list(self._readers.items()):
            if owner == player:
                self._readers.pop(queue)
                self._finish_reader(queue, discard=True)

    def _finish_reader(self, queue: asyncio.Queue[bytes | None], *, discard: bool = False) -> None:
        if discard or queue.full():
            while not queue.empty():
                queue.get_nowait()
        queue.put_nowait(None)

    async def serve(
        self, write: Callable[[bytes], Awaitable[None]], decoder: Decoder,
        encoder: Callable[[Callable[[bytes], Awaitable[None]]], Awaitable["FlowProcess"]] | None = None,
        player: str = "", *, confirm_write: bool = True,
    ) -> None:
        """Подключить ОДНОГО читателя (устройство). Звук сессии общий, `_pump` — один на всех.

        Стык элементов смешивается на `fade_bytes` (фейд, `_Fader`): хвост прежнего элемента
        придерживается и микшуется с головкой следующего — и на доигранном треке, и когда элемент
        сменили на ходу (станция, «дальше»).

        `encoder` — провод сессии (MP3): звук идёт в него, а читателям отдаётся его выход. Фейд и
        склейка остаются на PCM — кодировщик один на всю сессию. Без него (спеки) отдаётся PCM.

        ⚠ Очередь читателя не ждёт: медленный (или ушедший) участник теряет свои куски, но НЕ
        останавливает группу. Живой прибор тянет поток в темпе воспроизведения — очередь в
        десятки секунд звука его не переполнит, а мёртвый (закрытый ответ) не должен вешать всех.
        """
        if self.retired:
            return
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=READER_QUEUE)
        if player:
            self.disconnect(player)
        for frame in self._preroll:
            queue.put_nowait(frame)
        self._readers[queue] = player
        self._opened.add(player)
        if player and player == self._burst_for:
            # Одиночная колонка переподключилась (`prime_solo`): начало — пачкой, насос снова идёт.
            # Кто-то ещё уже читает — это группа, пачки нет.
            if all(owner == player for owner in self._readers.values()):
                self._reschedule = max(self._reschedule, self._burst_frames)
            self._burst_for, self._burst_frames = None, 0
            self._release()
        # Задержка ЭТОГО плеера, уже применённая к его чтению, и остаток звука, который надо выбросить.
        applied = 0.0
        drop = 0
        if self._pump is None or self._pump.done():
            self._pump = asyncio.create_task(self._run(decoder, encoder))
        try:
            while (chunk := await queue.get()) is not None:
                await self._playing.wait()
                if queue not in self._readers:
                    break
                # ⚠ Задержка ЖИВАЯ: правка в составе слышна сразу. Больше — колонке уходят кадры
                # тишины, меньше — выбрасываем лишнее из уже полученного; перезапуска потока нет.
                # ⚠⚠ Сдвиг — ДАННЫМИ, а не паузой записи: колонка начинает играть, набрав в буфер
                # свой запас, и пауза с последующей пачкой накопленного давала ей тот же запас в то
                # же время — сдвиг пропадал (проверка на объекте: Яндекс + HEOS заметно врозь).
                want = float(self.delay_of(player)) if self.delay_of is not None else 0.0
                if encoder is not None:
                    want = round(want / MP3_FRAME_MS) * MP3_FRAME_MS
                if want > applied:
                    pad = silent_frame() if encoder is not None else None
                    if pad is not None:
                        for _ in range(round((want - applied) / MP3_FRAME_MS)):
                            await write(pad)
                    else:
                        await asyncio.sleep((want - applied) / 1000)
                    if queue not in self._readers:
                        break
                    applied = want
                elif want < applied:
                    if encoder is not None:
                        drop += round((applied - want) / MP3_FRAME_MS)
                    else:
                        drop += int((applied - want) * FLOW_RATE / 1000) * FLOW_CHANNELS * FLOW_WIDTH
                    applied = want
                if drop:
                    if encoder is not None:
                        drop -= 1
                        continue
                    if len(chunk) <= drop:
                        drop -= len(chunk)
                        continue
                    chunk, drop = chunk[drop:], 0
                await write(chunk)
                if confirm_write and queue in self._readers:
                    self.confirm_write(player)
        except (ConnectionError, OSError) as error:
            LOGGER.debug("Плеер %s: запись аудиопотока закрыта (%s)", player, type(error).__name__)
        finally:
            self._readers.pop(queue, None)

    def confirm_write(self, player: str) -> None:
        if self.on_read is not None and self.on_read_player in (None, player):
            on_read, self.on_read = self.on_read, None
            on_read()

    async def _run(self, decoder: Decoder, encoder: Callable[..., Awaitable["FlowProcess"]] | None) -> None:
        """Один декодер (и один кодировщик) на сессию: куски уходят ВСЕМ читателям.

        Подача — как у Music Assistant и Snapcast (`music_flow_feed.py`): поток не прерывается, ручная
        смена гасит старое плавно и включает новое сразу, доигранный элемент сходится со следующим
        кроссфейдом, если тот открыт заранее.
        """

        clock = asyncio.get_running_loop().time
        deadline: float | None = None
        without_readers_since: float | None = None
        # ⚠ Первые `PREROLL_FRAMES` кадров сессии уходят без темпа — тот же предзапас, что получает
        # подключившийся позже из хвоста. Иначе первый читатель играл бы у живого края, а остальные
        # на секунду позже, и замер задержки зависел бы от того, кто подключился первым.
        # Пачка одиночной колонке (`prime_solo`) — тот же приём, только длиннее.
        self._reschedule = max(self._reschedule, PREROLL_FRAMES)
        # Сколько кадров ещё идёт по расписанию из прошлого: в это время отставание от часов — замысел.
        catchup = 0
        # Кадров MP3, ушедших в рассылку, и отсчётов PCM, ушедших в кодировщик.
        frames_out = 0
        samples_in = 0
        drained = asyncio.Event()

        async def broadcast(chunk: bytes) -> None:
            nonlocal deadline, catchup, frames_out
            await self._playing.wait()
            await self._released.wait()
            if encoder is not None:
                now = clock()
                if self._reschedule:
                    # Предзапас и пачка — расписание, начатое в прошлом: кадры, чей срок уже прошёл,
                    # уходят сразу, дальше — темп эфира. Источник медленнее эфира (радио) пачку не
                    # наберёт — тогда кадры просто уходят по мере появления.
                    catchup, self._reschedule = self._reschedule, 0
                    deadline = now - catchup * MP3_FRAME_MS / 1000
                if catchup > 0:
                    catchup -= 1
                elif deadline is None or now - deadline > 0.250:
                    deadline = now
                await asyncio.sleep(max(0, deadline - now))
                # ⚠ Отметка — срок кадра ПО РАСПИСАНИЮ, а не миг отправки: колонка играет кадры подряд,
                # и кадр пачки звучит тогда же, когда звучал бы, уйди он в темпе эфира. Так правило
                # «слышно через задержку колонки» (`flow_started`, замер) верно и для пачки; для кадра
                # в темпе срок и есть миг отправки.
                scheduled = deadline
                deadline += MP3_FRAME_MS / 1000
                while self._frame_marks and self._frame_marks[0][0] <= frames_out:
                    _, rest_ms, mark = self._frame_marks.popleft()
                    mark(scheduled * 1000 + rest_ms)
                frames_out += 1
                drained.set()
                self._preroll.append(chunk)
                if not self._readers and (item := self.current) is not None and item.source:
                    if len(self._preroll) >= RADIO_RECONNECT_FRAMES:
                        self._radio_ready.set()
            readers = list(self._readers.items())
            if len(readers) == 1 and readers[0][0].full():
                # ⚠ Единственный читатель — не повод снимать: плеер встал на паузу с пульта или из
                # приложения вендора. Ждём его (декодер встаёт вместе с нами, трек не убегает), а
                # снимаем только мёртвого. В группе медленный участник не держит остальных.
                queue, player = readers[0]
                with contextlib.suppress(TimeoutError, asyncio.TimeoutError):  # noqa: UP041 — 3.10
                    await asyncio.wait_for(queue.put(chunk), SOLE_READER_STALL_S)
                    return
            for queue, player in readers:
                if queue not in self._readers:
                    continue
                if queue.full():
                    LOGGER.warning("Аудиопоток: очередь плеера %s переполнена", player)
                    self._finish_reader(queue, discard=True)
                    self._readers.pop(queue)
                else:
                    queue.put_nowait(chunk)

        sink: FlowProcess | None = None

        def mark_at(mark: Callable[[float], None]) -> None:
            """Отметка на ТЕКУЩЕМ отсчёте: сработает, когда колонке уйдёт кадр MP3 с ним."""
            if encoder is None:
                mark(clock() * 1000)
                return
            frame, rest = divmod(samples_in, MP3_FRAME_SAMPLES)
            self._frame_marks.append((frame, rest * 1000 / FLOW_RATE, mark))

        async def push(out: bytes) -> None:
            """PCM в кодировщик с запасом не больше `ENCODER_LEAD_S` (см. `mp3_args`)."""
            nonlocal samples_in
            if not out:
                return
            if sink is None:
                await broadcast(out)
            else:
                while samples_in - frames_out * MP3_FRAME_SAMPLES > ENCODER_LEAD_S * FLOW_RATE:
                    drained.clear()
                    try:
                        await asyncio.wait_for(drained.wait(), ENCODER_STALL_S)
                    except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041 — 3.10
                        break
                await sink.write(out)
            samples_in += len(out) // _PCM_FRAME

        async def open_item(item: MediaItem, start: float) -> tuple[FlowProcess, bytes] | None:
            """Открыть элемент: ссылка, декодер и головка окна фейда; `None` — у элемента нет звука."""
            generated = self.generated(item) if self.generated is not None else None
            if generated is None:
                source = await self.source_of(item) if self.source_of is not None else None
                if source is None:
                    return None
                generated = await decoder(source, start)
            return generated, await read_head(generated, self.fade_bytes)

        prefetch = Prefetch(open_item)
        fader = _Fader(self.fade_bytes)
        skipped = 0
        # Прежний элемент сменили вручную (а не доиграли): его хвост гасится, новый — без фейда.
        switched = False
        # Звук уже шёл: на смене тишина держит поток, а до первого элемента колонка и так ждёт.
        played = False
        try:
            if encoder is not None:
                sink = await encoder(broadcast)
            while (item := self.current) is not None:
                if self.source_of is None or (not self._readers and (encoder is None or not item.source)):
                    return
                self.switch.clear()
                start = self.seek_offset
                self.seek_offset = 0.0
                prefetched = prefetch.take(item, start)
                opening = prefetched or asyncio.create_task(open_item(item, start))
                if prefetched is not None and not switched:
                    # Доигранный трек, следующий открывается заранее: дождаться его ради кроссфейда.
                    await asyncio.wait({opening}, timeout=PREFETCH_WAIT_S)
                if switched or not opening.done():
                    # Ручная смена или следующий не готов: старое гаснет сразу, пока новое открывается —
                    # тишина. Поток не прерывается: колонка не опустошает буфер и не набирает его заново.
                    await push(fade_out(fader.flush()))
                    while played and not opening.done() and encoder is not None and self._readers:
                        await push(silence(SILENCE_MS))
                switched = False
                opened = await opening
                if opened is None:
                    # Трек без ссылки (снят у правообладателя, регион) — пропуск, а не конец музыки:
                    # раньше один такой трек гасил весь поток и группу.
                    skipped += 1
                    LOGGER.warning("Поток сессии %s: источник не отдал ссылку на %s", self.token, item.id)
                    if skipped > MAX_SKIPPED or self.on_end is None:
                        return
                    await self.on_end()
                    if self.current is item:
                        return
                    continue
                skipped = 0
                process, head = opened
                played = True
                pcm_buffer: asyncio.Queue[bytes] | None = None
                producer: asyncio.Task[None] | None = None
                item_samples = 0
                try:
                    if self.on_item_start is not None:
                        started = self.on_item_start
                        mark_at(lambda at, item=item: started(item, at))
                    # С головкой микшуется придержанный хвост доигранного (кроссфейд); после ручной
                    # смены хвоста нет — новое звучит сразу.
                    out = fader.join(head)
                    await push(out)
                    item_samples += len(out) // _PCM_FRAME
                    if item.source and encoder is not None:
                        pcm_buffer = asyncio.Queue(RADIO_PCM_CHUNKS)
                        producer = asyncio.create_task(buffer_radio_pcm(process, pcm_buffer))
                    while True:
                        chunk = await (process.read(FLOW_CHUNK) if pcm_buffer is None else pcm_buffer.get())
                        if not chunk:
                            if producer is not None:
                                await producer
                            break
                        out = fader.feed(chunk)
                        if out:
                            if (note := getattr(process, "mark", None)) is not None:
                                mark_at(lambda at, note=note: self.probe_sent.append({"at": at, "note": note}))
                            await push(out)
                            item_samples += len(out) // _PCM_FRAME
                        duration = getattr(item, "duration", 0) or 0
                        if duration > 0 and item_samples >= (duration - PREFETCH_S) * FLOW_RATE:
                            if self.generated is None and 0 <= self.index + 1 < len(self.items):
                                prefetch.start(self.items[self.index + 1])
                        # Смену элемента замечаем на ближайшем куске звука: он приходит постоянно.
                        if self.switch.is_set() or self.current is not item:
                            switched = True
                            break
                        if self._readers:
                            without_readers_since = None
                        elif encoder is None or not item.source:
                            return
                        else:
                            # Короткое переподключение не должно перезапускать радиоисточник.
                            now = clock()
                            if without_readers_since is None:
                                without_readers_since = now
                            elif now - without_readers_since >= RADIO_RECONNECT_S:
                                return
                finally:
                    if producer is not None:
                        producer.cancel()
                        await asyncio.gather(producer, return_exceptions=True)
                    await stop_process(process)
                if switched:
                    continue
                await self._playing.wait()
                if self.switch.is_set() or self.current is not item:
                    switched = True
                    continue
                if self.on_end is not None:
                    await self.on_end()
                if self.current is item:  # очередь кончилась: дописываем придержанное и закрываем
                    await push(fade_out(fader.flush()))
                    return
        except Exception:  # noqa: BLE001 — упавший поток не должен вешать читателей молча
            LOGGER.exception("Поток сессии %s упал", self.token)
        finally:
            prefetch.drop()
            if sink is not None:
                await sink.stop()
            self._preroll.clear()
            for queue in list(self._readers):
                self._finish_reader(queue)


class FlowStreams:
    """Адреса сессий: токен → сессия, один на плеер (адрес не меняется между элементами).

    Адрес — сам себе пропуск (случайный токен, `providers/music_proxy.py`): плеер в локальной сети
    учётки не приносит.
    """

    def __init__(self, origin: Callable[[], str | None]) -> None:
        self._origin = origin
        self._by_player: dict[str, FlowSession] = {}
        # Токен адреса → (сессия, плеер); токен один на пару, чтобы правка задержки не меняла адрес.
        self._tokens: dict[tuple[str, str], str] = {}
        self._players: dict[str, tuple[FlowSession, str]] = {}

    def session(self, player: str) -> FlowSession:
        """Сессия плеера; у одного плеера — одна на весь дом (адрес её не меняется)."""
        session = self._by_player.get(player)
        if session is None:
            session = FlowSession(secrets.token_urlsafe(9))
            self._by_player[player] = session
        return session

    def current(self, player: str) -> FlowSession | None:
        """Сессия плеера, только если она уже есть: для lookup'ов, где новую заводить не нужно —
        иначе после каждой паузы/выхода оставался бы пустой зарегистрированный объект."""
        return self._by_player.get(player)

    def address(self, session: FlowSession, player: str, *, aac: bool = False) -> str | None:
        """Абсолютный адрес сессии ДЛЯ ЭТОГО плеера; `None` — дом не знает своего адреса.

        ⚠ Токен — на (сессия, плеер), а не один на сессию: по нему маршрут узнаёт, ЧЬЁ это чтение,
        и применяет его задержку (`FlowSession.serve`, `delay_of`). Группе адрес один по звуку,
        но у каждого участника свой токен.
        """
        base = self._origin()
        if not base:
            return None
        key = (session.token, player)
        suffix = ".aac" if aac else ""
        token = self._tokens.get(key)
        if token is not None and token.endswith(".aac") != aac:
            self.revoke(session, player)
            token = None
        if token is None:
            token = self._tokens[key] = secrets.token_urlsafe(9) + suffix
        self._players[token] = (session, player)
        # Тот же путь, что у короткого адреса трека (`http_music.py`).
        return f"{base.rstrip('/')}{URL_PREFIX}/stream/{token}"

    def revoke(self, session: FlowSession, player: str) -> None:
        if token := self._tokens.pop((session.token, player), None):
            self._players.pop(token, None)
        session.forget(player)

    def retire(self, session: FlowSession) -> None:
        session.retired = True
        for token_session, player in list(self._tokens):
            if token_session == session.token:
                self.revoke(session, player)
        session.stop()

    def find(self, token: str) -> tuple[FlowSession, str] | None:
        """Сессия и плеер по адресу; `None` — не наш токен."""
        return self._players.get(token)

    def drop(self, player: str) -> None:
        session = self._by_player.pop(player, None)
        if session is not None:
            self.retire(session)

    def close(self) -> None:
        for session in list(self._by_player.values()):
            self.retire(session)
        self._by_player.clear()
        self._tokens.clear()
