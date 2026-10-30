"""Сессии просмотра записи: непрерывный MPEG-TS своему go2rtc, перемотка и пауза — команды (дом 0.6.0+).

Зачем. До 0.6.0 каждая перемотка была новым списком ffconcat на хранилище, новым потоком go2rtc и новыми
переговорами WebRTC, а воспроизведение держал ffmpeg, которого запускал go2rtc: он жил, пока висел
WebRTC-потребитель, и серия перемоток копила их до OOM (`go2rtc_guard.py`). Здесь хозяин — дом
(`docs/superpowers/specs/2026-10-26-video-engine-rewrite-design.md` менеджера, «Канал просмотра»):
  * сессия заводит в своём go2rtc поток `play_<id>` с источником `http://127.0.0.1:<порт>/sessions/<id>.ts`
    и отдаёт по нему ОДИН непрерывный MPEG-TS — его go2rtc читает сам, без своего ffmpeg;
  * перемотка — новый внутренний ffmpeg той же сессии с новой метки: для go2rtc и WebRTC это тот же
    живой поток, у которого сменилось содержимое; переговоров нет;
  * пауза — перестать подавать: WebRTC стоит на последнем кадре; продолжение — перемотка на метку паузы;
  * ушёл потребитель — go2rtc закрывает HTTP (проба: сразу при закрытии, ~7 с при пропавшем телефоне),
    сессия снимается через `GRACE_S`; без зрителя вовсе — через `IDLE_S`.

⚠ Время на выходе ПРОДОЛЖАЕТСЯ через каждый перезапуск (`-output_ts_offset` = секунды с первого отданного
байта): с метками заново от нуля поток вставал на 8–14 с после каждой перемотки (проба 2026-10-26).
⚠ Звук — всегда Opus: A-law в MPEG-TS не ложится, а AAC go2rtc отдаёт WebRTC как есть, и тот его не берёт.
⚠ Дорожка звука объявлена, а пакетов в ней нет (камеры EYEPLUS с динамиком и микрофоном): ffmpeg ждал бы звук
до первого байта ~30 с (замер на объекте 2026-10-27) — такая запись играется без звука (`has_audio`). Так же —
звук с застывшими метками (EYEPLUS «Гостиная» .90.250, 2026-10-30): видео куска стоит на часы позже его начала, и
список даёт каждому куску начало видео (`_plan`).
⚠ SPS в потоке правится под go2rtc (`ts_sps.py`): с `nal_ref_idc = 2` он его выбрасывает, и запись не видна.
⚠ Через пустоты сессия идёт сама, следующим участком: время под иглой бандл считает по разметке.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib.parse import quote
from uuid import uuid4

from aiohttp import web

from . import host_load
from .const import LOGGER
from .go2rtc_embed import API_PORT
from .recorder_index import Segment, SegmentIndex
from .ts_sps import SpsFix

if TYPE_CHECKING:
    from .recorder import Recorder

# Сессий на дом и на камеру: сверх — снимается самая давняя БРОШЕННАЯ; смотрятся все — отказ (`SessionsBusy`).
MAX_SESSIONS = 8
MAX_PER_CAMERA = 3
# Столько раз подряд ffmpeg отдал байты и оборвался на том же месте — кусок битый, пропустить.
STUCK_TRIES = 2


class SessionsBusy(Exception):
    """Новую сессию не открыть: все места заняты смотрящими."""
# Сессия, к которой зритель так и не пришёл (переговоры не начались), и сессия, чей зритель ушёл.
IDLE_S = 30.0
GRACE_S = 10.0
# Сколько записи вперёд кладётся в один список; доиграли — следующий с конца прежнего.
PLAYLIST_S = 3 * 3600
# Насколько вперёд от метки в пустоте искать запись — сутки, как у прежнего списка (`recorder_api.FORWARD_S`).
FORWARD_S = 24 * 3600
# Насколько НАЗАД от метки годится последний кусок: у живого края идущий кусок ещё не в индексе. Дальше — записи нет.
BACK_S = 600.0
# Новых кусков у живого края нет (или ffmpeg падает без единого байта) — ждать столько до новой попытки.
RETRY_S = 10.0
# Ответ на перемотку ждёт первых байтов нового места не дольше этого.
FIRST_BYTES_S = 2.0
AUDIO = ["-c:a", "libopus", "-b:a", "24k", "-ac", "1"]
CHUNK = 64 * 1024
PLAY_DIR = Path(tempfile.gettempdir()) / "mega_home_play"

def clock() -> float:
    """Часы сроков сессий; шов спек — подменять `time.monotonic` нельзя: им живёт цикл событий."""
    return time.monotonic()


Spawn = Callable[..., Awaitable[Any]]
Keyframe = Callable[[Any, Path, float], Awaitable[float]]
AudioProbe = Callable[[Any, Path], Awaitable[bool]]
VideoStart = Callable[[Any, Path], Awaitable[float]]
# Видео куска начинается дальше этого от начала файла — список с началом видео у каждого куска (`_plan`).
OFFSET_S = 1.0
# Кусков в таком списке: проба начала видео — ~60 мс на кусок (замер .90.250), и перемотка ждёт их все.
OFFSET_PIECES = 8
# Сколько строк ошибок ffmpeg сессии попадает в журнал, когда он не отдал ни байта. ⚠ ПЕРВЫХ: причина —
# первая строка («Impossible to open '<кусок>'»), дальше её следствия; хвост терял её (объект 2026-10-27).
ERROR_HEAD = 3


def playlist_from(index: SegmentIndex, at_s: float, started: bool = False) -> tuple[list[Segment], float] | None:
    """Куски с метки ЧЕРЕЗ пустоты (до `PLAYLIST_S` записи) и `inpoint` в первом; нет записи — None.

    Метка в пустоте — ближайшая запись: вперёд в пределах суток, иначе последний кусок до неё с его
    начала, если он кончился не раньше `BACK_S` до метки. `started` — продолжение: первый кусок, НАЧАВШИЙСЯ после метки, и с его начала.
    """
    first = index.first_after(at_s, at_s + FORWARD_S, started) or (None if started else index.last_before(at_s))
    # ⚠ Назад — только у самой метки (живой край: идущий кусок ещё не в индексе). Без предела камера, переставшая
    # писать, отдавала на событие 28.10 8:40 запись 27.10 7:39 (объект .90.250), и жилец смотрел вчерашнее под
    # шапкой события. Дальше — честное «записи у этого момента нет».
    if first is not None and first[0] + first[1] < at_s - BACK_S:
        return None
    if first is None:
        return None
    start = at_s if not started and first[0] + first[1] > at_s else first[0]
    run = [first]
    for piece in index.between(first[0] + first[1], start + PLAYLIST_S):
        if piece[0] > run[-1][0]:
            run.append(piece)
    return run, max(start - first[0], 0.0)


def session_command(binary: str, playlist: Path, offset_s: float, audio: bool = True) -> list[str]:
    sound = ["-map", "0:a:0?", *AUDIO] if audio else ["-an"]
    return [
        binary, "-hide_banner", "-loglevel", "error", "-nostdin",
        "-re", "-f", "concat", "-safe", "0", "-i", str(playlist),
        "-map", "0:v:0", "-c:v", "copy", *sound,
        "-f", "mpegts", "-output_ts_offset", f"{offset_s:.3f}", "-muxdelay", "0", "-muxpreload", "0", "pipe:1",
    ]


async def _default_keyframe(recorder: Any, path: Path, offset: float) -> float:
    from .recorder_api import keyframe_before  # noqa: PLC0415 — служба импортирует запись, не наоборот

    return await keyframe_before(recorder, path, offset)


async def _default_start(recorder: Any, path: Path) -> float:
    from .recorder_api import video_start  # noqa: PLC0415 — служба импортирует запись, не наоборот

    return await video_start(recorder, path)


async def _drain(proc: Any, errors: list[str]) -> None:
    """Первые ошибки ffmpeg сессии — для журнала; канал читается до конца, иначе ffmpeg встал бы на записи в него."""
    stream = getattr(proc, "stderr", None)
    if stream is None:
        return
    while line := await stream.readline():
        if len(errors) < ERROR_HEAD and (text := line.decode(errors="replace").strip()):
            errors.append(text)


async def _default_audio(recorder: Any, path: Path) -> bool:
    from .recorder_api import has_audio  # noqa: PLC0415 — служба импортирует запись, не наоборот

    return await has_audio(recorder, path)


@dataclass
class PlaybackSession:
    id: str
    camera: str
    stream: str
    playlist: Path
    touched: float
    at_s: float = 0.0
    gen_started: float = 0.0
    last_begin: float = 0.0
    # Куски идущего списка (начало, длина) и `inpoint` первого — по ним время игры переводится во время записи.
    run: list[tuple[float, float]] = field(default_factory=list)
    inpoint: float = 0.0
    first_byte_at: float | None = None
    paused: bool = False
    proc: Any = None
    gen: int = 0
    reader: web.StreamResponse | None = None
    reader_gone_at: float | None = None
    fresh: asyncio.Event = field(default_factory=asyncio.Event)
    pump: asyncio.Future[None] | None = None
    closed: bool = False
    # Сколько списков собрано: у каждого свой файл (`_plan`).
    lists: int = 0
    # Есть ли в записи камеры звук — решается первым списком сессии; None — ещё не смотрели.
    audio: bool | None = None
    # Первый кусок идущего списка: пропал с диска — его надо забыть (`_forget_gone`).
    head: Path | None = None


@dataclass(frozen=True)
class _Planned:
    playlist: Path
    at_s: float
    last_begin: float
    run: list[tuple[float, float]]
    inpoint: float
    head: Path


class PlaybackSessions:
    def __init__(
        self, recorder: Recorder, port: Callable[[], int], spawn: Spawn | None = None, keyframe: Keyframe | None = None,
        audio: AudioProbe | None = None, start: VideoStart | None = None,
    ) -> None:
        self.rec, self.port = recorder, port
        self.spawn: Spawn = spawn or asyncio.create_subprocess_exec
        self.keyframe: Keyframe = keyframe or _default_keyframe
        self.audio: AudioProbe = audio or _default_audio
        self.start: VideoStart = start or _default_start
        self.sessions: dict[str, PlaybackSession] = {}
        self._sweeper: asyncio.Future[None] | None = None

    async def open(self, camera: str, at_s: float) -> tuple[str, str, float]:
        """Новая сессия на метке; ответ — id, имя потока go2rtc и где встанет первый кадр (ключевой)."""
        session = PlaybackSession(id=uuid4().hex[:12], camera=camera, stream="", playlist=Path(), touched=clock())
        session.stream = f"play_{session.id}"
        await self._evict(camera)
        start = await self._prepare(session, at_s, started=False)
        if start is None:
            raise LookupError("записи у этого момента нет")
        self.sessions[session.id] = session
        await self._go2rtc("PUT", f"name={quote(session.stream, safe='')}&src={quote(self._source(session), safe='')}")
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.ensure_future(self._sweep_forever())
        return session.id, session.stream, start

    async def seek(self, sid: str, at_s: float) -> float:
        session = self._get(sid)
        session.touched = clock()
        planned = await self._plan(session, at_s, started=False)
        if planned is None:
            raise LookupError("записи у этого момента нет")
        # ⚠ Прежняя подача становится чужой и только потом гаснет её ffmpeg: иначе она видит его смерть
        # обрывом посреди списка, переписывает список СТАРЫМ местом и заводит свой процесс поверх нового
        # (живой прогон 2026-10-26: картинка нового места через 3 с вместо 0,5).
        await self._retire(session)
        start = await self._commit(session, planned)
        session.paused = False
        if session.reader is not None:
            session.fresh.clear()
            session.pump = asyncio.ensure_future(self._feed(session, session.gen))
            try:
                # Ответ — когда новые байты пошли зрителю: по нему бандл снимает прикрытие превью.
                await asyncio.wait_for(session.fresh.wait(), FIRST_BYTES_S)
            except asyncio.TimeoutError:
                pass
        return start

    async def pause(self, sid: str) -> None:
        session = self._get(sid)
        session.touched = clock()
        session.paused = True
        await self._retire(session)

    async def close(self, sid: str) -> None:
        session = self.sessions.pop(sid, None)
        if session is None:
            return
        session.closed = True
        await self._stop_proc(session)
        if session.pump is not None:
            session.pump.cancel()
        await self._go2rtc("DELETE", f"src={quote(session.stream, safe='')}")
        await self.rec.env.run(lambda: session.playlist.unlink(missing_ok=True))

    async def close_all(self) -> None:
        for sid in list(self.sessions):
            await self.close(sid)
        if self._sweeper is not None:
            self._sweeper.cancel()

    async def serve(self, sid: str, request: web.Request) -> web.StreamResponse:
        """Поток сессии ОДНОМУ читателю — своему go2rtc; пока он читает, сессия жива."""
        session = self.sessions.get(sid)
        if session is None:
            return web.json_response({"error": "сессии нет"}, status=404)
        if session.reader is not None:
            return web.json_response({"error": "поток сессии уже читают"}, status=409)
        response = web.StreamResponse(headers={"Content-Type": "video/mp2t", "Cache-Control": "no-store"})
        await response.prepare(request)
        session.reader, session.reader_gone_at = response, None
        if not session.paused:
            # Зритель вернулся (go2rtc переподключился) — с того места, где картинка была бы сейчас.
            if session.first_byte_at is not None:
                await self._prepare(session, self._position(session), started=False)
            await self._restart(session)
        try:
            while not session.closed and request.transport is not None and not request.transport.is_closing():
                await asyncio.sleep(0.5)
        finally:
            if session.reader is response:
                session.reader, session.reader_gone_at = None, clock()
                await self._stop_proc(session)
        return response

    async def sweep(self) -> None:
        """Снять брошенные: зрителя не было `IDLE_S` с открытия либо он ушёл дольше `GRACE_S` назад."""
        now = clock()
        for session in list(self.sessions.values()):
            if session.reader is not None:
                continue
            gone = session.reader_gone_at
            if (gone is not None and now - gone > GRACE_S) or (gone is None and now - session.touched > IDLE_S):
                await self.close(session.id)

    async def _sweep_forever(self) -> None:
        while self.sessions:
            await asyncio.sleep(GRACE_S / 2)
            await self.sweep()

    def _get(self, sid: str) -> PlaybackSession:
        session = self.sessions.get(sid)
        if session is None:
            raise KeyError(sid)
        return session

    def _source(self, session: PlaybackSession) -> str:
        return f"http://127.0.0.1:{self.port()}/sessions/{session.id}.ts"

    def _played(self, session: PlaybackSession) -> float:
        """Сколько секунд списка отыграно: `-re` отдаёт его в реальном времени."""
        return max(clock() - session.gen_started, 0.0)

    def _position(self, session: PlaybackSession) -> float:
        """Где картинка сейчас — во времени ЗАПИСИ: пустоты ffconcat склеивает встык, поэтому обход кусков."""
        left = self._played(session) + session.inpoint
        for begin, length in session.run:
            if left < length:
                return begin + left
            left -= length
        return session.run[-1][0] + session.run[-1][1] if session.run else session.at_s

    async def _plan(self, session: PlaybackSession, at_s: float, started: bool) -> _Planned | None:
        """Новый список с метки — в СВОЙ файл, сессию не трогая: пока ищется ключевой кадр, прежнее место
        играет. Файл — во временном каталоге, а не на хранилище: на NAS это сетевая запись на каждую перемотку."""
        sink = self.rec.sink(session.camera)
        if sink is None:
            return None
        if not sink.index.loaded:
            await self.rec.env.run(sink.index.ensure, bool(self.rec.limit))
        found = await self.rec.env.run(playlist_from, sink.index, at_s, started)
        if found is None:
            return None
        run, inpoint = found
        if session.audio is None:
            session.audio = await self.audio(self.rec, run[0][2])
        if inpoint > 0:
            inpoint = await self.keyframe(self.rec, run[0][2], inpoint)
        # ⚠ Видео куска не с начала файла (застывшие метки звука, `has_audio`): ffconcat отсчитывает кусок от начала
        # файла и его длину — по последней метке, и запись вставала на часы. Тогда каждому куску — своё начало видео и
        # длина по индексу; список короче, чтобы не ждать проб всех кусков: доиграли — следующий.
        # ⚠ Режим решает СДВИГ первого куска, а не число кусков: у живого края в списке бывает один кусок.
        starts = [await self.start(self.rec, run[0][2])]
        shifted = starts[0] > OFFSET_S
        if shifted:
            run = run[:OFFSET_PIECES]
            starts += [await self.start(self.rec, path) for _begin, _length, path in run[1:]]
        lines = ["ffconcat version 1.0"]
        for number, (_begin, length, path) in enumerate(run):
            lines.append(f"file '{path}'")
            skip = inpoint if number == 0 else 0.0
            if shifted:
                lines += [f"inpoint {starts[number] + skip:.3f}", f"duration {length - skip:.3f}"]
            elif skip > 0:
                lines.append(f"inpoint {skip:.3f}")
        session.lists += 1
        target = PLAY_DIR / f"{session.id}-{session.lists}.ffconcat"

        def write() -> None:
            PLAY_DIR.mkdir(parents=True, exist_ok=True)
            target.write_text("\n".join(lines) + "\n", encoding="utf-8")

        await self.rec.env.run(write)
        return _Planned(target, run[0][0] + inpoint, run[-1][0], [(begin, length) for begin, length, _path in run], inpoint, run[0][2])

    async def _commit(self, session: PlaybackSession, planned: _Planned) -> float:
        old, session.playlist = session.playlist, planned.playlist
        session.at_s, session.last_begin, session.run, session.inpoint = planned.at_s, planned.last_begin, planned.run, planned.inpoint
        session.head = planned.head
        if old != planned.playlist:
            await self.rec.env.run(lambda: old.unlink(missing_ok=True) if old.name else None)
        return session.at_s

    async def _prepare(self, session: PlaybackSession, at_s: float, started: bool) -> float | None:
        planned = await self._plan(session, at_s, started)
        return await self._commit(session, planned) if planned is not None else None

    async def _retire(self, session: PlaybackSession) -> None:
        """Всё, что подавало прежнее место, — чужое (новый номер), затем снять его ffmpeg."""
        session.gen += 1
        if session.pump is not None:
            session.pump.cancel()
            session.pump = None
        await self._stop_proc(session)

    async def _restart(self, session: PlaybackSession) -> None:
        await self._retire(session)
        session.pump = asyncio.ensure_future(self._feed(session, session.gen))

    async def _feed(self, session: PlaybackSession, gen: int) -> None:
        """Подавать, пока сессия жива: конец списка — следующий с конца прежнего, у живого края — ждать."""
        failures = 0
        stuck, stuck_at = 0, None
        while not session.closed and session.reader is not None and not session.paused and gen == session.gen:
            offset = clock() - session.first_byte_at if session.first_byte_at is not None else 0.0
            binary = await self.rec.env.run(shutil.which, "ffmpeg") or "ffmpeg"
            proc = await self.spawn(
                *session_command(binary, session.playlist, offset, session.audio is not False),
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            session.proc, session.gen_started = proc, clock()
            host_load.track(f"play:{session.camera}:{session.id}", getattr(proc, "pid", None))
            errors: list[str] = []
            drain = asyncio.ensure_future(_drain(proc, errors))
            sent = await self._copy(session, proc, gen)
            await self._stop_proc(session, proc)
            await asyncio.wait({drain}, timeout=1.0)
            drain.cancel()
            if gen != session.gen or session.closed or session.paused or session.reader is None:
                return
            if not sent:
                # ⚠ Без этой строки отказ немой: сессия молча повторяет, а у жильца крутилка (EYEPLUS 2026-10-27).
                LOGGER.warning("Просмотр %s: ffmpeg сессии не отдал потока: %s", session.camera, " | ".join(errors) or "без сообщений")
            if not sent and await self._forget_gone(session):
                # Кусок пропал — с той же метки уже следующий; ждать нечего. Обрыв посреди списка на пропавшем
                # куске приходит сюда же: перепланирование ставит его первым.
                failures = 0
                if gen == session.gen and await self._prepare(session, session.at_s, started=False) is None:
                    await asyncio.sleep(RETRY_S)
                continue
            failures = 0 if sent else failures + 1
            position = self._position(session)
            length = sum(piece[1] for piece in session.run) - session.inpoint
            if not sent or self._played(session) < length - 1.0:
                # Оборвался посреди списка (хранилище отвалилось, кусок битый) — с того же места. Падает без
                # единого байта второй раз подряд — не крутить его без паузы.
                if failures >= 2:
                    await asyncio.sleep(RETRY_S)
                if sent:
                    # Не продвинулось ВПЕРЁД (откат к ключевому кадру — тоже на месте).
                    stuck = stuck + 1 if stuck_at is not None and position < stuck_at + 1.0 else 1
                    stuck_at = position if stuck == 1 else stuck_at
                    if stuck >= STUCK_TRIES:
                        # ⚠ Байты пошли, а место не сдвинулось — битый кусок: с того же ключевого кадра жилец видел одни
                        # и те же секунды по кругу (ревью 2026-10-27). Кусок — пропустить, причину — в журнал.
                        stuck, stuck_at = 0, None
                        begin = next((b for b, n in session.run if b <= position < b + n), position)
                        LOGGER.warning("Просмотр %s: обрыв на одном месте (%.0f) дважды — кусок пропущен", session.camera, position)
                        if gen == session.gen and await self._prepare(session, begin, started=True) is None:
                            await asyncio.sleep(RETRY_S)
                        continue
                if gen == session.gen and await self._prepare(session, position if sent else session.at_s, started=False) is None:
                    await asyncio.sleep(RETRY_S)
                continue
            # Доиграли список: дальше — куски после последнего; новых нет — живой край, ждать дописанных.
            while gen == session.gen and not session.closed and await self._prepare(session, session.last_begin, started=True) is None:
                await asyncio.sleep(RETRY_S)

    async def _forget_gone(self, session: PlaybackSession) -> bool:
        """Первого куска списка нет на диске — забыть его (день без папки — целиком); иначе отказ ffmpeg без
        конца. Нет каталога камеры — не смонтировано хранилище: куски не пропали, индекс не трогаем."""
        sink, head = self.rec.sink(session.camera), session.head
        if sink is None or head is None or not session.run:
            return False
        index, begin = sink.index, session.run[0][0]

        def check() -> bool:
            try:
                if head.exists() or not index.root.is_dir():
                    return False
                if head.parent.is_dir():
                    index.forget(begin)
                else:
                    index.drop_day(head.parent.name)
            except OSError:
                return False
            return True

        gone = await self.rec.env.run(check)
        if gone:
            LOGGER.warning("Просмотр %s: куска %s нет на диске — пропущен", session.camera, head)
        return gone

    async def _copy(self, session: PlaybackSession, proc: Any, gen: int) -> bool:
        sent = False
        fix = SpsFix()
        while chunk := await proc.stdout.read(CHUNK):
            reader = session.reader
            if reader is None or gen != session.gen:
                return sent
            data = fix.feed(chunk)
            if not data:
                continue
            if session.first_byte_at is None:
                session.first_byte_at = clock()
            try:
                await reader.write(data)
            except (ConnectionError, RuntimeError):
                return sent
            sent = True
            session.fresh.set()
        return sent

    async def _stop_proc(self, session: PlaybackSession, proc: Any = None) -> None:
        proc = proc or session.proc
        if proc is None:
            return
        if proc is session.proc:
            session.proc = None
        host_load.untrack(f"play:{session.camera}:{session.id}", getattr(proc, "pid", None))
        if proc.returncode is None:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                LOGGER.warning("Просмотр %s: ffmpeg сессии не снялся", session.id)

    async def _evict(self, camera: str) -> None:
        """Место под новую сессию — за счёт БРОШЕННЫХ (без читателя), самых давних первыми. ⚠ Смотрящую не снимать:
        тот зритель вставал, его перемотка вытесняла следующего — каскад (ревью 2026-10-27). Все смотрятся — отказ."""
        for limit, pool in ((MAX_PER_CAMERA, [s for s in self.sessions.values() if s.camera == camera]), (MAX_SESSIONS, list(self.sessions.values()))):
            idle = sorted((s for s in pool if s.reader is None), key=lambda s: s.touched)
            while len(pool) >= limit:
                if not idle:
                    raise SessionsBusy(f"запись этой камеры уже смотрят {len(pool)}" if limit == MAX_PER_CAMERA else "дом уже показывает предельное число записей")
                victim = idle.pop(0)
                pool.remove(victim)
                await self.close(victim.id)

    async def _go2rtc(self, method: str, query: str) -> None:
        url = f"http://127.0.0.1:{API_PORT}/api/streams?{query}"
        try:
            async with getattr(self.rec.env.session(), method.lower())(url) as answer:
                if answer.status >= 400:
                    LOGGER.warning("Просмотр: go2rtc ответил %s на %s", answer.status, method)
        except Exception as err:  # noqa: BLE001 — уборка потока не роняет сессию
            LOGGER.warning("Просмотр: go2rtc недоступен (%s): %s", method, err)

