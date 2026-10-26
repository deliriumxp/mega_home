"""Конвейер камеры: один хозяин её потоков в go2rtc и её процессов (раздел `video`, дом 0.6.0+).

Зачем. До 0.6.0 у камеры было до трёх процессов (запись, превью, детекция), каждый со своей копией
цикла жизни, и до четырёх мест, где её поток заводился в go2rtc (бандл, запись, превью, детекция).
Здесь камеру ведёт один объект (`docs/superpowers/specs/2026-10-26-video-engine-rewrite-design.md`
менеджера, «Конвейер камеры»):
  * заводит ВСЕ потоки камеры в своём go2rtc — имена те же, что у живого видео бандла;
  * держит по процессу на поток, который читает (`video_reader.py`), — одним циклом (`proc_runner.py`);
  * запись по движению своей детекции включает ВЫЗОВОМ (`RecordSink.motion`), без подстрок в событии;
  * силу движения копит своим рядом (`activity.ActivityBuckets`), последний кадр держит в памяти
    (`latest_frame.py`) — плитка, кадр события и редактор маски берут его без ffmpeg на запрос.
События движения и людей уходят концентратором тем же кадром, что у источника `motion` слушателя:
лента, менеджер и правила вложений разницы не видят.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from . import motion
from .activity import ActivityBuckets
from .go2rtc_embed import API_PORT
from .latest_frame import LatestFrame, pump
from .proc_runner import ProcessRunner
from .recorder_preview import PreviewSink
from .recorder_sink import RecordSink
from .video_reader import ReaderFds, ReaderSpec, reader_command, reader_plan
from .video_spec import CameraPlan, record_spec

if TYPE_CHECKING:
    from .recorder import Recorder

# Нет ни готового куска, ни кадра детекции, ни JPEG дольше этого — поток встал (камера пропала, а go2rtc
# держит соединение без данных). Первый кадр холодного потока go2rtc собирает секунды — запас с этим.
STALL_S = 60.0
# Источник событий своей детекции — тот же, что у слушателя (`listeners.py`, тип `motion`).
SOURCE = "motion"


class CameraPipeline:
    def __init__(self, recorder: Recorder, plan: CameraPlan, cache_root: Path) -> None:
        self.rec, self.plan = recorder, plan
        self.cache = cache_root / plan.id
        self.record: RecordSink | None = RecordSink(recorder, record_spec(plan), cache_root) if plan.record else None
        self.preview: PreviewSink | None = PreviewSink(self.record) if self.record is not None else None
        if self.record is not None:
            self.record.preview = self.preview
        self.activity = ActivityBuckets()
        self.frame = LatestFrame()
        # Пишет ли конвейер сейчас (хранилище годно); детекция и кадр идут и без него.
        self.record_on = False
        self.task: asyncio.Task[None] | None = None
        self.runners: dict[str, ProcessRunner] = {}
        self._progress: dict[str, float] = {}
        self._loops: dict[str, motion.MotionLoop] = {}
        self._consumers: dict[str, list[Any]] = {}

    def latest(self) -> tuple[bytes, float] | None:
        return self.frame.jpeg()

    def motion_changed(self, on: bool, at: float) -> None:
        if self.record is not None:
            self.record.motion(on, at)

    def share(self, at: float, value: float) -> None:
        self.activity.note(at, value)

    def emit(self, event: str, data: dict[str, Any], file: tuple[str, bytes] | None) -> None:
        # `local` — запись по движению и лента внутри дома видят событие без менеджера.
        self.rec.publish(self.plan.id, SOURCE, event, data, local=True, file=file)

    async def register(self) -> None:
        """Все потоки камеры — в свой go2rtc. ⚠ Повторная регистрация того же источника идущие потоки не
        рвёт (go2rtc 1.9.14), а после перезапуска go2rtc возвращает их на место."""
        for stream in self.plan.streams.values():
            url = f"http://127.0.0.1:{API_PORT}/api/streams?name={quote(stream.name, safe='')}&src={quote(stream.source, safe='')}"
            async with self.rec.env.session().put(url) as answer:
                if answer.status >= 400:
                    raise RuntimeError(f"go2rtc не принял поток {stream.name} ({answer.status})")

    async def run(self, record_on: bool) -> None:
        """Держать камеру, пока задачу не снимут. Хозяин — сторож записи (`Recorder._check`)."""
        self.record_on = record_on
        env, record = self.rec.env, self.record if record_on else None
        await env.run(lambda: (self.cache / "preview").mkdir(parents=True, exist_ok=True))
        if record is not None:
            if not record.index.loaded:
                await env.run(record.index.ensure, bool(self.rec.limit))
            if not record.previews.loaded:
                await env.run(record.previews.load)
        guard = asyncio.ensure_future(self._guard(record)) if record is not None and record.in_ram else None
        try:
            await self.register()
            await asyncio.gather(*(self._reader(spec) for spec in reader_plan(self.plan, self.cache, record_on)))
        finally:
            if guard is not None:
                guard.cancel()
            for key in list(self._loops) + list(self._consumers):
                self._retire(key)

    async def _guard(self, record: RecordSink) -> None:
        """Предел кэша в памяти — своим циклом: разбор кусков ждёт хранилище, а память ждать не может."""
        while True:
            await asyncio.sleep(2.0)
            await record.guard_once()

    async def _reader(self, spec: ReaderSpec) -> None:
        runner = ProcessRunner(self.rec.env, f"cam:{self.plan.id}:{spec.key}", stall_s=STALL_S)
        self.runners[spec.key] = runner
        await runner.run(lambda: self._spawn(spec), lambda: self._tick(spec))

    async def _spawn(self, spec: ReaderSpec) -> asyncio.subprocess.Process:
        self._retire(spec.key)
        loop = asyncio.get_running_loop()
        binary, nice = await self.rec.env.run(lambda: (shutil.which("ffmpeg"), shutil.which("nice")))
        if not binary:
            raise RuntimeError("в системе нет ffmpeg")
        person_r, person_w = os.pipe() if spec.person_width is not None else (None, None)
        latest_r, latest_w = os.pipe() if spec.latest else (None, None)
        if spec.record_dir is not None and self.record is not None:
            self.record.restart_list()
        argv = reader_command(binary, nice or "", spec, self.rec.preview, ReaderFds(person_w, latest_w))
        writers = tuple(fd for fd in (person_w, latest_w) if fd is not None)
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE if spec.detect_fps is not None else asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                pass_fds=writers,
                env={**os.environ, "TZ": "UTC"},
            )
        finally:
            for fd in writers:
                os.close(fd)
        self._progress[spec.key] = time.monotonic()
        consumers: list[Any] = []
        if spec.detect_fps is not None:
            consumers += await self._detection(spec, proc, loop, person_r)
        if latest_r is not None:
            reader, transport = await _pipe_reader(loop, latest_r)
            consumers += [transport, asyncio.ensure_future(pump(reader, self.frame, lambda: self._mark(spec.key)))]
        self._consumers[spec.key] = consumers
        return proc

    async def _detection(self, spec: ReaderSpec, proc: asyncio.subprocess.Process, loop: Any, person_r: int | None) -> list[Any]:
        assert self.plan.detect is not None
        params = self.plan.detect.params
        np = await loop.run_in_executor(None, importlib.import_module, "numpy")
        watch, model, tune = await motion._person_setup(loop, np, params)  # noqa: SLF001
        # Движение у камеры своё (подписка): детекция дома — только ради людей и окна записи не открывает.
        own = params.get("motionEvents") is not False
        run = motion.MotionLoop(
            np, params, emit=self.emit, on_motion=self.motion_changed if own else None, on_share=self.share,
            load_key=f"cam:{self.plan.id}:{spec.key}", person=(watch, model, tune) if watch is not None else None,
        )
        self._loops[spec.key] = run
        tasks: list[Any] = [asyncio.ensure_future(self._frames(proc, run, spec.key))]
        if person_r is not None:
            if watch is not None:
                tasks.append(asyncio.ensure_future(motion._person_frames(loop, np, person_r, run.big)))  # noqa: SLF001
            else:
                # Сеть «человек» не загрузилась — трубу всё равно читать: непрочитанная остановила бы ffmpeg.
                reader, transport = await _pipe_reader(loop, person_r)
                tasks += [transport, asyncio.ensure_future(_discard(reader))]
        return tasks

    async def _frames(self, proc: asyncio.subprocess.Process, run: motion.MotionLoop, key: str) -> None:
        assert proc.stdout is not None
        try:
            while True:
                raw = await proc.stdout.readexactly(motion.FRAME)
                self._mark(key)
                await run.frame(raw)
        except asyncio.IncompleteReadError:
            return

    def _mark(self, key: str) -> None:
        self._progress[key] = time.monotonic()

    async def _tick(self, spec: ReaderSpec) -> float:
        """Разобрать готовое и сказать, когда читатель в последний раз дал что-то."""
        last = self._progress.get(spec.key, 0.0)
        if spec.record_dir is not None and self.record is not None:
            last = max(last, await self.record.collect())
        if spec.preview_dir is not None and self.preview is not None:
            await self.rec.env.run(self.preview.collect, False)
        return last

    def _retire(self, key: str) -> None:
        run = self._loops.pop(key, None)
        if run is not None:
            run.close()
        for item in self._consumers.pop(key, []):
            (item.close if hasattr(item, "is_closing") else item.cancel)()

    def state(self) -> dict[str, Any]:
        shot = self.frame.jpeg()
        out: dict[str, Any] = self.record.state() if self.record is not None else {"mode": None}
        why = "; ".join(f"{key}: {runner.why}" for key, runner in self.runners.items() if runner.why)
        return {**out, "readers": why, "frameAt": round(shot[1], 3) if shot else None, "recording": self.record_on}


async def _pipe_reader(loop: Any, fd: int) -> tuple[asyncio.StreamReader, Any]:
    reader = asyncio.StreamReader(limit=2**24)
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(fd, "rb", 0))
    return reader, transport


async def _discard(reader: asyncio.StreamReader) -> None:
    while await reader.read(65536):
        pass
