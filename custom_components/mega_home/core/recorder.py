"""Запись камер дома: процесс на камеру, куски, уборка — закрытый список, пункты 2 и 3.

Зачем. Объекту без регистратора нужна запись, а писать её может только тот, кто работает
круглосуточно, — дом (менеджер: `docs/superpowers/specs/2026-10-23-own-video-design.md`).
Что писать, как долго хранить и что считать движением — раздел `recorder` конфига менеджера;
дом исполняет и смысла не знает.

⚠ Поток камеры берётся у СВОЕГО go2rtc (RTSP на петле, `go2rtc_embed.RTSP_PORT`), а не у
камеры: подключение к камере одно на всех — живое видео жильца, кадр плитки, запись. Поток
заводится под тем же именем и той же строкой, что у бандла; повторная регистрация того же
источника идущую запись не рвёт (замер 2026-10-23, go2rtc 1.9.14).

⚠ Куски — Matroska по 10 с без перекодирования: звук камер бывает A-law, а MP4 его не
несёт (замер стенда — «Could not find tag for codec pcm_alaw»). Время в имени — UTC,
длительность — в имени готового куска; индекс записи ведёт `recorder_index.py` в памяти.

⚠ «По движению» — по образцу Frigate и предзаписи регистраторов (движение — от камеры или своей
детекцией, `motion.py`, одним путём через концентратор событий): кусок ждёт в кэше, пока не
станет ясно, попал ли он в окно движения (запас до и после); не попал — удаляется, и на диск
или в сеть без движения не пишется ничего. Кэш — в памяти (`/dev/shm`), если её хватает.

⚠ Хранилище проверяется ДО записи и перед каждым куском: каталог не на системном диске (диске
настроек дома), а диск, который монтирует дом (`storage.py`), — смонтирован; иначе отвалившийся
диск означал бы запись на флеш контроллера до его заполнения. Системный диск — только явно
(`local`) и с лимитом объёма.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from . import activity, host_load, services, storage
from .const import LOGGER
from .go2rtc_embed import API_PORT, RTSP_PORT
from .host import Host
from .recorder_index import SegmentIndex, clean, day_of, epoch_of, segment_name
from .recorder_preview import PreviewIndex, PreviewTune, PreviewWriter, sweep_legacy, tune_of
# Раздел конфига и годность хранилища — `recorder_spec.py`; здесь под прежними именами для службы и спек.
from .recorder_spec import CameraSpec, camera_specs, storage_problem  # noqa: F401

SERVICE = "recorder"
SEGMENT_S = 10
SCAN_S = 2.0
RESTART_S = 10.0
CLEAN_EVERY_S = 60.0
# Проба хранилища (монтирование, каталог) не держит сторож дольше: пропавший NAS отвечает отказом
# только через повторы ядра.
STORAGE_CHECK_S = 90.0
# Камера, не сказавшая «движение кончилось», не держит окно вечно.
MAX_MOTION_S = 300.0
# ⚠ Готовых кусков нет дольше этого при живом ffmpeg — поток встал (камера пропала, а go2rtc держит
# соединение без данных): ffmpeg на RTSP ждёт чтения без срока, и запись молча стояла бы до
# перезапуска HA. Перезапуск процесса — тем же путём, что после его падения.
STALL_S = 90.0
RAM_DIR = Path("/dev/shm")
# Кэш «по движению» на камеру: запас до события плюс кусок в работе (~4 Мбит/с, с запасом).
RAM_PER_CAMERA = 64 * 1024**2
# Список готовых кусков ведёт сам ffmpeg (`-segment_list`, CSV «файл,начало,конец»): длительность
# куска и «кусок закрыт» без ffprobe на каждый кусок и без догадки «последний ещё пишется».
SEGMENT_LIST = "segments.csv"


class _Camera:
    """Одна камера: ffmpeg пишет куски в кэш, разбор раскладывает их в хранилище."""

    def __init__(self, recorder: Recorder, spec: CameraSpec, cache: Path) -> None:
        self.rec, self.spec, self.cache = recorder, spec, cache / spec.id
        self.index = SegmentIndex(recorder.storage / spec.id)
        self.task: asyncio.Task[None] | None = None
        self.why = ""
        self.last_saved = 0.0
        self.saved = 0
        self.dropped = 0
        self._tail: deque[str] = deque(maxlen=8)
        # Готовые куски по списку ffmpeg: имя → длительность; прочитанная часть списка — смещением.
        self._listed: dict[str, float] = {}
        self._list_offset = 0
        self._last_ready = 0.0
        # Окна движения: [начало, конец или None, последнее «есть движение»] — часы дома.
        self.windows: list[list[float | None]] = []
        # Превью-видео камеры (`recorder_preview.py`): свой процесс, свой индекс.
        self.previews = PreviewIndex(recorder.storage / spec.id)
        self.preview = PreviewWriter(self)
        self.preview_task: asyncio.Task[None] | None = None

    async def run(self) -> None:
        await self.rec.env.run(lambda: self.cache.mkdir(parents=True, exist_ok=True))
        while True:
            try:
                if not self.index.loaded:
                    await self.rec.env.run(self.index.ensure, bool(self.rec.limit))
                await self._register()
                await self._record()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — камера не роняет запись остальных
                self.why = f"запись не идёт: {err}"
                LOGGER.warning("Запись %s: %s", self.spec.id, err)
            # ⚠ Дописать начатое — тоже внутри защиты: отказ диска (ENOSPC, EIO, EROFS) иначе
            # ронял бы задачу камеры насовсем, и записи не было бы до перезапуска HA.
            try:
                await self._collect(final=True)
            except Exception as err:  # noqa: BLE001
                self.why = f"кусок не сохранился: {err}"
            await asyncio.sleep(RESTART_S)

    async def _register(self) -> None:
        url = (
            f"http://127.0.0.1:{API_PORT}/api/streams?name={quote(self.spec.stream, safe='')}"
            f"&src={quote(self.spec.source, safe='')}"
        )
        async with self.rec.env.session().put(url) as answer:
            if answer.status >= 400:
                raise RuntimeError(f"go2rtc не принял поток ({answer.status})")

    async def _record(self) -> None:
        binary = await self.rec.env.run(shutil.which, "ffmpeg")
        if not binary:
            raise RuntimeError("в системе нет ffmpeg")
        self._listed, self._list_offset = {}, 0
        proc = await asyncio.create_subprocess_exec(
            binary, "-hide_banner", "-loglevel", "error", "-nostdin",
            "-rtsp_transport", "tcp", "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{self.spec.stream}",
            "-map", "0:v", "-map", "0:a?", "-c", "copy",
            "-f", "segment", "-segment_time", str(SEGMENT_S), "-segment_format", "matroska",
            "-reset_timestamps", "1", "-strftime", "1",
            "-segment_list", str(self.cache / SEGMENT_LIST), "-segment_list_type", "csv",
            str(self.cache / "%Y%m%dT%H%M%S.mkv"),
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE, env={**os.environ, "TZ": "UTC"},
        )
        drain = asyncio.ensure_future(self._drain(proc))
        self.why = ""
        self._last_ready = time.monotonic()
        host_load.track(f"record:{self.spec.id}", proc.pid)
        try:
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(proc.wait()), SCAN_S)
                    break
                except asyncio.TimeoutError:
                    await self._collect()
                if time.monotonic() - self._last_ready > STALL_S:
                    proc.terminate()
                    raise RuntimeError(f"готовых кусков нет дольше {STALL_S:g} с — поток встал")
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), 5)
                except asyncio.TimeoutError:
                    proc.kill()
            drain.cancel()
            host_load.untrack(f"record:{self.spec.id}", proc.pid)
        raise RuntimeError(f"ffmpeg завершился ({proc.returncode}): {' | '.join(self._tail) or 'без слов'}")

    async def _drain(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for line in proc.stderr:
            text = line.decode(errors="ignore").strip()
            if text:
                self._tail.append(text)

    def _read_list(self) -> None:
        """Дочитать список ffmpeg с прошлого места: строка `файл,начало,конец` на готовый кусок."""
        path = self.cache / SEGMENT_LIST
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size < self._list_offset:
            self._list_offset = 0
        if size == self._list_offset:
            return
        with path.open("rb") as handle:
            handle.seek(self._list_offset)
            chunk = handle.read(size - self._list_offset)
        # Неполная последняя строка дочитается следующим разом.
        cut = chunk.rfind(b"\n") + 1
        self._list_offset += cut
        for line in chunk[:cut].decode(errors="ignore").splitlines():
            parts = line.rsplit(",", 2)
            try:
                self._listed[Path(parts[0]).name] = max(float(parts[2]) - float(parts[1]), 0.0)
            except (IndexError, ValueError):
                continue

    async def _collect(self, final: bool = False) -> None:
        """Готовые куски (по списку ffmpeg) — в хранилище или в корзину. `final` — и тот, что
        ffmpeg не успел закрыть в список (остановлен): длительность у него спрашивает ffprobe."""
        await self.rec.env.run(self._read_list)
        files = sorted(await self.rec.env.run(lambda: list(self.cache.glob("*.mkv"))))
        ready = files if final else [path for path in files if path.name in self._listed]
        if ready:
            self._last_ready = time.monotonic()
        now = time.time()
        for path in ready:
            start = epoch_of(path.stem)
            if start is None:
                await self.rec.env.run(path.unlink, True)
                continue
            length = self._listed.pop(path.name, 0.0) or await self.rec.probe(path)
            if length <= 0:
                await self.rec.env.run(path.unlink, True)
                continue
            if not self.spec.motion_mode or self.wanted(start, start + length, now):
                await self.rec.env.run(self._save, path, start, length)
            elif start + length + self.spec.pre_s < now - SEGMENT_S or final:
                self.dropped += 1
                await self.rec.env.run(path.unlink, True)

    def _save(self, path: Path, start: float, length: float) -> None:
        # ⚠ Перед КАЖДЫМ куском: отвалившийся диск оставляет пустой каталог на флеше контроллера.
        problem = self.rec.problem()
        if problem:
            raise OSError(problem)
        size = path.stat().st_size
        target = self.index.root / day_of(start) / segment_name(start, int(length * 1000))
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(target))
        # В индекс — после переноса: сорвавшийся перенос не оставляет в нём куска, которого нет.
        self.index.add(start, length, size)
        self.saved += 1
        self.last_saved = start + length

    def wanted(self, start: float, stop: float, now: float) -> bool:
        """Попал ли кусок в окно движения с запасом до и после."""
        for begin, end, _seen in self.windows:
            if begin - self.spec.pre_s <= stop and start <= (end if end is not None else now) + self.spec.post_s:
                return True
        return False

    def on_event(self, frame: dict[str, Any]) -> None:
        """Ответ источника событий: смены «началось/кончилось» по порядку, каждая — своим шагом.

        ⚠ Не «последнее слово решает»: короткое движение между двумя опросами приходит одним
        ответом «началось, кончилось», и по последнему слову окна не было бы вовсе.
        """
        if frame.get("access") != self.spec.access or frame.get("source") != self.spec.events:
            return
        data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
        # Ответ устройства — его текст; событие дома (своя детекция, `motion.py`) — данные одной
        # строкой JSON без пробелов: подстроки `on`/`off` менеджер пишет под эту форму.
        text = str(data.get("text") or "") or json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        at = float(frame.get("at") or time.time())
        for message in text.split(self.spec.split) if self.spec.split else [text]:
            if self.spec.require and not any(word in message for word in self.spec.require):
                continue
            if any(word in message for word in self.spec.skip):
                continue
            marks = sorted(
                [(m.start(), True) for word in self.spec.on for m in re.finditer(re.escape(word), message)]
                + [(m.start(), False) for word in self.spec.off for m in re.finditer(re.escape(word), message)]
            )
            for _pos, on in marks:
                self._step(on, at)
        self._expire(at)

    def _step(self, on: bool, at: float) -> None:
        open_ = self.windows[-1] if self.windows and self.windows[-1][1] is None else None
        if on:
            if open_ is None:
                self.windows.append([at, None, at])
            else:
                open_[2] = at
        elif open_ is not None:
            open_[1] = at
        else:
            # «Кончилось» без открытого окна — движение уложилось между опросами: окно-точка,
            # запас до и после сохранит его куски.
            self.windows.append([at, at, at])

    def _expire(self, now: float) -> None:
        for window in self.windows:
            if window[1] is None and now - float(window[2] or now) > MAX_MOTION_S:
                window[1] = window[2]
        keep = now - self.spec.post_s - 3600
        self.windows = [w for w in self.windows if w[1] is None or float(w[1]) > keep]

    def store_activity(self, rows: list[tuple[int, float]]) -> None:
        """Корзины силы движения — в `<день>/activity.csv`; уходят вместе с каталогом дня (`clean`)."""
        if self.rec.problem():
            return
        for day in sorted({day_of(bucket) for bucket, _value in rows}):
            activity.append(self.index.root / day, [row for row in rows if day_of(row[0]) == day])

    def state(self) -> dict[str, Any]:
        return {
            "mode": "motion" if self.spec.motion_mode else "continuous",
            "why": self.why,
            "saved": self.saved,
            "dropped": self.dropped,
            "lastSaved": round(self.last_saved, 3) or None,
            "motion": bool(self.windows and self.windows[-1][1] is None),
            "preview": self.preview.why,
        }


class Recorder:
    """Раздел `recorder` конфига → камеры, уборка и служба на петле (`recorder_api.py`)."""

    def __init__(self, env: Host, subscribe: Callable[[Callable[[dict[str, Any]], None]], Callable[[], None]]) -> None:
        self.env = env
        self._subscribe = subscribe
        self._unsubscribe: Callable[[], None] | None = None
        self._key: str | None = None
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self.cameras: dict[str, _Camera] = {}
        self.storage = Path("/")
        # Что смонтировать под хранилище (`storage.mount`), разрешён ли системный диск и лимит, байты.
        self.mount: dict[str, Any] | None = None
        self.local = False
        self.limit = 0
        # Превью-видео (`recorder_preview.PreviewTune`) — данными раздела, как и всё остальное.
        self.preview = PreviewTune()
        self.why = ""
        self._api: Any = None
        self._api_task: asyncio.Task[None] | None = None
        self._specs: list[CameraSpec] = []
        self._supervisor: asyncio.Task[None] | None = None
        # Уборка кадров и спрайтов прежних версий (`recorder_preview.sweep_legacy`) — раз на запуск камер.
        self._sweep: asyncio.Task[None] | None = None
        # Идущая проба хранилища (монтирование и каталог): вторая поверх висящей не заводится.
        self._checking: asyncio.Future[str] | None = None

    def apply(self, config: dict[str, Any] | None) -> None:
        # ⚠ Служба на петле — всегда, а не только при записи: папку хранилища выбирают ДО записи.
        # Не поднялась (порт, ошибка) — новая попытка на следующем конфиге.
        if self._api is None and (self._api_task is None or self._api_task.done()):
            self._api_task = self.env.spawn(self._start_api(), "mega_home: служба записи")
        section = (config or {}).get("recorder") if isinstance((config or {}).get("recorder"), dict) else None
        key = json.dumps(section, sort_keys=True)
        if key == self._key:
            return
        self._key = key
        self.env.spawn(self._reconfigure(section), "mega_home: запись камер")

    async def _start_api(self) -> None:
        from .recorder_api import start_api  # noqa: PLC0415

        self._api = await start_api(self)

    async def async_stop(self) -> None:
        self._key = None
        await self._reconfigure(None)
        task, self._api_task = self._api_task, None
        if isinstance(task, asyncio.Future) and not task.done():
            task.cancel()
        services.unregister(SERVICE)
        if self._api is not None:
            await self._api.cleanup()
            self._api = None
        # Подключённое для выбора папки — дому больше не нужно (рабочее хранилище живёт до контейнера).
        await self.env.run(storage.unmount, storage.PREVIEW_POINT)

    async def _reconfigure(self, section: dict[str, Any] | None) -> None:
        async with self._lock:
            await self._stop_all()
            self._specs = camera_specs(section)
            self.storage = Path(str((section or {}).get("storage") or ""))
            mount = (section or {}).get("mount") if isinstance((section or {}).get("mount"), dict) else None
            if self.mount is not None and mount != self.mount:
                # Хранилище сменили: прежний диск или сетевая папка больше не нужны дому.
                await self.env.run(storage.unmount, storage.MOUNT_POINT)
            self.mount = mount
            self.local = (section or {}).get("local") is True
            self.limit = int(float((section or {}).get("limitGb") or 0) * 1024**3)
            self.preview = tune_of((section or {}).get("preview"))
            self.why = ""
            if self._specs:
                self._supervisor = self.env.spawn(self._supervise(), "mega_home: запись камер, сторож")

    def problem(self) -> str:
        """Почему писать в хранилище нельзя сейчас; пусто — можно (в исполнителе: файловая система)."""
        if self.local and not self.limit:
            return "запись на системный диск — только с лимитом объёма"
        return storage_problem(self.storage, self.env.path(), self.local, storage.MOUNT_POINT if self.mount else None)

    async def _supervise(self) -> None:
        """Сторож записи: раз в минуту — хранилище, камеры и уборка.

        ⚠ Без него запись не переживала бы ничего: конфиг приходит раз в 15 минут и только
        при смене, а диск отваливается (NAS после сбоя сети HA сам не монтирует), монтируется
        позже старта HA, и задача камеры может упасть.
        """
        while True:
            try:
                await self._check()
            except Exception as err:  # noqa: BLE001 — сторож не умирает от одного прохода
                LOGGER.warning("Запись камер: проверка не прошла: %s", err)
            await asyncio.sleep(CLEAN_EVERY_S)

    def _storage_check(self) -> str:
        """Смонтировать, если пропало, и проверить каталог — одним блокирующим вызовом (исполнитель)."""
        problem = storage.mount(self.mount) if self.mount else ""
        return problem or self.problem()

    async def _check(self) -> None:
        async with self._lock:
            # Диск или сетевая папка дома: смонтировать, если пропали (NAS после сбоя сети, USB
            # переподключили, контейнер перезапустился — монтирование живёт до его остановки).
            # ⚠ Проба — ОДНА за раз: `mount(2)` к пропавшему NAS держит поток исполнителя минутами, а
            # срок ожидания его не снимает. Новая проба каждую минуту поверх висящей исчерпала бы
            # пул потоков HA — не только запись (ревью хранилища 2026-10-23).
            if self._checking is None or self._checking.done():
                self._checking = asyncio.ensure_future(self.env.run(self._storage_check))
            try:
                problem = await asyncio.wait_for(asyncio.shield(self._checking), STORAGE_CHECK_S)
            except asyncio.TimeoutError:
                problem = "хранилище не отвечает (сетевая папка недоступна?)"
            except Exception as err:  # noqa: BLE001 — отказ ФС (stat по мёртвой точке) — это причина, не падение
                problem = f"хранилище не отвечает: {err}"
            if problem:
                if problem != self.why:
                    LOGGER.warning("Запись камер: %s", problem)
                self.why = problem
                # Писать некуда — камеры стоят; начатое в кэше не переносим на флеш.
                await self._stop_cameras(collect=False)
                return
            self.why = ""
            if not self.cameras:
                await self._start_cameras()
            for camera in self.cameras.values():
                if camera.task is None or camera.task.done():
                    camera.task = self.env.spawn(camera.run(), f"mega_home: запись {camera.spec.id}")
                if camera.preview_task is None or camera.preview_task.done():
                    camera.preview_task = self.env.spawn(camera.preview.run(), f"mega_home: превью {camera.spec.id}")
        await self.env.run(clean, self.storage, list(self.cameras.values()), time.time(), self.limit)
        # Сила движения (`activity.py`): корзины забираются в цикле событий — их пишет детектор, — а
        # дописываются в ряд суток в исполнителе.
        for camera in list(self.cameras.values()):
            rows = activity.take(camera.spec.access, time.time())
            if rows:
                await self.env.run(camera.store_activity, rows)

    async def _start_cameras(self) -> None:
        ram = await self.env.run(self._ram_cache, sum(1 for s in self._specs if s.motion_mode))
        disk = self.storage / ".cache"
        await self.env.run(lambda: disk.mkdir(exist_ok=True))
        self.cameras = {
            spec.id: _Camera(self, spec, ram if spec.motion_mode and ram is not None else disk) for spec in self._specs
        }
        self._unsubscribe = self._subscribe(self._on_event)
        roots = [camera.index.root for camera in self.cameras.values()]
        self._sweep = self.env.spawn(self._sweep_legacy(roots), "mega_home: уборка кадров прежних версий")

    async def _sweep_legacy(self, roots: list[Path]) -> None:
        for root in roots:
            try:
                await self.env.run(sweep_legacy, root)
            except OSError as err:
                LOGGER.warning("Запись: прежние кадры превью в %s не убраны: %s", root, err)

    async def _stop_cameras(self, collect: bool) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        tasks = [t for c in self.cameras.values() for t in (c.task, c.preview_task) if t is not None]
        if self._sweep is not None:
            tasks.append(self._sweep)
            self._sweep = None
        for task in tasks:
            task.cancel()
        await asyncio.gather(*[t for t in tasks if isinstance(t, asyncio.Future)], return_exceptions=True)
        for camera in self.cameras.values() if collect else []:
            try:
                await camera._collect(final=True)  # noqa: SLF001 — дописать начатое до остановки
                await self.env.run(camera.preview.collect, True)
            except Exception as err:  # noqa: BLE001
                LOGGER.warning("Запись %s: начатое не сохранилось: %s", camera.spec.id, err)
        self.cameras = {}

    async def _stop_all(self) -> None:
        supervisor, self._supervisor = self._supervisor, None
        if supervisor is not None:
            supervisor.cancel()
            if isinstance(supervisor, asyncio.Future):
                await asyncio.gather(supervisor, return_exceptions=True)
        await self._stop_cameras(collect=True)

    def _ram_cache(self, motion_cameras: int) -> Path | None:
        """Кэш камер «по движению» в памяти, если её хватает; нет — их кэш на диске хранилища.

        ⚠ Постоянная запись — всегда кэш на диске хранилища: перенос внутри одной файловой
        системы — переименование, а из памяти — копия каждого куска.
        """
        if not motion_cameras or not RAM_DIR.is_dir():
            return None
        if shutil.disk_usage(RAM_DIR).free < motion_cameras * RAM_PER_CAMERA:
            return None
        path = RAM_DIR / "mega_home_rec"
        path.mkdir(exist_ok=True)
        return path

    def _on_event(self, frame: dict[str, Any]) -> None:
        for camera in self.cameras.values():
            camera.on_event(frame)

    async def probe(self, path: Path) -> float:
        """Длительность куска по ffprobe — только у куска, который ffmpeg не закрыл в список
        (остановлен посреди); не прочитался — 0 (кусок битый)."""
        binary = await self.env.run(shutil.which, "ffprobe")
        if not binary:
            return float(SEGMENT_S)
        proc = await asyncio.create_subprocess_exec(
            binary, "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), 15)
            return float(out.decode().strip() or 0)
        except (asyncio.TimeoutError, ValueError):
            if proc.returncode is None:
                proc.kill()
            return 0.0

    def state(self) -> dict[str, Any]:
        return {
            "enabled": self._key not in (None, "null"),
            "storage": str(self.storage) if self._specs else None,
            "mount": {"type": self.mount.get("type"), "source": self.mount.get("source")} if self.mount else None,
            "limitGb": round(self.limit / 1024**3, 1) if self.limit else None,
            "cache": sorted({str(c.cache.parent) for c in self.cameras.values()}) or None,
            "why": self.why,
            "cameras": {cid: camera.state() for cid, camera in self.cameras.items()},
        }

