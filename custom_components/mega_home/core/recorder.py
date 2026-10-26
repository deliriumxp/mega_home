"""Запись камер дома: сторож, хранилище, уборка — закрытый список, пункты 2 и 3.

Зачем. Объекту без регистратора нужна запись, а писать её может только тот, кто работает
круглосуточно, — дом (менеджер: `docs/superpowers/specs/2026-10-23-own-video-design.md`).
Что писать, как долго хранить и что считать движением — конфиг менеджера; дом исполняет и смысла
не знает. Разбор кусков одной камеры — `recorder_sink.py`.

Два раздела конфига. `video` (дом 0.6.0+) — камера одним описанием, её ведёт конвейер камеры
(`video_pipeline.py`: один ffmpeg на читаемый поток). `recorder` — прежний: у камеры свои процессы
записи (`_Camera` ниже) и превью (`recorder_preview.PreviewWriter`). Пришёл `video` — он главнее.

⚠ Поток камеры берётся у СВОЕГО go2rtc (RTSP на петле, `go2rtc_embed.RTSP_PORT`), а не у
камеры: подключение к камере одно на всех — живое видео жильца, кадр плитки, запись. Повторная
регистрация того же источника идущую запись не рвёт (замер 2026-10-23, go2rtc 1.9.14).

⚠ Куски — Matroska по 10 с без перекодирования: звук камер бывает A-law, а MP4 его не
несёт (замер стенда — «Could not find tag for codec pcm_alaw»). Время в имени — UTC,
длительность — в имени готового куска; индекс записи ведёт `recorder_index.py` в памяти.

⚠ Хранилище проверяется ДО записи и перед каждым куском: каталог не на системном диске (диске
настроек дома), а диск, который монтирует дом (`storage.py`), — смонтирован; иначе отвалившийся
диск означал бы запись на флеш контроллера до его заполнения. Системный диск — только явно
(`local`) и с лимитом объёма.
"""

from __future__ import annotations

import asyncio
import json
import os
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
from .recorder_cache import RAM_PER_CAMERA, cap_bytes, memory_available, ram_fits, shed  # noqa: F401
from .recorder_index import clean, day_of, epoch_of  # noqa: F401
from .recorder_preview import PreviewTune, PreviewWriter, sweep_legacy, tune_of
from .recorder_sink import MAX_MOTION_S, RAM_DIR, SEGMENT_LIST, SEGMENT_S, RecordSink  # noqa: F401
# Раздел конфига и годность хранилища — `recorder_spec.py`; здесь под прежними именами для службы и спек.
from .recorder_spec import CameraSpec, camera_specs, storage_problem  # noqa: F401

SERVICE = "recorder"
SCAN_S = 2.0
RESTART_S = 10.0
CLEAN_EVERY_S = 60.0
# Проба хранилища (монтирование, каталог) не держит сторож дольше: пропавший NAS отвечает отказом
# только через повторы ядра.
STORAGE_CHECK_S = 90.0
# ⚠ Готовых кусков нет дольше этого при живом ffmpeg — поток встал (камера пропала, а go2rtc держит
# соединение без данных): ffmpeg на RTSP ждёт чтения без срока, и запись молча стояла бы до
# перезапуска HA. Перезапуск процесса — тем же путём, что после его падения.
STALL_S = 90.0


class _Camera(RecordSink):
    """Камера раздела `recorder`: свой ffmpeg записи и свой процесс превью."""

    def __init__(self, recorder: Recorder, spec: CameraSpec, cache: Path) -> None:
        super().__init__(recorder, spec, cache)
        self.task: asyncio.Task[None] | None = None
        self._tail: deque[str] = deque(maxlen=8)
        self.preview = PreviewWriter(self)
        self.preview_task: asyncio.Task[None] | None = None

    async def run(self) -> None:
        await self.rec.env.run(lambda: self.cache.mkdir(parents=True, exist_ok=True))
        while True:
            try:
                if not self.index.loaded:
                    await self.rec.env.run(self.index.ensure, bool(self.rec.limit))
                await self.register()
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

    async def register(self) -> None:
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
        self.restart_list()
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
        # Предел кэша держит свой цикл: он не ждёт ни сеть, ни разбор кусков (`_collect`).
        guard = asyncio.ensure_future(self._guard()) if self.in_ram else None
        self.why = ""
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
            if guard is not None:
                guard.cancel()
            host_load.untrack(f"record:{self.spec.id}", proc.pid)
        raise RuntimeError(f"ffmpeg завершился ({proc.returncode}): {' | '.join(self._tail) or 'без слов'}")

    async def _guard(self) -> None:
        while True:
            await asyncio.sleep(SCAN_S)
            await self.guard_once()

    async def _drain(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for line in proc.stderr:
            text = line.decode(errors="ignore").strip()
            if text:
                self._tail.append(text)


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

    def sinks(self) -> list[RecordSink]:
        """Запись всех камер — делят память кэша (`RecordSink.guard_once`) и уборку."""
        return list(self.cameras.values())

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
        await self.env.run(clean, self.storage, self.sinks(), time.time(), self.limit)
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
        # ⚠ Свободное место `/dev/shm` память не меряет (половина памяти хоста): смотрим, сколько её реально.
        if not ram_fits(motion_cameras, shutil.disk_usage(RAM_DIR).free):
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
