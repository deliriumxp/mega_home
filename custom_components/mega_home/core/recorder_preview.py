"""Превью-видео записи: протяжка под пальцем — сдвигом времени в маленьком видео на телефоне.

Решение заказчика 2026-10-24 — по образцу Frigate (`frigate/output/preview.py`, `PreviewPlayer.tsx`
его веба): превью — отдельное видео низкого качества, телефон грузит кусок один раз и листает его
сам, дом на каждое движение пальца не делает ничего. Снято (живой дом 2026-10-24): кадр JPEG к
каждому куску и спрайт окна, собранный ffmpeg по запросу, — 15–18 с на окно при сетевой папке,
превью под пальцем не успевало никогда.

⚠ Свой процесс на камеру, а не второй выход ffmpeg записи: отказ кодировщика превью не роняет
запись. Подключение к камере не прибавляется — поток тот же, у своего go2rtc на петле.
⚠ Декодируются только ключевые кадры (`-skip_frame nokey`): вдесятеро дешевле полного декода
основного потока, кадр превью — раз в группу кадров камеры (у камер объекта 2–3 с).
⚠ Время — по часам дома (`-use_wallclock_as_timestamps`, `-copyts`): список кусков ffmpeg
(`-segment_list`) называет начало и конец куска в секундах UTC, ffprobe не нужен. Внутри файла
время с нуля (`-reset_timestamps`): место в видео = метка − начало куска.
⚠ mp4 фрагментами (`frag_keyframe`, ключевой кадр каждые 5 кадров): идущий кусок читается, пока
пишется; служба отдаёт его до последнего целого фрагмента (`whole_fragments`). ⚠ `flush_packets=1`
вложенному muxer обязателен: без него буфер вывода копит фрагменты до закрытия куска, и идущий
кусок 40 с стоял на 28 байтах (замер на камере объекта, ffmpeg 7.1).
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import time
from bisect import bisect_right
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from . import host_load
from .const import LOGGER
from .go2rtc_embed import RTSP_PORT
from .recorder_index import day_of, epoch_of, length_ms

if TYPE_CHECKING:
    from .recorder_sink import RecordSink

PREVIEW_S = 600
PREVIEW_DIR = "preview"
PREVIEW_LIST = "preview.csv"
# Умолчания раздела `recorder.preview` конфига: менеджер шлёт их данными — подстройка без релиза дома.
HEIGHT, CRF, GOP = 180, 33, 5
SCAN_S = 5.0
RESTART_S = 10.0
# Идущий кусок не растёт дольше этого — поток встал (тот же сторож, что у записи, `STALL_S`).
STALL_S = 120.0
# Отметка, что прежние кадры к кускам и спрайты убраны: уборка — один раз на камеру.
SWEPT = ".preview-video"
_NAME = re.compile(r"^(\d{6})-(\d+)\.mp4$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PreviewTune(NamedTuple):
    """Высота кадра, качество (`crf` x264) и ключевой кадр каждые `gop` кадров превью."""

    height: int = HEIGHT
    crf: int = CRF
    gop: int = GOP


def tune_of(raw: Any) -> PreviewTune:
    """Раздел `preview` конфига записи → значения в пределах; чужое и пропущенное — умолчание."""
    raw = raw if isinstance(raw, dict) else {}

    def bounded(key: str, default: int, low: int, high: int) -> int:
        try:
            return min(max(int(raw.get(key, default)), low), high)
        except (TypeError, ValueError):
            return default

    # Высота — чётная: `scale=-2:H` и yuv420p.
    return PreviewTune(bounded("height", HEIGHT, 90, 720) // 2 * 2, bounded("crf", CRF, 18, 45), bounded("gop", GOP, 1, 30))


def command(binary: str, nice: str, stream: str, cache: Path, tune: PreviewTune = PreviewTune()) -> list[str]:
    return [
        *([nice, "-n", "10"] if nice else []), binary, "-hide_banner", "-loglevel", "error", "-nostdin",
        "-rtsp_transport", "tcp", "-use_wallclock_as_timestamps", "1", "-skip_frame", "nokey",
        "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{stream}", "-copyts", "-map", "0:v:0", "-an",
        "-vf", f"scale=-2:{tune.height},format=yuv420p", *preview_output_args(tune, cache),
    ]


def preview_output_args(tune: PreviewTune, cache: Path) -> list[str]:
    """Кодировщик и куски превью — общие у своего процесса превью и у выхода читателя конвейера
    (`video_reader.py`); вход и фильтр масштаба у них свои."""
    return [
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-crf", str(tune.crf), "-g", str(tune.gop), "-bf", "0", "-fps_mode", "passthrough",
        "-f", "segment", "-segment_time", str(PREVIEW_S), "-segment_atclocktime", "1", "-reset_timestamps", "1",
        "-strftime", "1", "-segment_format", "mp4",
        "-segment_format_options", "movflags=+frag_keyframe+empty_moov+default_base_moof:flush_packets=1",
        "-segment_list", str(cache / PREVIEW_LIST), "-segment_list_type", "csv",
        str(cache / PREVIEW_DIR / "%Y%m%dT%H%M%S.mp4"),
    ]


def piece_name(begin: float, length: float) -> str:
    return f"{time.strftime('%H%M%S', time.gmtime(begin))}-{length_ms(length)}.mp4"


def whole_fragments(data: bytes) -> bytes:
    """Начало файла до конца последнего целого фрагмента (`moof` + `mdat`): хвост, который ffmpeg ещё
    дописывает, телефон не получит. Ни одного целого фрагмента — пусто (ISO/IEC 14496-12, 4.2: размер
    поля 1 — 64-битный следом, 0 — «до конца файла», то есть недописан)."""
    at, cut = 0, 0
    while at + 8 <= len(data):
        size, kind = int.from_bytes(data[at : at + 4], "big"), data[at + 4 : at + 8]
        head = 8
        if size == 1 and at + 16 <= len(data):
            size, head = int.from_bytes(data[at + 8 : at + 16], "big"), 16
        if size < head or at + size > len(data):
            break
        at += size
        if kind == b"mdat":
            cut = at
    return data[:cut]


class PreviewIndex:
    """Куски превью камеры: день → отсортированные (начало, длительность). Читается один раз."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._days: dict[str, list[tuple[float, float]]] = {}
        self.loaded = False

    def load(self) -> None:
        days: dict[str, list[tuple[float, float]]] = {}
        for folder in sorted(self.root.iterdir()) if self.root.is_dir() else []:
            if not _DAY.match(folder.name) or not (folder / PREVIEW_DIR).is_dir():
                continue
            stamp = folder.name.replace("-", "")
            for item in (folder / PREVIEW_DIR).iterdir():
                match = _NAME.match(item.name)
                begin = epoch_of(f"{stamp}T{match.group(1)}") if match else None
                if begin is not None:
                    days.setdefault(folder.name, []).append((begin, int(match.group(2)) / 1000))
        self._days = {name: sorted(pieces) for name, pieces in days.items()}
        self.loaded = True

    def add(self, begin: float, length: float) -> None:
        pieces = self._days.setdefault(day_of(begin), [])
        pieces.insert(bisect_right(pieces, (begin, length)), (begin, length))

    def between(self, start: float, stop: float) -> list[tuple[float, float]]:
        # Днём раньше — кусок, начатый до полуночи, кончается в этих сутках.
        first, last = day_of(start - PREVIEW_S), day_of(stop)
        return [
            p for name in sorted(self._days) if first <= name <= last
            for p in self._days[name] if p[0] + p[1] > start and p[0] < stop
        ]

    def path(self, begin: float) -> Path | None:
        for piece in self._days.get(day_of(begin), []):
            if abs(piece[0] - begin) < 0.001:
                return self.root / day_of(begin) / PREVIEW_DIR / piece_name(*piece)
        return None

    def drop_day(self, name: str) -> None:
        self._days.pop(name, None)

    def day_names(self) -> list[str]:
        return sorted(self._days)


class PreviewSink:
    """Куски превью из кэша камеры — в хранилище, в индекс и в объём дня. Без процесса: куски пишет либо
    свой процесс превью (`PreviewWriter`, раздел `recorder`), либо выход читателя конвейера (`video_reader.py`)."""

    def __init__(self, camera: RecordSink, root: Path | None = None) -> None:
        """`root` — где пишет ffmpeg превью (куски в `root/preview/`, список рядом); по умолчанию кэш записи камеры."""
        self.camera = camera
        self.cache = (root or camera.cache) / PREVIEW_DIR
        self.why = ""
        self._offset = 0

    def restart_list(self) -> None:
        """Новый ffmpeg начинает список заново."""
        self._offset = 0

    def current(self) -> tuple[float, Path] | None:
        """Идущий кусок: самый новый файл кэша (его ещё нет в списке ffmpeg)."""
        files = sorted(self.cache.glob("*.mp4")) if self.cache.is_dir() else []
        begin = epoch_of(files[-1].stem) if files else None
        return (begin, files[-1]) if begin is not None else None

    def collect(self, final: bool) -> None:
        """Закрытые куски (по списку ffmpeg) — в `<день>/preview/` хранилища, в индекс и в объём дня.
        `final` — и все оставшиеся в кэше: процесс остановлен, длительность — до последней записи в файл."""
        listed = self.cache.parent / PREVIEW_LIST
        lines: list[str] = []
        try:
            # ⚠ Новый ffmpeg пишет список заново: он короче прочитанного — читать с начала (ревью 2026-10-27: без этого
            # после первого перезапуска читателя превью не попадало в индекс).
            if listed.stat().st_size < self._offset:
                self._offset = 0
            with listed.open("rb") as handle:
                handle.seek(self._offset)
                chunk = handle.read()
            cut = chunk.rfind(b"\n") + 1
            self._offset += cut
            lines = chunk[:cut].decode(errors="ignore").splitlines()
        except OSError:
            pass
        done: list[tuple[Path, float, float]] = []
        for line in lines:
            parts = line.rsplit(",", 2)
            try:
                path, start, stop = self.cache / Path(parts[0]).name, float(parts[1]), float(parts[2])
            except (IndexError, ValueError):
                continue
            # Метки — часы дома (свой процесс превью, `-copyts`), у первого куска процесса начало с нуля —
            # оно в имени файла. ⚠ У читателя конвейера часов стены нет вовсе (вход общий с копией записи):
            # обе метки — время потока, начало — в имени, длина — их разность.
            begin = start if start > 1e9 else epoch_of(path.stem)
            length = stop - start if start > 1e9 or stop <= 1e9 else stop - (begin or 0)
            if begin is not None and length > 0:
                done.append((path, begin, length))
        listed_paths = {path for path, _begin, _length in done}
        for path in sorted(self.cache.glob("*.mp4")) if final and self.cache.is_dir() else []:
            begin, stop = epoch_of(path.stem), path.stat().st_mtime
            if path not in listed_paths and begin is not None and stop > begin:
                done.append((path, begin, stop - begin))
        for path, begin, length in done:
            try:
                self._store(path, begin, length)
            except OSError as err:
                # Кусок ждёт в кэше следующего разбора; исключение снимало бы декодер камеры вместе с детекцией.
                self.why = f"кусок превью {path.name} не сохранился: {err}"
                return
            if self.why.startswith("кусок превью"):
                self.why = ""

    def _store(self, path: Path, begin: float, length: float) -> None:
        camera = self.camera
        if not path.is_file():
            return
        if camera.rec.problem():
            path.unlink(missing_ok=True)
            return
        target = camera.previews.root / day_of(begin) / PREVIEW_DIR / piece_name(begin, length)
        target.parent.mkdir(parents=True, exist_ok=True)
        size = path.stat().st_size
        shutil.move(str(path), str(target))
        camera.previews.add(begin, length)
        # Объём дня — с превью: лимит «локального диска» считает всё, что лежит.
        camera.index.account(begin, size)

    def state(self) -> dict[str, Any]:
        return {"why": self.why}


class PreviewWriter(PreviewSink):
    """Свой процесс превью камеры раздела `recorder`: ffmpeg пишет куски в кэш камеры."""

    def __init__(self, camera: RecordSink) -> None:
        super().__init__(camera)
        self._reported = ""
        self._started = 0.0
        self._tail: deque[str] = deque(maxlen=6)

    async def run(self) -> None:
        env = self.camera.rec.env
        await env.run(lambda: self.cache.mkdir(parents=True, exist_ok=True))
        while True:
            try:
                if not self.camera.previews.loaded:
                    await env.run(self.camera.previews.load)
                # Оставшееся от прошлого процесса (остановка HA без уборки) — в хранилище до нового списка.
                await env.run(self.collect, True)
                # ⚠ Свой поток заводим ДО ffmpeg: иначе превью стартует раньше записи и получает
                # от go2rtc 404 «stream not found» (объект 2026-10-26, сразу после перезапуска HA).
                # PUT идемпотентен, а после перезапуска go2rtc он же возвращает поток на место.
                await self.camera.register()
                await self._write()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — превью не роняет запись
                self.why = f"превью не пишется: {err}"
                if self.why != self._reported:
                    self._reported = self.why
                    LOGGER.warning("Превью %s: %s", self.camera.spec.id, err)
            try:
                await env.run(self.collect, True)
            except Exception as err:  # noqa: BLE001
                self.why = f"кусок превью не сохранился: {err}"
            await asyncio.sleep(RESTART_S)

    async def _write(self) -> None:
        env = self.camera.rec.env
        binary, nice = await env.run(lambda: (shutil.which("ffmpeg"), shutil.which("nice")))
        if not binary:
            raise RuntimeError("в системе нет ffmpeg")
        self._offset = 0
        proc = await asyncio.create_subprocess_exec(
            *command(binary, nice or "", self.camera.spec.stream, self.camera.cache, self.camera.rec.preview),
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE, env={**os.environ, "TZ": "UTC"},
        )
        key = f"preview:{self.camera.spec.id}"
        host_load.track(key, proc.pid)
        self._started = time.time()
        self._tail.clear()
        drain = asyncio.ensure_future(self._drain(proc))
        try:
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(proc.wait()), SCAN_S)
                    break
                except asyncio.TimeoutError:
                    await env.run(self.collect, False)
                if await env.run(self._stalled):
                    raise RuntimeError(f"кусок превью не растёт дольше {STALL_S:g} с — поток встал")
                self.why = self._reported = ""
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), 5)
                except asyncio.TimeoutError:
                    proc.kill()
            drain.cancel()
            host_load.untrack(key, proc.pid)
        raise RuntimeError(f"ffmpeg превью завершился ({proc.returncode}): {' | '.join(self._tail) or 'без слов'}")

    async def _drain(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for line in proc.stderr:
            if text := line.decode(errors="ignore").strip():
                self._tail.append(text)

    def _stalled(self) -> bool:
        """Идущий кусок не растёт — или не появился вовсе — дольше `STALL_S`."""
        current = self.current()
        last = current[1].stat().st_mtime if current is not None else self._started
        return time.time() - max(last, self._started) > STALL_S


def sweep_legacy(root: Path) -> None:
    """Кадры к кускам и спрайты окон прежних версий дома (0.5.26–0.5.31) — с диска, один раз на камеру.
    ⚠ В исполнителе и не спеша: на сетевой папке это тысячи удалений на сутки записи."""
    if not root.is_dir() or (root / SWEPT).exists():
        return
    for folder in sorted(root.iterdir()):
        if not _DAY.match(folder.name) or not folder.is_dir():
            continue
        shutil.rmtree(folder / "sprites", ignore_errors=True)
        for item in os.scandir(folder):
            if item.name.endswith(".jpg"):
                try:
                    os.unlink(item.path)
                except OSError:
                    continue
    (root / SWEPT).touch()
