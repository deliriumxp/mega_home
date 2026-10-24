"""Индекс записи камеры — в памяти, у процесса записи; уборка архива по нему же.

⚠ Раньше индексом был сам каталог: каждый запрос службы (`/days`, `/segments`, `/playlist`,
`/frame`) перечитывал каталоги дней целиком — у постоянной записи 8 640 кусков в сутки, и
открытие архива на контроллере с USB-диском или сетевой папкой стоило секунд, а `/days` обходил
весь архив на каждое открытие. Теперь каталог читается ОДИН раз при старте записи камеры, дальше
индекс ведёт тот, кто пишет и убирает: новый кусок — `add`, уборка — `forget`/`drop_day`. Так
устроены регистраторы и Frigate (таблица записей), а не обход диска на каждый взгляд.

Память: на день — два массива чисел (начало, длительность), путь куска выводится из них; месяц
постоянной записи четырёх камер — ~1 млн кусков, 12 МБ.
"""

from __future__ import annotations

import re
import shutil
import threading
import time
from array import array
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SEGMENT_NAME = re.compile(r"^(\d{6})-(\d+)\.mkv$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Кусок длиннее этого не бывает (срок куска плюс группа кадров): докуда искать назад от начала окна.
MAX_SEGMENT_S = 60.0
# Меньше свободного — удаляются самые старые куски всех камер, не дожидаясь срока. У системного
# диска с лимитом планка ниже: на маленьком флеше 2 ГБ свободного недостижимы, и уборка стирала бы
# архив каждую минуту — объём там держит лимит.
MIN_FREE_BYTES = 2 * 1024**3
LIMITED_MIN_FREE_BYTES = 256 * 1024**2

# Кусок записи: начало (UTC, секунды), длительность (секунды), файл.
Segment = tuple[float, float, Path]


def epoch_of(stamp: str) -> float | None:
    """`YYYYMMDDTHHMMSS` (UTC, имя куска ffmpeg) → секунды."""
    try:
        return datetime(
            int(stamp[0:4]), int(stamp[4:6]), int(stamp[6:8]),
            int(stamp[9:11]), int(stamp[11:13]), int(stamp[13:15]), tzinfo=timezone.utc,
        ).timestamp()
    except (ValueError, IndexError):
        return None


def day_of(epoch: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(epoch))


def segment_name(begin: float, length_ms: int) -> str:
    return f"{time.strftime('%H%M%S', time.gmtime(begin))}-{length_ms}.mkv"


def _frame_size(segment: Path) -> int:
    """Кадр превью куска (`recorder_frames.frame_of`) — тот же стержень, `.jpg`; нет — 0."""
    try:
        return segment.with_suffix(".jpg").stat().st_size
    except OSError:
        return 0


def scan_day(folder: Path) -> list[Segment]:
    """Куски каталога дня по именам — только при загрузке индекса."""
    out: list[Segment] = []
    for item in folder.iterdir():
        match = SEGMENT_NAME.match(item.name)
        begin = epoch_of(f"{folder.name.replace('-', '')}T{match.group(1)}") if match else None
        if begin is not None:
            out.append((begin, int(match.group(2)) / 1000, item))
    return sorted(out)


class _Day:
    __slots__ = ("begins", "lengths", "size")

    def __init__(self) -> None:
        self.begins: array[float] = array("d")
        self.lengths: array[int] = array("I")
        self.size = 0


class SegmentIndex:
    """Куски одной камеры: день → отсортированные начала и длительности (мс)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._days: dict[str, _Day] = {}
        self._lock = threading.Lock()
        # ⚠ Обход каталога — один за раз, и `add` его ждёт: загрузку зовут запись камеры, служба (запрос
        # до первого прохода записи) и уборка, каждая в своём потоке исполнителя. Вторая загрузка
        # присваиванием затирала куски, добавленные записью после ЕЁ обхода, — до перезапуска HA их не
        # было в архиве. Кусок, добавленный ДО обхода, обход видит сам: в индекс он идёт после переноса.
        self._loading = threading.Lock()
        self.loaded = False

    def load(self, sizes: bool = False) -> None:
        """Прочитать каталог камеры целиком — один раз при старте записи (в исполнителе).
        `sizes` — объём дней нужен только уборке по лимиту: статистика каждого куска на сетевой папке
        стоит времени, и без лимита её не считаем."""
        with self._loading:
            self._load(sizes)

    def _load(self, sizes: bool) -> None:
        days: dict[str, _Day] = {}
        for folder in sorted(self.root.iterdir()) if self.root.is_dir() else []:
            if not folder.is_dir() or not _DAY.match(folder.name):
                continue
            day = _Day()
            for begin, length, path in scan_day(folder):
                day.begins.append(begin)
                day.lengths.append(int(length * 1000))
                if sizes:
                    day.size += path.stat().st_size + _frame_size(path)
            if sizes and len(day.begins):
                # Спрайты окон и заглушка (`recorder_frames`) — тоже объём дня: лимит считает всё, что лежит.
                day.size += sum(item.stat().st_size for item in (folder / "sprites").glob("*.jpg"))
            if len(day.begins):
                days[folder.name] = day
        with self._lock:
            self._days, self.loaded = days, True

    def ensure(self, sizes: bool = False) -> None:
        if self.loaded:
            return
        with self._loading:
            if not self.loaded:
                self._load(sizes)

    def _path(self, day: str, begin: float, length_ms: int) -> Path:
        return self.root / day / segment_name(begin, length_ms)

    def add(self, begin: float, length: float, size: int = 0) -> Path:
        """Новый кусок; тот же момент повторно — замена (перезапуск в ту же секунду)."""
        name, length_ms = day_of(begin), int(length * 1000)
        with self._loading, self._lock:
            day = self._days.setdefault(name, _Day())
            at = bisect_left(day.begins, begin)
            if at < len(day.begins) and day.begins[at] == begin:
                day.lengths[at] = length_ms
            else:
                day.begins.insert(at, begin)
                day.lengths.insert(at, length_ms)
            day.size += size
        return self._path(name, begin, length_ms)

    def account(self, begin: float, size: int) -> None:
        """Кадр, снятый к куску позже сохранения (`recorder_frames.backfill`), спрайт окна (снятый — с
        минусом) — в объём его дня."""
        with self._lock:
            day = self._days.get(day_of(begin))
            if day is not None:
                day.size = max(day.size + size, 0)

    def days(self) -> list[tuple[str, float, float]]:
        """Дни с записью: (имя дня UTC, первое начало, последнее начало)."""
        with self._lock:
            return [(name, day.begins[0], day.begins[-1]) for name, day in sorted(self._days.items())]

    def between(self, start: float, stop: float) -> list[Segment]:
        """Куски, пересекающие `[start, stop]`, по порядку."""
        out: list[Segment] = []
        first, last = day_of(start - MAX_SEGMENT_S), day_of(stop)
        with self._lock:
            for name in sorted(self._days):
                if name < first or name > last:
                    continue
                day = self._days[name]
                lo, hi = bisect_left(day.begins, start - MAX_SEGMENT_S), bisect_right(day.begins, stop)
                for index in range(lo, hi):
                    begin, length_ms = day.begins[index], day.lengths[index]
                    if begin + length_ms / 1000 >= start:
                        out.append((begin, length_ms / 1000, self._path(name, begin, length_ms)))
        return out

    def first_after(self, at: float, until: float, started: bool = False) -> Segment | None:
        """Первый кусок, который идёт после `at` (кончается позже; `started` — начинается позже) и
        начинается раньше `until`; нет — `None`. Без сборки списка: окно бывает сутками кусков."""
        first, last = day_of(at - MAX_SEGMENT_S), day_of(until)
        with self._lock:
            for name in sorted(self._days):
                if name < first or name > last:
                    continue
                day = self._days[name]
                for index in range(bisect_right(day.begins, at) if started else bisect_left(day.begins, at - MAX_SEGMENT_S), len(day.begins)):
                    begin, length_ms = day.begins[index], day.lengths[index]
                    if begin >= until:
                        return None
                    if started or begin + length_ms / 1000 > at:
                        return (begin, length_ms / 1000, self._path(name, begin, length_ms))
        return None

    def last_before(self, at: float) -> Segment | None:
        """Последний кусок, начавшийся раньше `at`; нет — `None`."""
        last = day_of(at)
        with self._lock:
            for name in sorted((n for n in self._days if n <= last), reverse=True):
                day = self._days[name]
                index = bisect_left(day.begins, at) - 1
                if index >= 0:
                    return (day.begins[index], day.lengths[index] / 1000, self._path(name, day.begins[index], day.lengths[index]))
        return None

    def oldest(self, count: int) -> list[Segment]:
        """Самые старые куски (из самого старого дня) — уборка по месту и лимиту."""
        with self._lock:
            for name in sorted(self._days):
                day = self._days[name]
                return [
                    (day.begins[i], day.lengths[i] / 1000, self._path(name, day.begins[i], day.lengths[i]))
                    for i in range(min(count, len(day.begins)))
                ]
        return []

    def forget(self, begin: float, size: int = 0) -> str | None:
        """Кусок удалён; вернуть имя дня, если он опустел (каталог убирает вызывающий)."""
        name = day_of(begin)
        with self._lock:
            day = self._days.get(name)
            if day is None:
                return None
            at = bisect_left(day.begins, begin)
            if at < len(day.begins) and day.begins[at] == begin:
                del day.begins[at]
                del day.lengths[at]
                day.size = max(day.size - size, 0)
            if len(day.begins):
                return None
            del self._days[name]
            return name

    def drop_day(self, name: str) -> None:
        with self._lock:
            self._days.pop(name, None)

    def day_names(self) -> list[str]:
        with self._lock:
            return sorted(self._days)

    def size(self) -> int:
        with self._lock:
            return sum(day.size for day in self._days.values())


def clean(storage: Path, cameras: list[Any], now: float, limit: int = 0) -> None:
    """По сроку камеры — каталоги дней целиком (с кадрами и `sprites/`); по месту и по лимиту объёма (`limit`, байты:
    «локальный диск») — самые старые куски всех камер. `cameras` — `_Camera` записи (`spec`, `index`)."""
    for camera in cameras:
        camera.index.ensure(bool(limit))
        cutoff = day_of(now - camera.spec.retain_days * 24 * 3600)
        # ⚠ И каталоги с диска, а не только дни индекса: в сутках без движения лежат одни кадры превью
        # (`_Camera._preview_only`) — индекс таких суток не знает, и они жили бы вечно.
        on_disk = [p.name for p in camera.index.root.iterdir() if p.is_dir() and _DAY.match(p.name)] if camera.index.root.is_dir() else []
        for name in sorted(set(camera.index.day_names()) | set(on_disk)):
            if name < cutoff:
                shutil.rmtree(camera.index.root / name, ignore_errors=True)
                camera.index.drop_day(name)
    # Шагами по 60 кусков (10 минут записи), не больше полусотни шагов за проход.
    for _step in range(50):
        floor = LIMITED_MIN_FREE_BYTES if limit else MIN_FREE_BYTES
        used = sum(camera.index.size() for camera in cameras) if limit else 0
        if shutil.disk_usage(storage).free >= floor and (not limit or used <= limit):
            break
        oldest = min(
            (((camera.index.day_names() or ["~"])[0], camera) for camera in cameras), key=lambda item: item[0], default=None
        )
        if oldest is None or oldest[0] == "~":
            break
        camera = oldest[1]
        for begin, _length, path in camera.index.oldest(60):
            # Кусок уходит вместе с кадром превью: в индексе их объём один.
            size = (path.stat().st_size if path.is_file() else 0) + _frame_size(path)
            path.unlink(missing_ok=True)
            path.with_suffix(".jpg").unlink(missing_ok=True)
            emptied = camera.index.forget(begin, size)
            if emptied is not None:
                shutil.rmtree(camera.index.root / emptied, ignore_errors=True)
    plays = storage / ".cache" / "play"
    for item in plays.iterdir() if plays.is_dir() else []:
        if now - item.stat().st_mtime > 6 * 3600:
            item.unlink(missing_ok=True)
