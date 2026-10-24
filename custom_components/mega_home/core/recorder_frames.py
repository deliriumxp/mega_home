"""Кадр превью к куску записи — слот спрайта шкалы записи (спека менеджера «Архив камеры: ленточная
шкала и спрайт превью» от 2026-10-23, раздел «Дом: кадр к куску и спрайт»).

Зачем. Превью под пальцем на шкале архива — кадр на каждые 10 с записи, собранный в спрайт окна.
Снимать его по каждой остановке пальца — декод основного потока на контроллере, и картинка
приходит, когда палец уже уехал. Поэтому кадр снимается заранее, один раз — когда кусок сохранён.

⚠ Из сохранённого файла, а не вторым ffmpeg на поток: у потока нет второго потребителя и его
сторожа, кадр есть ровно у сохранённых кусков (по движению — без лишней работы), работа — один
декод ключевого кадра на кусок (`-skip_frame nokey`), и та под `nice`, чтобы не отнимать процессор
у записи. Кадр не снялся — не ошибка записи: кусок сохранён, слот спрайта пуст.

Спрайт окна — 10 минут записи сеткой 10 × 6 по 10 с, собирается из этих кадров по запросу службы
(`/sprite`). Слоты — по ВРЕМЕНИ, а не по порядку кусков: телефон считает слот сам, без индекса, и
пропавший кадр не сдвигает соседей. Окно в прошлом лежит в `<день>/sprites/` под именем по своим
кадрам (кадр, снятый позже, — новая сборка); текущее — на лету, без кэша: в него ещё дописываются куски.
"""

from __future__ import annotations

import calendar
import contextlib
import os
import re
import shutil
import subprocess
import time
import zlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from uuid import uuid4

from .recorder_index import MAX_SEGMENT_S, SegmentIndex, day_of, epoch_of

if TYPE_CHECKING:
    from .recorder import Recorder

# Ширина кадра — ширина слота спрайта (160 × 90 у 16:9).
FRAME_WIDTH = 160
FRAME_TIMEOUT_S = 10
# Досъёмка кадров к кускам без них (`backfill`): пауза между кадрами и между проходами, когда кадры
# у всех. По одному с паузой — запись и показ идут на том же контроллере одновременно.
BACKFILL_PAUSE_S = 0.5
BACKFILL_IDLE_S = 600.0
DAY_S = 86_400
WINDOW_S, SLOT_S, COLS, ROWS, SLOT_W, SLOT_H = 600, 10, 10, 6, 160, 90
# Окно закрыто, когда его конец старше этого. Кадр последнего куска окна появляется позже конца
# окна на `pre_s` (10 с по умолчанию: кусок ждёт решения о движении) + `SEGMENT_S` (длина куска)
# + `FRAME_TIMEOUT_S` (снятие кадра под `nice`); остальное — запас на очередь исполнителя и проход
# по готовым кускам. Короче — спрайт без последних слотов лёг бы на диск насовсем.
# ⚠ Бандл держит то же число (`SPRITE_CLOSED_AFTER_MS`, `clip-sprite.ts` менеджера).
CLOSED_AFTER_S = 120
SPRITE_TIMEOUT_S = 15
# Кадр вписывается в слот с чёрными полями (16:9 — без полей), 60 кадров — одна сетка.
_TILE = f"scale={SLOT_W}:{SLOT_H}:force_original_aspect_ratio=decrease,pad={SLOT_W}:{SLOT_H}:(ow-iw)/2:(oh-ih)/2,tile={COLS}x{ROWS}"


def tools() -> tuple[str, str]:
    """ffmpeg и nice системы — один раз на старт камер (в исполнителе); нет ffmpeg — кадров нет.
    ⚠ Нет `nice` — кадр снимается без него, а не пропадает (как у `motion.py`)."""
    return shutil.which("ffmpeg") or "", shutil.which("nice") or ""


def frame_of(path: Path, out: Path, binary: str, nice: str = "") -> int:
    """Первый ключевой кадр куска `path` уменьшенным JPEG в `out`; размер файла или 0.
    Блокирует до `FRAME_TIMEOUT_S` — зовётся из исполнителя (`_Camera._save`)."""
    # ⚠ Пишется во временное имя и переименовывается целиком: спрайт не возьмёт недописанный кадр,
    # а сорвавшийся снимок не оставит файла, которого объём не учёл.
    part = out.with_name(out.name + ".part")
    command = [*([nice, "-n", "10"] if nice else []), binary, "-hide_banner", "-loglevel", "error", "-nostdin"]
    command += ["-skip_frame", "nokey", "-i", str(path), "-frames:v", "1", "-vf", f"scale={FRAME_WIDTH}:-2"]
    # Формат — явно: по имени `.part` ffmpeg его не выведет.
    command += ["-q:v", "6", "-f", "image2", "-c:v", "mjpeg", "-y", str(part)]
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=FRAME_TIMEOUT_S, check=True)
        if size := part.stat().st_size:
            os.replace(part, out)
            return size
    with contextlib.suppress(OSError):
        part.unlink(missing_ok=True)
    return 0


# Кадр превью в каталоге дня: `HHMMSS-<мс>.jpg` — у сохранённого куска (рядом `.mkv`) и у куска без
# движения, который не хранится (`_Camera._preview_only`).
FRAME_NAME = re.compile(r"^(\d{6})-(\d+)\.jpg$")
# Листинг каталога дня для спрайта живёт столько: на сетевой папке листинг — поход по сети, а окна
# спрайта при протяжке идут подряд по одному каталогу.
LISTING_TTL_S = 20.0
_LISTED: dict[Path, tuple[float, frozenset[str]]] = {}


def listing(folder: Path) -> frozenset[str]:
    """Имена в каталоге — из памяти, если листинг моложе `LISTING_TTL_S`; нет каталога — пусто.
    ⚠ Устаревшие листинги уходят на каждом вызове: у постоянной записи это ~17 тыс. имён на сутки
    камеры, и протяжка по неделе архива держала бы их в памяти HA до перезапуска."""
    now = time.monotonic()
    for stale, (at, _names) in list(_LISTED.items()):
        if now - at >= LISTING_TTL_S:
            _LISTED.pop(stale, None)
    hit = _LISTED.get(folder)
    if hit is not None and now - hit[0] < LISTING_TTL_S:
        return hit[1]
    try:
        names = frozenset(entry.name for entry in os.scandir(folder))
    except OSError:
        names = frozenset()
    _LISTED[folder] = (now, names)
    return names


def frames_in(root: Path, start: float, stop: float) -> list[tuple[float, float, Path]]:
    """Кадры превью, чьи куски пересекают `[start, stop)`: (начало, длительность, путь) по порядку.

    ⚠ По именам в каталогах дней, а не по индексу кусков: у куска без движения кадр есть, а куска нет.
    """
    out: list[tuple[float, float, Path]] = []
    for name in sorted({day_of(start - MAX_SEGMENT_S), day_of(stop)}):
        folder, stamp = root / name, name.replace("-", "")
        for entry in listing(folder):
            match = FRAME_NAME.match(entry)
            begin = epoch_of(f"{stamp}T{match.group(1)}") if match else None
            if begin is None:
                continue
            length = int(match.group(2)) / 1000
            if begin + length > start and begin < stop:
                out.append((begin, length, folder / entry))
    return sorted(out)


def present(paths: list[Path]) -> set[Path]:
    """Какие из `paths` есть на диске — по ОДНОМУ листингу на каталог, а не `stat` на каждый путь.

    ⚠ На сетевой папке `stat` — поход по сети: 60 кусков окна спрайта стоили 20 секунд, а листинг
    каталога дня — один заход (замер на живом доме 2026-10-24).
    """
    listed: dict[Path, set[str]] = {}
    out: set[Path] = set()
    for path in paths:
        if path.parent not in listed:
            try:
                listed[path.parent] = {entry.name for entry in os.scandir(path.parent)}
            except OSError:
                listed[path.parent] = set()
        if path.name in listed[path.parent]:
            out.add(path)
    return out


def missing_frames(indexes: list[SegmentIndex], skip: set[Path]) -> list[tuple[SegmentIndex, float, Path]]:
    """Куски без кадра у каждой камеры — из самого свежего её дня, где такие есть, свежие сначала;
    пусто — кадры у всех. День за днём, а не весь архив разом: статистика каждого куска на сетевой папке
    стоит времени, и день — ровно то, что обходится за один заход. `skip` — куски, у которых кадр не
    снялся: второй раз в этом запуске не пробуем."""
    out: list[tuple[SegmentIndex, float, Path]] = []
    for index in indexes:
        for name, _first, _last in reversed(index.days()):
            start = float(calendar.timegm(time.strptime(name, "%Y-%m-%d")))
            pieces = [(begin, path) for begin, _length, path in reversed(index.between(start, start + DAY_S)) if day_of(begin) == name]
            have = present([path.with_suffix(".jpg") for _begin, path in pieces])
            found = [(index, begin, path) for begin, path in pieces if path not in skip and path.with_suffix(".jpg") not in have]
            if found:
                out.extend(found)
                break
    return out


async def backfill(
    indexes: Callable[[], list[SegmentIndex]],
    tools: Callable[[], tuple[str, str]],
    run: Callable[..., Awaitable[Any]],
    sleep: Callable[[float], Awaitable[None]],
) -> None:
    """Кадры к кускам, записанным без них (запись до 0.5.26, сорвавшиеся снимки) — по одному, с паузой,
    свежие сначала: спрайт нужен прежде всего у последних суток. Без ffmpeg — ждём: его ищут на старте
    камер. Кадр в объём дня — как у снятого при сохранении (`SegmentIndex.account`)."""
    skip: set[Path] = set()
    while True:
        binary, nice = tools()
        pending = await run(missing_frames, indexes(), skip) if binary else []
        if not pending:
            await sleep(BACKFILL_IDLE_S)
            continue
        for index, begin, path in pending:
            size = await run(frame_of, path, path.with_suffix(".jpg"), binary, nice)
            if size:
                index.account(begin, size)
            else:
                skip.add(path)
            await sleep(BACKFILL_PAUSE_S)


def window_start(at: float) -> float:
    return at // WINDOW_S * WINDOW_S


def slot_frames(index: SegmentIndex, start: float) -> list[Path | None]:
    """Кадр каждого слота: кусок, покрывающий `start + i·SLOT_S`, иначе начавшийся в слоте, иначе
    пусто. Кусок без кадра в счёт не идёт — слот возьмёт соседний кусок или останется пустым; кадр куска
    без движения (сам кусок не хранится) — в счёт идёт."""
    found = frames_in(index.root, start, start + WINDOW_S)
    slots: list[Path | None] = []
    for moment in (start + i * SLOT_S for i in range(COLS * ROWS)):
        covering = next((p for b, n, p in found if b <= moment < b + n), None)
        slots.append(covering or next((p for b, _n, p in found if moment <= b < moment + SLOT_S), None))
    return slots


def sprite_path(root: Path, start: float, frames: list[Path | None]) -> Path:
    """Файл закрытого окна. ⚠ Имя несёт отпечаток кадров слотов: кадр, снятый ПОСЛЕ сборки (досъёмка
    записанного до 0.5.26 идёт часами, кусок по движению сохраняется с запасом), даёт другое имя, и
    спрайт с чёрными слотами не остаётся на диске навсегда — «фича покрывает уже записанное»."""
    mark = zlib.crc32("\n".join(frame.name if frame else "" for frame in frames).encode())
    return root / day_of(start) / "sprites" / f"{time.strftime('%H%M', time.gmtime(start))}-{mark:08x}.jpg"


def complete(index: SegmentIndex, start: float) -> bool:
    """У каждого куска записи в окне есть кадр — спрайт окна больше не изменится (листинг из памяти)."""
    return all(
        path.with_suffix(".jpg").name in listing(path.parent)
        for begin, length, path in index.between(start, start + WINDOW_S)
        if begin + length > start and begin < start + WINDOW_S
    )


def _ffmpeg(command: list[str]) -> bytes:
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        return subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, timeout=SPRITE_TIMEOUT_S, check=True).stdout
    return b""


def compose(frames: list[Path | None], binary: str, work: Path) -> bytes:
    """Сетка JPEG из кадров слотов; пустые слоты — чёрная заглушка `work/blank.jpg` (делается один
    раз на каталог). Ни одного кадра или нет ffmpeg — `b""` без запуска. Блокирует — из исполнителя."""
    if not binary or not any(frames):
        return b""
    try:
        # ⚠ Без `parents`: каталог дня — там, где лежат кадры. Его нет — диск пропал (листинг кадров
        # из памяти), и создать путь значило бы писать на флеш контроллера под точкой монтирования.
        work.mkdir(exist_ok=True)
    except OSError:
        return b""
    head = [binary, "-hide_banner", "-loglevel", "error", "-nostdin"]
    blank = work / "blank.jpg"
    if not blank.is_file():
        # ⚠ Заглушка — чёрный кадр РАЗМЕРА кадров камеры, а не готовый слот 160 × 90: на смене размера
        # входа ffmpeg пересобирает фильтры, `tile` теряет набранные слоты, и сетка выходит чёрной
        # (замер 2026-10-23, ffmpeg 7.1). Один размер у всех входов — вписывание с полями верно и у 4:3.
        first = next(frame for frame in frames if frame)
        image = _ffmpeg([*head, "-i", str(first), "-vf", "lutyuv=y=0:u=128:v=128", "-frames:v", "1", "-f", "image2", "-c:v", "mjpeg", "pipe:1"])
        # Через временное имя: два запроса разом не отдадут ffmpeg недописанную заглушку.
        part = work / f"blank-{uuid4().hex}.part"
        with contextlib.suppress(OSError):
            part.write_bytes(image)
            if image:
                os.replace(part, blank)
            part.unlink(missing_ok=True)
    listing = work / f"list-{uuid4().hex}.txt"
    try:
        # ⚠ Путь в кавычках списка concat: `'` внутри экранируется как `'\''` (правило ffmpeg).
        listing.write_text("".join(f"file '{_quoted(frame or blank)}'\n" for frame in frames), encoding="utf-8")
        tile = ["-vf", _TILE, "-frames:v", "1", "-q:v", "5", "-f", "image2", "-c:v", "mjpeg", "pipe:1"]
        # Камера сменила размер посреди окна — фильтры не пересобираются: кадр другого размера
        # растянется в слот, но соседние слоты не пропадут.
        return _ffmpeg([*head, "-reinit_filter", "0", "-f", "concat", "-safe", "0", "-i", str(listing), *tile])
    finally:
        listing.unlink(missing_ok=True)


def _quoted(path: Path) -> str:
    return str(path).replace("'", "'\\''")


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _store(target: Path, image: bytes) -> int:
    """Спрайт закрытого окна — на диск вместо прежних версий того же окна (и имени до отпечатка);
    изменение объёма дня, байты."""
    part, stem = target.with_name(f"{target.name}.{uuid4().hex}.part"), target.name.split("-", 1)[0]
    try:
        part.write_bytes(image)
        os.replace(part, target)
    except OSError:
        with contextlib.suppress(OSError):
            part.unlink(missing_ok=True)
        return 0
    grown = len(image)
    for old in target.parent.glob(f"{stem}*.jpg"):
        if old != target and (old.name == f"{stem}.jpg" or old.name.startswith(f"{stem}-")):
            with contextlib.suppress(OSError):
                size = _size(old)
                old.unlink()
                grown -= size
    return grown


def _sprite(index: SegmentIndex, start: float, closed: bool) -> tuple[bytes, bool]:
    """Спрайт окна и «не изменится»: окно закрыто и у каждого его куска есть кадр. Неполное закрытое
    окно тоже ложится на диск (имя — по кадрам), но браузеру уходит без долгого кэша."""
    frames = slot_frames(index, start)
    if not any(frames):
        return b"", False
    lasting = closed and complete(index, start)
    target = sprite_path(index.root, start, frames)
    if closed:
        with contextlib.suppress(OSError):
            return target.read_bytes(), lasting
    blank = target.parent / "blank.jpg"
    had_blank = blank.is_file()
    image = compose(frames, shutil.which("ffmpeg") or "", target.parent)
    # Спрайты и заглушка — в объёме дня: лимит «локального диска» считает всё, что лежит.
    grown = (_store(target, image) if closed and image else 0) + (0 if had_blank else _size(blank))
    if grown:
        index.account(start, grown)
    return image, lasting


async def sprite_for(recorder: Recorder, camera: str, at: float) -> tuple[bytes, bool]:
    """Спрайт окна под `at` (секунды UTC) и можно ли его держать в кэше (`_sprite`); пусто — кадров в
    окне нет."""
    start = window_start(at)
    closed = start + WINDOW_S + CLOSED_AFTER_S <= time.time()
    return await recorder.env.run(_sprite, recorder.cameras[camera].index, start, closed)
