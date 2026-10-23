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
пропавший кадр не сдвигает соседей. Окно в прошлом не меняется — собирается один раз и лежит в
`<день>/sprites/`; текущее — на лету, без кэша: в него ещё дописываются куски.
"""

from __future__ import annotations

import calendar
import contextlib
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from uuid import uuid4

from .recorder_index import SegmentIndex, day_of

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
    пусто. Кусок без кадра в счёт не идёт — слот возьмёт соседний кусок или останется пустым."""
    found = [(b, n, p.with_suffix(".jpg")) for b, n, p in index.between(start, start + WINDOW_S)]
    have = present([p for _b, _n, p in found])
    found = [(b, n, p) for b, n, p in found if p in have]
    slots: list[Path | None] = []
    for moment in (start + i * SLOT_S for i in range(COLS * ROWS)):
        covering = next((p for b, n, p in found if b <= moment < b + n), None)
        slots.append(covering or next((p for b, _n, p in found if moment <= b < moment + SLOT_S), None))
    return slots


def sprite_path(root: Path, start: float) -> Path:
    return root / day_of(start) / "sprites" / f"{time.strftime('%H%M', time.gmtime(start))}.jpg"


def _ffmpeg(command: list[str]) -> bytes:
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        return subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, timeout=SPRITE_TIMEOUT_S, check=True).stdout
    return b""


def compose(frames: list[Path | None], binary: str, work: Path) -> bytes:
    """Сетка JPEG из кадров слотов; пустые слоты — чёрная заглушка `work/blank.jpg` (делается один
    раз на каталог). Ни одного кадра или нет ffmpeg — `b""` без запуска. Блокирует — из исполнителя."""
    if not binary or not any(frames):
        return b""
    work.mkdir(parents=True, exist_ok=True)
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


def _sprite(index: SegmentIndex, start: float, closed: bool) -> bytes:
    target = sprite_path(index.root, start)
    if closed:
        with contextlib.suppress(OSError):
            return target.read_bytes()
    image = compose(slot_frames(index, start), shutil.which("ffmpeg") or "", target.parent)
    if closed and image:
        part = target.with_name(f"{target.name}.{uuid4().hex}.part")
        with contextlib.suppress(OSError):
            part.write_bytes(image)
            os.replace(part, target)
        with contextlib.suppress(OSError):
            part.unlink(missing_ok=True)
    return image


async def sprite_for(recorder: Recorder, camera: str, at: float) -> tuple[bytes, bool]:
    """Спрайт окна под `at` (секунды UTC) и закрыто ли оно; пусто — кадров в окне нет."""
    start = window_start(at)
    closed = start + WINDOW_S + CLOSED_AFTER_S <= time.time()
    return await recorder.env.run(_sprite, recorder.cameras[camera].index, start, closed), closed
