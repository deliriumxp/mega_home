"""Кадр превью к куску записи — слот спрайта шкалы записи (спека менеджера «Архив камеры: ленточная
шкала и спрайт превью» от 2026-10-23, раздел «Дом: кадр к куску и спрайт»).

Зачем. Превью под пальцем на шкале архива — кадр на каждые 10 с записи, собранный в спрайт окна.
Снимать его по каждой остановке пальца — декод основного потока на контроллере, и картинка
приходит, когда палец уже уехал. Поэтому кадр снимается заранее, один раз — когда кусок сохранён.

⚠ Из сохранённого файла, а не вторым ffmpeg на поток: у потока нет второго потребителя и его
сторожа, кадр есть ровно у сохранённых кусков (по движению — без лишней работы), работа — один
декод ключевого кадра на кусок (`-skip_frame nokey`), и та под `nice`, чтобы не отнимать процессор
у записи. Кадр не снялся — не ошибка записи: кусок сохранён, слот спрайта пуст.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
from pathlib import Path

# Ширина кадра — ширина слота спрайта (160 × 90 у 16:9).
FRAME_WIDTH = 160
FRAME_TIMEOUT_S = 10


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
