"""Обзор суток превью: кадр раз в 15 минут одним файлом на сутки — превью перемотки крупных масштабов (дом 0.6.3+).

Решение заказчика 2026-10-27: превью два, под крайние масштабы ленты, средние берут соседнее (`clip-preview-video.ts`
бандла): 10 мин и час — 10-минутные куски (`recorder_preview.py`), 6 ч и сутки — этот обзор. Причина — замер на
объекте: на сутках и 6 ч шаг ленты (15 и 5 минут) почти каждый раз попадал в новый кусок — 0,6–0,9 МБ ради одного
кадра, протяжка экрана суток — 144 куска, и превью грузилось без конца. Обзор суток — ~0,6 МБ на все сутки.

Как устроено: кадр сетки `начало суток UTC + k·STEP_S` берётся из закрытого куска превью, который его накрывает, в
`<день>/preview/overview/<k>.jpg`; из них — `<день>/preview/overview.mp4`, кадр k на секунде `k − k₀` видео
(переменная частота: у пустоты без куска кадра нет). Кадр — ключевой каждый: телефон листает файл временем `<video>`.
⚠ Дособирает сторож записи (раз в минуту, `Recorder._check`), не жест: на движение пальца дом не делает ничего
(антицель спеки ленты). Не больше `PASS_FRAMES` кадров за проход — так же фоном догоняется и уже записанное превью.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import host_load
from .const import LOGGER
from .recorder_index import day_of, epoch_of

STEP_S = 900
FRAMES = 86400 // STEP_S
PASS_FRAMES = 24
OVERVIEW_DIR = "overview"
OVERVIEW_FILE = "overview.mp4"
# Кусок превью начинается с секундами опоздания (`-segment_atclocktime`): метка сетки на стыке — в начале следующего.
EDGE_S = 2.0
LIST_DIR = Path(tempfile.gettempdir()) / "mega_home_overview"


@dataclass
class _Day:
    done: set[int] = field(default_factory=set)
    # Кадры, которые не извлеклись (битый кусок): не повторять каждый проход.
    failed: set[int] = field(default_factory=set)
    built: tuple[int, int] | None = None


def day_start(day: str) -> float:
    return epoch_of(f"{day.replace('-', '')}T000000") or 0.0


class Overviews:
    def __init__(self, env: Any) -> None:
        self.env = env
        self._days: dict[tuple[str, str], _Day] = {}

    def listed(self, sink: Any, start: float, stop: float) -> list[list[Any]]:
        """Собранные обзоры в окне (секунды): `[первый кадр мс, конец последней клетки мс, шаг с, идёт ли]`."""
        today, out = day_of(time.time()), []
        for (camera, day), state in sorted(self._days.items()):
            begin = day_start(day)
            if camera != sink.spec.id or state.built is None or begin >= stop or begin + 86400 <= start:
                continue
            first, last = state.built
            out.append([int((begin + first * STEP_S) * 1000), int((begin + (last + 1) * STEP_S) * 1000), STEP_S, day == today])
        return out

    def path(self, sink: Any, at: float) -> Path | None:
        day = day_of(at)
        state = self._days.get((sink.spec.id, day))
        return _folder(sink, day) / OVERVIEW_FILE if state is not None and state.built is not None else None

    async def step(self, sinks: list[Any]) -> None:
        """Один проход сторожа: недостающие кадры сетки (не больше `PASS_FRAMES`) и пересборка изменившихся суток."""
        binary, nice = await self.env.run(lambda: (shutil.which("ffmpeg"), shutil.which("nice")))
        if not binary:
            return
        budget = PASS_FRAMES
        for sink in sinks:
            if not sink.previews.loaded:
                continue
            for day in sink.previews.day_names():
                state = await self._state(sink, day)
                todo = (await self.env.run(_todo, sink.previews, day, state))[: max(budget, 0)]
                budget -= len(todo)
                added = 0
                for k, piece, offset in todo:
                    size = await self._extract(binary, nice or "", sink, piece, offset, _folder(sink, day) / OVERVIEW_DIR / f"{k}.jpg")
                    if size:
                        state.done.add(k)
                        sink.index.account(day_start(day), size)
                        added += 1
                    else:
                        state.failed.add(k)
                if state.done and (added or state.built is None):
                    await self._build(binary, nice or "", sink, day, state)
            if budget <= 0:
                return

    async def _state(self, sink: Any, day: str) -> _Day:
        key = (sink.spec.id, day)
        if key not in self._days:
            self._days[key] = await self.env.run(_load, _folder(sink, day))
        return self._days[key]

    async def _extract(self, binary: str, nice: str, sink: Any, piece: Path, offset: float, target: Path) -> int:
        await self.env.run(lambda: target.parent.mkdir(parents=True, exist_ok=True))
        argv = [binary, "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{offset:.3f}", "-i", str(piece),
                "-frames:v", "1", "-q:v", "4", "-y", str(target)]
        if not await self._ffmpeg(nice, argv, f"overview:{sink.spec.id}"):
            return 0
        return await self.env.run(lambda: target.stat().st_size if target.is_file() else 0)

    async def _build(self, binary: str, nice: str, sink: Any, day: str, state: _Day) -> None:
        folder = _folder(sink, day)
        frames = sorted(state.done)
        listing = LIST_DIR / f"{sink.spec.id}-{day}.ffconcat"
        lines = ["ffconcat version 1.0"]
        for at, k in enumerate(frames):
            # Кадр держится до следующего по сетке: секунда видео — клетка сетки.
            lines += [f"file '{folder / OVERVIEW_DIR / f'{k}.jpg'}'", f"duration {(frames[at + 1] if at + 1 < len(frames) else k + 1) - k}"]
        target, partial = folder / OVERVIEW_FILE, folder / f".{OVERVIEW_FILE}"

        def write() -> int:
            LIST_DIR.mkdir(parents=True, exist_ok=True)
            listing.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return target.stat().st_size if target.is_file() else 0

        before = await self.env.run(write)
        argv = [binary, "-hide_banner", "-loglevel", "error", "-nostdin", "-f", "concat", "-safe", "0", "-i", str(listing),
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p", "-fps_mode", "vfr", "-c:v", "libx264",
                "-preset", "veryfast", "-crf", "30", "-g", "1", "-bf", "0", "-an", "-movflags", "+faststart", "-f", "mp4",
                "-y", str(partial)]
        ok = await self._ffmpeg(nice, argv, f"overview:{sink.spec.id}")

        def swap() -> int:
            listing.unlink(missing_ok=True)
            if not ok or not partial.is_file():
                partial.unlink(missing_ok=True)
                return -1
            os.replace(partial, target)
            return target.stat().st_size

        size = await self.env.run(swap)
        if size < 0:
            LOGGER.warning("Обзор превью %s за %s не собрался", sink.spec.id, day)
            return
        sink.index.account(day_start(day), size - before)
        state.built = (frames[0], frames[-1])

    async def _ffmpeg(self, nice: str, argv: list[str], key: str) -> bool:
        proc = await asyncio.create_subprocess_exec(
            *([nice, "-n", "10"] if nice else []), *argv,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        host_load.track(key, proc.pid)
        try:
            return await asyncio.wait_for(proc.wait(), 60) == 0
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False
        finally:
            host_load.untrack(key, proc.pid)


def _folder(sink: Any, day: str) -> Path:
    return sink.previews.root / day / "preview"


def _load(folder: Path) -> _Day:
    """Кадры суток с диска (перезапуск HA); обзор новее всех кадров — собран, иначе пересоберётся."""
    state = _Day()
    frames = folder / OVERVIEW_DIR
    stamps: list[float] = []
    for item in frames.iterdir() if frames.is_dir() else []:
        if item.suffix == ".jpg" and item.stem.isdigit():
            state.done.add(int(item.stem))
            stamps.append(item.stat().st_mtime)
    built = folder / OVERVIEW_FILE
    if state.done and built.is_file() and built.stat().st_mtime >= max(stamps):
        state.built = (min(state.done), max(state.done))
    return state


def _todo(previews: Any, day: str, state: _Day) -> list[tuple[int, Path, float]]:
    """Кадры сетки суток, которых ещё нет, а кусок превью их накрывает: (k, кусок, место в нём)."""
    begin, out = day_start(day), []
    for k in range(FRAMES):
        if k in state.done or k in state.failed:
            continue
        at = begin + k * STEP_S
        for start, length in previews.between(at, at + EDGE_S):
            if start <= at + EDGE_S and at < start + length:
                path = previews.path(start)
                if path is not None:
                    out.append((k, path, max(at - start, 0.0)))
                break
    return out
