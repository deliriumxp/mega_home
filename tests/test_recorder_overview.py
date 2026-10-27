"""Обзор суток превью (`core/recorder_overview.py`): кадр раз в 15 минут одним файлом — превью крупных масштабов.

Здесь работает настоящий ffmpeg: куски превью — маленькие mp4, как у процесса превью, обзор собирается из них.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from mega_home.core import recorder_overview as ov
from mega_home.core.recorder_index import epoch_of
from mega_home.core.recorder_preview import PREVIEW_DIR, PreviewIndex, piece_name

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="нет ffmpeg")

DAY = "2026-10-20"


class _Env:
    async def run(self, func: Any, *args: Any) -> Any:
        return func(*args)


class _Index:
    def __init__(self) -> None:
        self.accounted = 0

    def account(self, _begin: float, size: int) -> None:
        self.accounted += size


class _Sink:
    def __init__(self, root: Path) -> None:
        self.spec = type("Spec", (), {"id": "cam"})()
        self.previews = PreviewIndex(root / "cam")
        self.index = _Index()


def _piece(root: Path, begin: float) -> None:
    folder = root / "cam" / DAY / PREVIEW_DIR
    folder.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=320x180:rate=0.5:duration=600",
         "-c:v", "libx264", "-g", "5", "-pix_fmt", "yuv420p", "-y", str(folder / piece_name(begin, 600.0))],
        check=True,
    )


def _pts(path: Path) -> list[float]:
    out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "frame=pts_time",
                          "-of", "csv=p=0", str(path)], capture_output=True, text=True, check=True).stdout
    return [float(line.strip(",")) for line in out.split()]


def test_обзор_суток_собирается_из_кусков_превью_по_сетке(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Кадр k сетки (начало суток UTC + k·15 мин) — из куска, который его накрывает; в файле кадр k — на
    секунде k − k₀, пустота без куска — без кадра. Проход сторожа берёт не больше `PASS_FRAMES` кадров."""
    start = epoch_of("20261020T000000")
    assert start is not None
    # 00:10–00:20 накрывает 00:15 (k=1), 00:30–00:40 — 00:30 (k=2), 01:00–01:10 — 01:00 (k=4); 00:45 (k=3) — пустота.
    for minutes in (10, 30, 60):
        _piece(tmp_path, start + minutes * 60)
    sink = _Sink(tmp_path)
    sink.previews.load()
    monkeypatch.setattr(ov, "PASS_FRAMES", 2)
    overviews = ov.Overviews(_Env())

    asyncio.run(overviews.step([sink]))
    built = tmp_path / "cam" / DAY / PREVIEW_DIR / ov.OVERVIEW_FILE
    assert _pts(built) == [0.0, 1.0]
    asyncio.run(overviews.step([sink]))
    assert _pts(built) == [0.0, 1.0, 3.0]
    assert sink.index.accounted > built.stat().st_size

    step = ov.STEP_S
    first_ms, stop_ms = int((start + step) * 1000), int((start + 5 * step) * 1000)
    assert overviews.listed(sink, start, start + 86400) == [[first_ms, stop_ms, step, False]]
    assert overviews.listed(sink, start + 86400, start + 2 * 86400) == []
    assert overviews.path(sink, first_ms / 1000) == built
    # Тот же обзор после перезапуска HA: кадры с диска, пересборки нет.
    again = ov.Overviews(_Env())
    mtime = built.stat().st_mtime_ns
    asyncio.run(again.step([sink]))
    assert built.stat().st_mtime_ns == mtime
    assert again.listed(sink, start, start + 86400) == [[first_ms, stop_ms, step, False]]
