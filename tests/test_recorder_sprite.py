"""Спрайт окна записи (`core/recorder_frames.py`, служба `/sprite`): слоты по времени, сборка
настоящим ffmpeg, кэш закрытого окна и его отсутствие у текущего.

⚠ Здесь ffmpeg НАСТОЯЩИЙ (кадры — крошечные JPEG из `lavfi`): сборка сетки — фильтр ffmpeg, и
подставной скрипт проверил бы только то, что команда вызвана, а не что сетка 10 × 6 собирается.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
import pytest

from mega_home.core import recorder as rec
from mega_home.core import services, storage
from mega_home.core.recorder_api import start_api
from mega_home.core.recorder_frames import compose, slot_frames, sprite_for, sprite_path, window_start
from mega_home.core.recorder_index import SegmentIndex

FFMPEG = shutil.which("ffmpeg") or ""
needs_ffmpeg = pytest.mark.skipif(not FFMPEG, reason="нет ffmpeg")


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


def _segment(root: Path, epoch: float, length_ms: int = 10_000, frame: str | None = None) -> Path:
    folder = root / "cam" / rec.day_of(epoch)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{time.strftime('%H%M%S', time.gmtime(epoch))}-{length_ms}.mkv"
    path.write_bytes(b"x")
    if frame:
        _jpeg(path.with_suffix(".jpg"), frame)
    return path


def _jpeg(out: Path, color: str, size: str = "32x18") -> None:
    subprocess.run(
        [FFMPEG, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", f"color={color}:s={size}", "-frames:v", "1", "-f", "image2", "-c:v", "mjpeg", "-y", str(out)],
        check=True,
    )


def _probe(image: bytes) -> tuple[int, int]:
    out = subprocess.run(
        [shutil.which("ffprobe") or "ffprobe", "-v", "error", "-show_entries", "stream=width,height", "-of", "csv=p=0", "-i", "pipe:0"],
        input=image, capture_output=True, check=True,
    ).stdout.decode().strip()
    width, height = out.split(",")
    return int(width), int(height)


def _brightness(image: bytes, slot: int, x: int = 40, y: int = 20, width: int = 80, height: int = 50) -> float:
    """Средняя яркость участка слота `slot` (сетка 10 × 6 по 160 × 90); по умолчанию — середина."""
    x, y = (slot % 10) * 160 + x, (slot // 10) * 90 + y
    raw = subprocess.run(
        [FFMPEG, "-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-vf", f"crop={width}:{height}:{x}:{y},format=gray", "-f", "rawvideo", "pipe:1"],
        input=image, capture_output=True, check=True,
    ).stdout
    return sum(raw) / len(raw)


@needs_ffmpeg
def test_слоты_по_времени_а_не_по_порядку(tmp_path: Path) -> None:
    """Слот — по моменту `начало + i·10 с`: кусок, покрывающий момент, иначе начавшийся в слоте.
    Пропуск во времени — пустой слот, а не сдвиг соседей; кусок без кадра слота не даёт."""
    start = window_start(rec.epoch_of("20261023T120712") or 0)
    assert start == rec.epoch_of("20261023T120000")
    first = _segment(tmp_path, start + 3, 5_000, frame="white")
    second = _segment(tmp_path, start + 25, 10_000, frame="white")
    _segment(tmp_path, start + 100, 10_000)
    index = SegmentIndex(tmp_path / "cam")
    index.load()
    slots = slot_frames(index, start)
    assert len(slots) == 60
    # 0 — кусок начался в слоте; 1 — момент 10 с не покрыт, начала нет; 2 — начался в слоте;
    # 3 — момент 30 с покрыт вторым куском; 10 — кусок есть, кадра нет.
    assert slots[:5] == [first.with_suffix(".jpg"), None, second.with_suffix(".jpg"), second.with_suffix(".jpg"), None]
    assert slots[10] is None and all(s is None for s in slots[4:])
    assert sprite_path(tmp_path / "cam", start) == tmp_path / "cam" / "2026-10-23" / "sprites" / "1200.jpg"


@needs_ffmpeg
def test_спрайт_собирается_ffmpeg_и_пустые_слоты_чёрные(tmp_path: Path) -> None:
    a, b = tmp_path / "a.jpg", tmp_path / "b.jpg"
    _jpeg(a, "white")
    _jpeg(b, "white")
    frames: list[Path | None] = [None] * 60
    frames[0], frames[37] = a, b
    work = tmp_path / "sprites"
    image = compose(frames, FFMPEG, work)
    assert image[:2] == b"\xff\xd8" and _probe(image) == (1600, 540)
    assert _brightness(image, 0) > 200 and _brightness(image, 37) > 200
    assert _brightness(image, 1) < 30 and _brightness(image, 59) < 30
    # Чёрная заглушка делается один раз на каталог; список кусков после сборки не остаётся.
    assert sorted(p.name for p in work.iterdir()) == ["blank.jpg"]
    assert compose([None] * 60, FFMPEG, tmp_path / "empty") == b"" and not (tmp_path / "empty").exists()
    # Камера 4:3 — кадр вписан в слот 16:9 с чёрными полями по бокам (120 × 90 и поля по 20).
    square = tmp_path / "c.jpg"
    _jpeg(square, "white", "64x48")
    wide = compose([None, square] + [None] * 58, FFMPEG, tmp_path / "four-three")
    assert _probe(wide) == (1600, 540) and _brightness(wide, 1) > 200 and _brightness(wide, 0) < 30
    assert _brightness(wide, 1, x=2, width=14) < 30 and _brightness(wide, 1, x=144, width=14) < 30


def _recorder(root: Path) -> rec.Recorder:
    recorder = rec.Recorder(FakeHost(root), lambda _listener: (lambda: None))
    recorder.storage = root
    spec = rec.camera_specs({"cameras": [{"id": "cam", "stream": "s", "source": "rtsp://x"}]})[0]
    recorder.cameras = {"cam": rec._Camera(recorder, spec, root / ".cache")}  # noqa: SLF001
    return recorder


def _get(recorder: rec.Recorder, *paths: str) -> list[tuple[int, str, bytes]]:
    async def scenario() -> list[tuple[int, str, bytes]]:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                out = []
                for path in paths:
                    response = await client.get(path)
                    out.append((response.status, response.headers.get("Cache-Control", ""), await response.read()))
                return out
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    return asyncio.run(scenario())


@needs_ffmpeg
def test_окно_без_кадров_404_и_без_файла_кэша() -> None:
    # Каталог — латиницей: путь кадра уходит в список ffmpeg.
    root = Path(tempfile.mkdtemp(prefix="rec-"))
    start = window_start(time.time() - 1200)
    _segment(root, start + 5)
    [(status, _cache, body)] = _get(_recorder(root), f"/sprite?camera=cam&at={int((start + 300) * 1000)}")
    assert status == 404 and "кадров в окне нет" in body.decode()
    assert not sprite_path(root / "cam", start).exists()
    [(foreign, _c, _b)] = _get(_recorder(root), f"/sprite?camera=chuzhaya&at={int(start * 1000)}")
    assert foreign == 404


@needs_ffmpeg
def test_закрытое_окно_кладётся_в_кэш_а_текущее_нет() -> None:
    root = Path(tempfile.mkdtemp(prefix="rec-"))
    past, now = window_start(time.time() - 1200), window_start(time.time())
    _segment(root, past + 40, frame="white")
    _segment(root, now, frame="white")
    recorder = _recorder(root)
    (closed, closed_cache, image), (current, current_cache, live) = _get(
        recorder, f"/sprite?camera=cam&at={int((past + 1) * 1000)}", f"/sprite?camera=cam&at={int(now * 1000)}"
    )
    assert closed == 200 and closed_cache == "private, max-age=3600" and _probe(image) == (1600, 540)
    assert current == 200 and current_cache == "no-store" and _probe(live) == (1600, 540)
    cached = sprite_path(root / "cam", past)
    assert cached.read_bytes() == image
    assert not sprite_path(root / "cam", now).exists()
    assert not list(cached.parent.glob("*.part")) and not list(cached.parent.glob("*.txt"))
    # Второй взгляд на закрытое окно — из файла, без сборки.
    cached.write_bytes(b"\xff\xd8cached")
    assert asyncio.run(sprite_for(recorder, "cam", past + 599)) == (b"\xff\xd8cached", True)
