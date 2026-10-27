"""Превью-видео записи (`core/recorder_preview.py`, служба `/previews` и `/preview`).

⚠ ffmpeg здесь не запускается: куски — файлы с именами, как их кладёт процесс превью, список — CSV,
как его пишет `-segment_list`. Настоящая команда проверена на камере объекта 2026-10-24 (Tapo `.90.147`,
ffmpeg 7.1): куски по часам дома, список в секундах UTC, 320×180 — ~280 КБ на 10 минут.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
import pytest

from mega_home.core import recorder as rec
from mega_home.core import services, storage
from mega_home.core.go2rtc_embed import RTSP_PORT
from mega_home.core.recorder_api import start_api
from mega_home.core.recorder_preview import (
    PREVIEW_LIST, SWEPT, PreviewIndex, PreviewTune, command, sweep_legacy, tune_of, whole_fragments,
)

SECTION = {"storage": "/media/video", "cameras": [{"id": "cam", "stream": "cam_main", "source": "rtsp://u:p@10.0.0.5/main"}]}


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


def _camera(root: Path) -> rec._Camera:  # noqa: SLF001
    recorder = rec.Recorder(FakeHost(root), lambda _listener: (lambda: None))
    recorder.storage = root
    # «Локальный диск» с лимитом: иначе хранилище во временной папке не годится (`problem`).
    recorder.local, recorder.limit = True, 1024**3
    camera = rec._Camera(recorder, rec.camera_specs(SECTION)[0], root / ".cache")  # noqa: SLF001
    camera.preview.cache.mkdir(parents=True)
    camera.previews.loaded = True
    return camera


def _box(kind: bytes, body: bytes = b"") -> bytes:
    return (8 + len(body)).to_bytes(4, "big") + kind + body


def test_команда_берёт_ключевые_кадры_своего_go2rtc_и_пишет_фрагменты() -> None:
    """Поток — у своего go2rtc на петле (подключение к камере одно), декод — только ключевых кадров,
    время — часы дома, mp4 — фрагментами (идущий кусок читается)."""
    argv = command("/usr/bin/ffmpeg", "/usr/bin/nice", "cam_main", Path("/c"))
    joined = " ".join(argv)
    assert argv[:3] == ["/usr/bin/nice", "-n", "10"]
    assert f"rtsp://127.0.0.1:{RTSP_PORT}/cam_main" in argv
    assert argv.index("-skip_frame") < argv.index("-i") and "-use_wallclock_as_timestamps 1" in joined and "-copyts" in argv
    # ⚠ `flush_packets` у вложенного muxer: без него идущий кусок 40 с стоял на 28 байтах (замер ffmpeg 7.1).
    assert "movflags=+frag_keyframe+empty_moov+default_base_moof:flush_packets=1" in argv and "libx264" in argv
    assert argv[argv.index("-segment_list") + 1] == f"/c/{PREVIEW_LIST}" and argv[-1] == "/c/preview/%Y%m%dT%H%M%S.mp4"
    assert command("/usr/bin/ffmpeg", "", "cam_main", Path("/c"))[0] == "/usr/bin/ffmpeg"


def test_качество_превью_данными_конфига_в_пределах() -> None:
    """Высота, crf и ключевой кадр — раздел `recorder.preview` менеджера: подстройка без релиза дома."""
    assert tune_of(None) == PreviewTune(180, 33, 5)
    assert tune_of({"height": 241, "crf": 99, "gop": "x"}) == PreviewTune(240, 45, 5)
    argv = command("/usr/bin/ffmpeg", "", "cam_main", Path("/c"), tune_of({"height": 360, "crf": 28, "gop": 10}))
    assert "scale=-2:360,format=yuv420p" in argv
    assert argv[argv.index("-crf") + 1] == "28" and argv[argv.index("-g") + 1] == "10"


def test_закрытые_куски_по_списку_ffmpeg_уходят_в_хранилище() -> None:
    """Список ffmpeg называет начало и конец в секундах UTC; первый кусок процесса — с нуля, его
    начало — в имени файла. Закрытый — в `<день>/preview/` с длительностью в имени, в индекс и в объём."""
    root = Path(tempfile.mkdtemp(prefix="pv-"))
    camera = _camera(root)
    first, second = rec.epoch_of("20261024T100000"), rec.epoch_of("20261024T100937")
    assert first is not None and second is not None
    for stamp in ("20261024T100000", "20261024T100937", "20261024T102000"):
        (camera.preview.cache / f"{stamp}.mp4").write_bytes(b"m" * 10)
    listed = camera.cache / PREVIEW_LIST
    listed.write_text(f"20261024T100000.mp4,0.000000,{second - 2:.6f}\n")
    camera.preview.collect(False)
    day = root / "cam" / "2026-10-24" / "preview"
    assert sorted(p.name for p in day.iterdir()) == ["100000-575000.mp4"]
    # Дописанная строка — с прошлого места, неполная ждёт.
    with listed.open("a") as handle:
        handle.write(f"20261024T100937.mp4,{second:.6f},{second + 600:.6f}\n20261024T1020")
    camera.preview.collect(False)
    assert sorted(p.name for p in day.iterdir()) == ["100000-575000.mp4", "100937-600000.mp4"]
    assert [p[0] for p in camera.previews.between(first, first + 3600)] == [first, second]
    assert camera.index.size() == 0  # день кусков ещё не заведён — объём ляжет на день, когда он будет
    # Остановка: идущий кусок — до последней записи в файл.
    live = camera.preview.cache / "20261024T102000.mp4"
    stop = second + 900
    os.utime(live, (stop, stop))
    camera.preview.collect(True)
    assert "102000-277000.mp4" in {p.name for p in day.iterdir()} and not live.exists()


def test_список_без_часов_стены_длина_по_меткам_потока() -> None:
    """Выход превью у читателя конвейера идёт без `-use_wallclock_as_timestamps` (вход общий с копией
    записи): список называет метки потока, начало куска — в имени, длина — конец минус начало."""
    root = Path(tempfile.mkdtemp(prefix="pv-"))
    camera = _camera(root)
    (camera.preview.cache / "20261024T100000.mp4").write_bytes(b"m" * 10)
    (camera.cache / PREVIEW_LIST).write_text("20261024T100000.mp4,12.500000,612.500000\n")
    camera.preview.collect(False)
    assert [p.name for p in (root / "cam" / "2026-10-24" / "preview").iterdir()] == ["100000-600000.mp4"]


def test_без_хранилища_кусок_не_ложится_на_флеш() -> None:
    root = Path(tempfile.mkdtemp(prefix="pv-"))
    camera = _camera(root)
    camera.rec.problem = lambda: "диск хранилища не смонтирован"  # type: ignore[method-assign]
    piece = camera.preview.cache / "20261024T100000.mp4"
    piece.write_bytes(b"m")
    os.utime(piece, (time.time(), time.time()))
    camera.preview.collect(True)
    assert not (root / "cam").exists() and not piece.exists()


def test_идущий_кусок_отдаётся_до_целого_фрагмента() -> None:
    """ffmpeg дописывает хвост — телефон получает файл до конца последнего целого `moof` + `mdat`."""
    head = _box(b"ftyp", b"isom") + _box(b"moov", b"x" * 20)
    one = _box(b"moof", b"a" * 8) + _box(b"mdat", b"b" * 30)
    partial = _box(b"moof", b"c" * 8) + _box(b"mdat", b"d" * 30)[:20]
    assert whole_fragments(head + one + partial) == head + one
    assert whole_fragments(head) == b""
    # 64-битный размер (поле 1) и «до конца файла» (поле 0, недописан).
    big = (1).to_bytes(4, "big") + b"mdat" + (16 + 4).to_bytes(8, "big") + b"zzzz"
    assert whole_fragments(head + big) == head + big
    assert whole_fragments(head + one + (0).to_bytes(4, "big") + b"mdat" + b"q" * 9) == head + one


def test_служба_отдаёт_список_и_куски(tmp_path: Path) -> None:
    root = Path(tempfile.mkdtemp(prefix="pv-"))
    camera = _camera(root)
    begin = rec.epoch_of("20261024T100000")
    assert begin is not None
    day = root / "cam" / "2026-10-24" / "preview"
    day.mkdir(parents=True)
    (day / "100000-600000.mp4").write_bytes(b"closed")
    camera.previews.loaded = False
    live = camera.preview.cache / "20261024T101000.mp4"
    head = _box(b"ftyp") + _box(b"moov")
    live.write_bytes(head + _box(b"moof") + _box(b"mdat", b"v") + _box(b"moof")[:5])
    os.utime(live, (begin + 700, begin + 700))
    recorder = camera.rec
    recorder.cameras = {"cam": camera}

    async def scenario() -> tuple:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                listing = await (await client.get(f"/previews?camera=cam&from={begin}&to={begin + 3600}")).json()
                closed = await client.get(f"/preview?camera=cam&start={int(begin * 1000)}")
                going = await client.get(f"/preview?camera=cam&start={int((begin + 600) * 1000)}")
                missing = (await client.get(f"/preview?camera=cam&start={int((begin + 5) * 1000)}")).status
                return listing, (closed.status, closed.headers["Cache-Control"], await closed.read()), (
                    going.headers["Cache-Control"], await going.read()), missing
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    listing, closed, going, missing = asyncio.run(scenario())
    ms = int(begin * 1000)
    assert listing == {"previews": [[ms, ms + 600_000]], "live": [ms + 600_000, ms + 700_000]}
    assert closed == (200, "private, max-age=86400", b"closed")
    assert going == ("no-store", head + _box(b"moof") + _box(b"mdat", b"v"))
    assert missing == 404


def test_путь_превью_после_загрузки_совпадает_с_файлом(tmp_path: Path) -> None:
    """Как у кусков записи: `int(8.027 * 1000)` = 8026 — после перезапуска HA путь вёл мимо файла."""
    begin = rec.epoch_of("20261027T033037")
    assert begin is not None
    day = tmp_path / "cam" / "2026-10-27" / "preview"
    day.mkdir(parents=True)
    piece = day / "033037-8027.mp4"
    piece.write_bytes(b"x")
    index = PreviewIndex(tmp_path / "cam")
    index.load()
    assert index.path(begin) == piece


def test_прежние_кадры_и_спрайты_убираются_один_раз(tmp_path: Path) -> None:
    """Кадры к кускам и спрайты дома 0.5.26–0.5.31 — мусор: уходят с диска, куски и превью остаются."""
    day = tmp_path / "cam" / "2026-10-23"
    (day / "sprites").mkdir(parents=True)
    (day / "sprites" / "1200-abc.jpg").write_bytes(b"s")
    (day / "120000-10000.mkv").write_bytes(b"x")
    (day / "120000-10000.jpg").write_bytes(b"j")
    (day / "120010-10000.jpg").write_bytes(b"j")
    (day / "blank.jpg").write_bytes(b"j")
    (day / "preview").mkdir()
    (day / "preview" / "120000-600000.mp4").write_bytes(b"p")
    sweep_legacy(tmp_path / "cam")
    assert sorted(p.name for p in day.iterdir()) == ["120000-10000.mkv", "preview"]
    assert (tmp_path / "cam" / SWEPT).exists()
    # Второй запуск каталог не обходит.
    (day / "late.jpg").write_bytes(b"j")
    sweep_legacy(tmp_path / "cam")
    assert (day / "late.jpg").exists()
