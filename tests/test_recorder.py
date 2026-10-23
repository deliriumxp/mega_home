"""Запись камер (`core/recorder.py`, `core/recorder_api.py`): раздел конфига, хранилище, окна
движения, уборка, индекс и список воспроизведения.

⚠ ffmpeg и go2rtc здесь не запускаются: куски — пустые файлы с именами, как их кладёт
запись, длительность в имени. Настоящий поток проверен на стенде 2026-10-23 (камера
`192.168.88.202` → go2rtc 1.9.14 → куски Matroska → `ffmpeg:<список>#input=playlist`).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
from mega_home.core import recorder as rec
from mega_home.core import services
from mega_home.core.recorder_api import start_api

SECTION = {
    "storage": "/media/video",
    "cameras": [
        {
            "id": "onvif-aa",
            "stream": "onvif_aa_main",
            "source": "rtsp://u:p@10.0.0.5/live/main",
            "mode": "motion",
            "retainDays": 3,
            "preSec": 10,
            "postSec": 20,
            "motion": {"access": "onvif-aa", "source": "events", "on": ['IsMotion" Value="true'], "off": ['IsMotion" Value="false']},
        },
        {"id": "../evil", "stream": "x", "source": "y"},
        {"id": "no-source", "stream": "x"},
    ],
}


def _camera(tmp_path: Path, **overrides) -> rec._Camera:  # noqa: SLF001
    spec = rec.camera_specs(SECTION)[0]
    recorder = rec.Recorder(FakeHost(tmp_path), lambda _listener: (lambda: None))
    recorder.storage = tmp_path
    return rec._Camera(recorder, spec if not overrides else rec.CameraSpec(**{**spec.__dict__, **overrides}), tmp_path / ".cache")


def _frame(text: str, at: float) -> dict:
    return {"access": "onvif-aa", "source": "events", "at": at, "data": {"text": text}}


def test_раздел_берёт_только_полные_камеры_с_безопасным_id() -> None:
    specs = rec.camera_specs(SECTION)
    assert [s.id for s in specs] == ["onvif-aa"]
    assert specs[0].motion_mode and specs[0].retain_days == 3
    assert rec.camera_specs(None) == []


def test_хранилище_без_диска_не_пишется() -> None:
    # Свой каталог: путь хранилища — только латиница (уходит в ffconcat и строку go2rtc), а в
    # `tmp_path` pytest — имя теста кириллицей.
    folder = Path(tempfile.mkdtemp(prefix="rec-")) / "video"
    assert "нет каталога" in rec.storage_problem(folder)
    folder.mkdir()
    assert "не смонтирован" in rec.storage_problem(folder)
    (folder / rec.STORAGE_MARK).touch()
    assert rec.storage_problem(folder) == ""
    assert "абсолютный" in rec.storage_problem(Path("video"))
    assert "абсолютный" in rec.storage_problem(Path("/media/vid'eo#x"))


def test_окно_движения_с_запасом_до_и_после(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    camera.on_event(_frame('<x Name="IsMotion" Value="true"/>', 1000))
    # Открытое окно: всё, что после «начала минус запас», нужно.
    assert camera.wanted(985, 995, now=1005)
    assert not camera.wanted(970, 980, now=1005)
    camera.on_event(_frame('<x Name="IsMotion" Value="false"/>', 1030))
    assert camera.wanted(1045, 1055, now=1100)
    assert not camera.wanted(1060, 1070, now=1100)
    # Чужой источник окно не трогает.
    camera.on_event({**_frame('IsMotion" Value="true', 1200), "access": "other"})
    assert not camera.wanted(1200, 1210, now=1210)


def test_короткое_движение_в_одном_ответе_сохраняется(tmp_path: Path) -> None:
    """«Началось, кончилось» одним ответом (движение между двумя опросами) — окно-точка с запасом."""
    camera = _camera(tmp_path)
    camera.on_event(_frame('IsMotion" Value="true … IsMotion" Value="false', 1000))
    assert camera.windows == [[1000, 1000, 1000]]
    assert camera.wanted(985, 995, now=1100)
    camera.on_event(_frame('IsMotion" Value="false … IsMotion" Value="true', 1050))
    assert camera.windows[-1][1] is None


def test_смены_считаются_только_в_сообщениях_темы_движения(tmp_path: Path) -> None:
    """`State` несут и «темно», и «закрыли объектив»: без темы движения окна нет."""
    camera = _camera(tmp_path, split="NotificationMessage", require=("/Motion<", "MotionAlarm<"))
    dark = '<NotificationMessage><Topic>tns1:VideoSource/ImageTooDark</Topic><x Name="IsMotion" Value="true"/>'
    camera.on_event(_frame(dark, 1000))
    assert camera.windows == []
    motion = '<NotificationMessage><Topic>tns1:VideoSource/MotionAlarm</Topic><x Name="IsMotion" Value="true"/>'
    camera.on_event(_frame(dark + motion, 1010))
    assert camera.windows[-1][:2] == [1010, None]


def test_движение_своей_детекции_открывает_и_закрывает_окно(tmp_path: Path) -> None:
    """Событие дома (`motion.py`) без текста: данные одной строкой JSON — та же сверка подстрок."""
    camera = _camera(tmp_path, events="motion", on=('"state":"start"',), off=('"state":"stop"',))
    own = {"access": "onvif-aa", "source": "motion", "event": "motion"}
    camera.on_event({**own, "at": 1000, "data": {"state": "start", "share": 0.012}})
    assert camera.windows[-1][:2] == [1000, None]
    camera.on_event({**own, "at": 1040, "data": {"share": 0.02, "state": "stop"}})
    assert camera.windows[-1][:2] == [1000, 1040]


def test_сторож_останавливает_запись_без_диска_и_поднимает_с_ним() -> None:
    folder = Path(tempfile.mkdtemp(prefix="rec-"))
    recorder = rec.Recorder(FakeHost(folder), lambda _listener: (lambda: None))
    recorder._specs = rec.camera_specs({**SECTION, "storage": str(folder)})  # noqa: SLF001
    recorder.storage = folder

    async def scenario() -> tuple[str, int, str, int]:
        await recorder._check()  # noqa: SLF001
        without = (recorder.why, len(recorder.cameras))
        (folder / rec.STORAGE_MARK).touch()
        await recorder._check()  # noqa: SLF001
        with_disk = (recorder.why, len(recorder.cameras))
        await recorder._stop_all()  # noqa: SLF001
        return (*without, *with_disk)

    why_without, cams_without, why_with, cams_with = asyncio.run(scenario())
    assert "не смонтирован" in why_without and cams_without == 0
    assert why_with == "" and cams_with == 1


def test_незакрытое_окно_закрывается_сроком(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    camera.on_event(_frame('IsMotion" Value="true', 1000))
    camera._expire(1000 + rec.MAX_MOTION_S + 1)  # noqa: SLF001
    assert camera.windows[-1][1] == 1000


def _segment(root: Path, camera: str, epoch: float, length_ms: int = 10_000) -> Path:
    folder = root / camera / rec.day_of(epoch)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{time.strftime('%H%M%S', time.gmtime(epoch))}-{length_ms}.mkv"
    path.write_bytes(b"x")
    return path


def test_индекс_и_уборка_по_сроку(tmp_path: Path) -> None:
    now = rec.epoch_of("20261023T120000")
    assert now is not None
    old = _segment(tmp_path, "onvif-aa", now - 5 * 24 * 3600)
    fresh = _segment(tmp_path, "onvif-aa", now - 60)
    found = rec.segments_of(tmp_path, "onvif-aa", now - 120, now)
    assert [(b, n) for b, n, _p in found] == [(now - 60, 10.0)]
    rec.clean(tmp_path, [_camera(tmp_path)], now)
    assert not old.exists() and fresh.exists()


def test_служба_отдаёт_дни_участки_и_список(tmp_path: Path) -> None:
    base = rec.epoch_of("20261023T120000")
    assert base is not None
    for offset in (0, 10, 20, 60, 6 * 3600):
        _segment(tmp_path, "onvif-aa", base + offset)
    recorder = rec.Recorder(FakeHost(tmp_path), lambda _listener: (lambda: None))
    recorder.storage = tmp_path
    recorder.cameras = {"onvif-aa": _camera(tmp_path)}

    async def scenario() -> tuple[dict, dict, dict, int, int, dict, int, dict]:
        runner = await start_api(recorder)
        try:
            port = services.resolve(rec.SERVICE)
            assert port
            async with TestClient(TestServer(runner.app)) as client:
                days = await (await client.get("/days?camera=onvif-aa")).json()
                # Сутки зрителя UTC+13 (полдень UTC — уже следующие местные сутки).
                local = await (await client.get("/days?camera=onvif-aa&offset=46800")).json()
                frame = (await client.get(f"/frame?camera=onvif-aa&at={int((base + 3600) * 1000)}")).status
                segments = await (await client.get(f"/segments?camera=onvif-aa&from={base}&to={base + 120}")).json()
                play = await (await client.post(f"/playlist?camera=onvif-aa&at={int((base + 15) * 1000)}")).json()
                missing = (await client.post(f"/playlist?camera=onvif-aa&at={int((base + 7 * 3600) * 1000)}")).status
                # После участка — пустота длиннее окна списка: продолжение всё равно названо.
                night = await (await client.post(f"/playlist?camera=onvif-aa&at={int((base + 65) * 1000)}")).json()
                foreign = (await client.get("/days?camera=chuzhaya")).status
                return days, segments, play, missing, foreign, local, frame, night
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    days, segments, play, missing, foreign, local, frame, night = asyncio.run(scenario())
    assert night["nextMs"] == int((base + 6 * 3600) * 1000)
    assert days == {"days": ["2026-10-23"]}
    ms = int(base * 1000)
    # Три куска подряд — один участок, кусок через 30 с пустоты — второй.
    assert segments["segments"][:2] == [[ms, ms + 30_000], [ms + 60_000, ms + 70_000]]
    assert play["startMs"] == ms + 15_000
    assert play["parts"][0] == [ms + 15_000, 5_000]
    # Список — только непрерывный участок; где запись продолжается — `nextMs`.
    assert len(play["parts"]) == 2
    assert play["nextMs"] == ms + 60_000
    assert local == {"days": ["2026-10-24"]}
    assert frame == 404
    path = play["source"].split("ffmpeg:", 1)[1].split("#", 1)[0]
    text = Path(path).read_text()
    assert text.startswith("ffconcat version 1.0\n") and "inpoint 5.000" in text
    assert play["source"].endswith("#input=playlist#video=copy#audio=copy")
    assert missing == 404 and foreign == 404


def test_без_раздела_записи_нет(tmp_path: Path) -> None:
    host = FakeHost(tmp_path)
    recorder = rec.Recorder(host, lambda _listener: (lambda: None))
    recorder.apply({"recorder": None})
    recorder.apply({"recorder": None})
    assert host.spawned == ["mega_home: запись камер"]
    assert recorder.state()["enabled"] is False
    assert os.listdir(tmp_path) == []
