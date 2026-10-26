"""Конвейер камеры (`core/video_pipeline.py`) и раздел `video` у записи (`core/recorder.py`).

⚠ ffmpeg и go2rtc здесь не запускаются: проверяется, что конвейер заводит потоки камеры, что своя
детекция включает запись вызовом и что раздел `video` главнее прежнего `recorder`. Командная строка
читателя — `test_video_reader.py`, цикл процесса — `test_proc_runner.py`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from fake_host import FakeHost
from mega_home.core import recorder as rec
from mega_home.core import storage, video_spec
from mega_home.core.video_pipeline import CameraPipeline

CAMERA = {
    "id": "onvif-aa",
    "streams": {
        "main": {"name": "onvif_aa", "source": "rtsp://u:p@10.0.0.5/main"},
        "sub": {"name": "onvif_aa_sub", "source": "rtsp://u:p@10.0.0.5/sub"},
    },
    "record": {"stream": "main", "mode": "motion"},
    "detect": {"stream": "sub"},
}
DETECT_ONLY = {**CAMERA, "id": "onvif-bb", "record": None}


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


class _Answer:
    status = 200

    async def __aenter__(self) -> _Answer:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Session:
    def __init__(self) -> None:
        self.puts: list[tuple[str, str]] = []

    def put(self, url: str) -> _Answer:
        query = parse_qs(urlparse(url).query)
        self.puts.append((query["name"][0], query["src"][0]))
        return _Answer()


def _recorder(tmp_path: Path, session: _Session | None = None) -> tuple[rec.Recorder, list[tuple]]:
    published: list[tuple] = []
    recorder = rec.Recorder(
        FakeHost(tmp_path, session),
        lambda _listener: (lambda: None),
        lambda access, source, event, data, local=False, file=None: published.append((access, source, event, data, local, file)),
    )
    recorder.storage = tmp_path
    return recorder, published


def _plan(raw: dict) -> video_spec.CameraPlan:
    section = video_spec.video_section({"video": {"storage": "/media/v", "cameras": [raw]}})
    assert section is not None
    return section.cameras[0]


def test_конвейер_регистрирует_все_потоки_камеры(tmp_path: Path) -> None:
    session = _Session()
    recorder, _published = _recorder(tmp_path, session)
    pipeline = CameraPipeline(recorder, _plan(CAMERA), tmp_path / ".cache")
    asyncio.run(pipeline.register())
    assert session.puts == [("onvif_aa", "rtsp://u:p@10.0.0.5/main"), ("onvif_aa_sub", "rtsp://u:p@10.0.0.5/sub")]


def test_своя_детекция_открывает_окно_записи_без_концентратора(tmp_path: Path) -> None:
    recorder, published = _recorder(tmp_path)
    pipeline = CameraPipeline(recorder, _plan(CAMERA), tmp_path / ".cache")
    assert pipeline.record is not None
    pipeline.motion_changed(True, 1000.0)
    assert pipeline.record.windows[-1][:2] == [1000.0, None]
    pipeline.emit("motion", {"state": "start", "share": 0.1}, None)
    assert published[-1] == ("onvif-aa", "motion", "motion", {"state": "start", "share": 0.1}, True, None)


def test_сила_движения_в_ряду_своей_камеры(tmp_path: Path) -> None:
    recorder, _published = _recorder(tmp_path)
    pipeline = CameraPipeline(recorder, _plan(CAMERA), tmp_path / ".cache")
    recorder.pipelines["onvif-aa"] = pipeline
    pipeline.share(1000.0, 0.3)
    assert pipeline.activity.pending(0, 2000) == [(1000, 0.3)]
    assert recorder.pending_activity("onvif-aa", 0, 2000) == [(1000, 0.3)]


def test_раздел_video_главнее_recorder(tmp_path: Path) -> None:
    recorder, _published = _recorder(tmp_path)
    legacy = {"storage": str(tmp_path), "cameras": [{"id": "old", "stream": "s", "source": "rtsp://x"}]}
    asyncio.run(recorder.reconfigure({"video": {"storage": str(tmp_path), "cameras": [CAMERA]}, "recorder": legacy}))
    assert set(recorder.pipelines) == {"onvif-aa"} and recorder.cameras == {}


def test_детекция_без_записи_идёт_без_хранилища(tmp_path: Path) -> None:
    recorder, _published = _recorder(tmp_path)
    missing = tmp_path / "нет-диска"
    asyncio.run(recorder.reconfigure({"video": {"storage": "/media/nothing", "cameras": [CAMERA, DETECT_ONLY]}}))
    asyncio.run(recorder._check())  # noqa: SLF001
    assert recorder.why  # хранилища нет
    assert {cid: p.record_on for cid, p in recorder.pipelines.items()} == {"onvif-aa": False, "onvif-bb": False}
    assert "mega_home: конвейер onvif-bb" in recorder.env.spawned and "mega_home: конвейер onvif-aa" in recorder.env.spawned
    assert not missing.exists()


def test_уборка_видит_записи_конвейера(tmp_path: Path) -> None:
    recorder, _published = _recorder(tmp_path)
    asyncio.run(recorder.reconfigure({"video": {"storage": str(tmp_path), "cameras": [CAMERA, DETECT_ONLY]}}))
    sinks = recorder.sinks()
    assert [sink.spec.id for sink in sinks] == ["onvif-aa"]
    assert recorder.sink("onvif-aa") is sinks[0] and recorder.sink("onvif-bb") is None


def test_служба_отвечает_по_камере_конвейера(tmp_path: Path) -> None:
    """Дни, участки и сила движения — у записи конвейера тем же путём, что у прежней камеры."""
    from aiohttp.test_utils import TestClient, TestServer

    from mega_home.core import services
    from mega_home.core.recorder_api import start_api

    recorder, _published = _recorder(tmp_path)
    asyncio.run(recorder.reconfigure({"video": {"storage": str(tmp_path), "cameras": [CAMERA]}}))
    recorder.pipelines["onvif-aa"].share(1000.0, 0.4)

    async def scenario() -> tuple[int, int, dict, int]:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                days = (await client.get("/days?camera=onvif-aa")).status
                segments = (await client.get("/segments?camera=onvif-aa&from=0&to=100")).status
                level = await (await client.get("/activity?camera=onvif-aa&from=0&to=2000")).json()
                foreign = (await client.get("/days?camera=onvif-bb")).status
                return days, segments, level, foreign
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    days, segments, level, foreign = asyncio.run(scenario())
    assert (days, segments, foreign) == (200, 200, 404)
    assert level["activity"] == [[1000000, 0.4]]
