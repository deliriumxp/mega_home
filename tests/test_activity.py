"""Сила движения по 10 с (`core/activity.py`): корзины детектора, ряд суток записи, служба `/activity`."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
from mega_home.core import activity, services, storage
from mega_home.core import recorder as rec
from mega_home.core.recorder_api import start_api

SECTION = {
    "storage": "/media/video",
    "cameras": [{
        "id": "cam", "stream": "cam_main", "source": "rtsp://u:p@10.0.0.5/main",
        "motion": {"access": "onvif-aa", "source": "motion", "on": ['"state":"start"']},
    }],
}
T0 = 1_792_836_000  # 2026-10-24 10:00 UTC — ровно на корзине


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})
    activity._live.clear()  # noqa: SLF001


def test_корзина_держит_максимум_и_отдаётся_закрытой() -> None:
    for at, share in ((T0 + 1, 0.02), (T0 + 4, 0.3), (T0 + 9, 0.1), (T0 + 12, 0.05)):
        activity.note("onvif-aa", at, share)
    assert activity.pending("onvif-aa", T0, T0 + 20) == [(T0, 0.3), (T0 + 10, 0.05)]
    # Идущая корзина остаётся в памяти до своего конца.
    assert activity.take("onvif-aa", T0 + 15) == [(T0, 0.3)]
    assert activity.take("onvif-aa", T0 + 15) == []
    assert activity.take("onvif-aa", T0 + 20) == [(T0 + 10, 0.05)]


def test_память_камеры_ограничена() -> None:
    for i in range(activity.MAX_BUCKETS + 50):
        activity.note("onvif-aa", T0 + i * activity.STEP_S, 0.1)
    kept = activity.pending("onvif-aa", 0, 1e12)
    assert len(kept) == activity.MAX_BUCKETS and kept[0][0] == T0 + 50 * activity.STEP_S


def test_ряд_суток_дописывается_и_читается_по_отрезку(tmp_path: Path) -> None:
    activity.append(tmp_path / "2026-10-24", [(T0, 0.3), (T0 + 10, 0.05)])
    activity.append(tmp_path / "2026-10-24", [(T0 + 600, 0.5)])
    (tmp_path / "2026-10-24" / activity.FILE).open("a").write("битая\n")
    assert activity.read(tmp_path / "2026-10-24", T0 + 5, T0 + 700) == [(T0, 0.3), (T0 + 10, 0.05), (T0 + 600, 0.5)]
    assert activity.read(tmp_path / "2026-10-25", 0, 1e12) == []


def _camera(root: Path) -> rec._Camera:  # noqa: SLF001
    recorder = rec.Recorder(FakeHost(root), lambda _listener: (lambda: None))
    recorder.storage = root
    recorder.local, recorder.limit = True, 1024**3
    camera = rec._Camera(recorder, rec.camera_specs(SECTION)[0], root / ".cache")  # noqa: SLF001
    recorder.cameras = {"cam": camera}
    return camera


def test_запись_кладёт_ряд_в_каталог_дня_а_служба_отдаёт_с_идущей_корзиной() -> None:
    root = Path(tempfile.mkdtemp(prefix="act-"))
    camera = _camera(root)
    camera.store_activity([(T0, 0.3), (T0 + 86_400, 0.2)])
    assert (root / "cam" / "2026-10-24" / activity.FILE).read_text() == f"{T0},0.3000\n"
    assert (root / "cam" / "2026-10-25" / activity.FILE).is_file()
    activity.note("onvif-aa", T0 + 30, 0.07)

    async def scenario() -> dict:
        runner = await start_api(camera.rec)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                return await (await client.get(f"/activity?camera=cam&from={T0 - 60}&to={T0 + 3600}")).json()
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    answer = asyncio.run(scenario())
    assert answer == {"step": 10, "activity": [[T0 * 1000, 0.3], [(T0 + 30) * 1000, 0.07]]}


def test_без_хранилища_ряд_не_ложится_на_флеш() -> None:
    root = Path(tempfile.mkdtemp(prefix="act-"))
    camera = _camera(root)
    camera.rec.local = False
    camera.store_activity([(T0, 0.3)])
    assert not (root / "cam").exists()
