"""События камеры — метки на записи (`core/recorder_events.py`, `core/recorder_events_api.py`).

Решение заказчика 2026-10-29: событие показывается, только пока под ним есть запись; кадр события живёт
столько же, сколько запись дня (до этого вложения вытеснялись за часы — чёрные заглушки на объекте .90.250).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
import pytest

from mega_home.core import recorder as rec
from mega_home.core import recorder_events, services, storage
from mega_home.core.recorder_api import start_api
from mega_home.core.recorder_index import clean, day_of

SECTION = {
    "storage": "/media/video",
    "cameras": [
        {
            "id": "onvif-aa",
            "stream": "onvif_aa_main",
            "source": "rtsp://u:p@10.0.0.5/live/main",
            "mode": "motion",
            "retainDays": 1,
            "preSec": 10,
            "postSec": 20,
            "motion": {"access": "onvif-aa", "source": "events", "on": ['IsMotion" Value="true'], "off": ['IsMotion" Value="false']},
        }
    ],
}
BASE = 1_793_000_000.0


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


def _camera(tmp_path: Path) -> rec._Camera:  # noqa: SLF001
    recorder = rec.Recorder(FakeHost(tmp_path), lambda _listener: (lambda: None))
    recorder.storage = tmp_path
    camera = rec._Camera(recorder, rec.camera_specs(SECTION)[0], tmp_path / ".cache")  # noqa: SLF001
    camera.snapshot = lambda: b"jpeg-now"
    return camera


def _motion(camera: rec._Camera, on: bool, at: float) -> None:  # noqa: SLF001
    value = "true" if on else "false"
    camera.on_event({"access": "onvif-aa", "source": "events", "at": at, "data": {"text": f'<x Name="IsMotion" Value="{value}"/>'}})


def _flush(camera: rec._Camera) -> None:  # noqa: SLF001
    recorder_events.write(camera.events.take())


def test_метка_без_записи_на_диск_не_ложится(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    _motion(camera, True, BASE)
    _motion(camera, False, BASE + 30)
    _flush(camera)
    assert recorder_events.read(camera.index.root, BASE - 60, BASE + 60) == []


def test_кусок_записи_в_окне_кладёт_метку_с_кадром_и_концом(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    _motion(camera, True, BASE)
    camera.events.recorded(BASE - 5, BASE + 5, camera.spec.pre_s, camera.spec.post_s)
    _flush(camera)
    # Окно ещё идёт — конца нет; кадр — последний кадр камеры в момент начала.
    [row] = recorder_events.read(camera.index.root, BASE - 60, BASE + 60)
    assert row == {"id": str(int(BASE * 1000)), "start": int(BASE * 1000), "stop": None, "labels": [], "frame": True}
    _motion(camera, False, BASE + 30)
    camera.events.label("person", b"shot")  # окно закрыто — ярлык некому
    _flush(camera)
    [row] = recorder_events.read(camera.index.root, BASE - 60, BASE + 60)
    assert row["stop"] == int((BASE + 30) * 1000)
    assert row["labels"] == []
    path = recorder_events.picture(camera.index.root, row["id"])
    assert path is not None and path.read_bytes() == b"jpeg-now"
    # Закрытая и записанная метка из памяти ушла.
    assert camera.events.marks == []


def test_ярлык_человека_меняет_кадр_на_снимок_участка(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    _motion(camera, True, BASE)
    camera.events.label("person", b"person-shot")
    camera.events.label("person", b"second")  # второй раз ярлык не повторяется и кадр не меняет
    camera.events.recorded(BASE, BASE + 10, camera.spec.pre_s, camera.spec.post_s)
    _flush(camera)
    [row] = recorder_events.read(camera.index.root, BASE - 60, BASE + 60)
    assert row["labels"] == ["person"]
    path = recorder_events.picture(camera.index.root, row["id"])
    assert path is not None and path.read_bytes() == b"person-shot"


def test_метка_уходит_уборкой_вместе_с_днём_записи(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    _motion(camera, True, BASE)
    camera.events.recorded(BASE, BASE + 10, camera.spec.pre_s, camera.spec.post_s)
    _flush(camera)
    day = camera.index.root / day_of(BASE)
    assert (day / recorder_events.FILE).is_file()
    clean(tmp_path, [camera], BASE + 3 * 86400)
    assert not day.exists()
    assert recorder_events.read(camera.index.root, BASE - 60, BASE + 60) == []


def test_служба_отдаёт_события_и_кадр_навсегда(tmp_path: Path) -> None:
    camera = _camera(tmp_path)
    _motion(camera, True, BASE)
    camera.events.recorded(BASE, BASE + 10, camera.spec.pre_s, camera.spec.post_s)
    _flush(camera)
    recorder = camera.rec
    (tmp_path / ".cache").mkdir(exist_ok=True)
    recorder.cameras = {"onvif-aa": camera}
    mark = str(int(BASE * 1000))

    async def scenario() -> tuple[dict, int, str, bytes, int, int]:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                events = await (await client.get(f"/events?camera=onvif-aa&from={BASE - 60}&to={BASE + 60}")).json()
                frame = await client.get(f"/event-frame?camera=onvif-aa&id={mark}")
                body = await frame.read()
                missing = (await client.get("/event-frame?camera=onvif-aa&id=1")).status
                foreign = (await client.get(f"/events?camera=chuzhaya&from={BASE}&to={BASE + 1}")).status
                return events, frame.status, frame.headers.get("Cache-Control", ""), body, missing, foreign
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    events, status, cache, body, missing, foreign = asyncio.run(scenario())
    assert [row["id"] for row in events["events"]] == [mark]
    assert (status, body) == (200, b"jpeg-now")
    assert "immutable" in cache
    assert (missing, foreign) == (404, 404)
