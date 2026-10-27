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


class _Listing(_Answer):
    def __init__(self, streams: dict) -> None:
        self.streams = streams

    async def json(self, **_kwargs: object) -> dict:
        return self.streams


class _Session:
    def __init__(self, live: dict | None = None) -> None:
        self.puts: list[tuple[str, str]] = []
        self.live = live or {}

    def put(self, url: str, **_kwargs: object) -> _Answer:
        query = parse_qs(urlparse(url).query)
        self.puts.append((query["name"][0], query["src"][0]))
        return _Answer()

    def get(self, _url: str, **_kwargs: object) -> _Listing:
        return _Listing(self.live)


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


def test_конвейер_заводит_пропавшие_и_сменившие_источник_потоки(tmp_path: Path) -> None:
    """Ревью 2026-10-27: старт конвейера заводил ВСЕ потоки повторным PUT — go2rtc подменял объект потока, и к панели
    с малым числом RTSP-сессий шло лишнее подключение. Заводится только пропавший или сменивший источник."""
    session = _Session(live={"onvif_aa": {"producers": [{"url": "rtsp://u:p@10.0.0.5/old"}]}, "onvif_aa_sub": {"producers": [{"url": "rtsp://u:p@10.0.0.5/sub"}]}})
    recorder, _published = _recorder(tmp_path, session)
    pipeline = CameraPipeline(recorder, _plan(CAMERA), tmp_path / ".cache")
    asyncio.run(pipeline._restore())  # noqa: SLF001
    assert session.puts == [("onvif_aa", "rtsp://u:p@10.0.0.5/main")]
    fresh = _Session()
    recorder, _published = _recorder(tmp_path, fresh)
    asyncio.run(CameraPipeline(recorder, _plan(CAMERA), tmp_path / ".cache")._restore())  # noqa: SLF001
    assert fresh.puts == [("onvif_aa", "rtsp://u:p@10.0.0.5/main"), ("onvif_aa_sub", "rtsp://u:p@10.0.0.5/sub")]


def test_запуск_записи_сначала_разбирает_куски_прежнего_ffmpeg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-27: последний кусок перед перезапуском читателя (камера отвалилась — момент происшествия) не попадал
    в архив — новый ffmpeg переписывал список, кусок оставался сиротой в кэше."""
    from mega_home.core import video_pipeline
    from mega_home.core.video_reader import reader_plan

    import tempfile

    storage = Path(tempfile.mkdtemp(prefix="pipe-"))  # путь хранилища — латиницей (`storage_problem`)
    recorder, _published = _recorder(storage, _Session())
    recorder.local, recorder.limit = True, 1024**3
    continuous = {**CAMERA, "record": {"stream": "main", "mode": "continuous"}}
    pipeline = CameraPipeline(recorder, _plan(continuous), storage / ".cache")
    assert pipeline.record is not None
    cache = pipeline.record.cache
    cache.mkdir(parents=True)
    piece = cache / "20261023T120000.mkv"
    piece.write_bytes(b"x")
    (cache / "segments.csv").write_text(f"{piece},0.000000,10.000000\n")
    pipeline.record.index.loaded = True
    spec = reader_plan(pipeline.plan, pipeline.record.cache, True)[0]
    assert spec.key == "record"
    monkeypatch.setattr(video_pipeline.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="нет ffmpeg"):
        asyncio.run(pipeline._spawn(spec))  # noqa: SLF001
    assert not piece.exists() and pipeline.record.saved == 1


def test_сбой_после_запуска_ffmpeg_не_оставляет_сироту(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-27: исключение после старта ffmpeg (настройка детекции, трубы) оставляло процесс — он держал
    потребителя go2rtc, а с ним подключение к камере, до перезапуска HA."""
    import sys

    from mega_home.core import video_pipeline
    from mega_home.core.video_reader import reader_plan

    recorder, _published = _recorder(tmp_path, _Session())
    pipeline = CameraPipeline(recorder, _plan(DETECT_ONLY), tmp_path / ".cache")
    spec = reader_plan(pipeline.plan, pipeline.cache, False)[0]
    started: list = []
    real = asyncio.create_subprocess_exec

    async def spawn(*_argv: str, **kwargs: object) -> asyncio.subprocess.Process:
        proc = await real(sys.executable, "-c", "import time; time.sleep(30)", stdin=asyncio.subprocess.DEVNULL,
                          stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, pass_fds=kwargs.get("pass_fds", ()))
        started.append(proc)
        return proc

    async def broken(*_args: object) -> list:
        raise RuntimeError("сеть «человек» не загрузилась")

    monkeypatch.setattr(video_pipeline.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(pipeline, "_detection", broken)

    async def scenario() -> int | None:
        with pytest.raises(RuntimeError, match="человек"):
            await pipeline._spawn(spec)  # noqa: SLF001
        return started[0].returncode

    assert asyncio.run(scenario()) is not None


def test_задача_конвейера_не_падает_на_отказе_go2rtc(tmp_path: Path) -> None:
    """Ревью 2026-10-27: исключение из `run()` (go2rtc ещё не поднят, неверная ссылка) убивало задачу камеры молча —
    в состоянии пусто. Теперь причина в состоянии, задача повторяет сама."""

    class _Refusing(_Session):
        def put(self, url: str, **_kwargs: object) -> _Answer:
            answer = super().put(url, **_kwargs)
            answer.status = 500
            return answer

    recorder, _published = _recorder(tmp_path, _Refusing())
    pipeline = CameraPipeline(recorder, _plan(DETECT_ONLY), tmp_path / ".cache")

    async def scenario() -> None:
        task = asyncio.ensure_future(pipeline.run(False))
        await asyncio.sleep(0.2)
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert "go2rtc не принял поток" in pipeline.state()["readers"]


def test_запуск_читателя_заводит_только_пропавшие_потоки(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Пропал поток из go2rtc — запуск читателя заводит его снова. ⚠ Живой поток повторным PUT НЕ трогается: go2rtc
    подменяет объект потока, новый потребитель открывает к камере ещё одно подключение, и панель Akuvox с малым
    числом RTSP-сессий перестала отдавать основной поток (.90.250 2026-10-27, читатель падал каждые ~15 с)."""
    from mega_home.core import video_pipeline
    from mega_home.core.video_reader import reader_plan

    session = _Session(live={"onvif_aa": {"producers": [{"url": "rtsp://u:p@10.0.0.5/main"}]}})
    recorder, _published = _recorder(tmp_path, session)
    pipeline = CameraPipeline(recorder, _plan(DETECT_ONLY), tmp_path / ".cache")
    spec = reader_plan(pipeline.plan, pipeline.cache, False)[0]
    monkeypatch.setattr(video_pipeline.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="нет ffmpeg"):
        asyncio.run(pipeline._spawn(spec))  # noqa: SLF001
    assert session.puts == [("onvif_aa_sub", "rtsp://u:p@10.0.0.5/sub")]


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
