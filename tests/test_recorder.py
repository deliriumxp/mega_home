"""Запись камер (`core/recorder.py`, `core/recorder_api.py`): раздел конфига, хранилище, окна
движения, уборка, индекс и список воспроизведения.

⚠ ffmpeg и go2rtc здесь не запускаются: куски — пустые файлы с именами, как их кладёт
запись, длительность в имени. Настоящий поток проверен на стенде 2026-10-23 (камера
`192.168.88.202` → go2rtc 1.9.14 → куски Matroska → `ffmpeg:<список>#input=playlist`).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
import pytest

from mega_home.core import recorder as rec
from mega_home.core import services, storage
from mega_home.core.recorder_api import start_api
from mega_home.core.recorder_index import SegmentIndex


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Таблица монтирований — машина с одним диском: временная папка тестов бывает в tmpfs, а
    tmpfs для дома — ФС контейнера, под запись не годная (`storage.disk_kind`)."""
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


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
    # «Диск настроек» — `/proc`: заведомо другое устройство, чем каталог во временной папке.
    folder, other = Path(tempfile.mkdtemp(prefix="rec-")) / "video", Path("/proc")
    assert "нет каталога" in rec.storage_problem(folder, other)
    folder.mkdir()
    assert rec.storage_problem(folder, other) == ""
    # Тот же диск, что настройки дома, — флеш контроллера: только явно, «локальный диск».
    assert "системном диске" in rec.storage_problem(folder, folder.parent)
    assert rec.storage_problem(folder, folder.parent, local=True) == ""
    # Диск монтирует дом — путь обязан быть внутри смонтированной точки.
    assert "не смонтирован" in rec.storage_problem(folder, other, mounted_at=folder.parent)
    assert "не смонтирован" in rec.storage_problem(folder, other, mounted_at=Path("/elsewhere"))
    # Точка, куда дом сам монтирует, — не «папка контроллера»: пропал диск — писали бы на флеш.
    assert "точка, куда дом сам подключает" in rec.storage_problem(storage.MOUNT_POINT / "video", other)
    assert "абсолютный" in rec.storage_problem(Path("video"), other)
    assert "абсолютный" in rec.storage_problem(Path("/media/vid'eo#x"), other)


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

    async def scenario() -> tuple[str, int, str, int, str]:
        # Хранилище на том же диске, что настройки дома (`FakeHost.path`), — системный диск.
        await recorder._check()  # noqa: SLF001
        without = (recorder.why, len(recorder.cameras))
        recorder.local = True
        await recorder._check()  # noqa: SLF001
        no_limit = recorder.why
        recorder.limit = 1024**3
        await recorder._check()  # noqa: SLF001
        with_disk = (recorder.why, len(recorder.cameras))
        await recorder._stop_all()  # noqa: SLF001
        return (*without, *with_disk, no_limit)

    why_without, cams_without, why_with, cams_with, no_limit = asyncio.run(scenario())
    assert "системном диске" in why_without and cams_without == 0
    assert "только с лимитом" in no_limit
    assert why_with == "" and cams_with == 1


def test_висящая_проба_хранилища_не_плодит_потоков(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью хранилища 2026-10-23: `mount(2)` к пропавшему NAS держит поток исполнителя минутами;
    сторож раз в минуту не заводит вторую пробу поверх висящей, а говорит «не отвечает»."""
    folder = Path(tempfile.mkdtemp(prefix="rec-"))
    recorder = rec.Recorder(FakeHost(folder), lambda _listener: (lambda: None))
    recorder._specs = rec.camera_specs({**SECTION, "storage": str(folder)})  # noqa: SLF001
    recorder.storage, recorder.mount = folder, {"type": "nfs", "source": "10.0.0.9:/v"}
    started: list[int] = []
    gate = asyncio.Event()

    async def hanging(func, *args):  # noqa: ANN001, ANN201
        started.append(1)
        await gate.wait()
        return ""

    monkeypatch.setattr(recorder.env, "run", hanging)
    monkeypatch.setattr(rec, "STORAGE_CHECK_S", 0.01)

    async def scenario() -> tuple[int, str]:
        await recorder._check()  # noqa: SLF001
        await recorder._check()  # noqa: SLF001
        why = recorder.why
        gate.set()
        await asyncio.sleep(0)
        return len(started), why

    count, why = asyncio.run(scenario())
    assert count == 1 and "не отвечает" in why


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
    camera = _camera(tmp_path)
    camera.index.load()
    found = camera.index.between(now - 120, now)
    assert [(b, n) for b, n, _p in found] == [(now - 60, 10.0)]
    rec.clean(tmp_path, [camera], now)
    assert not old.exists() and fresh.exists()
    # Уборка ведёт индекс сама: удалённый день из него ушёл, диск заново не читается.
    assert [name for name, _f, _l in camera.index.days()] == ["2026-10-23"]


def test_индекс_в_памяти_ведёт_запись_а_не_перечитывает_диск(tmp_path: Path) -> None:
    """Каталог читается один раз; новый кусок и уборка правят индекс, запрос диск не трогает."""
    base = rec.epoch_of("20261023T120000")
    assert base is not None
    _segment(tmp_path, "onvif-aa", base)
    _segment(tmp_path, "onvif-aa", base + 10)
    index = SegmentIndex(tmp_path / "onvif-aa")
    index.load()
    # Кусок, положенный записью, виден без обхода каталога — файла нет вовсе.
    path = index.add(base + 20, 10.0, 5)
    assert path == tmp_path / "onvif-aa" / "2026-10-23" / "120020-10000.mkv"
    assert [b for b, _n, _p in index.between(base + 5, base + 25)] == [base, base + 10, base + 20]
    # Окно, начатое посреди куска, захватывает его; окно до записи — пусто.
    assert [b for b, _n, _p in index.between(base + 25, base + 29)] == [base + 20]
    assert index.between(base - 100, base - 1) == []
    # Куски через полночь — в двух каталогах дней, но один ответ по порядку.
    midnight = rec.epoch_of("20261024T000000")
    assert midnight is not None
    index.add(midnight - 5, 10.0)
    index.add(midnight + 5, 10.0)
    assert [b for b, _n, _p in index.between(midnight - 1, midnight + 1)] == [midnight - 5]
    assert [name for name, _f, _l in index.days()] == ["2026-10-23", "2026-10-24"]
    assert [b for b, _n, _p in index.oldest(2)] == [base, base + 10]
    assert index.forget(base) is None and index.forget(base + 10) is None and index.forget(base + 20) is None
    assert index.forget(midnight - 5) == "2026-10-23"
    assert index.day_names() == ["2026-10-24"]
    # Тот же момент повторно (перезапуск в ту же секунду) — замена, не второй кусок.
    index.add(midnight + 5, 4.0)
    assert [n for _b, n, _p in index.between(midnight, midnight + 20)] == [4.0]


def test_загрузка_индекса_одна_и_не_затирает_добавленное(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-24: индекс грузят запись камеры, служба и уборка, каждая в своём потоке; вторая
    загрузка присваиванием затирала куски, добавленные записью после ЕЁ обхода. Обход один, `add` ждёт его."""
    import threading

    from mega_home.core import recorder_index

    base = rec.epoch_of("20261023T120000")
    assert base is not None
    _segment(tmp_path, "onvif-aa", base)
    index = SegmentIndex(tmp_path / "onvif-aa")
    entered, release, scans = threading.Event(), threading.Event(), []
    real = recorder_index.scan_day

    def slow(folder: Path) -> list:
        scans.append(folder)
        entered.set()
        release.wait(5)
        return real(folder)

    monkeypatch.setattr(recorder_index, "scan_day", slow)
    loader = threading.Thread(target=index.ensure)
    loader.start()
    assert entered.wait(5)
    others = [threading.Thread(target=index.ensure), threading.Thread(target=index.add, args=(base + 10, 10.0))]
    for thread in others:
        thread.start()
    time.sleep(0.05)  # вторая загрузка и запись уже ждут первую
    release.set()
    for thread in [loader, *others]:
        thread.join(5)
    assert len(scans) == 1
    assert [b for b, _n, _p in index.between(base, base + 20)] == [base, base + 10]


def test_готовность_и_длительность_куска_по_списку_ffmpeg(tmp_path: Path) -> None:
    """ffmpeg сам пишет список готовых кусков (`-segment_list`, CSV): кусок берётся, когда он
    в списке, длительность — оттуда, ffprobe на каждый кусок не зовётся."""
    # Свой каталог (латиницей — путь хранилища) и «локальный диск» с лимитом: иначе `_save` откажет.
    folder = Path(tempfile.mkdtemp(prefix="rec-"))
    camera = _camera(folder, motion_mode=False)
    camera.rec.local, camera.rec.limit = True, 1024**3
    camera.cache.mkdir(parents=True)
    listing = camera.cache / rec.SEGMENT_LIST
    for stamp in ("20261023T120000", "20261023T120010", "20261023T120020"):
        (camera.cache / f"{stamp}.mkv").write_bytes(b"x")
    listing.write_text(f"{camera.cache}/20261023T120000.mkv,0.000000,10.040000\n")
    probed: list[Path] = []

    async def probe(path: Path) -> float:
        probed.append(path)
        return 7.0

    camera.rec.probe = probe  # type: ignore[method-assign]
    asyncio.run(camera._collect())  # noqa: SLF001
    day = folder / "onvif-aa" / "2026-10-23"
    assert sorted(p.name for p in day.iterdir()) == ["120000-10040.mkv"]
    assert probed == [] and camera.saved == 1
    # Список дописан — дочитывается с прошлого места; неполная строка ждёт следующего раза.
    with listing.open("a") as handle:
        handle.write(f"{camera.cache}/20261023T120010.mkv,10.040000,20.000000\n{camera.cache}/2026")
    asyncio.run(camera._collect())  # noqa: SLF001
    assert sorted(p.name for p in day.iterdir()) == ["120000-10040.mkv", "120010-9960.mkv"]
    # Остановка: кусок, не закрытый в список, измеряет ffprobe.
    asyncio.run(camera._collect(final=True))  # noqa: SLF001
    assert [p.name for p in probed] == ["20261023T120020.mkv"]
    assert "120020-7000.mkv" in {p.name for p in day.iterdir()}
    assert [b for b, _n, _p in camera.index.between(0, 2e9)] == [rec.epoch_of("20261023T120000"), rec.epoch_of("20261023T120010"), rec.epoch_of("20261023T120020")]


def test_уборка_сносит_превью_дня_и_считает_его_в_объёме(tmp_path: Path) -> None:
    now = rec.epoch_of("20261023T120000")
    assert now is not None
    old = _segment(tmp_path, "onvif-aa", now - 5 * 24 * 3600)
    (old.parent / "preview").mkdir()
    (old.parent / "preview" / "120000-600000.mp4").write_bytes(b"p")
    camera = _camera(tmp_path)
    camera.previews.load()
    assert camera.previews.between(0, 2e9)
    rec.clean(tmp_path, [camera], now)
    assert not old.parent.exists() and camera.previews.between(0, 2e9) == []
    # Объём дня после перезапуска (индекс с диска) — куски и превью дня: лимит считает всё, что лежит.
    pieces = [_segment(tmp_path, "onvif-aa", now - 3600 + 10 * i) for i in range(61)]
    for piece in pieces:
        piece.write_bytes(b"x" * 100)
    (pieces[0].parent / "preview").mkdir()
    (pieces[0].parent / "preview" / "110000-600000.mp4").write_bytes(b"p" * 50)
    camera = _camera(tmp_path)
    camera.index.load(sizes=True)
    assert camera.index.size() == 61 * 100 + 50
    rec.clean(tmp_path, [camera], now, limit=61 * 100)
    assert [p.exists() for p in pieces] == [False] * 60 + [True]


def test_первичное_состояние_подписки_окна_не_открывает(tmp_path: Path) -> None:
    """`PropertyOperation="Initialized"` — состояние на момент входа в подписку, не смена: без
    `skip` каждый вход (перезапуск дома) с «движения нет» давал окно-точку и 30 с лишней записи."""
    camera = _camera(tmp_path, split="NotificationMessage", skip=('PropertyOperation="Initialized"',))
    initial = '<NotificationMessage><Message PropertyOperation="Initialized"><x Name="IsMotion" Value="false"/>'
    camera.on_event(_frame(initial, 1000))
    assert camera.windows == []
    changed = '<NotificationMessage><Message PropertyOperation="Changed"><x Name="IsMotion" Value="true"/>'
    camera.on_event(_frame(initial + changed, 1010))
    assert camera.windows[-1][:2] == [1010, None]


def test_лимит_локального_диска_убирает_старое(tmp_path: Path) -> None:
    now = rec.epoch_of("20261023T120000")
    assert now is not None
    pieces = [_segment(tmp_path, "onvif-aa", now - 3600 * h) for h in (50, 30, 1)]
    for piece in pieces:
        piece.write_bytes(b"x" * 1000)
    rec.clean(tmp_path, [_camera(tmp_path)], now, limit=2500)
    # Сверх лимита — самые старые сутки первыми, свежие остаются.
    assert [p.exists() for p in pieces] == [False, True, True]


def test_служба_отдаёт_дни_участки_и_список(tmp_path: Path) -> None:
    base = rec.epoch_of("20261023T120000")
    assert base is not None
    for offset in (0, 10, 20, 60, 6 * 3600):
        _segment(tmp_path, "onvif-aa", base + offset)
    recorder = rec.Recorder(FakeHost(tmp_path), lambda _listener: (lambda: None))
    recorder.storage = tmp_path
    (tmp_path / ".cache").mkdir()  # его заводит старт камер (`_start_cameras`)
    recorder.cameras = {"onvif-aa": _camera(tmp_path)}

    async def scenario() -> tuple[dict, dict, dict, dict, int, dict, int, dict]:
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
                # За концом всей записи — последний кусок с его начала (ближайшая запись), а не 404.
                missing = await (await client.post(f"/playlist?camera=onvif-aa&at={int((base + 7 * 3600) * 1000)}")).json()
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
    assert missing["startMs"] == ms + 6 * 3600_000 and "nextMs" not in missing and foreign == 404


def _playlists(tmp_path: Path, pieces: list[float], *marks: float) -> list[dict]:
    """Ответы `/playlist` на метки `marks` (секунды) у записи из кусков по 10 с с началами `pieces`."""
    camera = _camera(tmp_path)
    for begin in pieces:
        camera.index.add(begin, 10.0)
    camera.index.loaded = True
    recorder = rec.Recorder(FakeHost(tmp_path), lambda _listener: (lambda: None))
    recorder.storage = tmp_path
    (tmp_path / ".cache").mkdir(exist_ok=True)
    recorder.cameras = {"onvif-aa": camera}

    async def scenario() -> list[dict]:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                return [await (await client.post(f"/playlist?camera=onvif-aa&at={int(mark * 1000)}")).json() for mark in marks]
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    return asyncio.run(scenario())


def test_метка_в_пустоте_ведёт_к_ближайшей_записи_суток(tmp_path: Path) -> None:
    """Ревью 2026-10-24: список искал запись только на 3 ч вперёд. Выбор дня в календаре (полночь) у
    записи по движению, начатой к полудню, — 404; край прошлых суток бандла (`recordedDayEdge`, 23:59:59
    перед сутками без записи) — 404 всегда. Теперь — вперёд в пределах суток, иначе последний кусок до метки."""
    midnight = rec.epoch_of("20261023T000000")
    assert midnight is not None
    morning, evening, after = midnight + 11 * 3600, midnight + 18 * 3600, midnight + 2 * 86400 + 600
    day, edge, inside = _playlists(tmp_path, [morning, evening, after], midnight, midnight + 86399, morning + 5)
    assert day["startMs"] == int(morning * 1000) and day["nextMs"] == int(evening * 1000)
    # Край суток: следующие сутки пусты, через них вперёд не идём — конец записи этого дня.
    assert edge["startMs"] == int(evening * 1000) and edge["nextMs"] == int(after * 1000)
    assert inside["startMs"] == int((morning + 5) * 1000)


def test_список_по_окну_не_теряет_следующий_кусок(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-24: список, кончившийся по `PLAYLIST_S`, называл продолжением кусок ЗА следующим —
    кусок, начатый впритык, отсекался фильтром «после стыка», и 10 с записи терялись каждые 3 ч."""
    from mega_home.core import recorder_api

    monkeypatch.setattr(recorder_api, "PLAYLIST_S", 25)
    base = rec.epoch_of("20261023T120000")
    assert base is not None
    [play] = _playlists(tmp_path, [base + 10 * i for i in range(6)], base)
    assert [part[0] for part in play["parts"]] == [int((base + 10 * i) * 1000) for i in range(3)]
    assert play["nextMs"] == int((base + 30) * 1000)


def test_без_раздела_записи_нет(tmp_path: Path) -> None:
    host = FakeHost(tmp_path)
    recorder = rec.Recorder(host, lambda _listener: (lambda: None))
    recorder.apply({"recorder": None})
    recorder.apply({"recorder": None})
    # Служба — сразу и одна (хранилище выбирают до записи), раздел без смены — не пересобирается.
    assert host.spawned == ["mega_home: служба записи", "mega_home: запись камер"]
    assert recorder.state()["enabled"] is False
    assert os.listdir(tmp_path) == []


def test_ключевой_кадр_до_метки(tmp_path: Path) -> None:
    """`inpoint` встаёт на ключевой кадр, а не между ними: копия потока всё равно начнётся с него, и
    `startMs` обязан назвать это время."""
    from mega_home.core.recorder_api import _keyframe_before

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not shutil.which("ffprobe"):
        pytest.skip("нужны ffmpeg и ffprobe")
    clip = tmp_path / "c.mkv"
    import subprocess

    subprocess.run(
        [ffmpeg, "-v", "error", "-y", "-f", "lavfi", "-i", "testsrc=size=160x90:rate=10", "-t", "12", "-c:v", "libx264",
         "-g", "40", "-keyint_min", "40", "-sc_threshold", "0", "-pix_fmt", "yuv420p", str(clip)],
        check=True,
    )

    class _Env:
        async def run(self, func, *args):
            return func(*args)

    class _Rec:
        env = _Env()

    snapped = asyncio.run(_keyframe_before(_Rec(), clip, 6.3))
    assert snapped == pytest.approx(4.0, abs=0.01)
    assert asyncio.run(_keyframe_before(_Rec(), clip, 8.0)) == pytest.approx(8.0, abs=0.01)
    # Нечитаемый файл — прежнее поведение: метка как есть.
    assert asyncio.run(_keyframe_before(_Rec(), tmp_path / "нет.mkv", 6.3)) == 6.3


def test_сутки_из_одного_превью_уходят_по_сроку(tmp_path: Path) -> None:
    """Превью пишется всегда, и у суток без движения кусков нет: индекс кусков их не знает, а уборка
    по сроку обязана снести и их."""
    now = rec.epoch_of("20261023T120000")
    assert now is not None
    day = tmp_path / "onvif-aa" / "2026-10-17" / "preview"
    day.mkdir(parents=True)
    (day / "120000-600000.mp4").write_bytes(b"p")
    rec.clean(tmp_path, [_camera(tmp_path)], now)
    assert not day.parent.exists()
