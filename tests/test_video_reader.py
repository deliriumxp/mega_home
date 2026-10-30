"""Читатель потока конвейера (`core/video_reader.py`): один ffmpeg на поток камеры со всеми выходами.

⚠ ffmpeg здесь не запускается — проверяется план и командная строка. Что копия получает все пакеты при
декоде только ключевых кадров на втором выходе, проверено пробой 2026-10-26 (спека переделки движка).
"""

from __future__ import annotations

from pathlib import Path

from mega_home.core import video_spec
from mega_home.core.recorder_preview import PreviewTune
from mega_home.core.video_reader import ReaderFds, reader_command, reader_plan

CACHE = Path("/dev/shm/mega_home_rec/onvif-aa")
PREVIEW_ROOT = Path("/dev/shm/mega_home_preview/onvif-aa")
MAIN = {"name": "onvif_aa", "source": "rtsp://x/main"}
SUB = {"name": "onvif_aa_sub", "source": "rtsp://x/sub"}
RECORD = {"stream": "main", "mode": "motion"}


def _camera(streams: dict, record: dict | None, detect: dict | None) -> video_spec.CameraPlan:
    raw = {"id": "onvif-aa", "streams": streams, **({"record": record} if record else {}), **({"detect": detect} if detect else {})}
    section = video_spec.video_section({"video": {"storage": "/media/v", "cameras": [raw]}})
    assert section is not None
    return section.cameras[0]


CAM_MAIN_SUB = _camera({"main": MAIN, "sub": SUB}, RECORD, {"stream": "sub"})
CAM_MAIN_ONLY = _camera({"main": MAIN}, RECORD, {"stream": "main"})
CAM_RECORD_ONLY = _camera({"main": MAIN, "sub": SUB}, RECORD, None)
CAM_DETECT_ONLY = _camera({"main": MAIN, "sub": SUB}, None, {"stream": "sub", "person": {"width": 800}})


def _value(argv: list[str], flag: str) -> str:
    return argv[argv.index(flag) + 1]


def test_запись_основного_и_детекция_дополнительного_два_читателя() -> None:
    record, sub = reader_plan(CAM_MAIN_SUB, CACHE, record_on=True, preview_root=PREVIEW_ROOT)
    assert (record.key, record.stream, record.record_dir, record.preview_dir, record.latest, record.detect_fps) == (
        "record", "onvif_aa", CACHE, None, False, None,
    )
    assert (sub.key, sub.stream, sub.detect_fps, sub.preview_dir, sub.latest, sub.record_dir) == (
        "sub", "onvif_aa_sub", 5.0, PREVIEW_ROOT / "preview", True, None,
    )
    argv = reader_command("ffmpeg", "", record, PreviewTune(), ReaderFds(None, None))
    assert "-filter_complex" not in argv and "-skip_frame" not in argv


# Решение заказчика 2026-10-27 (ревью надёжности): запись — ВСЕГДА своим процессом, только копия. Общий ffmpeg
# с декодером останавливал запись, когда детекция («человек») не успевала читать трубу кадров, а отказ любого выхода
# снимал все. К камере подключение не прибавляется — поток раздаёт go2rtc.
def test_запись_всегда_своим_процессом_даже_на_том_же_потоке() -> None:
    record, main = reader_plan(CAM_MAIN_ONLY, CACHE, record_on=True, preview_root=PREVIEW_ROOT)
    assert (record.key, record.record_dir, record.detect_fps, record.preview_dir, record.latest) == ("record", CACHE, None, None, False)
    assert (main.key, main.record_dir, main.detect_fps, main.preview_dir, main.latest) == ("main", None, 5.0, PREVIEW_ROOT / "preview", True)
    argv = reader_command("ffmpeg", "", record, PreviewTune(), ReaderFds(None, None))
    assert "-filter_complex" not in argv and "-skip_frame" not in argv and _value(argv, "-c") == "copy"


def test_запись_без_детекции_превью_и_кадр_из_ключевых() -> None:
    record, main = reader_plan(CAM_RECORD_ONLY, CACHE, record_on=True, preview_root=PREVIEW_ROOT)
    assert record.key == "record" and record.record_dir == CACHE and not record.latest
    assert main.keyframes_only and main.latest and main.record_dir is None and main.preview_dir == PREVIEW_ROOT / "preview"
    argv = reader_command("ffmpeg", "", main, PreviewTune(), ReaderFds(None, 9))
    assert argv.index("-skip_frame") < argv.index("-i") and _value(argv, "-skip_frame") == "nokey"
    assert "-segment_list" in argv and _value(argv, "-segment_list") == str(PREVIEW_ROOT / "preview.csv")


def test_детекция_без_записи_без_превью() -> None:
    (sub,) = reader_plan(CAM_DETECT_ONLY, CACHE, record_on=True)
    assert sub.key == "sub" and sub.record_dir is None and sub.preview_dir is None and sub.latest
    assert sub.person_width == 800


def test_хранилище_не_годно_только_детекция_и_кадр() -> None:
    (sub,) = reader_plan(CAM_MAIN_SUB, CACHE, record_on=False)
    assert sub.key == "sub" and sub.record_dir is None and sub.preview_dir is None and sub.detect_fps == 5.0
    assert reader_plan(CAM_RECORD_ONLY, CACHE, record_on=False) == []


def test_вход_без_часов_стены() -> None:
    for camera in (CAM_MAIN_SUB, CAM_MAIN_ONLY, CAM_RECORD_ONLY, CAM_DETECT_ONLY):
        for spec in reader_plan(camera, CACHE, True):
            assert "-use_wallclock_as_timestamps" not in reader_command("ffmpeg", "", spec, PreviewTune(), ReaderFds(5, 9))


def test_особенности_камеры_данными_потока() -> None:
    """Звук и аргументы входа — данные менеджера (дом 0.6.13+): камера, чей звук объявлен без пакетов или с застывшими
    метками, пишется без звука; аргументы входа встают перед `-i` у всех читателей потока, без релиза дома."""
    tuned = {**MAIN, "input": ["-use_wallclock_as_timestamps", "1"], "audio": "drop"}
    record, main = reader_plan(_camera({"main": tuned}, RECORD, {"stream": "main"}), CACHE, True)
    for spec in (record, main):
        argv = reader_command("ffmpeg", "", spec, PreviewTune(), ReaderFds(None, None))
        assert argv[argv.index("-i") - 2 : argv.index("-i")] == ["-use_wallclock_as_timestamps", "1"]
    argv = reader_command("ffmpeg", "", record, PreviewTune(), ReaderFds(None, None))
    assert "0:a?" not in argv and "0:v" in argv
    # Без данных — как прежде: звук пишется, вход свой.
    plain = reader_command("ffmpeg", "", reader_plan(CAM_RECORD_ONLY, CACHE, True)[0], PreviewTune(), ReaderFds(None, None))
    assert "0:a?" in plain
    # Второго входа данными не завести.
    assert _camera({"main": {**MAIN, "input": ["-i", "rtsp://evil"]}}, RECORD, None).streams["main"].input == ()


def test_выход_записи_тот_же_что_у_прежнего_процесса() -> None:
    main, _sub = reader_plan(CAM_MAIN_SUB, CACHE, True)
    argv = reader_command("ffmpeg", "", main, PreviewTune(), ReaderFds(None, None))
    at = argv.index("-segment_time")
    assert argv[at - 2 : at + 8] == ["-f", "segment", "-segment_time", "10", "-segment_format", "matroska", "-reset_timestamps", "1", "-strftime", "1"]
    assert _value(argv, "-segment_list") == str(CACHE / "segments.csv")
    assert argv[-1] == str(CACHE / "%Y%m%dT%H%M%S.mkv")
    assert _value(argv, "-i") == f"rtsp://127.0.0.1:1986/onvif_aa"


def test_выходы_детекции_на_своих_дескрипторах() -> None:
    (sub,) = reader_plan(CAM_DETECT_ONLY, CACHE, True)
    argv = reader_command("nice", "", sub, PreviewTune(), ReaderFds(5, 9))
    graph = _value(argv, "-filter_complex")
    assert "split=3" in graph and "scale=176:96:flags=area,format=gray" in graph and "fps=0.5,scale=640:-2" in graph
    assert "pipe:1" in argv and "pipe:5" in argv and "pipe:9" in argv
    assert argv[argv.index("pipe:5") - 4 : argv.index("pipe:5")] == ["-c:v", "bmp", "-pix_fmt", "bgr24"]
    assert argv[argv.index("pipe:9") - 4 : argv.index("pipe:9")] == ["-c:v", "mjpeg", "-q:v", "5"]
    assert argv.index("-skip_loop_filter") < argv.index("-i")


def test_крупный_кадр_в_такт_сводки_с_маленьким() -> None:
    # `motion._person_pairs` сводит крупный № j с маленьким № j·ratio, ratio = round(fps · every).
    camera = _camera({"main": MAIN, "sub": SUB}, None, {"stream": "sub", "fps": 4, "person": {"every": 2}})
    (sub,) = reader_plan(camera, CACHE, True)
    assert sub.person_ratio == 8
    assert "fps=0.5,scale=640:-2:flags=area[big]" in _value(reader_command("ffmpeg", "", sub, PreviewTune(), ReaderFds(5, 9)), "-filter_complex")


def test_приоритет_ниже_дома() -> None:
    (sub,) = reader_plan(CAM_DETECT_ONLY, CACHE, True)
    argv = reader_command("ffmpeg", "/usr/bin/nice", sub, PreviewTune(), ReaderFds(None, 9))
    assert argv[:4] == ["/usr/bin/nice", "-n", "10", "ffmpeg"]
