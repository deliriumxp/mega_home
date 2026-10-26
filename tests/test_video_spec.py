"""Раздел `video` конфига (`core/video_spec.py`): камера одним описанием — потоки, запись, детекция.

Форма — контракт с менеджером (`video-config.util.ts`): дом её не толкует, только проверяет, что
имена годятся в путь RTSP и в каталог, а запись и детекция смотрят на поток, который камера назвала.
"""

from __future__ import annotations

from pathlib import Path

from mega_home.core import video_spec

CAMERA = {
    "id": "onvif-aa",
    "streams": {
        "main": {"name": "onvif_aa", "source": "rtsp://u:p@10.0.0.5/main"},
        "sub": {"name": "onvif_aa_sub", "source": "rtsp://u:p@10.0.0.5/sub"},
    },
    "record": {"stream": "main", "mode": "motion", "retainDays": 3, "preSec": 10, "postSec": 20},
    "detect": {"stream": "sub", "threshold": 30, "area": 6, "minSize": 1.5},
}

SECTION = {
    "storage": "/media/video",
    "limitGb": 2,
    "local": True,
    "preview": {"height": 240, "crf": 30, "gop": 4},
    "cameras": [
        CAMERA,
        {**CAMERA, "id": "../evil"},
        {"id": "no-streams", "record": CAMERA["record"]},
    ],
}


def test_раздел_video_берёт_камеры_с_потоками_и_безопасным_id() -> None:
    section = video_spec.video_section({"video": SECTION})
    assert section is not None
    assert [camera.id for camera in section.cameras] == ["onvif-aa"]
    camera = section.cameras[0]
    assert camera.streams["sub"] == video_spec.StreamPlan("onvif_aa_sub", "rtsp://u:p@10.0.0.5/sub")
    assert camera.record is not None and camera.record.mode == "motion" and camera.record.stream == "main"
    assert camera.record.retain_days == 3 and camera.record.camera_motion is None
    assert camera.detect is not None and camera.detect.stream == "sub" and camera.detect.params["minSize"] == 1.5
    assert section.storage == Path("/media/video") and section.local is True
    assert section.limit == 2 * 1024**3
    assert (section.preview.height, section.preview.crf, section.preview.gop) == (240, 30, 4)


def test_запись_и_детекция_на_несуществующем_потоке_отбрасываются() -> None:
    main_only = {**CAMERA, "streams": {"main": CAMERA["streams"]["main"]}, "record": {**CAMERA["record"], "stream": "sub"}}
    section = video_spec.video_section({"video": {**SECTION, "cameras": [main_only]}})
    assert section is not None
    camera = section.cameras[0]
    assert camera.record is None and camera.detect is None


def test_имя_потока_только_из_латиницы_цифр_и_подчёркивания() -> None:
    bad = {**CAMERA, "streams": {"main": {"name": "a/b", "source": "rtsp://x"}}}
    section = video_spec.video_section({"video": {**SECTION, "cameras": [bad]}})
    assert section is not None and section.cameras == ()


def test_словарь_движения_камеры_для_записи() -> None:
    words = {"access": "onvif-aa", "source": "events", "on": ["a"], "off": ["b"], "split": "M", "require": ["/Motion<"], "skip": ["I"]}
    camera = {**CAMERA, "record": {**CAMERA["record"], "cameraMotion": words}}
    section = video_spec.video_section({"video": {**SECTION, "cameras": [camera]}})
    assert section is not None
    record = section.cameras[0].record
    assert record is not None and record.camera_motion == video_spec.CameraMotion(
        "onvif-aa", "events", ("a",), ("b",), "M", ("/Motion<",), ("I",)
    )


def test_мост_к_записи_даёт_прежний_CameraSpec() -> None:
    section = video_spec.video_section({"video": SECTION})
    assert section is not None
    spec = video_spec.record_spec(section.cameras[0])
    assert (spec.id, spec.stream, spec.source, spec.motion_mode, spec.pre_s, spec.post_s, spec.retain_days) == (
        "onvif-aa", "onvif_aa", "rtsp://u:p@10.0.0.5/main", True, 10.0, 20.0, 3,
    )
    # Движение своей детекции приходит вызовом (`RecordSink.motion`), подстрок у записи нет.
    assert spec.on == () and spec.access == ""


def test_без_раздела_video_нет() -> None:
    assert video_spec.video_section({"recorder": {}}) is None
    assert video_spec.video_section({"video": "x"}) is None
    assert video_spec.video_section(None) is None
